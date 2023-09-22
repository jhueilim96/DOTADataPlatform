from datetime import datetime, timedelta
import logging
from airflow.models import DAG, Variable
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.microsoft.azure.transfers.local_to_adls import LocalFilesystemToADLSOperator 

default_args = {
    'owner' : 'jh', 
    'retries': 0,
    'retry_delay' : timedelta(minutes=1),
    'provide_context': True
}

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)
POSTGRES_CONN_ID = "postgres_azure"


@dag(
    dag_id='transform_league_team_v1', 
    default_args=default_args, 
    description='Transform league team from bronze to silver table, then repliicate a copy for ETL', 
    start_date=datetime(2023, 9, 5), 
    schedule='@once'
) 
def transform_league_team_stag():

    get_active_input_entity = PostgresOperator(
        task_id='get_active_entity',
        sql='sql/lookup_input_entity.sql',
        postgres_conn_id='postgres_azure',
        params={'object':'leagues'},
        do_xcom_push=True
    )

    @task
    def get_latest_file(container, entity, entity_id=[]):
        from airflow.providers.microsoft.azure.hooks.data_lake import AzureDataLakeStorageV2Hook
        import pandas as pd

        adls_client = AzureDataLakeStorageV2Hook(adls_conn_id='adls_id')
        adls_folder_path = f"{entity}"
        file_system_client = adls_client.get_file_system(file_system=container)
        
        logging.info(f"Listing in {container}/{adls_folder_path}")
        file_list = [item.get('name') for item in  file_system_client.get_paths(adls_folder_path)]

        logging.info(f"{len(file_list)} files found")
        df = pd.DataFrame(file_list, columns=['filename'])
        df['UpdateDateTimeString'] = df['filename'].apply(lambda x:x.rsplit('-')[-1].replace('.json', ''))
        df['UpdateDateTime'] = pd.to_datetime(df['UpdateDateTimeString'], format='%Y_%m_%d_%H_%M_%S')

        
        input_entity_ids = [str(row[-1]) for row in entity_id]
        logging.info(f'Inputs ids: {input_entity_ids}')
        if input_entity_ids != []:
            df['EntityId'] = df['filename'].apply(lambda x:x.rsplit('-')[-2])
            logging.info(f"Ids from file list: {df['EntityId'].tolist()}")

            # Filter by id
            df = df.loc[df['EntityId'].isin(input_entity_ids), :]
            df = df.loc[df.groupby('EntityId')['UpdateDateTime'].idxmax()]
            

        latest_jsons =  df['filename'].tolist()
        return latest_jsons
    
    @task
    def upsert_data_from_json(container, entity, filename):
        from airflow.providers.microsoft.azure.hooks.data_lake import AzureDataLakeStorageV2Hook    
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from psycopg2.extras import execute_values
        import traceback
        import json
        
        adls_client = AzureDataLakeStorageV2Hook(adls_conn_id='adls_id')
        adls_folder_path = f"{entity}"
        file_system_client = adls_client.get_file_system(file_system=container)
        file_client = file_system_client.get_file_client(filename)
        blob_data = file_client.download_file()
        json_data = blob_data.readall().decode('utf-8')
        json_data = json.loads(json_data)

        data_to_upsert = [list(doc.values()) for doc in json_data]
        logging.info(data_to_upsert)

        pg_hook = PostgresHook.get_hook(POSTGRES_CONN_ID)    
        conn = pg_hook.get_conn()
        cur = conn.cursor()

        upsert_query = """
        INSERT INTO dota.league_team_stag (team_id, rating, wins, losses, last_match_time_raw, name, tag, logo_url)
        VALUES %s
        ON CONFLICT (team_id) DO UPDATE
        SET 
            rating = EXCLUDED.rating,
            wins = EXCLUDED.wins,
            losses = EXCLUDED.losses,
            last_match_time_raw = EXCLUDED.last_match_time_raw,
            name = EXCLUDED.name, 
            tag = EXCLUDED.tag, 
            logo_url = EXCLUDED.logo_url
        """

        try:
            logger.info("Inserting values to DB")
            execute_values(cur, upsert_query, data_to_upsert)
        except Exception as error:
            logger.info('Rollback')
            cur.execute("ROLLBACK;")
            raise error
        else:
            logger.info('Insert is successful')
            conn.commit()
            cur.close()
        finally:
            conn.close()

        return None

    update_metadata = PostgresOperator(
        task_id='update_metadata',
        sql='sql/update_metadata.sql',
        postgres_conn_id='postgres_azure',
        params={'source':'OpenDota', 'object':'leagues-team', 'id_column_name':'team_id', 'source_silver_table':'dota.league_team_stag'}
    )
    
    
    entity = 'leagues-team'
    container ='bronze'

    input_entity = get_active_input_entity.output
    latest_jsons = get_latest_file(container, entity, input_entity)
    upsert = upsert_data_from_json.partial(container=container, entity=entity).expand(filename=latest_jsons)
    upsert >> update_metadata
    
transform_league_team_stag()