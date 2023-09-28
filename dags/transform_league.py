from datetime import datetime, timedelta
import logging
from airflow.decorators import dag, task

default_args = {
    'owner' : 'jh', 
    'retries': 0,
    'retry_delay' : timedelta(minutes=1),
    'provide_context': True
}



@dag(
    dag_id='transform_league_v1', 
    default_args=default_args, 
    description='Transform league from bronze to silver table, then repliicate a copy for ETL', 
    start_date=datetime(2023, 9, 5), 
    schedule='@once'
) 
def transform_league_stag():
    from airflow.providers.postgres.operators.postgres import PostgresOperator

    POSTGRES_CONN_ID = "postgres_azure" # "postgres_local" #

    @task
    def get_latest_file(container, entity):
        from airflow.providers.microsoft.azure.hooks.data_lake import AzureDataLakeStorageV2Hook
        import pandas as pd

        adls_client = AzureDataLakeStorageV2Hook(adls_conn_id='adls_id')
        adls_folder_path = f"{entity}"
        file_system_client = adls_client.get_file_system(file_system=container)
        
        logging.info(f"Listing in {container}/{adls_folder_path}")
        file_list = [item.get('name') for item in  file_system_client.get_paths(adls_folder_path)]

        df = pd.DataFrame(file_list, columns=['filename'])
        df['UpdateDateTimeString'] = df['filename'].apply(lambda x:x.rsplit('-')[-1].replace('.json', ''))
        df['UpdateDateTime'] = pd.to_datetime(df['UpdateDateTimeString'], format='%Y_%m_%d_%H_%M_%S')
        df = df.sort_values(by='UpdateDateTime', ignore_index=True, ascending=False)
        latest_filename = str(df.loc[0, 'filename'])

        return latest_filename
    
    @task
    def upsert_stage_table_from_json(container, entity, filename):
        from airflow.providers.microsoft.azure.hooks.data_lake import AzureDataLakeStorageV2Hook    
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from psycopg2.extras import execute_values
        import json
        
        adls_client = AzureDataLakeStorageV2Hook(adls_conn_id='adls_id')
        file_system_client = adls_client.get_file_system(file_system=container)
        file_client = file_system_client.get_file_client(filename)
        blob_data = file_client.download_file()
        json_data = blob_data.readall().decode('utf-8')
        json_data = json.loads(json_data)

        data_to_upsert = [list(doc.values()) for doc in json_data]


        pg_hook = PostgresHook.get_hook(POSTGRES_CONN_ID)    
        conn = pg_hook.get_conn()
        cur = conn.cursor()

        upsert_query = """
        INSERT INTO dota.league_stag (leagueid, ticket, banner, tier, name)
        VALUES %s
        ON CONFLICT (leagueid) DO UPDATE
        SET 
            ticket = EXCLUDED.ticket, 
            banner = EXCLUDED.banner, 
            tier = EXCLUDED.tier,
            name = EXCLUDED.name
        """

        try:
            logging.info("Inserting values to DB")
            execute_values(cur, upsert_query, data_to_upsert)
        except Exception as error:
            logging.info('Rollback')
            cur.execute("ROLLBACK;")
            raise error
        else:
            logging.info('Insert is successful')
            conn.commit()
            cur.close()
        finally:
            conn.close()

        return None
    
    update_metadata = PostgresOperator(
        task_id='update_metadata',
        sql='sql/update_metadata.sql',
        postgres_conn_id=POSTGRES_CONN_ID,
        params={'source':'OpenDota', 'object':'leagues', 'id_column_name':'leagueid', 'source_silver_table':'dota.league_stag'}
    )


    entity = 'leagues'
    container ='bronze'

    latest_file = get_latest_file(container, entity )
    upsert = upsert_stage_table_from_json(container, entity, latest_file)
    upsert >> update_metadata
    
transform_league_stag()