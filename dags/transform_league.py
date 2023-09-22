from datetime import datetime, timedelta
import requests
import json
import os
import shutil
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
    dag_id='transform_league_v1', 
    default_args=default_args, 
    description='Transform league from bronze to silver table, then repliicate a copy for ETL', 
    start_date=datetime(2023, 9, 5), 
    schedule='@once'
) 
def transform_league_stag():

    @task
    def get_latest_file(container, entity):
        from airflow.providers.microsoft.azure.hooks.data_lake import AzureDataLakeStorageV2Hook
        import pandas as pd

        adls_client = AzureDataLakeStorageV2Hook(adls_conn_id='adls_id')
        adls_folder_path = f"{entity}"
        file_system_client = adls_client.get_file_system(file_system=container)
        
        logger.info(f"Listing in {container}/{adls_folder_path}")
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
        params={'source':'OpenDota', 'object':'leagues', 'id_column_name':'leagueid'}
    )


    entity = 'leagues'
    container ='bronze'

    latest_file = get_latest_file(container, entity )
    upsert = upsert_stage_table_from_json(container, entity, latest_file)
    upsert >> update_metadata
    
transform_league_stag()