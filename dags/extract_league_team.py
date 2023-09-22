from datetime import datetime, timedelta
import requests
import json
import os
import logging

from airflow.models import DAG, Variable
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.microsoft.azure.transfers.local_to_adls import LocalFilesystemToADLSOperator 
from airflow.providers.microsoft.azure.hooks.data_lake import AzureDataLakeStorageV2Hook

default_args = {
    'owner' : 'jh', 
    'retries': 0,
    'retry_delay' : timedelta(minutes=1),
    'provide_context': True
}

logger = logging.getLogger(__name__)

@dag(
    dag_id='extract_league_team_v1', 
    default_args=default_args, 
    description='Get list of hero from OpenDota API', 
    start_date=datetime(2023, 9, 11), 
    schedule='@once'
) 
def extract_league_team():

    lookup_source_api = PostgresOperator(
        task_id='get_config_api',
        sql='SELECT source_api, is_active FROM etl.ingestion_config WHERE config_id = 3',
        postgres_conn_id='postgres_azure',
        do_xcom_push=True
    )

    lookup_source_entity = PostgresOperator(
        task_id='get_config_entity',
        sql='SELECT leagueid FROM dota.league_stag WHERE is_etl_active=true',
        postgres_conn_id='postgres_azure',
        do_xcom_push=True
    )

    @task
    def call_api_save_json(config, entity, entity_id): 
        API_KEY =  Variable.get("OPENDOTA_API_KEY")
        API_URL, is_active = config[0]

        if not is_active:
            raise ValueError('Config is_active is not enabled. Please check config table')

        params = {}
        entity_id = entity_id[0]
        API_URL = API_URL.format(entity_id)
        logger.info(f'Calling API - {API_URL}')
        response = requests.get(API_URL, params=params)
        if response.status_code != 200:
            print('Request failed with status code:', response.status_code)
            response.raise_for_status() 

        data = response.json()
        folder_path = f"~/{entity}"
        filename = entity + '-' + str(entity_id) + '-' + datetime.now().strftime("%Y_%m_%d_%H_%M_%S") + '.json'

        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
            print(f'Creating folder at {folder_path}')
        with open(os.path.join(folder_path, filename), 'w') as file:
            print(f'Saving data to file {filename} in {folder_path}')
            json.dump(data, file, indent=4)

        return folder_path
            

    @task
    def upload_adls_batch(container, entity, folder_path):
        adls_client = AzureDataLakeStorageV2Hook(adls_conn_id='adls_id')
        adls_folder_path = f"{entity}"
        folder_path = f"~/{entity}"
        print(f"{len(os.listdir(folder_path))} files found in folder: {folder_path}")
        for filename in os.listdir(folder_path):
            print(f"Uploading {filename} to {container}@{adls_folder_path}")
            directory_client = adls_client.get_directory_client(file_system_name=container, directory_name=adls_folder_path)
            file_client = directory_client.create_file(filename)
            with open(os.path.join(folder_path, filename), "rb") as data:
                file_client.upload_data(data, overwrite=True)
        return folder_path

    @task
    def delete_local_folder(folder_path):
        import shutil
        try:
            shutil.rmtree(folder_path)
            print(f"Folder '{folder_path}' and its contents removed successfully.")
        except OSError as e:
            print(f"Failed to remove folder '{folder_path}': {e}")

    entity = 'leagues-team'
    container ='bronze'

    config = lookup_source_api.output
    entity_ids = lookup_source_entity.output
    save_folder_path = call_api_save_json.partial(config=config, entity=entity).expand(entity_id=entity_ids)
    delete_folder_path = upload_adls_batch(container, entity, save_folder_path)
    delete_local_folder(delete_folder_path)

extract_league_team()