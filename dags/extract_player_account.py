from datetime import datetime, timedelta
from airflow.decorators import dag, task

default_args = {
    'owner' : 'jh', 
    'retries': 2,
    'retry_delay' : timedelta(minutes=1),
    'provide_context': True
}

@dag(
    dag_id='extract_player_account_v1', 
    default_args=default_args, 
    description='Get player account details from OpenDota API', 
    start_date=datetime(2023, 9, 11), 
    schedule='@once'
) 
def extract_player_account():
    from airflow.models import Variable
    from airflow.providers.postgres.operators.postgres import PostgresOperator
    from airflow.providers.microsoft.azure.hooks.data_lake import AzureDataLakeStorageV2Hook
    import logging
    import os
    import requests
    import json
    import time


    lookup_source_api = PostgresOperator(
        task_id='get_config_api',
        sql='sql/lookup_source_api.sql',
        postgres_conn_id='postgres_azure',
        do_xcom_push=True,
        params={'config_id':5}
    )

    lookup_source_entity = PostgresOperator(
        task_id='get_config_entity',
        sql='sql/lookup_source_entity_ids.sql',
        postgres_conn_id='postgres_azure',
        do_xcom_push=True,
        params = {'source':'OpenDota', 'object':'player'}
    )

    @task
    def call_api_save_json(config, entity, entity_id, api_delay_seconds=40, max_active_tis_per_dag=30): 
        API_KEY =  Variable.get("OPENDOTA_API_KEY")
        API_URL, is_active = config[0]

        if not is_active:
            raise ValueError('Config is_active is not enabled. Please check config table')

        params = {}
        entity_id = entity_id[0]
        API_URL = API_URL.format(entity_id)
        logging.info(f'Calling API - {API_URL}')
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

        time.sleep(api_delay_seconds)
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

    entity = 'players'
    container ='bronze'

    config = lookup_source_api.output
    entity_ids = lookup_source_entity.output
    save_folder_path = call_api_save_json.partial(config=config, entity=entity).expand(entity_id=entity_ids)
    delete_folder_path = upload_adls_batch(container, entity, save_folder_path)
    delete_local_folder(delete_folder_path)


extract_player_account()