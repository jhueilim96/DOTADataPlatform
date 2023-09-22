from datetime import datetime, timedelta
from airflow.decorators import dag, task
default_args = {
    'owner' : 'jh', 
    'retries': 0,
    'retry_delay' : timedelta(minutes=1),
    'provide_context': True
}

@dag(
    dag_id='extract_league_v1', 
    default_args=default_args, 
    description='Get list of hero from OpenDota API', 
    start_date=datetime(2023, 9, 5), 
    schedule='@once'
) 
def extract_league():
    from airflow.providers.postgres.operators.postgres import PostgresOperator

    get_config = PostgresOperator(
        task_id='get_config',
        sql='sql/lookup_extract_league.sql',
        postgres_conn_id='postgres_azure',
        do_xcom_push=True
    )

    @task
    def get_api(config, entity): 
        from airflow.models import  Variable
        import requests
        import json
        import os
        API_KEY =  Variable.get("OPENDOTA_API_KEY")
        API_URL, is_active = config[0]

        if not is_active:
            raise ValueError('Config is_active is not enabled. Please check config table')

        params = {}
        print('Calling API')
        response = requests.get(API_URL, params=params)
        if response.status_code != 200:
            print('Request failed with status code:', response.status_code)

        data = response.json()
        folder_path = f"~/{entity}"
        filename = entity + '-' + datetime.now().strftime("%Y_%m_%d_%H_%M_%S") + '.json'

        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
            print(f'Creating folder at {folder_path}')
        with open(os.path.join(folder_path, filename), 'w') as file:
            print(f'Saving data to file {filename} in {folder_path}')
            json.dump(data, file, indent=4)
        return folder_path

    @task
    def upload_adls_batch(container, entity, folder_path):
        import os
        from airflow.providers.microsoft.azure.transfers.local_to_adls import LocalFilesystemToADLSOperator 
        from airflow.providers.microsoft.azure.hooks.data_lake import AzureDataLakeStorageV2Hook

        adls_client = AzureDataLakeStorageV2Hook(adls_conn_id='adls_id')
        adls_folder_path = f"{entity}"
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
    
    
    op_kwargs={'entity': 'leagues', 'container':'bronze'}

    save_folder_path = get_api(get_config.output, op_kwargs['entity'])
    delete_folder_path = upload_adls_batch(op_kwargs['container'], op_kwargs['entity'], save_folder_path )
    delete_local_folder(delete_folder_path)
    

extract_league()