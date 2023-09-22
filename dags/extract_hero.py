from datetime import datetime, timedelta
import requests
import json
import os
import shutil

from airflow.models import DAG, Variable
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

def get_hero_from_api(entity, **kwargs):
    ti = kwargs['ti']
    API_KEY =  Variable.get("OPENDOTA_API_KEY")
    API_URL, is_active = ti.xcom_pull(task_ids='get_config')[0]

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

def upload_adls(container, entity):
    adls_client = AzureDataLakeStorageV2Hook(adls_conn_id='adls_id')
    folder_path = f"~/{entity}"
    adls_folder_path = f"{entity}"
    print(f"{len(os.listdir(folder_path))} files found in folder: {folder_path}")
    for filename in os.listdir(folder_path):
        print(f"Uploading {filename} to {container}@{adls_folder_path}")
        # adls_client.upload_file_to_directory(file_system_name=container, directory_name=adls_folder_path, file_name=filename, file_path=os.path.join(folder_path, filename), overwrite=True)
        directory_client = adls_client.get_directory_client(file_system_name=container, directory_name=adls_folder_path)
        file_client = directory_client.create_file(filename)
        with open(os.path.join(folder_path, filename), "rb") as data:
            file_client.upload_data(data, overwrite=True)

def delete_folder(entity):
    folder_path = f"~/{entity}"
    try:
        shutil.rmtree(folder_path)
        print(f"Folder '{folder_path}' and its contents removed successfully.")
    except OSError as e:
        print(f"Failed to remove folder '{folder_path}': {e}")


op_kwargs={'entity': 'heroes', 'container':'bronze'}


with DAG(
    dag_id='extract_hero_v1', 
    default_args=default_args, 
    description='Get list of hero from OpenDota API', 
    start_date=datetime(2023, 9, 5), 
    schedule='@once'
) as dag:
    get_config = PostgresOperator(
        task_id='get_config',
        sql='sql/lookup_extract_hero.sql',
        postgres_conn_id='postgres_azure',
        do_xcom_push=True
    )

    get_api = PythonOperator(
        task_id = 'get-heroes-from-api',
        python_callable=get_hero_from_api,
        do_xcom_push=True,
        op_kwargs=op_kwargs,
        provide_context=True
    )

    upload_adls_batch = PythonOperator(
        task_id='upload_adls', 
        python_callable=upload_adls,
        op_kwargs=op_kwargs
    )

    delete_local_folder = PythonOperator(
        task_id='clean_up_folder',
        python_callable=delete_folder,
        op_kwargs=op_kwargs

    )
    
    # BashOperator(
    #     task_id='cleanup_folder', 
    #     bash_command="cd ~; rm -r $folder;",
    #     env={'folder': '~/' + op_kwargs['entity']}
    # )

    get_config >> get_api >> upload_adls_batch >> delete_local_folder
    