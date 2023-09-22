from datetime import datetime, timedelta
from airflow.decorators import dag, task

default_args = {
    'owner' : 'jh', 
    'retries': 0,
    'retry_delay' : timedelta(minutes=1),
    'provide_context': True
}

@dag(
    dag_id='transform_hero_v1', 
    default_args=default_args, 
    description='Transform JSON from bronze layer to hero table in silver', 
    start_date=datetime(2023, 9, 11), 
    schedule='@once'
) 
def transform_hero_stag():
    import logging
    from airflow.providers.postgres.operators.postgres import PostgresOperator
    
    
    POSTGRES_CONN_ID = "postgres_azure"

    create_tables = PostgresOperator(
        task_id='create_table',
        sql='sql/create_table_stag_hero.sql',
        postgres_conn_id='postgres_azure'
    )

    @task
    def get_latest_file(container:str, entity:str) -> str:
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
        import pandas as pd
        import traceback
        import json
        import io
        
        adls_client = AzureDataLakeStorageV2Hook(adls_conn_id='adls_id')
        adls_folder_path = f"{entity}"
        file_system_client = adls_client.get_file_system(file_system=container)
        file_client = file_system_client.get_file_client(filename)
        blob_data = file_client.download_file()
        json_data = blob_data.readall().decode('utf-8')
        json_data = json.loads(json_data)

        df_hero = pd.DataFrame(json_data).drop(columns='roles')
        output_hero = io.StringIO()
        df_hero.to_csv(output_hero, sep='\t', header=False, index=False)
        output_hero.seek(0)

        df_hero_role = pd.json_normalize(json_data, record_path='roles', meta='id').rename(columns={0:'role'})
        output_role = io.StringIO()
        df_hero_role.to_csv(output_role, sep='\t', header=False, index=False)
        output_role.seek(0)

        pg_hook = PostgresHook.get_hook(POSTGRES_CONN_ID)    
        conn = pg_hook.get_conn()
        cur = conn.cursor()

        insert_query_hero = """
        INSERT INTO dota.hero_stag (id, name, localized_name, primary_attr, attack_type, legs)
        VALUES %s
        """
        insert_query_hero_role = """
        INSERT INTO dota.hero_role_stag (role, id)
        VALUES %s
        """
       
        try:
            logging.info("Inserting values to DB")
            cur.copy_expert('COPY dota.hero_stag (id, name, localized_name, primary_attr, attack_type, legs) FROM STDIN', file=output_hero)
            cur.copy_expert('COPY dota.hero_role_stag (role, id) FROM STDIN', file=output_role)
        except Exception as error:
            logging.info('Insert failed. Executing rollback')
            cur.execute("ROLLBACK;")
            raise error
        else:
            logging.info('Insert is successful')
            conn.commit()


        cur.close()
        conn.close()

        return None



    entity = 'heroes'
    container ='bronze'

    latest_file = get_latest_file(container, entity )
    upsert = upsert_stage_table_from_json(container, entity, latest_file)
    (latest_file, create_tables) >> upsert



transform_hero_stag()