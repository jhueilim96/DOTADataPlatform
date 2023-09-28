from datetime import datetime, timedelta
import logging
from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner' : 'jh', 
    'retries': 0,
    'retry_delay' : timedelta(minutes=1),
    'provide_context': True
}


@dag(
    dag_id='transform_team_player_v3',  
    default_args=default_args, 
    description='Transform JSON from bronze layer to player_stag table in silver', 
    start_date=datetime(2023, 9, 5), 
    schedule='@once'
) 
def transform_league_team_stag():

    POSTGRES_CONN_ID = "postgres_azure" # "postgres_local" #

    get_active_input_entity = PostgresOperator(
        task_id='get_active_entity',
        sql='sql/lookup_source_entity_ids.sql',
        postgres_conn_id=POSTGRES_CONN_ID,
        params={'source':'OpenDota', 'object':'leagues-team'}, # To parameterize object by calling config
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
    def upsert_data_from_json(container, entity, filenames):
        from airflow.providers.microsoft.azure.hooks.data_lake import AzureDataLakeStorageV2Hook    
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        import json
        import io
        import pandas as pd
        
        adls_client = AzureDataLakeStorageV2Hook(adls_conn_id='adls_id')
        file_system_client = adls_client.get_file_system(file_system=container)

        dfs = []
        for filename in filenames:
            file_client = file_system_client.get_file_client(filename)
            blob_data = file_client.download_file()
            json_data = blob_data.readall().decode('utf-8')
            json_data = json.loads(json_data)
            df = pd.DataFrame(json_data)

            # Assign team id to df
            team_id = filename.split('-')[-2]
            df['team_id'] = int(team_id)
            df['is_current_team_member'] = df['is_current_team_member'].astype(bool)

            dfs.append(df)

        merged_df = pd.concat(dfs, ignore_index=True)
        output = io.StringIO()
        merged_df.to_csv(output, sep='\t', header=False, index=False)
        output.seek(0)

        logging.info(f"Size of data: {len(output.getvalue())}")

        pg_hook = PostgresHook.get_hook(POSTGRES_CONN_ID)    
        conn = pg_hook.get_conn()
        cur = conn.cursor()

        sql_create_temp_table = """
            CREATE TEMP TABLE IF NOT EXISTS player_temp (
                team_id bigint not null,
                account_id bigint not null,
                name varchar(255), 
                games_played int,
                wins int,
                is_current_team_member boolean
            )
        """

        sql_upsert_target = """
            INSERT INTO dota.player_stag(
            team_id, account_id, name, games_played, wins, is_current_team_member, modified)
            
            SELECT 
                team_id, 
                account_id, 
                name, 
                games_played, 
                wins, 
                is_current_team_member,
                CURRENT_TIMESTAMP AS modified
            FROM player_temp

            ON CONFLICT (team_id, account_id) DO UPDATE
            SET
                name = EXCLUDED.name, 
                games_played = EXCLUDED.games_played, 
                wins = EXCLUDED.wins, 
                is_current_team_member = EXCLUDED.is_current_team_member,
                modified = EXCLUDED.modified
        """

        try:
            logging.info("Creating temp table DB")
            cur.execute(sql_create_temp_table)
            
            logging.info("Copy data into temp table")
            cur.copy_from(output, 'player_temp', \
                          columns=('account_id', 'name', 'games_played', 'wins', 'is_current_team_member','team_id'))
            
            logging.info(f"Upserting data into table: player_stag")
            cur.execute(sql_upsert_target)


        except Exception as error:
            logging.info('Data upsert failed. Executing rollback')
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
        params={'source':'OpenDota', 'object':'player', 'id_column_name':'account_id', 'source_silver_table':'dota.player_stag'}
    )
    
    
    entity = 'teams-players'
    container ='bronze'

    input_entity = get_active_input_entity.output
    latest_jsons = get_latest_file(container, entity, input_entity)
    upsert = upsert_data_from_json(container=container, entity=entity, filenames=latest_jsons)
    upsert >> update_metadata
    
transform_league_team_stag()