from datetime import datetime, timedelta
from airflow.decorators import dag, task

default_args = {
    'owner' : 'jh', 
    'retries': 0,
    'retry_delay' : timedelta(minutes=1),
    'provide_context': True
}

@dag(
    dag_id='transform_match_details_v1', 
    default_args=default_args, 
    description='Transform JSON from bronze layer to match players table in silver', 
    start_date=datetime(2023, 9, 11), 
    schedule='@once'
) 
def transform_match_details_stag():
    from airflow.providers.postgres.operators.postgres import PostgresOperator
    import logging

    POSTGRES_CONN_ID = "postgres_azure"

    # get_active_input_entity = PostgresOperator(
    #     task_id='get_active_entity',
    #     sql='sql/lookup_source_entity_ids.sql',
    #     postgres_conn_id='postgres_azure',
    #     params={'source':'OpenDota', 'object':'leagues'}, # To parameterize object by calling config
    #     do_xcom_push=True 
    # )

    get_active_input_entity = PostgresOperator(
        task_id='get_active_entity',
        sql='sql/lookup_source_entity_ids.sql',
        postgres_conn_id='postgres_azure',
        params={'source':'OpenDota', 'object':'match'}, # To parameterize object by calling config
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
        from psycopg2.extras import execute_values
        from jinja2 import Template
        import traceback
        import json
        import io
        import pandas as pd
        import numpy as np
        
        adls_client = AzureDataLakeStorageV2Hook(adls_conn_id='adls_id')
        adls_folder_path = f"{entity}"
        file_system_client = adls_client.get_file_system(file_system=container)

        outputs = []
        for filename in filenames:
            file_client = file_system_client.get_file_client(filename)
            blob_data = file_client.download_file()
            json_data = blob_data.readall().decode('utf-8')
            data = json.loads(json_data)

            list_field = [k for k, v in data.items() if isinstance(v, list)]
            dict_field = [k for k, v in data.items() if isinstance(v, dict)]
            non_flat_field = list_field # + dict_field
            cols_to_be_normalised = ['chat', 'draft_timings', 'objectives', 'picks_bans']
            cols_ignore = [ col for col in non_flat_field if col not in cols_to_be_normalised ] + ['cosmetics', 'players']
            col_key = 'match_id'

            output = {}

            # Simple Handling
            for col in cols_to_be_normalised:
                subdata = data.pop(col)
                df_subdata = pd.DataFrame(subdata)

                if not col_key in df_subdata.columns:
                    df_subdata[col_key] = data[col_key]

                output[col] = df_subdata.loc[:, [col_key] + [col for col in df_subdata.columns if col != col_key ]]

            # Special Handling
            cols_special_handling = ['players']
            target_cols_special = [['account_id',  'hero_id', 'match_id', \
                        'hero_damage', 'deaths', 'gold_per_min', 'xp_per_min', \
                            'kills', 'assists', 'isRadiant', 'win', 'lose', \
                            'last_hits', 'denies']]
            for col, target_cols in zip(cols_special_handling, target_cols_special):
                df_subdata = pd.json_normalize(data, 'players', max_level=0, errors='ignore')
                df_subdata = df_subdata.loc[:, target_cols]
                output[col] = df_subdata

            outputs.append(output)

        output_keys = outputs[0].keys()

        pg_hook = PostgresHook.get_hook(POSTGRES_CONN_ID)    
        conn = pg_hook.get_conn()
        cur = conn.cursor()

        for key in output_keys:
            merged_df = pd.concat([output[key] for output in outputs])

            output = io.StringIO()
            merged_df.to_csv(output, sep='\t', header=False, index=False)
            output.seek(0)

            logging.info(f"Size of data: {len(output.getvalue())}")

            sql_create_temp_table_template = """
                CREATE TEMP TABLE IF NOT EXISTS {{ temp_table }} (
                    {% for column in columns[:-1] %}{{ column[0] }} {{ column[1] }},
                    {% endfor %}
                    {{ columns[-1][0] }} {{ columns[-1][1] }}
                )
            """

            sql_upsert_target_template = """
                INSERT INTO {{ schema }}.{{ table }}(
                {% for column in columns %} {{ column[0] }}, {% endfor %} modified)
                
                SELECT 
                    {% for column in columns %}{{ column[0] }},
                    {% endfor %}
                    CURRENT_TIMESTAMP AS modified
                FROM {{ temp_table }}

                ON CONFLICT ({{ id_column }}) DO UPDATE
                SET
                    {% for column in columns %}{{ column[0] }} = EXCLUDED.{{ column[0] }},
                    {% endfor %}
                    modified = EXCLUDED.modified
            """

            schema = 'dota'
            table = f'match_{key}_stag'
            temp_table_name = f'match_{key}_temp'

            sql_get_columns = f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = '{schema}'
            AND table_name = '{table}'
            AND ordinal_position <> 1
            """
            cur.execute(sql_get_columns)
            columns = cur.fetchall()
            columns = [col for col in columns if col[0] not in ('created', 'modified')]

            sql_upsert_target =  Template(sql_upsert_target_template).\
                                    render(columns=columns, table=table, schema=schema, id_column='account_id', temp_table=temp_table_name)

            sql_create_temp_table = Template(sql_create_temp_table_template).\
                                    render(temp_table=temp_table_name, columns=columns)

            try:
                logging.info(f"Creating temp table: {temp_table_name}")
                logging.info(sql_create_temp_table)
                cur.execute(sql_create_temp_table)
                
                logging.info(f"Copy data into temp table: {temp_table_name}")
                logging.info(output.getvalue())
                cur.copy_from(output, temp_table_name, columns=(col[0] for col in columns))
                
                logging.info(f"Upserting data into table: {schema}.{table}")
                logging.info(sql_upsert_target)
                cur.execute(sql_upsert_target)


            except Exception as error:
                logging.info(f'Table {key} upsert failed. Executing rollback')
                cur.execute("ROLLBACK;")
                raise error
            else:
                logging.info(f'Insert table {key} is successful')

            
        conn.commit()
        cur.close()
        conn.close()

        return None

    # update_metadata = PostgresOperator(
    #     task_id='update_metadata',
    #     sql='sql/update_metadata.sql',
    #     postgres_conn_id='postgres_azure',
    #     params={'source':'OpenDota', 'object':'match', 'id_column_name':'match_id', 'source_silver_table':'dota.match_league_stag'}
    # )

    entity = 'matches'
    container ='bronze'

    input_entity = get_active_input_entity.output
    latest_jsons = get_latest_file(container, entity, input_entity)
    upsert = upsert_data_from_json(container=container, entity=entity, filenames=latest_jsons)
    # upsert >> update_metadata 

transform_match_details_stag()