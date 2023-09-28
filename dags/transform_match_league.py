from datetime import datetime, timedelta
from airflow.decorators import dag, task

default_args = {
    'owner' : 'jh', 
    'retries': 0,
    'retry_delay' : timedelta(minutes=1),
    'provide_context': True
}

@dag(
    dag_id='transform_match_league_v1', 
    default_args=default_args, 
    description='Transform JSON from bronze layer to match_league_stag table in silver', 
    start_date=datetime(2023, 9, 11), 
    schedule='@once'
) 
def transform_match_league_json_to_table_stag():
    from airflow.providers.postgres.operators.postgres import PostgresOperator
    import logging

    POSTGRES_CONN_ID = " postgres_azure" # "postgres_local" #

    get_active_input_entity = PostgresOperator(
        task_id='get_active_entity',
        sql='sql/lookup_source_entity_ids.sql',
        postgres_conn_id=POSTGRES_CONN_ID,
        params={'source':'OpenDota', 'object':'leagues'}, # To parameterize object by calling config
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
        import csv
        import io
        from jinja2 import Template
        from typing import List, Dict

        def flatten_json_to_csv(json_data) -> List[Dict]:
            def complex_field_to_json_string(data:Dict) -> Dict:
                keys_to_json = [key for key, value in data.items() if isinstance(value, dict) or isinstance(value, list)]
                for key in keys_to_json:
                    data[key] = json.dumps(data[key])
                return data
            
            if isinstance(json_data, list):
                return [complex_field_to_json_string(data) for data in json_data]
            elif isinstance(json_data, dict):
                return  [complex_field_to_json_string(json_data)]
            else:
                raise TypeError(f"Input json data is not dict or list. The input data has type : {type(json_data)}")
        
        def get_headers(data:List) -> List:
            headers = set([key for doc in data for key in doc.keys()])
            return list(headers)

        adls_client = AzureDataLakeStorageV2Hook(adls_conn_id='adls_id')
        file_system_client = adls_client.get_file_system(file_system=container)

        output = []
        for filename in filenames:
            file_client = file_system_client.get_file_client(filename)
            blob_data = file_client.download_file()
            json_data = blob_data.readall().decode('utf-8')
            data = json.loads(json_data)

            
            flatten_dict = flatten_json_to_csv(data)
            output += flatten_dict

        headers = get_headers(output)
        logging.info(f"Number of rows for input data: {len(output)}")
        logging.info(f"Number of headers to insert: {len(headers)}")

        output_buffer = io.StringIO()
        writer = csv.DictWriter(output_buffer, headers, delimiter='\t', extrasaction='ignore', quoting=csv.QUOTE_NONE, escapechar='\\')
        writer.writerows(rowdicts=output)

        logging.info(f"Size of data: {len(output_buffer.getvalue())}")

        pg_hook = PostgresHook.get_hook(POSTGRES_CONN_ID)    
        conn = pg_hook.get_conn()
        cur = conn.cursor()

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
        table = 'match_league_stag'
        temp_table_name = 'match_league_temp'
        id_column = 'match_id'

        sql_get_columns = f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = '{schema}'
        AND table_name = '{table}'
        AND is_identity = 'NO'
        """
        cur.execute(sql_get_columns)
        columns = cur.fetchall()
        columns = [col for col in columns if col[0] not in ('created', 'modified')]
        column_names = [col[0] for col in columns]

        if not set(headers).issubset(set(column_names)):
            raise ValueError(f"Unexpected columns from inputs files. Undefined columns : {set(column_names).difference(set(headers)) }")

        sql_create_temp_table = Template(sql_create_temp_table_template).\
                                 render(temp_table=temp_table_name, columns=columns)
        
        sql_upsert_target =  Template(sql_upsert_target_template).\
                                render(columns=columns, table=table, schema=schema, id_column=id_column, temp_table=temp_table_name)

        logging.info(sql_create_temp_table)
        logging.info(sql_upsert_target)
        try:
            logging.info(f"Creating temp table: {temp_table_name}")
            cur.execute(sql_create_temp_table)
            
            logging.info(f"Copy data into temp table: {temp_table_name}")
            output_buffer.seek(0)
            cur.copy_from(output_buffer, temp_table_name, columns=headers, null="")
            
            logging.info(f"Upserting data into table: {schema}.{table}")
            cur.execute(sql_upsert_target)


        except Exception as error:
            logging.info('Data upsert failed. Executing rollback')
            cur.execute("ROLLBACK;")
            raise error
        else:
            logging.info('Insert is successful')
            conn.commit()
        finally:
            cur.close()
            conn.close()

    update_metadata = PostgresOperator(
        task_id='update_metadata',
        sql='sql/update_metadata.sql',
        postgres_conn_id=POSTGRES_CONN_ID,
        params={'source':'OpenDota', 'object':'match', 'id_column_name':'match_id', 'source_silver_table':'dota.match_league_stag'}
    )

    entity = 'league-matches'
    container ='bronze'

    input_entity = get_active_input_entity.output
    latest_jsons = get_latest_file(container, entity, input_entity)
    upsert = upsert_data_from_json(container=container, entity=entity, filenames=latest_jsons)
    upsert >> update_metadata

transform_match_league_json_to_table_stag()