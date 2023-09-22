INSERT INTO etl.metadata (source, object, id_column_name, id_column_value, modified)
SELECT DISTINCT
	'{{ params.source }}' as source,
	'{{ params.object }}' as object,
	'{{ params.id_column_name }}' as id_column_name,
	{{ params.id_column_name }} as id_column_value,
	CURRENT_TIMESTAMP as modified
FROM {{ params.source_silver_table }}
ON CONFLICT (source, object, id_column_name, id_column_value) DO UPDATE
SET
	modified = EXCLUDED.modified