SELECT id_column_name, id_column_value FROM etl.metadata 
WHERE object = '{{ params.object }}' AND
is_active = true