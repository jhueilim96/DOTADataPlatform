SELECT id_column_value 
FROM etl.metadata 
WHERE is_active=true
AND source = '{{ params.source }}' AND object = '{{ params.object }}'