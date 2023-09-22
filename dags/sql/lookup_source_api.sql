SELECT source_api, is_active 
FROM etl.ingestion_config 
WHERE config_id = {{ params.config_id }}