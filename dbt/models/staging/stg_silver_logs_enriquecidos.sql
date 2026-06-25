{{ config(materialized='view') }}

SELECT
    CAST(user_id AS VARCHAR(100)) AS user_id,
    CAST(session_id AS VARCHAR(100)) AS session_id,
    CAST(device AS VARCHAR(50)) AS device,
    CAST(tipo_dispositivo AS VARCHAR(100)) AS tipo_dispositivo,
    CAST(page_visited AS VARCHAR(100)) AS page_visited,
    CAST(categoria_pagina AS VARCHAR(100)) AS categoria_pagina,
    CAST(duration_sec AS INTEGER) AS duration_sec,
    CAST(tempo_decorrido_segundos AS INTEGER) AS tempo_decorrido_segundos,
    CAST(nome_usuario AS VARCHAR(200)) AS nome_usuario,
    CAST(segmento_cliente AS VARCHAR(100)) AS segmento_cliente,
    CAST(estado AS VARCHAR(100)) AS estado,
    CAST(email_mascarado AS VARCHAR(200)) AS email_mascarado,
    CAST(ingestion_timestamp AS TIMESTAMP) AS ingestion_timestamp,
    CAST(silver_processed_at AS TIMESTAMP) AS silver_processed_at,
    CAST(enriched_processed_at AS TIMESTAMP) AS enriched_processed_at
FROM {{ source('raw_databricks', 'silver_logs_enriquecidos') }}