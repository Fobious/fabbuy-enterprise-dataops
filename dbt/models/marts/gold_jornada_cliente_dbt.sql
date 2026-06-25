{{ config(
    materialized='table',
    dist='user_id',
    sort='user_id'
) }}

SELECT
    user_id,
    nome_usuario,
    segmento_cliente,
    estado,
    tipo_dispositivo,
    categoria_pagina,
    COUNT(*) AS qtd_eventos_navegacao,
    SUM(duration_sec) AS tempo_total_navegacao_segundos,
    AVG(duration_sec) AS tempo_medio_navegacao_segundos,
    MAX(enriched_processed_at) AS ultima_atualizacao_gold_dbt
FROM {{ ref('stg_silver_logs_enriquecidos') }}
GROUP BY
    user_id,
    nome_usuario,
    segmento_cliente,
    estado,
    tipo_dispositivo,
    categoria_pagina