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

    qtd_eventos_navegacao,
    tempo_total_navegacao_segundos,
    tempo_medio_navegacao_segundos,

    -- Features compatíveis com o modelo do TP4
    CAST(tempo_medio_navegacao_segundos AS INTEGER) AS duration_sec,
    CAST(tempo_medio_navegacao_segundos AS INTEGER) AS tempo_decorrido_segundos,

    ultima_atualizacao_gold_dbt AS data_referencia_features
FROM {{ ref('gold_jornada_cliente_dbt') }}
WHERE tempo_medio_navegacao_segundos IS NOT NULL