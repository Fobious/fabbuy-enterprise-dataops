import dlt
from pyspark.sql.functions import col, current_timestamp, when, lit, expr


# =========================================================
# 1. Bronze Logs - Leitura da tabela Bronze criada no TP2
# =========================================================
@dlt.table(
    name="bronze_logs_navegacao",
    comment="Leitura da tabela Bronze de logs de navegação criada no TP2."
)
def bronze_logs_navegacao():
    return (
        spark.read.table("fabbuy_catalog.bronze.logs_navegacao")
        .select(
            "user_id",
            "session_id",
            "device",
            "page_visited",
            "duration_sec",
            "ingestion_timestamp"
        )
    )


# =========================================================
# 2. Silver Logs - Limpeza, enriquecimento e Expectations
# =========================================================
@dlt.table(
    name="silver_logs_navegacao",
    comment="Camada Silver dos logs de navegação, com limpeza, enriquecimento e regras de qualidade."
)
@dlt.expect_or_drop("duracao_valida", "duration_sec > 0")
@dlt.expect_or_fail("user_id_nao_nulo", "user_id IS NOT NULL")
@dlt.expect("duracao_alerta", "duration_sec < 300")
def silver_logs_navegacao():
    df = dlt.read("bronze_logs_navegacao")

    return (
        df
        .withColumn("tempo_decorrido_segundos", col("duration_sec"))
        .withColumn(
            "categoria_pagina",
            when(col("page_visited").like("product_%"), lit("Produto"))
            .when(col("page_visited") == "checkout", lit("Checkout"))
            .when(col("page_visited") == "home", lit("Home"))
            .otherwise(lit("Outros"))
        )
        .withColumn(
            "tipo_dispositivo",
            when(col("device") == "mobile", lit("Dispositivo Movel"))
            .when(col("device") == "desktop", lit("Computador"))
            .when(col("device") == "tablet", lit("Tablet"))
            .otherwise(lit("Nao Informado"))
        )
        .withColumn("silver_processed_at", current_timestamp())
    )


# =========================================================
# 3. Bronze Usuários CDC - Fonte de eventos de cadastro
# =========================================================
@dlt.table(
    name="bronze_usuarios_cdc",
    comment="Eventos de CDC de cadastro de usuários da FabBuy."
)
def bronze_usuarios_cdc():
    return (
        spark.readStream.table("fabbuy_catalog.bronze.usuarios_cdc_eventos")
        .select(
            "user_id",
            "nome_usuario",
            "segmento_cliente",
            "estado",
            "email_mascarado",
            "updated_at",
            "operacao"
        )
    )


# =========================================================
# 4. Silver Usuários Atualizada - CDC / Apply Changes
# =========================================================
dlt.create_streaming_table(
    name="silver_usuarios_atualizada",
    comment="Tabela Silver atualizada de usuários, mantida via CDC com APPLY CHANGES."
)

dlt.apply_changes(
    target="silver_usuarios_atualizada",
    source="bronze_usuarios_cdc",
    keys=["user_id"],
    sequence_by=col("updated_at"),
    apply_as_deletes=expr("operacao = 'DELETE'"),
    except_column_list=["operacao"],
    stored_as_scd_type=1
)
# =========================================================
# 5. Silver Logs Enriquecidos - Join com cadastro atualizado
# =========================================================
@dlt.table(
    name="silver_logs_enriquecidos",
    comment="Camada Silver enriquecida, unindo logs de navegação com cadastro atualizado de usuários via CDC."
)
def silver_logs_enriquecidos():
    logs = dlt.read("silver_logs_navegacao")
    usuarios = dlt.read("silver_usuarios_atualizada")

    return (
        logs.alias("l")
        .join(
            usuarios.alias("u"),
            col("l.user_id") == col("u.user_id"),
            "left"
        )
        .select(
            col("l.user_id"),
            col("l.session_id"),
            col("l.device"),
            col("l.tipo_dispositivo"),
            col("l.page_visited"),
            col("l.categoria_pagina"),
            col("l.duration_sec"),
            col("l.tempo_decorrido_segundos"),
            col("u.nome_usuario"),
            col("u.segmento_cliente"),
            col("u.estado"),
            col("u.email_mascarado"),
            col("l.ingestion_timestamp"),
            col("l.silver_processed_at"),
            current_timestamp().alias("enriched_processed_at")
        )
    )