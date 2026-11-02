
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, min as spark_min,
    max as spark_max, sum as spark_sum,
    when, row_number
)
from pyspark.sql.window import Window

SILVER_CLICKSTREAM = "s3a://techflow-silver/clickstream/"
SILVER_PEDIDOS = "s3a://techflow-silver/pedidos/"

ETAPAS = [
    "product_view",
    "add_to_cart",
    "checkout_start",
    "purchase"
]


def main():
    spark = (
        SparkSession.builder
        .appName("diagnostico-q2")
        .config("spark.hadoop.fs.s3a.endpoint", "http://medalhao-localstack:4566")
        .config("spark.hadoop.fs.s3a.access.key", "test")
        .config("spark.hadoop.fs.s3a.secret.key", "test")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem"
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        eventos = spark.read.parquet(SILVER_CLICKSTREAM)
        pedidos = spark.read.parquet(SILVER_PEDIDOS)

        print("\n=== 1. EVENTOS POR TIPO ===")

        eventos.groupBy("event_type").agg(
            count("*").alias("total_eventos"),
            countDistinct("session_id").alias("sessoes")
        ).orderBy("event_type").show(100, truncate=False)

        print("\n=== 2. SESSÕES POR DISPOSITIVO ===")

        eventos.groupBy("device").agg(
            countDistinct("session_id").alias("sessoes"),
            count("*").alias("eventos"),
            countDistinct("user_id").alias("usuarios_identificados")
        ).orderBy("device").show(truncate=False)

        print("\n=== 3. FUNIL CUMULATIVO POR DISPOSITIVO ===")

        etapas = eventos.filter(
            col("event_type").isin(ETAPAS)
        ).groupBy("device", "session_id").agg(
            spark_min(
                when(col("event_type") == "product_view",
                     col("event_timestamp"))
            ).alias("ts_view"),
            spark_min(
                when(col("event_type") == "add_to_cart",
                     col("event_timestamp"))
            ).alias("ts_cart"),
            spark_min(
                when(col("event_type") == "checkout_start",
                     col("event_timestamp"))
            ).alias("ts_checkout"),
            spark_min(
                when(col("event_type") == "purchase",
                     col("event_timestamp"))
            ).alias("ts_purchase")
        )

        funil = etapas.groupBy("device").agg(
            countDistinct(
                when(col("ts_view").isNotNull(), col("session_id"))
            ).alias("product_view"),
            countDistinct(
                when(col("ts_cart").isNotNull(), col("session_id"))
            ).alias("add_to_cart"),
            countDistinct(
                when(col("ts_checkout").isNotNull(), col("session_id"))
            ).alias("checkout_start"),
            countDistinct(
                when(col("ts_purchase").isNotNull(), col("session_id"))
            ).alias("purchase")
        ).orderBy("device")

        funil.show(truncate=False)

        print("\n=== 4. ORDEM CRONOLÓGICA DAS ETAPAS ===")

        sequencia = etapas.filter(
            col("ts_view").isNotNull() &
            col("ts_cart").isNotNull() &
            col("ts_checkout").isNotNull() &
            col("ts_purchase").isNotNull()
        ).withColumn(
            "sequencia_valida",
            (col("ts_view") <= col("ts_cart")) &
            (col("ts_cart") <= col("ts_checkout")) &
            (col("ts_checkout") <= col("ts_purchase"))
        )

        sequencia.groupBy("device", "sequencia_valida").agg(
            countDistinct("session_id").alias("sessoes")
        ).orderBy("device", "sequencia_valida").show(truncate=False)

        print("\n=== 5. EVENTOS DE COMPRA E IDENTIFICAÇÃO ===")

        compras = eventos.filter(col("event_type") == "purchase")

        compras.agg(
            count("*").alias("eventos_purchase"),
            countDistinct("session_id").alias("sessoes_purchase"),
            countDistinct("user_id").alias("usuarios_identificados"),
            spark_sum(
                when(col("user_id").isNull(), 1).otherwise(0)
            ).alias("eventos_sem_user_id")
        ).show(truncate=False)

        print("\n=== 6. PEDIDOS POR STATUS E CLIENTE ===")

        pedidos.groupBy("status").agg(
            countDistinct("id").alias("pedidos"),
            countDistinct("cliente_id").alias("clientes")
        ).orderBy("status").show(truncate=False)

        print("\n=== 7. COMPATIBILIDADE DE IDENTIFICADORES ===")

        print("Usuários distintos no clickstream:",
              eventos.select("user_id").distinct().count())

        print("Clientes distintos nos pedidos:",
              pedidos.select("cliente_id").distinct().count())

        print("Eventos com user_id preenchido:",
              eventos.filter(col("user_id").isNotNull()).count())

        print("Pedidos com cliente_id preenchido:",
              pedidos.filter(col("cliente_id").isNotNull()).count())

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
