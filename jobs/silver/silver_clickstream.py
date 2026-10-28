
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, current_timestamp, to_timestamp
)

BRONZE = "s3a://techflow-bronze/parquet/clickstream/"
SILVER_VALIDOS = "s3a://techflow-silver/clickstream/"
SILVER_REJEITADOS = "s3a://techflow-silver/auditoria/clickstream/"


def main():
    spark = (
        SparkSession.builder
        .appName("silver-clickstream")
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
        origem = spark.read.parquet(BRONZE)

        df = origem.select(
            col("event_id").cast("string").alias("event_id"),
            col("event_type").cast("string").alias("event_type"),
            col("event_timestamp").cast("timestamp").alias("event_timestamp"),
            col("user.user_id").cast("string").alias("user_id"),
            col("user.anonymous_id").cast("string").alias("anonymous_id"),
            col("user.session_id").cast("string").alias("session_id"),
            col("user.device").cast("string").alias("device"),
            col("context.channel").cast("string").alias("channel"),
            col("context.page.url").cast("string").alias("page_url"),
            col("context.page.referrer").cast("string").alias("page_referrer"),
            col("properties.sku").cast("string").alias("sku"),
            col("properties.quantity").cast("int").alias("quantity"),
            col("properties.unit_price").cast("decimal(18,2)").alias("unit_price"),
            col("properties.price").cast("decimal(18,2)").alias("price"),
            col("ab_test_variant").cast("string").alias("ab_test_variant"),
        )

        df = df.withColumn(
            "motivo_rejeicao",
            when(
                col("event_id").isNull() | (col("event_id") == ""),
                lit("event_id_invalido")
            )
            .when(
                col("event_type").isNull() | (col("event_type") == ""),
                lit("event_type_invalido")
            )
            .when(
                col("event_timestamp").isNull(),
                lit("timestamp_invalido")
            )
            .when(
                col("session_id").isNull() | (col("session_id") == ""),
                lit("session_id_invalido")
            )
            .otherwise(lit(None).cast("string"))
        ).withColumn("processado_em", current_timestamp())

        validos = df.filter(
            col("motivo_rejeicao").isNull()
        ).drop("motivo_rejeicao")

        rejeitados = df.filter(
            col("motivo_rejeicao").isNotNull()
        )

        print(f"Total de eventos lidos: {df.count()}")
        print(f"Eventos válidos: {validos.count()}")
        print(f"Eventos rejeitados: {rejeitados.count()}")

        print("Rejeitados por motivo:")
        rejeitados.groupBy("motivo_rejeicao").count().orderBy(
            "motivo_rejeicao"
        ).show(truncate=False)

        print("Eventos por tipo:")
        validos.groupBy("event_type").count().orderBy(
            "event_type"
        ).show(truncate=False)

        validos.write.mode("overwrite").parquet(SILVER_VALIDOS)
        rejeitados.write.mode("overwrite").parquet(SILVER_REJEITADOS)

        print(f"Eventos válidos gravados em: {SILVER_VALIDOS}")
        print(f"Auditoria gravada em: {SILVER_REJEITADOS}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
