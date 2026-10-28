
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, current_timestamp

BRONZE = "s3a://techflow-bronze/mysql/techflow_oltp/pedidos/*.jsonl"
SILVER_VALIDOS = "s3a://techflow-silver/pedidos/"
SILVER_REJEITADOS = "s3a://techflow-silver/auditoria/pedidos/"


def main():
    spark = (
        SparkSession.builder
        .appName("silver-pedidos")
        .config("spark.hadoop.fs.s3a.endpoint", "http://medalhao-localstack:4566")
        .config("spark.hadoop.fs.s3a.access.key", "test")
        .config("spark.hadoop.fs.s3a.secret.key", "test")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        origem = spark.read.json(BRONZE)

        df = origem.select(
            col("_airbyte_raw_id"),
            col("_airbyte_data.id").cast("long").alias("id"),
            col("_airbyte_data.cliente_id").cast("long").alias("cliente_id"),
            col("_airbyte_data.status").cast("string").alias("status"),
            col("_airbyte_data.canal").cast("string").alias("canal"),
            col("_airbyte_data.valor_frete").cast("decimal(18,2)").alias("valor_frete"),
            col("_airbyte_data.cupom").cast("string").alias("cupom"),
            col("_airbyte_data.criado_em").cast("timestamp").alias("criado_em"),
            col("_airbyte_data.atualizado_em").cast("timestamp").alias("atualizado_em"),
        )

        df = df.withColumn(
            "motivo_rejeicao",
            when(col("id").isNull(), lit("id_nulo"))
            .when(col("cliente_id").isNull(), lit("cliente_id_nulo"))
            .when(col("status").isNull() | (col("status") == ""), lit("status_invalido"))
            .when(col("criado_em").isNull(), lit("data_criacao_invalida"))
            .when(col("valor_frete").isNull() | (col("valor_frete") < 0), lit("frete_invalido"))
            .otherwise(lit(None).cast("string"))
        ).withColumn("processado_em", current_timestamp())

        validos = df.filter(col("motivo_rejeicao").isNull()).drop("motivo_rejeicao")
        rejeitados = df.filter(col("motivo_rejeicao").isNotNull())

        print(f"Total de pedidos lidos: {df.count()}")
        print(f"Pedidos válidos: {validos.count()}")
        print(f"Pedidos rejeitados: {rejeitados.count()}")

        print("Rejeitados por motivo:")
        rejeitados.groupBy("motivo_rejeicao").count().orderBy("motivo_rejeicao").show(truncate=False)

        validos.write.mode("overwrite").parquet(SILVER_VALIDOS)
        rejeitados.write.mode("overwrite").parquet(SILVER_REJEITADOS)

        print(f"Pedidos válidos gravados em: {SILVER_VALIDOS}")
        print(f"Auditoria gravada em: {SILVER_REJEITADOS}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
