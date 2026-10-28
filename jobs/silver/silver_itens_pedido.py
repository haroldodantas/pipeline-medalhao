
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, current_timestamp

BRONZE = "s3a://techflow-bronze/mysql/techflow_oltp/itens_pedido/*.jsonl"
SILVER_VALIDOS = "s3a://techflow-silver/itens_pedido/"
SILVER_REJEITADOS = "s3a://techflow-silver/auditoria/itens_pedido/"

def main():
    spark = (
        SparkSession.builder
        .appName("silver-itens-pedido")
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

        # Extrai os dados de negócio do envelope do Airbyte.
        df = origem.select(
            col("_airbyte_raw_id"),
            col("_airbyte_data.id").cast("long").alias("id"),
            col("_airbyte_data.pedido_id").cast("long").alias("pedido_id"),
            col("_airbyte_data.sku").cast("string").alias("sku"),
            col("_airbyte_data.quantidade").cast("integer").alias("quantidade"),
            col("_airbyte_data.preco_unitario").cast("decimal(18,2)").alias("preco_unitario"),
            col("_airbyte_data.desconto").cast("decimal(18,2)").alias("desconto"),
        )

        # Registra o motivo de rejeição, sem descartar silenciosamente.
        df = df.withColumn(
            "motivo_rejeicao",
            when(col("id").isNull(), lit("id_nulo"))
            .when(col("pedido_id").isNull(), lit("pedido_id_nulo"))
            .when(col("sku").isNull(), lit("sku_nulo"))
            .when(col("quantidade").isNull() | (col("quantidade") <= 0), lit("quantidade_invalida"))
            .when(col("preco_unitario").isNull(), lit("preco_nulo"))
            .when(col("preco_unitario") < 0, lit("preco_negativo"))
            .when(col("desconto").isNull() | (col("desconto") < 0), lit("desconto_invalido"))
            .otherwise(lit(None).cast("string"))
        )

        df = df.withColumn("processado_em", current_timestamp())

        validos = df.filter(col("motivo_rejeicao").isNull()).drop("motivo_rejeicao")
        rejeitados = df.filter(col("motivo_rejeicao").isNotNull())

        print(f"Total de itens lidos: {df.count()}")
        print(f"Itens válidos: {validos.count()}")
        print(f"Itens rejeitados: {rejeitados.count()}")

        print("Rejeitados por motivo:")
        rejeitados.groupBy("motivo_rejeicao").count().orderBy("motivo_rejeicao").show(truncate=False)

        validos.write.mode("overwrite").parquet(SILVER_VALIDOS)
        rejeitados.write.mode("overwrite").parquet(SILVER_REJEITADOS)

        print(f"Dados válidos gravados em: {SILVER_VALIDOS}")
        print(f"Auditoria gravada em: {SILVER_REJEITADOS}")

    finally:
        spark.stop()

if __name__ == "__main__":
    main()
