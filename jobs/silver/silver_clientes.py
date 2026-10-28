
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, current_timestamp

BRONZE = "s3a://techflow-bronze/mysql/techflow_oltp/clientes/*.jsonl"
SILVER_VALIDOS = "s3a://techflow-silver/clientes/"
SILVER_REJEITADOS = "s3a://techflow-silver/auditoria/clientes/"


def main():
    spark = (
        SparkSession.builder
        .appName("silver-clientes")
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
        origem = spark.read.json(BRONZE)

        df = origem.select(
            col("_airbyte_raw_id"),
            col("_airbyte_data.id").cast("long").alias("id"),
            col("_airbyte_data.nome").cast("string").alias("nome"),
            col("_airbyte_data.email").cast("string").alias("email"),
            col("_airbyte_data.cpf").cast("string").alias("cpf"),
            col("_airbyte_data.data_nascimento").cast("date").alias("data_nascimento"),
            col("_airbyte_data.cidade").cast("string").alias("cidade"),
            col("_airbyte_data.uf").cast("string").alias("uf"),
            col("_airbyte_data.criado_em").cast("timestamp").alias("criado_em"),
            col("_airbyte_data.atualizado_em").cast("timestamp").alias("atualizado_em"),
        )

        df = df.withColumn(
            "motivo_rejeicao",
            when(col("id").isNull(), lit("id_nulo"))
            .when(
                col("nome").isNull() | (col("nome") == ""),
                lit("nome_invalido")
            )
            .otherwise(lit(None).cast("string"))
        ).withColumn("processado_em", current_timestamp())

        validos = df.filter(
            col("motivo_rejeicao").isNull()
        ).drop("motivo_rejeicao")

        rejeitados = df.filter(
            col("motivo_rejeicao").isNotNull()
        )

        print(f"Total de clientes lidos: {df.count()}")
        print(f"Clientes válidos: {validos.count()}")
        print(f"Clientes rejeitados: {rejeitados.count()}")

        print("Rejeitados por motivo:")
        rejeitados.groupBy("motivo_rejeicao").count().orderBy(
            "motivo_rejeicao"
        ).show(truncate=False)

        validos.write.mode("overwrite").parquet(SILVER_VALIDOS)
        rejeitados.write.mode("overwrite").parquet(SILVER_REJEITADOS)

        print(f"Clientes válidos gravados em: {SILVER_VALIDOS}")
        print(f"Auditoria gravada em: {SILVER_REJEITADOS}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
