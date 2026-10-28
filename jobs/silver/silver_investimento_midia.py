
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, current_timestamp, trim, to_date
)

BRONZE = "s3a://techflow-bronze/parquet/investimento_midia/"
SILVER_VALIDOS = "s3a://techflow-silver/investimento_midia/"
SILVER_REJEITADOS = "s3a://techflow-silver/auditoria/investimento_midia/"


def main():
    spark = (
        SparkSession.builder
        .appName("silver-investimento-midia")
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
            to_date(col("data")).alias("data"),
            trim(col("canal")).alias("canal"),
            trim(col("campanha")).alias("campanha"),
            col("investimento").cast("decimal(18,2)").alias("investimento"),
            col("impressoes").cast("long").alias("impressoes"),
            col("cliques").cast("long").alias("cliques"),
        )

        df = df.withColumn(
            "motivo_rejeicao",
            when(col("data").isNull(), lit("data_invalida"))
            .when(
                col("canal").isNull() | (col("canal") == ""),
                lit("canal_invalido")
            )
            .when(
                col("campanha").isNull() | (col("campanha") == ""),
                lit("campanha_invalida")
            )
            .when(
                col("investimento").isNull() | (col("investimento") < 0),
                lit("investimento_invalido")
            )
            .when(
                col("impressoes").isNull() | (col("impressoes") < 0),
                lit("impressoes_invalidas")
            )
            .when(
                col("cliques").isNull()
                | (col("cliques") < 0)
                | (col("cliques") > col("impressoes")),
                lit("cliques_invalidos")
            )
            .otherwise(lit(None).cast("string"))
        ).withColumn("processado_em", current_timestamp())

        validos = df.filter(
            col("motivo_rejeicao").isNull()
        ).drop("motivo_rejeicao")

        rejeitados = df.filter(
            col("motivo_rejeicao").isNotNull()
        )

        print(f"Total de registros lidos: {df.count()}")
        print(f"Registros válidos: {validos.count()}")
        print(f"Registros rejeitados: {rejeitados.count()}")

        print("Rejeitados por motivo:")
        rejeitados.groupBy("motivo_rejeicao").count().orderBy(
            "motivo_rejeicao"
        ).show(truncate=False)

        validos.write.mode("overwrite").parquet(SILVER_VALIDOS)
        rejeitados.write.mode("overwrite").parquet(SILVER_REJEITADOS)

        print(f"Investimentos válidos gravados em: {SILVER_VALIDOS}")
        print(f"Auditoria gravada em: {SILVER_REJEITADOS}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
