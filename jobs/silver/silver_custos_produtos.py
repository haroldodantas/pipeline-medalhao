
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, current_timestamp,
    regexp_replace, to_date, trim
)

BRONZE = "s3a://techflow-bronze/parquet/custos_produtos/"
SILVER_VALIDOS = "s3a://techflow-silver/custos_produtos/"
SILVER_REJEITADOS = "s3a://techflow-silver/auditoria/custos_produtos/"


def main():
    spark = (
        SparkSession.builder
        .appName("silver-custos-produtos")
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
            trim(col("sku")).alias("sku"),
            regexp_replace(
                trim(col("custo_unitario")), ",", "."
            ).cast("decimal(18,2)").alias("custo_unitario"),
            to_date(
                trim(col("vigencia_inicio")), "dd/MM/yyyy"
            ).alias("vigencia_inicio"),
            to_date(
                trim(col("vigencia_fim")), "dd/MM/yyyy"
            ).alias("vigencia_fim"),
        )

        df = df.withColumn(
            "motivo_rejeicao",
            when(
                col("sku") == "TOTAL",
                lit("linha_totalizacao")
            )
            .when(
                col("sku").isNull() | (col("sku") == ""),
                lit("sku_invalido")
            )
            .when(
                col("custo_unitario").isNull(),
                lit("custo_nulo_ou_invalido")
            )
            .when(
                col("custo_unitario") < 0,
                lit("custo_negativo")
            )
            .when(
                col("vigencia_inicio").isNull(),
                lit("vigencia_inicio_invalida")
            )
            .when(
                col("vigencia_fim").isNotNull()
                & (col("vigencia_fim") < col("vigencia_inicio")),
                lit("periodo_vigencia_invalido")
            )
            .otherwise(lit(None).cast("string"))
        ).withColumn("processado_em", current_timestamp())

        validos = df.filter(
            col("motivo_rejeicao").isNull()
        ).drop("motivo_rejeicao")

        rejeitados = df.filter(
            col("motivo_rejeicao").isNotNull()
        )

        print(f"Total de custos lidos: {df.count()}")
        print(f"Custos válidos: {validos.count()}")
        print(f"Custos rejeitados: {rejeitados.count()}")

        print("Rejeitados por motivo:")
        rejeitados.groupBy("motivo_rejeicao").count().orderBy(
            "motivo_rejeicao"
        ).show(truncate=False)

        print("Amostra dos custos válidos:")
        validos.select(
            "sku", "custo_unitario",
            "vigencia_inicio", "vigencia_fim"
        ).show(5, truncate=False)

        validos.write.mode("overwrite").parquet(SILVER_VALIDOS)
        rejeitados.write.mode("overwrite").parquet(SILVER_REJEITADOS)

        print(f"Custos válidos gravados em: {SILVER_VALIDOS}")
        print(f"Auditoria gravada em: {SILVER_REJEITADOS}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
