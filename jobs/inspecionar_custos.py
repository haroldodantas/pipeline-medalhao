
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, to_date, regexp_replace
)

spark = (
    SparkSession.builder
    .appName("inspecionar-custo-rejeitado")
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

spark.sparkContext.setLogLevel("ERROR")

try:
    df = spark.read.parquet(
        "s3a://techflow-bronze/parquet/custos_produtos/"
    )

    df = df.withColumn(
        "inicio_convertido",
        to_date(trim(col("vigencia_inicio")), "dd/MM/yyyy")
    )

    print("Registro com data de início inválida:")
    df.filter(col("inicio_convertido").isNull()).select(
        "sku",
        "custo_unitario",
        "vigencia_inicio",
        "vigencia_fim"
    ).show(truncate=False)

finally:
    spark.stop()
