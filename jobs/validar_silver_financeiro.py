
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum

spark = (
    SparkSession.builder
    .appName("validar-silver-financeiro")
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
    custos = spark.read.parquet(
        "s3a://techflow-silver/custos_produtos/"
    )

    investimentos = spark.read.parquet(
        "s3a://techflow-silver/investimento_midia/"
    )

    print("VALIDAÇÃO DOS CUSTOS")
    custos.select(
        count("*").alias("quantidade_skus_custo"),
        sum("custo_unitario").alias("soma_custos")
    ).show(truncate=False)

    print("VALIDAÇÃO DOS INVESTIMENTOS")
    investimentos.select(
        count("*").alias("quantidade_registros"),
        sum("investimento").alias("investimento_total"),
        sum("impressoes").alias("impressoes_total"),
        sum("cliques").alias("cliques_total")
    ).show(truncate=False)

finally:
    spark.stop()
