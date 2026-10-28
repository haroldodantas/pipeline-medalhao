
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

BRONZE_BUCKET = "techflow-bronze"

def main():
    spark = (
        SparkSession.builder
        .appName("bronze-clickstream-to-parquet")
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

    origem = f"s3a://{BRONZE_BUCKET}/clickstream/*.jsonl"
    destino = f"s3a://{BRONZE_BUCKET}/parquet/clickstream/"

    try:
        print(f"Lendo arquivos: {origem}")

        df = spark.read.json(origem)

        # Converte o timestamp ISO 8601, preservando a informação de fuso
        df = df.withColumn(
            "event_timestamp",
            to_timestamp(col("event_timestamp")),
        )

        print(f"Total de eventos: {df.count()}")
        df.printSchema()

        df.write.mode("overwrite").parquet(destino)

        print(f"Parquet gravado em: {destino}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
