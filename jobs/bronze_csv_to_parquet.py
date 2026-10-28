
from pyspark.sql import SparkSession

BRONZE_BUCKET = "techflow-bronze"

def main():
    spark = (
        SparkSession.builder
        .appName("bronze-csv-to-parquet")
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

    arquivos = [
        "custos_produtos",
        "investimento_midia",
    ]

    try:
        for nome in arquivos:
            origem = f"s3a://{BRONZE_BUCKET}/csv/{nome}.csv"
            destino = f"s3a://{BRONZE_BUCKET}/parquet/{nome}/"

            print(f"Lendo: {origem}")

            leitura = (
                spark.read
                .option("header", "true")
                .option("inferSchema", "true")
            )

            if nome == "custos_produtos":
                leitura = leitura.option("sep", ";")

            df = leitura.csv(origem)

            print(f"Registros lidos: {df.count()}")
            df.printSchema()

            df.write.mode("overwrite").parquet(destino)

            print(f"Parquet gravado em: {destino}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
