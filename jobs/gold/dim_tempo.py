from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, explode, sequence, to_date, year, month, dayofmonth,
    quarter, date_format, dayofweek, dayofyear, lit, current_timestamp
)

DESTINO = "s3a://techflow-gold/dim_tempo/"

def main():
    spark = (
        SparkSession.builder
        .appName("gold-dim-tempo")
        .config("spark.hadoop.fs.s3a.endpoint", "http://medalhao-localstack:4566")
        .config("spark.hadoop.fs.s3a.access.key", "test")
        .config("spark.hadoop.fs.s3a.secret.key", "test")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        datas = spark.sql("""
            SELECT explode(
                sequence(
                    to_date('2026-07-14'),
                    to_date('2026-10-12'),
                    interval 1 day
                )
            ) AS data
        """)

        dim_tempo = (
            datas
            .withColumn("data_sk", date_format("data", "yyyyMMdd").cast("int"))
            .withColumn("ano", year("data"))
            .withColumn("trimestre", quarter("data"))
            .withColumn("mes", month("data"))
            .withColumn("nome_mes", date_format("data", "MMMM"))
            .withColumn("dia", dayofmonth("data"))
            .withColumn("dia_semana", dayofweek("data"))
            .withColumn("dia_ano", dayofyear("data"))
            .withColumn("fim_de_semana", col("dia_semana").isin(1, 7))
            .withColumn("inicio_mes", col("dia") == 1)
            .withColumn(
                "fim_mes",
                date_format("data", "yyyyMMdd") ==
                date_format(
                    __import__("pyspark.sql.functions", fromlist=["last_day"]).last_day("data"),
                    "yyyyMMdd"
                )
            )
            .withColumn("data_carga", current_timestamp())
            .select(
                "data_sk", "data", "ano", "trimestre", "mes",
                "nome_mes", "dia", "dia_semana", "dia_ano",
                "fim_de_semana", "inicio_mes", "fim_mes", "data_carga"
            )
        )

        total = dim_tempo.count()
        dim_tempo.write.mode("overwrite").parquet(DESTINO)

        print(f"Dias gravados na dimensão: {total}")
        print(f"Destino: {DESTINO}")
        dim_tempo.orderBy("data").show(5, truncate=False)
        dim_tempo.orderBy(col("data").desc()).show(5, truncate=False)

    finally:
        spark.stop()

if __name__ == "__main__":
    main()
