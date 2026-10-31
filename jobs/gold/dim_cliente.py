from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, current_timestamp
from pyspark.sql.window import Window

BRONZE = "s3a://techflow-silver/clientes/"
DESTINO = "s3a://techflow-gold/dim_cliente/"

def main():
    spark = (
        SparkSession.builder
        .appName("gold-dim-cliente")
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
        clientes = spark.read.parquet(BRONZE)

        if clientes.filter(col("id").isNull()).limit(1).count() > 0:
            raise ValueError("Existem clientes sem identificador.")

        duplicados = (
            clientes.groupBy("id")
            .count()
            .filter(col("count") > 1)
            .count()
        )

        if duplicados > 0:
            raise ValueError(f"Foram encontrados {duplicados} IDs duplicados.")

        janela = Window.orderBy("cliente_id")

        dim_cliente = (
            clientes
            .select(
                col("id").alias("cliente_id"),
                "nome",
                "email",
                "data_nascimento",
                "cidade",
                "uf",
                "criado_em"
            )
            .withColumn("cliente_sk", row_number().over(janela))
            .withColumn("data_carga", current_timestamp())
            .select(
                "cliente_sk",
                "cliente_id",
                "nome",
                "email",
                "data_nascimento",
                "cidade",
                "uf",
                "criado_em",
                "data_carga"
            )
        )

        total = dim_cliente.count()

        dim_cliente.write.mode("overwrite").parquet(DESTINO)

        print(f"Clientes gravados na dimensão: {total}")
        print(f"Destino: {DESTINO}")
        dim_cliente.orderBy("cliente_sk").show(5, truncate=False)

    finally:
        spark.stop()

if __name__ == "__main__":
    main()

