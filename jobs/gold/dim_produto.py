from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, current_timestamp
from pyspark.sql.window import Window

ORIGEM = "s3a://techflow-silver/produtos/"
DESTINO = "s3a://techflow-gold/dim_produto/"

def main():
    spark = (
        SparkSession.builder
        .appName("gold-dim-produto")
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
        produtos = spark.read.parquet(ORIGEM)

        if produtos.filter(col("sku").isNull()).limit(1).count() > 0:
            raise ValueError("Existem produtos sem SKU.")

        duplicados = (
            produtos.groupBy("sku")
            .count()
            .filter(col("count") > 1)
            .count()
        )

        if duplicados > 0:
            raise ValueError(f"Foram encontrados {duplicados} SKUs duplicados.")

        janela = Window.orderBy("sku")

        dim_produto = (
            produtos
            .select(
                "sku",
                "nome",
                "categoria",
                "marca",
                "ativo",
                "criado_em"
            )
            .withColumn("produto_sk", row_number().over(janela))
            .withColumn("data_carga", current_timestamp())
            .select(
                "produto_sk",
                "sku",
                "nome",
                "categoria",
                "marca",
                "ativo",
                "criado_em",
                "data_carga"
            )
        )

        total = dim_produto.count()
        dim_produto.write.mode("overwrite").parquet(DESTINO)

        print(f"Produtos gravados na dimensão: {total}")
        print(f"Destino: {DESTINO}")
        dim_produto.orderBy("produto_sk").show(5, truncate=False)

    finally:
        spark.stop()

if __name__ == "__main__":
    main()
