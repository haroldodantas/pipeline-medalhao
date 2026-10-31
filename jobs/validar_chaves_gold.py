from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():
    spark = (
        SparkSession.builder
        .appName("validar-chaves-gold")
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
        pedidos = spark.read.parquet("s3a://techflow-silver/pedidos/")
        itens = spark.read.parquet("s3a://techflow-silver/itens_pedido/")
        produtos = spark.read.parquet("s3a://techflow-silver/produtos/")
        clientes = spark.read.parquet("s3a://techflow-silver/clientes/")

        print("=== UNICIDADE DAS CHAVES ===")
        print(f"Pedidos duplicados por id: {pedidos.groupBy('id').count().filter(col('count') > 1).count()}")
        print(f"SKUs duplicados em produtos: {produtos.groupBy('sku').count().filter(col('count') > 1).count()}")

        print("\n=== INTEGRIDADE REFERENCIAL ===")
        itens_sem_pedido = itens.join(
            pedidos.select("id"),
            itens.pedido_id == pedidos.id,
            "left_anti"
        ).count()

        itens_sem_produto = itens.join(
            produtos.select("sku"),
            "sku",
            "left_anti"
        ).count()

        pedidos_sem_cliente = pedidos.join(
            clientes.select("id"),
            pedidos.cliente_id == clientes.id,
            "left_anti"
        ).count()

        print(f"Itens sem pedido correspondente: {itens_sem_pedido}")
        print(f"Itens sem produto correspondente: {itens_sem_produto}")
        print(f"Pedidos sem cliente correspondente: {pedidos_sem_cliente}")

    finally:
        spark.stop()

if __name__ == "__main__":
    main()
