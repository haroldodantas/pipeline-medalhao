from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, count, max as spark_max,
    when, lit, round as spark_round
)

spark = (
    SparkSession.builder
    .appName("Gold - Fato Vendas")
    .config("spark.hadoop.fs.s3a.endpoint", "http://medalhao-localstack:4566")
    .config("spark.hadoop.fs.s3a.access.key", "test")
    .config("spark.hadoop.fs.s3a.secret.key", "test")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

SILVER = "s3a://techflow-silver/"
GOLD = "s3a://techflow-gold/"

print("=== INICIANDO FATO VENDAS ===")

# 1. Leitura das tabelas Silver
itens = spark.read.parquet(SILVER + "itens_pedido/")
pedidos = spark.read.parquet(SILVER + "pedidos/")
custos = spark.read.parquet(SILVER + "custos_produtos/")

dim_cliente = spark.read.parquet(GOLD + "dim_cliente/")
dim_produto = spark.read.parquet(GOLD + "dim_produto/")
dim_tempo = spark.read.parquet(GOLD + "dim_tempo/")

# 2. Selecionar somente pedidos considerados como receita realizada
base = (
    itens.alias("i")
    .join(
        pedidos.alias("p"),
        col("i.pedido_id") == col("p.id"),
        "inner"
    )
    .filter(col("p.status").isin("pago", "enviado", "entregue"))
    .select(
        col("i.id").alias("item_id"),
        col("i.pedido_id").alias("pedido_id"),
        col("i.sku").alias("sku"),
        col("i.quantidade").alias("quantidade"),
        col("i.preco_unitario").alias("preco_unitario"),
        col("i.desconto").alias("desconto_unitario"),
        col("p.cliente_id").alias("cliente_id"),
        col("p.status").alias("status_pedido"),
        col("p.canal").alias("canal_venda"),
        to_date(col("p.criado_em")).alias("data_venda")
    )
)

total_itens = base.count()
print(f"Itens de pedidos realizados: {total_itens}")

# 3. Relacionar dimensão de clientes
v = base.alias("v")
dc = dim_cliente.alias("dc")

base = (
    v.join(dc, col("v.cliente_id") == col("dc.cliente_id"), "left")
    .select("v.*", col("dc.cliente_sk").alias("cliente_sk"))
)

# 4. Relacionar dimensão de produtos
v = base.alias("v")
dp = dim_produto.alias("dp")

base = (
    v.join(dp, col("v.sku") == col("dp.sku"), "left")
    .select("v.*", col("dp.produto_sk").alias("produto_sk"))
)

# 5. Relacionar dimensão de tempo
v = base.alias("v")
dt = dim_tempo.alias("dt")

base = (
    v.join(dt, col("v.data_venda") == col("dt.data"), "left")
    .select("v.*", col("dt.data_sk").alias("data_sk"))
)

# 6. Identificar SKUs que possuem algum cadastro de custo
skus_com_custo = (
    custos.select("sku")
    .distinct()
    .withColumn("tem_cadastro_custo", lit(True))
)

# 7. Encontrar custos aplicáveis na data da venda
v = base.alias("v")
c = custos.alias("c")

custos_aplicaveis = (
    v.join(
        c,
        (col("v.sku") == col("c.sku"))
        & (col("v.data_venda") >= col("c.vigencia_inicio"))
        & (col("v.data_venda") <= col("c.vigencia_fim")),
        "inner"
    )
    .groupBy(col("v.item_id").alias("item_id"))
    .agg(
        count(col("c.sku")).alias("qtd_registros_custo"),
        spark_max(col("c.custo_unitario")).alias("custo_unitario_encontrado")
    )
)

# 8. Consolidar os custos sem duplicar os itens vendidos
base = (
    base
    .join(skus_com_custo, "sku", "left")
    .join(custos_aplicaveis, "item_id", "left")
    .fillna({"qtd_registros_custo": 0})
)

# 9. Classificar a qualidade do custo
base = base.withColumn(
    "situacao_custo",
    when(col("qtd_registros_custo") == 1, "CUSTO_VALIDO")
    .when(col("qtd_registros_custo") > 1, "CUSTO_AMBIGUO")
    .when(col("tem_cadastro_custo") == True, "FORA_VIGENCIA")
    .otherwise("SEM_CADASTRO")
)

# 10. Calcular receita, desconto, custo e margem
base = (
    base
    .withColumn(
        "receita_bruta",
        spark_round(col("quantidade") * col("preco_unitario"), 2)
    )
    .withColumn(
        "desconto_total",
        spark_round(col("quantidade") * col("desconto_unitario"), 2)
    )
    .withColumn(
        "receita_liquida",
        spark_round(col("receita_bruta") - col("desconto_total"), 2)
    )
    .withColumn(
        "custo_unitario",
        when(
            col("situacao_custo") == "CUSTO_VALIDO",
            col("custo_unitario_encontrado")
        )
    )
    .withColumn(
        "custo_total",
        when(
            col("situacao_custo") == "CUSTO_VALIDO",
            spark_round(col("quantidade") * col("custo_unitario"), 2)
        )
    )
    .withColumn(
        "margem_bruta",
        when(
            col("situacao_custo") == "CUSTO_VALIDO",
            spark_round(col("receita_liquida") - col("custo_total"), 2)
        )
    )
    .withColumn(
        "margem_percentual",
        when(
            (col("situacao_custo") == "CUSTO_VALIDO")
            & (col("receita_liquida") != 0),
            spark_round(
                col("margem_bruta") / col("receita_liquida") * 100,
                2
            )
        )
    )
)

# 11. Selecionar os campos finais da fato
fato = base.select(
    "item_id",
    "pedido_id",
    "cliente_sk",
    "produto_sk",
    "data_sk",
    "data_venda",
    "sku",
    "quantidade",
    "preco_unitario",
    "desconto_unitario",
    "receita_bruta",
    "desconto_total",
    "receita_liquida",
    "custo_unitario",
    "custo_total",
    "margem_bruta",
    "margem_percentual",
    "situacao_custo",
    "qtd_registros_custo",
    "canal_venda",
    "status_pedido"
)

# 12. Validações antes da gravação
print("=== VALIDACOES DA FATO ===")

print("Total de registros:", fato.count())
print("Itens duplicados:", fato.groupBy("item_id").count()
      .filter(col("count") > 1).count())

print("Chaves de cliente sem correspondência:",
      fato.filter(col("cliente_sk").isNull()).count())

print("Chaves de produto sem correspondência:",
      fato.filter(col("produto_sk").isNull()).count())

print("Chaves de tempo sem correspondência:",
      fato.filter(col("data_sk").isNull()).count())

print("=== DISTRIBUICAO DA QUALIDADE DOS CUSTOS ===")
fato.groupBy("situacao_custo").count().orderBy("situacao_custo").show()

# 13. Resumo financeiro
print("=== RESUMO FINANCEIRO ===")
fato.agg(
    spark_round(spark_max("receita_liquida"), 2).alias("maior_receita_liquida_item")
).show()

# 14. Gravar Gold
DESTINO = GOLD + "fato_vendas/"

print(f"Gravando em {DESTINO}")

fato.write.mode("overwrite").parquet(DESTINO)

print("FATO VENDAS GRAVADA COM SUCESSO!")

spark.stop()
