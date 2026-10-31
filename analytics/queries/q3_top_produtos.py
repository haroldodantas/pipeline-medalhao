from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, count, when, round as spark_round
)

# ============================================================
# QUESTAO 3 - TOP 10 PRODUTOS POR RECEITA X MARGEM
# ============================================================

spark = (
    SparkSession.builder
    .appName("Analytics - Q3 Top Produtos")
    .config("spark.hadoop.fs.s3a.endpoint", "http://medalhao-localstack:4566")
    .config("spark.hadoop.fs.s3a.access.key", "test")
    .config("spark.hadoop.fs.s3a.secret.key", "test")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

GOLD = "s3a://techflow-gold/"

print("\n=== QUESTAO 3 - TOP 10 PRODUTOS ===")

# 1. Leitura das tabelas Gold
fato = spark.read.parquet(GOLD + "fato_vendas/")
produtos = spark.read.parquet(GOLD + "dim_produto/")

# 2. Associar os dados de vendas aos nomes dos produtos
base = (
    fato.join(
        produtos.select("produto_sk", "nome", "categoria"),
        on="produto_sk",
        how="inner"
    )
)

# 3. Ranking por receita liquida
ranking_receita = (
    base.groupBy("sku", "nome", "categoria")
    .agg(
        spark_sum("receita_liquida").alias("receita_liquida"),
        spark_sum("quantidade").alias("unidades_vendidas")
    )
    .orderBy(col("receita_liquida").desc())
    .limit(10)
)

print("\n=== TOP 10 PRODUTOS POR RECEITA LIQUIDA ===")
ranking_receita.show(10, truncate=False)

# 4. Ranking por margem bruta
# Somente itens com custo exatamente valido
base_custo_valido = base.filter(
    col("situacao_custo") == "CUSTO_VALIDO"
)

ranking_margem = (
    base_custo_valido.groupBy("sku", "nome", "categoria")
    .agg(
        spark_sum("margem_bruta").alias("margem_bruta"),
        spark_sum("receita_liquida").alias("receita_com_custo_valido"),
        spark_sum("quantidade").alias("unidades_com_custo_valido")
    )
    .orderBy(col("margem_bruta").desc())
    .limit(10)
)

print("\n=== TOP 10 PRODUTOS POR MARGEM BRUTA ===")
ranking_margem.show(10, truncate=False)

# 5. Comparacao dos rankings
skus_receita = {
    row["sku"] for row in ranking_receita.select("sku").collect()
}

skus_margem = {
    row["sku"] for row in ranking_margem.select("sku").collect()
}

comuns = sorted(skus_receita.intersection(skus_margem))
somente_receita = sorted(skus_receita - skus_margem)
somente_margem = sorted(skus_margem - skus_receita)

print("\n=== COMPARACAO DOS RANKINGS ===")
print("Produtos presentes nos dois rankings:", len(comuns))
print("SKUs em comum:", comuns)
print("Somente no ranking de receita:", somente_receita)
print("Somente no ranking de margem:", somente_margem)

# 6. Cobertura e qualidade dos custos
cobertura = base.agg(
    count("*").alias("total_itens"),
    spark_sum("receita_liquida").alias("receita_total"),
    spark_sum(
        when(
            col("situacao_custo") == "CUSTO_VALIDO",
            col("receita_liquida")
        ).otherwise(0)
    ).alias("receita_com_custo_valido"),
    spark_sum(
        when(
            col("situacao_custo") == "CUSTO_VALIDO", 1
        ).otherwise(0)
    ).alias("itens_com_custo_valido")
).first()

print("\n=== COBERTURA DOS CUSTOS ===")
print("Total de itens:", cobertura["total_itens"])
print("Receita liquida total: R$", cobertura["receita_total"])
print("Receita com custo valido: R$", cobertura["receita_com_custo_valido"])
print("Itens com custo valido:", cobertura["itens_com_custo_valido"])

if cobertura["receita_total"] and cobertura["receita_total"] != 0:
    percentual = (
        float(cobertura["receita_com_custo_valido"])
        / float(cobertura["receita_total"])
    ) * 100
    print("Cobertura da receita por custos validos: %.2f%%" % percentual)

print("\n=== FIM DA QUESTAO 3 ===")




# ============================================================
# ============================================================
# 7. EXPORTACAO DOS RESULTADOS EM CSV
# ============================================================

import csv
from pathlib import Path

RESULTS = Path("/home/jovyan/work/analytics/results")
RESULTS.mkdir(parents=True, exist_ok=True)

def exportar_csv(nome_arquivo, dataframe):
    linhas = [row.asDict() for row in dataframe.collect()]

    if not linhas:
        print("Sem registros para exportar:", nome_arquivo)
        return

    with open(RESULTS / nome_arquivo, "w", newline="", encoding="utf-8-sig") as arquivo_csv:
        escritor = csv.DictWriter(
            arquivo_csv,
            fieldnames=list(linhas[0].keys()),
            delimiter=";"
        )
        escritor.writeheader()
        escritor.writerows(linhas)

    print("Exportado:", nome_arquivo, "- registros:", len(linhas))


exportar_csv("q3_top10_receita.csv", ranking_receita)
exportar_csv("q3_top10_margem.csv", ranking_margem)

comparacao_linhas = (
    [{"sku": sku, "classificacao": "AMBOS"} for sku in comuns]
    + [{"sku": sku, "classificacao": "SOMENTE_RECEITA"} for sku in somente_receita]
    + [{"sku": sku, "classificacao": "SOMENTE_MARGEM"} for sku in somente_margem]
)

with open(RESULTS / "q3_comparacao_rankings.csv", "w", newline="", encoding="utf-8-sig") as arquivo_csv:
    escritor = csv.DictWriter(
        arquivo_csv,
        fieldnames=["sku", "classificacao"],
        delimiter=";"
    )
    escritor.writeheader()
    escritor.writerows(comparacao_linhas)

print("Exportado: q3_comparacao_rankings.csv - registros:", len(comparacao_linhas))
print("\nTodos os CSVs foram salvos em:", RESULTS)




spark.stop()
