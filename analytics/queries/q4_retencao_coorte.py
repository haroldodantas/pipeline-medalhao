
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, min as spark_min, countDistinct,
    trunc, months_between, floor, when,
    lit, round as spark_round, max as spark_max
)
from pathlib import Path
import csv
import matplotlib.pyplot as plt

# ============================================================
# QUESTAO 4 - RETENCAO POR COORTE
# ============================================================

spark = (
    SparkSession.builder
    .appName("Analytics - Q4 Retencao por Coorte")
    .config("spark.hadoop.fs.s3a.endpoint", "http://medalhao-localstack:4566")
    .config("spark.hadoop.fs.s3a.access.key", "test")
    .config("spark.hadoop.fs.s3a.secret.key", "test")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

GOLD = "s3a://techflow-gold/"
RESULTS = Path("/home/jovyan/work/analytics/results")
RESULTS.mkdir(parents=True, exist_ok=True)

print("\n=== QUESTAO 4 - RETENCAO POR COORTE ===")

# 1. Leitura da fato de vendas
fato = spark.read.parquet(GOLD + "fato_vendas/")

# 2. Uma data por cliente: primeira compra observada
primeira_compra = (
    fato
    .groupBy("cliente_sk")
    .agg(spark_min("data_venda").alias("data_primeira_compra"))
    .withColumn(
        "coorte",
        trunc(col("data_primeira_compra"), "month")
    )
)

# 3. Meses de atividade distintos por cliente
atividade = (
    fato
    .select("cliente_sk", "data_venda")
    .filter(col("data_venda").isNotNull())
    .withColumn("mes_atividade", trunc(col("data_venda"), "month"))
    .dropDuplicates(["cliente_sk", "mes_atividade"])
)

# 4. Associar cada atividade à coorte do cliente
base = (
    atividade
    .join(primeira_compra, "cliente_sk", "inner")
    .withColumn(
        "mes_coorte",
        floor(months_between(
            col("mes_atividade"),
            col("coorte")
        )).cast("integer")
    )
    .filter(col("mes_coorte") >= 0)
)

# 5. Tamanho original de cada coorte
tamanho_coorte = (
    primeira_compra
    .groupBy("coorte")
    .agg(countDistinct("cliente_sk").alias("clientes_coorte"))
)

# 6. Clientes ativos por coorte e idade mensal
retencao = (
    base
    .groupBy("coorte", "mes_coorte")
    .agg(countDistinct("cliente_sk").alias("clientes_ativos"))
    .join(tamanho_coorte, "coorte")
    .withColumn(
        "retencao_percentual",
        spark_round(
            col("clientes_ativos") / col("clientes_coorte") * 100,
            2
        )
    )
    .orderBy("coorte", "mes_coorte")
)

print("\n=== RETENCAO POR COORTE ===")
retencao.show(100, truncate=False)

# 7. Identificar a última data disponível
ultima_data = fato.agg(
    spark_max("data_venda").alias("ultima_data")
).first()["ultima_data"]

print("\nÚltima data disponível:", ultima_data)

# 8. Exportar detalhamento CSV
def exportar_csv(nome_arquivo, dataframe):
    linhas = [row.asDict() for row in dataframe.collect()]

    if not linhas:
        print("Sem registros para exportar:", nome_arquivo)
        return

    with open(
        RESULTS / nome_arquivo,
        "w",
        newline="",
        encoding="utf-8-sig"
    ) as arquivo_csv:
        escritor = csv.DictWriter(
            arquivo_csv,
            fieldnames=list(linhas[0].keys()),
            delimiter=";"
        )
        escritor.writeheader()
        escritor.writerows(linhas)

    print("Exportado:", nome_arquivo, "- registros:", len(linhas))


exportar_csv("q4_retencao_detalhada.csv", retencao)

# 9. Construir matriz de retenção
from datetime import date

linhas = retencao.collect()
coortes = sorted(set(row["coorte"] for row in linhas))

def indice_mes(data):
    return data.year * 12 + data.month

ultimo_mes = indice_mes(ultima_data)

matriz = {
    (row["coorte"], row["mes_coorte"]): float(row["retencao_percentual"])
    for row in linhas
}

max_mes = max(
    ultimo_mes - indice_mes(coorte)
    for coorte in coortes
)

arquivo_matriz = RESULTS / "q4_matriz_retencao.csv"

with open(arquivo_matriz, "w", newline="", encoding="utf-8-sig") as arquivo_csv:
    escritor = csv.writer(arquivo_csv, delimiter=";")
    escritor.writerow(["coorte"] + [f"mes_{i}" for i in range(max_mes + 1)])

    for coorte in coortes:
        valores = [str(coorte)]

        meses_observados = ultimo_mes - indice_mes(coorte)

        for mes in range(max_mes + 1):
            if mes > meses_observados:
                valores.append("")
            else:
                valores.append(matriz.get((coorte, mes), 0.0))

        escritor.writerow(valores)

print("Exportado:", arquivo_matriz)

# 10. Preparar dados do gráfico
import math

dados_grafico = []

for coorte in coortes:
    linha = []
    meses_observados = ultimo_mes - indice_mes(coorte)

    for mes in range(max_mes + 1):
        if mes > meses_observados:
            linha.append(float("nan"))
        else:
            linha.append(matriz.get((coorte, mes), 0.0))

    dados_grafico.append(linha)


# 11. Gerar heatmap com tamanho das coortes e observacoes

# Quantidade de clientes em cada coorte
tamanhos_coorte = {
    row["coorte"]: row["clientes_coorte"]
    for row in tamanho_coorte.collect()
}

fig, ax = plt.subplots(figsize=(13, 7))

imagem = ax.imshow(
    dados_grafico,
    aspect="auto",
    cmap="YlGnBu",
    vmin=0,
    vmax=100
)

ax.set_xticks(range(max_mes + 1))
ax.set_xticklabels([f"Mês {i}" for i in range(max_mes + 1)])

rotulos_coorte = [
    f"{coorte.strftime('%Y-%m')} (n={tamanhos_coorte[coorte]:,})"
    for coorte in coortes
]

ax.set_yticks(range(len(coortes)))
ax.set_yticklabels(rotulos_coorte)

ax.set_title(
    "Retenção de clientes por coorte",
    fontsize=15,
    pad=18
)

ax.set_xlabel("Meses desde a primeira compra")
ax.set_ylabel("Coorte de primeira compra (n = clientes iniciais)")

# Mostrar percentuais nas células observadas
for i, linha in enumerate(dados_grafico):
    for j, valor in enumerate(linha):
        if not math.isnan(valor):
            ax.text(
                j, i,
                f"{valor:.1f}%",
                ha="center",
                va="center",
                fontsize=10
            )

fig.colorbar(imagem, ax=ax, label="Retenção (%)")

# Ressalvas metodológicas
observacao = (
    f"Dados disponíveis até {ultima_data.strftime('%d/%m/%Y')}. "
    "Outubro representa período parcial. "
    "Coortes baseadas na primeira compra observada na base disponível."
)

fig.text(
    0.5,
    0.015,
    observacao,
    ha="center",
    fontsize=8,
    wrap=True
)

plt.tight_layout(rect=[0, 0.07, 1, 1])

arquivo_grafico = RESULTS / "q4_retencao_coorte.png"

plt.savefig(
    arquivo_grafico,
    dpi=150,
    bbox_inches="tight"
)

plt.close()

print("\nGráfico atualizado:", arquivo_grafico)


spark.stop()
