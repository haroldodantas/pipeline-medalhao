
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import csv
import os
import matplotlib.pyplot as plt


# ============================================================
# 1. CONFIGURAÇÃO
# ============================================================

spark = (
    SparkSession.builder
    .appName("Q2_Funil_Conversao")
    .config("spark.hadoop.fs.s3a.endpoint", "http://medalhao-localstack:4566")
    .config("spark.hadoop.fs.s3a.access.key", "test")
    .config("spark.hadoop.fs.s3a.secret.key", "test")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

CAMINHO_RESULTADOS = "/home/jovyan/work/analytics/results"
os.makedirs(CAMINHO_RESULTADOS, exist_ok=True)


# ============================================================
# 2. LEITURA DOS DADOS SILVER
# ============================================================

eventos = spark.read.parquet("s3a://techflow-silver/clickstream/")
pedidos = spark.read.parquet("s3a://techflow-silver/pedidos/")

eventos.createOrReplaceTempView("clickstream")
pedidos.createOrReplaceTempView("pedidos")


# ============================================================
# 3. FUNIL POR SESSÃO E DISPOSITIVO
# ============================================================

# Cada sessão só avança quando a etapa anterior existe
# e o timestamp da etapa seguinte é igual ou posterior.

sql_funil = """
WITH etapas AS (
    SELECT
        session_id,
        COALESCE(device, 'nao_identificado') AS dispositivo,

        MIN(CASE
            WHEN event_type = 'product_view'
            THEN event_timestamp
        END) AS ts_visualizacao,

        MIN(CASE
            WHEN event_type = 'add_to_cart'
            THEN event_timestamp
        END) AS ts_carrinho,

        MIN(CASE
            WHEN event_type = 'checkout_start'
            THEN event_timestamp
        END) AS ts_checkout,

        MIN(CASE
            WHEN event_type = 'purchase'
            THEN event_timestamp
        END) AS ts_compra

    FROM clickstream
    WHERE session_id IS NOT NULL
    GROUP BY session_id, device
),

sessoes AS (
    SELECT
        session_id,
        dispositivo,

        CASE
            WHEN ts_visualizacao IS NOT NULL
            THEN 1 ELSE 0
        END AS visualizou,

        CASE
            WHEN ts_visualizacao IS NOT NULL
             AND ts_carrinho IS NOT NULL
             AND ts_carrinho >= ts_visualizacao
            THEN 1 ELSE 0
        END AS adicionou_carrinho,

        CASE
            WHEN ts_visualizacao IS NOT NULL
             AND ts_carrinho IS NOT NULL
             AND ts_checkout IS NOT NULL
             AND ts_carrinho >= ts_visualizacao
             AND ts_checkout >= ts_carrinho
            THEN 1 ELSE 0
        END AS iniciou_checkout,

        CASE
            WHEN ts_visualizacao IS NOT NULL
             AND ts_carrinho IS NOT NULL
             AND ts_checkout IS NOT NULL
             AND ts_compra IS NOT NULL
             AND ts_carrinho >= ts_visualizacao
             AND ts_checkout >= ts_carrinho
             AND ts_compra >= ts_checkout
            THEN 1 ELSE 0
        END AS comprou

    FROM etapas
)

SELECT
    dispositivo,
    COUNT(*) AS sessoes,
    SUM(visualizou) AS visualizacoes,
    SUM(adicionou_carrinho) AS carrinhos,
    SUM(iniciou_checkout) AS checkouts,
    SUM(comprou) AS compras

FROM sessoes
GROUP BY dispositivo
ORDER BY dispositivo
"""

resultado = spark.sql(sql_funil).collect()


# ============================================================
# 4. CÁLCULO DAS TAXAS DE CONVERSÃO
# ============================================================

dados_funil = []

for linha in resultado:
    visualizacoes = linha["visualizacoes"]
    carrinhos = linha["carrinhos"]
    checkouts = linha["checkouts"]
    compras = linha["compras"]

    taxa_carrinho = (
        carrinhos / visualizacoes * 100
        if visualizacoes else 0
    )

    taxa_checkout = (
        checkouts / carrinhos * 100
        if carrinhos else 0
    )

    taxa_compra = (
        compras / checkouts * 100
        if checkouts else 0
    )

    conversao_total = (
        compras / visualizacoes * 100
        if visualizacoes else 0
    )

    dados_funil.append({
        "dispositivo": linha["dispositivo"],
        "sessoes": linha["sessoes"],
        "visualizacoes": visualizacoes,
        "carrinhos": carrinhos,
        "checkouts": checkouts,
        "compras": compras,
        "taxa_visualizacao_carrinho_pct": round(taxa_carrinho, 2),
        "taxa_carrinho_checkout_pct": round(taxa_checkout, 2),
        "taxa_checkout_compra_pct": round(taxa_compra, 2),
        "conversao_total_pct": round(conversao_total, 2)
    })


# ============================================================
# 5. EXPORTAÇÃO DO FUNIL PARA CSV
# ============================================================

arquivo_funil = os.path.join(
    CAMINHO_RESULTADOS,
    "q2_funil_por_dispositivo.csv"
)

with open(arquivo_funil, "w", newline="", encoding="utf-8-sig") as arquivo:
    escritor = csv.DictWriter(
        arquivo,
        fieldnames=list(dados_funil[0].keys()),
        delimiter=";"
    )
    escritor.writeheader()
    escritor.writerows(dados_funil)


# ============================================================
# 6. VALIDAÇÃO DAS COMPRAS CONTRA OS PEDIDOS
# ============================================================

sql_validacao = """
SELECT
    COUNT(DISTINCT c.session_id) AS sessoes_com_compra,
    COUNT(DISTINCT c.pedido_id) AS pedidos_referenciados,

    COUNT(DISTINCT CASE
        WHEN p.id IS NOT NULL THEN c.pedido_id
    END) AS pedidos_encontrados,

    COUNT(DISTINCT CASE
        WHEN p.status IN ('pago', 'enviado', 'entregue')
        THEN c.pedido_id
    END) AS pedidos_elegiveis,

    COUNT(DISTINCT CASE
        WHEN p.id IS NULL THEN c.pedido_id
    END) AS pedidos_sem_correspondencia

FROM clickstream c
LEFT JOIN pedidos p
    ON c.pedido_id = p.id
WHERE c.event_type = 'purchase'
"""

validacao = spark.sql(sql_validacao).first()

dados_validacao = [{
    "sessoes_com_compra": validacao["sessoes_com_compra"],
    "pedidos_referenciados": validacao["pedidos_referenciados"],
    "pedidos_encontrados": validacao["pedidos_encontrados"],
    "pedidos_elegiveis": validacao["pedidos_elegiveis"],
    "pedidos_sem_correspondencia": validacao["pedidos_sem_correspondencia"]
}]

arquivo_validacao = os.path.join(
    CAMINHO_RESULTADOS,
    "q2_validacao_pedidos.csv"
)

with open(arquivo_validacao, "w", newline="", encoding="utf-8-sig") as arquivo:
    escritor = csv.DictWriter(
        arquivo,
        fieldnames=list(dados_validacao[0].keys()),
        delimiter=";"
    )
    escritor.writeheader()
    escritor.writerows(dados_validacao)


# ============================================================
# 7. GRÁFICO DO FUNIL POR DISPOSITIVO
# ============================================================

dispositivos = [d["dispositivo"] for d in dados_funil]

etapas = {
    "Visualização": [d["visualizacoes"] for d in dados_funil],
    "Carrinho": [d["carrinhos"] for d in dados_funil],
    "Checkout": [d["checkouts"] for d in dados_funil],
    "Compra": [d["compras"] for d in dados_funil]
}

posicoes = list(range(len(dispositivos)))
largura = 0.18

plt.figure(figsize=(11, 6))

for indice, (nome, valores) in enumerate(etapas.items()):
    deslocamento = (indice - 1.5) * largura
    barras = plt.bar(
        [p + deslocamento for p in posicoes],
        valores,
        largura,
        label=nome
    )

    plt.bar_label(barras, padding=2, fontsize=8)

plt.xticks(posicoes, dispositivos)
plt.title("Funil de conversão por dispositivo")
plt.xlabel("Tipo de dispositivo")
plt.ylabel("Quantidade de sessões")
plt.legend()
plt.grid(axis="y", alpha=0.25)
plt.tight_layout()

arquivo_grafico = os.path.join(
    CAMINHO_RESULTADOS,
    "q2_funil_conversao.png"
)

plt.savefig(arquivo_grafico, dpi=150)
plt.close()


# ============================================================
# 8. RESULTADOS E INTERPRETAÇÃO INICIAL
# ============================================================

print("\n========== Q2 - FUNIL DE CONVERSÃO ==========\n")

for d in dados_funil:
    print(f"Dispositivo: {d['dispositivo']}")
    print(f"Visualizações: {d['visualizacoes']:,}")
    print(f"Carrinhos: {d['carrinhos']:,}")
    print(f"Checkouts: {d['checkouts']:,}")
    print(f"Compras: {d['compras']:,}")
    print(f"Conversão visualização → carrinho: {d['taxa_visualizacao_carrinho_pct']}%")
    print(f"Conversão carrinho → checkout: {d['taxa_carrinho_checkout_pct']}%")
    print(f"Conversão checkout → compra: {d['taxa_checkout_compra_pct']}%")
    print(f"Conversão total: {d['conversao_total_pct']}%")
    print("--------------------------------------------")

print("\n========== VALIDAÇÃO COM PEDIDOS ==========\n")
for chave, valor in dados_validacao[0].items():
    print(f"{chave}: {valor:,}")

print("\nArquivos gerados:")
print(arquivo_funil)
print(arquivo_validacao)
print(arquivo_grafico)

spark.stop()
