
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pathlib import Path
import csv

# ============================================================
# QUESTAO 1 - TICKET MEDIO MENSAL POR CATEGORIA
# ============================================================

spark = (
    SparkSession.builder
    .appName("Analytics - Q1 Ticket Medio por Categoria")
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

print("\n=== QUESTAO 1 - TICKET MEDIO POR CATEGORIA ===")

try:
    # 1. Leitura das tabelas Gold
    fato = spark.read.parquet(GOLD + "fato_vendas/")
    produtos = spark.read.parquet(GOLD + "dim_produto/")

    # 2. Associar cada item vendido à sua categoria
    base = (
        fato.join(
            produtos.select("produto_sk", "categoria"),
            on="produto_sk",
            how="inner"
        )
        .filter(col("data_venda").isNotNull())
    )

    base.createOrReplaceTempView("vendas_categoria")

    # 3. Consulta Spark SQL:
    # Ticket médio = receita líquida / pedidos distintos
    query_ticket = """
        SELECT
            date_format(data_venda, 'yyyy-MM') AS mes,
            categoria,
            COUNT(DISTINCT pedido_id) AS quantidade_pedidos,
            ROUND(SUM(receita_liquida), 2) AS receita_liquida,
            ROUND(
                SUM(receita_liquida) /
                COUNT(DISTINCT pedido_id),
                2
            ) AS ticket_medio
        FROM vendas_categoria
        GROUP BY
            date_format(data_venda, 'yyyy-MM'),
            categoria
        ORDER BY mes, categoria
    """

    ticket_mensal = spark.sql(query_ticket)

    print("\n=== TICKET MEDIO MENSAL POR CATEGORIA ===")
    ticket_mensal.show(100, truncate=False)

    # 4. Crescimento do ticket médio:
    # Comparação entre julho e setembro de 2026.
    # Outubro não entra porque é um mês parcial.

    ticket_mensal.createOrReplaceTempView("ticket_mensal")

    query_crescimento = """
        SELECT
            categoria,
            MAX(CASE WHEN mes = '2026-07'
                THEN ticket_medio END) AS ticket_julho,
            MAX(CASE WHEN mes = '2026-09'
                THEN ticket_medio END) AS ticket_setembro,
            ROUND(
                (
                    MAX(CASE WHEN mes = '2026-09'
                        THEN ticket_medio END)
                    /
                    MAX(CASE WHEN mes = '2026-07'
                        THEN ticket_medio END) - 1
                ) * 100,
                2
            ) AS crescimento_percentual
        FROM ticket_mensal
        GROUP BY categoria
        HAVING
            MAX(CASE WHEN mes = '2026-07'
                THEN ticket_medio END) IS NOT NULL
            AND
            MAX(CASE WHEN mes = '2026-09'
                THEN ticket_medio END) IS NOT NULL
            AND
            MAX(CASE WHEN mes = '2026-07'
                THEN ticket_medio END) <> 0
        ORDER BY crescimento_percentual DESC
    """

    crescimento = spark.sql(query_crescimento)

    print("\n=== CRESCIMENTO DO TICKET MEDIO: JULHO X SETEMBRO ===")
    crescimento.show(100, truncate=False)

    # 5. Identificar a categoria com maior crescimento
    maior_crescimento = crescimento.first()

    if maior_crescimento:
        print("\n=== CATEGORIA QUE MAIS CRESCEU ===")
        print("Categoria:", maior_crescimento["categoria"])
        print("Ticket em julho:", maior_crescimento["ticket_julho"])
        print("Ticket em setembro:", maior_crescimento["ticket_setembro"])
        print("Crescimento percentual:", maior_crescimento["crescimento_percentual"], "%")

    # 6. Exportação dos resultados em CSV
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
        ) as arquivo:
            escritor = csv.DictWriter(
                arquivo,
                fieldnames=list(linhas[0].keys()),
                delimiter=";"
            )
            escritor.writeheader()
            escritor.writerows(linhas)

        print("Exportado:", nome_arquivo, "- registros:", len(linhas))

    exportar_csv("q1_ticket_medio_mensal.csv", ticket_mensal)
    exportar_csv("q1_crescimento_categoria.csv", crescimento)

    # 7. Gráfico de evolução mensal do ticket médio
    try:
        import matplotlib.pyplot as plt

        dados = ticket_mensal.collect()
        meses = sorted(set(row["mes"] for row in dados))
        categorias = sorted(set(row["categoria"] for row in dados))

        plt.figure(figsize=(12, 6))

        for categoria in categorias:
            valores = {
                row["mes"]: float(row["ticket_medio"])
                for row in dados
                if row["categoria"] == categoria
            }

            eixo_y = [valores.get(mes) for mes in meses]

            plt.plot(
                meses,
                eixo_y,
                marker="o",
                label=categoria
            )

        plt.title(
            "Ticket medio mensal por categoria\n"
            "Outubro/2026: periodo parcial ate 12/10"
        )
        plt.xlabel("Mes")
        plt.ylabel("Ticket medio (R$)")
        plt.xticks(rotation=45)
        plt.grid(True, alpha=0.3)
        plt.legend(title="Categoria")
        plt.tight_layout()

        caminho_grafico = RESULTS / "q1_ticket_medio_mensal.png"
        plt.savefig(caminho_grafico, dpi=150)
        plt.close()

        print("\nGrafico salvo:", caminho_grafico)

    except ImportError:
        print(
            "\nATENCAO: matplotlib nao esta instalado. "
            "Os CSVs foram exportados, mas o grafico nao foi gerado."
        )

    print("\n=== QUESTAO 1 CONCLUIDA ===")

finally:
    spark.stop()
