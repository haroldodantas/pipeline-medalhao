
import csv
import random
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

random.seed(42)

DIRETORIO_SAIDA = Path("landing/csv")
TOTAL_REGISTROS_CUSTOS = 500
TOTAL_REGISTROS_MIDIA = 90

CANAIS_MIDIA = [
    "Google Ads",
    "google_ads",
    "google ads",
    "GOOGLE",
    "Meta Ads",
    "Facebook Ads",
    "facebook",
    "Instagram Ads",
    "instagram",
    "Email",
    "e-mail",
    "E-mail Marketing",
    "Organic Search",
    "organic_search",
    "Busca Orgânica",
    "Direct",
    "direto",
]


def formatar_moeda_brasileira(valor: Decimal) -> str:
    """Formata valores como 1.234,56."""
    return f"{valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def gerar_custos() -> None:
    caminho = DIRETORIO_SAIDA / "custos_produtos.csv"
    data_inicio = date.today() - timedelta(days=89)

    with caminho.open("w", encoding="latin-1", newline="") as arquivo:
        escritor = csv.writer(arquivo, delimiter=";")

        escritor.writerow([
            "sku",
            "custo_unitario",
            "vigencia_inicio",
            "vigencia_fim",
        ])

        for i in range(TOTAL_REGISTROS_CUSTOS):
            # Inclui deliberadamente SKUs que não existem no MySQL.
            sku_numero = random.randint(1, 250)
            sku = f"SKU-{sku_numero:04d}"

            custo = Decimal(
                str(round(random.uniform(8, 900), 2))
            )

            inicio = data_inicio + timedelta(
                days=random.randint(0, 89)
            )
            fim = inicio + timedelta(
                days=random.randint(15, 120)
            )

            escritor.writerow([
                sku,
                formatar_moeda_brasileira(custo),
                inicio.strftime("%d/%m/%Y"),
                fim.strftime("%d/%m/%Y"),
            ])

            # Linhas em branco intencionais para testar a qualidade.
            if i in (99, 249, 399):
                escritor.writerow([])

        # Linha de total intencional, que não representa um produto.
        escritor.writerow([
            "TOTAL",
            formatar_moeda_brasileira(Decimal("0.00")),
            "",
            "",
        ])

    print(f"Arquivo de custos criado: {caminho}")


def gerar_midia() -> None:
    caminho = DIRETORIO_SAIDA / "investimento_midia.csv"
    data_inicio = date.today() - timedelta(days=89)

    with caminho.open("w", encoding="utf-8", newline="") as arquivo:
        escritor = csv.writer(arquivo, delimiter=",")

        escritor.writerow([
            "data",
            "canal",
            "campanha",
            "investimento",
            "impressoes",
            "cliques",
        ])

        for i in range(TOTAL_REGISTROS_MIDIA):
            dia = data_inicio + timedelta(days=i)
            canal = random.choice(CANAIS_MIDIA)
            investimento = round(random.uniform(100, 5000), 2)
            impressoes = random.randint(5000, 200000)
            cliques = random.randint(100, min(impressoes, 10000))

            escritor.writerow([
                dia.isoformat(),
                canal,
                f"campanha_{canal.lower().replace(' ', '_')}_{i + 1}",
                f"{investimento:.2f}",
                impressoes,
                cliques,
            ])

    print(f"Arquivo de mídia criado: {caminho}")


def main() -> None:
    DIRETORIO_SAIDA.mkdir(parents=True, exist_ok=True)

    gerar_custos()
    gerar_midia()

    print("Geração dos CSVs concluída.")


if __name__ == "__main__":
    main()
