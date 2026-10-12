
import json
import random
from zoneinfo import ZoneInfo
from datetime import datetime, timedelta
from pathlib import Path
from uuid import uuid4

from faker import Faker

fake = Faker("pt_BR")
Faker.seed(42)
random.seed(42)

# Configurações do conjunto de dados
TOTAL_EVENTOS = 300_000
DIAS_HISTORICO = 90
PERCENTUAL_DUPLICADOS = 0.03

DIRETORIO_SAIDA = Path("landing/clickstream")

TIPOS_EVENTO = [
    "page_view",
    "product_view",
    "add_to_cart",
    "checkout_start",
    "purchase",
]

DISPOSITIVOS = ["desktop", "mobile", "tablet"]
CANAIS = ["organic_search", "paid_search", "social", "email", "direct"]
PAGINAS = ["/", "/produtos", "/categorias", "/carrinho", "/checkout"]


def gerar_evento(
    indice: int,
    inicio: datetime,
    fim: datetime,
) -> dict:
    """Gera um evento sintético com estrutura aninhada."""

    instante = inicio + timedelta(
        seconds=random.randint(0, int((fim - inicio).total_seconds()))
    )

    tipo = random.choices(
        TIPOS_EVENTO,
        weights=[35, 30, 15, 10, 10],
        k=1,
    )[0]

    event_id = str(uuid4())

    evento = {
        "event_id": event_id,
        "event_type": tipo,
        "event_timestamp": instante.isoformat(),
        "user": {
            "user_id": (
                str(random.randint(1, 5000))
                if random.random() >= 0.25
                else None
            ),
            "anonymous_id": str(uuid4()),
            "session_id": str(uuid4()),
            "device": random.choice(DISPOSITIVOS),
        },
        "context": {
            "channel": random.choice(CANAIS),
            "page": {
                "url": fake.url() + random.choice(PAGINAS),
                "referrer": random.choice(PAGINAS),
            },
        },
        "properties": {
            "sku": f"SKU-{random.randint(1, 200):04d}",
            "quantity": random.randint(1, 5),
            "price": round(random.uniform(20, 1500), 2),
        },
    }

    # Mudança de esquema a partir do 61º dia do histórico.
    if instante >= inicio + timedelta(days=60):
        evento["properties"]["unit_price"] = evento["properties"].pop("price")
        evento["ab_test_variant"] = random.choice(["A", "B", "C"])

    return evento


def main() -> None:
    DIRETORIO_SAIDA.mkdir(parents=True, exist_ok=True)

    for arquivo in DIRETORIO_SAIDA.glob("eventos_*.jsonl"):
        arquivo.unlink()

    # Limita a geração ao histórico dos últimos 90 dias.
   
    fuso = ZoneInfo("America/Sao_Paulo")
    fim = datetime.now(fuso).replace(microsecond=0)
    inicio = fim - timedelta(days=DIAS_HISTORICO)

    eventos = [
        gerar_evento(i, inicio, fim)
        for i in range(TOTAL_EVENTOS)
    ]

    # Introduz aproximadamente 3% de IDs duplicados.
    quantidade_duplicados = int(TOTAL_EVENTOS * PERCENTUAL_DUPLICADOS)
    indices_duplicados = random.sample(
        range(quantidade_duplicados, TOTAL_EVENTOS),
        quantidade_duplicados,
    )

    for destino, origem in enumerate(indices_duplicados):
        eventos[destino]["event_id"] = eventos[origem]["event_id"]

    # Grava JSONL em arquivos separados por data.
    arquivos_abertos = {}

    try:
        for evento in eventos:
            data_evento = datetime.fromisoformat(
                evento["event_timestamp"]
            ).date().isoformat()

            caminho = DIRETORIO_SAIDA / f"eventos_{data_evento}.jsonl"

            if caminho not in arquivos_abertos:
                arquivos_abertos[caminho] = caminho.open(
                    "a",
                    encoding="utf-8",
                )

            arquivos_abertos[caminho].write(
                json.dumps(evento, ensure_ascii=False) + "\n"
            )
    finally:
        for arquivo in arquivos_abertos.values():
            arquivo.close()

    print("Geração de clickstream concluída.")
    print(f"Eventos gerados: {len(eventos)}")
    print(f"IDs duplicados introduzidos: {quantidade_duplicados}")
    print(f"Diretório: {DIRETORIO_SAIDA.resolve()}")


if __name__ == "__main__":
    main()
