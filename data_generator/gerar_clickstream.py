
import os
import json
import random
from collections import Counter
from datetime import datetime, timedelta, time
from pathlib import Path
from uuid import uuid4
from zoneinfo import ZoneInfo

import mysql.connector


# ============================================================
# CONFIGURAÇÕES
# ============================================================

TOTAL_EVENTOS = 300_000
PERCENTUAL_DUPLICADOS = 0.03

SESSOES_ABANDONO_VIEW = 20_000
SESSOES_ABANDONO_CART = 10_000
SESSOES_ABANDONO_CHECKOUT = 5_000

DIRETORIO_SAIDA = Path("landing/clickstream")

FUSO = ZoneInfo("America/Sao_Paulo")

CANAIS = [
    "organic_search",
    "paid_search",
    "social",
    "email",
    "direct",
]

DISPOSITIVOS = ["desktop", "mobile", "tablet"]

PAGINAS = {
    "page_view": "/",
    "product_view": "/produtos",
    "add_to_cart": "/carrinho",
    "checkout_start": "/checkout",
    "purchase": "/checkout/sucesso",
}

random.seed(42)


# ============================================================
# MYSQL
# ============================================================

def carregar_pedidos():
    conexao = mysql.connector.connect(
        host=os.getenv("MYSQL_HOST", "127.0.0.1"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        user=os.getenv("MYSQL_USER", "etl_user"),
        password=os.getenv("MYSQL_PASSWORD", "change_me_local"),
        database=os.getenv("MYSQL_DATABASE", "techflow_oltp"),
    )

    cursor = conexao.cursor()

    sql = """
        SELECT
            p.id,
            p.cliente_id,
            p.criado_em,
            i.sku,
            i.quantidade,
            i.preco_unitario
        FROM pedidos p
        INNER JOIN itens_pedido i
            ON i.pedido_id = p.id
        WHERE p.status IN ('pago', 'enviado', 'entregue')
        ORDER BY p.id, i.id
    """

    try:
        cursor.execute(sql)
        linhas = cursor.fetchall()

    finally:
        cursor.close()
        conexao.close()

    pedidos = {}

    # Um item representativo por pedido.
    for linha in linhas:
        pedido_id, cliente_id, criado_em, sku, quantidade, preco = linha

        if pedido_id not in pedidos:
            pedidos[pedido_id] = {
                "pedido_id": pedido_id,
                "cliente_id": cliente_id,
                "criado_em": criado_em.replace(tzinfo=FUSO),
                "sku": sku,
                "quantidade": quantidade,
                "preco": float(preco) if preco is not None else None,
            }

    return list(pedidos.values())


# ============================================================
# EVENTOS
# ============================================================

def criar_evento(
    tipo,
    instante,
    user_id,
    anonymous_id,
    session_id,
    device,
    channel,
    sku,
    quantity,
    price=None,
    pedido_id=None,
    data_inicio=None,
):
    propriedades = {
        "sku": sku,
        "quantity": quantity,
    }

    # Mantém a mudança de esquema do gerador original.
    if instante >= data_inicio + timedelta(days=60):
        propriedades["unit_price"] = price
    else:
        propriedades["price"] = price

    if pedido_id is not None:
        propriedades["pedido_id"] = pedido_id

    caminho = PAGINAS[tipo]

    evento = {
        "event_id": str(uuid4()),
        "event_type": tipo,
        "event_timestamp": instante.isoformat(),
        "user": {
            "user_id": str(user_id) if user_id is not None else None,
            "anonymous_id": anonymous_id,
            "session_id": session_id,
            "device": device,
        },
        "context": {
            "channel": channel,
            "page": {
                "url": "https://techflow.example" + caminho,
                "referrer": "/",
            },
        },
        "properties": propriedades,
    }

    if instante >= data_inicio + timedelta(days=60):
        evento["ab_test_variant"] = random.choice(["A", "B", "C"])

    return evento


# ============================================================
# SESSÕES
# ============================================================

def gerar_sessao(
    eventos,
    sessoes,
    etapas,
    primeiro_instante,
    user_id,
    anonymous_id,
    device,
    channel,
    sku,
    quantity=1,
    price=None,
    pedido_id=None,
):
    session_id = str(uuid4())

    primeiro_instante = primeiro_instante.astimezone(FUSO)

    for indice, tipo in enumerate(etapas):
        instante = primeiro_instante + timedelta(minutes=indice)

        evento = criar_evento(
            tipo=tipo,
            instante=instante,
            user_id=user_id,
            anonymous_id=anonymous_id,
            session_id=session_id,
            device=device,
            channel=channel,
            sku=sku,
            quantity=quantity,
            price=price,
            pedido_id=pedido_id if tipo == "purchase" else None,
            data_inicio=DATA_INICIO,
        )

        eventos.append(evento)

    sessoes.append({
        "session_id": session_id,
        "primeiro_instante": primeiro_instante,
        "user_id": user_id,
        "anonymous_id": anonymous_id,
        "device": device,
        "channel": channel,
        "sku": sku,
        "quantity": quantity,
    })


# ============================================================
# GERAÇÃO PRINCIPAL
# ============================================================

def main():
    global DATA_INICIO

    print("Consultando pedidos no MySQL...")

    pedidos = carregar_pedidos()

    if not pedidos:
        raise RuntimeError("Nenhum pedido elegível encontrado.")

    print(f"Pedidos elegíveis: {len(pedidos):,}")

    menor_data = min(p["criado_em"] for p in pedidos)
    maior_data = max(p["criado_em"] for p in pedidos)

    DATA_INICIO = datetime.combine(
        menor_data.date(),
        time.min,
        tzinfo=FUSO,
    )

    DATA_FIM = maior_data

    # Identificação persistente por cliente.
    anonymous_por_cliente = {
        cliente_id: str(uuid4())
        for cliente_id in range(1, 5001)
    }

    eventos = []
    sessoes = []

    print("Gerando jornadas de compra...")

    # 1. Jornadas completas vinculadas aos pedidos reais.
    for pedido in pedidos:
        cliente_id = pedido["cliente_id"]

        gerar_sessao(
            eventos=eventos,
            sessoes=sessoes,
            etapas=[
                "product_view",
                "add_to_cart",
                "checkout_start",
                "purchase",
            ],
            primeiro_instante=pedido["criado_em"] - timedelta(minutes=3),
            user_id=cliente_id,
            anonymous_id=anonymous_por_cliente[cliente_id],
            device=random.choice(DISPOSITIVOS),
            channel=random.choice(CANAIS),
            sku=pedido["sku"],
            quantity=pedido["quantidade"],
            price=pedido["preco"],
            pedido_id=pedido["pedido_id"],
        )

    print("Gerando jornadas abandonadas...")

    intervalo = int((DATA_FIM - DATA_INICIO).total_seconds())

    if intervalo <= 7200:
        raise RuntimeError("Intervalo de datas insuficiente.")

    # 2. Sessões que visualizaram produto, mas não adicionaram ao carrinho.
    # 3. Sessões que adicionaram ao carrinho, mas não iniciaram checkout.
    # 4. Sessões que iniciaram checkout, mas não compraram.

    jornadas_abandonadas = [
        (
            SESSOES_ABANDONO_VIEW,
            ["product_view"],
        ),
        (
            SESSOES_ABANDONO_CART,
            ["product_view", "add_to_cart"],
        ),
        (
            SESSOES_ABANDONO_CHECKOUT,
            ["product_view", "add_to_cart", "checkout_start"],
        ),
    ]

    for quantidade_sessoes, etapas in jornadas_abandonadas:
        for _ in range(quantidade_sessoes):
            user_id = (
                random.randint(1, 5000)
                if random.random() >= 0.25
                else None
            )

            anonymous_id = (
                anonymous_por_cliente[user_id]
                if user_id is not None
                else str(uuid4())
            )

            instante = DATA_INICIO + timedelta(
                seconds=random.randint(3600, intervalo - 3600)
            )

            gerar_sessao(
                eventos=eventos,
                sessoes=sessoes,
                etapas=etapas,
                primeiro_instante=instante,
                user_id=user_id,
                anonymous_id=anonymous_id,
                device=random.choice(DISPOSITIVOS),
                channel=random.choice(CANAIS),
                sku=f"SKU-{random.randint(1, 200):04d}",
                quantity=random.randint(1, 5),
                price=round(random.uniform(20, 1500), 2),
            )

    # 5. Completa exatamente 300 mil eventos com page_view.
    eventos_base = len(eventos)

    quantidade_duplicados = int(
        TOTAL_EVENTOS * PERCENTUAL_DUPLICADOS
    )

    quantidade_page_views = (
        TOTAL_EVENTOS - eventos_base - quantidade_duplicados
    )

    if quantidade_page_views < 0:
        raise RuntimeError(
            f"Eventos base ({eventos_base}) ultrapassaram o limite."
        )

    print(f"Eventos das jornadas: {eventos_base:,}")
    print(f"Page views adicionais: {quantidade_page_views:,}")

    page_views_por_sessao, resto = divmod(
        quantidade_page_views,
        len(sessoes),
    )

    for indice, sessao in enumerate(sessoes):
        quantidade = page_views_por_sessao + (
            1 if indice < resto else 0
        )

        for pagina in range(quantidade):
            instante = sessao["primeiro_instante"] - timedelta(
                minutes=quantidade - pagina
            )

            evento = criar_evento(
                tipo="page_view",
                instante=instante,
                user_id=sessao["user_id"],
                anonymous_id=sessao["anonymous_id"],
                session_id=sessao["session_id"],
                device=sessao["device"],
                channel=sessao["channel"],
                sku=sessao["sku"],
                quantity=sessao["quantity"],
                price=round(random.uniform(20, 1500), 2),
                data_inicio=DATA_INICIO,
            )

            eventos.append(evento)

    # 6. Adiciona aproximadamente 3% de eventos duplicados.
    indices_origem = random.sample(
        range(len(eventos)),
        quantidade_duplicados
    )

    for indice in indices_origem:
        eventos.append(eventos[indice].copy())

    if len(eventos) != TOTAL_EVENTOS:
            raise RuntimeError(
                f"Quantidade incorreta: {len(eventos)} eventos."
            )

    # 7. Grava os arquivos JSONL separados por data.
    DIRETORIO_SAIDA.mkdir(parents=True, exist_ok=True)

    # Remove somente arquivos eventos_*.jsonl desta pasta local.
    for arquivo in DIRETORIO_SAIDA.glob("eventos_*.jsonl"):
        arquivo.unlink()

    arquivos_abertos = {}

    try:
        for evento in eventos:
            data_evento = datetime.fromisoformat(
                evento["event_timestamp"]
            ).date().isoformat()

            caminho = (
                DIRETORIO_SAIDA /
                f"eventos_{data_evento}.jsonl"
            )

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

    contagem = Counter(
        evento["event_type"] for evento in eventos
    )

    print("\n=== GERAÇÃO CONCLUÍDA ===")
    print(f"Total de eventos: {len(eventos):,}")
    print(f"Total de sessões: {len(sessoes):,}")
    print(f"IDs duplicados introduzidos: {quantidade_duplicados:,}")
    print(f"Pedidos vinculados: {len(pedidos):,}")

    print("\nEventos por tipo:")
    for tipo, total in sorted(contagem.items()):
        print(f"{tipo}: {total:,}")

    print(f"\nDiretório: {DIRETORIO_SAIDA.resolve()}")


if __name__ == "__main__":
    main()
