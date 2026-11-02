import json
from pathlib import Path
from collections import Counter

diretorio = Path("landing/clickstream")

eventos = []

for arquivo in diretorio.glob("*.jsonl"):
    with arquivo.open("r", encoding="utf-8") as f:
        for linha in f:
            eventos.append(json.loads(linha))

ids = [e["event_id"] for e in eventos]
contagem_ids = Counter(ids)

compras = [
    e for e in eventos
    if e["event_type"] == "purchase"
]

pedidos = {
    e["properties"].get("pedido_id")
    for e in compras
    if e["properties"].get("pedido_id") is not None
}

print("=== VALIDAÇÃO CLICKSTREAM ===")
print(f"Total de eventos: {len(eventos):,}")
print(f"IDs únicos: {len(contagem_ids):,}")
print(f"IDs repetidos: {sum(v - 1 for v in contagem_ids.values()):,}")
print(f"Eventos purchase: {len(compras):,}")
print(f"Compras com pedido_id: {len(pedidos):,}")
print(f"Pedidos únicos vinculados: {len(pedidos):,}")

print("\nEventos por tipo:")
for tipo, total in sorted(
    Counter(e["event_type"] for e in eventos).items()
):
    print(f"{tipo}: {total:,}")