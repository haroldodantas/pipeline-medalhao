
import os
import random
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP

import mysql.connector
from faker import Faker

fake = Faker("pt_BR")
Faker.seed(42)
random.seed(42)

TOTAL_CLIENTES = 5_000
TOTAL_PEDIDOS = 40_000
DIAS_HISTORICO = 90
TAMANHO_LOTE = 1_000

CATEGORIAS = [
    "Informática",
    "Celulares",
    "Áudio",
    "Periféricos",
    "Games",
    "Acessórios",
]

MARCAS = ["TechOne", "NovaTech", "BytePro", "SmartWave", "VisionX"]
CANAIS = ["site", "app", "marketplace", "loja"]
STATUS = ["pago", "enviado", "entregue", "cancelado", "pendente"]


def conectar():
    """Conecta ao MySQL usando as variáveis do ambiente."""
    return mysql.connector.connect(
        host=os.getenv("MYSQL_HOST", "127.0.0.1"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        user=os.getenv("MYSQL_USER", "etl_user"),
        password=os.getenv("MYSQL_PASSWORD", "change_me_local"),
        database=os.getenv("MYSQL_DATABASE", "techflow_oltp"),
    )


def decimal_brl(valor):
    return Decimal(str(valor)).quantize(
        Decimal("0.01"), rounding=ROUND_HALF_UP
    )


def lotes(registros, tamanho=TAMANHO_LOTE):
    for inicio in range(0, len(registros), tamanho):
        yield registros[inicio:inicio + tamanho]


def gerar_clientes(cursor):
    agora = datetime.now()
    inicio = agora - timedelta(days=DIAS_HISTORICO)

    registros = []
    emails = []

    for cliente_id in range(1, TOTAL_CLIENTES + 1):
        email = fake.unique.email().lower()
        emails.append(email)

        criado = fake.date_time_between(
            start_date=inicio, end_date=agora
        )

        registros.append((
            cliente_id,
            fake.name()[:150],
            email,
            fake.cpf(),
            fake.date_of_birth(minimum_age=18, maximum_age=85),
            fake.city()[:100],
            fake.estado_sigla(),
            criado,
            criado,
        ))

    # Duplicação intencional de e-mail em registros diferentes.
    for indice in random.sample(range(TOTAL_CLIENTES), 50):
        registro = list(registros[indice])
        registro[2] = emails[random.randrange(TOTAL_CLIENTES)]
        registros[indice] = tuple(registro)

    sql = """
        INSERT INTO clientes
        (id, nome, email, cpf, data_nascimento, cidade, uf,
         criado_em, atualizado_em)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    for lote in lotes(registros):
        cursor.executemany(sql, lote)

    return registros


def gerar_produtos(cursor):
    registros = []

    for numero in range(1, 201):
        sku = f"SKU-{numero:04d}"
        criado = datetime.now() - timedelta(days=random.randint(90, 365))

        registros.append((
            sku,
            f"{random.choice(MARCAS)} Produto {numero}",
            random.choice(CATEGORIAS),
            random.choice(MARCAS),
            random.choice([True, True, True, False]),
            criado,
            criado,
        ))

    sql = """
        INSERT INTO produtos
        (sku, nome, categoria, marca, ativo, criado_em, atualizado_em)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """

    for lote in lotes(registros):
        cursor.executemany(sql, lote)

    return [registro[0] for registro in registros]


def gerar_enderecos(cursor):
    registros = []

    for endereco_id in range(1, TOTAL_CLIENTES + 1):
        uf = fake.estado_sigla()
        registros.append((
            endereco_id,
            random.randint(1, TOTAL_CLIENTES),
            fake.postcode()[:9],
            fake.street_name()[:200],
            fake.city()[:100],
            uf,
            random.choice(["residencial", "comercial"]),
            datetime.now() - timedelta(days=random.randint(0, 90)),
        ))

    sql = """
        INSERT INTO enderecos
        (id, cliente_id, cep, logradouro, cidade, uf, tipo, criado_em)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """

    for lote in lotes(registros):
        cursor.executemany(sql, lote)


def gerar_pedidos(cursor):
    agora = datetime.now()
    inicio = agora - timedelta(days=DIAS_HISTORICO)
    registros = []

    for pedido_id in range(1, TOTAL_PEDIDOS + 1):
        criado = fake.date_time_between(
            start_date=inicio, end_date=agora
        )
        atualizado = criado + timedelta(
            seconds=random.randint(0, max(1, int((agora - criado).total_seconds())))
        )

        registros.append((
            pedido_id,
            random.randint(1, TOTAL_CLIENTES),
            random.choice(STATUS),
            random.choice(CANAIS),
            decimal_brl(random.uniform(0, 60)),
            random.choice([None, None, None, "BEMVINDO", "TECH10"]),
            criado,
            atualizado,
        ))

    sql = """
        INSERT INTO pedidos
        (id, cliente_id, status, canal, valor_frete, cupom,
         criado_em, atualizado_em)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """

    for lote in lotes(registros):
        cursor.executemany(sql, lote)


def gerar_itens(cursor, skus):
    registros = []
    item_id = 1

    for pedido_id in range(1, TOTAL_PEDIDOS + 1):
        for _ in range(random.randint(1, 4)):
            sku = random.choice(skus)
            quantidade = random.randint(1, 3)
            preco = decimal_brl(random.uniform(30, 8000))
            desconto = decimal_brl(
                preco * Decimal(str(random.choice([0, 0, 0.05, 0.10])))
            )

            # Aproximadamente 2% dos itens terão preço inválido.
            if random.random() < 0.02:
                preco = random.choice([None, Decimal("-10.00")])

            registros.append((
                item_id,
                pedido_id,
                sku,
                quantidade,
                preco,
                desconto,
            ))
            item_id += 1

    sql = """
        INSERT INTO itens_pedido
        (id, pedido_id, sku, quantidade, preco_unitario, desconto)
        VALUES (%s, %s, %s, %s, %s, %s)
    """

    for lote in lotes(registros):
        cursor.executemany(sql, lote)

    return len(registros)


def main():
    conexao = conectar()
    cursor = conexao.cursor()

    try:
        # O gerador não apaga tabelas existentes.
        # Use um banco vazio para a primeira execução.
        cursor.execute("SELECT COUNT(*) FROM clientes")
        if cursor.fetchone()[0] > 0:
            raise RuntimeError(
                "A tabela clientes já contém dados. "
                "O gerador foi interrompido para evitar duplicações."
            )

        print("Gerando produtos...")
        skus = gerar_produtos(cursor)

        print("Gerando clientes...")
        gerar_clientes(cursor)

        print("Gerando endereços...")
        gerar_enderecos(cursor)

        print("Gerando pedidos...")
        gerar_pedidos(cursor)

        print("Gerando itens dos pedidos...")
        total_itens = gerar_itens(cursor, skus)

        conexao.commit()

        print("\nGeração concluída:")
        print(f"Clientes: {TOTAL_CLIENTES}")
        print(f"Endereços: {TOTAL_CLIENTES}")
        print(f"Produtos: {len(skus)}")
        print(f"Pedidos: {TOTAL_PEDIDOS}")
        print(f"Itens: {total_itens}")

    except Exception:
        conexao.rollback()
        raise
    finally:
        cursor.close()
        conexao.close()


if __name__ == "__main__":
    main()
