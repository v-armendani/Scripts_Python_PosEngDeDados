"""Popula a base de origem (PostgreSQL) com dados fictícios.

Subcomandos:
    schema    aplica sql/02_schema.sql (idempotente)
    seed      carga inicial em volume via COPY (recusa se a base já tiver dados)
    simulate  gera INSERT/UPDATE/DELETE contínuos para exercitar o CDC

Exemplos:
    python src/populate.py schema
    python src/populate.py seed --seed 42
    python src/populate.py simulate --interval 1 --ops-per-tick 5

No container do laboratório:
    docker compose exec python python src/populate.py seed

Todos os dados são sintéticos (Faker pt_BR, e-mails em example.com, contas
bancárias inventadas). Os logs mostram apenas contagens, nunca conteúdo de linhas.

Códigos de saída: 0 ok | 1 erro no banco/estado | 2 configuração ou argumento
inválido | 3 Postgres inacessível.
"""

from __future__ import annotations

import argparse
import bisect
import csv
import hashlib
import io
import json
import logging
import math
import os
import random
import sys
import time
import uuid
from collections import Counter
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from faker import Faker

# Adapta uuid.UUID <-> UUID do Postgres nos dois sentidos
psycopg2.extras.register_uuid()

log = logging.getLogger("populate")

PROJECT_ROOT = Path(__file__).resolve().parents[1]

SCHEMA_FILE = PROJECT_ROOT / "sql" / "02_schema.sql"

TABLES = (
    "clientes",
    "fornecedores_legado",
    "produtos_catalogo",
    "pedidos",
    "itens_pedido",
    "transacoes_financeiras",
    "eventos_web",
)

CATEGORIAS: dict[str, tuple[str, ...]] = {
    "Eletrônicos": ("Fone Bluetooth", "Smartwatch", "Carregador USB-C", "Caixa de Som", "Teclado Mecânico"),
    "Casa": ("Jogo de Panelas", "Luminária", "Cafeteira", "Organizador", "Toalha de Banho"),
    "Esporte": ("Tênis de Corrida", "Garrafa Térmica", "Tapete de Yoga", "Halter", "Bicicleta Ergométrica"),
    "Livros": ("Romance", "Livro Técnico", "HQ", "Biografia", "Guia de Viagem"),
    "Moda": ("Camiseta", "Jaqueta", "Mochila", "Boné", "Calça Jeans"),
    "Beleza": ("Perfume", "Hidratante", "Shampoo", "Protetor Solar", "Kit Maquiagem"),
}

# Preço mediano (R$) por categoria. Os preços seguem uma log-normal: a maioria
# fica perto da mediana e poucos itens são bem mais caros (cauda longa).
PRECO_MEDIANO: dict[str, float] = {
    "Eletrônicos": 220.0,
    "Casa": 90.0,
    "Esporte": 120.0,
    "Livros": 45.0,
    "Moda": 80.0,
    "Beleza": 55.0,
}
PRECO_SIGMA = 0.6
PRECO_MIN, PRECO_MAX = 4.90, 4_999.90

STATUS_PEDIDO_PESOS = {"PENDENTE": 10, "PAGO": 20, "ENVIADO": 60, "CANCELADO": 10}
STATUS_CREDITO = ("APROVADO", "EM_ANALISE", "BLOQUEADO")
TIPOS_EVENTO_PESOS = {
    "page_view": 50,
    "search": 15,
    "add_to_cart": 15,
    "checkout": 8,
    "login": 7,
    "logout": 5,
}
DEVICES = ("desktop", "mobile", "tablet")
BROWSERS = ("chrome", "firefox", "safari", "edge")
CONTA_LOJA = "AG0001-CC00000001"

DIAS_HISTORICO = 180

COLUNAS = {
    "fornecedores_legado": ("id_fornecedor", "razao_social", "cnpj", "status_credito", "limite_credito"),
    "produtos_catalogo": ("id_produto", "id_fornecedor", "codigo_sku", "nome_produto", "categoria",
                          "preco_atual", "fl_excluido", "data_exclusao", "data_criacao",
                          "data_ultima_atualizacao"),
    "clientes": ("id_cliente", "nome", "email", "data_criacao", "data_ultima_atualizacao"),
    "pedidos": ("id_pedido", "id_cliente", "data_pedido", "status", "total_pedido", "data_criacao",
                "data_ultima_atualizacao"),
    "itens_pedido": ("id_item", "id_pedido", "id_produto", "quantidade", "preco_unitario", "data_criacao"),
    "transacoes_financeiras": ("id_transacao", "id_pedido", "conta_origem", "conta_destino", "tipo_movimento",
                               "valor", "moeda", "data_transacao", "hash_auditoria", "data_criacao"),
    "eventos_web": ("id_evento", "id_cliente", "timestamp_evento", "tipo_evento", "payload", "data_criacao"),
}


# =============================================================================
# Configuração: lê o .env da raiz do projeto e valida as variáveis.
# Erros de configuração usam ValueError, com mensagem do que falta.
# =============================================================================
def load_env() -> None:
    """Carrega o .env da raiz do projeto. Variáveis já exportadas têm prioridade."""
    env_file = PROJECT_ROOT / ".env"
    if not env_file.is_file():
        raise ValueError(
            f"Arquivo .env não encontrado em {env_file}. "
            "Copie o .env.example para .env e preencha os valores."
        )
    load_dotenv(env_file, override=False)


def require(*names: str) -> dict[str, str]:
    """Retorna as variáveis pedidas; falha listando todas as que faltarem."""
    values = {name: os.environ.get(name, "").strip() for name in names}
    missing = [name for name, value in values.items() if not value]
    if missing:
        raise ValueError(f"Variáveis obrigatórias ausentes no .env: {', '.join(missing)}")
    return values


def postgres_admin_config() -> dict[str, object]:
    """Parâmetros de conexão (psycopg2.connect) com o usuário admin da origem.

    O dicionário contém a senha: nunca logar ou imprimir o dicionário inteiro,
    use describe_postgres() para mensagens.
    """
    load_env()
    env = require("POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_DB", "POSTGRES_PORT")
    try:
        port = int(env["POSTGRES_PORT"])
    except ValueError as exc:
        raise ValueError(f"POSTGRES_PORT deve ser numérico, recebido: {env['POSTGRES_PORT']!r}") from exc
    return {
        # 127.0.0.1 e não "localhost": no Windows, localhost tenta IPv6 antes e
        # cada conexão demora ~2s, já que as portas só escutam em IPv4
        "host": os.environ.get("POSTGRES_HOST", "").strip() or "127.0.0.1",
        "port": port,
        "dbname": env["POSTGRES_DB"],
        "user": env["POSTGRES_USER"],
        "password": env["POSTGRES_PASSWORD"],
        "application_name": "lab_datacollect",
        # Explícito para não depender da code page do Windows (cp1252/cp850)
        "client_encoding": "UTF8",
    }


def describe_postgres(config: dict[str, object]) -> str:
    """Descrição segura da conexão para logs (sem a senha)."""
    return f"{config['host']}:{config['port']}/{config['dbname']} como {config['user']}"


# =============================================================================
# Geração de dados sintéticos
# Todas as funções recebem o gerador aleatório (rng) e o Faker explicitamente:
# com a mesma semente, produzem exatamente os mesmos dados.
# =============================================================================
def criar_geradores(seed: int | None) -> tuple[random.Random, Faker]:
    rng = random.Random(seed)
    fake = Faker("pt_BR")
    if seed is not None:
        fake.seed_instance(seed)
    return rng, fake


def gerar_uuid(rng: random.Random) -> uuid.UUID:
    return uuid.UUID(int=rng.getrandbits(128), version=4)


def gerar_valor(rng: random.Random, min_cents: int, max_cents: int) -> Decimal:
    return Decimal(rng.randint(min_cents, max_cents)) / 100


def gerar_data_entre(rng: random.Random, inicio: datetime, fim: datetime) -> datetime:
    if fim <= inicio:
        return fim
    return inicio + timedelta(seconds=rng.uniform(0, (fim - inicio).total_seconds()))


def gerar_preco(rng: random.Random, categoria: str) -> Decimal:
    bruto = rng.lognormvariate(math.log(PRECO_MEDIANO[categoria]), PRECO_SIGMA)
    bruto = min(max(bruto, PRECO_MIN), PRECO_MAX)
    return Decimal(math.floor(bruto)) + Decimal("0.90")  # preço de varejo: termina em ,90


def sortear_com_peso(rng: random.Random, pesos: dict[str, int]) -> str:
    return rng.choices(list(pesos), weights=list(pesos.values()))[0]


def gerar_email(rng: random.Random, fake: Faker) -> str:
    slug = fake.user_name().lower()[:60]
    return f"{slug}.{gerar_uuid(rng).hex[:8]}@example.com"


def gerar_conta(rng: random.Random) -> str:
    return f"AG{rng.randint(1, 9999):04d}-CC{rng.randint(1, 99_999_999):08d}"


def calcular_hash_auditoria(id_transacao: uuid.UUID, origem: str, destino: str, tipo: str,
                            valor: Decimal, data: datetime) -> str:
    raw = f"{id_transacao}|{origem}|{destino}|{tipo}|{valor:.4f}|{data.isoformat()}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def gerar_fornecedor(rng: random.Random, fake: Faker) -> tuple:
    return (
        f"FORN-{gerar_uuid(rng).hex[:10].upper()}",
        f"{fake.company()} {fake.company_suffix()}"[:150],
        "".join(ch for ch in fake.cnpj() if ch.isdigit()),
        rng.choice(STATUS_CREDITO),
        gerar_valor(rng, 1_000_000, 50_000_000),
    )


def gerar_produto(rng: random.Random, fake: Faker, agora: datetime, id_fornecedor: str,
                  excluido: bool) -> tuple:
    categoria = rng.choice(list(CATEGORIAS))
    base = rng.choice(CATEGORIAS[categoria])
    criacao = gerar_data_entre(rng, agora - timedelta(days=365), agora - timedelta(days=DIAS_HISTORICO))
    exclusao = gerar_data_entre(rng, criacao, agora) if excluido else None
    return (
        gerar_uuid(rng),
        id_fornecedor,
        f"SKU-{gerar_uuid(rng).hex[:10].upper()}",
        f"{base} {fake.word().capitalize()} {rng.randint(100, 999)}"[:100],
        categoria,
        gerar_preco(rng, categoria),
        excluido,
        exclusao,
        criacao,
        exclusao or criacao,
    )


def gerar_cliente(rng: random.Random, fake: Faker, agora: datetime) -> tuple:
    criacao = gerar_data_entre(rng, agora - timedelta(days=365), agora - timedelta(days=30))
    atualizacao = gerar_data_entre(rng, criacao, agora) if rng.random() < 0.2 else criacao
    return (gerar_uuid(rng), fake.name()[:100], gerar_email(rng, fake), criacao, atualizacao)


def gerar_itens(rng: random.Random, id_pedido: uuid.UUID, data: datetime,
                produtos: list[tuple[uuid.UUID, Decimal]]) -> list[tuple]:
    escolhidos = rng.sample(produtos, k=min(len(produtos), rng.randint(1, 5)))
    return [
        (gerar_uuid(rng), id_pedido, id_produto, rng.randint(1, 3), preco, data)
        for id_produto, preco in escolhidos
    ]


def calcular_total(itens: list[tuple]) -> Decimal:
    return sum((item[3] * item[4] for item in itens), Decimal("0.00"))


def gerar_transacao(rng: random.Random, id_pedido: uuid.UUID | None, tipo: str, valor: Decimal,
                    data: datetime) -> tuple:
    id_transacao = gerar_uuid(rng)
    if tipo in ("ESTORNO", "DEBITO"):
        origem, destino = CONTA_LOJA, gerar_conta(rng)
    else:
        origem, destino = gerar_conta(rng), CONTA_LOJA
    valor = valor.quantize(Decimal("0.0001"))
    return (
        id_transacao, id_pedido, origem, destino, tipo, valor, "BRL", data,
        calcular_hash_auditoria(id_transacao, origem, destino, tipo, valor, data), data,
    )


def gerar_evento(rng: random.Random, fake: Faker, id_cliente: uuid.UUID | None, timestamp: datetime,
                 skus: list[str]) -> tuple:
    tipo = sortear_com_peso(rng, TIPOS_EVENTO_PESOS)
    payload: dict[str, object] = {
        "session_id": str(gerar_uuid(rng)),
        "device": rng.choice(DEVICES),
        "browser": rng.choice(BROWSERS),
    }
    if tipo == "search":
        payload["termo_busca"] = fake.word()
        payload["url"] = "/busca"
    elif tipo in ("add_to_cart", "page_view") and skus:
        sku = rng.choice(skus)
        payload["sku"] = sku
        payload["url"] = f"/produto/{sku}"
    else:
        payload["url"] = f"/{tipo}"
    criacao = timestamp + timedelta(seconds=rng.randint(0, 5))
    return (gerar_uuid(rng), id_cliente, timestamp, tipo, json.dumps(payload, ensure_ascii=False), criacao)


# =============================================================================
# Acesso ao banco
# =============================================================================
def verificar_schema(cur) -> None:
    cur.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = 'public' AND table_name = ANY(%s)",
        (list(TABLES),),
    )
    existentes = {row[0] for row in cur.fetchall()}
    faltando = [t for t in TABLES if t not in existentes]
    if faltando:
        raise RuntimeError(
            f"Tabelas ausentes: {', '.join(faltando)}. Rode antes: python src/populate.py schema"
        )


def listar_tabelas_com_dados(cur) -> list[str]:
    com_dados = []
    for table in TABLES:
        cur.execute(f"SELECT EXISTS (SELECT 1 FROM {table})")  # nomes vêm da constante TABLES
        if cur.fetchone()[0]:
            com_dados.append(table)
    return com_dados


def valor_csv(value: object) -> object:
    # Em COPY ... CSV, campo vazio sem aspas = NULL. Nenhuma coluna gerada usa string vazia.
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def copiar_linhas(cur, table: str, rows: list[tuple]) -> None:
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    for row in rows:
        writer.writerow([valor_csv(v) for v in row])
    buffer.seek(0)
    cur.copy_expert(f"COPY {table} ({', '.join(COLUNAS[table])}) FROM STDIN WITH (FORMAT csv)", buffer)
    log.info("  %-24s %8d linhas", table, len(rows))


# =============================================================================
# schema
# =============================================================================
def cmd_schema(conn) -> None:
    if not SCHEMA_FILE.is_file():
        raise RuntimeError(f"Arquivo de schema não encontrado: {SCHEMA_FILE}")
    ddl = SCHEMA_FILE.read_text(encoding="utf-8")
    with conn, conn.cursor() as cur:  # `with conn` = commit no sucesso, rollback no erro
        cur.execute(ddl)
    log.info("Schema aplicado a partir de %s", SCHEMA_FILE.relative_to(PROJECT_ROOT))


# =============================================================================
# seed
# =============================================================================
def cmd_seed(conn, *, fornecedores: int, produtos: int, clientes: int, pedidos: int, eventos: int,
             seed: int | None, append: bool) -> None:
    volumes = {"fornecedores": fornecedores, "produtos": produtos, "clientes": clientes,
               "pedidos": pedidos, "eventos": eventos}
    for nome, valor in volumes.items():
        if valor < 1:
            raise ValueError(f"--{nome} deve ser >= 1 (recebido {valor})")

    rng, fake = criar_geradores(seed)
    agora = datetime.now(UTC)
    inicio_historico = agora - timedelta(days=DIAS_HISTORICO)
    started = time.perf_counter()

    with conn, conn.cursor() as cur:  # tudo em uma transação: falhou, nada é gravado
        verificar_schema(cur)
        com_dados = listar_tabelas_com_dados(cur)
        if com_dados and not append:
            raise RuntimeError(
                f"Tabelas já possuem dados ({', '.join(com_dados)}). "
                "Use --append para adicionar mais linhas, ou recrie o volume para recomeçar."
            )

        linhas_fornecedores = [gerar_fornecedor(rng, fake) for _ in range(fornecedores)]
        ids_fornecedor = [f[0] for f in linhas_fornecedores]

        linhas_produtos = [
            gerar_produto(rng, fake, agora, rng.choice(ids_fornecedor), excluido=rng.random() < 0.05)
            for _ in range(produtos)
        ]
        ativos = [(p[0], p[5]) for p in linhas_produtos if not p[6]] or [(p[0], p[5]) for p in linhas_produtos]
        skus = [p[2] for p in linhas_produtos]

        # Ordenados pela criação para sortear, em cada data, só clientes que já existiam
        linhas_clientes = sorted((gerar_cliente(rng, fake, agora) for _ in range(clientes)), key=lambda c: c[3])
        criacoes = [c[3] for c in linhas_clientes]

        def sortear_cliente_em(ts: datetime) -> tuple[uuid.UUID, datetime]:
            """Sorteia um cliente já cadastrado em `ts`; ajusta a data se ninguém existia."""
            existentes = bisect.bisect_right(criacoes, ts)
            if existentes == 0:
                return linhas_clientes[0][0], criacoes[0]
            return linhas_clientes[rng.randrange(existentes)][0], ts

        linhas_pedidos: list[tuple] = []
        linhas_itens: list[tuple] = []
        linhas_transacoes: list[tuple] = []
        for _ in range(pedidos):
            # Data sorteada primeiro e de forma uniforme: volume mensal estável,
            # sem o crescimento artificial que vinha da data de criação dos clientes
            id_cliente, data_pedido = sortear_cliente_em(gerar_data_entre(rng, inicio_historico, agora))
            status = sortear_com_peso(rng, STATUS_PEDIDO_PESOS)
            id_pedido = gerar_uuid(rng)
            itens_pedido = gerar_itens(rng, id_pedido, data_pedido, ativos)
            total = calcular_total(itens_pedido)
            # Mudança de status acontece em até 10 dias após o pedido. O teto é
            # sorteado (e não truncado em "agora") para não concentrar os pedidos
            # recentes num mesmo timestamp, o que distorceria a carga incremental.
            if status == "PENDENTE":
                atualizacao = data_pedido
            else:
                atualizacao = gerar_data_entre(rng, data_pedido, min(agora, data_pedido + timedelta(days=10)))
            linhas_pedidos.append((id_pedido, id_cliente, data_pedido, status, total, data_pedido, atualizacao))
            linhas_itens.extend(itens_pedido)

            # CANCELADO: metade foi paga e estornada; a outra metade nunca foi paga
            pago = status in ("PAGO", "ENVIADO") or (status == "CANCELADO" and rng.random() < 0.5)
            if pago:
                data_pagamento = gerar_data_entre(rng, data_pedido, atualizacao)
                linhas_transacoes.append(gerar_transacao(rng, id_pedido, "CREDITO", total, data_pagamento))
                if status == "CANCELADO":
                    linhas_transacoes.append(gerar_transacao(rng, id_pedido, "ESTORNO", total, atualizacao))

        for _ in range(max(1, pedidos // 10)):  # movimentos avulsos, sem pedido
            tipo = rng.choice(("CREDITO", "DEBITO"))
            linhas_transacoes.append(gerar_transacao(rng, None, tipo, gerar_valor(rng, 1_000, 500_000),
                                                     gerar_data_entre(rng, inicio_historico, agora)))

        linhas_eventos = []
        for _ in range(eventos):
            if rng.random() < 0.7:
                id_cliente, ts = sortear_cliente_em(gerar_data_entre(rng, inicio_historico, agora))
            else:
                id_cliente, ts = None, gerar_data_entre(rng, inicio_historico, agora)
            linhas_eventos.append(gerar_evento(rng, fake, id_cliente, ts, skus))

        log.info("Inserindo via COPY (seed=%s, append=%s):", seed, append)
        copiar_linhas(cur, "fornecedores_legado", linhas_fornecedores)
        copiar_linhas(cur, "produtos_catalogo", linhas_produtos)
        copiar_linhas(cur, "clientes", linhas_clientes)
        copiar_linhas(cur, "pedidos", linhas_pedidos)
        copiar_linhas(cur, "itens_pedido", linhas_itens)
        copiar_linhas(cur, "transacoes_financeiras", linhas_transacoes)
        copiar_linhas(cur, "eventos_web", linhas_eventos)

    log.info("Seed concluído em %.1fs (commit realizado).", time.perf_counter() - started)


# =============================================================================
# simulate — cada operação recebe (cursor, rng, fake) e retorna True se alterou
# alguma linha, False se não havia linha candidata (ex.: nenhum pedido PENDENTE).
# =============================================================================
def sortear_pedido(cur, status: tuple[str, ...]) -> tuple | None:
    cur.execute(
        "SELECT id_pedido, status, total_pedido FROM pedidos WHERE status = ANY(%s) "
        "ORDER BY random() LIMIT 1 FOR UPDATE SKIP LOCKED",
        (list(status),),
    )
    return cur.fetchone()


def agora_banco(cur) -> datetime:
    """Horário da transação no banco (CURRENT_TIMESTAMP).

    O simulate usa o relógio do banco, e não o do Python, para que as datas
    gravadas pelo script e as preenchidas pelo banco (DEFAULT e gatilho de
    data_ultima_atualizacao) sejam coerentes entre si dentro da transação.
    """
    cur.execute("SELECT CURRENT_TIMESTAMP")
    return cur.fetchone()[0]


def inserir_transacao(cur, row: tuple) -> None:
    colunas = COLUNAS["transacoes_financeiras"]
    cur.execute(
        f"INSERT INTO transacoes_financeiras ({', '.join(colunas)}) VALUES ({', '.join(['%s'] * len(colunas))})",
        row,
    )


def op_insert_cliente(cur, rng: random.Random, fake: Faker) -> bool:
    cur.execute("INSERT INTO clientes (id_cliente, nome, email) VALUES (%s, %s, %s)",
                (gerar_uuid(rng), fake.name()[:100], gerar_email(rng, fake)))
    return True


def op_insert_pedido(cur, rng: random.Random, _fake: Faker) -> bool:
    cur.execute("SELECT id_cliente FROM clientes ORDER BY random() LIMIT 1")
    cliente = cur.fetchone()
    cur.execute("SELECT id_produto, preco_atual FROM produtos_catalogo WHERE NOT fl_excluido "
                "ORDER BY random() LIMIT 5")
    produtos = cur.fetchall()
    if not cliente or not produtos:
        return False
    agora = agora_banco(cur)
    id_pedido = gerar_uuid(rng)
    itens = gerar_itens(rng, id_pedido, agora, produtos)
    cur.execute(
        "INSERT INTO pedidos (id_pedido, id_cliente, data_pedido, status, total_pedido) "
        "VALUES (%s, %s, %s, 'PENDENTE', %s)",
        (id_pedido, cliente[0], agora, calcular_total(itens)),
    )
    cur.executemany(
        "INSERT INTO itens_pedido (id_item, id_pedido, id_produto, quantidade, preco_unitario) "
        "VALUES (%s, %s, %s, %s, %s)",
        [item[:5] for item in itens],
    )
    return True


def op_pagar_pedido(cur, rng: random.Random, _fake: Faker) -> bool:
    pedido = sortear_pedido(cur, ("PENDENTE",))
    if not pedido:
        return False
    cur.execute("UPDATE pedidos SET status = 'PAGO' WHERE id_pedido = %s", (pedido[0],))
    inserir_transacao(cur, gerar_transacao(rng, pedido[0], "CREDITO", pedido[2], agora_banco(cur)))
    return True


def op_enviar_pedido(cur, _rng: random.Random, _fake: Faker) -> bool:
    pedido = sortear_pedido(cur, ("PAGO",))
    if not pedido:
        return False
    cur.execute("UPDATE pedidos SET status = 'ENVIADO' WHERE id_pedido = %s", (pedido[0],))
    return True


def op_cancelar_pedido(cur, rng: random.Random, _fake: Faker) -> bool:
    pedido = sortear_pedido(cur, ("PENDENTE", "PAGO"))
    if not pedido:
        return False
    cur.execute("UPDATE pedidos SET status = 'CANCELADO' WHERE id_pedido = %s", (pedido[0],))
    if pedido[1] == "PAGO":
        inserir_transacao(cur, gerar_transacao(rng, pedido[0], "ESTORNO", pedido[2], agora_banco(cur)))
    return True


def op_insert_evento(cur, rng: random.Random, fake: Faker) -> bool:
    id_cliente = None
    if rng.random() < 0.7:
        cur.execute("SELECT id_cliente FROM clientes ORDER BY random() LIMIT 1")
        row = cur.fetchone()
        id_cliente = row[0] if row else None
    cur.execute("SELECT codigo_sku FROM produtos_catalogo WHERE NOT fl_excluido ORDER BY random() LIMIT 1")
    sku = cur.fetchone()
    evento = gerar_evento(rng, fake, id_cliente, agora_banco(cur), [sku[0]] if sku else [])
    cur.execute(
        "INSERT INTO eventos_web (id_evento, id_cliente, timestamp_evento, tipo_evento, payload) "
        "VALUES (%s, %s, %s, %s, %s::jsonb)",
        evento[:5],
    )
    return True


def op_update_preco_produto(cur, rng: random.Random, _fake: Faker) -> bool:
    fator = Decimal(str(round(rng.uniform(0.85, 1.15), 4)))
    cur.execute(
        "UPDATE produtos_catalogo SET preco_atual = GREATEST(ROUND(preco_atual * %s, 2), 0.01) "
        "WHERE id_produto = (SELECT id_produto FROM produtos_catalogo WHERE NOT fl_excluido "
        "ORDER BY random() LIMIT 1)",
        (fator,),
    )
    return cur.rowcount > 0


def op_update_cliente(cur, rng: random.Random, fake: Faker) -> bool:
    if rng.random() < 0.5:
        coluna, valor = "nome", fake.name()[:100]
    else:
        coluna, valor = "email", gerar_email(rng, fake)
    cur.execute(
        f"UPDATE clientes SET {coluna} = %s "  # coluna vem de valor fixo acima
        "WHERE id_cliente = (SELECT id_cliente FROM clientes ORDER BY random() LIMIT 1)",
        (valor,),
    )
    return cur.rowcount > 0


def op_update_fornecedor(cur, rng: random.Random, _fake: Faker) -> bool:
    cur.execute(
        "UPDATE fornecedores_legado SET status_credito = %s, limite_credito = %s "
        "WHERE id_fornecedor = (SELECT id_fornecedor FROM fornecedores_legado ORDER BY random() LIMIT 1)",
        (rng.choice(STATUS_CREDITO), gerar_valor(rng, 1_000_000, 50_000_000)),
    )
    return cur.rowcount > 0


def op_soft_delete_produto(cur, _rng: random.Random, _fake: Faker) -> bool:
    cur.execute(
        "UPDATE produtos_catalogo SET fl_excluido = TRUE, data_exclusao = CURRENT_TIMESTAMP "
        "WHERE id_produto = (SELECT id_produto FROM produtos_catalogo WHERE NOT fl_excluido "
        "ORDER BY random() LIMIT 1)"
    )
    return cur.rowcount > 0


def op_delete_pedido_cancelado(cur, _rng: random.Random, _fake: Faker) -> bool:
    # Só pedidos sem transação: a FK de transacoes_financeiras não tem cascade.
    # Os itens do pedido são removidos pelo ON DELETE CASCADE.
    cur.execute(
        "DELETE FROM pedidos WHERE id_pedido = ("
        "  SELECT p.id_pedido FROM pedidos p WHERE p.status = 'CANCELADO' "
        "  AND NOT EXISTS (SELECT 1 FROM transacoes_financeiras t WHERE t.id_pedido = p.id_pedido) "
        "  ORDER BY random() LIMIT 1)"
    )
    return cur.rowcount > 0


def op_delete_evento_antigo(cur, _rng: random.Random, _fake: Faker) -> bool:
    cur.execute(
        "DELETE FROM eventos_web WHERE id_evento = "
        "(SELECT id_evento FROM eventos_web ORDER BY timestamp_evento LIMIT 1)"
    )
    return cur.rowcount > 0


Operacao = Callable[..., bool]

# nome -> (função, peso no sorteio)
OPERACOES: dict[str, tuple[Operacao, int]] = {
    "insert_cliente": (op_insert_cliente, 10),
    "insert_pedido": (op_insert_pedido, 20),
    "pagar_pedido": (op_pagar_pedido, 15),
    "enviar_pedido": (op_enviar_pedido, 12),
    "cancelar_pedido": (op_cancelar_pedido, 5),
    "insert_evento": (op_insert_evento, 20),
    "update_preco_produto": (op_update_preco_produto, 6),
    "update_cliente": (op_update_cliente, 4),
    "update_fornecedor": (op_update_fornecedor, 3),
    "soft_delete_produto": (op_soft_delete_produto, 1),
    "delete_pedido_cancelado": (op_delete_pedido_cancelado, 2),
    "delete_evento_antigo": (op_delete_evento_antigo, 2),
}


def log_resumo(ticks: int, aplicadas: Counter, ignoradas: Counter) -> None:
    log.info("Resumo: %d ticks, %d operações aplicadas, %d ignoradas (sem linha candidata).",
             ticks, sum(aplicadas.values()), sum(ignoradas.values()))
    for nome in OPERACOES:
        if aplicadas[nome] or ignoradas[nome]:
            log.info("  %-24s aplicadas=%-6d ignoradas=%d", nome, aplicadas[nome], ignoradas[nome])


def cmd_simulate(conn, *, interval: float, ops_per_tick: int, max_ticks: int | None, seed: int | None) -> None:
    if interval < 0 or ops_per_tick < 1 or (max_ticks is not None and max_ticks < 1):
        raise ValueError("--interval >= 0, --ops-per-tick >= 1 e --max-ticks >= 1")

    rng, fake = criar_geradores(seed)
    nomes = list(OPERACOES)
    pesos = [OPERACOES[n][1] for n in nomes]
    aplicadas: Counter = Counter()
    ignoradas: Counter = Counter()
    ticks = 0

    with conn, conn.cursor() as cur:
        verificar_schema(cur)

    log.info("Simulação iniciada (interval=%.2fs, ops/tick=%d, max_ticks=%s). Ctrl+C para parar.",
             interval, ops_per_tick, max_ticks or "∞")
    try:
        while max_ticks is None or ticks < max_ticks:
            tick_aplicadas: Counter = Counter()
            tick_ignoradas: Counter = Counter()
            op_atual = ""
            try:
                with conn, conn.cursor() as cur:  # 1 transação por tick (rollback se falhar)
                    for op_atual in rng.choices(nomes, weights=pesos, k=ops_per_tick):
                        funcao = OPERACOES[op_atual][0]
                        if funcao(cur, rng, fake):
                            tick_aplicadas[op_atual] += 1
                        else:
                            tick_ignoradas[op_atual] += 1
            except psycopg2.Error:
                log.error("Tick %d falhou na operação '%s' — rollback do tick.", ticks + 1, op_atual)
                raise
            # Só contabiliza depois do commit do tick
            ticks += 1
            aplicadas.update(tick_aplicadas)
            ignoradas.update(tick_ignoradas)
            partes = [f"{k}={v}" for k, v in sorted(tick_aplicadas.items())]
            partes += [f"{k} (ignorada)={v}" for k, v in sorted(tick_ignoradas.items())]
            log.info("Tick %d: %s", ticks, ", ".join(partes))
            if interval:
                time.sleep(interval)
    except KeyboardInterrupt:
        log.info("Interrompido pelo usuário (transação do tick em andamento foi descartada).")
    finally:
        log_resumo(ticks, aplicadas, ignoradas)


# =============================================================================
# CLI
# =============================================================================
def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Popula a base de origem do laboratório.")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("schema", help="aplica sql/02_schema.sql (idempotente)")

    seed = sub.add_parser("seed", help="carga inicial em volume")
    seed.add_argument("--fornecedores", type=int, default=50)
    seed.add_argument("--produtos", type=int, default=500)
    seed.add_argument("--clientes", type=int, default=2_000)
    seed.add_argument("--pedidos", type=int, default=10_000)
    seed.add_argument("--eventos", type=int, default=50_000)
    seed.add_argument("--seed", type=int, default=None, help="semente para dados reproduzíveis")
    seed.add_argument("--append", action="store_true", help="permite inserir mesmo com dados existentes")

    sim = sub.add_parser("simulate", help="mutações contínuas para exercitar o CDC")
    sim.add_argument("--interval", type=float, default=1.0, help="segundos entre ticks")
    sim.add_argument("--ops-per-tick", type=int, default=5)
    sim.add_argument("--max-ticks", type=int, default=None, help="padrão: infinito")
    sim.add_argument("--seed", type=int, default=None)
    return parser


def main(argv: list[str] | None = None) -> int:
    # No Windows, pipes/redirecionamentos usam a code page local e quebram acentos
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):  # Jupyter/IDEs podem trocar o stream
            stream.reconfigure(encoding="utf-8")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(message)s")
    args = build_parser().parse_args(argv)

    conn = None
    try:
        config = postgres_admin_config()
        log.info("Conectando em %s", describe_postgres(config))
        conn = psycopg2.connect(**config)
        if args.command == "schema":
            cmd_schema(conn)
        elif args.command == "seed":
            cmd_seed(conn, fornecedores=args.fornecedores, produtos=args.produtos, clientes=args.clientes,
                     pedidos=args.pedidos, eventos=args.eventos, seed=args.seed, append=args.append)
        elif args.command == "simulate":
            cmd_simulate(conn, interval=args.interval, ops_per_tick=args.ops_per_tick,
                         max_ticks=args.max_ticks, seed=args.seed)
    except ValueError as exc:  # configuração ou argumento inválido
        log.error("%s", exc)
        return 2
    except psycopg2.OperationalError as exc:
        log.error("Falha de conexão/operação no Postgres: %s", str(exc).strip())
        return 3
    except psycopg2.Error as exc:
        # message_primary não inclui o DETAIL, que pode conter valores das linhas
        primary = exc.diag.message_primary or type(exc).__name__
        log.error("Erro no Postgres (%s, sqlstate=%s): %s — transação desfeita.",
                  type(exc).__name__, exc.pgcode, primary)
        return 1
    except RuntimeError as exc:  # estado inválido (schema ausente, base já populada)
        log.error("%s", exc)
        return 1
    finally:
        if conn is not None:
            conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
