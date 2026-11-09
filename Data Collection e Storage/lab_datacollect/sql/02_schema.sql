-- =============================================================================
-- Schema da base de origem (lab_source)
-- Executado automaticamente na 1ª inicialização do Postgres (pasta sql/ montada
-- em /docker-entrypoint-initdb.d), depois do 01_cdc_setup.sh.
-- Idempotente: também pode ser reaplicado com `populate schema` sem perder dados.
-- =============================================================================

-- uuid_generate_v4() vem da extensão uuid-ossp
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- 1. Tabela de Clientes (Unifica Pedidos e Telemetria)
CREATE TABLE IF NOT EXISTS clientes (
    id_cliente UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    nome VARCHAR(100) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    data_criacao TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    data_ultima_atualizacao TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- 2. Tabela de Fornecedores (Relacionada ao Catálogo)
-- Sem coluna de atualização de propósito: simula um sistema legado que só
-- permite carga completa no batch; as mudanças dela só aparecem via CDC.
CREATE TABLE IF NOT EXISTS fornecedores_legado (
    id_fornecedor VARCHAR(50) PRIMARY KEY,
    razao_social VARCHAR(150) NOT NULL,
    cnpj VARCHAR(14) NOT NULL,
    status_credito VARCHAR(20),
    limite_credito NUMERIC(12, 2)
);

-- 3. Catálogo de Produtos (Relacionado ao Fornecedor)
CREATE TABLE IF NOT EXISTS produtos_catalogo (
    id_produto UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    id_fornecedor VARCHAR(50) REFERENCES fornecedores_legado(id_fornecedor),
    codigo_sku VARCHAR(50) UNIQUE NOT NULL,
    nome_produto VARCHAR(100) NOT NULL,
    categoria VARCHAR(50),
    preco_atual NUMERIC(10, 2) NOT NULL,
    fl_excluido BOOLEAN NOT NULL DEFAULT FALSE,
    data_exclusao TIMESTAMPTZ,
    data_criacao TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    data_ultima_atualizacao TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- 4. Pedidos (Relacionado ao Cliente)
CREATE TABLE IF NOT EXISTS pedidos (
    id_pedido UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    id_cliente UUID NOT NULL REFERENCES clientes(id_cliente),
    data_pedido TIMESTAMPTZ NOT NULL,
    status VARCHAR(20) CHECK (status IN ('PENDENTE', 'PAGO', 'ENVIADO', 'CANCELADO')),
    total_pedido NUMERIC(10, 2) NOT NULL,
    data_criacao TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    data_ultima_atualizacao TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- 5. Itens do Pedido
CREATE TABLE IF NOT EXISTS itens_pedido (
    id_item UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    id_pedido UUID NOT NULL REFERENCES pedidos(id_pedido) ON DELETE CASCADE,
    id_produto UUID NOT NULL REFERENCES produtos_catalogo(id_produto),
    quantidade INT NOT NULL CHECK (quantidade > 0),
    preco_unitario NUMERIC(10, 2) NOT NULL,
    data_criacao TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- 6. Transações Financeiras (Relacionado opcionalmente ao Pedido)
CREATE TABLE IF NOT EXISTS transacoes_financeiras (
    id_transacao UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    id_pedido UUID REFERENCES pedidos(id_pedido),
    conta_origem VARCHAR(50) NOT NULL,
    conta_destino VARCHAR(50) NOT NULL,
    tipo_movimento VARCHAR(10) CHECK (tipo_movimento IN ('CREDITO', 'DEBITO', 'ESTORNO')),
    valor NUMERIC(15, 4) NOT NULL CHECK (valor > 0),
    moeda VARCHAR(3) DEFAULT 'BRL',
    data_transacao TIMESTAMPTZ NOT NULL,
    hash_auditoria VARCHAR(256) NOT NULL,
    data_criacao TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- 7. Eventos Web (Telemetria)
CREATE TABLE IF NOT EXISTS eventos_web (
    id_evento UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    id_cliente UUID REFERENCES clientes(id_cliente),
    timestamp_evento TIMESTAMPTZ NOT NULL,
    tipo_evento VARCHAR(50) NOT NULL,
    payload JSONB NOT NULL,
    data_criacao TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- -----------------------------------------------------------------------------
-- data_ultima_atualizacao automática em UPDATE
-- Sem isso a coluna só recebe o DEFAULT no INSERT e a extração incremental
-- (cenário 1) não enxerga as alterações.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_set_data_ultima_atualizacao()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.data_ultima_atualizacao := CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE TRIGGER trg_clientes_atualizacao
    BEFORE UPDATE ON clientes
    FOR EACH ROW EXECUTE FUNCTION fn_set_data_ultima_atualizacao();

CREATE OR REPLACE TRIGGER trg_produtos_catalogo_atualizacao
    BEFORE UPDATE ON produtos_catalogo
    FOR EACH ROW EXECUTE FUNCTION fn_set_data_ultima_atualizacao();

CREATE OR REPLACE TRIGGER trg_pedidos_atualizacao
    BEFORE UPDATE ON pedidos
    FOR EACH ROW EXECUTE FUNCTION fn_set_data_ultima_atualizacao();

-- -----------------------------------------------------------------------------
-- REPLICA IDENTITY FULL: o WAL guarda a linha anterior completa em UPDATE e
-- DELETE (o "before" do CDC), não só a chave primária. Custa mais WAL.
-- -----------------------------------------------------------------------------
ALTER TABLE clientes               REPLICA IDENTITY FULL;
ALTER TABLE fornecedores_legado    REPLICA IDENTITY FULL;
ALTER TABLE produtos_catalogo      REPLICA IDENTITY FULL;
ALTER TABLE pedidos                REPLICA IDENTITY FULL;
ALTER TABLE itens_pedido           REPLICA IDENTITY FULL;
ALTER TABLE transacoes_financeiras REPLICA IDENTITY FULL;
ALTER TABLE eventos_web            REPLICA IDENTITY FULL;
