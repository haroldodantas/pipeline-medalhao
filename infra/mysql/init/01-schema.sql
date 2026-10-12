
CREATE TABLE IF NOT EXISTS clientes (
    id BIGINT UNSIGNED NOT NULL,
    nome VARCHAR(150) NOT NULL,
    email VARCHAR(255),
    cpf VARCHAR(14),
    data_nascimento DATE,
    cidade VARCHAR(100),
    uf CHAR(2),
    criado_em DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    atualizado_em DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
        ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    INDEX idx_clientes_email (email),
    INDEX idx_clientes_atualizado_em (atualizado_em)
);

CREATE TABLE IF NOT EXISTS enderecos (
    id BIGINT UNSIGNED NOT NULL,
    cliente_id BIGINT UNSIGNED NOT NULL,
    cep VARCHAR(9),
    logradouro VARCHAR(200),
    cidade VARCHAR(100),
    uf CHAR(2),
    tipo VARCHAR(30),
    criado_em DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    CONSTRAINT fk_enderecos_clientes
        FOREIGN KEY (cliente_id) REFERENCES clientes(id),
    INDEX idx_enderecos_cliente_id (cliente_id)
);

CREATE TABLE IF NOT EXISTS produtos (
    sku VARCHAR(40) NOT NULL,
    nome VARCHAR(200) NOT NULL,
    categoria VARCHAR(100),
    marca VARCHAR(100),
    ativo BOOLEAN NOT NULL DEFAULT TRUE,
    criado_em DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    atualizado_em DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
        ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (sku),
    INDEX idx_produtos_categoria (categoria),
    INDEX idx_produtos_atualizado_em (atualizado_em)
);

CREATE TABLE IF NOT EXISTS pedidos (
    id BIGINT UNSIGNED NOT NULL,
    cliente_id BIGINT UNSIGNED NOT NULL,
    status VARCHAR(30) NOT NULL,
    canal VARCHAR(50),
    valor_frete DECIMAL(18,2) NOT NULL DEFAULT 0.00,
    cupom VARCHAR(50),
    criado_em DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    atualizado_em DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
        ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    CONSTRAINT fk_pedidos_clientes
        FOREIGN KEY (cliente_id) REFERENCES clientes(id),
    INDEX idx_pedidos_cliente_id (cliente_id),
    INDEX idx_pedidos_atualizado_em (atualizado_em),
    INDEX idx_pedidos_criado_em (criado_em)
);

CREATE TABLE IF NOT EXISTS itens_pedido (
    id BIGINT UNSIGNED NOT NULL,
    pedido_id BIGINT UNSIGNED NOT NULL,
    sku VARCHAR(40) NOT NULL,
    quantidade INT NOT NULL,
    preco_unitario DECIMAL(18,2),
    desconto DECIMAL(18,2) NOT NULL DEFAULT 0.00,
    PRIMARY KEY (id),
    CONSTRAINT fk_itens_pedido_pedidos
        FOREIGN KEY (pedido_id) REFERENCES pedidos(id),
    CONSTRAINT fk_itens_pedido_produtos
        FOREIGN KEY (sku) REFERENCES produtos(sku),
    INDEX idx_itens_pedido_pedido_id (pedido_id),
    INDEX idx_itens_pedido_sku (sku)
);
