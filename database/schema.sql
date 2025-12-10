-- Estrutura do banco de dados CNPJ
CREATE DATABASE IF NOT EXISTS cnpj_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE cnpj_db;

-- Tabela de empresas (dados básicos)
CREATE TABLE IF NOT EXISTS empresas (
    cnpj_basico VARCHAR(8) PRIMARY KEY,
    razao_social VARCHAR(150),
    nat_juridica VARCHAR(4),
    qualif_resp VARCHAR(2),
    cap_social DECIMAL(18,2),
    porte VARCHAR(2),
    ente_fed_resp VARCHAR(1),
    data_inicio_atividade DATE,
    situacao_cadastral VARCHAR(2),
    INDEX idx_situacao (situacao_cadastral)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Tabela de estabelecimentos (dados específicos)
CREATE TABLE IF NOT EXISTS estabelecimentos (
    cnpj_basico VARCHAR(8),
    cnpj_ordem VARCHAR(4),
    cnpj_dv VARCHAR(2),
    identificador_matriz_filial VARCHAR(1),
    nome_fantasia VARCHAR(55),
    situacao_cadastral VARCHAR(2),
    motivo_situacao_cadastral VARCHAR(2),
    data_situacao_cadastral DATE,
    codigo_municipio VARCHAR(4),
    uf VARCHAR(2),
    cnae_fiscal_principal VARCHAR(7),
    cnae_fiscal_secundaria TEXT,
    logradouro VARCHAR(60),
    numero VARCHAR(10),
    complemento VARCHAR(60),
    bairro VARCHAR(60),
    cep VARCHAR(8),
    ddd_1 VARCHAR(4),
    telefone_1 VARCHAR(9),
    ddd_2 VARCHAR(4),
    telefone_2 VARCHAR(9),
    correio_eletronico VARCHAR(115),
    indicador_mei VARCHAR(1),
    PRIMARY KEY (cnpj_basico, cnpj_ordem, cnpj_dv),
    INDEX idx_cnpj_completo (cnpj_basico, cnpj_ordem, cnpj_dv),
    INDEX idx_uf (uf),
    INDEX idx_situacao (situacao_cadastral),
    INDEX idx_municipio (codigo_municipio),
    FOREIGN KEY (cnpj_basico) REFERENCES empresas(cnpj_basico) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Tabelas de descrições (para decodificação)
CREATE TABLE IF NOT EXISTS natureza_juridica_descricoes (
    codigo VARCHAR(4) PRIMARY KEY,
    descricao VARCHAR(150)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS qualificacao_responsavel_descricoes (
    codigo VARCHAR(2) PRIMARY KEY,
    descricao VARCHAR(150)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS cnae_descricoes (
    codigo VARCHAR(7) PRIMARY KEY,
    descricao VARCHAR(150)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS municipios (
    codigo VARCHAR(4) PRIMARY KEY,
    descricao VARCHAR(100)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Índices adicionais para performance
CREATE INDEX idx_estabelecimentos_situacao_motivo ON estabelecimentos(situacao_cadastral, motivo_situacao_cadastral);
CREATE INDEX idx_estabelecimentos_uf_municipio ON estabelecimentos(uf, codigo_municipio);

-- View para consulta completa de CNPJ
CREATE VIEW vw_cnpj_completo AS
SELECT 
    CONCAT(e.cnpj_basico, est.cnpj_ordem, est.cnpj_dv) AS cnpj_completo,
    e.razao_social,
    est.nome_fantasia,
    CASE est.situacao_cadastral
        WHEN '1' THEN 'NULA'
        WHEN '2' THEN 'ATIVA'
        WHEN '3' THEN 'SUSPENSA'
        WHEN '4' THEN 'INAPTA'
        WHEN '8' THEN 'BAIXADA'
        ELSE 'DESCONHECIDA'
    END AS situacao_descricao,
    est.situacao_cadastral,
    est.motivo_situacao_cadastral,
    est.uf,
    m.descricao AS municipio,
    est.cnae_fiscal_principal,
    cnae.descricao AS cnae_descricao,
    est.data_situacao_cadastral
FROM empresas e
JOIN estabelecimentos est ON e.cnpj_basico = est.cnpj_basico
LEFT JOIN municipios m ON est.codigo_municipio = m.codigo
LEFT JOIN cnae_descricoes cnae ON est.cnae_fiscal_principal = cnae.codigo;