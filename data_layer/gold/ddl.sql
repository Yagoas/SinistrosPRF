-- Inicialização do Banco de Dados para Sinistros PRF

------------------------------ SCHEMA GOLD ------------------------------

CREATE SCHEMA IF NOT EXISTS dw;

-- Dimensão Temporal
DROP TABLE IF EXISTS dw.dim_temporal CASCADE;
CREATE TABLE dw.dim_temporal (
    srk_tmp SERIAL PRIMARY KEY,
    tmp_dta DATE NOT NULL,
    tmp_ano INTEGER NOT NULL,
    tmp_hra INTEGER,
    tmp_dsm VARCHAR(13),
    tmp_per VARCHAR(9),
    tmp_psm VARCHAR(15)
);

-- Dimensão Localização
DROP TABLE IF EXISTS dw.dim_localizacao CASCADE;
CREATE TABLE dw.dim_localizacao (
    srk_loc SERIAL PRIMARY KEY,
    loc_uf VARCHAR(2) NOT NULL,
    loc_ldd VARCHAR(19),
    loc_reg VARCHAR(12),
    loc_mun VARCHAR(100),
    loc_rod VARCHAR(6),
    loc_nrd VARCHAR(5),
    loc_km DECIMAL(10,2),
    loc_lat DECIMAL(12,10),
    loc_lng DECIMAL(12,10)
);

-- Dimensão Sinistro
DROP TABLE IF EXISTS dw.dim_sinistro CASCADE;
CREATE TABLE dw.dim_sinistro (
    srk_sns SERIAL PRIMARY KEY,
    sns_tip VARCHAR(50),
    sns_cau VARCHAR(100),
    sns_cap VARCHAR(5),
    sns_ord INTEGER,
    sns_grv VARCHAR(20)
);

-- Dimensão Via
DROP TABLE IF EXISTS dw.dim_via CASCADE;
CREATE TABLE dw.dim_via (
    srk_via SERIAL PRIMARY KEY,
    via_cmt VARCHAR(30),
    via_tip VARCHAR(20),
    via_tra VARCHAR(150),
    via_sen VARCHAR(20),
    via_uso VARCHAR(6)
);

-- Dimensão Pessoa
DROP TABLE IF EXISTS dw.dim_pessoa CASCADE;
CREATE TABLE dw.dim_pessoa (
    srk_pes SERIAL PRIMARY KEY,
    pes_tip VARCHAR(20),
    pes_sex VARCHAR(20),
    pes_idd INTEGER,
    pes_fxa VARCHAR(13),
    pes_fxc VARCHAR(20),
    pes_esf VARCHAR(20)
);

-- Dimensão Veículo
DROP TABLE IF EXISTS dw.dim_veiculo CASCADE;
CREATE TABLE dw.dim_veiculo (
    srk_vei SERIAL PRIMARY KEY,
    vei_id BIGINT,
    vei_tip VARCHAR(20),
    vei_mrc VARCHAR(50),
    vei_ano INTEGER
);

-- Tabela Fato Sinistros
DROP TABLE IF EXISTS dw.fat_sinistro CASCADE;
CREATE TABLE dw.fat_sinistro (
    sns_id BIGINT NOT NULL,
    srk_tmp INTEGER REFERENCES dw.dim_temporal(srk_tmp),
    srk_loc INTEGER REFERENCES dw.dim_localizacao(srk_loc),
    srk_sns INTEGER REFERENCES dw.dim_sinistro(srk_sns),
    srk_via INTEGER REFERENCES dw.dim_via(srk_via),
    srk_pes INTEGER REFERENCES dw.dim_pessoa(srk_pes),
    srk_vei INTEGER REFERENCES dw.dim_veiculo(srk_vei),
    fat_ils INTEGER DEFAULT 0,
    fat_fle INTEGER DEFAULT 0,
    fat_fgr INTEGER DEFAULT 0,
    fat_fer INTEGER DEFAULT 0,
    fat_mrt INTEGER DEFAULT 0
);

-- Índices para performance nas dimensões
CREATE INDEX idx_dim_temporal_dta ON dw.dim_temporal(tmp_dta);
CREATE INDEX idx_dim_localizacao_uf ON dw.dim_localizacao(loc_uf);
CREATE INDEX idx_dim_localizacao_reg ON dw.dim_localizacao(loc_reg);

-- Índices para performance na fato
CREATE INDEX idx_fat_sinistro_tmp ON dw.fat_sinistro(srk_tmp);
CREATE INDEX idx_fat_sinistro_loc ON dw.fat_sinistro(srk_loc);
CREATE INDEX idx_fat_sinistro_sns ON dw.fat_sinistro(srk_sns);
CREATE INDEX idx_fat_sinistro_via ON dw.fat_sinistro(srk_via);
CREATE INDEX idx_fat_sinistro_pes ON dw.fat_sinistro(srk_pes);
CREATE INDEX idx_fat_sinistro_vei ON dw.fat_sinistro(srk_vei);

-- Mensagem de sucesso
DO $$
BEGIN
    RAISE NOTICE 'Schema dw criado com sucesso!';
END $$;
