-- Inicialização do Banco de Dados para Sinistros PRF

-- Configurações iniciais
SET timezone = 'America/Sao_Paulo';

-- Extensões necessárias
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";

------------------------------ SCHEMA SILVER ------------------------------

CREATE SCHEMA IF NOT EXISTS dl;

-- Drop da tabela se existir
DROP TABLE IF EXISTS dl.tb_sinistros_silver CASCADE;

-- Tabela única com TODOS os dados tratados
CREATE TABLE dl.tb_sinistros_silver (
    -- IDENTIFICADORES PRIMÁRIOS
    sinistro_id BIGINT NOT NULL,
    id_envolvido BIGINT,
    veiculo_id BIGINT,
    
    -- DADOS TEMPORAIS
    data DATE,
    horario TIME,
    data_hora TIMESTAMP,
    ano INTEGER,
    hora INTEGER,
    dia_semana VARCHAR(13),
    periodo VARCHAR(9),
    periodo_semana VARCHAR(15),

    -- LOCALIZAÇÃO GEOGRÁFICA
    uf VARCHAR(2),
    localidade VARCHAR(19),
    regiao VARCHAR(12),
    municipio VARCHAR(100),
    
    -- Rodovia
    rodovia VARCHAR(6),
    rodovia_numero VARCHAR(5),
    quilometro DECIMAL(10,2),
    
    -- Coordenadas
    latitude DECIMAL(12,10),
    longitude DECIMAL(12,10),
    
    -- CARACTERÍSTICAS DO SINISTRO
    sinistro_tipo VARCHAR(50),
    sinistro_causa VARCHAR(100),
    sinistro_causa_principal VARCHAR(5),
    sinistro_ordem_tipo INTEGER,
    
    -- CONDIÇÕES AMBIENTAIS E DA VIA
    condicao_meteorologica VARCHAR(30),
    via_tipo VARCHAR(20),
    via_tracado VARCHAR(150),
    via_sentido VARCHAR(20),
    uso_solo VARCHAR(6),
    
    -- DADOS DO ENVOLVIDO/PESSOA
    envolvido_idade INTEGER,
    envolvido_sexo VARCHAR(20),
    envolvido_tipo VARCHAR(20),
    estado_fisico VARCHAR(20),
    
    -- Faixas etárias calculadas
    faixa_etaria_ano VARCHAR(13),
    faixa_etaria_classe VARCHAR(20),
    
    -- DADOS DO VEÍCULO
    veiculo_tipo VARCHAR(20),
    veiculo_marca_modelo VARCHAR(50),
    veiculo_ano_fabricacao INTEGER,
    
    -- TOTALIZADORES E CONTADORES
    ilesos INTEGER DEFAULT 0,
    feridos_leves INTEGER DEFAULT 0,
    feridos_graves INTEGER DEFAULT 0,
    feridos INTEGER DEFAULT 0,
    mortos INTEGER DEFAULT 0,

    -- CLASSIFICAÇÕES CALCULADAS DO SINISTRO
    gravidade VARCHAR(20),
    ups INTEGER
);

-- Índices para performance
CREATE INDEX idx_silver_pk ON dl.tb_sinistros_silver(sinistro_id, id_envolvido, veiculo_id);
CREATE INDEX idx_silver_data ON dl.tb_sinistros_silver(data);
CREATE INDEX idx_silver_ano ON dl.tb_sinistros_silver(ano);
CREATE INDEX idx_silver_uf ON dl.tb_sinistros_silver(uf);
CREATE INDEX idx_silver_regiao ON dl.tb_sinistros_silver(regiao);

-- Mensagem de sucesso
DO $$
BEGIN
    RAISE NOTICE 'Banco de dados inicializado com sucesso!';
    RAISE NOTICE '   - Schema dl criado';
END $$;
