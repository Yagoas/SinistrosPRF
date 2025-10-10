-- Inicialização do banco de dados para a camada Silver - LAKEHOUSE

-- Configurações iniciais
SET timezone = 'America/Sao_Paulo';

-- Criação do schema
CREATE SCHEMA IF NOT EXISTS sinistros;

-- Extensões necessárias
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";


------------------------------ TABELA SILVER LAYER - LAKEHOUSE --------------------------------

-- Drop da tabela se existir
DROP TABLE IF EXISTS sinistros.tb_sinistros_silver CASCADE;

-- Tabela única com TODOS os dados tratados
CREATE TABLE sinistros.tb_sinistros_silver (
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
    periodo VARCHAR(9), -- Madrugada, Manhã, Tarde, Noite
    periodo_semana VARCHAR(15), -- Segunda à Sexta, Final de semana

    -- LOCALIZAÇÃO GEOGRÁFICA
    uf VARCHAR(2),
    localidade VARCHAR(19), -- Nome completo do estado
    regiao VARCHAR(12), -- Norte, Nordeste, Centro-Oeste, Sudeste, Sul
    municipio VARCHAR(100),
    
    -- Rodovia
    rodovia VARCHAR(6), -- BR-XXX formatado
    rodovia_numero VARCHAR(5), -- Apenas o número
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
    via_tipo VARCHAR(20), -- tipo_pista
    via_tracado VARCHAR(150), -- tracado_via  
    via_sentido VARCHAR(20), -- sentido_via
    uso_solo VARCHAR(6), -- Urbano/Rural
    
    -- DADOS DO ENVOLVIDO/PESSOA
    envolvido_idade INTEGER,
    envolvido_sexo VARCHAR(20),
    envolvido_tipo VARCHAR(20),
    estado_fisico VARCHAR(20),
    
    -- Faixas etárias calculadas
    faixa_etaria_ano VARCHAR(13), -- 0-9, 10-19, 20-29, etc, "Não informado"
    faixa_etaria_classe VARCHAR(20), -- Criança, Adolescente, Adulto, Idoso
    
    -- DADOS DO VEÍCULO
    veiculo_tipo VARCHAR(20),
    veiculo_marca_modelo VARCHAR(50),
    veiculo_ano_fabricacao INTEGER,
    
    -- TOTALIZADORES E CONTADORES
    ilesos INTEGER DEFAULT 0,
    feridos_leves INTEGER DEFAULT 0,
    feridos_graves INTEGER DEFAULT 0,
    feridos INTEGER DEFAULT 0, -- Calculado: feridos_leves + feridos_graves
    mortos INTEGER DEFAULT 0,

    -- CLASSIFICAÇÕES CALCULADAS DO SINISTRO
    gravidade VARCHAR(20), -- Com morto, Com ferido, Sem vítima, Não informado
    ups INTEGER -- Unidade Padrão de Severidade (1, 4, 6, 13)
);


------------------------------ ÍNDICES PARA PERFORMANCE ------------------------------

-- Índice primário composto (sinistro + envolvido + veículo)
CREATE INDEX idx_silver_pk ON sinistros.tb_sinistros_silver(sinistro_id, id_envolvido, veiculo_id);

-- Índices temporais
CREATE INDEX idx_silver_data ON sinistros.tb_sinistros_silver(data);
CREATE INDEX idx_silver_ano ON sinistros.tb_sinistros_silver(ano);
CREATE INDEX idx_silver_periodo ON sinistros.tb_sinistros_silver(periodo);
CREATE INDEX idx_silver_dia_semana ON sinistros.tb_sinistros_silver(dia_semana);

-- Índices geográficos
CREATE INDEX idx_silver_uf ON sinistros.tb_sinistros_silver(uf);
CREATE INDEX idx_silver_localidade ON sinistros.tb_sinistros_silver(localidade);
CREATE INDEX idx_silver_regiao ON sinistros.tb_sinistros_silver(regiao);
CREATE INDEX idx_silver_municipio ON sinistros.tb_sinistros_silver(municipio);
CREATE INDEX idx_silver_rodovia ON sinistros.tb_sinistros_silver(rodovia);

-- Índices de análise
CREATE INDEX idx_silver_gravidade ON sinistros.tb_sinistros_silver(gravidade);
CREATE INDEX idx_silver_tipo ON sinistros.tb_sinistros_silver(sinistro_tipo);
CREATE INDEX idx_silver_causa ON sinistros.tb_sinistros_silver(sinistro_causa);
CREATE INDEX idx_silver_ups ON sinistros.tb_sinistros_silver(ups);

-- Índices demográficos
CREATE INDEX idx_silver_idade ON sinistros.tb_sinistros_silver(envolvido_idade);
CREATE INDEX idx_silver_sexo ON sinistros.tb_sinistros_silver(envolvido_sexo);
CREATE INDEX idx_silver_faixa_etaria ON sinistros.tb_sinistros_silver(faixa_etaria_classe);

-- Índices de veículos
CREATE INDEX idx_silver_veiculo_tipo ON sinistros.tb_sinistros_silver(veiculo_tipo);
CREATE INDEX idx_silver_veiculo_ano ON sinistros.tb_sinistros_silver(veiculo_ano_fabricacao);

-- Índice geoespacial
CREATE INDEX idx_silver_location ON sinistros.tb_sinistros_silver USING btree(latitude, longitude);


------------------------------ VIEWS ÚTEIS PARA ANÁLISE ------------------------------

-- View de sinistros únicos (agregando os envolvidos)
CREATE OR REPLACE VIEW sinistros.vw_silver_sinistros_unicos AS
SELECT 
    sinistro_id,
    data_hora,
    uf,
    localidade,
    regiao,
    municipio,
    rodovia,
    quilometro,
    latitude,
    longitude,
    sinistro_tipo,
    sinistro_causa,
    condicao_meteorologica,
    via_tipo,
    via_tracado,
    uso_solo,
    dia_semana,
    periodo,
    MAX(ups) as ups_sinistro,
    -- Totalizadores únicos por sinistro
    MAX(ilesos) as total_ilesos,
    MAX(feridos_leves) as total_feridos_leves,
    MAX(feridos_graves) as total_feridos_graves,
    MAX(feridos) as total_feridos,
    MAX(mortos) as total_mortos,
    -- Contadores de envolvidos
    COUNT(DISTINCT id_envolvido) FILTER (WHERE id_envolvido IS NOT NULL) as total_pessoas_envolvidas,
    COUNT(DISTINCT veiculo_id) FILTER (WHERE veiculo_id IS NOT NULL) as total_veiculos_envolvidos
FROM sinistros.tb_sinistros_silver
WHERE sinistro_causa_principal = 'Sim'
GROUP BY sinistro_id, data_hora, uf, localidade, regiao, municipio,
         rodovia, quilometro, latitude, longitude, sinistro_tipo, sinistro_causa,
         condicao_meteorologica, via_tipo, via_tracado, uso_solo, dia_semana,
         periodo;

-- View de pessoas envolvidas
CREATE OR REPLACE VIEW sinistros.vw_silver_pessoas AS
SELECT DISTINCT
    sinistro_id,
    id_envolvido,
    envolvido_idade,
    envolvido_sexo,
    envolvido_tipo,
    estado_fisico,
    faixa_etaria_ano,
    faixa_etaria_classe
FROM sinistros.tb_sinistros_silver
WHERE id_envolvido IS NOT NULL;

-- View de veículos envolvidos
CREATE OR REPLACE VIEW sinistros.vw_silver_veiculos AS
SELECT DISTINCT
    sinistro_id,
    veiculo_id,
    veiculo_tipo,
    veiculo_marca_modelo,
    veiculo_ano_fabricacao
FROM sinistros.tb_sinistros_silver
WHERE veiculo_id IS NOT NULL;

-- View de estatísticas por UF
CREATE OR REPLACE VIEW sinistros.vw_silver_estatisticas_uf AS
SELECT 
    uf,
    localidade,
    regiao,
    COUNT(DISTINCT sinistro_id) as total_sinistros,
    SUM(DISTINCT mortos) as total_mortos,
    SUM(DISTINCT feridos) as total_feridos,
    SUM(DISTINCT ilesos) as total_ilesos,
    AVG(DISTINCT ups) as ups_medio,
    COUNT(DISTINCT CASE WHEN gravidade = 'Com morto' THEN sinistro_id END) as sinistros_com_morto,
    COUNT(DISTINCT CASE WHEN gravidade = 'Com ferido' THEN sinistro_id END) as sinistros_com_ferido,
    COUNT(DISTINCT CASE WHEN gravidade = 'Sem vítima' THEN sinistro_id END) as sinistros_sem_vitima
FROM sinistros.tb_sinistros_silver
GROUP BY uf, localidade, regiao
ORDER BY total_sinistros DESC;


------------------------------ FUNÇÕES AUXILIARES ------------------------------

-- Função para obter estatísticas da tabela Silver
CREATE OR REPLACE FUNCTION get_silver_stats()
RETURNS TABLE(
    total_registros BIGINT,
    sinistros_unicos BIGINT,
    pessoas_envolvidas BIGINT,
    veiculos_envolvidos BIGINT,
    periodo_dados_inicio DATE,
    periodo_dados_fim DATE,
    ufs_distintas BIGINT,
    municipios_distintos BIGINT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        COUNT(*) as total_registros,
        COUNT(DISTINCT sinistro_id) as sinistros_unicos,
        COUNT(DISTINCT id_envolvido) FILTER (WHERE id_envolvido IS NOT NULL) as pessoas_envolvidas,
        COUNT(DISTINCT veiculo_id) FILTER (WHERE veiculo_id IS NOT NULL) as veiculos_envolvidos,
        MIN(data) as periodo_dados_inicio,
        MAX(data) as periodo_dados_fim,
        COUNT(DISTINCT uf) as ufs_distintas,
        COUNT(DISTINCT municipio) as municipios_distintos
    FROM sinistros.tb_sinistros_silver;
END;
$$ LANGUAGE plpgsql;


------------------------------ COMENTÁRIOS NAS TABELAS ------------------------------

COMMENT ON SCHEMA sinistros IS 'Schema principal para dados de sinistros da PRF - Camada Silver (Lakehouse)';

COMMENT ON TABLE sinistros.tb_sinistros_silver IS 'Tabela única Silver Layer - Todos os dados de sinistros tratados e desnormalizados (estilo Lakehouse)';

-- Comentários importantes sobre a abordagem
COMMENT ON COLUMN sinistros.tb_sinistros_silver.sinistro_id IS 'ID do sinistro (pode repetir para múltiplos envolvidos/veículos)';
COMMENT ON COLUMN sinistros.tb_sinistros_silver.id_envolvido IS 'ID da pessoa envolvida (NULL se registro representa apenas o sinistro)';
COMMENT ON COLUMN sinistros.tb_sinistros_silver.veiculo_id IS 'ID do veículo envolvido (NULL se registro representa apenas o sinistro)';
COMMENT ON COLUMN sinistros.tb_sinistros_silver.gravidade IS 'Calculado: Com morto, Com ferido, Sem vítima baseado nos totalizadores';
COMMENT ON COLUMN sinistros.tb_sinistros_silver.ups IS 'Unidade Padrão de Severidade: 13=morto, 6=atropelamento, 4=ferido, 1=danos materiais';
COMMENT ON COLUMN sinistros.tb_sinistros_silver.faixa_etaria_ano IS 'Faixa etária em anos: 0-9, 10-19, 20-29, etc.';
COMMENT ON COLUMN sinistros.tb_sinistros_silver.faixa_etaria_classe IS 'Classificação conforme o ECA: Criança, Adolescente, Adulto, Idoso';


-- Finalização
SELECT 'Database Silver Layer inicializado com sucesso!' as status,
       'Estrutura: Lakehouse' as info;