-- CONSULTAS SINISTROS PRF
-- Data Warehouse: Schema DW (Gold Layer)

-- ================================================================================
-- RESUMO GERAL - CARDS PRINCIPAIS
-- Retorna totais gerais de sinistros, veículos, envolvidos e vítimas

SELECT 
    COUNT(DISTINCT f.sinistro_id) as total_sinistros,
    COUNT(DISTINCT f.veiculo_dim_id) as total_veiculos,
    COUNT(DISTINCT f.envolvido_id) as total_envolvidos,
    SUM(CASE WHEN e.estado_fisico = 'Ferido Leve' THEN 1 ELSE 0 END) as ferimentos_leves,
    SUM(CASE WHEN e.estado_fisico = 'Ferido Grave' THEN 1 ELSE 0 END) as ferimentos_graves,
    SUM(CASE WHEN e.estado_fisico = 'Morto' THEN 1 ELSE 0 END) as obitos,
    SUM(CASE WHEN e.estado_fisico = 'Ileso' THEN 1 ELSE 0 END) as ilesos,
    SUM(CASE WHEN e.estado_fisico = 'Ignorado' THEN 1 ELSE 0 END) as ignorados
FROM dw.fato_sinistros f
LEFT JOIN dw.dim_envolvido e ON f.envolvido_id = e.envolvido_id;


-- ================================================================================
-- SINISTROS POR REGIÃO/UF/MUNICÍPIO
-- Análise geográfica dos sinistros

SELECT 
    l.regiao,
    l.uf,
    l.municipio,
    COUNT(DISTINCT f.sinistro_id) as total_sinistros,
    SUM(CASE WHEN t.gravidade = 'Com morto' THEN 1 ELSE 0 END) as com_mortos,
    SUM(CASE WHEN t.gravidade = 'Com ferido' THEN 1 ELSE 0 END) as com_feridos,
    SUM(CASE WHEN t.gravidade = 'Sem vítima' THEN 1 ELSE 0 END) as sem_vitimas
FROM dw.fato_sinistros f
INNER JOIN dw.dim_local l ON f.local_id = l.local_id
INNER JOIN dw.dim_tipo t ON f.tipo_id = t.tipo_id
GROUP BY l.regiao, l.uf, l.municipio
ORDER BY total_sinistros DESC;


-- ================================================================================
-- SINISTROS POR ANO/MÊS/DIA/HORA (SÉRIE TEMPORAL)
-- Análise temporal dos sinistros

SELECT 
    d.ano,
    EXTRACT(MONTH FROM d.data) AS mes,
    EXTRACT(DAY FROM d.data) AS dia,
    d.hora,
    d.dia_semana,
    COUNT(DISTINCT f.sinistro_id) as total_sinistros,
    SUM(CASE WHEN t.gravidade = 'Com morto' THEN 1 ELSE 0 END) as com_mortos,
    SUM(CASE WHEN t.gravidade = 'Com ferido' THEN 1 ELSE 0 END) as com_feridos,
    SUM(CASE WHEN t.gravidade = 'Sem vítima' THEN 1 ELSE 0 END) as sem_vitimas
FROM dw.fato_sinistros f
INNER JOIN dw.dim_data d ON f.data_id = d.data_id
INNER JOIN dw.dim_tipo t ON f.tipo_id = t.tipo_id
GROUP BY d.ano, EXTRACT(MONTH FROM d.data), EXTRACT(DAY FROM d.data), d.hora, d.dia_semana
ORDER BY d.ano, EXTRACT(MONTH FROM d.data), EXTRACT(DAY FROM d.data), d.hora;


-- ================================================================================
-- SINISTROS POR RODOVIA E QUILÔMETRO
-- Top 10 trechos (rodovia + km) com mais sinistros

SELECT
    l.rodovia,
    l.quilometro,
    l.uf,
    COUNT(DISTINCT f.sinistro_id) as total_sinistros,
    v.via_tracado,
    v.via_tipo
FROM dw.fato_sinistros f
INNER JOIN dw.dim_local l ON f.local_id = l.local_id
INNER JOIN dw.dim_via v ON f.via_id = v.via_id
WHERE l.rodovia IS NOT NULL AND l.quilometro IS NOT NULL
GROUP BY l.rodovia, l.quilometro, l.uf, v.via_tracado, v.via_tipo
ORDER BY total_sinistros DESC
LIMIT 10;


-- ================================================================================
-- SINISTROS POR TRAÇADO DA VIA
-- Análise por características geométricas da via

SELECT 
    v.via_tracado,
    COUNT(DISTINCT f.sinistro_id) as total_sinistros,
    ROUND(COUNT(DISTINCT f.sinistro_id) * 100.0 / SUM(COUNT(DISTINCT f.sinistro_id)) OVER (), 2) as percentual
FROM dw.fato_sinistros f
INNER JOIN dw.dim_via v ON f.via_id = v.via_id
WHERE v.via_tracado IS NOT NULL
GROUP BY v.via_tracado
ORDER BY total_sinistros DESC;


-- ================================================================================
-- SINISTROS POR TIPO DE VIA
-- Classificação por tipo de via

SELECT 
    v.via_tipo,
    COUNT(DISTINCT f.sinistro_id) as total_sinistros,
    ROUND(COUNT(DISTINCT f.sinistro_id) * 100.0 / SUM(COUNT(DISTINCT f.sinistro_id)) OVER (), 2) as percentual
FROM dw.fato_sinistros f
INNER JOIN dw.dim_via v ON f.via_id = v.via_id
WHERE v.via_tipo IS NOT NULL
GROUP BY v.via_tipo
ORDER BY total_sinistros DESC;


-- ================================================================================
-- SINISTROS POR USO DO SOLO (RURAL/URBANO)
-- Análise urbano vs rural

SELECT 
    v.uso_solo,
    COUNT(DISTINCT f.sinistro_id) as total_sinistros,
    ROUND(COUNT(DISTINCT f.sinistro_id) * 100.0 / SUM(COUNT(DISTINCT f.sinistro_id)) OVER (), 2) as percentual
FROM dw.fato_sinistros f
INNER JOIN dw.dim_via v ON f.via_id = v.via_id
WHERE v.uso_solo IS NOT NULL
GROUP BY v.uso_solo
ORDER BY total_sinistros DESC;


-- ================================================================================
-- SINISTROS POR CAUSA
-- Top 15 causas de sinistros

SELECT 
    t.sinistro_causa,
    COUNT(DISTINCT f.sinistro_id) as total_sinistros,
    ROUND(COUNT(DISTINCT f.sinistro_id) * 100.0 / SUM(COUNT(DISTINCT f.sinistro_id)) OVER (), 2) as percentual
FROM dw.fato_sinistros f
INNER JOIN dw.dim_tipo t ON f.tipo_id = t.tipo_id
WHERE t.sinistro_causa IS NOT NULL
GROUP BY t.sinistro_causa
ORDER BY total_sinistros DESC
LIMIT 15;


-- ================================================================================
-- SINISTROS POR TIPO
-- Top 15 tipos de sinistros

SELECT 
    t.sinistro_tipo,
    COUNT(DISTINCT f.sinistro_id) as total_sinistros,
    ROUND(COUNT(DISTINCT f.sinistro_id) * 100.0 / SUM(COUNT(DISTINCT f.sinistro_id)) OVER (), 2) as percentual
FROM dw.fato_sinistros f
INNER JOIN dw.dim_tipo t ON f.tipo_id = t.tipo_id
WHERE t.sinistro_tipo IS NOT NULL
GROUP BY t.sinistro_tipo
ORDER BY total_sinistros DESC
LIMIT 15;



-- ================================================================================
-- PERFIL DOS ENVOLVIDOS
-- Análise demográfica das pessoas envolvidas

SELECT 
    e.envolvido_sexo,
    e.faixa_etaria_classe,
    e.estado_fisico,
    COUNT(*) as total_pessoas,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentual
FROM dw.fato_sinistros f
INNER JOIN dw.dim_envolvido e ON f.envolvido_id = e.envolvido_id
WHERE e.envolvido_sexo IS NOT NULL AND e.faixa_etaria_classe IS NOT NULL
GROUP BY e.envolvido_sexo, e.faixa_etaria_classe, e.estado_fisico
ORDER BY total_pessoas DESC;