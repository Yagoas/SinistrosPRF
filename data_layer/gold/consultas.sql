-- CONSULTAS SINISTROS PRF
-- Data Warehouse: Schema Gold (Gold Layer)

-- ================================================================================
-- RESUMO GERAL (cartões e gráficos dinâmicos)
-- Retorna os indicadores principais para os cartões do dashboard

SELECT 
    TO_CHAR(t.tmp_dta, 'MM/YYYY') AS mes_ano,
    l.loc_uf AS uf,
    COUNT(DISTINCT f.srk_sns) AS total_sinistros,
    COUNT(DISTINCT v.srk_vei) AS total_veiculos,
    COUNT(DISTINCT f.srk_pes) AS total_envolvidos,
    SUM(f.fat_fle) AS total_ferimentos_leves,
    SUM(f.fat_fgr) AS total_ferimentos_graves,
    SUM(f.fat_mrt) AS total_obitos,
    SUM(f.fat_ils) AS total_ilesos,
    SUM(f.fat_fle + f.fat_fgr) AS total_feridos,
    (COUNT(*) - SUM(f.fat_ils) - SUM(f.fat_fle) - SUM(f.fat_fgr) - SUM(f.fat_mrt)) AS total_ignorados,
    c.cat_grv AS gravidade
FROM dw.fat_sinistro f

LEFT JOIN dw.dim_veiculo v 
    ON f.srk_vei = v.srk_vei
LEFT JOIN dw.dim_temporal t 
    ON f.srk_tmp = t.srk_tmp
LEFT JOIN dw.dim_categorizacao c 
    ON f.srk_cat = c.srk_cat
LEFT JOIN dw.dim_localizacao l 
    ON f.srk_loc = l.srk_loc
GROUP BY
    TO_CHAR(t.tmp_dta, 'MM/YYYY'),
    l.loc_uf,
    c.cat_grv
ORDER BY
    mes_ano ASC,
    l.loc_uf ASC;


-- ================================================================================
-- DIMENSÃO TEMPORAL (CALENDÁRIO)
-- Tabela calendário com todas as datas disponíveis para a página resumo dinâmica

SELECT DISTINCT
    DATE(t.tmp_dta) AS data,
    EXTRACT(MONTH FROM t.tmp_dta) AS mes,
    t.tmp_ano AS ano,
    TO_CHAR(t.tmp_dta, 'MM/YYYY') AS mes_ano,
    t.tmp_dsm AS dia_semana,
    t.tmp_psm AS periodo
FROM dw.dim_temporal t

ORDER BY
    t.tmp_dta ASC;


-- ================================================================================
-- DIMENSÃO GEOGRÁFICA (LOCALIZAÇÃO)
-- Hierarquia geográfica para a página resumo dinâmica

SELECT DISTINCT
    l.loc_reg AS regiao,
    l.loc_uf AS uf,
    l.loc_ldd AS localidade,
    l.loc_mun AS municipio
FROM dw.dim_localizacao l

ORDER BY
    l.loc_reg ASC,
    l.loc_uf ASC,
    l.loc_ldd ASC,
    l.loc_mun ASC;


-- ================================================================================
-- SINISTROS POR COORDENADAS GEOGRÁFICAS
-- Dados para visualização em mapa (latitude/longitude)
-- Agrupa sinistros por ponto geográfico com métricas de vítimas

SELECT 
    l.loc_lat AS latitude,
    l.loc_lng AS longitude,
    COUNT(DISTINCT f.srk_sns) AS total_sinistros,
    SUM(f.fat_fle) AS total_ferimentos_leves,
    SUM(f.fat_fgr) AS total_ferimentos_graves,
    SUM(f.fat_mrt) AS total_obitos,
    SUM(f.fat_ils) AS total_ilesos
FROM dw.fat_sinistro f

LEFT JOIN dw.dim_localizacao l 
    ON f.srk_loc = l.srk_loc
GROUP BY
    l.loc_lat,
    l.loc_lng;


-- ================================================================================
-- PERFIL DOS ENVOLVIDOS
-- Análise demográfica das pessoas envolvidas

SELECT 
    p.pes_sex,
    p.pes_fxc,
    p.pes_esf,
    COUNT(*) as total_pessoas,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentual
FROM dw.fat_sinistro f

LEFT JOIN dw.dim_pessoa p 
    ON f.srk_pes = p.srk_pes
WHERE p.pes_sex IS NOT NULL 
    AND p.pes_fxc IS NOT NULL
GROUP BY p.pes_sex, p.pes_fxc, p.pes_esf
ORDER BY total_pessoas DESC;


-- ================================================================================
-- SINISTROS POR TIPO
-- Top 15 tipos de sinistros

SELECT 
    s.cat_tip,
    COUNT(DISTINCT f.srk_sns) as total_sinistros,
    ROUND(COUNT(DISTINCT f.srk_sns) * 100.0 / SUM(COUNT(DISTINCT f.srk_sns)) OVER (), 2) as percentual
FROM dw.fat_sinistro f

LEFT JOIN dw.dim_categorizacao s 
    ON f.srk_cat = s.srk_cat
WHERE s.cat_tip IS NOT NULL
GROUP BY s.cat_tip
ORDER BY total_sinistros DESC
LIMIT 15;


-- ================================================================================
-- SINISTROS POR CAUSA
-- Top 15 causas de sinistros

SELECT 
    s.cat_cau,
    COUNT(DISTINCT f.srk_sns) as total_sinistros,
    ROUND(COUNT(DISTINCT f.srk_sns) * 100.0 / SUM(COUNT(DISTINCT f.srk_sns)) OVER (), 2) as percentual
FROM dw.fat_sinistro f

LEFT JOIN dw.dim_categorizacao s 
    ON (f.srk_cat = s.srk_cat) AND
    s.cat_cap = 'Sim'
WHERE s.cat_cau IS NOT NULL
GROUP BY s.cat_cau
ORDER BY total_sinistros DESC
LIMIT 15;


-- ================================================================================
-- SINISTROS POR USO DO SOLO (RURAL/URBANO)
-- Análise urbano vs rural

SELECT 
    v.via_uso,
    COUNT(DISTINCT f.srk_sns) as total_sinistros,
    ROUND(COUNT(DISTINCT f.srk_sns) * 100.0 / SUM(COUNT(DISTINCT f.srk_sns)) OVER (), 2) as percentual
FROM dw.fat_sinistro f

LEFT JOIN dw.dim_via v 
    ON f.srk_via = v.srk_via
WHERE v.via_uso IS NOT NULL
GROUP BY v.via_uso
ORDER BY total_sinistros DESC;

-- ================================================================================
-- SINISTROS POR RODOVIA E QUILÔMETRO
-- Top 10 trechos (rodovia + km) com mais sinistros

SELECT
    l.loc_rod,
    l.loc_km,
    l.loc_uf,
    COUNT(DISTINCT f.srk_sns) as total_sinistros,
    v.via_tra,
    v.via_tip
FROM dw.fat_sinistro f

LEFT JOIN dw.dim_localizacao l 
    ON f.srk_loc = l.srk_loc
LEFT JOIN dw.dim_via v 
    ON f.srk_via = v.srk_via
WHERE l.loc_rod IS NOT NULL 
    AND l.loc_km IS NOT NULL
GROUP BY l.loc_rod, l.loc_km, l.loc_uf, v.via_tra, v.via_tip
ORDER BY total_sinistros DESC;


-- ================================================================================
-- SINISTROS POR TRAÇADO DA VIA
-- Top 10 trechos por características geométricas da via

SELECT 
    v.via_tra,
    COUNT(DISTINCT f.srk_sns) as total_sinistros,
    ROUND(COUNT(DISTINCT f.srk_sns) * 100.0 / SUM(COUNT(DISTINCT f.srk_sns)) OVER (), 2) as percentual
FROM dw.fat_sinistro f

LEFT JOIN dw.dim_via v 
    ON f.srk_via = v.srk_via
WHERE v.via_tra IS NOT NULL
GROUP BY v.via_tra
ORDER BY total_sinistros DESC
LIMIT 10;


-- ================================================================================
-- SINISTROS POR TIPO DE VIA
-- Classificação por tipo de via

SELECT 
    v.via_tip,
    COUNT(DISTINCT f.srk_sns) as total_sinistros,
    ROUND(COUNT(DISTINCT f.srk_sns) * 100.0 / SUM(COUNT(DISTINCT f.srk_sns)) OVER (), 2) as percentual
FROM dw.fat_sinistro f

LEFT JOIN dw.dim_via v 
    ON f.srk_via = v.srk_via
WHERE v.via_tip IS NOT NULL
GROUP BY v.via_tip
ORDER BY total_sinistros DESC;