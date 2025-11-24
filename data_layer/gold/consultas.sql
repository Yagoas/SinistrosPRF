-- CONSULTAS SINISTROS PRF
-- Data Warehouse: Schema Gold (Gold Layer)

-- ================================================================================
-- RESUMO GERAL - CARDS PRINCIPAIS
-- Retorna totais gerais de sinistros com métricas agregadas

SELECT 
    COUNT(DISTINCT f.sns_srk) as total_sinistros,
    SUM(f.fat_ils) as total_ilesos,
    SUM(f.fat_fle) as total_feridos_leves,
    SUM(f.fat_fgr) as total_feridos_graves,
    SUM(f.fat_fer) as total_feridos,
    SUM(f.fat_mrt) as total_mortos
FROM dw.fat_sinistro f;


-- ================================================================================
-- SINISTROS POR REGIÃO/UF/MUNICÍPIO
-- Análise geográfica dos sinistros

SELECT 
    l.loc_reg,
    l.loc_uf,
    l.loc_ldd,
    COUNT(*) as total_sinistros,
    SUM(f.fat_mrt) as total_mortos,
    SUM(f.fat_fer) as total_feridos,
    SUM(f.fat_ils) as total_ilesos
FROM dw.fat_sinistro f
INNER JOIN dw.dim_localizacao l ON f.srk_loc = l.srk_loc
GROUP BY l.loc_reg, l.loc_uf, l.loc_ldd
ORDER BY total_sinistros DESC;


-- ================================================================================
-- SINISTROS POR ANO/MÊS/DIA/HORA (SÉRIE TEMPORAL)
-- Análise temporal dos sinistros

SELECT 
    t.tmp_ano,
    EXTRACT(MONTH FROM t.tmp_dta) AS mes,
    EXTRACT(DAY FROM t.tmp_dta) AS dia,
    t.tmp_hra,
    t.tmp_dsm,
    t.tmp_per,
    COUNT(*) as total_sinistros,
    SUM(f.fat_mrt) as total_mortos,
    SUM(f.fat_fer) as total_feridos
FROM dw.fat_sinistro f
INNER JOIN dw.dim_temporal t ON f.srk_tmp = t.srk_tmp
GROUP BY t.tmp_ano, EXTRACT(MONTH FROM t.tmp_dta), EXTRACT(DAY FROM t.tmp_dta), t.tmp_hra, t.tmp_dsm, t.tmp_per
ORDER BY t.tmp_ano, EXTRACT(MONTH FROM t.tmp_dta), EXTRACT(DAY FROM t.tmp_dta), t.tmp_hra;


-- ================================================================================
-- SINISTROS POR RODOVIA E QUILÔMETRO
-- Top 10 trechos (rodovia + km) com mais sinistros

SELECT
    l.loc_rod,
    l.loc_klm,
    l.loc_uf,
    COUNT(*) as total_sinistros,
    v.via_trc,
    v.via_tip
FROM dw.fat_sinistro f
INNER JOIN dw.dim_localizacao l ON f.srk_loc = l.srk_loc
INNER JOIN dw.dim_via v ON f.srk_via = v.srk_via
WHERE l.loc_rod IS NOT NULL AND l.loc_klm IS NOT NULL
GROUP BY l.loc_rod, l.loc_klm, l.loc_uf, v.via_trc, v.via_tip
ORDER BY total_sinistros DESC
LIMIT 10;


-- ================================================================================
-- SINISTROS POR TRAÇADO DA VIA
-- Análise por características geométricas da via

SELECT 
    v.via_trc,
    COUNT(*) as total_sinistros,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentual
FROM dw.fat_sinistro f
INNER JOIN dw.dim_via v ON f.srk_via = v.srk_via
WHERE v.via_trc IS NOT NULL
GROUP BY v.via_trc
ORDER BY total_sinistros DESC;


-- ================================================================================
-- SINISTROS POR TIPO DE VIA
-- Classificação por tipo de via

SELECT 
    v.via_tip,
    COUNT(*) as total_sinistros,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentual
FROM dw.fat_sinistro f
INNER JOIN dw.dim_via v ON f.srk_via = v.srk_via
WHERE v.via_tip IS NOT NULL
GROUP BY v.via_tip
ORDER BY total_sinistros DESC;


-- ================================================================================
-- SINISTROS POR USO DO SOLO (RURAL/URBANO)
-- Análise urbano vs rural

SELECT 
    v.via_uso,
    COUNT(*) as total_sinistros,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentual
FROM dw.fat_sinistro f
INNER JOIN dw.dim_via v ON f.srk_via = v.srk_via
WHERE v.via_uso IS NOT NULL
GROUP BY v.via_uso
ORDER BY total_sinistros DESC;


-- ================================================================================
-- SINISTROS POR CAUSA
-- Top 15 causas de sinistros

SELECT 
    s.sns_csa,
    COUNT(*) as total_sinistros,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentual
FROM dw.fat_sinistro f
INNER JOIN dw.dim_sinistro s ON f.srk_sns = s.srk_sns
WHERE s.sns_csa IS NOT NULL
GROUP BY s.sns_csa
ORDER BY total_sinistros DESC
LIMIT 15;


-- ================================================================================
-- SINISTROS POR TIPO
-- Top 15 tipos de sinistros

SELECT 
    s.sns_tip,
    COUNT(*) as total_sinistros,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentual
FROM dw.fat_sinistro f
INNER JOIN dw.dim_sinistro s ON f.srk_sns = s.srk_sns
WHERE s.sns_tip IS NOT NULL
GROUP BY s.sns_tip
ORDER BY total_sinistros DESC
LIMIT 15;



-- ================================================================================
-- PERFIL DOS ENVOLVIDOS
-- Análise demográfica das pessoas envolvidas

SELECT 
    p.pes_sex,
    p.pes_fxe,
    p.pes_etf,
    COUNT(*) as total_pessoas,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentual
FROM dw.fat_sinistro f
INNER JOIN dw.dim_pessoa p ON f.srk_pes = p.srk_pes
WHERE p.pes_sex IS NOT NULL AND p.pes_fxe IS NOT NULL
GROUP BY p.pes_sex, p.pes_fxe, p.pes_etf
ORDER BY total_pessoas DESC;