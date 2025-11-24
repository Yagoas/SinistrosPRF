-- CONSULTAS SINISTROS PRF
-- Data Warehouse: Schema Gold (Gold Layer)

-- ================================================================================
-- RESUMO GERAL - CARDS PRINCIPAIS
-- Retorna totais gerais de sinistros com métricas agregadas

SELECT 
    COUNT(DISTINCT f.srk_sns) as total_sinistros,
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
    l.loc_km,
    l.loc_uf,
    COUNT(*) as total_sinistros,
    v.via_tra,
    v.via_tip
FROM dw.fat_sinistro f
INNER JOIN dw.dim_localizacao l ON f.srk_loc = l.srk_loc
INNER JOIN dw.dim_via v ON f.srk_via = v.srk_via
WHERE l.loc_rod IS NOT NULL AND l.loc_km IS NOT NULL
GROUP BY l.loc_rod, l.loc_km, l.loc_uf, v.via_tra, v.via_tip
ORDER BY total_sinistros DESC
LIMIT 10;


-- ================================================================================
-- SINISTROS POR TRAÇADO DA VIA
-- Análise por características geométricas da via

SELECT 
    v.via_tra,
    COUNT(*) as total_sinistros,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentual
FROM dw.fat_sinistro f
INNER JOIN dw.dim_via v ON f.srk_via = v.srk_via
WHERE v.via_tra IS NOT NULL
GROUP BY v.via_tra
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
    s.cat_cau,
    COUNT(*) as total_sinistros,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentual
FROM dw.fat_sinistro f
INNER JOIN dw.dim_categorizacao s ON f.srk_cat = s.srk_cat
WHERE s.cat_cau IS NOT NULL
GROUP BY s.cat_cau
ORDER BY total_sinistros DESC
LIMIT 15;


-- ================================================================================
-- SINISTROS POR TIPO
-- Top 15 tipos de sinistros

SELECT 
    s.cat_tip,
    COUNT(*) as total_sinistros,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentual
FROM dw.fat_sinistro f
INNER JOIN dw.dim_categorizacao s ON f.srk_cat = s.srk_cat
WHERE s.cat_tip IS NOT NULL
GROUP BY s.cat_tip
ORDER BY total_sinistros DESC
LIMIT 15;


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
INNER JOIN dw.dim_pessoa p ON f.srk_pes = p.srk_pes
WHERE p.pes_sex IS NOT NULL AND p.pes_fxc IS NOT NULL
GROUP BY p.pes_sex, p.pes_fxc, p.pes_esf
ORDER BY total_pessoas DESC;