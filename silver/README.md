# 🥈 Silver Layer - Pipeline ETL Lakehouse

Esta camada implementa um pipeline ETL completo que transforma dados brutos da camada Bronze em dados estruturados e limpos, salvos como CSV e carregados em PostgreSQL, simulando uma arquitetura Lakehouse.

## 🛠️ Características Principais

- ✅ **Pipeline ETL Automatizado**: Extract, Transform, Load totalmente automatizado
- ✅ **Dados Persistentes**: CSV salvo localmente + PostgreSQL containerizado
- ✅ **Arquitetura Lakehouse**: Baseada em princípios Lakehouse com dados tratados
- ✅ **Qualidade de Dados**: Limpeza, normalização e validação de dados
- ✅ **Docker Containerizado**: Ambiente isolado e reproduzível
- ✅ **Logs Detalhados**: Monitoramento completo do processo ETL

## 🏗️ Arquitetura Atual

```
silver/
├── 📂 database/                    # Configuração PostgreSQL
│   ├── init.sql                   # Schema de criação das tabelas
│   └── ...
├── 📂 etl/                        # Pipeline ETL
│   ├── 📂 data/                   # 📁 Dados persistentes (CSV)
│   │   └── sinistros_tratado.csv  # ✅ Dados transformados
│   ├── 📂 jobs/                   # Jobs ETL
│   │   ├── extract.py             # 🌐 Extração de dados oficiais PRF
│   │   ├── transform.py           # 🔄 Transformações e limpeza
│   │   ├── load.py               # 📥 Carga no PostgreSQL
│   │   └── pipeline.py           # 🚀 Orquestrador principal
│   ├── 📂 scripts/               # Scripts auxiliares
│   └── 📂 utils/                 # Utilitários
│       ├── database.py           # 🐘 Conexão PostgreSQL
│       └── logging_utils.py      # 📝 Sistema de logs
├── 📂 models/                    # Modelagem de dados
└── README.md
```

## 🛠️ Como Usar

### 1. Setup Completo com Docker

```bash
# No diretório raiz do projeto
make setup

# Ou manualmente
docker-compose up -d
```

### 2. Acessar Dados

```bash
# CSV local (sempre disponível)
silver/etl/data/sinistros_tratado.csv

# PostgreSQL via pgAdmin
http://localhost:8080
# admin@sinistros.com / admin123

# PostgreSQL direto
psql -h localhost -p 5432 -U admin -d sinistros_prf
```

## 🐘 PostgreSQL Containerizado

### Configuração Automática

- **Versão**: PostgreSQL 15 Alpine
- **Porta**: 5432
- **Database**: sinistros_prf
- **Usuário**: admin
- **Senha**: admin123
- **pgAdmin**: http://localhost:8080

### Tabela Principal

```sql
-- Estrutura completa da tabela silver (45 colunas)
sinistros.tb_sinistros_silver
├── sinistro_id                    # ID único do sinistro
├── id_envolvido                   # ID da pessoa envolvida
├── veiculo_id                     # ID do veículo
├── data                           # Data do sinistro (YYYY-MM-DD)
├── horario                        # Hora do sinistro (HH:MM:SS)
├── data_hora                      # Data e hora combinadas
├── ano                            # Ano do sinistro
├── hora                           # Hora extraída (0-23)
├── dia_semana                     # Dia da semana (Segunda-feira, etc.)
├── periodo                        # Período do dia (Manhã, Tarde, Noite, Madrugada)
├── periodo_semana                 # Período da semana (Segunda à Sexta, Final de semana)
├── uf                             # Estado (sigla)
├── localidade                     # Nome completo do estado
├── regiao                         # Região (Norte, Nordeste, Centro-Oeste, Sudeste, Sul)
├── municipio                      # Nome do município
├── rodovia                        # Rodovia formatada (BR-XXX)
├── rodovia_numero                 # Número da rodovia
├── quilometro                     # Quilômetro da rodovia
├── latitude                       # Coordenada latitude
├── longitude                      # Coordenada longitude
├── sinistro_tipo                  # Tipo do sinistro
├── sinistro_causa                 # Causa do sinistro
├── sinistro_causa_principal       # Causa principal
├── sinistro_ordem_tipo            # Ordem do tipo de acidente
├── condicao_meteorologica         # Condição do tempo
├── via_tipo                       # Tipo da pista
├── via_tracado                    # Traçado da via
├── via_sentido                    # Sentido da via
├── uso_solo                       # Uso do solo (Urbano/Rural)
├── envolvido_idade                # Idade da pessoa envolvida
├── envolvido_sexo                 # Sexo da pessoa (Masculino/Feminino)
├── envolvido_tipo                 # Tipo de envolvimento (Condutor, Passageiro, Pedestre)
├── estado_fisico                  # Estado físico da pessoa
├── faixa_etaria_ano               # Faixa etária por década (0-9, 10-19, etc.)
├── faixa_etaria_classe            # Faixa etária por classificação (Criança, Adolescente, Adulto, Idoso)
├── veiculo_tipo                   # Tipo do veículo
├── veiculo_marca_modelo           # Marca/modelo do veículo
├── veiculo_ano_fabricacao         # Ano de fabricação do veículo
├── ilesos                         # Número de ilesos
├── feridos_leves                  # Número de feridos leves
├── feridos_graves                 # Número de feridos graves
├── feridos                        # Total de feridos (leves + graves)
├── mortos                         # Número de mortos
├── gravidade                      # Gravidade do sinistro (Com morto, Com ferido, Sem vítima)
└── ups                            # UPS - Unidade Padrão de Severidade (1, 4, 6, 13)
```

## 🔄 Pipeline ETL Detalhado

### 📥 Extract (extract.py)

- **Fonte**: Dados oficiais da PRF (2024-2025)
- **Formato**: CSV comprimido via HTTP
- **Cache**: Evita downloads caso já existam
- **Validação**: Verificação de integridade dos arquivos

### 🔄 Transform (transform.py)

Implementa as transformações do notebook `tratamento_raw.ipynb`:

#### Limpeza e Normalização

- ✅ Remove colunas irrelevantes (6 colunas removidas)
- ✅ Normaliza strings (espaços, nulos, vírgulas decimais)
- ✅ Converte tipos de dados (datas, números, categorias)
- ✅ Renomeia colunas para padrões consistentes

#### Enriquecimento de Dados

- ✅ **Geolocalização**: UF → nome completo e região
- ✅ **Temporal**: Cálculo do dia da semana
- ✅ **Estatísticas**: Totais de feridos, pessoas envolvidas
- ✅ **Categorização**: Idade, ano de fabricação dos veículos, faixas etárias

#### Tratamento de Outliers

- ✅ Idades > 200 anos
- ✅ Anos de fabricação inválidos
- ✅ Coordenadas fora do Brasil

#### Resultado Final

- **Registros**: 981.790 linhas (100% preservados)
- **Colunas**: 37 → 45 (8 novas colunas derivadas)
- **Qualidade**: Dados limpos e estruturados

### 📤 Load (load.py)

- **Destino Duplo**: CSV local + PostgreSQL
- **Estratégia**: Truncate & Insert (refresh completo)
- **Validação**: Verificação pós-carga
- **Performance**: Otimizado para grandes volumes

## 📊 Dados Processados

### Estatísticas dos Dados (2024-2025)

```
📈 RESUMO DO DATASET SILVER
================================
Total de Registros: 981.790
Total de Colunas: 45
Período: 2024-2025

🔢 Estatísticas Principais:
├── Sinistros Únicos: 120.348
├── Pessoas Envolvidas: 294.539
└── Veículos Envolvidos: 230.691

📅 Distribuição Temporal:
├── 2024: 603.215 registros (61.4%)
└── 2025: 378.575 registros (38.6%)

💾 Armazenamento:
├── CSV: ~200MB (comprimido)
├── PostgreSQL: ~500MB (indexado)
└── Memória: ~1.580MB (processamento)
```

## Consultas Úteis

### Consultas SQL (PostgreSQL)

```sql
-- Conectar ao banco
-- psql -h localhost -p 5432 -U admin -d sinistros_prf

-- Estatísticas gerais por ano
SELECT
    EXTRACT(YEAR FROM data_inversa) as ano,
    COUNT(*) as total_acidentes,
    SUM(mortos) as total_mortos,
    SUM(feridos) as total_feridos,
    ROUND(AVG(mortos::numeric), 2) as media_mortos_acidente
FROM sinistros.tb_sinistros_silver
GROUP BY EXTRACT(YEAR FROM data_inversa)
ORDER BY ano;

-- Top 10 trechos mais perigosos
SELECT
    uf,
    br,
    COUNT(*) as acidentes,
    SUM(mortos) as mortos,
    SUM(feridos) as feridos,
    ROUND((SUM(mortos) + SUM(feridos))::numeric / COUNT(*), 2) as vitimas_por_acidente
FROM sinistros.tb_sinistros_silver
GROUP BY uf, br
HAVING COUNT(*) >= 100  -- Mínimo 100 acidentes
ORDER BY vitimas_por_acidente DESC, acidentes DESC
LIMIT 10;

-- Distribuição de acidentes por fase do dia
SELECT
    fase_dia,
    COUNT(*) as acidentes,
    ROUND(COUNT(*)::numeric * 100 / SUM(COUNT(*)) OVER (), 1) as percentual
FROM sinistros.tb_sinistros_silver
GROUP BY fase_dia
ORDER BY acidentes DESC;

-- Condições meteorológicas vs mortalidade
SELECT
    condicao_meteorologica,
    COUNT(*) as acidentes,
    SUM(mortos) as mortos,
    ROUND(SUM(mortos)::numeric / COUNT(*) * 100, 2) as taxa_mortalidade_pct
FROM sinistros.tb_sinistros_silver
WHERE condicao_meteorologica IS NOT NULL
GROUP BY condicao_meteorologica
HAVING COUNT(*) >= 50
ORDER BY taxa_mortalidade_pct DESC;
```

## 🎯 Qualidade de Dados

### Validações Implementadas

#### ✅ Limpeza Automática

- **Encoding**: UTF-8 para preservar caracteres especiais
- **Strings**: Remoção de espaços, normalização de nulos
- **Números**: Conversão de vírgulas decimais para pontos
- **Padronização**: Tratamento consistente de valores vazios

#### ✅ Validação de Domínios

- **Datas**: Não aceita datas futuras
- **Coordenadas**: Validação territorial brasileira
- **UFs**: Apenas estados válidos
- **Idades**: Limite de 200 anos

#### ✅ Integridade de Dados

- **Totais**: Soma de vítimas por categoria
- **Consistência**: Relacionamento pessoas/veículos
- **Duplicatas**: Detecção e remoção automática
- **Completude**: Tratamento de valores nulos


### Logs e Monitoramento

```bash
# Logs do pipeline ETL
docker logs sinistros_etl_setup

# Logs do PostgreSQL
docker logs sinistros_postgres
```

---

**⚡ Silver Layer - Dados limpos, estruturados e prontos para análise! ⚡**
