# Análise de Sinistros em Rodovias Federais do Brasil

![Python](https://img.shields.io/badge/python-v3.11+-blue.svg)
![Pandas](https://img.shields.io/badge/pandas-analysis-green.svg)
![Matplot](https://img.shields.io/badge/matplotlib-analysis-green.svg)
![Seaborn](https://img.shields.io/badge/seaborn-analysis-green.svg)
![Follium](https://img.shields.io/badge/follium-map-green.svg)
![PostgreSQL](https://img.shields.io/badge/postgresql-database-blue.svg)
![Docker](https://img.shields.io/badge/docker-containerized-blue.svg)
![PowerBI](https://img.shields.io/badge/powerbi-visualization-yellow.svg)
![Jupyter](https://img.shields.io/badge/jupyter-notebook-orange.svg)
![Status](https://img.shields.io/badge/status-em%20desenvolvimento-yellow.svg)
<!-- ![Status](https://img.shields.io/badge/status-concluido-green.svg) -->

## 📋 Sobre o Projeto

Este projeto foi desenvolvido para a disciplina **SDB2 (Sistemas de Banco de Dados 2)** da **Universidade de Brasília (UnB)**, com o objetivo de realizar uma análise exploratória e estatística dos dados de sinistralidade nas rodovias federais brasileiras, utilizando a **arquitetura Medallion** para processamento e análise de dados.

Para a análise, foram utilizados dados oficiais de sinistros rodoviários disponibilizados pela **Polícia Rodoviária Federal (PRF)**, abrangendo os anos de 2024 e 2025, totalizando aproximadamente 980 mil registros. O projeto envolve desde o tratamento inicial dos dados brutos até a criação de um data warehouse em modelo star schema, além da visualização dos resultados por meio de dashboards interativos no Power BI.

É possível visualizar as etapas do projeto, desde a ingestão dos dados brutos (Bronze Layer), passando pela limpeza e transformação (Silver Layer), até a modelagem dimensional e análise final (Gold Layer).

Como resultado, temos:

- [Mapa interativo dos sinistros](https://yagoas.github.io/SinistrosPRF/assets/mapa_sinistros.html)
- Dashboard no Power BI:
  <!-- ![Dashboard Power BI](../assets/dashboard_powerbi) -->


## 🏗️ Arquitetura do Projeto

O projeto segue a **arquitetura Medallion** com três camadas principais:

### 🥉 **Bronze Layer (Raw Data)**
- Dados brutos da PRF sem processamento
- Armazenamento em arquivos CSV originais
- Preservação da estrutura e formato original

### 🥈 **Silver Layer (Lakehouse)**
- Dados limpos e estruturados
- **PostgreSQL containerizado** como banco de dados
- **Jobs ETL automatizados** para ingestão e transformação
- Modelagem relacional com MER, DER, DLD e DDL
- Camada de qualidade e governança de dados

### 🥇 **Gold Layer (Data Warehouse)**
- **Modelo Star Schema** para análise dimensional
- Exportação em arquivos CSV otimizados
- Documentação completa (MER, DER, DLD, DDL)
- Dados agregados e prontos para visualização

## 🎯 Objetivos

- **Análise Exploratória**: Compreender os padrões de sinistros nas rodovias federais
- **Tratamento de Dados**: Implementar pipeline ETL automatizado
- **Modelagem Dimensional**: Criar data warehouse em star schema
- **Visualização**: Dashboards interativos no Power BI
- **Insights**: Extrair informações relevantes para políticas públicas de segurança viária

## 📊 Dados Utilizados

O projeto utiliza os dados oficiais de **sinistros rodoviários** disponibilizados pela **Polícia Rodoviária Federal (PRF)**, contendo:

- **Período**: 2024-2025
- **Registros**: Aproximadamente 980k registros e 120k sinistros
- **Variáveis**: 37 colunas incluindo localização, horário, tipo de acidente, vítimas, condições meteorológicas, veículos envolvidos, etc.
- **Referência**: <a ref="https://www.gov.br/prf/pt-br/acesso-a-informacao/dados-abertos/dados-abertos-da-prf"><b>Dados Abertos da PRF (Agrupados por pessoa - Todas as causas e tipos de acidentes)</b></a>. Acessado em: 13/09/2025

### 📁 Estrutura do Projeto

```
📦 SDB2 - Projeto/
├── 📂 assets/                    # Repositório auxiliar
├── 📂 bronze/                    # Camada Bronze (Raw Data)
│   ├── data/
│   │   ├── acidentes2024_todas_causas_tipos.csv
│   │   └── acidentes2025_todas_causas_tipos.csv
│   └── README.md
├── 📂 silver/                    # Camada Silver (Lakehouse)
│   ├── database/
│   │   ├── docker-compose.yml
│   │   ├── init.sql
│   │   └── Dockerfile
│   ├── etl/
│   │   ├── jobs/
│   │   └── scripts/
│   ├── models/
│   │   ├── MER/
│   │   ├── DER/
│   │   ├── DLD/
│   │   └── DDL/
│   └── README.md
├── 📂 gold/                      # Camada Gold (Data Warehouse)
│   ├── data/
│   │   ├── dim_tempo.csv
│   │   ├── dim_localizacao.csv
│   │   ├── dim_pessoa.csv
│   │   ├── dim_veiculo.csv
│   │   └── fato_sinistros.csv
│   ├── models/
│   │   ├── MER/
│   │   ├── DER/
│   │   ├── DLD/
│   │   └── DDL/
│   └── README.md
├── 📂 visualization/             # Visualizações
│   ├── powerbi/
│   │   └── dashboard_sinistros.pbix
│   └── reports/
├── 📂 notebooks/                 # Análises exploratórias
│   └── exploratory_analysis.ipynb
├── 📂 config/                    # Configurações
│   ├── database.env
│   └── etl_config.yaml
└── 📄 README.md
```

## 🛠️ Tecnologias Utilizadas

### **Linguagem & Ambiente**
- **Python 3.11+** - Linguagem principal
- **Jupyter Notebook** - Ambiente de desenvolvimento interativo
- **Visual Studio Code** - IDE principal com extensões para Jupyter

### **Camada Bronze**
- **CSV Files** - Armazenamento de dados brutos

### **Camada Silver (Lakehouse)**
- **PostgreSQL** - Banco de dados relacional
- **Docker & Docker Compose** - Containerização
- **?** Orquestração de jobs ETL
- **?** - ORM para interação com o banco

### **Camada Gold (Data Warehouse)**
- **Star Schema** - Modelagem dimensional
- **CSV Export** - Formato de saída otimizado

### **Visualização & Análise**
- **Power BI** - Dashboards e relatórios
- **Pandas** - Manipulação e análise de dados
- **NumPy** - Operações numéricas
- **Matplotlib & Seaborn** - Visualização estatística
- **TQDM** - Barra de progresso para scripts
- **Folium & Branca** - Mapas interativos

### **Modelagem de Dados**
```python
├── PostgreSQL      # Banco relacional (Silver)
├── Star Schema     # Modelagem dimensional (Gold)
├── MER/DER/DLD     # Documentação de modelagem
└── DDL Scripts     # Scripts de criação
```

## 🚀 Como Executar

1. **Clone o repositório**
2. **Configure o ambiente Silver** (PostgreSQL + Docker)
3. **Execute os jobs ETL** para popular o Lakehouse
4. **Gere a camada Gold** com modelo Star Schema
5. **Conecte o Power BI** aos dados da Gold Layer
6. **Visualize os dashboards** e extraia insights

## 📈 Resultados Esperados

- **Data Pipeline automatizado** da Bronze à Gold
- **Banco de dados normalizado** na Silver Layer
- **Data Warehouse dimensional** na Gold Layer
- **Dashboards interativos** no Power BI
- **Documentação técnica completa** de todos os modelos
- **Insights** sobre **segurança viária**

## 👥 Equipe

**Disciplina**: SDB2 - Sistemas de Banco de Dados 2  
**Instituição**: Universidade de Brasília (UnB)  
**Período**: 2º Semestre de 2025  
**Integrantes**:
<div align="center">
<table>
  <tr>
    <td align="center">
      <a href="https://github.com/Yagoas">
        <img style="border-radius: 50%;" src="https://github.com/Yagoas.png" width="190px;" alt=""/>
        <br /><sub><b>Yago Amin Santos (19/0101091)</b></sub>
      </a>
    </td>
    <td align="center">
      <!-- <a href="https://github.com/USER">
        <img style="border-radius: 50%;" src="https://github.com/Yagoas.png" width="190px;" alt=""/>
        <br /><sub><b>NOME (MATRICULA)</b></sub> -->
      </a>
    </td>
    <td align="center">
      <!-- <a href="https://github.com/USER">
        <img style="border-radius: 50%;" src="https://github.com/Yagoas.png" width="190px;" alt=""/>
        <br /><sub><b>NOME (MATRICULA)</b></sub> -->
      </a>
    </td>
    <td align="center">
      <!-- <a href="https://github.com/USER">
        <img style="border-radius: 50%;" src="https://github.com/Yagoas.png" width="190px;" alt=""/>
        <br /><sub><b>NOME (MATRICULA)</b></sub>
      </a> -->
    </td>
  </tr>
</table>
</div>

## 📝 Licença
Este projeto foi desenvolvido para fins acadêmicos na UnB e sua replicação deve ser devidamente referenciada.