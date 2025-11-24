# Imagem base do PostgreSQL
FROM postgres:15-alpine

# Informações do maintainer
LABEL maintainer="SinistrosPRF"
LABEL description="Banco de dados PostgreSQL para análise de sinistros PRF"

# Variáveis de ambiente padrão
ENV POSTGRES_DB=sinistros_prf
ENV POSTGRES_USER=prf_user
ENV POSTGRES_PASSWORD=prf_pass

# Copiar scripts DDL para o diretório de init do PostgreSQL
COPY data_layer/silver/ddl.sql /docker-entrypoint-initdb.d/01_silver_ddl.sql
COPY data_layer/gold/ddl.sql /docker-entrypoint-initdb.d/02_gold_ddl.sql

# Expor porta padrão do PostgreSQL
EXPOSE 5432