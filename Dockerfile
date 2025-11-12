# Imagem base Python
# builder
FROM python:3.12.3 AS builder

# Criar ambiente virtual
RUN python -m venv /app/.venv

# Definir diretório de trabalho
WORKDIR /app

# Configurar PATH para usar o venv
ENV PATH="/app/.venv/bin:$PATH"

# # Copiar requirements
COPY requirements.txt /app/

# Instalar dependências
RUN pip install --upgrade pip
RUN pip install -r requirements.txt


# Copiar código fonte
COPY data_layer/bronze/ /app/data_layer/bronze/
COPY data_layer/silver/etl/ /app/data_layer/silver/etl/
COPY data_layer/silver/database/init.sql /app/data_layer/silver/database/init.sql

# Criar diretórios necessários
RUN mkdir -p /app/data_layer/silver/data

# Definir PYTHONPATH
ENV PYTHONPATH=/app/data_layer/silver

# runtime
FROM python:3.12.3-slim AS runtime

# Copiar ambiente virtual do builder
COPY --from=builder /app/.venv/ /app/.venv/

# Copiar arquivos fonte do builder
COPY --from=builder /app/data_layer/bronze/ /app/data_layer/bronze/
COPY --from=builder /app/data_layer/silver/ /app/data_layer/silver/

# Instalar dependências do sistema para PostgreSQL
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Configurar ambiente
ENV PATH=/app/.venv/bin:$PATH \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/data_layer/silver

WORKDIR /project

# Copiar script de entrada Python
COPY docker_entrypoint.py /app/entrypoint.py
RUN chmod +x /app/entrypoint.py

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import sys; sys.exit(0)"

# Usar Python como entrypoint
ENTRYPOINT ["python", "/app/entrypoint.py"]