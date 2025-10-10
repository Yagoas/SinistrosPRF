# Imagem base Python
# builder
FROM python:3.12.3 AS builder

# Criar ambiente virtual
RUN python -m venv /usr/src/.venv

# Definir diretório de trabalho
WORKDIR /usr/src

# Configurar PATH para usar o venv
ENV PATH="/usr/src/.venv/bin:$PATH"

# # Copiar requirements
COPY requirements.txt /usr/src/

# Instalar dependências
RUN pip install --upgrade pip
RUN pip install -r requirements.txt


# Copiar código fonte
COPY bronze/ /usr/src/bronze/
COPY silver/etl/ /usr/src/silver/etl/
COPY silver/database/init.sql /usr/src/silver/database/init.sql

# Criar diretórios necessários
RUN mkdir -p /usr/src/silver/data

# Definir PYTHONPATH
ENV PYTHONPATH=/usr/src/silver

# runtime
FROM python:3.12.3-slim AS runtime

# Copiar ambiente virtual do builder
COPY --from=builder /usr/src/.venv/ /usr/src/.venv/

# Copiar arquivos fonte do builder
COPY --from=builder /usr/src/bronze/ /usr/src/bronze/
COPY --from=builder /usr/src/silver/ /usr/src/silver/

# Instalar dependências do sistema para PostgreSQL
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Configurar ambiente
ENV PATH=/usr/src/.venv/bin:$PATH \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/usr/src/silver

WORKDIR /project

# Copiar script de entrada Python
COPY docker_entrypoint.py /usr/src/entrypoint.py
RUN chmod +x /usr/src/entrypoint.py

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import sys; sys.exit(0)"

# Usar Python como entrypoint
ENTRYPOINT ["python", "/usr/src/entrypoint.py"]