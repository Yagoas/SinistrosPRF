#!/bin/bash

echo "🚀 Iniciando ETL Pipeline Silver Lakehouse..."

# Aguardar PostgreSQL ficar pronto (tempo fixo + verificação simples)
echo "⏳ Aguardando PostgreSQL inicializar..."
sleep 10

echo "🔍 Testando conexão..."
python -c "
import psycopg2
try:
    conn = psycopg2.connect(host='postgres', port=5432, user='admin', password='admin123', database='sinistros_prf')
    print('✅ PostgreSQL está pronto!')
    conn.close()
except Exception as e:
    print(f'❌ Erro de conexão: {e}')
    print('Tentando mesmo assim...')
"

# Executar pipeline ETL 
echo "🔄 Executando Pipeline ETL..."

# Definir PYTHONPATH para incluir o diretório raiz
export PYTHONPATH=/usr/src:$PYTHONPATH

cd /usr/src/silver/etl/jobs

# Modificar imports no pipeline.py para usar imports relativos
sed -i 's/from silver.etl.jobs.transform/from transform/g' pipeline.py
sed -i 's/from silver.etl.jobs.load/from load/g' pipeline.py

python pipeline.py \
    --bronze-path /usr/src/bronze \
    --output-dir /usr/src/silver/data

echo "🎉 Pipeline ETL concluído!"

# Manter container rodando para inspeção (opcional)
if [ "$KEEP_ALIVE" = "true" ]; then
    echo "💤 Container em modo de espera..."
    tail -f /dev/null
fi