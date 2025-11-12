# MAKEFILE PARA DOCKER - COMANDOS SIMPLIFICADOS

# Variáveis
COMPOSE = compose.yml

.PHONY: help setup start stop restart etl logs clean status build check

#!/bin/bash

# Comando padrão
help: ## Mostrar esta ajuda
	@echo ""
	@echo "SINISTROS PRF - DOCKER COMMANDS"
	@echo "=================================="
	@echo ""
	@echo "COMANDOS PRINCIPAIS:"
	@echo "  make setup       - Setup completo (primeira vez)"
	@echo "  make db          - Subir apenas banco + pgAdmin"
	@echo "  make etl-silver  - Executar ETL Silver (Bronze → Silver)"
	@echo "  make etl-gold    - Executar ETL Gold (Silver → DW)"
	@echo "  make etl-full    - Executar ETL completo (Silver + Gold)"
	@echo "  make start       - Iniciar banco + interface"
	@echo "  make stop        - Parar todos containers"
	@echo "  make restart     - Reiniciar serviços"
	@echo "  make logs        - Ver logs dos containers"
	@echo "  make status      - Status dos containers"
	@echo "  make clean       - Limpar tudo (cuidado!)"
	@echo "  make build       - Build imagens"
	@echo ""
	@echo "ACESSO:"
	@echo "  PostgreSQL: localhost:5432"
	@echo "  pgAdmin:    http://localhost:8080"
	@echo "  User: admin / Password: admin123"
	@echo ""

check: ## Verificar se Docker está rodando
	@docker info >/dev/null 2>&1 || docker info >nul 2>&1 || (echo "Docker não está rodando!" && exit 1)
	@echo "Docker está rodando"

setup: check ## Setup completo - primeira execução
	@echo "SETUP COMPLETO"
	@echo "================================="
	@echo "1. Limpando containers antigos..."
	-@docker stop sinistros_postgres sinistros_pgadmin sinistros_etl_setup 2>nul
	-@docker rm sinistros_postgres sinistros_pgadmin sinistros_etl_setup 2>nul
	-@docker compose -f $(COMPOSE) down -v 2>nul
	@echo "2. Construindo imagens..."
	@docker compose -f $(COMPOSE) build
	@echo "3. Iniciando PostgreSQL + pgAdmin..."
	@docker compose -f $(COMPOSE) up -d postgres pgadmin
	@echo "4. Aguardando PostgreSQL ficar pronto..."
	@timeout /t 10 /nobreak >nul
	@echo "5. Executando ETL dos dados para o lakehouse..."
	@docker compose -f $(COMPOSE) --profile setup up etl_setup
	@echo ""
	@echo "Setup concluído!"
	@echo "pgAdmin: http://localhost:8080"
	@echo "PostgreSQL: localhost:5432"

db: check ## Subir apenas PostgreSQL + pgAdmin (cria se não existir)
	@echo "INICIANDO BANCO DE DADOS"
	@echo "=========================="
	@docker compose -f $(COMPOSE) up -d postgres pgadmin
	@echo ""
	@echo "Banco de dados iniciado!"
	@echo "pgAdmin: http://localhost:8080"
	@echo "PostgreSQL: localhost:5432"

etl-silver: check ## Executar apenas ETL Silver (Bronze → Silver)
	@echo "ETL SILVER: Bronze → Silver"
	@echo "=============================="
	@echo "Verificando se PostgreSQL está rodando..."
	@docker ps --filter "name=sinistros_postgres" --filter "status=running" -q >nul 2>&1 || (echo "❌ PostgreSQL não está rodando! Use 'make db' primeiro." && exit 1)
	@echo "Executando ETL Silver..."
	@docker compose -f $(COMPOSE) run --rm -e PYTHONPATH=/app/data_layer/silver --entrypoint "" etl_setup sh -c "cd /app/data_layer/silver && python etl/jobs/pipeline.py"
	@echo ""
	@echo "✅ ETL Silver concluído!"

etl-gold: check ## Executar apenas ETL Gold (Silver → DW)
	@echo "ETL GOLD: Silver → Data Warehouse"
	@echo "===================================="
	@echo "Verificando se PostgreSQL está rodando..."
	@docker ps --filter "name=sinistros_postgres" --filter "status=running" -q >nul 2>&1 || (echo "❌ PostgreSQL não está rodando! Use 'make db' primeiro." && exit 1)
	@echo "Executando ETL Gold..."
	@docker compose -f $(COMPOSE) run --rm -e PYTHONPATH=/app/data_layer/gold --entrypoint "" etl_setup sh -c "cd /app/data_layer/gold && python etl/jobs/pipeline.py"
	@echo ""
	@echo "✅ ETL Gold concluído!"

etl-full: check ## Executar ETL completo (Silver + Gold)
	@echo "ETL COMPLETO: Bronze → Silver → DW"
	@echo "====================================="
	@echo "Verificando se PostgreSQL está rodando..."
	@docker ps --filter "name=sinistros_postgres" --filter "status=running" -q >nul 2>&1 || (echo "❌ PostgreSQL não está rodando! Use 'make db' primeiro." && exit 1)
	@echo "Executando pipeline completo..."
	@docker compose -f $(COMPOSE) --profile setup up etl_setup
	@echo ""
	@echo "✅ ETL completo concluído!"

start: check ## Iniciar apenas banco + interface (dados existentes)
	@echo "INICIANDO SERVIÇOS"
	@echo "====================="
	@echo "Usando dados existentes..."
	@docker compose -f $(COMPOSE) up -d postgres pgadmin
	@echo ""
	@echo "Serviços iniciados!"
	@echo "pgAdmin: http://localhost:8080"
	@echo "PostgreSQL: localhost:5432"

stop: ## Parar todos os serviços
	@echo "PARANDO SERVIÇOS"
	@echo "==================="
	-@docker compose -f $(COMPOSE) down 2>/dev/null || docker compose -f $(COMPOSE) down 2>nul || true
	@echo "Todos os serviços foram parados!"

restart: stop start ## Reiniciar serviços

# Comandos de manutenção
	@echo "CONSTRUINDO IMAGENS"
	@echo "====================="
	@docker compose -f $(COMPOSE) build

logs: ## Ver logs dos containers
	@echo "LOGS DOS CONTAINERS"
	@echo "======================"
	@docker compose -f $(COMPOSE) logs -f 2>/dev/null || docker compose -f $(COMPOSE) logs -f 2>nul || echo "❌ Nenhum container está rodando!"

status: ## Status dos containers
	@echo "STATUS DOS CONTAINERS"
	@echo "========================"
	@docker ps --filter "name=sinistros" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" || echo "❌ Nenhum container encontrado"

clean: ## Limpeza completa (REMOVE TODOS OS DADOS!)
	@echo "LIMPEZA COMPLETA"
	@echo "=================="
	@echo "ATENÇÃO: Isso vai deletar TODOS os dados!"
	@echo "Removendo containers, volumes e imagens..."
	-@docker stop sinistros_postgres sinistros_pgadmin sinistros_etl_setup 2>nul
	-@docker rm sinistros_postgres sinistros_pgadmin sinistros_etl_setup 2>nul
	-@docker compose -f $(COMPOSE) down -v 2>nul
	-@docker volume rm sinistros_postgres_data 2>nul
	-@docker volume rm sinistros_pgadmin_data 2>nul
	-@docker volume rm sinistros_etl_logs 2>nul
	-@docker volume rm sinistros_etl_data 2>nul
	-@docker image rm sinistrosprf-etl_setup 2>nul
	-@docker image rm sdb2-projeto-etl_setup 2>nul
	@echo "Limpeza concluída!"
	

# Comandos de manutenção
build: check ## Build imagens
	@echo "CONSTRUINDO IMAGENS"
	@echo "====================="
	@docker compose -f $(COMPOSE) build

rebuild: clean setup ## Rebuild completo do zero

update: ## Atualizar imagens Docker
	@echo "ATUALIZANDO IMAGENS"
	@echo "====================="
	@docker compose -f $(COMPOSE) pull

# Comandos de debug
shell-postgres: ## Shell no container PostgreSQL
	@docker compose -f $(COMPOSE) exec postgres psql -U admin -d sinistros_prf

shell-etl: ## Shell no container ETL
	@docker compose -f $(COMPOSE) run --rm etl_setup /bin/bash

# Backup e restore
backup: ## Backup do banco
	@echo "CRIANDO BACKUP"
	@echo "================"
	@if not exist backups mkdir backups
	@docker compose -f $(COMPOSE) exec postgres pg_dump -U admin sinistros_prf > backups/sinistros_%date:~10,4%%date:~4,2%%date:~7,2%_%time:~0,2%%time:~3,2%%time:~6,2%.sql
	@echo "Backup criado em backups/"

# Informações do sistema
info: ## Informações do sistema
	@echo "INFORMAÇÕES DO SISTEMA"
	@echo "=========================="
	@echo "Docker Version:"
	@docker --version
	@echo ""
	@echo "Docker Compose Version:"
	@docker compose version
	@echo ""
	@echo "Volumes Docker:"
	@docker volume ls --filter "name=sinistros"
	@echo ""
	@echo "Imagens Docker:"
	@docker images --filter "reference=*sinistros*"
