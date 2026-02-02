.PHONY: init check-secrets up down restart logs psql-warehouse psql-meta clean nuke setup-gold help

DC = docker-compose
CONTAINER_META = pizza_metadata_db
CONTAINER_WAREHOUSE = pizza_warehouse_db
CONTAINER_SCHEDULER = pizza_airflow_scheduler
GOLD_SCHEMA_FILE = models/gold_schema_md.sql

help:
	@echo "🛠️  Commands:"
	@echo "  make init           - Setup folders and prompt for passwords"
	@echo "  make up             - Build and start containers"
	@echo "  make setup-gold     - 🏗️  Apply Gold Schema from $(GOLD_SCHEMA_FILE)"
	@echo "  make down           - Stop containers"
	@echo "  make restart        - Restart containers"
	@echo "  make logs           - Tail Airflow Scheduler logs"
	@echo "  make psql-warehouse - Connect to Data Warehouse"
	@echo "  make psql-meta      - Connect to Metadata DB"
	@echo "  make clean          - Prune docker & clean checkpoints"
	@echo "  make nuke           - Destroy everything including volumes!"

check-secrets:
	@mkdir -p secrets
	@if [ ! -s secrets/postgres_password.txt ]; then \
		printf "🔑 Podaj hasło dla ROOT Postgresa (postgres_password.txt): "; \
		read pass; \
		echo "$$pass" > secrets/postgres_password.txt; \
	fi
	@if [ ! -s secrets/airflow_db_password.txt ]; then \
		printf "🔑 Podaj hasło dla AIRFLOW DB (airflow_db_password.txt): "; \
		read pass; \
		echo "$$pass" > secrets/airflow_db_password.txt; \
	fi
	@if [ ! -s secrets/warehouse_password.txt ]; then \
		printf "🔑 Podaj hasło dla WAREHOUSE DB (warehouse_password.txt): "; \
		read pass; \
		echo "$$pass" > secrets/warehouse_password.txt; \
	fi

init: check-secrets
	@echo "📂 Creating project structure..."
	mkdir -p logs secrets models
	mkdir -p data/bronze data/silver data/gold data/checkpoints data/archive
	mkdir -p src/common src/generator src/pipelines/silver src/pipelines/gold
	@echo "📜 Creating SQL init files if missing..."
	@touch init_metadata.sql
	@touch init_warehouse.sql
	@echo "✅ Init complete."

setup-gold:
	@echo "🏗️  Wdrażanie schematu GOLD z pliku $(GOLD_SCHEMA_FILE)..."
	@if [ -f $(GOLD_SCHEMA_FILE) ]; then \
		cat $(GOLD_SCHEMA_FILE) | docker exec -i $(CONTAINER_WAREHOUSE) psql -U warehouse_admin -d data_warehouse; \
		echo "✅ Schemat wdrożony pomyślnie."; \
	else \
		echo "❌ Błąd: Nie znaleziono pliku $(GOLD_SCHEMA_FILE)"; \
	fi

up: check-secrets
	@echo "🐳 Starting containers..."
	$(DC) up -d --build --remove-orphans
	@echo "✅ System is running!"

down:
	$(DC) down

restart: down up

logs:
	$(DC) logs -f $(CONTAINER_SCHEDULER)

psql-warehouse:
	@echo "🔌 Connecting to Data Warehouse..."
	docker exec -it $(CONTAINER_WAREHOUSE) psql -U warehouse_admin -d data_warehouse

psql-meta:
	@echo "🔌 Connecting to Metadata DB..."
	docker exec -it $(CONTAINER_META) psql -U root_admin -d postgres

clean:
	@echo "🧹 Cleaning up..."
	docker system prune -f
	rm -rf data/checkpoints/*

nuke:
	@echo "NUKING EVERYTHING (Volumes included)..."
	$(DC) down -v
	rm -rf data/checkpoints/*
	@echo "✅ Clean slate."