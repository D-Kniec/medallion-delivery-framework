.PHONY: init up down restart logs psql-warehouse psql-airflow clean

DC = docker-compose
CONTAINER_DB = pizza_postgres
CONTAINER_SCHEDULER = pizza_airflow_scheduler

init:
	mkdir -p secrets logs chk src/bronze src/silver src/gold
	touch secrets/postgres_password.txt secrets/airflow_db_password.txt

up:
	$(DC) up -d --build --remove-orphans

down:
	$(DC) down

restart: down up

logs:
	$(DC) logs -f $(CONTAINER_SCHEDULER)

psql-warehouse:
	docker exec -it $(CONTAINER_DB) psql -U admin_user -d warehouse_db

psql-airflow:
	docker exec -it $(CONTAINER_DB) psql -U airflow_user -d airflow_metadata

clean:
	docker system prune -f