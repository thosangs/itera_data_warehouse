ifneq (,$(wildcard ./.env))
    include .env
    export
endif

.PHONY: help
help: ## Show this help
	@echo "Commands:"
	@grep -hE '^[a-zA-Z0-9_-]+:.*## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

setup: ## Create .env with defaults if missing
	@echo "Preparing .env..."
	@if [ ! -f .env ]; then \
		echo "SA_PASSWORD=YourStrong!Passw0rd" > .env; \
		echo "DB_NAME=PracticeDB" >> .env; \
		echo "SQLPAD_ADMIN=admin@example.com" >> .env; \
		echo "SQLPAD_ADMIN_PASSWORD=changeme" >> .env; \
		echo "AIRFLOW_CONTAINER_NAME=airflow" >> .env; \
		echo "AIRFLOW_HOST_NAME=airflow" >> .env; \
		echo "AIRFLOW_WEBSERVER_PORT=80" >> .env; \
		echo ".env created. Please review and adjust as needed."; \
	else \
		echo ".env already exists; skipping."; \
	fi

up: ## Start services for a specific day (use: make up DAY=8)
	@if [ -z "$(DAY)" ]; then echo "Please provide DAY, e.g. make up DAY=8" && exit 1; fi
	@COMPOSE_FILE="lectures/day_$(DAY)/docker/docker-compose.yml"; \
	if [ ! -f "$$COMPOSE_FILE" ]; then \
	  echo "Compose file not found: $$COMPOSE_FILE" && exit 1; \
	fi; \
	echo "Starting services using $$COMPOSE_FILE..."; \
	docker-compose -f "$$COMPOSE_FILE" --env-file .env up -d

down: ## Stop services for a specific day (use: make down DAY=8)
	@if [ -z "$(DAY)" ]; then echo "Please provide DAY, e.g. make down DAY=8" && exit 1; fi
	@COMPOSE_FILE="lectures/day_$(DAY)/docker/docker-compose.yml"; \
	if [ ! -f "$$COMPOSE_FILE" ]; then \
	  echo "Compose file not found: $$COMPOSE_FILE" && exit 1; \
	fi; \
	echo "Stopping services using $$COMPOSE_FILE..."; \
	docker-compose -f "$$COMPOSE_FILE" --env-file .env down

clean: ## Remove containers, volumes, and orphans for a specific day (use: make clean DAY=8)
	@if [ -z "$(DAY)" ]; then echo "Please provide DAY, e.g. make clean DAY=8" && exit 1; fi
	@COMPOSE_FILE="lectures/day_$(DAY)/docker/docker-compose.yml"; \
	if [ ! -f "$$COMPOSE_FILE" ]; then \
	  echo "Compose file not found: $$COMPOSE_FILE" && exit 1; \
	fi; \
	echo "Cleaning up using $$COMPOSE_FILE..."; \
	docker-compose -f "$$COMPOSE_FILE" --env-file .env down --volumes --remove-orphans

# --- Labs: generic day runner ---
# Usage: make day-run DAY=8
# Expects SQL at /opt/workspace/day_$(DAY)/sql inside container (mounted read-only)

define SQLCMD_IN_CONTAINER
/bin/bash -lc 'if [ -x /opt/mssql-tools18/bin/sqlcmd ]; then SQLCMD="/opt/mssql-tools18/bin/sqlcmd -C"; elif [ -x /opt/mssql-tools/bin/sqlcmd ]; then SQLCMD=/opt/mssql-tools/bin/sqlcmd; else echo "sqlcmd not found" >&2; exit 1; fi; $$SQLCMD -S localhost -U sa -P $$SA_PASSWORD -d $$DB_NAME -b -i "$$1"'
endef


.PHONY: day-run
day-run: ## Start services and run all SQL files for DAY (e.g., DAY=8)
	@if [ -z "$(DAY)" ]; then echo "Please provide DAY, e.g. make day-run DAY=8" && exit 1; fi
	@$(MAKE) up DAY=$(DAY)
	@echo "Waiting for mssql to be healthy..."; \
	until [ "$$(/usr/bin/env docker inspect -f '{{.State.Health.Status}}' mssql 2>/dev/null)" = "healthy" ]; do \
	  sleep 2; printf "."; \
	done; echo " mssql is healthy.";
	@DAY=$(DAY) bash scripts/day-run.sh