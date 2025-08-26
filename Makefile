SHELL := /bin/bash
DC := docker-compose

# Load .env if present
ifneq (,$(wildcard .env))
  include .env
  export $(shell sed 's/=.*//' .env)
endif

# Ensure schemas exist in PostGIS
init-db:
	@echo "Creating schemas in PostGIS (if missing)…"
	@docker exec postgis psql -U $(POSTGRES_USER) -d $(POSTGRES_DB) -v ON_ERROR_STOP=1 -c "CREATE SCHEMA IF NOT EXISTS raw; CREATE SCHEMA IF NOT EXISTS stage; CREATE SCHEMA IF NOT EXISTS core; CREATE SCHEMA IF NOT EXISTS marts;"
	@echo "✅ Schemas ready: raw, stage, core, marts"

up:     ; $(DC) up --build
down:   ; $(DC) down
rset:   ; $(DC) down -v
duck:   ; $(DC) up --build duck
logs:   ; $(DC) logs -f --tail=200

psql:
	@docker exec -it postgis psql -U $(POSTGRES_USER) -d $(POSTGRES_DB)

lab:	; open "http://localhost:8888/?token=$(JUPYTER_TOKEN)"
rescue:	; docker cp jupyter:/home/jovyan /tmp/jupyter_home_backup