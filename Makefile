####
## Gramedia Digital - Data Engineer Take Home Test
## by Mario Caesar // caesarmario87@gmail.com
## Makefile for docker compose command
####

PROJECT_NAME ?= gramedia-de
COMPOSE_FILE ?= docker-compose.yml
ENV_FILE     ?= ./docker/.env
DC           ?= docker compose
COMPOSE_ARGS = -p $(PROJECT_NAME) --env-file $(ENV_FILE) -f $(COMPOSE_FILE)

.PHONY: up down restart nuke

up:
	$(DC) $(COMPOSE_ARGS) up -d --build

down:
	$(DC) $(COMPOSE_ARGS) down --remove-orphans

restart: down up

nuke:
	$(DC) $(COMPOSE_ARGS) down -v --remove-orphans
