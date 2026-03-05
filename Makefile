.PHONY: up down test lint

up:
	docker compose up --build

down:
	docker compose down

lint:
	ruff check .

test:
	pytest
