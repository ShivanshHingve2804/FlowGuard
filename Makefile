.PHONY: test lint format benchmark run docker-up docker-down

test:
	pytest tests/ -v --cov=flowguard --cov-report=term-missing

lint:
	ruff check .
	ruff format --check .
	mypy src/

format:
	ruff check --fix .
	ruff format .

benchmark:
	python -m flowguard.cli.benchmark --algorithm all --requests 10000 --concurrency 50 --keys 16

run:
	python -m uvicorn examples.demo_app:app --reload --port 8888

docker-up:
	docker compose up -d --build

docker-down:
	docker compose down -v
