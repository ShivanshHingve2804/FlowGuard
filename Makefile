.PHONY: test lint benchmark run

test:
	pytest tests/ -v

lint:
	flake8 src/ tests/
	mypy src/ tests/

benchmark:
	python -m flowguard.cli.benchmark

run:
	python -m uvicorn examples.main:app --reload
