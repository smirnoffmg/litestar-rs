.DEFAULT_GOAL := check

install:
	uv sync
	uv run pre-commit install

lint:
	uv run ruff check .
	uv run ruff format --check .

format:
	uv run ruff format .
	uv run ruff check --fix .

typecheck:
	uv run mypy

imports:
	uv run lint-imports

test:
	uv run pytest -m unit

test-int:
	uv run pytest -m integration

cov:
	uv run pytest --cov --cov-report=term-missing

docs:
	uv run mkdocs build --strict

check: lint typecheck imports test

.PHONY: install lint format typecheck imports test test-int cov docs check
