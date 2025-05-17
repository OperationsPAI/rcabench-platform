# https://github.com/casey/just
# https://docs.astral.sh/uv/

dev:
    uv sync
    just fmt
    just lint

fmt:
    uv run ruff format

lint:
    uv run ruff check
    uv run pyright --pythonpath .venv/bin/python

ci:
    uv sync --locked
    uv run ruff format --check
    just lint
    uv build
