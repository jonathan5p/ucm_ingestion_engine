.PHONY: test format

test:
	uv run -m pytest -v --disable-warnings -n 4 --cov=src/ingestion_engine --cov-report=html:coverage_html_report --cov-fail-under=80 tests/ 

# Lint and format code
format:
	uv run ruff check --fix src/
	uv run ruff format src/
