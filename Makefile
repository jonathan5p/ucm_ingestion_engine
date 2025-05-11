.PHONY: test format

test:
	uv run -m pytest -v --disable-warnings -n 4 --cov=src/ingestion_engine --cov-report=html:coverage_html_report --cov-fail-under=80 tests/ 

# Lint and format code
format:
	uv run ruff . --fix --ignore E501,E203,W503,W504,E722,E402,W605,W601,F841,F405,F841,F405,E501,E203,W503,W504,E722,E402,W605,W601,F841,F405,F841,F405
	uv run ruff format .
