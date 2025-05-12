VENV_DIR := .venv
REQUIREMENTS := requirements.txt

.PHONY: setup test format activate deactivate clean help
setup:
	@echo "Creating virtual environment..."
	@uv venv --python=python3.11
	@echo "Virtual environment created at $(VENV_DIR)"
	@echo "Activating virtual environment..."
	@sh $(VENV_DIR)/bin/activate
	@echo "Installing dependencies..."
	@uv sync
	@echo "Project setup complete."

# Run tests
test:
	@echo "Running tests..."
	uv run -m pytest -v --disable-warnings -n 4 --cov=src/ingestion_engine --cov-report=html:coverage_html_report --cov-fail-under=80 tests/

# Format code
format:
	@echo "Formatting code..."
	uv run ruff check --fix src/
	uv run ruff format src/

# Activate the virtual environment
activate:
	@echo "Activating virtual environment..."
	@source $(VENV_DIR)/bin/activate
	@echo "Virtual environment activated"

# Deactivate the virtual environment
deactivate:
	@echo "Deactivating virtual environment..."
	deactivate
	@echo "Virtual environment deactivated"

# Clean up (remove virtual environment)
clean:
	@echo "Cleaning up..."
	rm -rf $(VENV_DIR)
	@echo "Virtual environment removed"

# List available targets
help:
	@echo "Available targets:"
	@echo "  setup       - Create and set up the virtual environment"
	@echo "  test        - Run tests with pytest"
	@echo "  format      - Format code with ruff"
	@echo "  activate    - Activate the virtual environment"
	@echo "  deactivate  - Deactivate the virtual environment"
	@echo "  clean       - Remove the virtual environment"
	@echo "  shell       - Start a shell in the virtual environment"
	@echo "  help        - Show this help message"
