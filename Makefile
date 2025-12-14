.PHONY: install test lint fmt clean

PY ?= python3

install:
	@echo "Note: Installation is managed via pkgmgr."
	@echo "      To install this tool, run:"
	@echo "          pkgmgr install fidedu"
	@echo ""
	@echo "If you still want to run it locally without installing, use:"
	@echo "    $(PY) -m fidedu.cli /path/to/scan [--compress] [-v]"

test:
	@echo "Running tests with $(PY)…"
	$(PY) -m unittest discover -s tests -p "test_*.py" -v

lint:
	@echo "Running ruff…"
	ruff check .

fmt:
	@echo "Formatting with ruff…"
	ruff format .

clean:
	find . -name '__pycache__' -type d -exec rm -rf {} +
	find . -name '*.pyc' -delete
	find . -name '*.egg-info' -type d -exec rm -rf {} +
	find . -name build -type d -exec rm -rf {} +
	find . -name dist -type d -exec rm -rf {} +

install:
	@echo "Installing fidedu in editable mode…"
	$(PY) -m pip install --upgrade pip
	$(PY) -m pip install -e .
