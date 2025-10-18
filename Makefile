.PHONY: install test lint fmt clean

PY      ?= python3

install:
	@echo "Note: Installation is managed via pkgmgr."
	@echo "      To install this tool, run:"
	@echo "          pkgmgr install fidedu"
	@echo ""
	@echo "If you still want to run it locally without installing, use:"
	@echo "    $(PY) main.py /path/to/source /path/to/store [--compress] [-v]"

test:
	@echo "Running tests with $(PY)…"
	@$(PY) test.py -v

lint:
	@echo "No linter configured. You can run e.g.: ruff check . || true"

fmt:
	@echo "No formatter configured. You can run e.g.: ruff format . || true"

clean:
	@find . -name '__pycache__' -type d -exec rm -rf {} +
