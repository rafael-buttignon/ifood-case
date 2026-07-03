.PHONY: help setup run shell test lint format

help:
	@printf "Available commands:\n"
	@printf "  make help    Show this help message.\n"
	@printf "  make setup   Create .venv and install local dependencies.\n"
	@printf "  make run     Run the local Silver and Gold pipeline.\n"
	@printf "  make shell   Open an interactive Spark shell with Delta enabled.\n"
	@printf "  make test    Run the test suite.\n"
	@printf "  make lint    Run Ruff checks and formatting validation.\n"
	@printf "  make format  Apply Ruff fixes and formatting.\n"

setup:
	python -m venv .venv
	.venv/bin/python -m pip install --upgrade pip
	.venv/bin/python -m pip install -r requirements.txt

run:
	SPARK_LOCAL_IP=127.0.0.1 .venv/bin/python -m core.runner

shell:
	SPARK_LOCAL_IP=127.0.0.1 .venv/bin/python -i -c "import atexit; from core.spark import LocalSparkService; spark = LocalSparkService.create_session(app_name='ifood-shell'); atexit.register(spark.stop); print('Spark session ready as spark')"

test:
	PYSPARK_PYTHON=.venv/bin/python SPARK_LOCAL_IP=127.0.0.1 \
		.venv/bin/python -m pytest

lint:
	.venv/bin/ruff check .
	.venv/bin/ruff format --check .

format:
	.venv/bin/ruff check --fix .
	.venv/bin/ruff format .
