# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ##### Criação do Catalogo, Schemas e Volumes

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS ifood_case;
# MAGIC CREATE SCHEMA IF NOT EXISTS ifood_case.landing;
# MAGIC CREATE SCHEMA IF NOT EXISTS ifood_case.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS ifood_case.gold;
# MAGIC CREATE VOLUME IF NOT EXISTS ifood_case.landing.source_files;
# MAGIC
# MAGIC SHOW CATALOGS;
# MAGIC SHOW SCHEMAS IN ifood_case;
# MAGIC SHOW VOLUMES IN ifood_case.landing;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Essa é basicamente uma etapa opcional de validação.
# MAGIC - Esse notebook valida se os arquivos .parquet esperados chegaram corretamente na pasta Landing antes de continuar o processamento.

# COMMAND ----------

# Databricks notebook source

from __future__ import annotations

import logging

LANDING_PATH = "/Volumes/ifood_case/landing/source_files"
EXPECTED_FILES = [f"yellow_tripdata_2023-{month:02d}.parquet" for month in range(1, 6)]
logger = logging.getLogger(__name__)


logger.info(f"Landing path: {LANDING_PATH}")
for file_name in EXPECTED_FILES:
    logger.info(f"- {file_name}")

files = dbutils.fs.ls(LANDING_PATH)
logger.info(f"Files found: {len(files)}")

uploaded_files = {
    file_info.name for file_info in files if file_info.name.endswith(".parquet")
}

missing_files = sorted(set(EXPECTED_FILES) - uploaded_files)
unexpected_files = sorted(uploaded_files - set(EXPECTED_FILES))

logger.info(f"Missing files: {missing_files}")
logger.info(f"Unexpected files: {unexpected_files}")

display(files)

# COMMAND ----------

df = spark.read.parquet(
    "/Volumes/ifood_case/landing/source_files/yellow_tripdata_2023-05.parquet"
)

display(df)
