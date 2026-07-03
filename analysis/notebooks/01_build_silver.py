# Databricks notebook source
# MAGIC %md
# MAGIC ##### Instalação do Projeto

# COMMAND ----------

# MAGIC %pip install git+https://github.com/rafael-buttignon/ifood-case.git

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Essa função transforma os dados brutos da Landing em uma Silver mais limpa e confiável. Ela valida colunas obrigatórias, ajusta tipos, filtra registros inválidos, mantém apenas dados do mês correto, adiciona metadados e entrega um schema final padronizado.

# COMMAND ----------

# Databricks notebook source

from pyspark.sql import functions as F

from core.connector import LandingFileConnector
from core.sync import DeltaSyncService

LANDING_PATH = "/Volumes/ifood_case/landing/source_files"
SILVER_TABLE = "ifood_case.silver.yellow_trips"

silver_trips = LandingFileConnector(spark, LANDING_PATH).load()

DeltaSyncService.overwrite_table(silver_trips, SILVER_TABLE)

display(
    silver_trips.groupBy("source_file")
    .count()
    .select(
        F.col("source_file"),
        F.col("count").alias("trip_count"),
        F.date_format(
            F.to_date(
                F.regexp_extract("source_file", r"(2023-\d{2})", 1),
                "yyyy-MM",
            ),
            "yyyy-MM",
        ).alias("pickup_month"),
    )
    .orderBy("source_file")
)

# COMMAND ----------

spark.read.table("ifood_case.silver.yellow_trips").display()
