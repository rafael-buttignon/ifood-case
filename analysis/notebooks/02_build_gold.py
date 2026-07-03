# Databricks notebook source
# MAGIC %md
# MAGIC ##### Instalação do Projeto

# COMMAND ----------

# MAGIC %pip install git+https://github.com/rafael-buttignon/ifood-case.git

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Esse notebook usa funções internas do projeto para construir as tabelas Gold e usa select, round, date_format, lpad e cálculos simples apenas para mostrar o resultado no display. A lógica real da Gold está nas funções build_monthly_average_total_amount e build_may_hourly_average_passengers.

# COMMAND ----------

# Databricks notebook source

from pyspark.sql import functions as F

from core.sync import DeltaSyncService
from core.transform import GoldTransformService

silver_trips = spark.read.table("ifood_case.silver.yellow_trips")

monthly_gold = GoldTransformService.build_monthly_average_total_amount(silver_trips)
may_gold = GoldTransformService.build_may_hourly_average_passengers(silver_trips)

DeltaSyncService.overwrite_table(
    monthly_gold,
    "ifood_case.gold.monthly_average_total_amount",
)
DeltaSyncService.overwrite_table(
    may_gold,
    "ifood_case.gold.may_hourly_average_passengers",
)

display(
    monthly_gold.select(
        F.date_format("pickup_month", "yyyy-MM").alias("pickup_month"),
        F.round("average_total_amount", 2).alias("average_total_amount"),
        F.col("trip_count").alias("trip_count"),
        F.col("negative_amount_count").alias("negative_amount_count"),
        F.round(
            F.col("negative_amount_count") * F.lit(100.0) / F.col("trip_count"),
            2,
        ).alias("negative_amount_pct"),
    ).orderBy("pickup_month")
)

display(
    may_gold.select(
        F.lpad(F.col("pickup_hour").cast("string"), 2, "0").alias("pickup_hour"),
        F.round("average_passenger_count", 2).alias("average_passenger_count"),
        F.col("trip_count").alias("trip_count"),
        F.col("null_passenger_count").alias("null_passenger_count"),
        F.round(
            F.col("null_passenger_count") * F.lit(100.0) / F.col("trip_count"),
            2,
        ).alias("null_passenger_pct"),
    ).orderBy("pickup_hour")
)


# COMMAND ----------

spark.read.table("ifood_case.silver.yellow_trips").display()
