from __future__ import annotations

from abc import ABC
from datetime import date, datetime

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class BaseTransformService(ABC):
    """Shared helpers for DataFrame transformation services."""

    @staticmethod
    def cast_timestamp_ntz(dataframe: DataFrame, column_name: str) -> DataFrame:
        return dataframe.withColumn(
            column_name,
            F.col(column_name).cast("timestamp_ntz"),
        )


class SilverTransformService(BaseTransformService):
    REQUIRED_SOURCE_COLUMNS = (
        "VendorID",
        "passenger_count",
        "total_amount",
        "tpep_dropoff_datetime",
        "tpep_pickup_datetime",
    )

    SILVER_COLUMNS = (
        "VendorID",
        "passenger_count",
        "total_amount",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "pickup_month",
        "pickup_hour",
        "source_file",
        "ingested_at",
    )

    @classmethod
    def normalize_source_schema(cls, dataframe: DataFrame) -> DataFrame:
        return dataframe.withColumn(
            "VendorID",
            F.col("VendorID").cast("long"),
        ).withColumn("passenger_count", F.col("passenger_count").cast("double"))

    @classmethod
    def build_validated_dataframe(
        cls,
        dataframe: DataFrame,
        *,
        expected_month: date,
        source_file: str,
        ingested_at: datetime,
    ) -> DataFrame:
        missing_columns = [
            column_name
            for column_name in cls.REQUIRED_SOURCE_COLUMNS
            if column_name not in dataframe.columns
        ]
        if missing_columns:
            missing_list = ", ".join(missing_columns)
            raise ValueError(f"Missing required columns: {missing_list}")

        month_start = expected_month.replace(day=1)
        next_month = date(
            month_start.year + month_start.month // 12,
            month_start.month % 12 + 1,
            1,
        )

        normalized = cls.normalize_source_schema(dataframe).withColumn(
            "total_amount",
            F.col("total_amount").cast("double"),
        )
        normalized = cls.cast_timestamp_ntz(normalized, "tpep_pickup_datetime")
        normalized = cls.cast_timestamp_ntz(normalized, "tpep_dropoff_datetime")

        return (
            normalized.filter(
                (F.col("tpep_pickup_datetime") >= F.lit(month_start))
                & (F.col("tpep_pickup_datetime") < F.lit(next_month))
            )
            .filter(F.col("tpep_dropoff_datetime") >= F.col("tpep_pickup_datetime"))
            .filter(F.col("VendorID").isNotNull() & F.col("total_amount").isNotNull())
            .withColumn(
                "pickup_month",
                F.trunc("tpep_pickup_datetime", "month").cast("date"),
            )
            .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
            .withColumn("source_file", F.lit(source_file))
            .withColumn("ingested_at", F.lit(ingested_at).cast("timestamp"))
            .select(*cls.SILVER_COLUMNS)
        )


class GoldTransformService(BaseTransformService):
    @staticmethod
    def build_monthly_average_total_amount(dataframe: DataFrame) -> DataFrame:
        return (
            dataframe.groupBy("pickup_month")
            .agg(
                F.avg("total_amount").alias("average_total_amount"),
                F.count(F.lit(1)).alias("trip_count"),
                F.sum(
                    F.when(F.col("total_amount") < 0, F.lit(1)).otherwise(F.lit(0))
                ).alias("negative_amount_count"),
            )
            .select(
                "pickup_month",
                F.col("average_total_amount")
                .cast("double")
                .alias("average_total_amount"),
                F.col("trip_count").cast("long").alias("trip_count"),
                F.col("negative_amount_count")
                .cast("long")
                .alias("negative_amount_count"),
            )
        )

    @staticmethod
    def build_may_hourly_average_passengers(dataframe: DataFrame) -> DataFrame:
        return (
            dataframe.filter(F.col("pickup_month") == F.lit(date(2023, 5, 1)))
            .groupBy("pickup_hour")
            .agg(
                F.avg("passenger_count").alias("average_passenger_count"),
                F.count(F.lit(1)).alias("trip_count"),
                F.sum(
                    F.when(F.col("passenger_count").isNull(), F.lit(1)).otherwise(
                        F.lit(0)
                    )
                ).alias("null_passenger_count"),
            )
            .select(
                "pickup_hour",
                F.col("average_passenger_count")
                .cast("double")
                .alias("average_passenger_count"),
                F.col("trip_count").cast("long").alias("trip_count"),
                F.col("null_passenger_count")
                .cast("long")
                .alias("null_passenger_count"),
            )
        )
