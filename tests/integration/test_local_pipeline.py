from datetime import date, datetime
from pathlib import Path

import pytest
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StructField,
    StructType,
    TimestampType,
)

from core.runner import LocalPipelineRunner


class TestLocalPipelineRunnerIntegration:
    @staticmethod
    def _build_month_schema(month: int) -> StructType:
        vendor_type = LongType() if month == 1 else IntegerType()
        passenger_type = DoubleType() if month in (1, 5) else LongType()
        return StructType(
            [
                StructField("VendorID", vendor_type, False),
                StructField("passenger_count", passenger_type, True),
                StructField("total_amount", DoubleType(), False),
                StructField("tpep_pickup_datetime", TimestampType(), False),
                StructField("tpep_dropoff_datetime", TimestampType(), False),
            ]
        )

    @staticmethod
    def _build_month_rows(month: int) -> list[Row]:
        if month < 5:
            return [
                Row(
                    VendorID=month,
                    passenger_count=float(month) if month == 1 else month,
                    total_amount=float(month * 10),
                    tpep_pickup_datetime=datetime(2023, month, 10, 8),
                    tpep_dropoff_datetime=datetime(2023, month, 10, 8, 10),
                )
            ]

        return [
            Row(
                VendorID=5,
                passenger_count=None,
                total_amount=10.0,
                tpep_pickup_datetime=datetime(2023, 5, 10, 8),
                tpep_dropoff_datetime=datetime(2023, 5, 10, 8, 10),
            ),
            Row(
                VendorID=5,
                passenger_count=0.0,
                total_amount=-2.0,
                tpep_pickup_datetime=datetime(2023, 5, 10, 8, 15),
                tpep_dropoff_datetime=datetime(2023, 5, 10, 8, 25),
            ),
            Row(
                VendorID=5,
                passenger_count=2.0,
                total_amount=4.0,
                tpep_pickup_datetime=datetime(2023, 5, 10, 8, 30),
                tpep_dropoff_datetime=datetime(2023, 5, 10, 8, 40),
            ),
        ]

    def test_reprocessing_publishes_one_delta_row_per_source_month(
        self,
        spark: SparkSession,
        tmp_path: Path,
        ingested_at: datetime,
    ):
        landing_path = tmp_path / "landing"
        silver_path = tmp_path / "silver" / "yellow_trips"
        landing_path.mkdir()
        pipeline_runner = LocalPipelineRunner(spark)

        for month in range(1, 6):
            spark.createDataFrame(
                self._build_month_rows(month),
                self._build_month_schema(month),
            ).write.parquet(
                str(landing_path / f"yellow_tripdata_2023-{month:02d}.parquet")
            )

        for _ in range(2):
            pipeline_runner.run_silver_pipeline(
                landing_path=str(landing_path),
                silver_path=str(silver_path),
                ingested_at=ingested_at,
            )

        published = spark.read.format("delta").load(str(silver_path))

        assert [
            row.asDict()
            for row in (
                published.groupBy("source_file")
                .count()
                .orderBy("source_file")
                .collect()
            )
        ] == [
            {"source_file": f"yellow_tripdata_2023-{month:02d}.parquet", "count": 1}
            for month in range(1, 5)
        ] + [{"source_file": "yellow_tripdata_2023-05.parquet", "count": 3}]

        monthly_gold_path = tmp_path / "gold" / "monthly_average_total_amount"
        may_gold_path = tmp_path / "gold" / "may_hourly_average_passengers"

        for _ in range(2):
            pipeline_runner.run_gold_pipeline(
                silver_path=str(silver_path),
                monthly_gold_path=str(monthly_gold_path),
                may_gold_path=str(may_gold_path),
            )

        monthly_gold = spark.read.format("delta").load(str(monthly_gold_path))
        may_gold = spark.read.format("delta").load(str(may_gold_path))

        assert [
            row.asDict() for row in monthly_gold.orderBy("pickup_month").collect()
        ] == [
            {
                "pickup_month": date(2023, month, 1),
                "average_total_amount": pytest.approx(
                    10.0 * month if month < 5 else 4.0
                ),
                "trip_count": 1 if month < 5 else 3,
                "negative_amount_count": 0 if month < 5 else 1,
            }
            for month in range(1, 6)
        ]
        assert [row.asDict() for row in may_gold.orderBy("pickup_hour").collect()] == [
            {
                "pickup_hour": 8,
                "average_passenger_count": 1.0,
                "trip_count": 3,
                "null_passenger_count": 1,
            }
        ]
