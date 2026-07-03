from datetime import date, datetime

import pytest
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import DoubleType, LongType

from core.transform import SilverTransformService


class TestSilverTransformService:
    @staticmethod
    def _build_trip_row(**overrides):
        trip_row = {
            "VendorID": 1,
            "passenger_count": 1.0,
            "total_amount": 10.0,
            "tpep_pickup_datetime": datetime(2023, 5, 10, 8, 0),
            "tpep_dropoff_datetime": datetime(2023, 5, 10, 8, 10),
        }
        trip_row.update(overrides)
        return Row(**trip_row)

    def test_normalizes_schema_differences_between_months(self, spark: SparkSession):
        dataframe = spark.createDataFrame(
            [
                self._build_trip_row(
                    VendorID=2,
                    passenger_count=3,
                    total_amount=24.5,
                    tpep_pickup_datetime=datetime(2023, 5, 10, 8, 30),
                    tpep_dropoff_datetime=datetime(2023, 5, 10, 8, 50),
                )
            ]
        )

        normalized = SilverTransformService.normalize_source_schema(dataframe)

        assert isinstance(normalized.schema["VendorID"].dataType, LongType)
        assert isinstance(normalized.schema["passenger_count"].dataType, DoubleType)

    def test_prepares_the_silver_consumption_contract(
        self,
        spark: SparkSession,
        ingested_at: datetime,
    ):
        dataframe = spark.createDataFrame(
            [
                self._build_trip_row(
                    VendorID=2,
                    passenger_count=3,
                    total_amount=24.5,
                    tpep_pickup_datetime=datetime(2023, 5, 10, 8, 30),
                    tpep_dropoff_datetime=datetime(2023, 5, 10, 8, 50),
                    ignored_column="not part of the contract",
                )
            ]
        )

        silver = SilverTransformService.build_validated_dataframe(
            dataframe,
            expected_month=date(2023, 5, 1),
            source_file="yellow_tripdata_2023-05.parquet",
            ingested_at=ingested_at,
        )

        assert silver.first().asDict() == {
            "VendorID": 2,
            "passenger_count": 3.0,
            "total_amount": 24.5,
            "tpep_pickup_datetime": datetime(2023, 5, 10, 8, 30),
            "tpep_dropoff_datetime": datetime(2023, 5, 10, 8, 50),
            "pickup_month": date(2023, 5, 1),
            "pickup_hour": 8,
            "source_file": "yellow_tripdata_2023-05.parquet",
            "ingested_at": ingested_at,
        }

    def test_rejects_pickups_outside_the_source_month(
        self,
        spark: SparkSession,
        ingested_at: datetime,
    ):
        dataframe = spark.createDataFrame(
            [
                self._build_trip_row(
                    VendorID=1,
                    tpep_pickup_datetime=datetime(2023, 5, 1, 0, 0),
                    tpep_dropoff_datetime=datetime(2023, 5, 1, 0, 10),
                ),
                self._build_trip_row(
                    VendorID=2,
                    total_amount=20.0,
                    tpep_pickup_datetime=datetime(2023, 4, 30, 23, 59),
                    tpep_dropoff_datetime=datetime(2023, 5, 1, 0, 9),
                ),
            ]
        )

        silver = SilverTransformService.build_validated_dataframe(
            dataframe,
            expected_month=date(2023, 5, 1),
            source_file="yellow_tripdata_2023-05.parquet",
            ingested_at=ingested_at,
        )

        assert [row.VendorID for row in silver.collect()] == [1]

    def test_rejects_dropoffs_before_pickups(
        self,
        spark: SparkSession,
        ingested_at: datetime,
    ):
        dataframe = spark.createDataFrame(
            [
                self._build_trip_row(
                    VendorID=1,
                    tpep_pickup_datetime=datetime(2023, 5, 10, 8, 0),
                    tpep_dropoff_datetime=datetime(2023, 5, 10, 8, 10),
                ),
                self._build_trip_row(
                    VendorID=2,
                    total_amount=20.0,
                    tpep_pickup_datetime=datetime(2023, 5, 10, 9, 0),
                    tpep_dropoff_datetime=datetime(2023, 5, 10, 8, 59),
                ),
            ]
        )

        silver = SilverTransformService.build_validated_dataframe(
            dataframe,
            expected_month=date(2023, 5, 1),
            source_file="yellow_tripdata_2023-05.parquet",
            ingested_at=ingested_at,
        )

        assert [row.VendorID for row in silver.collect()] == [1]

    def test_rejects_rows_missing_required_values(
        self,
        spark: SparkSession,
        ingested_at: datetime,
    ):
        dataframe = spark.createDataFrame(
            [
                self._build_trip_row(passenger_count=None),
                self._build_trip_row(
                    VendorID=None,
                    passenger_count=1.0,
                    total_amount=20.0,
                    tpep_pickup_datetime=datetime(2023, 5, 10, 9, 0),
                    tpep_dropoff_datetime=datetime(2023, 5, 10, 9, 10),
                ),
                self._build_trip_row(
                    VendorID=2,
                    passenger_count=1.0,
                    total_amount=None,
                    tpep_pickup_datetime=datetime(2023, 5, 10, 10, 0),
                    tpep_dropoff_datetime=datetime(2023, 5, 10, 10, 10),
                ),
            ]
        )

        silver = SilverTransformService.build_validated_dataframe(
            dataframe,
            expected_month=date(2023, 5, 1),
            source_file="yellow_tripdata_2023-05.parquet",
            ingested_at=ingested_at,
        )

        assert [row.VendorID for row in silver.collect()] == [1]

    def test_reports_missing_source_columns(self, spark: SparkSession):
        dataframe = spark.createDataFrame([Row(VendorID=1, passenger_count=1.0)])

        with pytest.raises(
            ValueError,
            match=(
                "Missing required columns: total_amount, "
                "tpep_dropoff_datetime, tpep_pickup_datetime"
            ),
        ):
            SilverTransformService.build_validated_dataframe(
                dataframe,
                expected_month=date(2023, 5, 1),
                source_file="yellow_tripdata_2023-05.parquet",
                ingested_at=datetime(2023, 6, 1),
            )
