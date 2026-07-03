from datetime import date, datetime

from pyspark.sql import Row, SparkSession

from core.transform import GoldTransformService


class TestGoldTransformService:
    @staticmethod
    def _build_gold_source_dataframe(spark: SparkSession):
        return spark.createDataFrame(
            [
                Row(
                    pickup_month=date(2023, 5, 1),
                    pickup_hour=8,
                    total_amount=10.0,
                    passenger_count=1.0,
                    tpep_pickup_datetime=datetime(2023, 5, 10, 8, 0),
                    tpep_dropoff_datetime=datetime(2023, 5, 10, 8, 10),
                    VendorID=1,
                    source_file="yellow_tripdata_2023-05.parquet",
                    ingested_at=datetime(2023, 6, 1),
                ),
                Row(
                    pickup_month=date(2023, 5, 1),
                    pickup_hour=9,
                    total_amount=-2.0,
                    passenger_count=2.0,
                    tpep_pickup_datetime=datetime(2023, 5, 11, 9, 0),
                    tpep_dropoff_datetime=datetime(2023, 5, 11, 9, 10),
                    VendorID=2,
                    source_file="yellow_tripdata_2023-05.parquet",
                    ingested_at=datetime(2023, 6, 1),
                ),
                Row(
                    pickup_month=date(2023, 6, 1),
                    pickup_hour=10,
                    total_amount=20.0,
                    passenger_count=3.0,
                    tpep_pickup_datetime=datetime(2023, 6, 10, 10, 0),
                    tpep_dropoff_datetime=datetime(2023, 6, 10, 10, 10),
                    VendorID=3,
                    source_file="yellow_tripdata_2023-06.parquet",
                    ingested_at=datetime(2023, 6, 1),
                ),
            ]
        )

    def test_builds_monthly_average_total_amount(self, spark: SparkSession):
        dataframe = self._build_gold_source_dataframe(spark)

        gold = GoldTransformService.build_monthly_average_total_amount(dataframe)

        assert [row.asDict() for row in gold.orderBy("pickup_month").collect()] == [
            {
                "pickup_month": date(2023, 5, 1),
                "average_total_amount": 4.0,
                "trip_count": 2,
                "negative_amount_count": 1,
            },
            {
                "pickup_month": date(2023, 6, 1),
                "average_total_amount": 20.0,
                "trip_count": 1,
                "negative_amount_count": 0,
            },
        ]

    def test_builds_may_hourly_average_passengers(self, spark: SparkSession):
        dataframe = spark.createDataFrame(
            [
                Row(
                    pickup_month=date(2023, 5, 1),
                    pickup_hour=8,
                    total_amount=10.0,
                    passenger_count=None,
                    tpep_pickup_datetime=datetime(2023, 5, 10, 8, 0),
                    tpep_dropoff_datetime=datetime(2023, 5, 10, 8, 10),
                    VendorID=1,
                    source_file="yellow_tripdata_2023-05.parquet",
                    ingested_at=datetime(2023, 6, 1),
                ),
                Row(
                    pickup_month=date(2023, 5, 1),
                    pickup_hour=8,
                    total_amount=11.0,
                    passenger_count=0.0,
                    tpep_pickup_datetime=datetime(2023, 5, 10, 8, 15),
                    tpep_dropoff_datetime=datetime(2023, 5, 10, 8, 25),
                    VendorID=2,
                    source_file="yellow_tripdata_2023-05.parquet",
                    ingested_at=datetime(2023, 6, 1),
                ),
                Row(
                    pickup_month=date(2023, 5, 1),
                    pickup_hour=8,
                    total_amount=12.0,
                    passenger_count=2.0,
                    tpep_pickup_datetime=datetime(2023, 5, 10, 8, 30),
                    tpep_dropoff_datetime=datetime(2023, 5, 10, 8, 40),
                    VendorID=3,
                    source_file="yellow_tripdata_2023-05.parquet",
                    ingested_at=datetime(2023, 6, 1),
                ),
                Row(
                    pickup_month=date(2023, 6, 1),
                    pickup_hour=8,
                    total_amount=99.0,
                    passenger_count=10.0,
                    tpep_pickup_datetime=datetime(2023, 6, 10, 8, 0),
                    tpep_dropoff_datetime=datetime(2023, 6, 10, 8, 10),
                    VendorID=4,
                    source_file="yellow_tripdata_2023-06.parquet",
                    ingested_at=datetime(2023, 6, 1),
                ),
            ]
        )

        gold = GoldTransformService.build_may_hourly_average_passengers(dataframe)

        assert [row.asDict() for row in gold.orderBy("pickup_hour").collect()] == [
            {
                "pickup_hour": 8,
                "average_passenger_count": 1.0,
                "trip_count": 3,
                "null_passenger_count": 1,
            }
        ]
