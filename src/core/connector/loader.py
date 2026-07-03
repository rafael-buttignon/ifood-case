from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date, datetime, timezone

from pyspark.sql import DataFrame, SparkSession

from core.transform import SilverTransformService


class BaseConnectorService(ABC):
    """Base connector for loading datasets from a source system."""

    def __init__(self, spark: SparkSession, source_path: str) -> None:
        self.spark = spark
        self.source_path = source_path.rstrip("/")

    @abstractmethod
    def load(self, *, ingested_at: datetime | None = None) -> DataFrame:
        """Load data from the configured source."""


class LandingFileConnector(BaseConnectorService):
    """File connector for the five monthly Yellow Taxi landing files."""

    def load(self, *, ingested_at: datetime | None = None) -> DataFrame:
        batch_time = ingested_at or datetime.now(timezone.utc).replace(tzinfo=None)
        monthly_frames: list[DataFrame] = []

        for month in range(1, 6):
            source_file = f"yellow_tripdata_2023-{month:02d}.parquet"
            source_path = f"{self.source_path}/{source_file}"
            source_dataframe = self.spark.read.parquet(source_path)
            monthly_frames.append(
                SilverTransformService.build_validated_dataframe(
                    source_dataframe,
                    expected_month=date(2023, month, 1),
                    source_file=source_file,
                    ingested_at=batch_time,
                )
            )

        consolidated_dataframe = monthly_frames[0]
        for monthly_frame in monthly_frames[1:]:
            consolidated_dataframe = consolidated_dataframe.unionByName(monthly_frame)

        return consolidated_dataframe
