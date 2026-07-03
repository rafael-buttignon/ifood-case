from datetime import datetime
from unittest.mock import MagicMock, call, patch

from core.connector import LandingFileConnector


class FakeFrame:
    def __init__(self, label: str) -> None:
        self.label = label

    def unionByName(self, other: "FakeFrame") -> "FakeFrame":
        return FakeFrame(f"{self.label}+{other.label}")


class TestLandingFileConnector:
    def test_loads_and_unions_five_months(self, ingested_at: datetime):
        spark = MagicMock()
        raw_dataframes = [object() for _ in range(5)]
        spark.read.parquet.side_effect = raw_dataframes
        transformed_frames = [FakeFrame(f"month_{month}") for month in range(1, 6)]

        with patch(
            "core.connector.loader.SilverTransformService.build_validated_dataframe",
            side_effect=transformed_frames,
        ) as build_validated_dataframe:
            dataframe = LandingFileConnector(
                spark,
                "/tmp/landing/",
            ).load(ingested_at=ingested_at)

        assert dataframe.label == "month_1+month_2+month_3+month_4+month_5"
        assert spark.read.parquet.call_args_list == [
            call(f"/tmp/landing/yellow_tripdata_2023-{month:02d}.parquet")
            for month in range(1, 6)
        ]
        assert build_validated_dataframe.call_count == 5
