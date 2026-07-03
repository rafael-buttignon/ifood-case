from datetime import datetime
from unittest.mock import MagicMock, call, patch

from core.runner import LocalPipelineRunner


class TestLocalPipelineRunner:
    def test_builds_and_persists_silver(self, ingested_at: datetime):
        spark = MagicMock()
        runner = LocalPipelineRunner(spark)
        silver_dataframe = MagicMock()

        with patch("core.runner.LandingFileConnector") as landing_file_connector:
            landing_file_connector.return_value.load.return_value = silver_dataframe

            with patch("core.runner.DeltaSyncService") as delta_sync_service:
                runner.run_silver_pipeline(
                    landing_path="data/landing",
                    silver_path="data/silver/yellow_trips",
                    ingested_at=ingested_at,
                )

        landing_file_connector.assert_called_once_with(spark, "data/landing")
        landing_file_connector.return_value.load.assert_called_once_with(
            ingested_at=ingested_at
        )
        delta_sync_service.overwrite_path.assert_called_once_with(
            silver_dataframe,
            "data/silver/yellow_trips",
        )

    def test_builds_and_persists_gold(self):
        spark = MagicMock()
        runner = LocalPipelineRunner(spark)
        silver_dataframe = MagicMock()
        monthly_gold_dataframe = MagicMock()
        may_gold_dataframe = MagicMock()
        spark.read.format.return_value.load.return_value = silver_dataframe

        with patch("core.runner.GoldTransformService") as gold_transform_service:
            gold_transform_service.build_monthly_average_total_amount.return_value = (
                monthly_gold_dataframe
            )
            gold_transform_service.build_may_hourly_average_passengers.return_value = (
                may_gold_dataframe
            )

            with patch("core.runner.DeltaSyncService") as delta_sync_service:
                runner.run_gold_pipeline(
                    silver_path="data/silver/yellow_trips",
                    monthly_gold_path="data/gold/monthly_average_total_amount",
                    may_gold_path="data/gold/may_hourly_average_passengers",
                )

        spark.read.format.assert_called_once_with("delta")
        spark.read.format.return_value.load.assert_called_once_with(
            "data/silver/yellow_trips"
        )
        gold_transform_service.build_monthly_average_total_amount.assert_called_once_with(
            silver_dataframe
        )
        gold_transform_service.build_may_hourly_average_passengers.assert_called_once_with(
            silver_dataframe
        )
        assert delta_sync_service.overwrite_path.call_args_list == [
            call(monthly_gold_dataframe, "data/gold/monthly_average_total_amount"),
            call(may_gold_dataframe, "data/gold/may_hourly_average_passengers"),
        ]
