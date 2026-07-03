import argparse
import logging
from datetime import datetime

from pyspark.sql import SparkSession

from core.connector import LandingFileConnector
from core.spark import LocalSparkService
from core.sync import DeltaSyncService
from core.transform import GoldTransformService

logger = logging.getLogger(__name__)


class LocalPipelineRunner:
    """Simple orchestrator for the local Silver and Gold pipelines."""

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    def run_silver_pipeline(
        self,
        *,
        landing_path: str,
        silver_path: str,
        ingested_at: datetime | None = None,
    ) -> None:
        silver_dataframe = LandingFileConnector(self.spark, landing_path).load(
            ingested_at=ingested_at
        )
        DeltaSyncService.overwrite_path(silver_dataframe, silver_path)

    def run_gold_pipeline(
        self,
        *,
        silver_path: str,
        monthly_gold_path: str,
        may_gold_path: str,
    ) -> None:
        silver_dataframe = self.spark.read.format("delta").load(silver_path)
        DeltaSyncService.overwrite_path(
            GoldTransformService.build_monthly_average_total_amount(silver_dataframe),
            monthly_gold_path,
        )
        DeltaSyncService.overwrite_path(
            GoldTransformService.build_may_hourly_average_passengers(silver_dataframe),
            may_gold_path,
        )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build the local Yellow Taxi Silver Delta table."
    )
    parser.add_argument("--landing-path", default="data/landing")
    parser.add_argument(
        "--silver-path",
        default="data/silver/yellow_trips",
    )
    parser.add_argument(
        "--monthly-gold-path",
        default="data/gold/monthly_average_total_amount",
    )
    parser.add_argument(
        "--may-gold-path",
        default="data/gold/may_hourly_average_passengers",
    )
    args = parser.parse_args()

    spark = LocalSparkService.create_session(app_name="ifood-yellow-taxi-pipeline")
    spark.sparkContext.setLogLevel("WARN")
    pipeline_runner = LocalPipelineRunner(spark)
    try:
        pipeline_runner.run_silver_pipeline(
            landing_path=args.landing_path,
            silver_path=args.silver_path,
        )
        pipeline_runner.run_gold_pipeline(
            silver_path=args.silver_path,
            monthly_gold_path=args.monthly_gold_path,
            may_gold_path=args.may_gold_path,
        )
        silver_row_count = spark.read.format("delta").load(args.silver_path).count()
        monthly_rows = spark.read.format("delta").load(args.monthly_gold_path).count()
        may_rows = spark.read.format("delta").load(args.may_gold_path).count()
        logger.info(f"Published {silver_row_count:,} Silver rows to {args.silver_path}")
        logger.info(f"Published {monthly_rows:,} Gold rows to {args.monthly_gold_path}")
        logger.info(f"Published {may_rows:,} Gold rows to {args.may_gold_path}")
    finally:
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )
    main()
