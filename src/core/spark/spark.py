from __future__ import annotations

import os
import sys
from abc import ABC

from pyspark.sql import SparkSession


class BaseSparkService(ABC):
    """Shared Spark environment and builder configuration."""

    @staticmethod
    def configure_python_runtime() -> None:
        os.environ["PYSPARK_PYTHON"] = sys.executable
        os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    @staticmethod
    def resolve_ivy_dir() -> str:
        ivy_dir = os.environ.get("SPARK_IVY_DIR", "/tmp/ivy")
        os.makedirs(ivy_dir, exist_ok=True)
        return ivy_dir

    @classmethod
    def build_base_builder(
        cls,
        *,
        app_name: str,
        master: str,
        shuffle_partitions: int,
    ):
        cls.configure_python_runtime()
        ivy_dir = cls.resolve_ivy_dir()
        return (
            SparkSession.builder.master(master)
            .appName(app_name)
            .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
            .config("spark.ui.enabled", "false")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.jars.ivy", ivy_dir)
        )


class LocalSparkService(BaseSparkService):
    """Local Spark session factory with Delta Lake enabled."""

    @staticmethod
    def resolve_delta_configurator():
        try:
            from delta import configure_spark_with_delta_pip
        except ImportError as exc:
            raise RuntimeError(
                "Local Spark support requires the 'local' extra: "
                "pip install -e .[local]"
            ) from exc
        return configure_spark_with_delta_pip

    @classmethod
    def create_session(
        cls,
        *,
        app_name: str,
        master: str = "local[*]",
        shuffle_partitions: int = 8,
    ) -> SparkSession:
        builder = (
            cls.build_base_builder(
                app_name=app_name,
                master=master,
                shuffle_partitions=shuffle_partitions,
            )
            .config(
                "spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension",
            )
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
        )
        configure_spark_with_delta_pip = cls.resolve_delta_configurator()
        return configure_spark_with_delta_pip(builder).getOrCreate()
