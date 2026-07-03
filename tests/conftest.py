from collections.abc import Iterator
from datetime import datetime

import pytest
from pyspark.sql import SparkSession

from core.spark import LocalSparkService


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    session = LocalSparkService.create_session(
        app_name="ifood-case-tests",
        master="local[2]",
        shuffle_partitions=2,
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def ingested_at() -> datetime:
    return datetime(2023, 6, 1, 12, 0)
