from __future__ import annotations

from abc import ABC

from pyspark.sql import DataFrame


class BaseSyncService(ABC):
    """Shared write configuration for persisted DataFrames."""

    write_format = "delta"
    write_mode = "overwrite"
    overwrite_schema = "true"

    @classmethod
    def build_writer(cls, dataframe: DataFrame):
        return (
            dataframe.write.format(cls.write_format)
            .mode(cls.write_mode)
            .option("overwriteSchema", cls.overwrite_schema)
        )


class DeltaSyncService(BaseSyncService):
    """Delta Lake persistence service for path and table targets."""

    @classmethod
    def overwrite_path(cls, dataframe: DataFrame, destination: str) -> None:
        cls.build_writer(dataframe).save(destination)

    @classmethod
    def overwrite_table(cls, dataframe: DataFrame, table_name: str) -> None:
        cls.build_writer(dataframe).saveAsTable(table_name)
