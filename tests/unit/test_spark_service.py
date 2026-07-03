import builtins
import os
from unittest.mock import patch

import pytest

from core.spark import BaseSparkService, LocalSparkService


class TestBaseSparkService:
    def test_builds_common_builder(self, tmp_path, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("SPARK_IVY_DIR", str(tmp_path / "ivy-cache"))

        builder = BaseSparkService.build_base_builder(
            app_name="ifood-test",
            master="local[2]",
            shuffle_partitions=4,
        )

        assert os.environ["PYSPARK_PYTHON"]
        assert os.environ["PYSPARK_DRIVER_PYTHON"]
        assert builder._options["spark.master"] == "local[2]"
        assert builder._options["spark.app.name"] == "ifood-test"
        assert builder._options["spark.sql.shuffle.partitions"] == "4"
        assert builder._options["spark.jars.ivy"] == str(tmp_path / "ivy-cache")


class TestLocalSparkService:
    def test_raises_clear_error_when_delta_is_missing(self):
        original_import = builtins.__import__

        def failing_import(name, *args, **kwargs):
            if name == "delta":
                raise ImportError("delta missing")
            return original_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=failing_import):
            with pytest.raises(
                RuntimeError, match="Local Spark support requires the 'local' extra"
            ):
                LocalSparkService.resolve_delta_configurator()
