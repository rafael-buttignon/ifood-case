from unittest.mock import MagicMock

from core.sync import DeltaSyncService


def build_dataframe_with_writer() -> tuple[MagicMock, MagicMock]:
    writer = MagicMock()
    writer.format.return_value = writer
    writer.mode.return_value = writer
    writer.option.return_value = writer

    dataframe = MagicMock()
    dataframe.write = writer
    return dataframe, writer


class TestDeltaSyncService:
    def test_overwrites_path(self):
        dataframe, writer = build_dataframe_with_writer()

        DeltaSyncService.overwrite_path(dataframe, "/tmp/silver")

        writer.format.assert_called_once_with("delta")
        writer.mode.assert_called_once_with("overwrite")
        writer.option.assert_called_once_with("overwriteSchema", "true")
        writer.save.assert_called_once_with("/tmp/silver")

    def test_overwrites_table(self):
        dataframe, writer = build_dataframe_with_writer()

        DeltaSyncService.overwrite_table(dataframe, "ifood_case.silver.yellow_trips")

        writer.format.assert_called_once_with("delta")
        writer.mode.assert_called_once_with("overwrite")
        writer.option.assert_called_once_with("overwriteSchema", "true")
        writer.saveAsTable.assert_called_once_with("ifood_case.silver.yellow_trips")
