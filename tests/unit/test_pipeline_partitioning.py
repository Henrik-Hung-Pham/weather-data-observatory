"""The whole run must use one timestamp so every layer shares a partition."""

import pytest

from data_pipeline.pipeline import DataPipeline


class _FakeWeather:
    def __init__(self, record: dict) -> None:
        self._record = record

    def to_dict(self) -> dict:
        return dict(self._record)


class FakeAPIClient:
    def __init__(self, records: list[dict]) -> None:
        self._records = records

    def fetch_multiple_cities(self, cities):
        return [_FakeWeather(r) for r in self._records]


class RecordingStorage:
    """Captures the timestamp passed with every write_json call."""

    def __init__(self) -> None:
        self.writes: list[tuple[str, str, object]] = []

    def write_json(self, data, layer, filename, timestamp=None) -> str:
        self.writes.append((layer, filename, timestamp))
        return f"{layer}/{filename}.json"


class FakeDatabase:
    def insert_weather_data(self, records) -> int:
        return len(records)

    def insert_pipeline_run(self, run_result) -> None:
        pass

    def insert_quality_metrics(self, run_id, gate_result) -> None:
        pass


class FakeAlerter:
    def alert_pipeline_result(self, **kwargs) -> bool:
        return False


@pytest.mark.unit
def test_all_layers_share_one_run_timestamp(sample_bronze_data):
    storage = RecordingStorage()
    pipeline = DataPipeline(
        api_client=FakeAPIClient(sample_bronze_data),
        storage=storage,
        database=FakeDatabase(),
        alerter=FakeAlerter(),
    )

    result = pipeline.run(cities=["London", "Paris"])
    assert result.status == "success"

    # Every layer object written during the run carries the same timestamp,
    # equal to the run's started_at.
    layer_timestamps = {
        layer: ts for layer, _, ts in storage.writes if layer in {"bronze", "silver", "gold"}
    }
    assert set(layer_timestamps) == {"bronze", "silver", "gold"}
    assert set(layer_timestamps.values()) == {result.started_at}

    # The run-result object is also stamped with the same run timestamp.
    run_result_writes = [ts for _, name, ts in storage.writes if name.startswith("pipeline_run_")]
    assert run_result_writes and all(ts == result.started_at for ts in run_result_writes)
