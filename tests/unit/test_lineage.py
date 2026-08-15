"""Unit tests for run lineage."""

from unittest.mock import MagicMock

import pytest

from data_pipeline.lineage import Artifact, LineageManifest


@pytest.mark.unit
def test_manifest_add_and_to_dict() -> None:
    manifest = LineageManifest(run_id="r-1", cities=["London", "Paris"])
    manifest.add("bronze", "bronze/weather/.../b.json", 2)
    manifest.add("silver", "silver/weather/.../s.json", 2)

    d = manifest.to_dict()
    assert d["run_id"] == "r-1"
    assert d["cities"] == ["London", "Paris"]
    assert d["artifact_count"] == 2
    assert d["artifacts"][0] == {
        "layer": "bronze",
        "key": "bronze/weather/.../b.json",
        "record_count": 2,
    }
    assert "started_at" in d


@pytest.mark.unit
def test_manifest_starts_empty() -> None:
    manifest = LineageManifest(run_id="r-2", cities=[])
    assert manifest.artifacts == []
    assert manifest.to_dict()["artifact_count"] == 0


@pytest.mark.unit
def test_artifact_fields() -> None:
    art = Artifact(layer="gold", key="k", record_count=5)
    assert (art.layer, art.key, art.record_count) == ("gold", "k", 5)


@pytest.mark.unit
def test_pipeline_records_bronze_artifact() -> None:
    """_ingest_to_bronze should record a bronze artifact in the manifest."""
    from data_pipeline.pipeline import DataPipeline

    pipeline = DataPipeline.__new__(DataPipeline)
    pipeline.storage = MagicMock()
    pipeline.storage.write_json.return_value = "bronze/weather/year=2024/month=01/day=01/b.json"
    pipeline.api_client = MagicMock()
    pipeline.api_client.fetch_multiple_cities.return_value = [
        MagicMock(to_dict=lambda: {"city": "London", "timestamp": "t"})
    ]
    pipeline._manifest = LineageManifest(run_id="r-3", cities=["London"])

    pipeline._ingest_to_bronze(["London"])

    assert len(pipeline._manifest.artifacts) == 1
    art = pipeline._manifest.artifacts[0]
    assert art.layer == "bronze"
    assert art.key.startswith("bronze/weather/")
    assert art.record_count == 1


@pytest.mark.unit
def test_record_artifact_is_noop_without_manifest() -> None:
    from data_pipeline.pipeline import DataPipeline

    pipeline = DataPipeline.__new__(DataPipeline)
    pipeline._manifest = None
    # Must not raise.
    pipeline._record_artifact("bronze", "k", 1)


@pytest.mark.unit
def test_persist_lineage_manifest_best_effort() -> None:
    from data_pipeline.pipeline import DataPipeline

    pipeline = DataPipeline.__new__(DataPipeline)
    pipeline.storage = MagicMock()
    pipeline.storage.write_json.side_effect = RuntimeError("s3 down")
    pipeline._manifest = LineageManifest(run_id="r-4", cities=["X"])
    # Must swallow the storage error.
    pipeline._persist_lineage_manifest()
    pipeline.storage.write_json.assert_called_once()
