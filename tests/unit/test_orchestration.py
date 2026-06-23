"""Unit tests for the Dagster orchestration layer.

Dagster is an optional extra; these tests skip when it isn't installed (as in
the default CI job). They run in any environment with `.[orchestration]`.
"""

from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("dagster")


def test_definitions_load() -> None:
    from data_pipeline.orchestration.definitions import (
        defs,
        weather_pipeline_job,
        weather_schedule,
    )

    assert weather_pipeline_job.name == "weather_pipeline_job"
    assert weather_schedule.name == "weather_pipeline_schedule"
    # Definitions should expose exactly the one medallion asset.
    asset_keys = [k.to_user_string() for k in defs.get_repository_def().assets_defs_by_key]
    assert "weather_observatory" in asset_keys


def test_asset_materializes_on_success() -> None:
    from dagster import build_asset_context

    from data_pipeline.orchestration import definitions as orch

    fake_result = MagicMock(
        run_id="r-1",
        status="success",
        records_ingested=5,
        records_transformed=5,
        records_loaded=5,
        duration_seconds=1.23,
        quality_gate_passed=True,
    )
    with patch.object(orch, "DataPipeline") as mock_pipeline:
        mock_pipeline.return_value.run.return_value = fake_result
        result = orch.weather_observatory(build_asset_context())

    assert result.metadata["status"].value == "success"
    assert result.metadata["records_loaded"].value == 5


def test_asset_raises_failure_when_blocked() -> None:
    from dagster import Failure, build_asset_context

    from data_pipeline.orchestration import definitions as orch

    fake_result = MagicMock(
        run_id="r-2",
        status="blocked",
        records_ingested=5,
        records_transformed=0,
        records_loaded=0,
        duration_seconds=0.5,
        quality_gate_passed=False,
        quality_gate_reason="bronze gate blocked",
        error_message="",
    )
    with patch.object(orch, "DataPipeline") as mock_pipeline:
        mock_pipeline.return_value.run.return_value = fake_result
        with pytest.raises(Failure):
            orch.weather_observatory(build_asset_context())
