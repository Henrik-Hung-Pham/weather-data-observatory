"""Unit tests for Prometheus metrics export."""

from datetime import datetime, timezone
from uuid import uuid4

import pytest

from data_pipeline.metrics import PipelineMetrics
from data_pipeline.pipeline import PipelineRunResult
from data_pipeline.quality.gates import (
    QualityGateResult,
    QualityGateStatus,
    QualityIssue,
    QualitySeverity,
)


def _sample_result() -> PipelineRunResult:
    gate = QualityGateResult(
        gate_name="bronze_quality_gate",
        run_id=uuid4(),
        status=QualityGateStatus.WARNED,
        layer="bronze",
        issues=[
            QualityIssue(
                rule_name="range_check",
                severity=QualitySeverity.WARNING,
                message="out of range",
            )
        ],
    )
    return PipelineRunResult(
        run_id=uuid4(),
        status="success",
        started_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        duration_seconds=1.5,
        records_ingested=5,
        records_transformed=5,
        records_loaded=5,
        quality_results=[gate],
    )


@pytest.mark.unit
def test_record_populates_registry():
    metrics = PipelineMetrics(enabled=False)
    metrics.record(_sample_result())

    reg = metrics.registry
    assert reg.get_sample_value("observatory_pipeline_runs_total", {"status": "success"}) == 1.0
    assert reg.get_sample_value("observatory_pipeline_duration_seconds") == 1.5
    assert reg.get_sample_value("observatory_pipeline_records", {"stage": "ingested"}) == 5.0
    assert reg.get_sample_value("observatory_pipeline_records", {"stage": "loaded"}) == 5.0
    # Quality gate evaluation recorded by layer + status.
    assert (
        reg.get_sample_value(
            "observatory_quality_gate_total", {"layer": "bronze", "status": "warned"}
        )
        == 1.0
    )
    # One WARNING issue, zero CRITICAL issues on the bronze layer.
    assert (
        reg.get_sample_value(
            "observatory_quality_issues", {"layer": "bronze", "severity": "warning"}
        )
        == 1.0
    )
    assert (
        reg.get_sample_value(
            "observatory_quality_issues", {"layer": "bronze", "severity": "critical"}
        )
        == 0.0
    )


@pytest.mark.unit
def test_is_active_requires_enabled_and_url():
    assert PipelineMetrics(enabled=False, pushgateway_url="http://gw:9091").is_active() is False
    assert PipelineMetrics(enabled=True, pushgateway_url="").is_active() is False
    assert PipelineMetrics(enabled=True, pushgateway_url="http://gw:9091").is_active() is True


@pytest.mark.unit
def test_push_is_noop_when_inactive():
    metrics = PipelineMetrics(enabled=False)
    assert metrics.push() is False


@pytest.mark.unit
def test_record_and_push_records_then_returns_push_result():
    metrics = PipelineMetrics(enabled=False)
    # Inactive -> push() returns False, but the run is still recorded.
    assert metrics.record_and_push(_sample_result()) is False
    assert (
        metrics.registry.get_sample_value(
            "observatory_pipeline_runs_total", {"status": "success"}
        )
        == 1.0
    )


@pytest.mark.unit
def test_push_swallows_gateway_errors(monkeypatch):
    metrics = PipelineMetrics(enabled=True, pushgateway_url="http://unreachable:9091")
    metrics.record(_sample_result())

    def _boom(*args, **kwargs):
        raise OSError("connection refused")

    monkeypatch.setattr("data_pipeline.metrics.push_to_gateway", _boom)
    # Never raises; returns False on failure.
    assert metrics.push() is False
