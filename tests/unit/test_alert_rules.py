"""Guards tying the Prometheus alert rules to the metrics the code emits.

``infra/prometheus/alerts.yml`` is not executed by anything in this repo, so
a renamed metric or a mistyped label value would break alerting silently --
and an alert that can never fire is worse than no alert, because it reads as
coverage. These tests resolve every selector in the rules against samples
produced by ``PipelineMetrics`` itself, so a rename fails CI instead.
"""

import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
import yaml

from data_pipeline.metrics import PipelineMetrics
from data_pipeline.pipeline import PipelineRunResult
from data_pipeline.quality.gates import (
    QualityGateResult,
    QualityGateStatus,
    QualityIssue,
    QualitySeverity,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
ALERTS_YAML = REPO_ROOT / "infra" / "prometheus" / "alerts.yml"

# observatory_foo, or observatory_foo{a="b",c="d"}
_SELECTOR = re.compile(r"(observatory_[a-z0-9_]+)(?:\{([^}]*)\})?")
_LABEL = re.compile(r'(\w+)\s*=\s*"([^"]*)"')


def _rules() -> list[dict[str, Any]]:
    doc = yaml.safe_load(ALERTS_YAML.read_text(encoding="utf-8"))
    return [rule for group in doc["groups"] for rule in group["rules"]]


def _selectors() -> list[tuple[str, dict[str, str], str]]:
    """Every (metric, labels, alert name) referenced by a rule expression."""
    found = []
    for rule in _rules():
        for metric, label_text in _SELECTOR.findall(rule["expr"]):
            labels = dict(_LABEL.findall(label_text or ""))
            found.append((metric, labels, rule["alert"]))
    return found


def _result(status: str, gate_status: QualityGateStatus, severity: QualitySeverity, loaded: int):
    gate = QualityGateResult(
        gate_name="bronze_quality_gate",
        run_id=uuid4(),
        status=gate_status,
        layer="bronze",
        issues=[QualityIssue(rule_name="range_check", severity=severity, message="x")],
    )
    return PipelineRunResult(
        run_id=uuid4(),
        status=status,
        started_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        duration_seconds=1.5,
        records_ingested=5,
        records_transformed=5,
        records_loaded=loaded,
        quality_results=[gate],
    )


def _emitted_samples() -> set[tuple[str, frozenset[tuple[str, str]]]]:
    """Every (sample name, labels) the exporter can produce.

    One run cannot exercise every terminal status or gate outcome at once, so
    several are recorded into separate registries and unioned.
    """
    runs = [
        ("failed", QualityGateStatus.ERROR, QualitySeverity.CRITICAL, 0),
        ("blocked", QualityGateStatus.BLOCKED, QualitySeverity.WARNING, 0),
        ("success", QualityGateStatus.PASSED, QualitySeverity.INFO, 5),
    ]
    samples = set()
    for status, gate_status, severity, loaded in runs:
        metrics = PipelineMetrics(enabled=False)
        metrics.record(_result(status, gate_status, severity, loaded))
        for family in metrics.registry.collect():
            for sample in family.samples:
                samples.add((sample.name, frozenset(sample.labels.items())))
    return samples


@pytest.mark.unit
def test_every_alert_selector_matches_an_emitted_metric():
    emitted = _emitted_samples()
    for metric, labels, alert in _selectors():
        wanted = set(labels.items())
        assert any(
            name == metric and wanted <= set(sample_labels) for name, sample_labels in emitted
        ), f"{alert} selects {metric}{labels or ''}, which data_pipeline.metrics never emits"


@pytest.mark.unit
def test_alert_label_values_come_from_the_code_not_from_guesswork():
    """Catches a typo'd label value that still parses as a valid selector."""
    run_statuses = {"running", "success", "failed", "blocked"}
    for metric, labels, alert in _selectors():
        if metric == "observatory_pipeline_runs_total" and "status" in labels:
            assert labels["status"] in run_statuses, alert
        if metric == "observatory_quality_gate_total" and "status" in labels:
            assert labels["status"] in {s.value for s in QualityGateStatus}, alert
        if metric == "observatory_quality_issues" and "severity" in labels:
            assert labels["severity"] in {s.value for s in QualitySeverity}, alert
        if metric == "observatory_pipeline_records" and "stage" in labels:
            assert labels["stage"] in {"ingested", "transformed", "loaded"}, alert


@pytest.mark.unit
def test_rules_are_actionable():
    rules = _rules()
    assert rules, "no alert rules found"

    names = [rule["alert"] for rule in rules]
    assert len(names) == len(set(names)), "duplicate alert names"

    for rule in rules:
        assert rule["labels"]["severity"] in {"critical", "warning"}, rule["alert"]
        # An alert without a description is a pager that says nothing.
        assert rule["annotations"]["summary"].strip(), rule["alert"]
        assert rule["annotations"]["description"].strip(), rule["alert"]


@pytest.mark.unit
def test_the_dead_mans_switch_exists():
    """The one rule that fires for something that did *not* happen.

    Everything else here reacts to a run that reported in. Without this, a
    scheduler that stops firing is invisible: Pushgateway keeps serving the
    last sample forever, so no counter ever changes.
    """
    by_name = {rule["alert"]: rule for rule in _rules()}
    missing = by_name["PipelineRunMissing"]
    assert "time()" in missing["expr"]
    assert "observatory_pipeline_last_run_timestamp_seconds" in missing["expr"]
    assert missing["labels"]["severity"] == "critical"
    # absent() covers the case time() arithmetic cannot: no series at all.
    assert "absent(" in by_name["PipelineMetricsAbsent"]["expr"]
