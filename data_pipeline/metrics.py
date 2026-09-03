"""Prometheus metrics export for pipeline runs.

The pipeline is a short-lived **batch job**, not a long-running server, so a
scrape endpoint would disappear before Prometheus could scrape it. The
idiomatic sink for batch jobs is the **Pushgateway**: each run collects its
metrics into a dedicated registry and pushes them at the end.

Best-effort: collecting metrics always works; a push failure is logged but
never propagates, so a metrics outage can't take down the pipeline. Disabled
by default — set ``METRICS_ENABLED=true`` and ``PROMETHEUS_PUSHGATEWAY_URL``.
"""

import logging
from typing import TYPE_CHECKING

from prometheus_client import CollectorRegistry, Counter, Gauge, push_to_gateway

from data_pipeline.config import get_settings

if TYPE_CHECKING:
    from data_pipeline.pipeline import PipelineRunResult

logger = logging.getLogger(__name__)

_SEVERITIES = ("critical", "warning", "info")


class PipelineMetrics:
    """Collects per-run Prometheus metrics and pushes them to a Pushgateway."""

    def __init__(
        self,
        enabled: bool | None = None,
        pushgateway_url: str | None = None,
        job_name: str | None = None,
    ):
        """Initialize the metrics collector.

        Args:
            enabled: Master on/off switch. Falls back to settings.
            pushgateway_url: Prometheus Pushgateway base URL. Falls back to settings.
            job_name: Pushgateway job label. Falls back to settings.
        """
        settings = get_settings()
        self.enabled = enabled if enabled is not None else settings.metrics_enabled
        self.pushgateway_url = (
            pushgateway_url if pushgateway_url is not None else settings.prometheus_pushgateway_url
        )
        self.job_name = job_name or settings.metrics_job_name

        # A dedicated registry per instance keeps runs isolated and avoids
        # duplicate-timeseries errors on the global default registry.
        self.registry = CollectorRegistry()
        self.runs_total = Counter(
            "observatory_pipeline_runs_total",
            "Pipeline runs by terminal status",
            ["status"],
            registry=self.registry,
        )
        self.duration_seconds = Gauge(
            "observatory_pipeline_duration_seconds",
            "Duration of the most recent pipeline run",
            registry=self.registry,
        )
        self.records = Gauge(
            "observatory_pipeline_records",
            "Record counts for the most recent run, by stage",
            ["stage"],
            registry=self.registry,
        )
        self.last_run_timestamp = Gauge(
            "observatory_pipeline_last_run_timestamp_seconds",
            "Unix start time of the most recent run",
            registry=self.registry,
        )
        self.gate_total = Counter(
            "observatory_quality_gate_total",
            "Quality-gate evaluations by layer and status",
            ["layer", "status"],
            registry=self.registry,
        )
        self.quality_issues = Gauge(
            "observatory_quality_issues",
            "Quality issues in the most recent run, by layer and severity",
            ["layer", "severity"],
            registry=self.registry,
        )

    def is_active(self) -> bool:
        """Whether metrics will actually be pushed (enabled and configured)."""
        return bool(self.enabled and self.pushgateway_url)

    def record(self, result: "PipelineRunResult") -> None:
        """Populate the registry from a pipeline run result."""
        self.runs_total.labels(status=result.status).inc()
        self.duration_seconds.set(result.duration_seconds)
        self.records.labels(stage="ingested").set(result.records_ingested)
        self.records.labels(stage="transformed").set(result.records_transformed)
        self.records.labels(stage="loaded").set(result.records_loaded)
        if result.started_at is not None:
            self.last_run_timestamp.set(result.started_at.timestamp())

        for gate in result.quality_results:
            self.gate_total.labels(layer=gate.layer, status=gate.status.value).inc()
            for severity in _SEVERITIES:
                count = sum(1 for issue in gate.issues if issue.severity.value == severity)
                self.quality_issues.labels(layer=gate.layer, severity=severity).set(count)

    def push(self) -> bool:
        """Push the collected metrics to the Pushgateway. Never raises.

        Returns:
            True if the push succeeded, False otherwise (including when
            metrics are inactive).
        """
        if not self.is_active():
            logger.debug("Metrics inactive; skipping Prometheus push")
            return False
        try:
            push_to_gateway(self.pushgateway_url, job=self.job_name, registry=self.registry)
            return True
        except Exception as e:  # OSError, connection failures, etc.
            logger.warning("Prometheus push failed: %s", e)
            return False

    def record_and_push(self, result: "PipelineRunResult") -> bool:
        """Record a run's metrics and push them. Best-effort; never raises."""
        try:
            self.record(result)
        except Exception as e:  # pragma: no cover - defensive
            logger.warning("Failed to record metrics: %s", e)
            return False
        return self.push()
