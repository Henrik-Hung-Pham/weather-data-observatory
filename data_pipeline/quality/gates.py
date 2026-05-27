"""Quality Gate implementation for the data pipeline.

Implements shift-left data quality with automatic pipeline blocking
when data quality issues are detected.
"""

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any
from uuid import UUID, uuid4

from data_pipeline.config import get_settings

logger = logging.getLogger(__name__)


class QualityGateStatus(Enum):
    """Quality gate evaluation status."""

    PASSED = "passed"
    WARNED = "warned"
    BLOCKED = "blocked"
    ERROR = "error"


class QualitySeverity(Enum):
    """Severity levels for quality issues."""

    INFO = "info"  # Logged but doesn't affect gate
    WARNING = "warning"  # Logged, may affect gate in strict mode
    CRITICAL = "critical"  # Always blocks the pipeline


@dataclass
class QualityIssue:
    """Represents a single data quality issue."""

    rule_name: str
    severity: QualitySeverity
    message: str
    affected_records: int = 0
    details: dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass
class QualityGateResult:
    """Result of a quality gate evaluation."""

    gate_name: str
    run_id: UUID
    status: QualityGateStatus
    layer: str
    issues: list[QualityIssue] = field(default_factory=list)
    metrics: dict[str, Any] = field(default_factory=dict)
    evaluated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    duration_ms: float = 0

    @property
    def passed(self) -> bool:
        """Check if the gate passed (not blocked)."""
        return self.status in (QualityGateStatus.PASSED, QualityGateStatus.WARNED)

    @property
    def blocked(self) -> bool:
        """Check if the gate is blocked."""
        return self.status == QualityGateStatus.BLOCKED

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for storage."""
        return {
            "gate_name": self.gate_name,
            "run_id": str(self.run_id),
            "status": self.status.value,
            "layer": self.layer,
            "issues": [
                {
                    "rule_name": i.rule_name,
                    "severity": i.severity.value,
                    "message": i.message,
                    "affected_records": i.affected_records,
                }
                for i in self.issues
            ],
            "metrics": self.metrics,
            "evaluated_at": self.evaluated_at.isoformat(),
            "duration_ms": self.duration_ms,
        }


class QualityGate:
    """Quality gate for pipeline control.

    Implements data quality checks with configurable blocking behavior.
    Supports both "warn" mode (log and continue) and "block" mode (stop pipeline).
    """

    def __init__(
        self,
        gate_name: str = "default_gate",
        mode: str | None = None,
    ):
        """Initialize quality gate.

        Args:
            gate_name: Name of this quality gate.
            mode: Gate mode - "warn" or "block". Uses settings if not provided.
        """
        settings = get_settings()
        self.gate_name = gate_name
        self.mode = mode or settings.quality_gate_mode
        self.run_id = uuid4()
        self._rules: list[Callable[[list[dict[str, Any]]], list[QualityIssue]]] = []

    def add_rule(
        self,
        rule_func: Callable[[list[dict[str, Any]]], list[QualityIssue]],
    ) -> "QualityGate":
        """Add a quality rule to the gate.

        Args:
            rule_func: Function that takes data and returns list of QualityIssue.

        Returns:
            Self for method chaining.
        """
        self._rules.append(rule_func)
        return self

    def evaluate(
        self,
        data: list[dict[str, Any]],
        layer: str,
        validation_results: dict[str, Any] | None = None,
    ) -> QualityGateResult:
        """Evaluate all quality rules against the data.

        Args:
            data: Data records to evaluate.
            layer: Data layer (bronze, silver, gold).
            validation_results: Optional Great Expectations validation results.

        Returns:
            QualityGateResult with evaluation status.
        """
        import time

        start_time = time.time()
        issues: list[QualityIssue] = []

        logger.info(f"🔍 Evaluating quality gate '{self.gate_name}' for {layer} layer")

        # Run custom rules
        for rule in self._rules:
            try:
                rule_issues = rule(data)
                issues.extend(rule_issues)
            except Exception as e:
                logger.error(f"Rule evaluation failed: {e}")
                issues.append(
                    QualityIssue(
                        rule_name="rule_execution",
                        severity=QualitySeverity.WARNING,
                        message=f"Rule execution failed: {e}",
                    )
                )

        # Process Great Expectations results if provided
        if validation_results:
            ge_issues = self._process_ge_results(validation_results)
            issues.extend(ge_issues)

        # Determine gate status
        status = self._determine_status(issues)

        duration_ms = (time.time() - start_time) * 1000

        # Calculate metrics
        metrics = {
            "total_records": len(data),
            "rules_evaluated": len(self._rules),
            "issues_found": len(issues),
            "critical_issues": sum(1 for i in issues if i.severity == QualitySeverity.CRITICAL),
            "warning_issues": sum(1 for i in issues if i.severity == QualitySeverity.WARNING),
        }

        result = QualityGateResult(
            gate_name=self.gate_name,
            run_id=self.run_id,
            status=status,
            layer=layer,
            issues=issues,
            metrics=metrics,
            duration_ms=duration_ms,
        )

        # Log result
        self._log_result(result)

        return result

    def _process_ge_results(
        self,
        validation_results: dict[str, Any],
    ) -> list[QualityIssue]:
        """Process Great Expectations validation results.

        Args:
            validation_results: GE validation result dictionary.

        Returns:
            List of QualityIssue from failed expectations.
        """
        issues: list[QualityIssue] = []

        if not validation_results.get("success", True):
            results = validation_results.get("results", [])

            for result in results:
                if not result.get("success", True):
                    expectation_type = result.get("expectation_config", {}).get(
                        "expectation_type", "unknown"
                    )

                    # Map expectation failures to severity
                    severity = QualitySeverity.WARNING
                    if "schema" in expectation_type.lower():
                        severity = QualitySeverity.CRITICAL
                    elif "null" in expectation_type.lower():
                        severity = QualitySeverity.WARNING

                    issues.append(
                        QualityIssue(
                            rule_name=expectation_type,
                            severity=severity,
                            message=f"Expectation failed: {expectation_type}",
                            details=result.get("result", {}),
                        )
                    )

        return issues

    def _determine_status(self, issues: list[QualityIssue]) -> QualityGateStatus:
        """Determine gate status based on issues and mode.

        Args:
            issues: List of quality issues found.

        Returns:
            QualityGateStatus based on severity and mode.
        """
        if not issues:
            return QualityGateStatus.PASSED

        has_critical = any(i.severity == QualitySeverity.CRITICAL for i in issues)
        has_warning = any(i.severity == QualitySeverity.WARNING for i in issues)

        # Critical issues always block
        if has_critical:
            return QualityGateStatus.BLOCKED

        # In block mode, warnings also block
        if has_warning and self.mode == "block":
            return QualityGateStatus.BLOCKED

        # In warn mode, warnings are just logged
        if has_warning:
            return QualityGateStatus.WARNED

        return QualityGateStatus.PASSED

    def _log_result(self, result: QualityGateResult) -> None:
        """Log quality gate result."""
        if result.passed:
            if result.status == QualityGateStatus.WARNED:
                logger.warning(
                    f"⚠️ Quality gate '{self.gate_name}' passed with warnings: "
                    f"{len(result.issues)} issues"
                )
            else:
                logger.info(f"✅ Quality gate '{self.gate_name}' PASSED")
        else:
            logger.error(
                f"🛑 Quality gate '{self.gate_name}' BLOCKED: {len(result.issues)} issues found"
            )
            for issue in result.issues:
                logger.error(f"  - [{issue.severity.value}] {issue.message}")


class QualityGateBlocked(Exception):  # noqa: N818  intentional name; "Blocked" matches the QualityGateStatus enum value, not generic error semantics
    """Exception raised when a quality gate blocks the pipeline."""

    def __init__(self, result: QualityGateResult):
        self.result = result
        issues_summary = ", ".join(f"{i.rule_name}: {i.message}" for i in result.issues[:3])
        super().__init__(
            f"Quality gate '{result.gate_name}' blocked for {result.layer} layer: {issues_summary}"
        )


# ============================================================================
# Built-in Quality Rules
# ============================================================================


def schema_drift_rule(
    expected_columns: set[str],
) -> Callable[[list[dict[str, Any]]], list[QualityIssue]]:
    """Create a rule that detects schema drift.

    Args:
        expected_columns: Set of expected column names.

    Returns:
        Rule function.
    """

    def check_schema(data: list[dict[str, Any]]) -> list[QualityIssue]:
        if not data:
            return []

        actual_columns = set(data[0].keys())
        missing = expected_columns - actual_columns
        extra = actual_columns - expected_columns

        issues: list[QualityIssue] = []

        if missing:
            issues.append(
                QualityIssue(
                    rule_name="schema_drift",
                    severity=QualitySeverity.CRITICAL,
                    message=f"Missing expected columns: {missing}",
                    details={"missing_columns": list(missing)},
                )
            )

        if extra:
            issues.append(
                QualityIssue(
                    rule_name="schema_drift",
                    severity=QualitySeverity.WARNING,
                    message=f"Unexpected columns found: {extra}",
                    details={"extra_columns": list(extra)},
                )
            )

        return issues

    return check_schema


def null_check_rule(
    required_columns: list[str],
) -> Callable[[list[dict[str, Any]]], list[QualityIssue]]:
    """Create a rule that checks for null values in required columns.

    Args:
        required_columns: List of columns that should not have nulls.

    Returns:
        Rule function.
    """

    def check_nulls(data: list[dict[str, Any]]) -> list[QualityIssue]:
        issues: list[QualityIssue] = []

        for col in required_columns:
            null_count = sum(1 for record in data if record.get(col) is None)

            if null_count > 0:
                null_pct = (null_count / len(data)) * 100
                severity = QualitySeverity.CRITICAL if null_pct > 10 else QualitySeverity.WARNING

                issues.append(
                    QualityIssue(
                        rule_name="null_check",
                        severity=severity,
                        message=f"Column '{col}' has {null_count} null values ({null_pct:.1f}%)",
                        affected_records=null_count,
                        details={"column": col, "null_percentage": null_pct},
                    )
                )

        return issues

    return check_nulls


def range_check_rule(
    column: str,
    min_val: float,
    max_val: float,
) -> Callable[[list[dict[str, Any]]], list[QualityIssue]]:
    """Create a rule that checks values are within expected range.

    Args:
        column: Column to check.
        min_val: Minimum expected value.
        max_val: Maximum expected value.

    Returns:
        Rule function.
    """

    def check_range(data: list[dict[str, Any]]) -> list[QualityIssue]:
        out_of_range = [
            record
            for record in data
            if record.get(column) is not None and not (min_val <= record[column] <= max_val)
        ]

        if out_of_range:
            return [
                QualityIssue(
                    rule_name="range_check",
                    severity=QualitySeverity.WARNING,
                    message=f"Column '{column}' has {len(out_of_range)} values outside range [{min_val}, {max_val}]",
                    affected_records=len(out_of_range),
                    details={
                        "column": column,
                        "expected_range": [min_val, max_val],
                        "sample_violations": [r[column] for r in out_of_range[:5]],
                    },
                )
            ]

        return []

    return check_range


def freshness_rule(
    timestamp_column: str,
    max_age_hours: int = 24,
) -> Callable[[list[dict[str, Any]]], list[QualityIssue]]:
    """Create a rule that checks data freshness.

    Args:
        timestamp_column: Column containing timestamps.
        max_age_hours: Maximum acceptable age in hours.

    Returns:
        Rule function.
    """

    def check_freshness(data: list[dict[str, Any]]) -> list[QualityIssue]:
        if not data:
            return []

        now = datetime.now(timezone.utc)
        stale_count = 0

        for record in data:
            ts_value = record.get(timestamp_column)
            if ts_value:
                try:
                    if isinstance(ts_value, str):
                        ts = datetime.fromisoformat(ts_value.replace("Z", "+00:00"))
                    else:
                        ts = ts_value

                    age_hours = (now - ts).total_seconds() / 3600
                    if age_hours > max_age_hours:
                        stale_count += 1
                except (ValueError, TypeError):
                    pass

        if stale_count > 0:
            return [
                QualityIssue(
                    rule_name="freshness_check",
                    severity=QualitySeverity.WARNING,
                    message=f"{stale_count} records are older than {max_age_hours} hours",
                    affected_records=stale_count,
                )
            ]

        return []

    return check_freshness
