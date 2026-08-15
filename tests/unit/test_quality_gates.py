"""Unit tests for Quality Gates."""

import pytest
from uuid import uuid4

from data_pipeline.quality.gates import (
    QualityGate,
    QualityGateResult,
    QualityGateStatus,
    QualityIssue,
    QualitySeverity,
    QualityGateBlocked,
    build_gate_for_layer,
    schema_drift_rule,
    null_check_rule,
    range_check_rule,
    freshness_rule,
    unique_check_rule,
)


class TestQualityGate:
    """Tests for QualityGate."""

    @pytest.fixture
    def gate(self):
        """Create a quality gate in block mode."""
        return QualityGate("test_gate", mode="block")

    @pytest.fixture
    def warn_gate(self):
        """Create a quality gate in warn mode."""
        return QualityGate("test_gate", mode="warn")

    @pytest.mark.unit
    def test_gate_initialization(self, gate):
        """Test gate initializes correctly."""
        assert gate.gate_name == "test_gate"
        assert gate.mode == "block"
        assert gate.run_id is not None

    @pytest.mark.unit
    def test_gate_add_rule(self, gate):
        """Test adding rules to gate."""
        def dummy_rule(data):
            return []
        
        result = gate.add_rule(dummy_rule)
        
        assert result is gate  # Method chaining
        assert len(gate._rules) == 1

    @pytest.mark.unit
    def test_gate_evaluate_passes(self, gate, sample_bronze_data):
        """Test gate passes with valid data."""
        result = gate.evaluate(sample_bronze_data, "bronze")
        
        assert isinstance(result, QualityGateResult)
        assert result.passed is True
        assert result.status == QualityGateStatus.PASSED

    @pytest.mark.unit
    def test_gate_evaluate_with_warning(self, warn_gate, sample_bronze_data):
        """Test gate warns but doesn't block in warn mode."""
        def warning_rule(data):
            return [QualityIssue(
                rule_name="test_warning",
                severity=QualitySeverity.WARNING,
                message="Warning message",
            )]
        
        warn_gate.add_rule(warning_rule)
        result = warn_gate.evaluate(sample_bronze_data, "bronze")
        
        assert result.passed is True
        assert result.status == QualityGateStatus.WARNED
        assert len(result.issues) == 1

    @pytest.mark.unit
    def test_gate_evaluate_blocks_on_critical(self, gate, sample_bronze_data):
        """Test gate blocks on critical issues."""
        def critical_rule(data):
            return [QualityIssue(
                rule_name="test_critical",
                severity=QualitySeverity.CRITICAL,
                message="Critical issue",
            )]
        
        gate.add_rule(critical_rule)
        result = gate.evaluate(sample_bronze_data, "bronze")
        
        assert result.passed is False
        assert result.blocked is True
        assert result.status == QualityGateStatus.BLOCKED

    @pytest.mark.unit
    def test_gate_blocks_on_warning_in_block_mode(self, gate, sample_bronze_data):
        """Test gate blocks on warnings when in block mode."""
        def warning_rule(data):
            return [QualityIssue(
                rule_name="test_warning",
                severity=QualitySeverity.WARNING,
                message="Warning message",
            )]
        
        gate.add_rule(warning_rule)
        result = gate.evaluate(sample_bronze_data, "bronze")
        
        assert result.passed is False
        assert result.status == QualityGateStatus.BLOCKED

    @pytest.mark.unit
    def test_gate_result_to_dict(self, gate, sample_bronze_data):
        """Test gate result serialization."""
        result = gate.evaluate(sample_bronze_data, "bronze")
        
        result_dict = result.to_dict()
        
        assert "gate_name" in result_dict
        assert "run_id" in result_dict
        assert "status" in result_dict
        assert "layer" in result_dict
        assert "metrics" in result_dict


class TestQualityRules:
    """Tests for built-in quality rules."""

    @pytest.mark.unit
    def test_schema_drift_rule_passes(self, sample_bronze_data):
        """Test schema drift rule passes with expected columns."""
        expected = {"city", "country", "temperature_celsius", "humidity"}
        rule = schema_drift_rule(expected)
        
        issues = rule(sample_bronze_data)
        
        # Should pass (no critical issues about missing columns)
        critical_missing = [i for i in issues if "Missing" in i.message]
        assert len(critical_missing) == 0

    @pytest.mark.unit
    def test_schema_drift_rule_detects_missing(self, sample_bronze_data):
        """Test schema drift rule detects missing columns."""
        expected = {"city", "country", "missing_column"}
        rule = schema_drift_rule(expected)
        
        issues = rule(sample_bronze_data)
        
        assert len(issues) > 0
        assert any("Missing" in i.message for i in issues)
        assert any(i.severity == QualitySeverity.CRITICAL for i in issues)

    @pytest.mark.unit
    def test_null_check_rule_passes(self, sample_bronze_data):
        """Test null check passes with no nulls."""
        rule = null_check_rule(["city", "country"])
        
        issues = rule(sample_bronze_data)
        
        assert len(issues) == 0

    @pytest.mark.unit
    def test_null_check_rule_detects_nulls(self):
        """Test null check detects null values."""
        data = [
            {"city": "London", "value": 1},
            {"city": None, "value": 2},
            {"city": "Paris", "value": None},
        ]
        
        rule = null_check_rule(["city"])
        issues = rule(data)
        
        assert len(issues) == 1
        assert issues[0].affected_records == 1

    @pytest.mark.unit
    def test_range_check_rule_passes(self, sample_bronze_data):
        """Test range check passes with valid values."""
        rule = range_check_rule("temperature_celsius", -100, 100)
        
        issues = rule(sample_bronze_data)
        
        assert len(issues) == 0

    @pytest.mark.unit
    def test_range_check_rule_detects_violations(self):
        """Test range check detects out-of-range values."""
        data = [
            {"temperature": 25},
            {"temperature": 150},  # Out of range
            {"temperature": -50},
        ]
        
        rule = range_check_rule("temperature", -100, 100)
        issues = rule(data)
        
        assert len(issues) == 1
        assert issues[0].affected_records == 1


class TestQualityGateBlocked:
    """Tests for QualityGateBlocked exception."""

    @pytest.mark.unit
    def test_exception_message(self):
        """Test exception contains result information."""
        result = QualityGateResult(
            gate_name="test_gate",
            run_id=uuid4(),
            status=QualityGateStatus.BLOCKED,
            layer="bronze",
            issues=[
                QualityIssue(
                    rule_name="test_rule",
                    severity=QualitySeverity.CRITICAL,
                    message="Test failure",
                )
            ],
        )
        
        exc = QualityGateBlocked(result)

        assert "test_gate" in str(exc)
        assert "bronze" in str(exc)
        assert exc.result == result


class TestUniqueCheckRule:
    """Tests for the unique_check_rule (replaces GE compound-uniqueness)."""

    @pytest.mark.unit
    def test_unique_check_passes_when_keys_distinct(self):
        data = [
            {"city": "London", "timestamp": "2024-01-30T12:00:00+00:00"},
            {"city": "London", "timestamp": "2024-01-30T13:00:00+00:00"},
            {"city": "Paris", "timestamp": "2024-01-30T12:00:00+00:00"},
        ]
        issues = unique_check_rule(["city", "timestamp"])(data)
        assert issues == []

    @pytest.mark.unit
    def test_unique_check_detects_duplicate_compound_key(self):
        data = [
            {"city": "London", "timestamp": "2024-01-30T12:00:00+00:00"},
            {"city": "London", "timestamp": "2024-01-30T12:00:00+00:00"},
        ]
        issues = unique_check_rule(["city", "timestamp"])(data)
        assert len(issues) == 1
        assert issues[0].severity == QualitySeverity.CRITICAL
        assert issues[0].affected_records == 1

    @pytest.mark.unit
    def test_unique_check_empty_data(self):
        assert unique_check_rule(["city"])([]) == []


class TestBuildGateForLayer:
    """Tests for the per-layer gate factory shared by pipeline and CLI."""

    @pytest.mark.unit
    def test_bronze_gate_passes_on_valid_data(self, sample_bronze_data):
        gate = build_gate_for_layer("bronze", mode="block")
        result = gate.evaluate(sample_bronze_data, "bronze")
        assert result.passed is True

    @pytest.mark.unit
    def test_silver_gate_named_per_layer(self):
        gate = build_gate_for_layer("silver", mode="warn")
        assert gate.gate_name == "silver_quality_gate"
        assert gate.mode == "warn"

    @pytest.mark.unit
    def test_gold_gate_blocks_on_duplicate_key(self):
        data = [
            {"city": "London", "temperature_celsius": 12.0, "timestamp": "2024-01-30T12:00:00+00:00"},
            {"city": "London", "temperature_celsius": 12.0, "timestamp": "2024-01-30T12:00:00+00:00"},
        ]
        gate = build_gate_for_layer("gold", mode="block")
        result = gate.evaluate(data, "gold")
        assert result.blocked is True

    @pytest.mark.unit
    def test_unknown_layer_raises(self):
        with pytest.raises(ValueError):
            build_gate_for_layer("platinum")
