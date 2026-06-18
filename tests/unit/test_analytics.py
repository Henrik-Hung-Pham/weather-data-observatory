"""Unit tests for dashboard analytics helpers."""

import pytest

from data_pipeline.analytics import detect_temperature_anomalies


def _readings(city: str, temps: list[float]) -> list[dict]:
    return [{"city": city, "temperature_celsius": t} for t in temps]


@pytest.mark.unit
def test_flags_clear_outlier() -> None:
    # A tight cluster plus one extreme value.
    records = _readings("London", [12, 12.5, 11.8, 12.2, 50.0])
    anomalies = detect_temperature_anomalies(records, z_threshold=3.0)
    assert len(anomalies) == 1
    assert anomalies[0]["temperature_celsius"] == 50.0
    assert "z_score" in anomalies[0]


@pytest.mark.unit
def test_no_anomaly_for_uniform_data() -> None:
    records = _readings("Paris", [20, 21, 19, 20, 20])
    assert detect_temperature_anomalies(records, z_threshold=3.0) == []


@pytest.mark.unit
def test_zero_variance_is_skipped() -> None:
    records = _readings("Tokyo", [15, 15, 15, 15])
    assert detect_temperature_anomalies(records) == []


@pytest.mark.unit
def test_below_min_samples_is_skipped() -> None:
    records = _readings("Berlin", [10, 99])  # only 2 samples
    assert detect_temperature_anomalies(records, min_samples=3) == []


@pytest.mark.unit
def test_per_city_scoping() -> None:
    # An "extreme" value for one city is normal for another; scoring is per-city.
    records = _readings("Cold", [-30, -31, -29, -30]) + _readings("Hot", [40, 41, 39, 40])
    assert detect_temperature_anomalies(records) == []


@pytest.mark.unit
def test_handles_missing_and_nonnumeric_temps() -> None:
    records = [
        {"city": "X", "temperature_celsius": 10},
        {"city": "X", "temperature_celsius": None},
        {"city": "X", "temperature_celsius": "bad"},
        {"city": "X", "temperature_celsius": 10.5},
        {"city": "X", "temperature_celsius": 9.5},
        {"city": "X", "temperature_celsius": 80},
    ]
    anomalies = detect_temperature_anomalies(records, z_threshold=2.0)
    assert any(a["temperature_celsius"] == 80 for a in anomalies)


@pytest.mark.unit
def test_results_sorted_by_abs_zscore() -> None:
    records = _readings("S", [10, 11, 9, 10, 40, 70])
    anomalies = detect_temperature_anomalies(records, z_threshold=1.0)
    zscores = [abs(a["z_score"]) for a in anomalies]
    assert zscores == sorted(zscores, reverse=True)
