"""Lightweight analytics helpers for the dashboard.

Kept dependency-free (stdlib only) and free of Streamlit/DB imports so the
logic is unit-testable in CI, separate from the presentation layer.
"""

from collections import defaultdict
from statistics import median
from typing import Any

# Consistency constant relating MAD to the standard deviation of a normal
# distribution (0.6745 = Phi^-1(0.75)). See Iglewicz & Hoaglin (1993).
_MAD_SCALE = 0.6745


def detect_temperature_anomalies(
    records: list[dict[str, Any]],
    z_threshold: float = 3.5,
    min_samples: int = 3,
) -> list[dict[str, Any]]:
    """Flag temperature readings that are statistical outliers per city.

    Uses the **modified z-score** (median + median absolute deviation) rather
    than mean + standard deviation: a single extreme value inflates the stdev
    so much that a plain z-score can't exceed ~sqrt(n-1) for small samples,
    making 3-sigma detection impossible. The MAD-based score is robust to the
    very outliers we want to catch.

        modified_z = 0.6745 * (x - median) / MAD

    Cities with fewer than ``min_samples`` readings, or with zero MAD (more
    than half the readings identical), are skipped — there's no meaningful
    spread to score against.

    Args:
        records: Gold weather records (each a dict with ``city`` and
            ``temperature_celsius``).
        z_threshold: Absolute modified z-score at/above which a reading is
            anomalous (3.5 is the Iglewicz-Hoaglin recommendation).
        min_samples: Minimum readings per city before scoring.

    Returns:
        Anomalous records (copies) sorted by descending absolute modified
        z-score, each with an added ``z_score`` key.
    """
    by_city: dict[Any, list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        temp = record.get("temperature_celsius")
        if temp is None:
            continue
        try:
            float(temp)
        except (TypeError, ValueError):
            continue
        by_city[record.get("city")].append(record)

    anomalies: list[dict[str, Any]] = []
    for recs in by_city.values():
        temps = [float(r["temperature_celsius"]) for r in recs]
        if len(temps) < min_samples:
            continue
        med = median(temps)
        mad = median([abs(t - med) for t in temps])
        if mad == 0:
            continue
        for record in recs:
            z = _MAD_SCALE * (float(record["temperature_celsius"]) - med) / mad
            if abs(z) >= z_threshold:
                anomalies.append({**record, "z_score": round(z, 2)})

    anomalies.sort(key=lambda r: abs(r["z_score"]), reverse=True)
    return anomalies
