"""Run lineage manifest.

Captures, per pipeline run, the inputs (cities) and the artifacts each layer
produced (their S3 keys + record counts), so a run can be traced end-to-end:
which Bronze/Silver/Gold objects belong to run ``X``. The manifest is a plain
data structure (no S3/DB imports) so it is trivially unit-testable; the
pipeline serialises it to a ``lineage`` prefix in the data lake.
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


@dataclass
class Artifact:
    """A single data-lake object produced by a run."""

    layer: str
    key: str
    record_count: int


@dataclass
class LineageManifest:
    """End-to-end lineage for one pipeline run."""

    run_id: str
    cities: list[str]
    started_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    artifacts: list[Artifact] = field(default_factory=list)

    def add(self, layer: str, key: str, record_count: int) -> None:
        """Record an artifact produced for ``layer``."""
        self.artifacts.append(Artifact(layer=layer, key=key, record_count=record_count))

    def to_dict(self) -> dict[str, Any]:
        """Serialise to a JSON-friendly dict."""
        return {
            "run_id": self.run_id,
            "cities": self.cities,
            "started_at": self.started_at,
            "artifact_count": len(self.artifacts),
            "artifacts": [
                {"layer": a.layer, "key": a.key, "record_count": a.record_count}
                for a in self.artifacts
            ],
        }
