from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover
    try:
        import tomli as tomllib  # type: ignore[no-redef]
    except ModuleNotFoundError:
        tomllib = None


@dataclass
class Config:
    db_path: str = "labelwatch.db"
    service_url: str = "https://bsky.social"
    labeler_dids: List[str] = field(default_factory=list)

    window_minutes: int = 15
    baseline_hours: int = 24
    spike_k: float = 10.0
    min_current_count: int = 50
    flip_flop_window_hours: int = 24
    max_events_per_scan: int = 200000
    max_evidence: int = 50

    concentration_window_hours: int = 24
    concentration_threshold: float = 0.25
    concentration_min_labels: int = 20

    churn_window_hours: int = 24
    churn_threshold: float = 0.8
    churn_min_targets: int = 10

    def to_receipt_dict(self) -> dict:
        return {
            "window_minutes": self.window_minutes,
            "baseline_hours": self.baseline_hours,
            "spike_k": self.spike_k,
            "min_current_count": self.min_current_count,
            "flip_flop_window_hours": self.flip_flop_window_hours,
            "max_events_per_scan": self.max_events_per_scan,
            "max_evidence": self.max_evidence,
            "concentration_window_hours": self.concentration_window_hours,
            "concentration_threshold": self.concentration_threshold,
            "concentration_min_labels": self.concentration_min_labels,
            "churn_window_hours": self.churn_window_hours,
            "churn_threshold": self.churn_threshold,
            "churn_min_targets": self.churn_min_targets,
        }


def load_config(path: Optional[str]) -> Config:
    if not path:
        return Config()
    if tomllib is None:
        raise RuntimeError("tomllib not available; use Python 3.11+ or omit config file")
    with open(path, "rb") as f:
        data = tomllib.load(f)
    cfg = Config()
    for field_name in cfg.__dataclass_fields__:
        if field_name in data:
            setattr(cfg, field_name, data[field_name])
    return cfg
