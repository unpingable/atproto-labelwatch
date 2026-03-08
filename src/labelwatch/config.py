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

    discovery_enabled: bool = False
    discovery_interval_hours: int = 24
    reference_dids: List[str] = field(default_factory=lambda: [
        "did:plc:ar7c4by46qjdydhdevvrndac",   # Bluesky Moderation
        "did:plc:e4elbtctnfqocyfcml6h2lf7",   # Skywatch Blue
    ])

    multi_ingest_timeout: int = 15
    multi_ingest_budget: int = 300
    multi_ingest_max_pages: int = 5

    spike_min_count_reference: int = 50
    spike_min_count_default: int = 5
    confidence_min_events: int = 100
    confidence_min_age_hours: int = 168

    noise_policy_enabled: bool = True

    warmup_enabled: bool = True
    warmup_min_age_hours: int = 48
    warmup_min_events: int = 20
    warmup_min_scans: int = 3
    warmup_suppress_alerts: bool = True

    derive_interval_minutes: int = 30
    regime_hysteresis_scans: int = 2

    coverage_window_minutes: int = 30
    coverage_threshold: float = 0.5

    alert_budget_per_rule: int = 10  # max alerts per (rule, labeler) per window
    alert_budget_window_hours: int = 24  # budget window size

    boundary_enabled: bool = False  # Phase 1: behind flag
    boundary_window_hours: int = 24
    boundary_min_labelers: int = 2  # minimum distinct labelers per target
    boundary_min_events_per_labeler: int = 1  # binary presence: 1 family = 1 "event"
    boundary_jsd_min: float = 0.15  # contradiction edge threshold
    boundary_min_top_share: float = 0.30  # minimum top-family share
    boundary_lag_max_s: int = 21600  # 6 hours
    boundary_lag_min_overlap: float = 0.30  # minimum family overlap for lead/lag
    boundary_max_targets: int = 500  # cap shared-target scan
    boundary_participant_top_k: int = 25  # cap pairwise per target

    driftwatch_facts_path: str = ""  # path to facts.sqlite, empty = disabled

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
            "spike_min_count_reference": self.spike_min_count_reference,
            "spike_min_count_default": self.spike_min_count_default,
            "confidence_min_events": self.confidence_min_events,
            "confidence_min_age_hours": self.confidence_min_age_hours,
            "alert_budget_per_rule": self.alert_budget_per_rule,
            "alert_budget_window_hours": self.alert_budget_window_hours,
            "boundary_window_hours": self.boundary_window_hours,
            "boundary_min_labelers": self.boundary_min_labelers,
            "boundary_min_events_per_labeler": self.boundary_min_events_per_labeler,
            "boundary_jsd_min": self.boundary_jsd_min,
            "boundary_min_top_share": self.boundary_min_top_share,
            "boundary_lag_max_s": self.boundary_lag_max_s,
            "boundary_lag_min_overlap": self.boundary_lag_min_overlap,
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
