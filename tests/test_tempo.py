"""Tests for Paper 22 tempo estimation and observation health."""

from labelwatch.derive import TempoEstimate, estimate_labeler_tempo


class TestEstimateLabelerTempo:
    def test_insufficient_data(self):
        result = estimate_labeler_tempo([1.0, 2.0], last_event_age_secs=100)
        assert result.observation_health == "insufficient_data"
        assert result.t_p_median_secs is None
        assert result.confidence == "low"

    def test_healthy_high_volume(self):
        # Regular 60s interarrivals, last event 30s ago
        interarrivals = [60.0] * 200
        result = estimate_labeler_tempo(interarrivals, last_event_age_secs=30)
        assert result.t_p_median_secs == 60.0
        assert result.observation_health == "healthy"
        assert result.observation_ratio < 3.0
        assert result.confidence == "high"
        assert result.temporal_failure is None

    def test_lagging(self):
        # Regular 60s interarrivals, last event 5 minutes ago
        interarrivals = [60.0] * 50
        result = estimate_labeler_tempo(interarrivals, last_event_age_secs=300)
        assert result.observation_health == "lagging"
        assert result.observation_ratio >= 3.0
        assert result.observation_ratio < 10.0
        assert result.confidence == "medium"

    def test_blind(self):
        # Regular 60s interarrivals, last event 30 minutes ago
        interarrivals = [60.0] * 50
        result = estimate_labeler_tempo(interarrivals, last_event_age_secs=1800)
        assert result.observation_health == "blind"
        assert result.observation_ratio >= 10.0

    def test_stale_observation_failure(self):
        # Lagging with good probes → stale observation
        interarrivals = [60.0] * 50
        result = estimate_labeler_tempo(interarrivals, last_event_age_secs=600, probe_success_ratio=0.9)
        assert result.temporal_failure == "stale_observation"

    def test_probe_instability_failure(self):
        # Lagging with bad probes → probe instability
        interarrivals = [60.0] * 50
        result = estimate_labeler_tempo(interarrivals, last_event_age_secs=600, probe_success_ratio=0.3)
        assert result.temporal_failure == "probe_instability"

    def test_cadence_drift(self):
        # Starts fast (10s), recent events much slower (100s)
        interarrivals = [10.0] * 80 + [100.0] * 20
        result = estimate_labeler_tempo(interarrivals, last_event_age_secs=15)
        assert result.temporal_failure == "cadence_drift"

    def test_low_confidence(self):
        interarrivals = [60.0] * 10
        result = estimate_labeler_tempo(interarrivals, last_event_age_secs=30)
        assert result.confidence == "low"

    def test_medium_confidence(self):
        interarrivals = [60.0] * 50
        result = estimate_labeler_tempo(interarrivals, last_event_age_secs=30)
        assert result.confidence == "medium"

    def test_percentiles(self):
        # Mix of fast and slow
        interarrivals = [10.0] * 50 + [100.0] * 50
        result = estimate_labeler_tempo(interarrivals, last_event_age_secs=30)
        assert result.t_p_p25_secs is not None
        assert result.t_p_p75_secs is not None
        assert result.t_p_p25_secs < result.t_p_median_secs
        assert result.t_p_p75_secs > result.t_p_median_secs

    def test_filters_zero_and_negative(self):
        interarrivals = [0.0, -1.0, 60.0, 60.0, 60.0, 60.0, 60.0]
        result = estimate_labeler_tempo(interarrivals, last_event_age_secs=30)
        assert result.sample_count == 5  # only the 60s values
        assert result.t_p_median_secs == 60.0

    def test_no_last_event_age(self):
        interarrivals = [60.0] * 50
        result = estimate_labeler_tempo(interarrivals, last_event_age_secs=0)
        assert result.observation_ratio is None
        assert result.observation_health == "insufficient_data"
