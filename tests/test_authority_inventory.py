"""Tests for authority_effect classifier and inventory aggregation.

Proves:
  - Known governance/enforcement/visibility/advisory/reputational/descriptive/
    telemetry/decorative labels land in the right buckets.
  - Unknown labels surface in `unknown`, not silently dropped.
  - The inventory's per-group label counts sum to the total label count.
  - Per-label aggregates (event_count, labeler_count, target_count) are correct.
  - HTML renderer surfaces the actual label list and is descriptive in tone.
"""
from __future__ import annotations

import time

from labelwatch import db
import pytest

from labelwatch.authority_inventory import (
    DEFAULT_OPEN_GROUPS,
    build_authority_effect_inventory,
    build_per_labeler_authority_inventories,
    render_authority_effect_html,
    render_labeler_authority_profile_html,
)
from labelwatch.label_family import (
    AUTHORITY_EFFECT_ORDER,
    LABELER_DEFAULT_EFFECT,
    classify_authority_effect,
    normalize_family,
)


# ---------------------------------------------------------------------------
# Classifier-level tests: no DB needed
# ---------------------------------------------------------------------------


def test_enforcement_instruction_includes_mod_takedown():
    assert classify_authority_effect("mod-takedown") == "enforcement_instruction"


def test_visibility_affecting_includes_mod_hide_and_gate():
    assert classify_authority_effect("mod-hide") == "visibility_affecting"
    assert classify_authority_effect("mod-gate") == "visibility_affecting"


def test_advisory_includes_mod_warn_and_nudity_and_graphic_media():
    assert classify_authority_effect("mod-warn") == "advisory"
    assert classify_authority_effect("nudity") == "advisory"
    assert classify_authority_effect("graphic-media") == "advisory"


def test_reputational_policy_claims():
    # Policy-claim families that attach normative charge.
    for family in (
        "spam", "misleading", "harassment", "hate", "violence",
        "adult-sexual", "impersonation", "inauthenticity",
    ):
        assert classify_authority_effect(family) == "reputational", family


def test_reputational_political_affiliation_labels():
    for family in (
        "uspol", "trump", "maga-trump", "elon-musk",
        "terf-gc", "gaza-genocide-supporter",
        "inverted-red-triangle", "hammer-sickle",
    ):
        assert classify_authority_effect(family) == "reputational", family


def test_reputational_stance_category_accusations():
    # Promoted from unknown after observing them in the live report.
    # The string marks a target by socially charged stance/category, not by
    # behavior, infrastructure, or enforcement action.
    for family in ("ai-hater", "substack-platforms-nazis"):
        assert classify_authority_effect(family) == "reputational", family


def test_reputational_interpretive_metric_labels():
    # Verdict-shaped "metrics" that are really claims.
    for family in (
        "fringe-media", "amplifier", "engagementfarmer",
        "low-quality-replies", "modlist-author",
        "troll", "intolerance", "intolerant",
    ):
        assert classify_authority_effect(family) == "reputational", family


def test_descriptive_includes_identity_families():
    for family in (
        "gay-post", "gay-user", "trans-post", "sapphic", "bisexual",
        "pan", "religion",
        "he", "she", "they", "it", "hethey", "shethey", "sheher", "hehim", "theythem",
    ):
        assert classify_authority_effect(family) == "descriptive", family


def test_telemetry_includes_raw_behavioral_metrics():
    for family in (
        "handle-changed", "many-handle-chgs",
        "bot", "bot-reply", "new-acct-replies",
        "bulk-following", "follow-farming", "mass-follow-high",
        "posting-daily-made-over-100-posts-yesterday",
        "no-gap-more-than-one-hours",
        "high-metadata-changes-five",
        "posted-same-url-mid",
        "site-standard", "internal-independent", "internal-other",
    ):
        assert classify_authority_effect(family) == "telemetry", family


def test_decorative_includes_novelty_badge_families():
    for family in (
        "scat-post", "urine", "feces", "diaper",
        "animalistic-mask", "sports-betting", "spoiler-parent",
    ):
        assert classify_authority_effect(family) == "decorative", family


def test_unknown_label_surfaced_not_dropped():
    # A label not in the map and not in DOMAIN_MAP returns unknown.
    # No structural fallback, no guessing.
    assert classify_authority_effect("a-novel-bespoke-label") == "unknown"
    assert classify_authority_effect("weird-new-thing") == "unknown"


def test_unknown_for_unmapped_bang_prefix():
    # ATProto !-prefix is reserved for protocol actions, but a new unknown
    # !-prefix label should NOT default to enforcement_instruction. The
    # classifier refuses to guess; the report surfaces it as unknown.
    assert classify_authority_effect("!some-future-action") == "unknown"


def test_unknown_for_unmapped_novelty_domain_label():
    # `intolerance` is mapped explicitly to reputational; a hypothetical
    # unmapped novelty-domain label should NOT default to decorative — the
    # classifier returns unknown rather than collapsing into a domain synonym.
    assert classify_authority_effect("brand-new-novelty-thing") == "unknown"


def test_classifier_returns_one_of_known_values():
    # Every output must be in AUTHORITY_EFFECT_ORDER.
    samples = [
        "mod-takedown", "mod-hide", "mod-warn", "nudity",
        "spam", "terf-gc", "gay-post", "handle-changed", "scat-post",
        "totally-unknown-label",
    ]
    for s in samples:
        assert classify_authority_effect(s) in AUTHORITY_EFFECT_ORDER, s


# ---------------------------------------------------------------------------
# Inventory aggregation: in-memory DB
# ---------------------------------------------------------------------------


def _make_db():
    conn = db.connect(":memory:")
    db.init_db(conn)
    return conn


def _seed(conn, events):
    """Seed label_events. Each event: labeler_did, val, target_did, ts, [neg]."""
    for i, e in enumerate(events):
        conn.execute(
            "INSERT INTO label_events(labeler_did, uri, val, neg, ts, event_hash, target_did) "
            "VALUES(?, ?, ?, ?, ?, ?, ?)",
            (
                e["labeler_did"],
                e.get("uri", f"at://{e['target_did']}/app.bsky.feed.post/{i}"),
                e["val"],
                e.get("neg", 0),
                e["ts"],
                e.get("event_hash", f"hash_{i}_{time.monotonic_ns()}"),
                e["target_did"],
            ),
        )
    conn.commit()


WINDOW_START = "2026-05-01T00:00:00Z"
WINDOW_END = "2026-06-01T00:00:00Z"
IN_WINDOW = "2026-05-15T12:00:00Z"


def test_inventory_groups_labels_by_authority_effect():
    conn = _make_db()
    _seed(conn, [
        # enforcement_instruction
        {"labeler_did": "did:plc:A", "val": "!takedown", "target_did": "did:plc:t1", "ts": IN_WINDOW},
        # visibility_affecting
        {"labeler_did": "did:plc:A", "val": "!hide", "target_did": "did:plc:t1", "ts": IN_WINDOW},
        {"labeler_did": "did:plc:B", "val": "!hide", "target_did": "did:plc:t2", "ts": IN_WINDOW},
        # advisory
        {"labeler_did": "did:plc:A", "val": "!warn", "target_did": "did:plc:t1", "ts": IN_WINDOW},
        # reputational (policy claim)
        {"labeler_did": "did:plc:A", "val": "spam", "target_did": "did:plc:t3", "ts": IN_WINDOW},
        {"labeler_did": "did:plc:B", "val": "spam", "target_did": "did:plc:t3", "ts": IN_WINDOW},
        {"labeler_did": "did:plc:B", "val": "spam", "target_did": "did:plc:t4", "ts": IN_WINDOW},
        # reputational (political)
        {"labeler_did": "did:plc:C", "val": "terf-gc", "target_did": "did:plc:t5", "ts": IN_WINDOW},
        # descriptive (identity)
        {"labeler_did": "did:plc:D", "val": "gay-post", "target_did": "did:plc:t6", "ts": IN_WINDOW},
        # telemetry
        {"labeler_did": "did:plc:E", "val": "handle-changed", "target_did": "did:plc:t7", "ts": IN_WINDOW},
        # decorative
        {"labeler_did": "did:plc:F", "val": "scat-post", "target_did": "did:plc:t8", "ts": IN_WINDOW},
        # unknown
        {"labeler_did": "did:plc:G", "val": "a-novel-bespoke-label", "target_did": "did:plc:t9", "ts": IN_WINDOW},
    ])

    inv = build_authority_effect_inventory(conn, WINDOW_START, WINDOW_END)

    def vals_in(group: str) -> set[str]:
        return {lbl["value"] for lbl in inv["groups"][group]["labels"]}

    assert "!takedown" in vals_in("enforcement_instruction")
    assert "!hide" in vals_in("visibility_affecting")
    assert "!warn" in vals_in("advisory")
    assert "spam" in vals_in("reputational")
    assert "terf-gc" in vals_in("reputational")
    assert "gay-post" in vals_in("descriptive")
    assert "handle-changed" in vals_in("telemetry")
    assert "scat-post" in vals_in("decorative")
    assert "a-novel-bespoke-label" in vals_in("unknown")


def test_inventory_does_not_silently_drop_labels():
    """Sum of group label_counts must equal total distinct labels observed."""
    conn = _make_db()
    _seed(conn, [
        # Mix of known and unknown labels
        {"labeler_did": "did:plc:A", "val": "spam", "target_did": "did:plc:t1", "ts": IN_WINDOW},
        {"labeler_did": "did:plc:A", "val": "!hide", "target_did": "did:plc:t1", "ts": IN_WINDOW},
        {"labeler_did": "did:plc:A", "val": "scat-post", "target_did": "did:plc:t2", "ts": IN_WINDOW},
        {"labeler_did": "did:plc:A", "val": "first-unknown", "target_did": "did:plc:t3", "ts": IN_WINDOW},
        {"labeler_did": "did:plc:A", "val": "second-unknown", "target_did": "did:plc:t4", "ts": IN_WINDOW},
        {"labeler_did": "did:plc:A", "val": "handle-changed", "target_did": "did:plc:t5", "ts": IN_WINDOW},
    ])

    inv = build_authority_effect_inventory(conn, WINDOW_START, WINDOW_END)

    total_in_groups = sum(g["label_count"] for g in inv["groups"].values())
    assert inv["total_label_count"] == 6
    assert total_in_groups == inv["total_label_count"], (
        "Sum of group label_counts must equal the total — labels must not be "
        "silently dropped."
    )

    # Unknown labels must be individually listed (not just counted).
    unknown_values = {lbl["value"] for lbl in inv["groups"]["unknown"]["labels"]}
    assert "first-unknown" in unknown_values
    assert "second-unknown" in unknown_values


def test_inventory_excludes_negations():
    """neg=1 (label removal) events are excluded from active-application counts."""
    conn = _make_db()
    _seed(conn, [
        {"labeler_did": "did:plc:A", "val": "spam", "target_did": "did:plc:t1", "ts": IN_WINDOW, "neg": 0},
        {"labeler_did": "did:plc:A", "val": "spam", "target_did": "did:plc:t2", "ts": IN_WINDOW, "neg": 1},
        {"labeler_did": "did:plc:A", "val": "spam", "target_did": "did:plc:t3", "ts": IN_WINDOW, "neg": 0},
    ])
    inv = build_authority_effect_inventory(conn, WINDOW_START, WINDOW_END)
    spam_rows = [lbl for lbl in inv["groups"]["reputational"]["labels"] if lbl["value"] == "spam"]
    assert len(spam_rows) == 1
    assert spam_rows[0]["event_count"] == 2  # only neg=0 rows counted


def test_inventory_excludes_out_of_window_events():
    conn = _make_db()
    _seed(conn, [
        {"labeler_did": "did:plc:A", "val": "spam", "target_did": "did:plc:t1", "ts": "2026-04-01T00:00:00Z"},
        {"labeler_did": "did:plc:A", "val": "spam", "target_did": "did:plc:t2", "ts": IN_WINDOW},
        {"labeler_did": "did:plc:A", "val": "spam", "target_did": "did:plc:t3", "ts": "2026-06-15T00:00:00Z"},
    ])
    inv = build_authority_effect_inventory(conn, WINDOW_START, WINDOW_END)
    spam_rows = [lbl for lbl in inv["groups"]["reputational"]["labels"] if lbl["value"] == "spam"]
    assert len(spam_rows) == 1
    assert spam_rows[0]["event_count"] == 1


def test_inventory_per_label_aggregates():
    """event_count = rows; labeler_count = distinct labeler_did; target_count = distinct target_did."""
    conn = _make_db()
    _seed(conn, [
        # spam: 5 events, 2 labelers, 3 targets
        {"labeler_did": "did:plc:A", "val": "spam", "target_did": "did:plc:t1", "ts": IN_WINDOW},
        {"labeler_did": "did:plc:A", "val": "spam", "target_did": "did:plc:t1", "ts": IN_WINDOW},  # dup target
        {"labeler_did": "did:plc:A", "val": "spam", "target_did": "did:plc:t2", "ts": IN_WINDOW},
        {"labeler_did": "did:plc:B", "val": "spam", "target_did": "did:plc:t2", "ts": IN_WINDOW},
        {"labeler_did": "did:plc:B", "val": "spam", "target_did": "did:plc:t3", "ts": IN_WINDOW},
    ])
    inv = build_authority_effect_inventory(conn, WINDOW_START, WINDOW_END)
    spam = next(
        lbl for lbl in inv["groups"]["reputational"]["labels"] if lbl["value"] == "spam"
    )
    assert spam["event_count"] == 5
    assert spam["labeler_count"] == 2
    assert spam["target_count"] == 3


def test_inventory_labels_sorted_by_event_count_desc():
    conn = _make_db()
    _seed(conn, [
        # scat-post: 3 events
        {"labeler_did": "did:plc:A", "val": "scat-post", "target_did": "did:plc:t1", "ts": IN_WINDOW},
        {"labeler_did": "did:plc:A", "val": "scat-post", "target_did": "did:plc:t2", "ts": IN_WINDOW},
        {"labeler_did": "did:plc:A", "val": "scat-post", "target_did": "did:plc:t3", "ts": IN_WINDOW},
        # diaper: 1 event
        {"labeler_did": "did:plc:A", "val": "diaper", "target_did": "did:plc:t4", "ts": IN_WINDOW},
        # urine: 2 events
        {"labeler_did": "did:plc:A", "val": "urine", "target_did": "did:plc:t5", "ts": IN_WINDOW},
        {"labeler_did": "did:plc:A", "val": "urine", "target_did": "did:plc:t6", "ts": IN_WINDOW},
    ])
    inv = build_authority_effect_inventory(conn, WINDOW_START, WINDOW_END)
    decorative = inv["groups"]["decorative"]["labels"]
    assert [lbl["value"] for lbl in decorative] == ["scat-post", "urine", "diaper"]


def test_inventory_carries_family_metadata():
    """The 'family' field per label = normalize_family(val)."""
    conn = _make_db()
    _seed(conn, [
        # 'porn' canonicalizes to family 'adult-sexual'
        {"labeler_did": "did:plc:A", "val": "porn", "target_did": "did:plc:t1", "ts": IN_WINDOW},
    ])
    inv = build_authority_effect_inventory(conn, WINDOW_START, WINDOW_END)
    porn_rows = [
        lbl for lbl in inv["groups"]["reputational"]["labels"] if lbl["value"] == "porn"
    ]
    assert len(porn_rows) == 1
    assert porn_rows[0]["family"] == "adult-sexual"
    assert porn_rows[0]["family"] == normalize_family("porn")


def test_inventory_top_level_shape():
    conn = _make_db()
    _seed(conn, [
        {"labeler_did": "did:plc:A", "val": "spam", "target_did": "did:plc:t1", "ts": IN_WINDOW},
    ])
    inv = build_authority_effect_inventory(conn, WINDOW_START, WINDOW_END)
    assert inv["axis"] == "authority_effect"
    assert inv["window"]["start"] == WINDOW_START
    assert inv["window"]["end"] == WINDOW_END
    assert "family_version" in inv
    assert inv["group_order"] == list(AUTHORITY_EFFECT_ORDER)
    # Every group key in AUTHORITY_EFFECT_ORDER is present (even if empty).
    for g in AUTHORITY_EFFECT_ORDER:
        assert g in inv["groups"]


# ---------------------------------------------------------------------------
# HTML rendering
# ---------------------------------------------------------------------------


def test_html_renders_actual_label_namespace():
    conn = _make_db()
    _seed(conn, [
        {"labeler_did": "did:plc:A", "val": "terf-gc", "target_did": "did:plc:t1", "ts": IN_WINDOW},
        {"labeler_did": "did:plc:A", "val": "scat-post", "target_did": "did:plc:t2", "ts": IN_WINDOW},
        {"labeler_did": "did:plc:A", "val": "weirdo-label", "target_did": "did:plc:t3", "ts": IN_WINDOW},
    ])
    inv = build_authority_effect_inventory(conn, WINDOW_START, WINDOW_END)
    html = render_authority_effect_html(inv)
    # Actual label strings present — the affordance is "see the namespace"
    assert "terf-gc" in html
    assert "scat-post" in html
    assert "weirdo-label" in html
    # Descriptive, not accusatory copy
    assert "moralizing" not in html.lower()
    assert "bad labeler" not in html.lower()
    # The reputational group description includes the descriptive disclaimer.
    assert "Not an inference about labeler intent" in html


def test_html_default_open_groups_match_spec():
    conn = _make_db()
    _seed(conn, [
        {"labeler_did": "did:plc:A", "val": "!takedown", "target_did": "did:plc:t1", "ts": IN_WINDOW},
        {"labeler_did": "did:plc:A", "val": "!hide", "target_did": "did:plc:t1", "ts": IN_WINDOW},
        {"labeler_did": "did:plc:A", "val": "spam", "target_did": "did:plc:t1", "ts": IN_WINDOW},
        {"labeler_did": "did:plc:A", "val": "weirdo-label", "target_did": "did:plc:t1", "ts": IN_WINDOW},
        {"labeler_did": "did:plc:A", "val": "!warn", "target_did": "did:plc:t1", "ts": IN_WINDOW},
        {"labeler_did": "did:plc:A", "val": "scat-post", "target_did": "did:plc:t1", "ts": IN_WINDOW},
        {"labeler_did": "did:plc:A", "val": "handle-changed", "target_did": "did:plc:t1", "ts": IN_WINDOW},
        {"labeler_did": "did:plc:A", "val": "gay-post", "target_did": "did:plc:t1", "ts": IN_WINDOW},
    ])
    inv = build_authority_effect_inventory(conn, WINDOW_START, WINDOW_END)
    html = render_authority_effect_html(inv)
    for group in DEFAULT_OPEN_GROUPS:
        # The summary line for each default-open group includes "<details open>"
        human = group.replace("_", " ")
        # Find the position of this group's summary, then check the preceding
        # <details ...> tag.
        marker = f"<strong>{human}</strong>"
        idx = html.find(marker)
        assert idx > 0, f"Group {group} missing from HTML"
        # Look back to the most recent <details before idx
        details_open = html.rfind("<details", 0, idx)
        assert details_open >= 0
        details_tag_end = html.find(">", details_open)
        assert " open" in html[details_open:details_tag_end + 1], (
            f"Group {group} should be open by default"
        )


def test_labeler_default_effect_binds_unmapped_val_when_all_emitters_hinted():
    """If a val isn't in the explicit map and all emitting labelers are in
    LABELER_DEFAULT_EFFECT with the same effect, the val gets that effect."""
    conn = _make_db()
    hinted_did = next(iter(LABELER_DEFAULT_EFFECT.keys()))
    expected_effect = LABELER_DEFAULT_EFFECT[hinted_did]
    _seed(conn, [
        {"labeler_did": hinted_did, "val": "crushed-piano",
         "target_did": "did:plc:victim1", "ts": IN_WINDOW},
        {"labeler_did": hinted_did, "val": "crushed-piano",
         "target_did": "did:plc:victim2", "ts": IN_WINDOW},
    ])
    inv = build_authority_effect_inventory(conn, WINDOW_START, WINDOW_END)
    decorative_vals = {lbl["value"] for lbl in inv["groups"][expected_effect]["labels"]}
    assert "crushed-piano" in decorative_vals
    row = next(lbl for lbl in inv["groups"][expected_effect]["labels"]
               if lbl["value"] == "crushed-piano")
    assert row["labeler_fallback"] is True


def test_labeler_default_does_not_override_explicit_val_map():
    """Label-level mapping always wins. A hinted-decorative labeler emitting
    `spam` does NOT get spam reclassified — spam stays reputational."""
    conn = _make_db()
    hinted_did = next(iter(LABELER_DEFAULT_EFFECT.keys()))
    _seed(conn, [
        {"labeler_did": hinted_did, "val": "spam",
         "target_did": "did:plc:t1", "ts": IN_WINDOW},
    ])
    inv = build_authority_effect_inventory(conn, WINDOW_START, WINDOW_END)
    reputational_vals = {lbl["value"] for lbl in inv["groups"]["reputational"]["labels"]}
    assert "spam" in reputational_vals
    row = next(lbl for lbl in inv["groups"]["reputational"]["labels"]
               if lbl["value"] == "spam")
    assert row["labeler_fallback"] is False


def test_labeler_default_only_fires_when_all_emitters_hinted():
    """If even one non-hinted labeler emits the val, fall through to unknown."""
    conn = _make_db()
    hinted_did = next(iter(LABELER_DEFAULT_EFFECT.keys()))
    _seed(conn, [
        {"labeler_did": hinted_did, "val": "weird-bespoke-thing",
         "target_did": "did:plc:t1", "ts": IN_WINDOW},
        # A non-hinted labeler emits the same val.
        {"labeler_did": "did:plc:notahint", "val": "weird-bespoke-thing",
         "target_did": "did:plc:t2", "ts": IN_WINDOW},
    ])
    inv = build_authority_effect_inventory(conn, WINDOW_START, WINDOW_END)
    unknown_vals = {lbl["value"] for lbl in inv["groups"]["unknown"]["labels"]}
    assert "weird-bespoke-thing" in unknown_vals


def test_html_truncation_preserves_full_list_in_json():
    """When HTML truncates a large group, the JSON artifact still has the full list."""
    conn = _make_db()
    # 60 distinct unknown labels — exceeds default max_labels_per_group=50
    events = []
    for i in range(60):
        events.append({
            "labeler_did": "did:plc:A",
            "val": f"unknown-label-{i:03d}",
            "target_did": f"did:plc:t{i}",
            "ts": IN_WINDOW,
        })
    _seed(conn, events)
    inv = build_authority_effect_inventory(conn, WINDOW_START, WINDOW_END)
    assert inv["groups"]["unknown"]["label_count"] == 60
    assert len(inv["groups"]["unknown"]["labels"]) == 60  # JSON: full list

    html = render_authority_effect_html(inv, max_labels_per_group=50)
    assert "+10 more labels" in html
    assert "authority_effect_inventory.json" in html


# ---------------------------------------------------------------------------
# Per-labeler authority profile tests
# ---------------------------------------------------------------------------


def test_per_labeler_inventory_filters_to_one_labeler():
    """Building with labeler_did= must restrict the inventory to that labeler only."""
    conn = _make_db()
    _seed(conn, [
        # Labeler A: spam (reputational) + !hide (visibility)
        {"labeler_did": "did:plc:A", "val": "spam", "target_did": "did:plc:t1", "ts": IN_WINDOW},
        {"labeler_did": "did:plc:A", "val": "spam", "target_did": "did:plc:t2", "ts": IN_WINDOW},
        {"labeler_did": "did:plc:A", "val": "!hide", "target_did": "did:plc:t1", "ts": IN_WINDOW},
        # Labeler B: emits gay-post — must NOT appear in A's profile
        {"labeler_did": "did:plc:B", "val": "gay-post", "target_did": "did:plc:t3", "ts": IN_WINDOW},
    ])

    inv = build_authority_effect_inventory(conn, WINDOW_START, WINDOW_END, labeler_did="did:plc:A")
    assert inv["scope"] == "labeler"
    assert inv["labeler_did"] == "did:plc:A"
    assert inv["total_event_count"] == 3
    assert inv["total_label_count"] == 2

    a_vals = set()
    for g in inv["groups"].values():
        for lbl in g["labels"]:
            a_vals.add(lbl["value"])
    assert "spam" in a_vals
    assert "!hide" in a_vals
    assert "gay-post" not in a_vals, "Labeler B's labels leaked into A's profile"


def test_per_labeler_inventory_target_counts_scoped_to_labeler():
    """target_count must reflect targets *this* labeler hit, not network-wide."""
    conn = _make_db()
    _seed(conn, [
        # A's spam targets: t1, t2  (2 distinct)
        {"labeler_did": "did:plc:A", "val": "spam", "target_did": "did:plc:t1", "ts": IN_WINDOW},
        {"labeler_did": "did:plc:A", "val": "spam", "target_did": "did:plc:t2", "ts": IN_WINDOW},
        # B's spam targets: t3, t4  (would inflate to 4 if not scoped)
        {"labeler_did": "did:plc:B", "val": "spam", "target_did": "did:plc:t3", "ts": IN_WINDOW},
        {"labeler_did": "did:plc:B", "val": "spam", "target_did": "did:plc:t4", "ts": IN_WINDOW},
    ])
    inv = build_authority_effect_inventory(conn, WINDOW_START, WINDOW_END, labeler_did="did:plc:A")
    spam_row = next(
        lbl for lbl in inv["groups"]["reputational"]["labels"] if lbl["value"] == "spam"
    )
    assert spam_row["event_count"] == 2
    assert spam_row["target_count"] == 2  # A's targets only, not 4


def test_per_labeler_renderer_requires_labeler_scope():
    """render_labeler_authority_profile_html must reject network-scope inventories
    to keep "per-labeler view" honest. Wrong-scope rendering would silently show
    network aggregate data on a per-labeler page."""
    conn = _make_db()
    _seed(conn, [
        {"labeler_did": "did:plc:A", "val": "spam", "target_did": "did:plc:t1", "ts": IN_WINDOW},
    ])
    inv_network = build_authority_effect_inventory(conn, WINDOW_START, WINDOW_END)
    with pytest.raises(ValueError, match="labeler_did"):
        render_labeler_authority_profile_html(inv_network)


def test_per_labeler_renderer_empty_window():
    """A labeler with no events in the window still renders cleanly."""
    conn = _make_db()
    inv = build_authority_effect_inventory(
        conn, WINDOW_START, WINDOW_END, labeler_did="did:plc:noevents"
    )
    html = render_labeler_authority_profile_html(inv)
    assert "Authority profile" in html
    assert "No active label events" in html
    # No distribution strip when empty
    assert "authority-profile-strip" not in html


def test_per_labeler_renderer_distribution_strip_present():
    """The horizontal distribution strip renders when there are events."""
    conn = _make_db()
    _seed(conn, [
        {"labeler_did": "did:plc:A", "val": "spam", "target_did": "did:plc:t1", "ts": IN_WINDOW},
        {"labeler_did": "did:plc:A", "val": "!hide", "target_did": "did:plc:t1", "ts": IN_WINDOW},
    ])
    inv = build_authority_effect_inventory(conn, WINDOW_START, WINDOW_END, labeler_did="did:plc:A")
    html = render_labeler_authority_profile_html(inv)
    assert 'class="authority-profile-strip"' in html
    # Both effects should appear in the strip
    assert "reputational" in html
    assert "visibility affecting" in html


def test_per_labeler_renderer_single_effect_summary_line():
    """When all events fall in one effect, the summary names it as primary."""
    conn = _make_db()
    _seed(conn, [
        {"labeler_did": "did:plc:A", "val": "spam", "target_did": f"did:plc:t{i}", "ts": IN_WINDOW}
        for i in range(10)
    ])
    inv = build_authority_effect_inventory(conn, WINDOW_START, WINDOW_END, labeler_did="did:plc:A")
    html = render_labeler_authority_profile_html(inv)
    assert "Primary effect: reputational" in html
    assert "100%" in html


def test_bulk_per_labeler_matches_per_call_results():
    """Bulk build must produce the same per-labeler inventories as N per-call
    builds. This is what justifies the collapse — same data, fewer queries."""
    conn = _make_db()
    _seed(conn, [
        {"labeler_did": "did:plc:A", "val": "spam", "target_did": "did:plc:t1", "ts": IN_WINDOW},
        {"labeler_did": "did:plc:A", "val": "spam", "target_did": "did:plc:t2", "ts": IN_WINDOW},
        {"labeler_did": "did:plc:A", "val": "!hide", "target_did": "did:plc:t1", "ts": IN_WINDOW},
        {"labeler_did": "did:plc:B", "val": "spam", "target_did": "did:plc:t3", "ts": IN_WINDOW},
        {"labeler_did": "did:plc:B", "val": "gay-post", "target_did": "did:plc:t4", "ts": IN_WINDOW},
    ])

    bulk = build_per_labeler_authority_inventories(conn, WINDOW_START, WINDOW_END)

    for did in ("did:plc:A", "did:plc:B"):
        per_call = build_authority_effect_inventory(
            conn, WINDOW_START, WINDOW_END, labeler_did=did
        )
        # Same totals
        assert bulk[did]["total_event_count"] == per_call["total_event_count"]
        assert bulk[did]["total_label_count"] == per_call["total_label_count"]
        # Same labels in each group
        for group_name in bulk[did]["group_order"]:
            bulk_vals = sorted(lbl["value"] for lbl in bulk[did]["groups"][group_name]["labels"])
            per_vals = sorted(lbl["value"] for lbl in per_call["groups"][group_name]["labels"])
            assert bulk_vals == per_vals, f"Group {group_name} disagrees for {did}"


def test_bulk_per_labeler_scopes_targets_correctly():
    """target_count in the bulk view must be per-(val, labeler), not network-wide.
    This is the SQLite trap the per-call version already guards against; the bulk
    version uses GROUP BY (val, labeler_did) so it gets the same property for free."""
    conn = _make_db()
    _seed(conn, [
        # A's spam targets: t1, t2  (2 distinct)
        {"labeler_did": "did:plc:A", "val": "spam", "target_did": "did:plc:t1", "ts": IN_WINDOW},
        {"labeler_did": "did:plc:A", "val": "spam", "target_did": "did:plc:t2", "ts": IN_WINDOW},
        # B's spam targets: t3 (1 distinct, would inflate to 3 if not scoped)
        {"labeler_did": "did:plc:B", "val": "spam", "target_did": "did:plc:t3", "ts": IN_WINDOW},
    ])
    bulk = build_per_labeler_authority_inventories(conn, WINDOW_START, WINDOW_END)

    a_spam = next(lbl for lbl in bulk["did:plc:A"]["groups"]["reputational"]["labels"] if lbl["value"] == "spam")
    b_spam = next(lbl for lbl in bulk["did:plc:B"]["groups"]["reputational"]["labels"] if lbl["value"] == "spam")
    assert a_spam["target_count"] == 2
    assert b_spam["target_count"] == 1


def test_bulk_per_labeler_omits_labelers_with_no_events():
    """A labeler row in the DB but no events in window must not appear in the
    bulk dict. Callers should treat a missing key as 'no events'."""
    conn = _make_db()
    _seed(conn, [
        {"labeler_did": "did:plc:has-events", "val": "spam", "target_did": "did:plc:t1", "ts": IN_WINDOW},
    ])
    # Also insert a labeler with a row in `labelers` but no events
    conn.execute(
        "INSERT INTO labelers(labeler_did) VALUES (?)",
        ("did:plc:no-events",),
    )
    conn.commit()

    bulk = build_per_labeler_authority_inventories(conn, WINDOW_START, WINDOW_END)
    assert "did:plc:has-events" in bulk
    assert "did:plc:no-events" not in bulk


def test_per_labeler_renderer_multi_effect_summary_line():
    """Three or more significant effects switch to a list, not a primary/secondary."""
    conn = _make_db()
    events = []
    # 10 events of each of 3 different families → 33% each
    for i in range(10):
        events.append({"labeler_did": "did:plc:A", "val": "spam", "target_did": f"did:plc:r{i}", "ts": IN_WINDOW})
        events.append({"labeler_did": "did:plc:A", "val": "!hide", "target_did": f"did:plc:v{i}", "ts": IN_WINDOW})
        events.append({"labeler_did": "did:plc:A", "val": "!warn", "target_did": f"did:plc:a{i}", "ts": IN_WINDOW})
    _seed(conn, events)
    inv = build_authority_effect_inventory(conn, WINDOW_START, WINDOW_END, labeler_did="did:plc:A")
    html = render_labeler_authority_profile_html(inv)
    assert "Effects with significant volume share" in html
    assert "reputational" in html
    assert "visibility affecting" in html
    assert "advisory" in html
