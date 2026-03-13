"""Tests for the Bluesky posting scaffold."""
from unittest.mock import MagicMock, patch

from labelwatch.posting import (
    BlueskyConfig,
    BlueskyPublisher,
    FindingPost,
    LinkCard,
)


def test_dry_run_post_text():
    cfg = BlueskyConfig(handle="test.bsky.social", app_password="x", dry_run=True)
    pub = BlueskyPublisher(cfg)
    result = pub.post_text("hello world")
    assert result["dry_run"] is True
    assert result["text"] == "hello world"


def test_dry_run_post_link_card():
    cfg = BlueskyConfig(handle="test.bsky.social", app_password="x", dry_run=True)
    pub = BlueskyPublisher(cfg)
    card = LinkCard(url="https://example.com", title="Example", description="desc")
    result = pub.post_link_card("check this out", card)
    assert result["dry_run"] is True
    assert result["text"] == "check this out"
    assert result["card"]["url"] == "https://example.com"
    assert result["card"]["title"] == "Example"


def test_finding_post_render_text():
    finding = FindingPost(
        headline="New labeler detected",
        summary="example.bsky.social started labeling content today.",
        detail_url="https://labelwatch.neutral.zone/v1/registry",
        card_title="Labelwatch: new labeler",
        card_description="Registry entry for a newly discovered labeler.",
    )
    text = finding.render_text()
    assert "New labeler detected" in text
    assert "example.bsky.social" in text
    assert "https://labelwatch.neutral.zone/v1/registry" in text
    # Three paragraphs separated by double newlines
    assert text.count("\n\n") == 2


def test_finding_post_to_link_card():
    finding = FindingPost(
        headline="headline",
        summary="summary",
        detail_url="https://example.com/finding",
        card_title="Card Title",
        card_description="Card desc",
    )
    card = finding.to_link_card()
    assert card.url == "https://example.com/finding"
    assert card.title == "Card Title"
    assert card.description == "Card desc"
    assert card.thumb_path is None


def test_dry_run_post_finding():
    cfg = BlueskyConfig(handle="test.bsky.social", app_password="x", dry_run=True)
    pub = BlueskyPublisher(cfg)
    finding = FindingPost(
        headline="Disagreement detected",
        summary="Two labelers classify the same targets differently.",
        detail_url="https://labelwatch.neutral.zone/boundary",
        card_title="Boundary fight",
        card_description="A substantive disagreement between labelers.",
    )
    result = pub.post_finding(finding)
    assert result["dry_run"] is True
    assert "Disagreement detected" in result["text"]
    assert result["card"]["url"] == "https://labelwatch.neutral.zone/boundary"


def test_config_from_env():
    env = {
        "LABELWATCH_BSKY_HANDLE": "labelwatch.bsky.social",
        "LABELWATCH_BSKY_APP_PASSWORD": "test-pass",
        "LABELWATCH_BSKY_DRY_RUN": "true",
    }
    with patch.dict("os.environ", env, clear=False):
        cfg = BlueskyConfig.from_env()
    assert cfg.handle == "labelwatch.bsky.social"
    assert cfg.app_password == "test-pass"
    assert cfg.dry_run is True


def test_config_from_env_no_dry_run():
    env = {
        "LABELWATCH_BSKY_HANDLE": "test.bsky.social",
        "LABELWATCH_BSKY_APP_PASSWORD": "pass",
    }
    with patch.dict("os.environ", env, clear=False):
        cfg = BlueskyConfig.from_env()
    assert cfg.dry_run is False


def test_client_not_created_on_dry_run():
    """Dry run should never call login."""
    cfg = BlueskyConfig(handle="test.bsky.social", app_password="x", dry_run=True)
    pub = BlueskyPublisher(cfg)
    pub.post_text("test")
    # _client should still be None — never touched
    assert pub._client is None


def test_post_text_calls_send_post():
    """Non-dry-run should call client.send_post."""
    cfg = BlueskyConfig(handle="test.bsky.social", app_password="x", dry_run=False)
    mock_client = MagicMock()
    mock_client.send_post.return_value = MagicMock(uri="at://...", cid="baf...")
    pub = BlueskyPublisher(cfg, client=mock_client)
    pub._logged_in = True  # skip login
    pub.post_text("hello")
    mock_client.send_post.assert_called_once_with(text="hello")
