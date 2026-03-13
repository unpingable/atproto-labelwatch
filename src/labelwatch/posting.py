"""Bluesky posting for the labelwatch account.

Thin wrapper around the ATProto Python SDK. Keep this module small and
boring so auth or posting shape can change later without dragging
labelwatch internals through it.

Usage:
    from labelwatch.posting import BlueskyConfig, BlueskyPublisher, FindingPost

    cfg = BlueskyConfig.from_env()
    publisher = BlueskyPublisher(cfg)
    result = publisher.post_text("Hello from labelwatch")
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from atproto import Client, models

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class BlueskyConfig:
    handle: str
    app_password: str
    dry_run: bool = False

    @classmethod
    def from_env(cls) -> BlueskyConfig:
        return cls(
            handle=os.environ["LABELWATCH_BSKY_HANDLE"],
            app_password=os.environ["LABELWATCH_BSKY_APP_PASSWORD"],
            dry_run=os.environ.get("LABELWATCH_BSKY_DRY_RUN", "").lower()
            in {"1", "true", "yes", "on"},
        )


@dataclass(frozen=True)
class LinkCard:
    url: str
    title: str
    description: str = ""
    thumb_path: Optional[Path] = None


@dataclass(frozen=True)
class FindingPost:
    headline: str
    summary: str
    detail_url: str
    card_title: str
    card_description: str
    dedupe_key: Optional[str] = None
    thumb_path: Optional[Path] = None

    def render_text(self) -> str:
        parts = [self.headline.strip()]
        if self.summary.strip():
            parts.append(self.summary.strip())
        parts.append(self.detail_url)
        return "\n\n".join(parts)

    def to_link_card(self) -> LinkCard:
        return LinkCard(
            url=self.detail_url,
            title=self.card_title,
            description=self.card_description,
            thumb_path=self.thumb_path,
        )


class BlueskyPublisher:
    """One-way publisher to Bluesky via app password."""

    def __init__(
        self,
        config: BlueskyConfig,
        client: Optional[Client] = None,
    ) -> None:
        self._config = config
        self._client = client
        self._logged_in = False

    @property
    def client(self) -> Client:
        if self._client is None:
            self._client = Client()
        if not self._logged_in and not self._config.dry_run:
            self._client.login(self._config.handle, self._config.app_password)
            self._logged_in = True
        return self._client

    def post_text(self, text: str) -> Any:
        if self._config.dry_run:
            log.info("dry-run post_text: %r", text)
            return {"dry_run": True, "text": text}
        return self.client.send_post(text=text)

    def post_link_card(self, text: str, card: LinkCard) -> Any:
        embed = self._build_external_embed(card)
        if self._config.dry_run:
            payload = {
                "dry_run": True,
                "text": text,
                "card": {
                    "url": card.url,
                    "title": card.title,
                    "description": card.description,
                },
            }
            log.info("dry-run post_link_card: %s", payload)
            return payload
        return self.client.send_post(text=text, embed=embed)

    def post_finding(self, finding: FindingPost) -> Any:
        return self.post_link_card(
            text=finding.render_text(),
            card=finding.to_link_card(),
        )

    def _build_external_embed(
        self, card: LinkCard
    ) -> models.AppBskyEmbedExternal.Main:
        external_kwargs: dict[str, Any] = {
            "uri": card.url,
            "title": card.title,
            "description": card.description,
        }
        if card.thumb_path is not None:
            external_kwargs["thumb"] = self._upload_blob(card.thumb_path)
        external = models.AppBskyEmbedExternal.External(**external_kwargs)
        return models.AppBskyEmbedExternal.Main(external=external)

    def _upload_blob(self, path: Path) -> Any:
        blob = self.client.upload_blob(path.read_bytes())
        return blob.blob
