import hashlib
import json
import struct
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CARD = ROOT / "src" / "labelwatch" / "_instruments" / "social-card-v1.png"
ARTWORK = ROOT / "assets" / "social-card-artwork.png"
PROVENANCE = ROOT / "assets" / "social-card.provenance.json"


def test_social_card_dimensions_and_provenance():
    card = CARD.read_bytes()
    assert card.startswith(b"\x89PNG\r\n\x1a\n")
    assert struct.unpack(">II", card[16:24]) == (1200, 630)
    provenance = json.loads(PROVENANCE.read_text())
    assert provenance["final_sha256"] == hashlib.sha256(card).hexdigest()
    assert provenance["artwork_sha256"] == hashlib.sha256(ARTWORK.read_bytes()).hexdigest()
    assert provenance["perceptual_review"]["result"] == "pass"


def test_card_copy_is_static_and_plain_language():
    source = (ROOT / "assets" / "social-card.src.html").read_text()
    assert "ATProto Observatory" in source
    assert "What labels have been attached here?" in source
    assert "Observe testimony." in source
    assert "Don’t confuse it with truth." in source
    assert "did:" not in source
    assert "2026-" not in source
