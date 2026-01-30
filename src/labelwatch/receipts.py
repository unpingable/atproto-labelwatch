from __future__ import annotations

from typing import Any, Dict, List

from .utils import hash_sha256, stable_json


def config_hash(config_dict: Dict[str, Any]) -> str:
    return hash_sha256(stable_json(config_dict))


def receipt_hash(rule_id: str, labeler_did: str, ts: str, inputs: Dict[str, Any], evidence_hashes: List[str], cfg_hash: str) -> str:
    payload = {
        "rule_id": rule_id,
        "labeler_did": labeler_did,
        "ts": ts,
        "inputs": inputs,
        "evidence_hashes": evidence_hashes,
        "config_hash": cfg_hash,
    }
    return hash_sha256(stable_json(payload))
