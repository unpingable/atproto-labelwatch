#!/usr/bin/env python3
import hashlib
import json
from pathlib import Path
import sys

manifest_path = Path(sys.argv[1] if len(sys.argv) > 1 else "src/labelwatch/_instruments/neutral-instruments.manifest.json")
manifest = json.loads(manifest_path.read_text())
root = manifest_path.parent
for relative, expected in manifest["files"].items():
    actual = hashlib.sha256((root / relative).read_bytes()).hexdigest()
    if actual != expected:
        raise SystemExit(f"snapshot drift: {relative}: expected {expected}, got {actual}")
print(f"neutral-instruments {manifest['version']} ({manifest['source_commit']}): verified {len(manifest['files'])} files")
