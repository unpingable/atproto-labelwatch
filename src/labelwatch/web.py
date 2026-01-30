from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Optional

from . import db


def serve(db_path: str, host: str = "127.0.0.1", port: int = 8000, limit: int = 20) -> None:
    conn = db.connect(db_path)
    db.init_db(conn)

    class Handler(BaseHTTPRequestHandler):
        def _send_json(self, payload, status=200):
            data = json.dumps(payload).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self):  # noqa: N802 - stdlib signature
            if self.path == "/health":
                self._send_json({"ok": True})
                return
            if self.path.startswith("/recent-alerts"):
                rows = conn.execute(
                    "SELECT * FROM alerts ORDER BY ts DESC LIMIT ?",
                    (limit,),
                ).fetchall()
                self._send_json([dict(r) for r in rows])
                return
            self._send_json({"error": "not found"}, status=404)

    server = HTTPServer((host, port), Handler)
    server.serve_forever()
