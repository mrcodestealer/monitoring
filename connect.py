#!/usr/bin/env python3
"""
Start the monitoring bot.

**Default (``LARK_EVENT_MODE=ws``)** — Feishu/Lark **long connection** (official ``lark_oapi.ws.Client``):
no public Request URL, no ~3s HTTP URL verification. Optionally runs HTTP in a **background thread**
(``ENABLE_HTTP=1``) for ``/health``, ``/grafana/ping``, ``/webhook/event`` (legacy).

**HTTP-only (``LARK_EVENT_MODE=http``)** — same as before: only Flask/Waitress on ``PORT`` (default 5002).

Env::

  LARK_EVENT_MODE=ws|http     # default ws
  ENABLE_HTTP=0|1           # default 1 when mode=ws (sidecar); ignored when mode=http
  PORT=5002
  LARK_WS_LOG_LEVEL=INFO    # DEBUG|INFO|WARNING|ERROR for SDK WS logs

Usage::

  cd "/path/to/monitoring bot"
  python connect.py

Install::

  pip install -U -r requirements.txt
"""

import os
import threading
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent / ".env")


def main() -> None:
    port = int(os.getenv("PORT", "5002"))
    mode = (os.getenv("LARK_EVENT_MODE") or "ws").strip().lower()
    # Import after load_dotenv so main.py sees the same env as this process.
    from main import app, logger, start_lark_ws_client_blocking  # noqa: WPS433

    def run_http() -> None:
        try:
            from waitress import serve

            logger.info(
                "HTTP sidecar (Waitress) on 0.0.0.0:%s threads=8 — /health /grafana/ping /webhook/event",
                port,
            )
            serve(app, host="0.0.0.0", port=port, threads=8, channel_timeout=120)
        except ImportError:
            logger.warning("waitress not installed — pip install waitress; using Flask dev server")
            app.run(host="0.0.0.0", port=port, threaded=True, use_reloader=False)

    if mode == "http":
        logger.info("LARK_EVENT_MODE=http — Feishu events via POST /webhook/event only")
        run_http()
        return

    if mode != "ws":
        raise SystemExit(f"Unknown LARK_EVENT_MODE={mode!r} (use ws or http)")

    http_on = os.getenv("ENABLE_HTTP", "1").strip().lower() in ("1", "true", "yes", "on")
    if http_on:
        threading.Thread(target=run_http, name="http-sidecar", daemon=True).start()
    else:
        logger.info("ENABLE_HTTP=0 — only Lark WebSocket client (no HTTP listener)")

    start_lark_ws_client_blocking()


if __name__ == "__main__":
    main()
