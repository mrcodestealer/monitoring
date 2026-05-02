#!/usr/bin/env python3
"""
Start the monitoring bot.

**Default (``LARK_EVENT_MODE=ws``)** — Feishu/Lark **long connection** (official ``lark_oapi.ws.Client``):
no public Request URL, no ~3s HTTP URL verification. IM events arrive **only on WebSocket DATA frames** —
journalctl will **not** show ``POST /webhook/event`` for chat messages (that path is HTTP mode only).
Optionally runs HTTP in a **background thread** (``ENABLE_HTTP=1``) for ``/health``, ``/grafana/ping``,
``/webhook/event`` (legacy / manual tests).

**HTTP-only (``LARK_EVENT_MODE=http``)** — same as before: only Flask/Waitress on ``PORT`` (default 5002).

Env::

  LARK_EVENT_MODE=ws|http     # default ws; http = Feishu POST /webhook/event only (no WebSocket)
  ENABLE_HTTP=0|1           # default 1 when mode=ws (sidecar); ignored when mode=http
  PORT=5002
  LARK_WEBHOOK_PUBLIC_URL=    # optional; http mode logs this as the Feishu Request URL hint
  WAITRESS_THREADS=24        # avoid Lark 3s timeout when all workers busy (HTTP mode)
  LARK_WS_LOG_LEVEL=INFO    # DEBUG|INFO|WARNING|ERROR for SDK WS logs
  LARK_WS_USE_HTTP_KEYS=0   # 1=把 LARK_ENCRYPT_KEY/VERIFICATION_TOKEN 传给 WS handler（一般勿开；长连接文档要求空）
  LARK_WS_EXTRA_IM_TYPES=   # 逗号分隔的额外 event_type（如控制台实际推送非标准名）；与 receive_v1 同形时挂 customized handler
  LARK_WS_TRANSPORT_LOG=1   # 0=关闭；默认 1 在每条 WS DATA 帧打 header.type（event/card）与长度
  LARK_WS_BOOTSTRAP_FRAMES=16  # 启动后前 N 条下行 protobuf 帧打 INFO（CONTROL/DATA）；0=关闭
  LARK_WS_LOG_FRAME_METHOD=0   # 1=每条帧都打 INFO（比 bootstrap 更啰嗦）
  LARK_WS_SDK_DEBUG=0       # 1=Lark SDK 内部 DEBUG（含 payload 片段）

Usage::

  cd "/path/to/monitoring bot"
  python connect.py

Install::

  pip install -U -r requirements.txt
"""

import os
import threading
import time
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent / ".env")


def main() -> None:
    port = int(os.getenv("PORT", "5002"))
    raw_mode = (os.getenv("LARK_EVENT_MODE") or "ws").strip().lower()
    # ``Environment=LARK_EVENT_MODE=`` in systemd → empty string → treat as default ws.
    mode = raw_mode if raw_mode else "ws"
    # Import after load_dotenv so module-level os.getenv() in main.py matches this process.
    from main import app, logger, start_lark_ws_client_blocking  # noqa: WPS433

    def run_http() -> None:
        try:
            from waitress import serve

            try:
                threads = int(os.getenv("WAITRESS_THREADS", "24"))
            except ValueError:
                threads = 24
            threads = max(4, min(threads, 128))
            logger.info(
                "HTTP (Waitress) on 0.0.0.0:%s threads=%s — /health /grafana/ping /webhook/event "
                "(raise WAITRESS_THREADS if webhooks queue behind slow requests)",
                port,
                threads,
            )
            serve(app, host="0.0.0.0", port=port, threads=threads, channel_timeout=120)
        except ImportError:
            logger.warning("waitress not installed — pip install waitress; using Flask dev server")
            app.run(host="0.0.0.0", port=port, threaded=True, use_reloader=False)

    if mode == "http":
        logger.info("LARK_EVENT_MODE=http — Feishu events via POST /webhook/event only")
        hint = (os.getenv("LARK_WEBHOOK_PUBLIC_URL") or "").strip()
        if hint:
            logger.info("Feishu developer console → 事件与回调 → Request URL (示例配置): %s", hint)
        else:
            logger.info(
                "Set LARK_WEBHOOK_PUBLIC_URL in .env to log your Feishu Request URL hint "
                "(e.g. http://47.84.112.211:5002/webhook/event)."
            )
        run_http()
        return

    if mode != "ws":
        raise SystemExit(f"Unknown LARK_EVENT_MODE={mode!r} (use ws or http)")

    http_on = os.getenv("ENABLE_HTTP", "1").strip().lower() in ("1", "true", "yes", "on")
    http_thread: Optional[threading.Thread] = None
    if http_on:
        # Non-daemon so if WebSocket start fails we can ``join()`` and keep Waitress alive for systemd.
        http_thread = threading.Thread(target=run_http, name="http-sidecar", daemon=False)
        http_thread.start()
        time.sleep(0.2)
    else:
        logger.info("ENABLE_HTTP=0 — only Lark WebSocket client (no HTTP listener)")

    try:
        start_lark_ws_client_blocking()
    except Exception as e:
        logger.exception(
            "Lark WebSocket client failed to start or exited (check APP_ID/APP_SECRET/LARK_HOST, "
            "egress firewall, and Feishu app long-connection mode)."
        )
        if http_on and http_thread is not None:
            logger.warning(
                "Continuing with HTTP sidecar only — use POST /webhook/event for events, "
                "or set LARK_EVENT_MODE=http after fixing credentials."
            )
            http_thread.join()
            return
        raise SystemExit(1) from e


if __name__ == "__main__":
    main()
