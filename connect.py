#!/usr/bin/env python3
"""
Start the monitoring bot on PORT (default 5002).

Why use this entry point
------------------------
Feishu/Lark requires the event webhook to return HTTP **200 within ~3 seconds**.
``/webhook/event`` ACKs immediately; **Grafana + Lark send** run in a **background thread**
(see ``main._monitoring_background_worker``).

Usage::

  cd "/path/to/monitoring bot"
  python connect.py

  PORT=5002 python connect.py

Same as ``python main.py`` for the server process; ``connect.py`` documents the fast-ACK
pattern and loads ``.env`` before importing ``main``.

Install deps (includes Feishu **lark-oapi** SDK)::

  pip install -U -r requirements.txt
"""

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent / ".env")


def main() -> None:
    port = int(os.getenv("PORT", "5002"))
    # Import after load_dotenv so main.py sees the same env as this process.
    from main import app, logger  # noqa: WPS433 (import inside function)

    logger.info(
        "Monitoring bot listening on 0.0.0.0:%s — webhook returns quickly; /monitoring work is async",
        port,
    )
    app.run(
        host="0.0.0.0",
        port=port,
        threaded=True,
        use_reloader=False,
    )


if __name__ == "__main__":
    main()
