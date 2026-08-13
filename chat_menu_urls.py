#!/usr/bin/env python3
"""
Print the signed group-menu URLs for one ``chat_id`` (see ``/menu/coremetrics`` in ``main.py``).

    python3 chat_menu_urls.py oc_51b6fbf2636525acfb4ead3afa3c93ce

``CHAT_MENU_TRIGGER_SECRET`` / ``CHAT_MENU_PUBLIC_BASE_URL`` come from the environment, else from
the ``_CFG`` defaults in ``main.py`` — read by regex, not import, so this stays runnable on a
laptop without the bot's dependencies (flask / lark_oapi / playwright).
"""
from __future__ import annotations

import hashlib
import hmac
import os
import re
import sys
from pathlib import Path
from urllib.parse import urlencode


def _cfg(key: str, default: str = "") -> str:
    v = os.environ.get(key, "").strip()
    if v:
        return v
    try:
        text = Path(__file__).with_name("main.py").read_text(encoding="utf-8", errors="replace")
    except OSError:
        return default
    m = re.search(rf'^\s*"{re.escape(key)}"\s*:\s*"([^"]*)"', text, re.MULTILINE)
    return m.group(1) if m else default


def sign(op: str, chat_id: str, secret: str) -> str:
    """Must stay identical to ``_chat_menu_sign`` in ``main.py``."""
    return hmac.new(
        secret.encode("utf-8"), f"{op}:{chat_id}".encode("utf-8"), hashlib.sha256
    ).hexdigest()[:32]


def main(argv: list[str]) -> int:
    if len(argv) != 2 or not argv[1].startswith("oc_"):
        print((__doc__ or "").strip())
        return 2
    chat_id = argv[1]
    secret = _cfg("CHAT_MENU_TRIGGER_SECRET")
    base = _cfg("CHAT_MENU_PUBLIC_BASE_URL").rstrip("/")
    if not secret:
        print("CHAT_MENU_TRIGGER_SECRET is empty — set it in main.py ``_CFG`` or the environment")
        return 1
    if not base.startswith("http"):
        print("CHAT_MENU_PUBLIC_BASE_URL is not a http(s) base URL")
        return 1

    trigger_q = urlencode({"chat": chat_id, "sig": sign("coremetrics", chat_id, secret)})
    print("menu link (what the group menu opens — clicking it posts the graph):")
    print(f"  {base}/menu/coremetrics?{trigger_q}\n")
    admin_sig = sign("admin", chat_id, secret)
    for op in ("install", "list", "delete"):
        q = urlencode({"op": op, "chat": chat_id, "sig": admin_sig})
        print(f"{op}:\n  curl -sS '{base}/menu/admin?{q}'")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
