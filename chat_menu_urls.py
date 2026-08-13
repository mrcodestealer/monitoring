#!/usr/bin/env python3
"""
Print the signed group-menu URLs (see ``/menu/coremetrics`` and ``/menu/admin`` in ``main.py``).

    python3 chat_menu_urls.py                                     # list every group + its links
    python3 chat_menu_urls.py oc_51b6fbf2636525acfb4ead3afa3c93ce  # one group

Each group needs a link carrying **its own** ``chat_id``: a menu tap tells the server nothing
about where it happened, so the link alone decides which group the graph is posted to.

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
    if len(argv) > 2 or (len(argv) == 2 and not argv[1].startswith("oc_")):
        print((__doc__ or "").strip())
        return 2
    secret = _cfg("CHAT_MENU_TRIGGER_SECRET")
    base = _cfg("CHAT_MENU_PUBLIC_BASE_URL").rstrip("/")
    if not secret:
        print("CHAT_MENU_TRIGGER_SECRET is empty — set it in main.py ``_CFG`` or the environment")
        return 1
    if not base.startswith("http"):
        print("CHAT_MENU_PUBLIC_BASE_URL is not a http(s) base URL")
        return 1

    if len(argv) == 1:
        q = urlencode({"op": "groups", "sig": sign("groups", "all", secret)})
        print("every group the bot is in, with each group's own menu link + sync command:")
        print(f"  curl -sS 'http://127.0.0.1:5002/menu/admin?{q}'")
        print("\n(run it on the server — the box can't reach its own public IP)")
        return 0

    chat_id = argv[1]
    print("menu links for this group (paste into the group's own menu — each posts here):")
    for slug in ("coremetrics", "freespin"):
        q = urlencode({"chat": chat_id, "sig": sign(slug, chat_id, secret)})
        print(f"  {slug:12s} {base}/menu/{slug}?{q}")
    print()
    admin_sig = sign("admin", chat_id, secret)
    for op in ("sync", "list", "install", "delete"):
        q = urlencode({"op": op, "chat": chat_id, "sig": admin_sig})
        print(f"{op}:\n  curl -sS '{base}/menu/admin?{q}'")
    q = urlencode({"chat": chat_id, "sig": admin_sig})
    print("\npanel (no-webview card button — post it, then pin it in the group):")
    print(f"  curl -sS '{base}/menu/panel?{q}'")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
