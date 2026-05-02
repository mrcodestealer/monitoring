#!/usr/bin/env python3
"""
Lark webhook + Grafana session login (credentials from .env).
Run: python main.py  → listens on 0.0.0.0:5002, webhook: POST /webhook/event
Public URL example: http://47.84.112.211:5002/webhook/event
"""

import logging
import os
from typing import Any, Dict, Optional, Tuple

import requests
from dotenv import load_dotenv
from flask import Flask, jsonify, request

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

GRAFANA_BASE_URL = os.getenv("GRAFANA_BASE_URL", "https://grafana.client8.me").rstrip("/")
GRAFANA_DASHBOARD_PATH = os.getenv(
    "GRAFANA_DASHBOARD_PATH",
    "/d/281e8816-ccb0-4335-922b-6b248491fd28/core-metrics-arms-aliyun",
)
GRAFANA_USER = os.getenv("GRAFANA_USER") or os.getenv("GRAFANA_ID") or os.getenv("grafanaid")
GRAFANA_PASSWORD = os.getenv("GRAFANA_PASSWORD") or os.getenv("grafanapassword")
VERIFICATION_TOKEN = os.getenv("VERIFICATION_TOKEN", "")


def grafana_login_session() -> requests.Session:
    if not GRAFANA_USER or not GRAFANA_PASSWORD:
        raise ValueError("Set GRAFANA_USER and GRAFANA_PASSWORD in .env")

    session = requests.Session()
    login_url = f"{GRAFANA_BASE_URL}/login"
    resp = session.post(
        login_url,
        json={"user": GRAFANA_USER, "password": GRAFANA_PASSWORD},
        headers={"Content-Type": "application/json"},
        timeout=30,
    )
    resp.raise_for_status()
    # Grafana sets grafana_session cookie on success
    if "grafana_session" not in session.cookies.get_dict():
        logger.warning("Login returned 200 but no grafana_session cookie; check credentials / SSO")
    return session


def fetch_grafana_dashboard(
    session: Optional[requests.Session] = None,
    extra_query: Optional[Dict[str, str]] = None,
) -> requests.Response:
    """GET dashboard HTML after login (same as opening the link in a browser)."""
    if session is None:
        session = grafana_login_session()
    params = {
        "orgId": "1",
        "from": "now-15m",
        "to": "now",
        "timezone": "browser",
        "refresh": "5s",
    }
    if extra_query:
        params.update(extra_query)
    url = f"{GRAFANA_BASE_URL}{GRAFANA_DASHBOARD_PATH}"
    resp = session.get(url, params=params, timeout=60)
    return resp


def _extract_url_verification(data: Dict[str, Any]) -> Optional[Tuple[str, str]]:
    """Return (token, challenge) if this is a Lark URL verification payload."""
    if not isinstance(data, dict):
        return None
    # Event subscription 2.0
    if data.get("header", {}).get("event_type") == "url_verification":
        ev = data.get("event") or {}
        ch = ev.get("challenge")
        tok = ev.get("token")
        if ch is not None:
            return (str(tok or ""), str(ch))
    # Legacy / flat
    if data.get("type") == "url_verification":
        return (str(data.get("token") or ""), str(data.get("challenge") or ""))
    return None


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"ok": True})


@app.route("/webhook/event", methods=["POST"])
def webhook_event():
    data = request.get_json(silent=True)
    if not data:
        return jsonify({"error": "invalid json"}), 400

    uv = _extract_url_verification(data)
    if uv:
        token, challenge = uv
        if VERIFICATION_TOKEN and token != VERIFICATION_TOKEN:
            logger.warning("url_verification token mismatch")
            return jsonify({"error": "invalid verification token"}), 403
        return jsonify({"challenge": challenge})

    # Other Lark events: extend here (e.g. message, card actions)
    logger.info("event received: %s", data.get("header", data) if isinstance(data, dict) else data)
    return jsonify({})


@app.route("/grafana/ping", methods=["GET"])
def grafana_ping():
    """Optional: verify .env login and that the dashboard URL is reachable."""
    try:
        r = fetch_grafana_dashboard()
        return jsonify(
            {
                "status_code": r.status_code,
                "final_url": r.url,
                "bytes": len(r.content),
            }
        )
    except Exception as e:
        logger.exception("grafana ping failed")
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    port = int(os.getenv("PORT", "5002"))
    app.run(host="0.0.0.0", port=port, debug=os.getenv("FLASK_DEBUG") == "1")
