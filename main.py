#!/usr/bin/env python3
"""
Lark webhook + Grafana session login (credentials from .env).
Run: python main.py  → listens on 0.0.0.0:5002, webhook: POST /webhook/event
Public URL example: http://47.84.112.211:5002/webhook/event
"""

import logging
import os
import time
from typing import Any, Dict, Generator, List, Optional, Tuple

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
GRAFANA_DASHBOARD_UID = os.getenv(
    "GRAFANA_DASHBOARD_UID", "281e8816-ccb0-4335-922b-6b248491fd28"
)
GRAFANA_PANEL_TITLE = os.getenv("GRAFANA_PANEL_TITLE", "请求总数/1m")
# Browser URL time range: last 10 minutes (matches “latest 10 mins”). Override e.g. now-1m if you want.
GRAFANA_DASHBOARD_FROM = os.getenv("GRAFANA_DASHBOARD_FROM", "now-10m")
GRAFANA_DASHBOARD_TO = os.getenv("GRAFANA_DASHBOARD_TO", "now")
# Prometheus query_range step (seconds); 60 → up to 10 buckets in 10m
GRAFANA_QUERY_STEP = int(os.getenv("GRAFANA_QUERY_STEP", "60"))
GRAFANA_QUERY_LOOKBACK_SECONDS = int(os.getenv("GRAFANA_QUERY_LOOKBACK_SECONDS", "600"))
GRAFANA_USER = os.getenv("GRAFANA_USER") or os.getenv("GRAFANA_ID") or os.getenv("grafanaid")
GRAFANA_PASSWORD = os.getenv("GRAFANA_PASSWORD") or os.getenv("grafanapassword")
VERIFICATION_TOKEN = os.getenv("VERIFICATION_TOKEN", "")
# For Open API (e.g. send message) — see https://open.feishu.cn/document/ukTMukTMukTM/ukDNz4SO0MjL5QzM/auth-v3/auth/tenant_access_token_internal
APP_ID = os.getenv("APP_ID")
APP_SECRET = os.getenv("APP_SECRET")


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
        "from": GRAFANA_DASHBOARD_FROM,
        "to": GRAFANA_DASHBOARD_TO,
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


def _walk_panels(panels: Optional[List[Dict[str, Any]]]) -> Generator[Dict[str, Any], None, None]:
    for p in panels or []:
        yield p
        if p.get("type") == "row" and p.get("panels"):
            yield from _walk_panels(p["panels"])


def _datasource_uid(ds: Any) -> Optional[str]:
    if isinstance(ds, dict):
        uid = ds.get("uid")
        if uid:
            return str(uid)
    return None


def _find_panel(dashboard: Dict[str, Any], title: str) -> Optional[Dict[str, Any]]:
    for p in _walk_panels(dashboard.get("panels")):
        if (p.get("title") or "").strip() == title.strip():
            return p
    return None


def _fetch_dashboard_model(session: requests.Session, uid: str) -> Dict[str, Any]:
    r = session.get(
        f"{GRAFANA_BASE_URL}/api/dashboards/uid/{uid}",
        params={"orgId": "1"},
        timeout=60,
    )
    r.raise_for_status()
    return r.json().get("dashboard") or {}


def _prometheus_query_range(
    session: requests.Session,
    datasource_uid: str,
    expr: str,
    start_unix: int,
    end_unix: int,
    step: int,
) -> Dict[str, Any]:
    base = f"{GRAFANA_BASE_URL}/api/datasources/proxy/uid/{datasource_uid}/api/v1/query_range"
    params = {
        "query": expr,
        "start": str(start_unix),
        "end": str(end_unix),
        "step": str(step),
    }
    r = session.get(base, params=params, timeout=120)
    r.raise_for_status()
    return r.json()


def fetch_request_total_1m_series() -> Dict[str, Any]:
    """
    Same data as Grafana panel「请求总数/1m」: last GRAFANA_QUERY_LOOKBACK_SECONDS, step GRAFANA_QUERY_STEP.
    Uses dashboard JSON + Prometheus query_range via Grafana proxy (not HTML scraping).
    """
    end = int(time.time())
    start = end - GRAFANA_QUERY_LOOKBACK_SECONDS
    session = grafana_login_session()
    dash = _fetch_dashboard_model(session, GRAFANA_DASHBOARD_UID)
    panel = _find_panel(dash, GRAFANA_PANEL_TITLE)
    if not panel:
        raise ValueError(f'Panel titled "{GRAFANA_PANEL_TITLE}" not found on dashboard {GRAFANA_DASHBOARD_UID}')

    panel_ds = _datasource_uid(panel.get("datasource"))
    series_out: List[Dict[str, Any]] = []
    for t in panel.get("targets") or []:
        expr = (t.get("expr") or "").strip()
        if not expr:
            continue
        ds_uid = _datasource_uid(t.get("datasource")) or panel_ds
        if not ds_uid:
            logger.warning("skip target without datasource uid: %s", t.get("refId"))
            continue
        raw = _prometheus_query_range(session, ds_uid, expr, start, end, GRAFANA_QUERY_STEP)
        series_out.append(
            {
                "refId": t.get("refId"),
                "legendFormat": t.get("legendFormat"),
                "expr": expr,
                "datasourceUid": ds_uid,
                "prometheus": raw,
            }
        )

    if not series_out:
        raise ValueError("No Prometheus expr targets on panel (check panel JSON / datasource)")

    return {
        "panelTitle": GRAFANA_PANEL_TITLE,
        "dashboardUid": GRAFANA_DASHBOARD_UID,
        "window": {"startUnix": start, "endUnix": end, "stepSeconds": GRAFANA_QUERY_STEP},
        "series": series_out,
    }


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


@app.route("/metrics/request-total-1m", methods=["GET"])
def metrics_request_total_1m():
    """Last 10 minutes of「请求总数/1m」panel (1-minute step). Poll every 1m from cron or Lark."""
    try:
        data = fetch_request_total_1m_series()
        return jsonify(data)
    except Exception as e:
        logger.exception("request-total-1m failed")
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    port = int(os.getenv("PORT", "5002"))
    app.run(host="0.0.0.0", port=port, debug=os.getenv("FLASK_DEBUG") == "1")
