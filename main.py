#!/usr/bin/env python3
"""
Lark webhook + Grafana session login (credentials from .env).
Run: python main.py  → listens on 0.0.0.0:5002, webhook: POST /webhook/event
Public URL example: http://47.84.112.211:5002/webhook/event
群/at 机器人发 /monitoring → 回复「请求总数/1m」近 10 分钟摘要。
"""

import base64
import hashlib
import json
import logging
import os
import re
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
VERIFICATION_TOKEN = (os.getenv("VERIFICATION_TOKEN") or "").strip()
# For Open API (e.g. send message) — see https://open.feishu.cn/document/ukTMukTMukTM/ukDNz4SO0MjL5QzM/auth-v3/auth/tenant_access_token_internal
APP_ID = os.getenv("APP_ID")
APP_SECRET = os.getenv("APP_SECRET")
LARK_HOST = os.getenv("LARK_HOST", "https://open.larksuite.com").rstrip("/")
MONITORING_TRIGGER = os.getenv("MONITORING_TRIGGER", "/monitoring")
LARK_ENCRYPT_KEY = (
    os.getenv("LARK_ENCRYPT_KEY") or os.getenv("ENCRYPT_KEY") or os.getenv("FEISHU_ENCRYPT_KEY") or ""
).strip()
LARK_BOT_OPEN_ID = (os.getenv("LARK_BOT_OPEN_ID") or "").strip()


def _feishu_decrypt_encrypt_field(ciphertext_b64: str, encrypt_key: str) -> str:
    """Decrypt Lark ``encrypt`` field (AES-256-CBC + PKCS7), same as Feishu open-platform samples."""
    try:
        from Crypto.Cipher import AES
    except ImportError as e:
        raise ImportError("pip install pycryptodome") from e

    bs = AES.block_size
    key = hashlib.sha256(encrypt_key.encode("utf-8")).digest()
    enc = base64.b64decode(ciphertext_b64)
    iv = enc[:bs]
    cipher = AES.new(key, AES.MODE_CBC, iv)
    raw = cipher.decrypt(enc[bs:])
    pad_len = raw[-1]
    if pad_len < 1 or pad_len > bs:
        raise ValueError("invalid PKCS7 padding")
    raw = raw[:-pad_len]
    return raw.decode("utf-8")


def _feishu_maybe_decrypt_webhook_payload(raw: Any) -> Any:
    """
    When 开发者后台 → 事件与回调 enables Encrypt Key, POST body is only ``{"encrypt":"..."}``.
    Set LARK_ENCRYPT_KEY to the same key (or turn encryption off in console).
    """
    if not isinstance(raw, dict) or "encrypt" not in raw:
        return raw
    if not LARK_ENCRYPT_KEY:
        logger.warning(
            "Lark POST has `encrypt` but LARK_ENCRYPT_KEY is unset — "
            "set it or disable encryption in 事件与回调; events will be ignored."
        )
        return raw
    try:
        plain = _feishu_decrypt_encrypt_field(str(raw["encrypt"]), LARK_ENCRYPT_KEY)
        if plain.startswith("\ufeff"):
            plain = plain.lstrip("\ufeff")
        return json.loads(plain)
    except ImportError as e:
        logger.error("%s — encrypted webhooks need pycryptodome.", e)
        return raw
    except Exception as e:
        logger.exception("Lark decrypt failed: %s", e)
        return raw


def _lark_legacy_event_callback_message_to_v2(data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Old ``type: event_callback`` + ``event.type: message`` → schema-2-like dict for one code path."""
    if data.get("type") != "event_callback":
        return None
    ev = data.get("event")
    if not isinstance(ev, dict) or ev.get("type") != "message":
        return None
    token = str(data.get("token") or (data.get("header") or {}).get("token") or "")
    chat_id = ev.get("open_chat_id") or ev.get("chat_id") or ""
    text_raw = ev.get("text_without_at_bot") or ev.get("text") or ""
    if not text_raw and ev.get("content"):
        try:
            c = json.loads(ev["content"])
            text_raw = c.get("text") or ""
        except (json.JSONDecodeError, TypeError):
            text_raw = ""
    msg_type = (ev.get("msg_type") or "text").lower()
    return {
        "schema": "2.0",
        "header": {"event_type": "im.message.receive_v1", "token": token},
        "event": {
            "message": {
                "chat_id": chat_id,
                "chat_type": ev.get("chat_type") or "group",
                "message_type": "text" if msg_type == "text" else msg_type,
                "content": json.dumps({"text": text_raw}),
                "mentions": ev.get("mentions") or [],
            },
            "sender": {"sender_id": {"open_id": ev.get("open_id") or ""}},
        },
    }


def _lark_normalize_webhook(data: Dict[str, Any]) -> Dict[str, Any]:
    legacy = _lark_legacy_event_callback_message_to_v2(data)
    return legacy if legacy else data


def _lark_coerce_event_json_string(data: Dict[str, Any]) -> Dict[str, Any]:
    """Some gateways pass ``event`` as a JSON string instead of an object."""
    ev = data.get("event")
    if isinstance(ev, str):
        try:
            parsed = json.loads(ev)
            data["event"] = parsed if isinstance(parsed, dict) else {}
        except (json.JSONDecodeError, TypeError):
            data["event"] = {}
    return data


def _lark_header_event_type(data: Dict[str, Any]) -> str:
    """``header.event_type`` or top-level ``event_type`` (proxies sometimes flatten the body)."""
    h = data.get("header")
    if isinstance(h, dict):
        et = h.get("event_type")
        if et is not None:
            return str(et).strip()
    et2 = data.get("event_type")
    if et2 is not None:
        return str(et2).strip()
    return ""


def _lark_collect_post_text(obj: Any, out: List[str]) -> None:
    """Depth-first collect human text from rich post / mixed blocks."""
    if isinstance(obj, dict):
        tag = obj.get("tag")
        if tag == "text" and "text" in obj:
            t = obj.get("text")
            if t is not None:
                out.append(str(t))
        elif tag in ("a", "code") and "text" in obj:
            t = obj.get("text")
            if t is not None:
                out.append(str(t))
        for v in obj.values():
            _lark_collect_post_text(v, out)
    elif isinstance(obj, list):
        for x in obj:
            _lark_collect_post_text(x, out)


def _lark_extract_plain_text_from_message(msg: Dict[str, Any]) -> str:
    """Support ``text`` and rich ``post`` bodies (common when @mentioning in mobile clients)."""
    content_str = msg.get("content") or "{}"
    mtype = (msg.get("message_type") or "").lower()
    try:
        obj = json.loads(content_str)
    except (json.JSONDecodeError, TypeError):
        return ""

    if not mtype:
        if "text" in obj and isinstance(obj.get("text"), str):
            mtype = "text"
        elif any(k in obj for k in ("zh_cn", "en_us", "ja_jp")) or isinstance(obj.get("content"), list):
            mtype = "post"

    if mtype == "text":
        return obj.get("text") or ""

    if mtype == "post":
        for locale_key in ("zh_cn", "en_us", "ja_jp"):
            block = obj.get(locale_key)
            if not isinstance(block, dict):
                continue
            parts: List[str] = []
            for row in block.get("content") or []:
                if isinstance(row, list):
                    for cell in row:
                        if isinstance(cell, dict) and cell.get("tag") == "text":
                            parts.append(cell.get("text") or "")
                elif isinstance(row, dict) and row.get("tag") == "text":
                    parts.append(row.get("text") or "")
            if parts:
                return "".join(parts)
        parts2: List[str] = []
        _lark_collect_post_text(obj, parts2)
        if parts2:
            return "".join(parts2)
        return obj.get("text") or ""

    parts3: List[str] = []
    _lark_collect_post_text(obj, parts3)
    if parts3:
        return "".join(parts3)
    return obj.get("text") or ""


def _lark_clean_command_text(raw_text: str, mentions: Any) -> str:
    """Remove @ placeholders so ``/monitoring`` survives after <at>...</at> blocks."""
    text = raw_text or ""
    if isinstance(mentions, list):
        for m in mentions:
            if isinstance(m, dict):
                k = m.get("key")
                if k:
                    text = text.replace(str(k), "")
    text = re.sub(r"@_user_\d+", "", text)
    text = re.sub(r"<[^>]+>", "", text)
    text = re.sub(r"[\u200b\uFEFF\u00A0]", "", text)
    text = text.replace("／", "/").replace("＼", "\\")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _text_has_monitoring_trigger(raw_text: str, clean: str) -> bool:
    tri = MONITORING_TRIGGER
    raw = raw or ""
    clean = clean or ""
    if tri in raw or tri in clean:
        return True
    tl = tri.lower()
    return tl in clean.lower() or tl in raw.lower()


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


def _lark_tenant_access_token() -> str:
    if not APP_ID or not APP_SECRET:
        raise ValueError("APP_ID and APP_SECRET required for Lark reply")
    url = f"{LARK_HOST}/open-apis/auth/v3/tenant_access_token/internal"
    r = requests.post(url, json={"app_id": APP_ID, "app_secret": APP_SECRET}, timeout=20)
    r.raise_for_status()
    j = r.json()
    if j.get("code") != 0:
        raise RuntimeError(f"Lark token: {j}")
    return str(j["tenant_access_token"])


def _lark_send_text(receive_id_type: str, receive_id: str, text: str) -> None:
    token = _lark_tenant_access_token()
    url = f"{LARK_HOST}/open-apis/im/v1/messages"
    params = {"receive_id_type": receive_id_type}
    body = {
        "receive_id": receive_id,
        "msg_type": "text",
        "content": json.dumps({"text": text}),
    }
    r = requests.post(
        url,
        params=params,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json=body,
        timeout=30,
    )
    r.raise_for_status()
    j = r.json()
    if j.get("code") != 0:
        raise RuntimeError(f"Lark send failed: {j} (HTTP {r.status_code})")


def _format_monitoring_reply(payload: Dict[str, Any]) -> str:
    w = payload.get("window") or {}
    span = int(w.get("endUnix", 0)) - int(w.get("startUnix", 0))
    lines: List[str] = [
        f"【{payload.get('panelTitle')}】最近 {span}s（步长 {w.get('stepSeconds')}s）",
        f"Dashboard: {GRAFANA_BASE_URL}/d/{payload.get('dashboardUid')}",
    ]
    for s in payload.get("series") or []:
        prom = s.get("prometheus") or {}
        pdata = prom.get("data") or {}
        results = pdata.get("result") or []
        ref = s.get("refId") or "?"
        if not results:
            lines.append(f"- [{ref}] 无数据 / no data")
            continue
        for r in results[:12]:
            m = r.get("metric") or {}
            legend_bits = [f"{k}={v}" for k, v in sorted(m.items()) if k not in ("__name__",)][:4]
            legend = ", ".join(legend_bits) or str(m.get("__name__", ref))
            vals = r.get("values") or []
            if vals:
                last = vals[-1]
                lines.append(f"- {legend}\n  latest: {last[1]} @ {last[0]} ({len(vals)} pts)")
            else:
                lines.append(f"- {legend}\n  (empty)")
    out = "\n".join(lines)
    if len(out) > 4500:
        out = out[:4490] + "\n…(truncated)"
    return out


def _lark_header_token_ok(data: Dict[str, Any]) -> bool:
    if not VERIFICATION_TOKEN:
        return True
    h = data.get("header") or {}
    tok = h.get("token") if isinstance(h, dict) else None
    if tok is None:
        tok = h.get("Token") if isinstance(h, dict) else None
    if tok is None:
        tok = h.get("verification_token") if isinstance(h, dict) else None
    if tok is not None and str(tok).strip() == VERIFICATION_TOKEN:
        return True
    top = data.get("verification_token")
    if top is not None and str(top).strip() == VERIFICATION_TOKEN:
        return True
    top_tok = data.get("token")
    if top_tok is not None and str(top_tok).strip() == VERIFICATION_TOKEN:
        return True
    return False


def _handle_im_message_receive(data: Dict[str, Any]) -> Tuple[Any, int]:
    if not _lark_header_token_ok(data):
        logger.warning(
            "im.message token mismatch: set VERIFICATION_TOKEN to match 事件与回调 → Verification Token"
        )
        return jsonify({"error": "invalid verification token"}), 403

    event = data.get("event") or {}
    msg = event.get("message") or {}
    mtype = (msg.get("message_type") or "").lower()
    if mtype and mtype not in ("text", "post"):
        logger.info("im.message ignored: message_type=%r", mtype)
        return jsonify({}), 200

    sender = (event.get("sender") or {}).get("sender_id") or {}
    sender_open = (sender.get("open_id") or "").strip()
    if LARK_BOT_OPEN_ID and sender_open == LARK_BOT_OPEN_ID:
        return jsonify({}), 200

    raw_text = _lark_extract_plain_text_from_message(msg)
    mentions = msg.get("mentions") or []
    clean = _lark_clean_command_text(raw_text, mentions)

    if not _text_has_monitoring_trigger(raw_text, clean):
        logger.info(
            "im.message no trigger raw=%r clean=%r (need %r)",
            (raw_text or "")[:160],
            (clean or "")[:160],
            MONITORING_TRIGGER,
        )
        return jsonify({}), 200

    chat_id = (msg.get("chat_id") or msg.get("open_chat_id") or "").strip()
    open_id = sender_open

    try:
        payload = fetch_request_total_1m_series()
        reply = _format_monitoring_reply(payload)
    except Exception as e:
        logger.exception("monitoring fetch failed")
        reply = f"监控数据拉取失败：{e}"

    try:
        if chat_id:
            _lark_send_text("chat_id", chat_id, reply)
        elif open_id:
            _lark_send_text("open_id", open_id, reply)
        else:
            logger.warning("no chat_id/open_chat_id or sender open_id; cannot reply. msg keys=%s", list(msg.keys()))
    except Exception as e:
        logger.exception("lark send failed: %s", e)

    return jsonify({}), 200


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"ok": True})


@app.route("/webhook/event", methods=["POST"])
def webhook_event():
    raw = request.get_json(silent=True)
    if not raw:
        return jsonify({"error": "invalid json"}), 400

    data = _feishu_maybe_decrypt_webhook_payload(raw)
    if isinstance(raw, dict) and raw.get("encrypt") is not None and data is raw:
        logger.error(
            "Webhook still encrypted — install pycryptodome and set LARK_ENCRYPT_KEY, "
            "or disable 加密 in Lark 事件与回调"
        )
        return jsonify({}), 200

    data = _lark_normalize_webhook(data if isinstance(data, dict) else {})
    if not isinstance(data, dict):
        return jsonify({"error": "invalid payload"}), 400

    data = _lark_coerce_event_json_string(data)

    uv = _extract_url_verification(data)
    if uv:
        token, challenge = uv
        if VERIFICATION_TOKEN and str(token).strip() != VERIFICATION_TOKEN:
            logger.warning("url_verification token mismatch")
            return jsonify({"error": "invalid verification token"}), 403
        return jsonify({"challenge": challenge})

    et = _lark_header_event_type(data)
    if et in ("im.message.receive_v1", "im.message.receive_v2"):
        logger.info("handling %s", et)
        return _handle_im_message_receive(data)

    logger.info(
        "event ignored: event_type=%r keys=%s (subscribe 消息与群组 → 接收消息; ensure URL hits this service)",
        et,
        list(data.keys())[:20],
    )
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
