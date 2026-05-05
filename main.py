#!/usr/bin/env python3
"""
Lark events (WebSocket 长连接, 推荐) 或 HTTP webhook + Grafana。

启动: ``python main.py``

**配置**：编辑本文件顶部 ``_CFG`` 字典（不再读取 ``.env``）。也可用 **systemd ``Environment=KEY=value``** 覆盖同名键。

**默认 ``LARK_EVENT_MODE=ws``** — 长连接；可选 ``ENABLE_HTTP=1`` 并行 HTTP。

**``LARK_EVENT_MODE=http``** — 监听 ``PORT``（本仓库默认 **5002**，与同机运行的 ``Chatbox/main.py``（常用 **5000**）错开端口），事件走 ``POST /webhook/event``。
默认 HTTP 栈为 **Flask ``threaded=True``**（实现方式对齐 Chatbox）；生产可设 ``HTTP_SERVER=waitress``。

端口解析顺序：**环境变量 ``PORT`` → ``LARKBOT_PORT`` → ``_CFG["PORT"]``**（与 Chatbox 的 ``PORT``/``LARKBOT_PORT`` 习惯一致）。

飞书后台「事件与回调」；``APP_ID`` / ``APP_SECRET`` 必填。国际 Lark ``LARK_HOST=https://open.larksuite.com``。

群/at 机器人发 /monitoring → Grafana 摘要。HTTP 回调先返回 ``{}`` 再后台处理。
"""

import base64
import copy
import hashlib
import json
import logging
import os
import re
import threading
import time
import warnings
from typing import Any, Dict, Generator, List, Optional, Tuple

import requests
from flask import Flask, Response, g, jsonify, request

# ---------------------------------------------------------------------------
# 单一配置：只改这里（也可用 systemd Environment= 覆盖同名变量，无需 .env）
# 勿将含真实密钥的 main.py 提交到公开仓库；泄露请到飞书/Grafana 后台轮换。
# ---------------------------------------------------------------------------
_CFG: Dict[str, Any] = {
    "PORT": 5002,
    "HTTP_SERVER": "flask",
    "LARK_EVENT_MODE": "http",
    "ENABLE_HTTP": "1",
    "WAITRESS_THREADS": 24,
    "LARK_HOST": "https://open.larksuite.com",
    "LARK_WEBHOOK_PUBLIC_URL": "http://47.84.112.211:5002/webhook/event",
    "LARK_WEBHOOK_EXTRA_PATHS": "",
    "GRAFANA_BASE_URL": "https://grafana.client8.me",
    "GRAFANA_DASHBOARD_PATH": "/d/281e8816-ccb0-4335-922b-6b248491fd28/core-metrics-arms-aliyun",
    "GRAFANA_DASHBOARD_UID": "281e8816-ccb0-4335-922b-6b248491fd28",
    "GRAFANA_PANEL_TITLE": "请求总数/1m",
    "GRAFANA_DASHBOARD_FROM": "now-10m",
    "GRAFANA_DASHBOARD_TO": "now",
    "GRAFANA_QUERY_STEP": 60,
    "GRAFANA_QUERY_LOOKBACK_SECONDS": 600,
    "GRAFANA_USER": "om_duty",
    "GRAFANA_PASSWORD": "5tgb%TGB094",
    "VERIFICATION_TOKEN": "QlZMYp7rogAS914dxxMVNgboUKxQP7jc",
    "APP_ID": "cli_a97fcc6df7615ed1",
    "APP_SECRET": "NwAi6xJxMYDHMFAQcTG8ZfJxpeTOibvy",
    "MONITORING_TRIGGER": "/monitoring",
    "LARK_ENCRYPT_KEY": "",
    "LARK_BOT_OPEN_ID": "",
    "LARK_WS_LOG_LEVEL": "INFO",
    "LARK_WS_USE_HTTP_KEYS": "0",
    "LARK_WS_EXTRA_IM_TYPES": "",
    "LARK_WS_TRANSPORT_LOG": "1",
    "LARK_WS_BOOTSTRAP_FRAMES": 16,
    "LARK_WS_LOG_FRAME_METHOD": "0",
    "LARK_WS_SDK_DEBUG": "0",
    "LARK_WEBHOOK_WSGI_LOG": "0",
    "LARK_WEBHOOK_TIMING_LOG": "0",
}


def _cfg_raw(key: str) -> Any:
    """``os.environ`` wins (systemd), else ``_CFG``."""
    if key in os.environ and str(os.environ.get(key, "")).strip() != "":
        return os.environ[key]
    return _CFG.get(key)


def _cfg_str(key: str, default: str = "") -> str:
    v = _cfg_raw(key)
    if v is None:
        return default
    s = str(v).strip()
    return s if s else default


def _cfg_int(key: str, default: int) -> int:
    v = _cfg_raw(key)
    if v is None or (isinstance(v, str) and not str(v).strip()):
        return default
    try:
        return int(v)
    except (TypeError, ValueError):
        return default


def _cfg_listen_port(default: int = 5002) -> int:
    """
    Same order as ``Chatbox/main.py``: ``PORT`` or ``LARKBOT_PORT`` in env (skip blanks),
    then ``_CFG["PORT"]`` / ``_CFG["LARKBOT_PORT"]``, then ``default`` (default **5002** here vs Chatbox **5000**).
    """
    for raw in (os.environ.get("PORT"), os.environ.get("LARKBOT_PORT")):
        if raw is None:
            continue
        s = str(raw).strip()
        if not s:
            continue
        try:
            return int(s)
        except ValueError:
            continue
    for key in ("PORT", "LARKBOT_PORT"):
        v = _CFG.get(key)
        if v is None:
            continue
        s = str(v).strip()
        if not s:
            continue
        try:
            return int(s)
        except ValueError:
            continue
    return default


# ``lark_oapi`` → ``ws/pb/google/__init__.py`` uses ``pkg_resources.declare_namespace`` (no upstream fix yet).
warnings.filterwarnings(
    "ignore",
    message=r"pkg_resources is deprecated as an API",
    category=UserWarning,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _lark_env_truthy(key: str) -> bool:
    v = _cfg_raw(key)
    if v is None:
        return False
    return str(v).strip().lower() in ("1", "true", "yes", "on")


app = Flask(__name__)


class _WsgiWebhookDiagMiddleware:
    """
    Optional WSGI logging — **default off**: sync writes to journald on every webhook can add latency.
    Feishu URL verification is often quoted as **~1s total budget** (RTT + handler); enable only when debugging::

      LARK_WEBHOOK_WSGI_LOG=1
    """

    def __init__(self, flask_app: Any):
        self.flask_app = flask_app

    def __call__(self, environ: Any, start_response: Any):
        path = environ.get("PATH_INFO") or ""
        if path.rstrip("/") == "/webhook/event" and _lark_env_truthy("LARK_WEBHOOK_WSGI_LOG"):
            logger.info(
                "WSGI enter %s %s content_length=%s expect=%r remote=%s",
                environ.get("REQUEST_METHOD"),
                path,
                environ.get("CONTENT_LENGTH"),
                environ.get("HTTP_EXPECT"),
                environ.get("REMOTE_ADDR"),
            )
        return self.flask_app(environ, start_response)


app.wsgi_app = _WsgiWebhookDiagMiddleware(app.wsgi_app)


def _request_is_webhook_event() -> bool:
    return (request.path or "").rstrip("/") == "/webhook/event"


@app.before_request
def _lark_webhook_request_timer_start():
    if (
        _request_is_webhook_event()
        and request.method == "POST"
        and _lark_env_truthy("LARK_WEBHOOK_TIMING_LOG")
    ):
        g._lark_wh_t0 = time.perf_counter()


@app.after_request
def _lark_webhook_request_timer_end(response: Response):
    """Optional timing log — ``LARK_WEBHOOK_TIMING_LOG=1``. Default off to avoid journald latency on hot path."""
    if not (
        _request_is_webhook_event()
        and request.method == "POST"
        and _lark_env_truthy("LARK_WEBHOOK_TIMING_LOG")
    ):
        return response
    t0 = getattr(g, "_lark_wh_t0", None)
    if t0 is not None:
        elapsed_ms = (time.perf_counter() - t0) * 1000
        xff = (request.headers.get("X-Forwarded-For") or "").split(",")[0].strip()
        remote = xff or (request.remote_addr or "")
        ua = (request.headers.get("User-Agent") or "")[:160]
        if elapsed_ms > 1000:
            logger.warning(
                "webhook/event POST slow elapsed_ms=%.1f status=%s remote=%s ua=%r",
                elapsed_ms,
                response.status_code,
                remote,
                ua,
            )
        else:
            logger.info(
                "webhook/event POST elapsed_ms=%.1f status=%s remote=%s",
                elapsed_ms,
                response.status_code,
                remote,
            )
    return response


# Lark duplicate pushes (same message_id) — align with Chatbox processed_messages pattern.
_processed_lark_message_ids: set = set()
_PROCESSED_LARK_IDS_CAP = 4000
_monitoring_reply_dispatch_lock = threading.Lock()
_lark_oapi_client: Optional[Any] = None
_lark_oapi_client_lock = threading.Lock()
# Set when WebSocket picks a working open.feishu.cn vs open.larksuite.com (``_get_lark_oapi_client`` must match).
_lark_open_api_domain_override: Optional[str] = None
_lark_ws_transport_log_installed: bool = False
_lark_ws_recv_method_log_installed: bool = False
_lark_ws_saw_data_frame: bool = False
# First N inbound protobuf frames logged at INFO (CONTROL vs DATA) without setting LARK_WS_LOG_FRAME_METHOD.
_LARK_WS_BOOTSTRAP_FRAMES_DEFAULT = 16
_lark_ws_bootstrap_frames_left: int = 0

GRAFANA_BASE_URL = _cfg_str("GRAFANA_BASE_URL", "https://grafana.client8.me").rstrip("/")
GRAFANA_DASHBOARD_PATH = _cfg_str(
    "GRAFANA_DASHBOARD_PATH",
    "/d/281e8816-ccb0-4335-922b-6b248491fd28/core-metrics-arms-aliyun",
)
GRAFANA_DASHBOARD_UID = _cfg_str(
    "GRAFANA_DASHBOARD_UID", "281e8816-ccb0-4335-922b-6b248491fd28"
)
GRAFANA_PANEL_TITLE = _cfg_str("GRAFANA_PANEL_TITLE", "请求总数/1m")
# Browser URL time range: last 10 minutes (matches “latest 10 mins”). Override e.g. now-1m if you want.
GRAFANA_DASHBOARD_FROM = _cfg_str("GRAFANA_DASHBOARD_FROM", "now-10m")
GRAFANA_DASHBOARD_TO = _cfg_str("GRAFANA_DASHBOARD_TO", "now")
# Prometheus query_range step (seconds); 60 → up to 10 buckets in 10m
GRAFANA_QUERY_STEP = _cfg_int("GRAFANA_QUERY_STEP", 60)
GRAFANA_QUERY_LOOKBACK_SECONDS = _cfg_int("GRAFANA_QUERY_LOOKBACK_SECONDS", 600)
GRAFANA_USER = (
    _cfg_str("GRAFANA_USER")
    or _cfg_str("GRAFANA_ID")
    or _cfg_str("grafanaid")
)
GRAFANA_PASSWORD = _cfg_str("GRAFANA_PASSWORD") or _cfg_str("grafanapassword")
VERIFICATION_TOKEN = _cfg_str("VERIFICATION_TOKEN", "").strip()
# For Open API (e.g. send message) — see Lark auth tenant_access_token_internal
APP_ID = _cfg_str("APP_ID", "").strip() or None
APP_SECRET = _cfg_str("APP_SECRET", "").strip() or None
# Default matches ``lark_oapi.core.const.FEISHU_DOMAIN`` — 国际 Lark 用 ``https://open.larksuite.com``（见 ``_CFG``）
LARK_HOST = _cfg_str("LARK_HOST", "https://open.feishu.cn").rstrip("/")
MONITORING_TRIGGER = _cfg_str("MONITORING_TRIGGER", "/monitoring")
LARK_ENCRYPT_KEY = (
    _cfg_str("LARK_ENCRYPT_KEY")
    or _cfg_str("ENCRYPT_KEY")
    or _cfg_str("FEISHU_ENCRYPT_KEY")
    or ""
).strip()
LARK_BOT_OPEN_ID = _cfg_str("LARK_BOT_OPEN_ID", "").strip()

# 群聊里富媒体等类型仍可能带可解析文本；仅跳过明显无 /monitoring 的类型。
_SKIP_IM_MESSAGE_TYPES = frozenset(
    {
        "image",
        "file",
        "audio",
        "media",
        "sticker",
        "location",
        "folder",
        "system",
        "hongbao",
        "share_chat",
        "share_user",
    }
)


def _lark_dict_pick_str(d: Any, *keys: str) -> str:
    """Lark payloads may use snake_case (HTTP) or camelCase (WebSocket / international)."""
    if not isinstance(d, dict):
        return ""
    for k in keys:
        v = d.get(k)
        if v is None:
            continue
        s = str(v).strip()
        if s:
            return s
    return ""


def _lark_message_chat_id(msg: Dict[str, Any]) -> str:
    """Group / topic chat id for ``create_message`` (``receive_id_type=chat_id``)."""
    cid = _lark_dict_pick_str(msg, "chat_id", "chatId", "open_chat_id", "openChatId")
    if cid:
        return cid
    c = msg.get("container")
    if isinstance(c, dict):
        return _lark_dict_pick_str(c, "chat_id", "chatId", "open_chat_id", "openChatId")
    return ""


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


def _lark_safe_parse_json_body(req: Any) -> Optional[Dict[str, Any]]:
    """Prefer ``get_json``; fallback to raw body (some proxies strip / alter Content-Type). Same idea as Chatbox."""
    raw = req.get_json(silent=True)
    if isinstance(raw, dict):
        return raw
    b = req.get_data(cache=False)
    if not b:
        return None
    if b.startswith(b"\xef\xbb\xbf"):
        b = b[3:]
    try:
        parsed = json.loads(b.decode("utf-8"))
        return parsed if isinstance(parsed, dict) else None
    except Exception:
        return None


def _lark_is_schema_v2(data: Any) -> bool:
    """Schema may arrive as str ``2.0`` or occasionally non-string — same guard as Chatbox."""
    if not isinstance(data, dict):
        return False
    s = data.get("schema")
    return s == "2.0" or str(s).strip() == "2.0"


def _lark_looks_like_lark_card_update_credential(token_str: Any) -> bool:
    """
    Flat ``card.action.trigger_v1`` uses top-level ``token`` = card credential (``c-``/``d-``), not Verification Token.
    Do not treat that as verification (Chatbox :func:`_lark_looks_like_lark_card_update_credential`).
    """
    s = (str(token_str or "")).strip()
    if not s:
        return False
    return s.startswith("c-") or s.startswith("d-")


def _lark_extract_verification_token(data: Any) -> Optional[str]:
    """
    App Verification Token: schema 2.0 ``header.token``; some payloads ``verification_token``.
    Same extraction order as Chatbox :func:`_lark_extract_verification_token`.
    """
    if not isinstance(data, dict):
        return None
    h = data.get("header")
    if isinstance(h, dict):
        for key in ("token", "Token", "verification_token"):
            t = h.get(key)
            if t is not None:
                return str(t).strip()
    vt = data.get("verification_token")
    if vt is not None:
        return str(vt).strip()
    t2 = data.get("token")
    if t2 is None:
        return None
    ts = str(t2).strip()
    if _lark_looks_like_lark_card_update_credential(ts):
        return None
    return ts


def _lark_coerce_event_dict(data: Any) -> Any:
    """Some gateways deliver ``event`` as a JSON string — normalize to dict (Chatbox :func:`_lark_coerce_event_dict`)."""
    if not isinstance(data, dict):
        return data
    ev = data.get("event")
    if isinstance(ev, str):
        try:
            parsed = json.loads(ev)
            data["event"] = parsed if isinstance(parsed, dict) else {}
        except Exception:
            data["event"] = {}
    elif ev is None and isinstance(data, dict):
        het = _lark_header_event_type(data)
        if isinstance(het, str) and het.startswith("card.action"):
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
    if not isinstance(msg, dict):
        return ""
    raw_c = msg.get("content")
    if raw_c is None:
        raw_c = msg.get("Content")
    if raw_c is None:
        raw_c = msg.get("body")
    if isinstance(raw_c, dict):
        content_str = json.dumps(raw_c, ensure_ascii=False)
    elif isinstance(raw_c, str):
        content_str = raw_c or "{}"
    else:
        content_str = "{}"
    mtype = (_lark_dict_pick_str(msg, "message_type", "messageType") or "").lower()
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
    raw = raw_text or ""
    clean = clean or ""
    if tri in raw or tri in clean:
        return True
    tl = tri.lower()
    return tl in clean.lower() or tl in raw.lower()


def _is_simple_hi_greeting(raw_text: str, clean: str, mentions: Any) -> bool:
    """
    Reply `hi` when user pings the bot and message contains a simple greeting token.
    """
    normalized = (clean or raw_text or "").strip().lower()
    normalized = re.sub(r"[!,.。！？]+", "", normalized).strip()
    # If platform already stripped @mention into "text_without_at_bot", keep simple one-word greeting working.
    if normalized in ("hi", "hello", "hey"):
        return True

    has_mention = False
    if isinstance(mentions, list) and len(mentions) > 0:
        has_mention = True
    low_raw = (raw_text or "").lower()
    if "<at" in low_raw:
        has_mention = True
    if re.search(r"(^|\s)@[^\s]+", (raw_text or "")):
        has_mention = True
    if not has_mention:
        return False

    # Remove plain-text @mentions before token matching, e.g. "@Monitoring bot hi".
    normalized = re.sub(r"(^|\s)@[^\s]+", " ", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return bool(re.search(r"\b(hi|hello|hey)\b", normalized))


def _reserve_message_id_once(mid: str) -> bool:
    if not mid:
        return True
    with _monitoring_reply_dispatch_lock:
        if mid in _processed_lark_message_ids:
            logger.info("duplicate message_id=%s — skip (already dispatched)", mid)
            return False
        _processed_lark_message_ids.add(mid)
        return True


def grafana_login_session() -> requests.Session:
    if not GRAFANA_USER or not GRAFANA_PASSWORD:
        raise ValueError("Set GRAFANA_USER and GRAFANA_PASSWORD in _CFG (top of main.py)")

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


def _lark_is_url_verification_payload(data: Dict[str, Any]) -> bool:
    """True for challenge/URL verification POST (several Feishu/Lark body shapes)."""
    if not isinstance(data, dict):
        return False
    if _lark_header_event_type(data) == "url_verification":
        return True
    if data.get("type") == "url_verification":
        return True
    ev = data.get("event")
    if isinstance(ev, dict) and str(ev.get("type") or "").strip() == "url_verification":
        return True
    return False


def _extract_url_verification(data: Dict[str, Any]) -> Optional[Tuple[str, str]]:
    """
    Return (token_hint, challenge) for Lark URL verification.

    Challenge may live in ``event.challenge``, or top-level ``challenge`` if a proxy
    flattened the JSON. Token: prefer :func:`_lark_extract_verification_token` at call site.
    """
    if not isinstance(data, dict):
        return None
    if not _lark_is_url_verification_payload(data):
        return None

    if data.get("type") == "url_verification":
        return (str(data.get("token") or ""), str(data.get("challenge") or ""))

    ev = data.get("event") if isinstance(data.get("event"), dict) else {}
    ch = ev.get("challenge")
    if ch is None:
        ch = data.get("challenge")
    if ch is None:
        return None

    tok = ev.get("token")
    if tok is None or (isinstance(tok, str) and not str(tok).strip()):
        h = data.get("header") if isinstance(data.get("header"), dict) else {}
        tok = h.get("token") or h.get("Token") or h.get("verification_token")
    return (str(tok or ""), str(ch))


def _lark_ack_only_event_type(het: str) -> bool:
    """Subscribed but not handled — still HTTP 200 (Chatbox :func:`_lark_ack_only_event_type`)."""
    if not het:
        return False
    h = het.lower()
    if h.startswith("meeting_room."):
        return True
    return False


def _lark_min_json_response(payload: Dict[str, Any], status: int = 200) -> Response:
    """Tight JSON body + explicit length — return before logging for URL verification."""
    body = json.dumps(payload, ensure_ascii=True, separators=(",", ":"))
    return Response(
        body,
        status=status,
        mimetype="application/json; charset=utf-8",
        headers={"X-Accel-Buffering": "no"},
    )


# Pre-built body avoids json.dumps on the hot ACK path (tiny win; no computation before flush).
_FEISHU_WEBHOOK_ACK_EMPTY_BODY = b"{}"


def _lark_feishu_webhook_ack_immediate() -> Response:
    """Feishu event/card HTTP callbacks should get 200 within ~3s; empty body is accepted after ACK."""
    return Response(
        _FEISHU_WEBHOOK_ACK_EMPTY_BODY,
        status=200,
        mimetype="application/json; charset=utf-8",
        headers={
            "Content-Length": "2",
            "X-Accel-Buffering": "no",
        },
    )


def _lark_webhook_url_verification_response_or_none(data: Dict[str, Any]) -> Optional[Response]:
    """If payload is Feishu URL verification / challenge, return minimal JSON immediately."""
    if data.get("type") == "url_verification":
        ch0 = data.get("challenge", "")
        return _lark_min_json_response({"challenge": str(ch0) if ch0 is not None else ""})
    uv = _extract_url_verification(data)
    if not uv:
        return None
    token_from_event, challenge = uv
    if VERIFICATION_TOKEN:
        effective_tok = _lark_extract_verification_token(data) or str(token_from_event or "").strip()
        if effective_tok != VERIFICATION_TOKEN:
            logger.warning(
                "url_verification token mismatch (exp_len=%s got_len=%s)",
                len(VERIFICATION_TOKEN),
                len(effective_tok or ""),
            )
            return _lark_min_json_response({"error": "invalid verification token"}, status=403)
    logger.debug("url_verification OK, challenge len=%s", len(str(challenge)))
    return _lark_min_json_response({"challenge": str(challenge)})


def _fast_plaintext_url_verification_response(raw_in: Dict[str, Any]) -> Optional[Response]:
    """
    Return Flask response for URL verification **before** decrypt/normalize pipeline.
    Uses :class:`Response` (not ``jsonify``) and no success logging so bytes leave ASAP.
    """
    if "encrypt" in raw_in:
        return None
    work = dict(raw_in)
    _lark_coerce_event_dict(work)
    if work.get("type") == "url_verification":
        ch0 = work.get("challenge", "")
        return _lark_min_json_response({"challenge": str(ch0) if ch0 is not None else ""})
    uv = _extract_url_verification(work)
    if not uv:
        return None
    token_from_event, challenge = uv
    if VERIFICATION_TOKEN:
        effective_tok = _lark_extract_verification_token(work) or str(token_from_event or "").strip()
        if effective_tok != VERIFICATION_TOKEN:
            logger.warning(
                "url_verification token mismatch (fast path) exp_len=%s got_len=%s",
                len(VERIFICATION_TOKEN),
                len(effective_tok or ""),
            )
            return _lark_min_json_response({"error": "invalid verification token"}, status=403)
    return _lark_min_json_response({"challenge": str(challenge)})


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


def _lark_api_domain() -> str:
    """Open Platform API host (tenant token + send message); align with working WS region when set."""
    d = (_lark_open_api_domain_override or LARK_HOST or "").strip().rstrip("/")
    return d or "https://open.feishu.cn"


def _get_lark_oapi_client() -> Any:
    """Singleton Feishu/Lark OpenAPI client (``lark-oapi``); token refresh handled by SDK."""
    global _lark_oapi_client
    if not APP_ID or not APP_SECRET:
        raise ValueError("APP_ID and APP_SECRET required for Lark reply")
    try:
        from lark_oapi import Client
    except ImportError as e:
        raise ImportError(
            "Install the Feishu/Lark Python SDK: pip install -U lark-oapi"
        ) from e
    with _lark_oapi_client_lock:
        if _lark_oapi_client is None:
            _lark_oapi_client = (
                Client.builder()
                .app_id(str(APP_ID).strip())
                .app_secret(str(APP_SECRET).strip())
                .domain(_lark_api_domain())
                .timeout(120.0)
                .build()
            )
    return _lark_oapi_client


def _lark_send_text(receive_id_type: str, receive_id: str, text: str) -> None:
    from lark_oapi.api.im.v1.model.create_message_request import CreateMessageRequest
    from lark_oapi.api.im.v1.model.create_message_request_body import CreateMessageRequestBody

    client = _get_lark_oapi_client()
    body = (
        CreateMessageRequestBody.builder()
        .receive_id(receive_id)
        .msg_type("text")
        .content(json.dumps({"text": text}))
        .build()
    )
    req = (
        CreateMessageRequest.builder()
        .receive_id_type(receive_id_type)
        .request_body(body)
        .build()
    )
    resp = client.im.v1.message.create(req)
    if not resp.success():
        raise RuntimeError(
            f"Lark send failed: code={resp.code!r} msg={resp.msg!r} log_id={resp.get_log_id()!r}"
        )


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


def _lark_verify_event_token(data: Dict[str, Any]) -> bool:
    """True when ``_lark_extract_verification_token`` matches ``VERIFICATION_TOKEN`` (Chatbox pattern)."""
    if not VERIFICATION_TOKEN:
        return True
    tok = _lark_extract_verification_token(data)
    return tok == VERIFICATION_TOKEN


def _monitoring_background_worker(chat_id: str, open_id: str, mid: str) -> None:
    """
    Grafana + Lark send can exceed Feishu's ~3s webhook limit — run off the request thread.
    """
    logger.info("monitoring background job start mid=%r chat=%r open=%r", mid, bool(chat_id), bool(open_id))
    try:
        payload = fetch_request_total_1m_series()
        reply = _format_monitoring_reply(payload)
    except Exception as e:
        logger.exception("monitoring fetch failed (background)")
        reply = f"监控数据拉取失败：{e}"

    sent = False
    try:
        if chat_id:
            _lark_send_text("chat_id", chat_id, reply)
            sent = True
            logger.info(
                "monitoring reply sent (background) receive_id_type=chat_id chat_id_prefix=%s... len=%s",
                chat_id[:16],
                len(reply),
            )
        elif open_id:
            _lark_send_text("open_id", open_id, reply)
            sent = True
            logger.info("monitoring reply sent (background) receive_id_type=open_id len=%s", len(reply))
        else:
            logger.warning(
                "monitoring background: no chat_id/open_chat_id or sender open_id; msg cannot be sent"
            )
    except Exception as e:
        logger.exception("lark send failed (background): %s", e)

    # ``mid`` was reserved on the webhook thread before ACK to avoid duplicate workers (HTTP returns {} immediately).
    if sent and mid and len(_processed_lark_message_ids) > _PROCESSED_LARK_IDS_CAP:
        _processed_lark_message_ids.clear()


def _hi_reply_background_worker(chat_id: str, open_id: str, mid: str) -> None:
    """Best-effort quick greeting reply used for '@bot hi'."""
    try:
        if chat_id:
            _lark_send_text("chat_id", chat_id, "hi")
            logger.info("hi reply sent receive_id_type=chat_id chat_id_prefix=%s...", chat_id[:16])
            return
        if open_id:
            _lark_send_text("open_id", open_id, "hi")
            logger.info("hi reply sent receive_id_type=open_id open_id_prefix=%s...", open_id[:16])
            return
        logger.warning("hi reply skipped: no chat_id/open_id")
    except Exception as e:
        logger.exception("hi reply failed: %s", e)
    finally:
        if mid and len(_processed_lark_message_ids) > _PROCESSED_LARK_IDS_CAP:
            _processed_lark_message_ids.clear()


def _serialize_lark_user_id(uid: Any) -> Dict[str, Any]:
    if uid is None:
        return {}
    out: Dict[str, Any] = {}
    for k in ("user_id", "open_id", "union_id"):
        v = getattr(uid, k, None)
        if v:
            out[k] = v
    return out


def _lark_ws_sdk_event_to_dict(model: Any) -> Dict[str, Any]:
    """
    Normalize WebSocket handler payloads to plain dict (same shape as HTTP webhook).

    Feishu docs recommend ``register_p2_im_message_receive_v1`` for long connection; that passes
    typed SDK models. ``JSON.marshal`` converts nested objects reliably; ``CustomizedEvent`` works too.
    """
    from lark_oapi.core.json import JSON

    if isinstance(model, dict):
        out = dict(model)
        _lark_coerce_event_dict(out)
        return out if isinstance(out, dict) else {}
    try:
        s = JSON.marshal(model)
        if not s:
            return {}
        obj = json.loads(s)
        if isinstance(obj, dict):
            _lark_coerce_event_dict(obj)
            return obj
    except Exception as e:
        logger.warning("Lark WS SDK event JSON marshal failed: %s", e)
    return {}


def _lark_customized_event_to_schema2_dict(ce: Any) -> Dict[str, Any]:
    """Backward-compatible path for customized handlers; prefer :func:`_lark_ws_sdk_event_to_dict`."""
    return _lark_ws_sdk_event_to_dict(ce)


def _process_im_message_event(data: Dict[str, Any]) -> None:
    """
    Shared handler for ``im.message`` from HTTP webhook or WebSocket (``CustomizedEvent`` v1/v2).
    HTTP path verifies token before calling; WS path uses ``EventDispatcherHandler.builder('', '')``.
    """
    try:
        _process_im_message_event_impl(data)
    except Exception:
        logger.exception("im.message handler crashed (swallowed so WS / HTTP worker stays up)")


def _process_im_message_event_impl(data: Dict[str, Any]) -> None:
    if isinstance(data, dict):
        data = _lark_coerce_event_dict(data)
    event = data.get("event") if isinstance(data.get("event"), dict) else {}
    raw_msg = event.get("message")
    msg = raw_msg if isinstance(raw_msg, dict) else {}
    mid = _lark_dict_pick_str(msg, "message_id", "messageId")
    mtype = (_lark_dict_pick_str(msg, "message_type", "messageType") or "").lower()
    chat_resolved = _lark_message_chat_id(msg)
    logger.info(
        "im.message mid=%r mtype=%r chat_prefix=%r",
        mid or None,
        mtype or None,
        (chat_resolved[:12] + "…") if len(chat_resolved) > 12 else (chat_resolved or None),
    )
    logger.debug("im.message msg_keys=%s", list(msg.keys())[:24] if isinstance(msg, dict) else [])
    if mtype and mtype in _SKIP_IM_MESSAGE_TYPES:
        logger.info("im.message ignored (non-textual): message_type=%r", mtype)
        return

    send_wrap = event.get("sender")
    if not isinstance(send_wrap, dict):
        send_wrap = {}
    sid = send_wrap.get("sender_id") or send_wrap.get("senderId")
    if isinstance(sid, dict):
        sender = sid
    elif sid is not None and hasattr(sid, "open_id"):
        sender = _serialize_lark_user_id(sid)
    else:
        sender = {}
    sender_open = _lark_dict_pick_str(sender, "open_id", "openId", "user_id", "userId")
    if LARK_BOT_OPEN_ID and sender_open == LARK_BOT_OPEN_ID:
        return

    raw_text = _lark_extract_plain_text_from_message(msg)
    if not (raw_text or "").strip():
        fb = _lark_dict_pick_str(event, "text_without_at_bot", "textWithoutAtBot", "text")
        if fb:
            raw_text = fb
    mentions_raw = msg.get("mentions") or []
    mentions = mentions_raw if isinstance(mentions_raw, list) else []
    clean = _lark_clean_command_text(raw_text, mentions)
    chat_id = chat_resolved
    open_id = sender_open

    if _is_simple_hi_greeting(raw_text, clean, mentions):
        if not _reserve_message_id_once(mid):
            return
        logger.info(
            "hi greeting matched — background hi reply mid=%r chat_id=%r open_id_prefix=%r",
            mid,
            bool(chat_id),
            (open_id[:12] + "…") if len(open_id) > 12 else open_id,
        )
        threading.Thread(
            target=_hi_reply_background_worker,
            args=(chat_id, open_id, mid),
            daemon=True,
            name="hi-reply",
        ).start()
        return

    if not _text_has_monitoring_trigger(raw_text, clean):
        logger.info(
            "im.message no trigger raw=%r clean=%r (need %r)",
            (raw_text or "")[:160],
            (clean or "")[:160],
            MONITORING_TRIGGER,
        )
        return

    logger.info(
        "monitoring trigger matched — background job mid=%r chat_id=%r open_id_prefix=%r",
        mid,
        bool(chat_id),
        (open_id[:12] + "…") if len(open_id) > 12 else open_id,
    )

    if not _reserve_message_id_once(mid):
        return

    threading.Thread(
        target=_monitoring_background_worker,
        args=(chat_id, open_id, mid),
        daemon=True,
        name="monitoring-reply",
    ).start()


def _ws_log_message_snip(data: Dict[str, Any]) -> Tuple[Any, Any, str]:
    """Safe for ``event.message`` missing or null (``dict.get('message', {})`` returns None if key exists)."""
    ev = data.get("event") if isinstance(data.get("event"), dict) else {}
    msg = ev.get("message") or {}
    if not isinstance(msg, dict):
        msg = {}
    mid = _lark_dict_pick_str(msg, "message_id", "messageId") or None
    mtype = _lark_dict_pick_str(msg, "message_type", "messageType") or None
    chat = (_lark_message_chat_id(msg) or "")[:12]
    return mid, mtype, chat


def _handle_im_message_receive(data: Dict[str, Any]) -> Response:
    """
    HTTP path: Feishu ~3s deadline — return ``{}`` immediately (no deepcopy on request thread).
    WebSocket path still calls :func:`_process_im_message_event` synchronously (no HTTP timeout).
    """

    def _worker(ref: Dict[str, Any]) -> None:
        try:
            payload = copy.deepcopy(ref)
            et = _lark_header_event_type(payload)
            logger.info(
                "handling %s (async) message_id=%r chat_id_prefix=%s",
                et,
                ((payload.get("event") or {}).get("message") or {}).get("message_id"),
                str(((payload.get("event") or {}).get("message") or {}).get("chat_id") or "")[:12],
            )
            _process_im_message_event(payload)
        except Exception:
            logger.exception("lark im.message webhook worker failed")

    threading.Thread(target=_worker, args=(data,), daemon=True, name="lark-im-webhook").start()
    return _lark_feishu_webhook_ack_immediate()


def _on_ws_p2_im_message_receive_v1(data: Any) -> None:
    """Official WS handler for ``im.message.receive_v1`` (Feishu long-connection sample code)."""
    try:
        logger.info("WS_HANDLER_HIT type=im.message.receive_v1")
        payload = _lark_ws_sdk_event_to_dict(data)
        mid, mtype, chat = _ws_log_message_snip(payload)
        logger.info("ws im.message.receive_v1 mid=%r mtype=%r chat=%r", mid, mtype, chat)
        _process_im_message_event(payload)
    except Exception:
        logger.exception("WebSocket P2ImMessageReceiveV1 handler failed")


def _on_ws_im_message_p2_customized(ce: Any) -> None:
    """
    Fallback for ``im.message.receive_v2`` or extra types (``LARK_WS_EXTRA_IM_TYPES``).
    ``receive_v1`` is handled by :func:`_on_ws_p2_im_message_receive_v1` per Feishu SDK guidance.
    """
    try:
        et = getattr(getattr(ce, "header", None), "event_type", None) or "?"
        logger.info("WS_HANDLER_HIT type=%s", et)
        data = _lark_ws_sdk_event_to_dict(ce)
        mid, mtype, chat = _ws_log_message_snip(data)
        logger.info("ws im.message %s mid=%r mtype=%r chat=%r", et, mid, mtype, chat)
        _process_im_message_event(data)
    except Exception:
        logger.exception("WebSocket im.message customized handler failed")


def _lark_ws_patch_dispatcher_inbound_log(handler: Any) -> None:
    """
    Wrap ``do_without_validation`` so we always see ``header.event_type`` for DATA/EVENT frames.
    Catches ``processor not found`` and logs the missing type (SDK default log may go to another logger).
    """
    orig = handler.do_without_validation

    def _wrapped(payload: bytes) -> Any:
        et_log: Any = None
        try:
            obj = json.loads(payload.decode("utf-8", errors="replace"))
            h = obj.get("header") if isinstance(obj.get("header"), dict) else {}
            et_log = h.get("event_type")
            if et_log:
                logger.info(
                    "Lark WS inbound event_type=%r schema=%r",
                    et_log,
                    obj.get("schema"),
                )
            else:
                logger.info(
                    "Lark WS inbound (no header.event_type) top_keys=%r event_keys=%r",
                    list(obj.keys())[:14],
                    list((obj.get("event") or {}).keys())[:14] if isinstance(obj.get("event"), dict) else None,
                )
        except Exception as ex:
            logger.warning(
                "Lark WS payload not JSON (%s) len=%s head=%r",
                ex,
                len(payload),
                payload[:80],
            )
        try:
            return orig(payload)
        except Exception as e:
            es = str(e).lower()
            if et_log is not None and "processor" in es and "not found" in es:
                logger.error(
                    "Lark WS no handler for event_type=%r — add to LARK_WS_EXTRA_IM_TYPES in .env (comma-separated) "
                    "or upgrade lark-oapi. err=%s",
                    et_log,
                    e,
                )
            raise

    handler.do_without_validation = _wrapped  # type: ignore[method-assign]


def _lark_ws_reset_bootstrap_frame_budget() -> int:
    """How many inbound WS protobuf frames to log at INFO on this connection (0 = off)."""
    global _lark_ws_bootstrap_frames_left
    raw = str(_cfg_int("LARK_WS_BOOTSTRAP_FRAMES", _LARK_WS_BOOTSTRAP_FRAMES_DEFAULT))
    try:
        n = int(raw)
    except ValueError:
        n = _LARK_WS_BOOTSTRAP_FRAMES_DEFAULT
    _lark_ws_bootstrap_frames_left = max(0, min(n, 500))
    return _lark_ws_bootstrap_frames_left


def _lark_ws_install_recv_frame_method_log(client_cls: Any) -> None:
    """
    Always patch inbound ``Frame.method`` logging:

    - By default, first ``LARK_WS_BOOTSTRAP_FRAMES`` frames at INFO (CONTROL vs DATA).
    - Set ``LARK_WS_LOG_FRAME_METHOD=1`` to log **every** frame.

    DATA frames carry Feishu business payloads (often IM events). CONTROL is ping/config.
    If you only ever see CONTROL after @mentioning the bot, Feishu is not pushing IM events to this connection
    (subscription, scopes, duplicate WS consumer, etc.).
    """
    global _lark_ws_recv_method_log_installed
    if _lark_ws_recv_method_log_installed:
        return
    from lark_oapi.ws.enum import FrameType
    from lark_oapi.ws.pb.pbbp2_pb2 import Frame as LarkWsPbFrame

    _orig = client_cls._handle_message

    async def _wrapped_handle_message(self: Any, msg: bytes) -> None:
        global _lark_ws_bootstrap_frames_left
        full = _lark_env_truthy("LARK_WS_LOG_FRAME_METHOD")
        want_log = full or (_lark_ws_bootstrap_frames_left > 0)
        if want_log and not full:
            _lark_ws_bootstrap_frames_left -= 1
        if want_log:
            try:
                pb = LarkWsPbFrame()
                pb.ParseFromString(msg)
                ft = FrameType(pb.method)
                logger.info(
                    "Lark WS recv frame.method=%s bytes=%s (DATA=push payload; CONTROL=heartbeat/config)",
                    getattr(ft, "name", str(ft)),
                    len(msg),
                )
            except Exception as ex:
                logger.warning("Lark WS recv frame parse failed: %s bytes=%s", ex, len(msg))
        return await _orig(self, msg)

    client_cls._handle_message = _wrapped_handle_message  # type: ignore[method-assign]
    _lark_ws_recv_method_log_installed = True


def _lark_ws_install_transport_frame_log(client_cls: Any) -> None:
    """
    Log every DATA-frame ``header.type`` (e.g. ``event`` / ``card``). Must patch the **same** ``Client`` class
    later used by ``LarkWsClient(...)`` (import identity issues prevented logs on some deployments).
    """
    global _lark_ws_transport_log_installed, _lark_ws_saw_data_frame
    if _lark_ws_transport_log_installed:
        return
    if _cfg_str("LARK_WS_TRANSPORT_LOG", "1").strip().lower() in ("0", "false", "no", "off"):
        logger.info("Lark WS transport frame logging disabled (LARK_WS_TRANSPORT_LOG=0)")
        return

    from lark_oapi.ws.const import HEADER_TYPE
    from lark_oapi.ws import client as _lark_ws_client_mod

    _orig_hdf = client_cls._handle_data_frame

    async def _logged_handle_data_frame(self: Any, frame: Any) -> None:
        global _lark_ws_saw_data_frame
        try:
            hs = frame.headers
            t = _lark_ws_client_mod._get_by_key(hs, HEADER_TYPE)
            plen = len(frame.payload or b"")
            logger.info("Lark WS DATA frame header.type=%r payload_len=%s", t, plen)
            _lark_ws_saw_data_frame = True
        except Exception as ex:
            logger.warning("Lark WS DATA frame log failed: %s", ex)
        return await _orig_hdf(self, frame)

    client_cls._handle_data_frame = _logged_handle_data_frame  # type: ignore[method-assign]
    _lark_ws_transport_log_installed = True
    logger.info(
        "Lark WS transport frame log patch applied to %s._handle_data_frame",
        getattr(client_cls, "__name__", "Client"),
    )


def _lark_ws_start_no_data_watchdog() -> None:
    """If zero DATA frames in 120s, emit ERROR (console subscription / duplicate client)."""

    def _watch() -> None:
        time.sleep(120)
        if _lark_ws_saw_data_frame:
            return
        logger.error(
            "Lark WS: 启动 120 秒内未收到任何 DATA 帧 — 飞书未往本连接推事件。请逐项核对："
            "① 开发者后台「事件与回调」→ 订阅方式必须是「使用长连接接收事件」且保存成功（保存时本服务须已连接）；"
            "② 勿同时选「将回调发送至开发者服务器」；③ 已订阅「消息与群组」→「接收消息」；"
            "④ 机器人已在目标群且具备 @ 机器人相关权限；⑤ 同 APP_ID 仅一条 WS（关其它环境/旧进程）；"
            "⑥ 可设 LARK_WS_SDK_DEBUG=1 看 Lark SDK 原始日志；⑦ 默认会打前若干帧 frame.method：若始终无 DATA、仅有 CONTROL，"
            "说明链路通但飞书未往本连接推事件（订阅/权限/多实例）。⑧ 长连接模式下 IM 事件不会走 HTTP POST /webhook/event。"
        )

    threading.Thread(target=_watch, name="lark-ws-watchdog", daemon=True).start()


def _lark_ws_domain_try_order() -> List[str]:
    """Prefer ``LARK_HOST``, then try the other public Open Platform host (fixes 1000040351)."""
    seen: set = set()
    out: List[str] = []
    raw = (LARK_HOST or "").strip().rstrip("/")
    for d in (raw, "https://open.feishu.cn", "https://open.larksuite.com"):
        if not d or d in seen:
            continue
        seen.add(d)
        out.append(d)
    return out


def start_lark_ws_client_blocking() -> None:
    """
    Official long-connection mode (no public Request URL, no HTTP challenge).
    Blocks until disconnect (or fatal error). Requires ``APP_ID`` + ``APP_SECRET``.
    """
    if not APP_ID or not APP_SECRET:
        raise RuntimeError("APP_ID and APP_SECRET are required for Lark WebSocket client")

    from lark_oapi import EventDispatcherHandler
    from lark_oapi.core.enum import LogLevel
    from lark_oapi.ws.client import Client as LarkWsClient

    global _lark_ws_saw_data_frame
    _lark_ws_saw_data_frame = False
    _n_boot = _lark_ws_reset_bootstrap_frame_budget()
    _lark_ws_install_transport_frame_log(LarkWsClient)
    _lark_ws_install_recv_frame_method_log(LarkWsClient)
    if _n_boot:
        logger.info(
            "Lark WS bootstrap: will log first %s inbound protobuf frames at INFO "
            "(CONTROL vs DATA). Long-connection IM events do **not** produce HTTP POST /webhook/event.",
            _n_boot,
        )
    logger.info(
        "Reminder: with LARK_EVENT_MODE=ws, Feishu delivers IM events only on the WebSocket — "
        "expect journal lines like 'Lark WS recv frame.method=DATA' / 'Lark WS DATA frame', not POST /webhook/event."
    )
    if _cfg_str("LARK_WS_TRANSPORT_LOG", "1").strip().lower() not in ("0", "false", "no", "off"):
        _lark_ws_start_no_data_watchdog()

    # 飞书「使用长连接接收事件」文档：builder 前两参须为 **空字符串**（勿传 HTTP 回调的 Encrypt/Verification）。
    ws_use_http_keys = _lark_env_truthy("LARK_WS_USE_HTTP_KEYS")
    enc = (LARK_ENCRYPT_KEY or "") if ws_use_http_keys else ""
    ver = (VERIFICATION_TOKEN or "") if ws_use_http_keys else ""
    if ws_use_http_keys:
        logger.warning(
            "LARK_WS_USE_HTTP_KEYS=1 — passing encrypt/verification into WS handler (non-standard; "
            "prefer empty per Feishu long-connection doc)."
        )
    else:
        logger.info(
            "Lark WS EventDispatcherHandler.builder('', '') — HTTP 的 VERIFICATION_TOKEN/LARK_ENCRYPT_KEY 不用于长连接"
        )
    bld = (
        EventDispatcherHandler.builder(enc, ver)
        .register_p2_im_message_receive_v1(_on_ws_p2_im_message_receive_v1)
        .register_p2_customized_event("im.message.receive_v2", _on_ws_im_message_p2_customized)
    )
    for raw_t in _cfg_str("LARK_WS_EXTRA_IM_TYPES", "").replace(";", ",").split(","):
        t = raw_t.strip()
        if not t:
            continue
        logger.info("Lark WS also registering custom event_type=%r (LARK_WS_EXTRA_IM_TYPES)", t)
        bld = bld.register_p2_customized_event(t, _on_ws_im_message_p2_customized)
    handler = bld.build()
    pmap = getattr(handler, "_processorMap", None) or {}
    logger.info("Lark WS p2 processors registered: %s", sorted(pmap.keys()))
    _lark_ws_patch_dispatcher_inbound_log(handler)

    level_name = _cfg_str("LARK_WS_LOG_LEVEL", "INFO").strip().upper()
    log_level = getattr(LogLevel, level_name, LogLevel.INFO)
    if _lark_env_truthy("LARK_WS_SDK_DEBUG"):
        log_level = LogLevel.DEBUG
        logger.info("LARK_WS_SDK_DEBUG=1 — Lark SDK internal logs at DEBUG")

    logger.warning(
        "长连接为集群投递：同 APP 若有多条 WS 或其它实例，仅随机一台会收到消息；请只保留一个 monitoring 进程。"
    )
    logger.warning(
        "若发消息后始终没有「Lark WS DATA frame」或「Lark WS inbound」日志：请到飞书开发者后台确认 "
        "「事件与回调」订阅方式为「使用长连接接收事件」并已保存成功（保存时本进程须在线）；"
        "且已订阅「接收消息」并具备群 @ 等权限；勿与「将回调发送至开发者服务器」混用。"
        " 调试可加 LARK_WS_LOG_FRAME_METHOD=1 看每条下行帧是 CONTROL 还是 DATA。"
    )

    last_domain_err: Optional[BaseException] = None
    global _lark_open_api_domain_override, _lark_oapi_client
    for domain in _lark_ws_domain_try_order():
        dnorm = domain.rstrip("/")
        with _lark_oapi_client_lock:
            _lark_oapi_client = None
        _lark_open_api_domain_override = dnorm
        cli = LarkWsClient(
            str(APP_ID).strip(),
            str(APP_SECRET).strip(),
            log_level=log_level,
            event_handler=handler,
            domain=dnorm,
            auto_reconnect=True,
        )
        logger.info(
            "Lark WebSocket client starting (domain=%s); WS handlers: "
            "p2 im.message.receive_v1 (typed SDK) + p2 im.message.receive_v2 (customized)",
            dnorm,
        )
        try:
            cli.start()
        except Exception as e:
            err = str(e)
            if "1000040351" in err or "incorrect domain" in err.lower():
                last_domain_err = e
                logger.warning(
                    "Lark WebSocket domain rejected on %r (%s) — trying alternate open-platform host if any.",
                    domain,
                    err,
                )
                continue
            raise

    if last_domain_err is not None:
        logger.error(
            "Lark WebSocket: every candidate domain failed with incorrect-domain (1000040351). "
            "Set LARK_HOST explicitly to the host shown in your Feishu/Lark developer console. Last: %s",
            last_domain_err,
        )
        raise last_domain_err


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"ok": True})


@app.route("/webhook/event", methods=["GET", "POST", "OPTIONS", "HEAD"], strict_slashes=False)
def webhook_event():
    if request.method == "POST":
        logger.info(
            "WEBHOOK_HIT method=POST path=%s remote=%s len=%s ct=%r",
            request.path,
            request.remote_addr,
            request.content_length,
            (request.headers.get("Content-Type") or "")[:120],
        )
        # Chatbox style ingress marker for journalctl grepping.
        print(
            "[lark] webhook POST len=%s path=%s ct=%s"
            % (
                request.content_length,
                request.path,
                (request.headers.get("Content-Type") or "")[:80],
            ),
            flush=True,
        )
    # Chatbox: OPTIONS must not 405 — some clients preflight the callback URL.
    if request.method == "OPTIONS":
        return "", 204
    if request.method == "HEAD":
        return "", 200

    if request.method == "GET":
        # No secrets — use to confirm env + URL reachability from browser/curl.
        _listen_port = _cfg_listen_port(5002)
        app_id = (APP_ID or "").strip()
        lark_sdk_version: Optional[str] = None
        try:
            from lark_oapi.core.const import VERSION as _lark_oapi_pkg_version  # type: ignore

            lark_sdk_version = str(_lark_oapi_pkg_version)
        except ImportError:
            lark_sdk_version = None
        return jsonify(
            {
                "ok": True,
                "hint": "Feishu must POST JSON to this path for events (HTTP mode only).",
                "lark_event_mode_tip": (
                    "默认 ``python main.py`` + ``LARK_EVENT_MODE=ws`` 使用官方 WebSocket 长连接，无需配置 Request URL。"
                    "若仍用 HTTP 回调，请设 LARK_EVENT_MODE=http；并核对下方 url_protocol_tip。"
                ),
                "url_protocol_tip": (
                    "Lark 请求 URL 校验走 POST。若控制台填了 https:// 而本服务只监听 http://（无 TLS），"
                    f"客户端会一直握手直到约 3s 超时 — 请改为 http://IP:{_listen_port}/webhook/event，或在前面加 Nginx/证书。"
                ),
                "lark_host": LARK_HOST,
                "lark_oapi_installed": lark_sdk_version is not None,
                "lark_oapi_version": lark_sdk_version,
                "app_id_prefix": (app_id[:12] + "…") if len(app_id) > 12 else app_id,
                "verification_token_configured": bool(VERIFICATION_TOKEN),
                "app_secret_configured": bool(APP_SECRET),
                "encrypt_key_configured": bool(LARK_ENCRYPT_KEY),
                "grafana_user_configured": bool(GRAFANA_USER),
                "feishu_url_verify_local_test_cn": (
                    "勿只 POST {\"challenge\":\"...\"}：不会被识别为 URL 校验，会走事件 token 校验 → 403 Invalid token（属正常）。"
                    "正确测本机延迟请用 legacy 体：{\"type\":\"url_verification\",\"token\":\"与 _CFG 中 VERIFICATION_TOKEN 一致\",\"challenge\":\"ping\"}，"
                    "应返回 HTTP 200 且 JSON 内含 challenge。"
                ),
                "feishu_url_verify_local_test_en": (
                    "Posting only {\"challenge\":\"...\"} is NOT a Feishu url_verification payload — it falls through to "
                    "event token verification → 403 is correct. For a local latency test use "
                    "{\"type\":\"url_verification\",\"token\":\"YOUR_VERIFICATION_TOKEN\",\"challenge\":\"ping\"} "
                    "(expect 200 and echoed challenge)."
                ),
                "feishu_timeout_local_200_cn": (
                    "若本机 curl 很快 200，但飞书控制台仍报约 3s 超时：多半是「飞书机房到你公网 IP」链路问题，而非 Python 处理慢。"
                    "请①用境外/另一台云的 curl 测公网 URL；②控制台 URL 必须与应用一致（http/https、端口）；③安全组放行源站入站；"
                    "④查看 journalctl 是否在点击校验时出现 webhook/event POST elapsed_ms=…（若无日志=请求未到进程）。"
                ),
                "feishu_timeout_local_200_en": (
                    "If local curl returns 200 quickly but the Lark console still shows ~3s timeout, the delay is usually "
                    "network/TLS/firewall path from Lark servers to your public URL — not Flask handler time. "
                    "curl the public URL from an external VPS; fix http vs https; open security groups; check logs for "
                    "webhook/event POST elapsed_ms when you click verify (no log means the request never reached the app)."
                ),
                "checklist_cn": [
                    "推荐：开发者后台「事件与回调」→ 使用长连接接收事件，运行 ``python main.py``（LARK_EVENT_MODE=ws，默认），无需公网 URL。",
                    "若用 HTTP：Request URL 须指向本服务 POST /webhook/event（公网可达），并设 LARK_EVENT_MODE=http。",
                    "订阅「消息与群组」→「接收消息 v2.0」；群内需权限：@机器人消息 (im:message.group_at_msg) 或群全量消息权限。",
                    "VERIFICATION_TOKEN 与后台「Verification Token」一致（无多余空格）。",
                    "国内飞书应用将 LARK_HOST 设为 https://open.feishu.cn；国际用 https://open.larksuite.com。",
                    "机器人需能力「机器人」+ 权限「以应用身份发消息」等，且机器人在目标群内。",
                    "发 /monitoring 后看日志：handling im.message / monitoring background job / monitoring reply sent (background)。",
                    "飞书约 3s 超时：请用 python main.py 启动；webhook 先 200，Grafana 在后台线程执行。",
                    "发消息依赖 lark-oapi：pip install -U lark-oapi；GET 本 URL 可查看 lark_oapi_version。",
                    "lark_oapi_installed=false 只影响发消息，不影响「请求 URL 校验」；校验失败多半是 VERIFICATION_TOKEN 与后台不一致。",
                    "若用 systemd：可在 unit 里 Environment=VERIFICATION_TOKEN=… / Environment=PORT=5002（与同机 Chatbox 的 5000 区分），或 EnvironmentFile=-/path/to/.env；修改后 daemon-reload && restart。",
                    f"若飞书提示 3s 超时：云厂商安全组/防火墙须放行公网入站 TCP {_listen_port}；本机 curl -m 5 -X POST http://IP:{_listen_port}/webhook/event -H Content-Type:application/json -d '{{...}}' 测连通。",
                    "仍超时：核对控制台 URL 与监听一致（http/https）；排查时设 LARK_WEBHOOK_WSGI_LOG=1 再看 journal。",
                    "curl 勿只发 {\"challenge\":\"ping\"}→403 正常；应用 {\"type\":\"url_verification\",\"token\":\"…\",\"challenge\":\"ping\"} 测 POST 延迟。",
                    "本机 200 仍超时：外网 curl POST url_verification；设 LARK_WEBHOOK_TIMING_LOG=1 看 elapsed_ms；或改用 ws 模式。",
                    "URL 校验文档常见「约 1s」总预算（含 RTT）：默认关闭 webhook 热路径 INFO 日志；排查时再设 LARK_WEBHOOK_WSGI_LOG=1 / LARK_WEBHOOK_TIMING_LOG=1。",
                    "HTTP 校验仍失败可改 LARK_EVENT_MODE=ws 用长连接，免 Request URL。",
                ],
            }
        )

    raw_in = _lark_safe_parse_json_body(request)
    if raw_in is None:
        snip = ""
        try:
            raw_b = request.get_data(cache=False, as_text=True)
            if raw_b:
                snip = raw_b[:300].replace("\n", " ")
        except Exception:
            pass
        logger.warning(
            "webhook POST body not JSON remote=%s ct=%r snip=%r",
            request.remote_addr,
            (request.headers.get("Content-Type") or ""),
            snip,
        )
        return jsonify({"error": "invalid json"}), 400

    if isinstance(raw_in, dict):
        fast_resp = _fast_plaintext_url_verification_response(raw_in)
        if fast_resp is not None:
            return fast_resp

    if request.method == "POST":
        logger.debug(
            "webhook POST remote=%s len=%s ct=%r",
            request.remote_addr,
            request.content_length,
            (request.headers.get("Content-Type") or "")[:120],
        )

    data = _feishu_maybe_decrypt_webhook_payload(raw_in)

    if isinstance(raw_in, dict) and raw_in.get("encrypt") is not None and data is raw_in:
        logger.error(
            "Webhook still encrypted — set LARK_ENCRYPT_KEY + pycryptodome, or disable 加密 (Chatbox logs this as 403)."
        )
        return jsonify({"error": "Invalid token"}), 403

    if not isinstance(data, dict):
        return jsonify({"error": "invalid payload"}), 400

    data = _lark_coerce_event_dict(data)
    uv_early = _lark_webhook_url_verification_response_or_none(data)
    if uv_early is not None:
        return uv_early

    data = _lark_normalize_webhook(data)
    data = _lark_coerce_event_dict(data)
    uv_after_norm = _lark_webhook_url_verification_response_or_none(data)
    if uv_after_norm is not None:
        return uv_after_norm

    if not _lark_verify_event_token(data):
        logger.warning(
            "webhook token mismatch: expected VERIFICATION_TOKEN, got %r schema=%r schema_v2=%s",
            _lark_extract_verification_token(data),
            data.get("schema"),
            _lark_is_schema_v2(data),
        )
        return jsonify({"error": "Invalid token"}), 403

    et = _lark_header_event_type(data)
    et_l = (et or "").lower()
    logger.info(
        "WEBHOOK_EVENT event_type=%r schema=%r keys=%s",
        et,
        data.get("schema"),
        list(data.keys())[:20],
    )
    # Card interactions also require a fast 200; business logic should update the card asynchronously via Open API.
    if et_l.startswith("card.action"):
        return _lark_feishu_webhook_ack_immediate()

    if _lark_ack_only_event_type(et):
        return _lark_feishu_webhook_ack_immediate()

    if et in ("im.message.receive_v1", "im.message.receive_v2"):
        return _handle_im_message_receive(data)

    logger.info(
        "event ignored: event_type=%r keys=%s (subscribe 消息与群组 → 接收消息 v2.0)",
        et,
        list(data.keys())[:20],
    )
    return _lark_feishu_webhook_ack_immediate()


def _register_lark_webhook_duplicate_paths() -> None:
    """
    Chatbox-compatible legacy callback paths:
    set ``LARK_WEBHOOK_EXTRA_PATHS=/callback,/open/event`` to map additional POST paths
    onto the same ``webhook_event`` handler.
    """
    extra = _cfg_str("LARK_WEBHOOK_EXTRA_PATHS", "").strip()
    if not extra:
        return
    for i, raw in enumerate(extra.split(",")):
        path = raw.strip()
        if not path or path.rstrip("/") == "/webhook/event":
            continue
        app.add_url_rule(path, f"lark_webhook_extra_{i}", webhook_event, methods=["POST", "GET", "OPTIONS", "HEAD"])
        logger.info("Extra webhook route registered: %s", path)
        print(f"[lark] Extra webhook POST route registered: {path}", flush=True)


_register_lark_webhook_duplicate_paths()


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


def run_monitoring_bot() -> None:
    """
    Process entrypoint: HTTP-only, WebSocket-only, or WS + HTTP sidecar (see module docstring).
    Uses :data:`app`, :data:`logger`, :func:`start_lark_ws_client_blocking` from this module.
    """
    port = _cfg_listen_port(5002)
    raw_mode = _cfg_str("LARK_EVENT_MODE", "ws").strip().lower()
    mode = raw_mode if raw_mode else "ws"

    def run_http() -> None:
        stack = _cfg_str("HTTP_SERVER", "flask").strip().lower()
        use_waitress = stack in ("waitress", "wsgi")
        if not use_waitress:
            logger.info(
                "HTTP (Flask threaded=True, Chatbox/main.py style) on 0.0.0.0:%s — "
                "/health /grafana/ping /webhook/event (set HTTP_SERVER=waitress for Waitress)",
                port,
            )
            app.run(host="0.0.0.0", port=port, threaded=True, use_reloader=False, debug=False)
            return
        try:
            from waitress import serve

            try:
                threads = _cfg_int("WAITRESS_THREADS", 24)
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
            logger.warning("waitress not installed — pip install waitress; falling back to Flask threaded server")
            app.run(host="0.0.0.0", port=port, threaded=True, use_reloader=False, debug=False)

    if mode == "http":
        logger.info("LARK_EVENT_MODE=http — Feishu events via POST /webhook/event only")
        hint = _cfg_str("LARK_WEBHOOK_PUBLIC_URL", "").strip()
        if hint:
            logger.info("Feishu developer console → 事件与回调 → Request URL (示例配置): %s", hint)
            if hint.lower().startswith("https://"):
                logger.error(
                    "LARK_WEBHOOK_PUBLIC_URL / 控制台若使用 https:// 而本进程仅 plain HTTP，飞书会 TLS 握手失败或卡住≈3s。"
                    "请改为 http://…:%s/webhook/event，或在前面加 Nginx/证书终止 TLS。",
                    port,
                )
            if hint.rstrip("/").endswith("/webhook/event/"):
                logger.warning(
                    "Request URL 尽量不要带末尾 /；已启用 strict_slashes=False，仍建议与控制台完全一致。"
                )
        else:
            logger.info(
                "Set LARK_WEBHOOK_PUBLIC_URL in _CFG to log your Feishu Request URL hint "
                "(e.g. http://YOUR_IP:%s/webhook/event).",
                port,
            )
        logger.warning(
            "飞书 HTTP「请求网址校验」文档常写 **约 1 秒内** 返回 challenge（含网络往返）；推送事件常见 **约 3 秒**。"
            "webhook 热路径默认 **不写** WSGI/耗时 INFO，避免 journald 延迟；若仍超时，先试 ``LARK_EVENT_MODE=ws`` 长连接免 URL 校验，"
            "或在前面加 Nginx+HTTPS；排查时再设 LARK_WEBHOOK_WSGI_LOG=1。"
        )
        run_http()
        return

    if mode != "ws":
        raise SystemExit(f"Unknown LARK_EVENT_MODE={mode!r} (use ws or http)")

    http_on = _cfg_str("ENABLE_HTTP", "1").strip().lower() in ("1", "true", "yes", "on")
    http_thread: Optional[threading.Thread] = None
    if http_on:
        http_thread = threading.Thread(target=run_http, name="http-sidecar", daemon=False)
        http_thread.start()
        time.sleep(0.2)
    else:
        logger.info("ENABLE_HTTP=0 — only Lark WebSocket client (no HTTP listener)")

    try:
        start_lark_ws_client_blocking()
    except Exception:
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
        raise SystemExit(1)


if __name__ == "__main__":
    run_monitoring_bot()