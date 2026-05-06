#!/usr/bin/env python3
"""
Lark events：**HTTP webhook** 或 **WebSocket 长连接**（二选一，由 ``LARK_EVENT_MODE`` 决定）+ Grafana。

启动: ``python main.py``

**配置**：编辑本文件顶部 ``_CFG`` 字典（不再读取 ``.env``）。也可用 **systemd ``Environment=KEY=value``** 覆盖同名键。

**``LARK_EVENT_MODE=http``（纯 HTTP，不启 WebSocket）** — 监听 ``PORT``，事件只走 ``POST /webhook/event``。飞书后台请选 **「将事件发送至开发者服务器」** Request URL，**不要**再选「使用长连接接收事件」，否则事件可能仍发到别处或产生混淆。

**``LARK_EVENT_MODE=ws``** — 仅 Lark **长连接**收事件；可选 ``ENABLE_HTTP=1`` 并行起 HTTP 侧车（健康检查等），此时建议 ``LARK_HTTP_IGNORE_IM_WHEN_EVENT_MODE_WS=1``，避免 IM 在 HTTP+WS 各收一次。
若同一条触发出现 **两条回复**：见 ``LARK_WS_REGISTER_IM_MESSAGE_V2``、``LARK_HTTP_IGNORE_IM_WHEN_EVENT_MODE_WS``、``MONITORING_IM_DEBOUNCE_SECONDS``。

**HTTP 模式** — 监听 ``PORT``（本仓库默认 **5002**，与同机运行的 ``Chatbox/main.py``（常用 **5000**）错开端口），事件走 ``POST /webhook/event``。
默认 HTTP 栈为 **Flask ``threaded=True``**（实现方式对齐 Chatbox）；生产可设 ``HTTP_SERVER=waitress``。

端口解析顺序：**环境变量 ``PORT`` → ``LARKBOT_PORT`` → ``_CFG["PORT"]``**（与 Chatbox 的 ``PORT``/``LARKBOT_PORT`` 习惯一致）。

飞书后台「事件与回调」；``APP_ID`` / ``APP_SECRET`` 必填。国际 Lark ``LARK_HOST=https://open.larksuite.com``。

群/at 机器人发 ``/monitoring`` **或仅 @ 机器人（无其它正文）** → 同一条 Grafana 摘要（最近 10 分钟，与 ``GRAFANA_QUERY_LOOKBACK_SECONDS`` / ``now-10m`` 一致）。
默认 ``MONITORING_MESSAGE_CARD_ENABLE=1``：用户侧 **一条** 交互卡片（``msg_type=interactive``），截图嵌在卡片内，不再跟一条独立 PNG。设 ``0`` 则仍为「纯文字 + 独立图片」两条消息。需在飞书应用开通「发送消息卡片」权限。

HTTP 回调先返回 ``{}`` 再后台处理。HTTP 跌幅告警命中时可额外转发到 ``MONITORING_ALERT_CHAT_ID``（群 ``chat_id``，如 ``oc_…``）。

可选 ``GRAFANA_SCREENSHOT_ENABLE=1``：与卡片同开时先截一张上传为 ``image_key`` 嵌入卡片；仅关卡片时仍为无头 Chromium 截图后单独发图（需 ``pip install playwright`` 与 ``playwright install chromium``）。
默认 ``GRAFANA_SCREENSHOT_FULL_PAGE=1`` 截整页滚动区域（长 dashboard 全部图表）；设为 ``0`` 则仅视口大小（易只拍到上半屏）。
多轮滚动 + Spinner 轮询见 ``GRAFANA_SCREENSHOT_STABILIZE_ROUNDS`` 等键；Prometheus 无数据/报错的格子无法被脚本「画出曲线」。
"""

import base64
import copy
import hashlib
import json
import logging
import os
from urllib.parse import urlencode
from datetime import datetime
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
    "GRAFANA_BASE_URL": "https://grafana.client8.me",
    "GRAFANA_DASHBOARD_PATH": "/d/281e8816-ccb0-4335-922b-6b248491fd28/core-metrics-arms-aliyun",
    "GRAFANA_DASHBOARD_UID": "281e8816-ccb0-4335-922b-6b248491fd28",
    "GRAFANA_PANEL_TITLE": "请求总数/1m",
    "GRAFANA_DASHBOARD_FROM": "now-10m",
    "GRAFANA_DASHBOARD_TO": "now",
    "GRAFANA_QUERY_STEP": 60,
    "GRAFANA_QUERY_LOOKBACK_SECONDS": 600,
    # Prometheus 最近分钟桶常未跑完；query_range 的 end 用「现在 − 该秒数」，最新点落在「约前两分钟」
    "GRAFANA_QUERY_END_LAG_SECONDS": 120,
    # 无头截图（Playwright）：0=关；1=文字后发 PNG（需 ``pip install playwright`` + ``playwright install chromium``）
    "GRAFANA_SCREENSHOT_ENABLE": "1",
    "GRAFANA_SCREENSHOT_WIDTH": 1400,
    "GRAFANA_SCREENSHOT_HEIGHT": 1080,
    "GRAFANA_SCREENSHOT_TIMEOUT_MS": 90000,
    "GRAFANA_SCREENSHOT_FULL_PAGE": "1",
    # 截图前点 Grafana「Dock menu」收起左侧导航（Grafana 12 mega-menu）；0=跳过
    "GRAFANA_SCREENSHOT_DOCK_NAV": "1",
    # kiosk=tv 在部分 Grafana+无头环境下主区空白；默认不附带 kiosk（需旧行为可设 tv）
    "GRAFANA_SCREENSHOT_KIOSK": "",
    # 截图前先打开站点根路径再进 dashboard，利于 session 与 SPA bootstrap
    "GRAFANA_SCREENSHOT_BOOT_WARM": "1",
    # 1=尝试点 Grafana 时间栏「Refresh」触发拉数；找不到按钮则整页 reload 一次
    "GRAFANA_SCREENSHOT_REFRESH": "1",
    # Refresh 后等 Spinner 的最长毫秒（过大会拖慢整条截图）
    "GRAFANA_SCREENSHOT_POST_REFRESH_SPINNER_MS": 1600,
    # 1=点击折叠的 dashboard 行（如只显示 KPI 标题无图时）
    "GRAFANA_SCREENSHOT_EXPAND_ROWS": "1",
    # 截图 URL 用 now-10m / now（与浏览器一致）；0 则用 Prometheus 窗口的绝对毫秒时间戳
    "GRAFANA_SCREENSHOT_RELATIVE_RANGE": "1",
    # 等 #reactRoot 出现图表 DOM 的最长毫秒（过大会拖很久）
    "GRAFANA_SCREENSHOT_POPULATE_MAX_MS": 4500,
    # 整页截图稳定：默认 1 轮即可；仍无法保证 Prometheus「No data」有曲线
    "GRAFANA_SCREENSHOT_STABILIZE_ROUNDS": 1,
    "GRAFANA_SCREENSHOT_SCROLL_PAUSE_MS": 100,
    "GRAFANA_SCREENSHOT_SETTLE_MS": 300,
    "GRAFANA_SCREENSHOT_SPINNER_MAX_MS": 7000,
    # 至少等到 N 个 .react-grid-item（0=不等待；经典大屏可设 4–8；Scenes 布局可能为 0）
    "GRAFANA_SCREENSHOT_MIN_GRID_ITEMS": 0,
    "GRAFANA_USER": "om_duty",
    "GRAFANA_PASSWORD": "5tgb%TGB094",
    "VERIFICATION_TOKEN": "QlZMYp7rogAS914dxxMVNgboUKxQP7jc",
    "APP_ID": "cli_a97fcc6df7615ed1",
    "APP_SECRET": "NwAi6xJxMYDHMFAQcTG8ZfJxpeTOibvy",
    "MONITORING_TRIGGER": "/monitoring",
    # 1=仅 @ 机器人且无其它正文也触发（与 /monitoring 同）；1+下面 ANY=1 则 @ 且带任意文字也触发
    "MONITORING_AT_MENTION_ENABLE": "0",
    "MONITORING_AT_MENTION_ANY_TEXT": "0",
    # HTTP 均值告警命中时，除原群外再发一份文字到该群（chat_id，常为 oc_…）；空=关闭
    "MONITORING_ALERT_CHAT_ID": "",
    "LARK_ENCRYPT_KEY": "",
    "LARK_BOT_OPEN_ID": "",
    "LARK_WS_LOG_LEVEL": "INFO",
    "LARK_WS_USE_HTTP_KEYS": "0",
    "LARK_WS_EXTRA_IM_TYPES": "",
    # 1=同时订阅 im.message.receive_v2（易与 v1 对同一条消息各投递一次 → 两条回复）；默认 0
    "LARK_WS_REGISTER_IM_MESSAGE_V2": "0",
    # 同一 chat+发送者+触发正文在 N 秒内只跑一次监控任务；0=关闭（默认 5）
    "MONITORING_IM_DEBOUNCE_SECONDS": "5",
    # 同一会话在 N 秒内仅接受一次 monitoring 触发（在启动后台线程前兜底，拦同秒双 envelope）
    "MONITORING_CHAT_TRIGGER_DEBOUNCE_SECONDS": "0",
    # 同一触发在 N 秒内只允许 **一次** 真正发到飞书（拦双 POST / 双进程竞态）；0=关闭（默认 12）
    "MONITORING_SEND_COALESCE_SECONDS": "12",
    # 同一会话(chat_id/open_id)在 N 秒内只允许一次用户可见发送（兜底拦截同秒双 envelope）；0=关闭
    "MONITORING_CHAT_COALESCE_SECONDS": "0",
    # 1=且 LARK_EVENT_MODE=ws 时忽略 HTTP webhook 上的 im.message（避免与长连接重复处理）
    "LARK_HTTP_IGNORE_IM_WHEN_EVENT_MODE_WS": "1",
    # 1=当配置 ws 模式但尚未收到任何 WS DATA 帧时，允许 HTTP IM 回退处理（避免 200 但无回复）
    "LARK_HTTP_IM_FALLBACK_WHEN_WS_NO_DATA": "1",
    # 1=监控摘要用 **一条** 飞书交互卡片
    "MONITORING_MESSAGE_CARD_ENABLE": "1",
    # 1=把截图嵌进卡片；0=卡片仅文字，截图在下一条消息单独发送
    "MONITORING_CARD_EMBED_SCREENSHOT": "0",
    # 1=在监控卡片底部展示 callback 按钮（实现方式参考 Chatbox/jenkinsupdate 的 card JSON 2.0）
    "MONITORING_MESSAGE_CARD_BUTTON_ENABLE": "1",
    "MONITORING_MESSAGE_CARD_BUTTON_TEXT": "Resend screenshot",
    "LARK_WS_TRANSPORT_LOG": "1",
    "LARK_WS_BOOTSTRAP_FRAMES": 16,
    "LARK_WS_LOG_FRAME_METHOD": "0",
    "LARK_WS_SDK_DEBUG": "0",
    "LARK_WEBHOOK_WSGI_LOG": "0",
    "LARK_WEBHOOK_TIMING_LOG": "0",
    # 请求总数/1m：仅 HTTP 序列参与跌幅判断；平均跌幅 ≥ 该阈值时 @ 下面 open_id（可用环境变量覆盖）
    "TARGET_USER_OPEN_ID": "",
    "MONITORING_HTTP_DROP_ALERT_PCT": 10,
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


def _cfg_float(key: str, default: float) -> float:
    v = _cfg_raw(key)
    if v is None or (isinstance(v, str) and not str(v).strip()):
        return default
    try:
        return float(v)
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
_monitoring_im_trigger_last: Dict[str, float] = {}
_monitoring_chat_trigger_last: Dict[str, float] = {}
_monitoring_inflight_keys: set = set()
_processed_lark_im_event_ids: set = set()
_PROCESSED_IM_EVENT_IDS_CAP = 4000
_monitoring_user_reply_sent_at: Dict[str, float] = {}
_monitoring_user_send_in_progress: set = set()
_monitoring_chat_reply_sent_at: Dict[str, float] = {}
_monitoring_chat_send_in_progress: set = set()
_monitoring_card_action_event_ids: set = set()
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
GRAFANA_QUERY_END_LAG_SECONDS = _cfg_int("GRAFANA_QUERY_END_LAG_SECONDS", 120)
GRAFANA_SCREENSHOT_WIDTH = _cfg_int("GRAFANA_SCREENSHOT_WIDTH", 1400)
GRAFANA_SCREENSHOT_HEIGHT = _cfg_int("GRAFANA_SCREENSHOT_HEIGHT", 1080)
GRAFANA_SCREENSHOT_TIMEOUT_MS = _cfg_int("GRAFANA_SCREENSHOT_TIMEOUT_MS", 90000)
GRAFANA_SCREENSHOT_STABILIZE_ROUNDS = max(
    1, min(8, _cfg_int("GRAFANA_SCREENSHOT_STABILIZE_ROUNDS", 1))
)
GRAFANA_SCREENSHOT_SCROLL_PAUSE_MS = max(
    60, min(3000, _cfg_int("GRAFANA_SCREENSHOT_SCROLL_PAUSE_MS", 100))
)
GRAFANA_SCREENSHOT_SETTLE_MS = max(
    0, min(120_000, _cfg_int("GRAFANA_SCREENSHOT_SETTLE_MS", 300))
)
GRAFANA_SCREENSHOT_SPINNER_MAX_MS = max(
    2000, min(60_000, _cfg_int("GRAFANA_SCREENSHOT_SPINNER_MAX_MS", 7000))
)
GRAFANA_SCREENSHOT_POPULATE_MAX_MS = max(
    1500, min(90_000, _cfg_int("GRAFANA_SCREENSHOT_POPULATE_MAX_MS", 4500))
)
GRAFANA_SCREENSHOT_POST_REFRESH_SPINNER_MS = max(
    400, min(30_000, _cfg_int("GRAFANA_SCREENSHOT_POST_REFRESH_SPINNER_MS", 1600))
)
GRAFANA_SCREENSHOT_MIN_GRID_ITEMS = max(
    0, min(200, _cfg_int("GRAFANA_SCREENSHOT_MIN_GRID_ITEMS", 0))
)
GRAFANA_SCREENSHOT_KIOSK = _cfg_str("GRAFANA_SCREENSHOT_KIOSK", "").strip()
GRAFANA_SCREENSHOT_RELATIVE_RANGE = _lark_env_truthy("GRAFANA_SCREENSHOT_RELATIVE_RANGE")
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
TARGET_USER_OPEN_ID = _cfg_str(
    "TARGET_USER_OPEN_ID", "ou_d7bc33724e2d6ced4050c944c2ca5650"
).strip()
MONITORING_HTTP_DROP_ALERT_PCT = _cfg_float("MONITORING_HTTP_DROP_ALERT_PCT", 10.0)
LARK_ENCRYPT_KEY = (
    _cfg_str("LARK_ENCRYPT_KEY")
    or _cfg_str("ENCRYPT_KEY")
    or _cfg_str("FEISHU_ENCRYPT_KEY")
    or ""
).strip()
LARK_BOT_OPEN_ID = _cfg_str("LARK_BOT_OPEN_ID", "").strip()
MONITORING_AT_MENTION_ENABLE = _lark_env_truthy("MONITORING_AT_MENTION_ENABLE")
MONITORING_AT_MENTION_ANY_TEXT = _lark_env_truthy("MONITORING_AT_MENTION_ANY_TEXT")
MONITORING_ALERT_CHAT_ID = _cfg_str("MONITORING_ALERT_CHAT_ID", "").strip()
MONITORING_MESSAGE_CARD_ENABLE = _lark_env_truthy("MONITORING_MESSAGE_CARD_ENABLE")

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


def _lark_message_chat_id_aliases(msg: Dict[str, Any]) -> List[str]:
    """Collect all chat id aliases (``chat_id`` / ``open_chat_id`` from message and container)."""
    out: List[str] = []

    def _add(v: Any) -> None:
        s = (str(v).strip() if v is not None else "")
        if s and s not in out:
            out.append(s)

    if isinstance(msg, dict):
        for k in ("chat_id", "chatId", "open_chat_id", "openChatId"):
            _add(msg.get(k))
        c = msg.get("container")
        if isinstance(c, dict):
            for k in ("chat_id", "chatId", "open_chat_id", "openChatId"):
                _add(c.get(k))
    return out


def _lark_im_message_dedupe_id(msg: Dict[str, Any]) -> str:
    return _lark_dict_pick_str(
        msg, "message_id", "messageId", "open_message_id", "openMessageId"
    )


def _lark_im_payload_event_id(data: Dict[str, Any]) -> str:
    """Feishu may put ``event_id`` at top level, under ``header``, or under ``event`` depending on schema/version."""
    if not isinstance(data, dict):
        return ""
    top = _lark_dict_pick_str(data, "event_id", "eventId", "uuid")
    if top:
        return top
    h = data.get("header") if isinstance(data.get("header"), dict) else {}
    x = _lark_dict_pick_str(h, "event_id", "eventId", "event_uuid", "eventUuid", "uuid")
    if x:
        return x
    ev = data.get("event") if isinstance(data.get("event"), dict) else {}
    return _lark_dict_pick_str(ev, "event_id", "eventId")


def _lark_im_message_time_token(msg: Dict[str, Any]) -> str:
    return _lark_dict_pick_str(msg, "create_time", "createTime", "update_time", "updateTime")


def _monitoring_processed_stick(
    mid: str,
    im_event_id: str,
    chat_id: str,
    sender_debounce: str,
    msg_time: str,
) -> str:
    """Stable id for ``_processed_lark_message_ids`` when ``message_id`` is missing in one POST duplicate."""
    m = (mid or "").strip()
    if m:
        return m
    e = (im_event_id or "").strip()
    if e:
        return f"evt:{e}"
    if (msg_time or "").strip() and ((chat_id or "").strip() or (sender_debounce or "").strip()):
        return f"tm:{(chat_id or '').strip()}:{msg_time.strip()}:{sender_debounce}"
    return ""


def _monitoring_try_begin_user_send(dispatch_key: str) -> bool:
    """
    Serialize user-visible sends for the same ``dispatch_key`` (HTTP double-post / race).
    Returns False if another send is in progress or completed within the coalesce window.
    """
    dk = (dispatch_key or "").strip()
    if not dk:
        return True
    sec = _cfg_float("MONITORING_SEND_COALESCE_SECONDS", 12.0)
    if sec <= 0:
        return True
    now = time.monotonic()
    with _monitoring_reply_dispatch_lock:
        if dk in _monitoring_user_send_in_progress:
            return False
        prev = _monitoring_user_reply_sent_at.get(dk, 0.0)
        if prev > 0.0 and (now - prev) < sec:
            return False
        _monitoring_user_send_in_progress.add(dk)
        if len(_monitoring_user_reply_sent_at) > 800:
            for k, t1 in sorted(_monitoring_user_reply_sent_at.items(), key=lambda kv: kv[1])[:300]:
                try:
                    del _monitoring_user_reply_sent_at[k]
                except KeyError:
                    pass
    return True


def _monitoring_end_user_send(dispatch_key: str, success: bool) -> None:
    dk = (dispatch_key or "").strip()
    if not dk:
        return
    with _monitoring_reply_dispatch_lock:
        _monitoring_user_send_in_progress.discard(dk)
        if success:
            _monitoring_user_reply_sent_at[dk] = time.monotonic()


def _monitoring_try_begin_chat_send(chat_key: str) -> bool:
    """
    Coarse safety gate by conversation key (chat/open_id).
    This blocks envelope variants that accidentally bypass dispatch-key dedupe.
    """
    ck = (chat_key or "").strip()
    if not ck:
        return True
    sec = _cfg_float("MONITORING_CHAT_COALESCE_SECONDS", 10.0)
    if sec <= 0:
        return True
    now = time.monotonic()
    with _monitoring_reply_dispatch_lock:
        if ck in _monitoring_chat_send_in_progress:
            return False
        prev = _monitoring_chat_reply_sent_at.get(ck, 0.0)
        if prev > 0.0 and (now - prev) < sec:
            return False
        _monitoring_chat_send_in_progress.add(ck)
        if len(_monitoring_chat_reply_sent_at) > 800:
            for k, t1 in sorted(_monitoring_chat_reply_sent_at.items(), key=lambda kv: kv[1])[:300]:
                try:
                    del _monitoring_chat_reply_sent_at[k]
                except KeyError:
                    pass
    return True


def _monitoring_end_chat_send(chat_key: str, success: bool) -> None:
    ck = (chat_key or "").strip()
    if not ck:
        return
    with _monitoring_reply_dispatch_lock:
        _monitoring_chat_send_in_progress.discard(ck)
        if success:
            _monitoring_chat_reply_sent_at[ck] = time.monotonic()


def _lark_skip_http_im_message_when_ws_mode() -> bool:
    if not _lark_env_truthy("LARK_HTTP_IGNORE_IM_WHEN_EVENT_MODE_WS"):
        return False
    if _cfg_str("LARK_EVENT_MODE", "http").strip().lower() != "ws":
        return False
    # Fail-safe: if WS path is configured but we haven't seen any WS DATA frame yet,
    # do not drop HTTP IM events (otherwise webhook returns 200 but bot never replies).
    if _lark_env_truthy("LARK_HTTP_IM_FALLBACK_WHEN_WS_NO_DATA") and not _lark_ws_saw_data_frame:
        logger.warning(
            "ws mode configured but no WS DATA frame observed yet — allowing HTTP IM fallback "
            "(set LARK_HTTP_IM_FALLBACK_WHEN_WS_NO_DATA=0 to force skip)."
        )
        return False
    return True


def _lark_im_sender_debounce_token(sender: Dict[str, Any], open_id: str) -> str:
    u = _lark_dict_pick_str(sender, "union_id", "unionId")
    if u:
        return u
    o = (open_id or "").strip()
    if o:
        return o
    return _lark_dict_pick_str(sender, "user_id", "userId")


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


def _lark_message_mentions_bot(mentions: Any) -> bool:
    """True when the message ``mentions`` list includes this app bot (``LARK_BOT_OPEN_ID``)."""
    bot = (LARK_BOT_OPEN_ID or "").strip()
    if not bot or not isinstance(mentions, list):
        return False
    for m in mentions:
        if not isinstance(m, dict):
            continue
        ido = m.get("id")
        if isinstance(ido, str) and ido.strip() == bot:
            return True
        if isinstance(ido, dict):
            for k in ("open_id", "openId", "user_id", "userId", "union_id", "unionId"):
                v = ido.get(k)
                if v and str(v).strip() == bot:
                    return True
        for k in ("open_id", "openId", "user_id", "userId"):
            v = m.get(k)
            if v and str(v).strip() == bot:
                return True
    return False


def _text_should_run_monitoring(raw_text: str, clean: str, mentions: Any) -> bool:
    """
    Run the same job as ``/monitoring`` when the command is present, or when the user @mentions
    the bot (see ``MONITORING_AT_MENTION_ENABLE`` / ``MONITORING_AT_MENTION_ANY_TEXT``).
    """
    if _text_has_monitoring_trigger(raw_text, clean):
        return True
    if not MONITORING_AT_MENTION_ENABLE:
        return False
    if not _lark_message_mentions_bot(mentions):
        return False
    if MONITORING_AT_MENTION_ANY_TEXT:
        return True
    return not (clean or "").strip()


def _monitoring_dispatch_body_key(clean: str, raw_text: str, mentions: Any) -> str:
    """
    Normalize IM debounce key so ``/monitoring`` / @ 机器人 variants from different clients share one key
    (avoids two background workers when ``clean`` whitespace or mention markup differs slightly).
    """
    tri = (MONITORING_TRIGGER or "/monitoring").strip().lower()
    raw_l = (raw_text or "").lower()
    cl = re.sub(r"\s+", " ", (clean or "").strip().lower())
    if tri in raw_l or tri in cl:
        return tri
    if MONITORING_AT_MENTION_ENABLE and _lark_message_mentions_bot(mentions):
        if MONITORING_AT_MENTION_ANY_TEXT:
            return f"__at_any__:{cl[:240]}"
        if not cl.strip():
            return "__at_only__"
    return cl[:320] or "__body__"


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


def fetch_request_total_1m_series(session: Optional[requests.Session] = None) -> Dict[str, Any]:
    """
    Same data as Grafana panel「请求总数/1m」: last ``GRAFANA_QUERY_LOOKBACK_SECONDS`` (default **600 = 10m**),
    step ``GRAFANA_QUERY_STEP``. Align with dashboard ``GRAFANA_DASHBOARD_FROM`` / ``now-10m`` for screenshots.
    ``end`` = now − ``GRAFANA_QUERY_END_LAG_SECONDS`` (default 120) so the newest bucket is ~2 minutes old,
    avoiding incomplete recent-minute series that look like a false drop.
    Uses dashboard JSON + Prometheus query_range via Grafana proxy (not HTML scraping).
    """
    lag = max(0, int(GRAFANA_QUERY_END_LAG_SECONDS))
    end = int(time.time()) - lag
    start = end - GRAFANA_QUERY_LOOKBACK_SECONDS
    sess = session or grafana_login_session()
    dash = _fetch_dashboard_model(sess, GRAFANA_DASHBOARD_UID)
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
        raw = _prometheus_query_range(sess, ds_uid, expr, start, end, GRAFANA_QUERY_STEP)
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


def _lark_tenant_access_token_string() -> str:
    """Same tenant token as SDK; used for multipart image upload (``requests``)."""
    if not APP_ID or not APP_SECRET:
        raise ValueError("APP_ID and APP_SECRET required")
    url = f"{_lark_api_domain()}/open-apis/auth/v3/tenant_access_token/internal"
    r = requests.post(
        url,
        json={"app_id": str(APP_ID).strip(), "app_secret": str(APP_SECRET).strip()},
        headers={"Content-Type": "application/json; charset=utf-8"},
        timeout=30,
    )
    r.raise_for_status()
    j = r.json()
    if int(j.get("code", -1)) != 0:
        raise RuntimeError(f"tenant_token: {j}")
    tok = j.get("tenant_access_token")
    if not tok:
        raise RuntimeError(f"no tenant_access_token: {j}")
    return str(tok)


def _lark_upload_png_image_key(png: bytes) -> str:
    tok = _lark_tenant_access_token_string()
    url = f"{_lark_api_domain()}/open-apis/im/v1/images"
    r = requests.post(
        url,
        headers={"Authorization": f"Bearer {tok}"},
        files={"image": ("grafana.png", png, "image/png")},
        data={"image_type": "message"},
        timeout=120,
    )
    r.raise_for_status()
    j = r.json()
    if int(j.get("code", -1)) != 0:
        raise RuntimeError(f"image upload: {j}")
    key = (j.get("data") or {}).get("image_key")
    if not key:
        raise RuntimeError(f"no image_key: {j}")
    return str(key)


def _lark_send_image_message(receive_id_type: str, receive_id: str, image_key: str) -> None:
    from lark_oapi.api.im.v1.model.create_message_request import CreateMessageRequest
    from lark_oapi.api.im.v1.model.create_message_request_body import CreateMessageRequestBody

    client = _get_lark_oapi_client()
    body = (
        CreateMessageRequestBody.builder()
        .receive_id(receive_id)
        .msg_type("image")
        .content(json.dumps({"image_key": image_key}))
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
            f"Lark image send failed: code={resp.code!r} msg={resp.msg!r} log_id={resp.get_log_id()!r}"
        )


def _monitoring_reply_to_card_md(reply: str) -> str:
    s = (reply or "")[:3800]
    s = s.replace("```", "'''")
    if len(reply or "") > 3800:
        s += "\n\n…(truncated)"
    return s


def _monitoring_card_body_md_strip_title(reply: str) -> str:
    r = (reply or "").strip()
    dup = f"【{GRAFANA_PANEL_TITLE}】graph"
    if r.startswith(dup):
        r = r[len(dup) :].lstrip("\n")
    return _monitoring_reply_to_card_md(r)


def _monitoring_card_callback_payload_strings(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Match jenkinsupdate behavior: scalar callback values are stringified for client compatibility."""
    out: Dict[str, Any] = {}
    for k, v in payload.items():
        ks = str(k)
        if isinstance(v, (dict, list)):
            out[ks] = v
        elif v is None:
            out[ks] = ""
        else:
            out[ks] = str(v)
    return out


def _monitoring_card_v2_callback_button(
    label: str,
    btn_type: str,
    payload: Dict[str, Any],
    *,
    element_id: str = "mon_rfsh",
) -> Dict[str, Any]:
    btn: Dict[str, Any] = {
        "tag": "button",
        "text": {"tag": "plain_text", "content": label},
        "type": btn_type,
        "behaviors": [{"type": "callback", "value": _monitoring_card_callback_payload_strings(payload)}],
    }
    eid = (element_id or "").strip()[:20]
    if eid:
        btn["element_id"] = eid
    return btn


def _monitoring_interactive_card_dict(
    reply: str,
    receive_id_type: str,
    receive_id: str,
    lark_img_key: Optional[str] = None,
) -> Dict[str, Any]:
    """Feishu card JSON v2 — markdown card, optional embedded PNG."""
    title = f"📊 【{GRAFANA_PANEL_TITLE}】graph"
    elements: List[Dict[str, Any]] = [
        {"tag": "markdown", "content": _monitoring_card_body_md_strip_title(reply)},
    ]
    ik = (lark_img_key or "").strip()
    if ik:
        elements.append(
            {
                "tag": "img",
                "img_key": ik,
                "alt": {"tag": "plain_text", "content": "Grafana"},
            }
        )
    if _lark_env_truthy("MONITORING_MESSAGE_CARD_BUTTON_ENABLE"):
        cb_payload: Dict[str, Any] = {"k": "monitoring_btn", "v": "refresh"}
        rt = (receive_id_type or "").strip()
        rv = (receive_id or "").strip()
        if rt in ("chat_id", "open_id") and rv:
            cb_payload["rid_t"] = rt
            cb_payload["rid"] = rv
        elements.append(
            _monitoring_card_v2_callback_button(
                _cfg_str("MONITORING_MESSAGE_CARD_BUTTON_TEXT", "Resend screenshot")[:40],
                "primary",
                cb_payload,
                element_id="mon_rfsh",
            )
        )
    return {
        "schema": "2.0",
        "config": {"update_multi": True, "wide_screen_mode": True},
        "header": {
            "template": "blue",
            "title": {"tag": "plain_text", "content": title[:190]},
            "subtitle": {"tag": "plain_text", "content": "Grafana · monitoring"},
        },
        "body": {"elements": elements},
    }


def _lark_send_interactive_card(receive_id_type: str, receive_id: str, card: Dict[str, Any]) -> None:
    """Send ``msg_type=interactive`` via HTTP ``im/v1/messages`` (reliable JSON encoding)."""
    tok = _lark_tenant_access_token_string()
    url = f"{_lark_api_domain()}/open-apis/im/v1/messages"
    content_str = json.dumps(card, ensure_ascii=False)
    payload = {"receive_id": receive_id, "msg_type": "interactive", "content": content_str}
    r = requests.post(
        url,
        params={"receive_id_type": receive_id_type},
        headers={
            "Authorization": f"Bearer {tok}",
            "Content-Type": "application/json; charset=utf-8",
        },
        json=payload,
        timeout=60,
    )
    r.raise_for_status()
    j = r.json()
    if int(j.get("code", -1)) != 0:
        raise RuntimeError(f"im/v1/messages interactive failed: {j}")


def _lark_send_monitoring_user_message(
    receive_id_type: str,
    receive_id: str,
    reply: str,
    lark_img_key: Optional[str] = None,
) -> Tuple[bool, bool]:
    """
    Send monitoring summary to the user: interactive card (optional embedded PNG) or plain text.

    Returns ``(sent_interactive_card_ok, embedded_png_in_card)``.
    """
    rid = (receive_id or "").strip()
    if not rid:
        raise ValueError("empty receive_id for monitoring message")
    if MONITORING_MESSAGE_CARD_ENABLE:
        try:
            card = _monitoring_interactive_card_dict(reply, receive_id_type, rid, lark_img_key)
            _lark_send_interactive_card(receive_id_type, rid, card)
            return True, bool((lark_img_key or "").strip())
        except Exception as e:
            logger.warning(
                "monitoring interactive card failed (%s) — fallback to plain text; "
                "check app permission「发送消息卡片」.",
                e,
            )
    _lark_send_text(receive_id_type, rid, reply)
    return False, False


# Playwright ``wait_for_function`` / ``evaluate``: true when dashboard body looks mounted (not only header).
_GRAFANA_JS_REACTROOT_HAS_CHARTS = """() => {
  const rr = document.getElementById('reactRoot');
  if (!rr) return false;
  const n = (sel) => rr.querySelectorAll(sel).length;
  const grid = n('.react-grid-item');
  const uplot = n('[data-testid="uplot-main-div"]');
  const canv = n('canvas');
  const panels = n('[data-testid^="data-testid Panel"], [class*="PanelChrome"], [class*="panel-content"]');
  const main = document.querySelector('main');
  const mh = main ? main.getBoundingClientRect().height : 0;
  if (grid + uplot + canv >= 1) return true;
  if (panels >= 1 && mh > 140) return true;
  if (canv >= 1 && mh > 100) return true;
  return false;
}"""


def _grafana_playwright_dock_nav_only(page: Any, timeout_ms: int) -> None:
    """
    Grafana 12：左侧 mega-menu 展开时先点「Dock menu」(#dock-menu-button) 再拍主区；
    若未展开则先点 #mega-menu-toggle 再 Dock。仅负责点击，不负责等图（见 ``_grafana_wait_dashboard_ready``）。
    """
    if not _lark_env_truthy("GRAFANA_SCREENSHOT_DOCK_NAV"):
        return
    t = min(25000, max(5000, int(timeout_ms)))
    try:
        page.locator("#reactRoot").wait_for(state="visible", timeout=t)
    except Exception:
        pass

    def _try_dock() -> bool:
        loc = page.locator("#dock-menu-button")
        if loc.count() == 0:
            return False
        try:
            loc.first.click(timeout=2500)
            return True
        except Exception:
            return False

    try:
        if not _try_dock():
            mt = page.locator("#mega-menu-toggle")
            if mt.count() > 0:
                try:
                    mt.first.click(timeout=2000)
                    page.wait_for_timeout(350)
                except Exception:
                    pass
            _try_dock()
    except Exception as e:
        logger.info("Grafana screenshot: dock nav optional step failed: %s", e)

    page.wait_for_timeout(280)


def _grafana_expand_collapsed_dashboard_rows(page: Any) -> None:
    """
    Grafana dashboards often collapse row groups (only the row title e.g. ``KPI`` is visible).
    Click collapsed row toggles so panels mount and queries run.
    """
    if not _lark_env_truthy("GRAFANA_SCREENSHOT_EXPAND_ROWS"):
        return
    selectors = (
        '[data-testid="dashboard-row-title"] [aria-expanded="false"]',
        '[data-testid="dashboard-row-title"] button[aria-expanded="false"]',
        'section[data-testid="dashboard-row"] button[aria-expanded="false"]',
    )
    for sel in selectors:
        loc = page.locator(sel)
        try:
            n = loc.count()
        except Exception:
            continue
        if n == 0:
            continue
        clicked = 0
        for i in range(min(int(n), 14)):
            try:
                loc.nth(i).click(timeout=900)
                clicked += 1
                page.wait_for_timeout(40)
            except Exception:
                pass
        if clicked:
            logger.info(
                "Grafana screenshot: expanded %s collapsed dashboard row(s) via %r",
                clicked,
                sel,
            )
            page.wait_for_timeout(180)
        return


def _grafana_close_open_menus(page: Any) -> None:
    """Escape stray overlays (e.g. auto-refresh interval picker opened by a mis-click)."""
    try:
        page.keyboard.press("Escape")
        page.wait_for_timeout(100)
    except Exception:
        pass


def _grafana_click_dashboard_refresh(
    page: Any, timeout_ms: int, spinner_budget_ms: Optional[int] = None
) -> None:
    """
    Run **Refresh dashboard** (re-query). Order matters: Grafana often exposes a **refresh interval**
    control whose name also contains \"Refresh\" — clicking it only opens the **5s/10s/off** menu and
    **does not** load panels (blank main area + open dropdown in screenshots).
    """
    if not _lark_env_truthy("GRAFANA_SCREENSHOT_REFRESH"):
        return
    _grafana_close_open_menus(page)
    tclick = min(3500, max(1200, int(timeout_ms) // 35))
    spin_cap = (
        int(spinner_budget_ms)
        if spinner_budget_ms is not None
        else int(GRAFANA_SCREENSHOT_POST_REFRESH_SPINNER_MS)
    )
    spin_cap = max(600, min(25_000, spin_cap))
    # Exact \"Refresh dashboard\" first; avoid broad ``aria-label*=\"Refresh\"`` (interval picker).
    locators: List[Any] = [
        page.locator('button[aria-label="Refresh dashboard"]').first,
        page.locator('[aria-label="Refresh dashboard"]').first,
        page.get_by_role("button", name=re.compile(r"refresh\s+dashboard", re.I)).first,
        page.locator('[data-testid="refresh-dashboard-button"]').first,
        page.locator('[data-testid*="RefreshPicker"][data-testid*="run"]').first,
        page.locator('[data-testid*="refresh"][data-testid*="Run"]').first,
        page.get_by_role("button", name=re.compile(r"^run query$", re.I)).first,
    ]
    for idx, loc in enumerate(locators):
        try:
            if loc.count() == 0:
                continue
        except Exception:
            continue
        try:
            loc.click(timeout=tclick)
            logger.info("Grafana screenshot: clicked Refresh/run control (locator #%s)", idx)
            page.wait_for_timeout(220)
            _grafana_close_open_menus(page)
            _grafana_wait_loading_like_gone(page, spin_cap)
            return
        except Exception:
            _grafana_close_open_menus(page)
            continue
    try:
        logger.info("Grafana screenshot: no explicit Refresh control — using full page reload instead")
        page.reload(wait_until="load", timeout=timeout_ms)
        page.wait_for_timeout(380)
    except Exception as e:
        logger.info("Grafana screenshot: refresh fallback reload failed: %s", e)


def _grafana_loading_like_count(page: Any) -> int:
    """Rough count of visible Grafana-style loading elements (deduped by element)."""
    try:
        return int(
            page.evaluate(
                """() => {
                  const q = [
                    '[data-testid="Spinner"]',
                    '[data-testid="data-testid Panel loading bar"]',
                    '.panel-loading',
                    '[class*="PanelLoader"]',
                    '[class*="panel-loading"]',
                    '.fa-spin',
                    '.gf-spin',
                  ];
                  const seen = new Set();
                  for (const s of q) {
                    document.querySelectorAll(s).forEach((el) => {
                      const r = el.getBoundingClientRect();
                      const st = window.getComputedStyle(el);
                      if (r.width < 2 || r.height < 2) return;
                      if (st && st.visibility === "hidden") return;
                      if (st && st.display === "none") return;
                      seen.add(el);
                    });
                  }
                  return seen.size;
                }"""
            )
            or 0
        )
    except Exception:
        return 0


def _grafana_wait_loading_like_gone(page: Any, budget_ms: int) -> None:
    """Poll until loading-like elements stay at 0 for a few ticks (queries + canvas paint)."""
    if budget_ms <= 0:
        return
    deadline = time.monotonic() + budget_ms / 1000.0
    stable = 0
    last_c = -1
    while time.monotonic() < deadline:
        c = _grafana_loading_like_count(page)
        if c != last_c:
            logger.debug("Grafana screenshot: loading-like count=%s", c)
            last_c = c
        if c == 0:
            stable += 1
            if stable >= 2:
                return
        else:
            stable = 0
        page.wait_for_timeout(160)
    c = _grafana_loading_like_count(page)
    if c > 0:
        logger.warning(
            "Grafana screenshot: after %sms loading-like elements still present (count≈%s) — capture may be partial",
            budget_ms,
            c,
        )


def _grafana_wait_min_react_grid_items(page: Any, min_items: int, budget_ms: int) -> None:
    """Classic dashboards use ``.react-grid-item``; scenes may skip (set MIN_GRID_ITEMS=0)."""
    if min_items <= 0 or budget_ms <= 0:
        return
    deadline = time.monotonic() + budget_ms / 1000.0
    while time.monotonic() < deadline:
        try:
            n = page.locator(".react-grid-item").count()
            if n >= min_items:
                logger.info("Grafana screenshot: react-grid-item count=%s (>= %s)", n, min_items)
                return
        except Exception:
            pass
        page.wait_for_timeout(280)
    try:
        n = page.locator(".react-grid-item").count()
    except Exception:
        n = -1
    logger.warning(
        "Grafana screenshot: react-grid-item count=%s did not reach %s within %sms",
        n,
        min_items,
        budget_ms,
    )


def _grafana_scroll_paint_lazy_panels(page: Any) -> None:
    """Scroll by ~viewport steps so off-screen panels mount and uPlot/canvas paint."""
    pause = int(GRAFANA_SCREENSHOT_SCROLL_PAUSE_MS)
    try:
        vh = int(page.evaluate("() => window.innerHeight || 900") or 900)
        vh = max(400, min(vh, 2400))
        step = max(220, int(vh * 0.72))
        h = page.evaluate(
            "() => Math.max(document.body.scrollHeight, (document.scrollingElement || document.documentElement).scrollHeight)"
        )
        h = int(h or 0)
        h = min(max(h, 800), 48000)
        y = 0
        while y <= h:
            page.evaluate("(yy) => window.scrollTo(0, yy)", y)
            page.wait_for_timeout(pause)
            y += step
        page.evaluate("() => window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(pause)
        page.evaluate("() => window.scrollTo(0, 0)")
        page.wait_for_timeout(max(90, int(pause * 0.45)))
    except Exception as e:
        logger.info("Grafana screenshot: scroll paint step skipped: %s", e)


def _grafana_stabilize_dashboard_render(
    page: Any, timeout_ms: int, rounds: Optional[int] = None
) -> None:
    """
    Multiple scroll passes + spinner polling so lower panels finish Prometheus queries before PNG.
    ``rounds`` overrides ``GRAFANA_SCREENSHOT_STABILIZE_ROUNDS`` (e.g. ``1`` on reload retry).
    """
    r = GRAFANA_SCREENSHOT_STABILIZE_ROUNDS if rounds is None else max(1, min(8, int(rounds)))
    sm = int(GRAFANA_SCREENSHOT_SPINNER_MAX_MS)
    per_round = max(600, min(3200, int(sm * 0.36)))
    final_spin = max(800, min(4000, int(sm * 0.5)))

    if GRAFANA_SCREENSHOT_MIN_GRID_ITEMS > 0:
        _grafana_wait_min_react_grid_items(
            page,
            GRAFANA_SCREENSHOT_MIN_GRID_ITEMS,
            min(12000, max(4000, timeout_ms // 3)),
        )

    for rnd in range(r):
        logger.info("Grafana screenshot: stabilize round %s/%s", rnd + 1, r)
        _grafana_scroll_paint_lazy_panels(page)
        _grafana_wait_loading_like_gone(page, per_round)

    _grafana_scroll_paint_lazy_panels(page)
    _grafana_wait_loading_like_gone(page, final_spin)


def _grafana_dashboard_has_visual_content(page: Any) -> bool:
    """True when #reactRoot looks like a loaded dashboard (see ``_GRAFANA_JS_REACTROOT_HAS_CHARTS``)."""
    try:
        return bool(page.evaluate(_GRAFANA_JS_REACTROOT_HAS_CHARTS))
    except Exception:
        return False


def _grafana_wait_dashboard_body_populated(page: Any, budget_ms: int) -> bool:
    """Short wait_for_function — budget capped by ``GRAFANA_SCREENSHOT_POPULATE_MAX_MS`` style callers."""
    b = max(1000, min(int(GRAFANA_SCREENSHOT_POPULATE_MAX_MS), int(budget_ms)))
    try:
        page.wait_for_function(_GRAFANA_JS_REACTROOT_HAS_CHARTS, timeout=b)
        logger.info("Grafana screenshot: reactRoot looks populated")
        return True
    except Exception as e:
        logger.warning(
            "Grafana screenshot: populate wait stopped after %sms: %s",
            b,
            e,
        )
        return False


def _grafana_build_screenshot_dashboard_url(start_unix: int, end_unix: int) -> str:
    params: List[Tuple[str, str]] = [("orgId", "1")]
    if GRAFANA_SCREENSHOT_RELATIVE_RANGE:
        params.extend(
            [
                ("from", (GRAFANA_DASHBOARD_FROM or "now-10m").strip()),
                ("to", (GRAFANA_DASHBOARD_TO or "now").strip()),
            ]
        )
    else:
        params.extend(
            [
                ("from", str(int(start_unix) * 1000)),
                ("to", str(int(end_unix) * 1000)),
            ]
        )
    k = (GRAFANA_SCREENSHOT_KIOSK or "").strip().lower()
    if k and k not in ("0", "false", "no", "off"):
        if k in ("1", "true", "yes", "on"):
            params.append(("kiosk", "1"))
        else:
            params.append(("kiosk", k))
    q = urlencode(params)
    return f"{GRAFANA_BASE_URL}{GRAFANA_DASHBOARD_PATH}?{q}"


def _grafana_wait_dashboard_ready(page: Any, timeout_ms: int) -> None:
    """
    SPA 在 ``domcontentloaded`` 时往往还没有 panel；此处在 ``load`` 之后仍要等网格/画布出现。
    与 ``GRAFANA_SCREENSHOT_DOCK_NAV`` 无关：关 dock 时也必须执行，否则截到空白主区。
    """
    t = min(14000, max(4000, int(timeout_ms) // 3))
    try:
        page.locator("#reactRoot").wait_for(state="visible", timeout=min(9000, t))
    except Exception:
        pass

    selectors = (
        '[data-testid="uplot-main-div"]',
        ".react-grid-item",
        '[data-testid="dashboard-panel-content"]',
        '[data-testid="panel-content"]',
        "main canvas",
        '[class*="PanelChrome"]',
    )
    matched: Optional[str] = None
    slot = min(5000, max(1600, t // 2))
    for sel in selectors:
        try:
            page.locator(sel).first.wait_for(state="visible", timeout=slot)
            matched = sel
            break
        except Exception:
            continue

    if not matched:
        try:
            safe_title = (GRAFANA_PANEL_TITLE or "").replace('"', '\\"')
            if safe_title:
                page.locator(f'h2[title="{safe_title}"]').first.wait_for(
                    state="visible", timeout=slot
                )
                matched = f'h2[title="{safe_title}"]'
        except Exception:
            logger.warning(
                "Grafana screenshot: no known panel/grid selector matched — continuing "
                "(selectors tried: %s; panel title: %r)",
                selectors,
                GRAFANA_PANEL_TITLE,
            )
    else:
        logger.info("Grafana screenshot: dashboard content wait matched %r", matched)

    page.wait_for_timeout(320)


def _playwright_cookie_list(session: requests.Session) -> List[Dict[str, Any]]:
    """
    Use per-cookie ``url`` (Grafana origin) so ``add_cookies`` matches Playwright rules;
    ``domain``+``path`` alone often fails on Linux headless before first navigation.
    """
    base = str(GRAFANA_BASE_URL).rstrip("/")
    out: List[Dict[str, Any]] = []
    for c in session.cookies:
        out.append({"name": c.name, "value": c.value, "url": base})
    return out


def _grafana_headless_screenshot_png(session: requests.Session, start_unix: int, end_unix: int) -> bytes:
    """
    Headless Chromium (Playwright) opens the same dashboard URL as the UI, with session cookies.
    Requires: ``pip install playwright`` and ``playwright install chromium`` on the server.

    ``GRAFANA_SCREENSHOT_FULL_PAGE=1`` (default): ``page.screenshot(full_page=True)`` — full scroll height
    so KPI rows below the fold are included. ``0`` captures only the viewport (``WIDTH``×``HEIGHT``).

    Defaults favor **low latency** (short sleeps, tight spinner/populate caps). If captures go blank,
    raise ``GRAFANA_SCREENSHOT_POPULATE_MAX_MS`` and ``GRAFANA_SCREENSHOT_POST_REFRESH_SPINNER_MS`` first.
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as e:
        raise ImportError(
            "Playwright not installed — pip install playwright && playwright install chromium"
        ) from e

    url = _grafana_build_screenshot_dashboard_url(start_unix, end_unix)
    cookies = _playwright_cookie_list(session)
    timeout_ms = max(5000, int(GRAFANA_SCREENSHOT_TIMEOUT_MS))
    full_page = _lark_env_truthy("GRAFANA_SCREENSHOT_FULL_PAGE")
    logger.info(
        "Grafana screenshot: relative_range=%s url=%s",
        GRAFANA_SCREENSHOT_RELATIVE_RANGE,
        url[:300] + ("…" if len(url) > 300 else ""),
    )

    with sync_playwright() as p:
        # Linux / systemd 常见：缺 sandbox 时 Chromium 起不来；Docker 也常用这几项
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
            ],
        )
        try:
            context = browser.new_context(
                viewport={
                    "width": max(400, int(GRAFANA_SCREENSHOT_WIDTH)),
                    "height": max(300, int(GRAFANA_SCREENSHOT_HEIGHT)),
                },
                user_agent=(
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
                ),
                locale="en-US",
            )
            if cookies:
                context.add_cookies(cookies)
            page = context.new_page()
            try:
                page.add_init_script(
                    "try{Object.defineProperty(navigator,'webdriver',{get:()=>undefined});}catch(e){}"
                )
            except Exception:
                pass

            base = str(GRAFANA_BASE_URL).rstrip("/")
            if _lark_env_truthy("GRAFANA_SCREENSHOT_BOOT_WARM"):
                page.goto(f"{base}/", wait_until="domcontentloaded", timeout=min(20000, timeout_ms))
                page.wait_for_timeout(220)

            # Grafana 为 SPA：先根路径再 dashboard；相对时间 URL 与浏览器一致
            page.goto(url, wait_until="load", timeout=timeout_ms)
            page.wait_for_timeout(200)
            _grafana_playwright_dock_nav_only(page, timeout_ms)
            _grafana_click_dashboard_refresh(page, timeout_ms)
            _grafana_expand_collapsed_dashboard_rows(page)
            _grafana_wait_dashboard_ready(page, timeout_ms)
            _grafana_wait_dashboard_body_populated(page, int(GRAFANA_SCREENSHOT_POPULATE_MAX_MS))
            _grafana_stabilize_dashboard_render(page, timeout_ms)
            page.wait_for_timeout(180)

            if not _grafana_dashboard_has_visual_content(page):
                _grafana_expand_collapsed_dashboard_rows(page)
                _grafana_click_dashboard_refresh(
                    page,
                    timeout_ms,
                    spinner_budget_ms=min(1400, int(GRAFANA_SCREENSHOT_POST_REFRESH_SPINNER_MS)),
                )
                _grafana_wait_dashboard_body_populated(
                    page, min(3200, int(GRAFANA_SCREENSHOT_POPULATE_MAX_MS))
                )
                _grafana_scroll_paint_lazy_panels(page)
                page.wait_for_timeout(120)

            if not _grafana_dashboard_has_visual_content(page):
                logger.warning(
                    "Grafana screenshot: chart DOM not detected — reload once (kiosk=%r)",
                    GRAFANA_SCREENSHOT_KIOSK or "(off)",
                )
                try:
                    page.reload(wait_until="load", timeout=timeout_ms)
                    page.wait_for_timeout(450)
                    _grafana_playwright_dock_nav_only(page, timeout_ms)
                    _grafana_click_dashboard_refresh(page, timeout_ms)
                    _grafana_expand_collapsed_dashboard_rows(page)
                    _grafana_wait_dashboard_ready(page, min(20000, timeout_ms // 2))
                    _grafana_wait_dashboard_body_populated(
                        page, min(8000, int(GRAFANA_SCREENSHOT_POPULATE_MAX_MS))
                    )
                    _grafana_stabilize_dashboard_render(page, timeout_ms, rounds=1)
                except Exception as e:
                    logger.warning("Grafana screenshot: reload retry failed: %s", e)

            if not _grafana_dashboard_has_visual_content(page):
                logger.error(
                    "Grafana screenshot: still no chart-like DOM — PNG may be blank "
                    "(session cookie / GRAFANA_DASHBOARD_PATH / try GRAFANA_SCREENSHOT_RELATIVE_RANGE=0 "
                    "or GRAFANA_SCREENSHOT_KIOSK=tv)."
                )

            if GRAFANA_SCREENSHOT_SETTLE_MS > 0:
                page.wait_for_timeout(int(GRAFANA_SCREENSHOT_SETTLE_MS))
            _grafana_close_open_menus(page)
            png = page.screenshot(type="png", full_page=full_page)
            return png
        finally:
            browser.close()


def _metric_series_is_http_leg(metric: Dict[str, Any]) -> bool:
    """Pick Prometheus rows that correspond to the HTTP series (legend ``http`` / label value ``http``)."""
    for k, v in metric.items():
        if k == "__name__":
            continue
        if str(v).strip().lower() == "http":
            return True
    return False


def _compact_http_legend(metric: Dict[str, Any], ref: str) -> str:
    """
    Prefer a ``callType=http``-style token when a label value is ``http``,
    but **append other labels** so multiple http streams (different instance/job/…)
    do not look like duplicate lines with mysteriously different values.
    """
    http_pair: Optional[str] = None
    other_bits: List[str] = []
    for k, v in sorted(metric.items()):
        if k == "__name__":
            continue
        if str(v).strip().lower() == "http":
            if http_pair is None:
                http_pair = f"{k}=http"
        else:
            other_bits.append(f"{k}={v}")
    if http_pair:
        if not other_bits:
            return http_pair
        tail = ", ".join(other_bits[:5])
        if len(other_bits) > 5:
            tail += ", …"
        return f"{http_pair} | {tail}"
    bits = [f"{k}={v}" for k, v in sorted(metric.items()) if k != "__name__"]
    return ", ".join(bits[:4]) or str(metric.get("__name__", ref))


def _merge_http_timeseries_points(payload: Dict[str, Any]) -> List[Tuple[float, float]]:
    """Sum all HTTP-leg series per Unix timestamp (ascending)."""
    by_ts: Dict[float, float] = {}
    for s in payload.get("series") or []:
        prom = s.get("prometheus") or {}
        pdata = prom.get("data") or {}
        for r in pdata.get("result") or []:
            m = r.get("metric") or {}
            if not _metric_series_is_http_leg(m):
                continue
            for pair in r.get("values") or []:
                if len(pair) < 2:
                    continue
                try:
                    ts = float(pair[0])
                    val = float(pair[1])
                except (TypeError, ValueError):
                    continue
                by_ts[ts] = by_ts.get(ts, 0.0) + val
    if not by_ts:
        return []
    return sorted(by_ts.items(), key=lambda x: x[0])


def _best_consecutive_drop_run(vals: List[float], ts: List[float]) -> Optional[Dict[str, Any]]:
    """
    Longest weakly-decreasing runs (each step ``vals[k+1] <= vals[k]``); score each run by
    ``(start - end) / start * 100`` over the whole span (not single-minute deltas).
    Returns the run with the largest such percentage (tie: more buckets wins).
    """
    L = len(vals)
    if L < 2:
        return None
    best: Optional[Dict[str, Any]] = None
    i = 0
    while i < L:
        j = i
        while j + 1 < L and vals[j + 1] <= vals[j]:
            j += 1
        if j > i and vals[i] > 0 and vals[j] < vals[i]:
            pct = (vals[i] - vals[j]) / vals[i] * 100.0
            buckets = j - i + 1
            cand = {"pct": round(pct, 2), "from_ts": ts[i], "to_ts": ts[j], "buckets": buckets}
            if best is None or pct > float(best["pct"]) or (
                pct == float(best["pct"]) and buckets > int(best["buckets"])
            ):
                best = cand
        i = j + 1
    return best


def _best_consecutive_spike_run(vals: List[float], ts: List[float]) -> Optional[Dict[str, Any]]:
    """Weakly-increasing runs; score ``(end - start) / start * 100`` over the span."""
    L = len(vals)
    if L < 2:
        return None
    best: Optional[Dict[str, Any]] = None
    i = 0
    while i < L:
        j = i
        while j + 1 < L and vals[j + 1] >= vals[j]:
            j += 1
        if j > i and vals[i] > 0 and vals[j] > vals[i]:
            pct = (vals[j] - vals[i]) / vals[i] * 100.0
            buckets = j - i + 1
            cand = {"pct": round(pct, 2), "from_ts": ts[i], "to_ts": ts[j], "buckets": buckets}
            if best is None or pct > float(best["pct"]) or (
                pct == float(best["pct"]) and buckets > int(best["buckets"])
            ):
                best = cand
        i = j + 1
    return best


def _http_drop_spike_analysis(
    points: List[Tuple[float, float]], alert_threshold_pct: float
) -> Dict[str, Any]:
    """
    For N=1..10 minutes (N buckets at panel step): compare mean(previous N) vs mean(last N).
    ``hit_alert`` if any N has average drop ≥ ``alert_threshold_pct``.

    Max drop / max spike use **whole consecutive** weakly monotonic segments (multi-minute),
    scored from first to last bucket of each segment — not single adjacent-minute comparisons.
    """
    out: Dict[str, Any] = {
        "pointCount": len(points),
        "hit_alert": False,
        "alert_threshold_pct": alert_threshold_pct,
        "windows": [],
        "worst_avg_drop_window": None,
        "consecutive_max_drop": None,
        "consecutive_max_spike": None,
    }
    if len(points) < 2:
        return out

    vals = [p[1] for p in points]
    ts = [p[0] for p in points]
    L = len(points)

    drop_run = _best_consecutive_drop_run(vals, ts)
    if drop_run is not None:
        out["consecutive_max_drop"] = {
            "pct": drop_run["pct"],
            "from_ts": drop_run["from_ts"],
            "to_ts": drop_run["to_ts"],
            "buckets": drop_run["buckets"],
        }
    spike_run = _best_consecutive_spike_run(vals, ts)
    if spike_run is not None:
        out["consecutive_max_spike"] = {
            "pct": spike_run["pct"],
            "from_ts": spike_run["from_ts"],
            "to_ts": spike_run["to_ts"],
            "buckets": spike_run["buckets"],
        }

    worst_avg_drop_pct: Optional[float] = None
    windows: List[Dict[str, Any]] = []

    for n in range(1, 11):
        if L < 2 * n:
            continue
        pre = vals[L - 2 * n : L - n]
        cur = vals[L - n : L]
        avg_pre = sum(pre) / n
        avg_cur = sum(cur) / n
        if avg_pre <= 0:
            continue
        drop_pct = (avg_pre - avg_cur) / avg_pre * 100.0
        row = {
            "n_minutes": n,
            "avg_drop_pct": round(drop_pct, 2),
            "avg_baseline": avg_pre,
            "avg_recent": avg_cur,
            "baseline_from_ts": ts[L - 2 * n],
            "baseline_to_ts": ts[L - n - 1],
            "recent_from_ts": ts[L - n],
            "recent_to_ts": ts[L - 1],
        }
        windows.append(row)
        if drop_pct >= alert_threshold_pct:
            out["hit_alert"] = True
        if worst_avg_drop_pct is None or drop_pct > worst_avg_drop_pct:
            worst_avg_drop_pct = drop_pct
            out["worst_avg_drop_window"] = dict(row)

    out["windows"] = windows
    return out


def _http_analysis_for_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    pts = _merge_http_timeseries_points(payload)
    a = _http_drop_spike_analysis(pts, MONITORING_HTTP_DROP_ALERT_PCT)
    a["point_count"] = len(pts)
    a["merged_points"] = [[t, v] for t, v in pts]
    return a


def _fmt_ts_short(ts: Any) -> str:
    try:
        return datetime.fromtimestamp(int(float(ts))).strftime("%m-%d %H:%M")
    except (TypeError, ValueError, OSError):
        return str(ts)


def _fmt_num(v: Any) -> str:
    try:
        f = float(v)
        if abs(f - round(f)) < 1e-6:
            return f"{int(round(f)):,}"
        return f"{f:,.2f}"
    except (TypeError, ValueError):
        return str(v)


def _format_http_analysis_lines(analysis: Dict[str, Any]) -> List[str]:
    """
    Compact footer: max drop/spike from best consecutive monotonic run (first→last bucket %).
    Threshold line matches product copy; @mention is still driven by ``hit_alert`` (mean windows).
    """
    thr = float(analysis.get("alert_threshold_pct") or MONITORING_HTTP_DROP_ALERT_PCT)
    lines: List[str] = [
        "",
        f"【HTTP only 】ALERT will trigger when drop/spike >= {thr:g}%",
    ]

    cd = analysis.get("consecutive_max_drop") or analysis.get("adjacent_max_drop")
    cs = analysis.get("consecutive_max_spike") or analysis.get("adjacent_max_spike")
    if isinstance(cd, dict) and cd.get("from_ts") is not None:
        p = cd.get("pct")
        lines.append(
            f"- max drop : {_fmt_ts_short(cd['from_ts'])} → {_fmt_ts_short(cd['to_ts'])}  -{p}%"
        )
    else:
        lines.append("- max drop : n/a")

    if isinstance(cs, dict) and cs.get("from_ts") is not None:
        p = cs.get("pct")
        lines.append(f"- max spike: {_fmt_ts_short(cs['from_ts'])} → {_fmt_ts_short(cs['to_ts'])}  +{p}%")
    else:
        lines.append("- max spike: n/a")

    return lines


def _format_monitoring_reply(payload: Dict[str, Any]) -> str:
    """
    Lark-friendly compact layout: ``【panel】graph`` + short ``Dashboard: …/d/{uid}`` + HTTP table + footer.
    """
    max_rows = 11
    uid = str(payload.get("dashboardUid") or GRAFANA_DASHBOARD_UID)
    base = str(GRAFANA_BASE_URL).rstrip("/")
    http_ex = _http_analysis_for_payload(payload)

    lines: List[str] = [
        f"【{payload.get('panelTitle')}】graph",
        f"Dashboard: {base}/d/{uid}",
    ]
    for s in payload.get("series") or []:
        prom = s.get("prometheus") or {}
        pdata = prom.get("data") or {}
        results = pdata.get("result") or []
        ref = s.get("refId") or "?"
        if not results:
            lines.append(f"- [{ref}] no data")
            continue
        http_results = [
            r for r in results[:24] if _metric_series_is_http_leg(r.get("metric") or {})
        ]
        if not http_results:
            lines.append(f"- [{ref}] no http-labeled series (skipped {len(results)} rows)")
            continue
        for r in http_results[:12]:
            m = r.get("metric") or {}
            legend = _compact_http_legend(m, str(ref))
            vals = r.get("values") or []
            if not vals:
                lines.append(f"- {legend}\n  (empty)")
                continue
            lines.append(f"- [{ref}] {legend}")
            tail = vals[-max_rows:]
            lines.append("  time          value")
            for pair in tail:
                lines.append(f"  {_fmt_ts_short(pair[0]):<14} {_fmt_num(pair[1])}")

    if http_ex.get("hit_alert") and TARGET_USER_OPEN_ID:
        lines.append(f'<at user_id="{TARGET_USER_OPEN_ID}"> </at>')
    lines.extend(_format_http_analysis_lines(http_ex))

    out = "\n".join(lines)
    if len(out) > 4500:
        out = out[:4490] + "\n…(truncated)"
    return out


def _lark_verify_event_token(data: Dict[str, Any]) -> bool:
    """True when ``_lark_extract_verification_token`` matches ``VERIFICATION_TOKEN`` (Chatbox pattern)."""
    if not VERIFICATION_TOKEN:
        return True
    et = _lark_header_event_type(data)
    if isinstance(et, str) and et.startswith("card.action"):
        raw_tok = data.get("token")
        if _lark_looks_like_lark_card_update_credential(raw_tok):
            # Card callback webhooks may use c-/d- credential token instead of app verification token.
            return True
    tok = _lark_extract_verification_token(data)
    return tok == VERIFICATION_TOKEN


def _lark_card_action_value(data: Dict[str, Any]) -> Dict[str, Any]:
    ev = data.get("event") if isinstance(data.get("event"), dict) else {}
    act = ev.get("action")
    if isinstance(act, dict):
        val = act.get("value")
        if isinstance(val, dict):
            return val
    val2 = ev.get("value")
    if isinstance(val2, dict):
        return val2
    return {}


def _lark_card_action_target_ids(data: Dict[str, Any]) -> Tuple[str, str]:
    ev = data.get("event") if isinstance(data.get("event"), dict) else {}
    chat_id = _lark_dict_pick_str(ev, "open_chat_id", "openChatId", "chat_id", "chatId")
    op = ev.get("operator") if isinstance(ev.get("operator"), dict) else {}
    op_id = op.get("operator_id") if isinstance(op.get("operator_id"), dict) else {}
    open_id = _lark_dict_pick_str(op_id, "open_id", "openId", "user_id", "userId")
    if not open_id:
        open_id = _lark_dict_pick_str(op, "open_id", "openId", "user_id", "userId")
    return chat_id, open_id


def _monitoring_send_screenshot_on_card_click(chat_id: str, open_id: str) -> None:
    try:
        if not _lark_env_truthy("GRAFANA_SCREENSHOT_ENABLE"):
            raise RuntimeError("GRAFANA_SCREENSHOT_ENABLE=0")
        sess = grafana_login_session()
        payload = fetch_request_total_1m_series(session=sess)
        w = payload.get("window") or {}
        su = int(w.get("startUnix") or 0)
        eu = int(w.get("endUnix") or 0)
        if su <= 0 or eu <= 0:
            raise RuntimeError(f"invalid screenshot window start={su} end={eu}")
        png = _grafana_headless_screenshot_png(sess, su, eu)
        key = _lark_upload_png_image_key(png)
        if (chat_id or "").strip():
            _lark_send_image_message("chat_id", chat_id.strip(), key)
        elif (open_id or "").strip():
            _lark_send_image_message("open_id", open_id.strip(), key)
        else:
            raise RuntimeError("missing chat_id/open_id")
        logger.info(
            "monitoring card-action screenshot sent chat=%r open=%r bytes=%s",
            bool(chat_id),
            bool(open_id),
            len(png),
        )
    except Exception as e:
        logger.exception("monitoring card-action screenshot failed")
        msg = f"刷新截图失败：{e}"
        try:
            if (chat_id or "").strip():
                _lark_send_text("chat_id", chat_id.strip(), msg)
            elif (open_id or "").strip():
                _lark_send_text("open_id", open_id.strip(), msg)
        except Exception:
            logger.exception("monitoring card-action error text send failed")


def _handle_monitoring_card_action(data: Dict[str, Any]) -> None:
    val = _lark_card_action_value(data)
    k = _lark_dict_pick_str(val, "k")
    v = _lark_dict_pick_str(val, "v")
    if not (k == "monitoring_btn" and v == "refresh"):
        logger.info("card.action ignored value=%r", val or None)
        return
    ev_id = _lark_im_payload_event_id(data)
    with _monitoring_reply_dispatch_lock:
        if ev_id and ev_id in _monitoring_card_action_event_ids:
            logger.info("duplicate card.action event_id=%r — skip", ev_id)
            return
        if ev_id:
            _monitoring_card_action_event_ids.add(ev_id)
            if len(_monitoring_card_action_event_ids) > 2000:
                _monitoring_card_action_event_ids.clear()
                _monitoring_card_action_event_ids.add(ev_id)
    chat_id, open_id = _lark_card_action_target_ids(data)
    # Prefer original card target from callback payload so group-card clicks reply in the same group
    # instead of falling back to operator open_id (private message).
    rid_t = _lark_dict_pick_str(val, "rid_t", "receive_id_type")
    rid = _lark_dict_pick_str(val, "rid", "receive_id")
    if rid_t == "chat_id" and rid:
        chat_id = rid
        open_id = ""
    elif rid_t == "open_id" and rid:
        open_id = rid
    logger.info("card.action refresh accepted chat=%r open=%r event_id=%r", bool(chat_id), bool(open_id), ev_id or None)
    threading.Thread(
        target=_monitoring_send_screenshot_on_card_click,
        args=(chat_id, open_id),
        daemon=True,
        name="monitoring-card-action",
    ).start()


def _run_monitoring_background_job(
    chat_id: str,
    open_id: str,
    mid: str,
    dispatch_key: str,
    source_chat_aliases: Optional[List[str]] = None,
) -> None:
    try:
        _monitoring_background_worker(chat_id, open_id, mid, dispatch_key, source_chat_aliases)
    finally:
        if dispatch_key:
            with _monitoring_reply_dispatch_lock:
                _monitoring_inflight_keys.discard(dispatch_key)


def _monitoring_background_worker(
    chat_id: str,
    open_id: str,
    mid: str,
    dispatch_key: str = "",
    source_chat_aliases: Optional[List[str]] = None,
) -> None:
    """
    Grafana + Lark send can exceed Feishu's ~3s webhook limit — run off the request thread.
    """
    logger.info("monitoring background job start mid=%r chat=%r open=%r", mid, bool(chat_id), bool(open_id))
    conv_key = (chat_id or "").strip() or (f"open:{(open_id or '').strip()}" if (open_id or "").strip() else "")
    if conv_key and not _monitoring_try_begin_chat_send(conv_key):
        logger.warning(
            "monitoring: blocked duplicate by conversation gate key=%r (MONITORING_CHAT_COALESCE_SECONDS)",
            conv_key[:96],
        )
        return
    if dispatch_key and not _monitoring_try_begin_user_send(dispatch_key):
        logger.warning(
            "monitoring: blocked duplicate **user-visible** send (MONITORING_SEND_COALESCE_SECONDS or concurrent send)"
        )
        _monitoring_end_chat_send(conv_key, False)
        return

    user_visible_send_ok = False
    try:
        grafana_session: Optional[requests.Session] = None
        payload: Optional[Dict[str, Any]] = None
        http_ex: Optional[Dict[str, Any]] = None
        try:
            grafana_session = grafana_login_session()
            payload = fetch_request_total_1m_series(session=grafana_session)
            http_ex = _http_analysis_for_payload(payload)
            reply = _format_monitoring_reply(payload)
        except Exception as e:
            logger.exception("monitoring fetch failed (background)")
            reply = f"监控数据拉取失败：{e}"
            grafana_session = None
            payload = None
            http_ex = None

        sent = False
        used_interactive_card = False
        embedded_png_in_card = False
        try:
            pre_key: Optional[str] = None
            if (
                (chat_id or open_id)
                and MONITORING_MESSAGE_CARD_ENABLE
                and _lark_env_truthy("MONITORING_CARD_EMBED_SCREENSHOT")
                and _lark_env_truthy("GRAFANA_SCREENSHOT_ENABLE")
                and grafana_session is not None
                and payload is not None
            ):
                w0 = payload.get("window") or {}
                su0 = int(w0.get("startUnix") or 0)
                eu0 = int(w0.get("endUnix") or 0)
                if su0 > 0 and eu0 > 0:
                    try:
                        pre_key = _lark_upload_png_image_key(
                            _grafana_headless_screenshot_png(grafana_session, su0, eu0)
                        )
                    except Exception:
                        logger.exception("monitoring pre-screenshot for interactive card failed")

            if chat_id:
                used_interactive_card, embedded_png_in_card = _lark_send_monitoring_user_message(
                    "chat_id", chat_id, reply, pre_key if _lark_env_truthy("MONITORING_CARD_EMBED_SCREENSHOT") else None
                )
                sent = True
                user_visible_send_ok = True
                logger.info(
                    "monitoring reply sent (background) chat_id_prefix=%s... len=%s card=%s embedded_png=%s",
                    chat_id[:16],
                    len(reply),
                    used_interactive_card,
                    embedded_png_in_card,
                )
            elif open_id:
                used_interactive_card, embedded_png_in_card = _lark_send_monitoring_user_message(
                    "open_id", open_id, reply, pre_key if _lark_env_truthy("MONITORING_CARD_EMBED_SCREENSHOT") else None
                )
                sent = True
                user_visible_send_ok = True
                logger.info(
                    "monitoring reply sent (background) open_id len=%s card=%s embedded_png=%s",
                    len(reply),
                    used_interactive_card,
                    embedded_png_in_card,
                )
            else:
                logger.warning(
                    "monitoring background: no chat_id/open_chat_id or sender open_id; msg cannot be sent"
                )

            alert_chat_id = (MONITORING_ALERT_CHAT_ID or "").strip()
            src_alias = {str(x).strip() for x in (source_chat_aliases or []) if str(x).strip()}
            if (chat_id or "").strip():
                src_alias.add((chat_id or "").strip())
            suppress_alert_copy = alert_chat_id in src_alias
            if http_ex and http_ex.get("hit_alert") and alert_chat_id and not suppress_alert_copy:
                try:
                    _lark_send_text("chat_id", alert_chat_id, reply)
                    logger.info(
                        "monitoring alert copy sent (background) alert_chat_id_prefix=%s... len=%s",
                        alert_chat_id[:16],
                        len(reply),
                    )
                except Exception:
                    logger.exception(
                        "monitoring alert forward failed (background) alert_chat_id=%r",
                        alert_chat_id[:24],
                    )
            elif http_ex and http_ex.get("hit_alert") and alert_chat_id and suppress_alert_copy:
                logger.info(
                    "monitoring alert copy skipped: source chat matches MONITORING_ALERT_CHAT_ID alias"
                )

            _raw_ss = _cfg_raw("GRAFANA_SCREENSHOT_ENABLE")
            logger.info(
                "monitoring screenshot gate sent=%s session=%s payload=%s ENABLE_raw=%r ENABLE_truthy=%s",
                sent,
                grafana_session is not None,
                payload is not None,
                _raw_ss,
                _lark_env_truthy("GRAFANA_SCREENSHOT_ENABLE"),
            )

            if sent and grafana_session is not None and payload is not None:
                if not _lark_env_truthy("GRAFANA_SCREENSHOT_ENABLE"):
                    logger.info(
                        "monitoring screenshot skipped: set GRAFANA_SCREENSHOT_ENABLE=1 (and install playwright + chromium)"
                    )
                elif used_interactive_card and embedded_png_in_card:
                    logger.info(
                        "monitoring: Grafana PNG embedded in interactive card — no separate image message"
                    )
                elif pre_key:
                    try:
                        if chat_id:
                            _lark_send_image_message("chat_id", chat_id, pre_key)
                        else:
                            _lark_send_image_message("open_id", open_id, pre_key)
                        logger.info(
                            "monitoring Grafana screenshot sent (fallback image after text/card) pre_key set"
                        )
                    except Exception:
                        logger.exception(
                            "monitoring follow-up image send failed (card may have been plain text)"
                        )
                else:
                    w = payload.get("window") or {}
                    su = int(w.get("startUnix") or 0)
                    eu = int(w.get("endUnix") or 0)
                    if su <= 0 or eu <= 0:
                        logger.warning(
                            "monitoring screenshot skipped: invalid window start=%s end=%s", su, eu
                        )
                    else:
                        try:
                            jar = grafana_session.cookies.get_dict()
                            if "grafana_session" not in jar:
                                logger.warning(
                                    "monitoring screenshot: no grafana_session cookie — expect login wall in PNG"
                                )
                            n_cookies = len(_playwright_cookie_list(grafana_session))
                            logger.info(
                                "monitoring screenshot start cookies=%s window=%s..%s",
                                n_cookies,
                                su,
                                eu,
                            )
                            png = _grafana_headless_screenshot_png(grafana_session, su, eu)
                            key = _lark_upload_png_image_key(png)
                            if chat_id:
                                _lark_send_image_message("chat_id", chat_id, key)
                            else:
                                _lark_send_image_message("open_id", open_id, key)
                            logger.info(
                                "monitoring Grafana screenshot sent (background) bytes=%s",
                                len(png),
                            )
                        except Exception:
                            logger.exception(
                                "monitoring Grafana screenshot or Lark image upload failed (text was already sent)"
                            )
            elif sent:
                logger.warning(
                    "monitoring screenshot skipped: sent text but grafana_session or payload is missing (unexpected)"
                )
        except Exception as e:
            logger.exception("monitoring lark text/image failed (background): %s", e)

        if sent and mid and len(_processed_lark_message_ids) > _PROCESSED_LARK_IDS_CAP:
            for _ in range(500):
                if len(_processed_lark_message_ids) <= _PROCESSED_LARK_IDS_CAP - 200:
                    break
                try:
                    _processed_lark_message_ids.pop()
                except KeyError:
                    break
    finally:
        if dispatch_key:
            _monitoring_end_user_send(dispatch_key, user_visible_send_ok)
        if conv_key:
            _monitoring_end_chat_send(conv_key, user_visible_send_ok)


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
    mid = _lark_im_message_dedupe_id(msg)
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

    if not _text_should_run_monitoring(raw_text, clean, mentions):
        logger.info(
            "im.message no trigger raw=%r clean=%r (need %r or @bot per MONITORING_AT_MENTION_*)",
            (raw_text or "")[:160],
            (clean or "")[:160],
            MONITORING_TRIGGER,
        )
        return

    chat_id = chat_resolved
    open_id = sender_open
    chat_aliases = _lark_message_chat_id_aliases(msg)
    sender_debounce = _lark_im_sender_debounce_token(sender, open_id)
    im_event_id = _lark_im_payload_event_id(data)
    msg_time = _lark_im_message_time_token(msg)
    body_key = _monitoring_dispatch_body_key(clean, raw_text, mentions)
    processed_stick = _monitoring_processed_stick(
        mid, im_event_id, chat_id or "", sender_debounce, msg_time
    )

    logger.info(
        "monitoring trigger matched — background job mid=%r event_id=%r msg_time=%r stick=%r chat_id=%r open_id_prefix=%r",
        mid,
        im_event_id or None,
        msg_time or None,
        (processed_stick[:72] + "…") if len(processed_stick) > 72 else (processed_stick or None),
        bool(chat_id),
        (open_id[:12] + "…") if len(open_id) > 12 else open_id,
    )

    debounce_sec = 0.0
    raw_db = _cfg_raw("MONITORING_IM_DEBOUNCE_SECONDS")
    if raw_db is not None and str(raw_db).strip() != "":
        try:
            debounce_sec = float(raw_db)
        except (TypeError, ValueError):
            debounce_sec = 5.0
    # Some duplicated Feishu deliveries for the same human message can differ in sender/message envelope fields.
    # Keep debounce/send key stable on chat + normalized command body only, so variants collapse into one worker.
    debounce_key = f"{(chat_id or '').strip()}\n{body_key}"
    chat_gate_key = (chat_id or "").strip() or (f"open:{(open_id or '').strip()}" if (open_id or "").strip() else "")
    chat_gate_sec = _cfg_float("MONITORING_CHAT_TRIGGER_DEBOUNCE_SECONDS", 8.0)
    now_m = time.monotonic()
    with _monitoring_reply_dispatch_lock:
        if chat_gate_key and chat_gate_sec > 0:
            prev_chat = _monitoring_chat_trigger_last.get(chat_gate_key, 0.0)
            if prev_chat > 0.0 and (now_m - prev_chat) < chat_gate_sec:
                logger.info(
                    "monitoring chat-trigger debounce skip (%.2fs) key=%r",
                    chat_gate_sec,
                    chat_gate_key[:96],
                )
                return
        if im_event_id and im_event_id in _processed_lark_im_event_ids:
            logger.info("duplicate IM event_id=%s — skip", im_event_id)
            return
        if processed_stick and processed_stick in _processed_lark_message_ids:
            logger.info("duplicate monitoring dispatch stick=%r — skip", processed_stick[:96])
            return
        if debounce_key in _monitoring_inflight_keys:
            logger.info("monitoring skip — same trigger already **in flight** (wait for job to finish)")
            return
        if debounce_sec > 0:
            prev_t = _monitoring_im_trigger_last.get(debounce_key, 0.0)
            if now_m - prev_t < debounce_sec:
                logger.info(
                    "monitoring debounce skip (%.2fs) chat=%r",
                    debounce_sec,
                    bool(chat_id),
                )
                return
            _monitoring_im_trigger_last[debounce_key] = now_m
            if len(_monitoring_im_trigger_last) > 600:
                for k, _ in sorted(_monitoring_im_trigger_last.items(), key=lambda kv: kv[1])[:220]:
                    try:
                        del _monitoring_im_trigger_last[k]
                    except KeyError:
                        pass
                _monitoring_im_trigger_last[debounce_key] = now_m
        if chat_gate_key and chat_gate_sec > 0:
            _monitoring_chat_trigger_last[chat_gate_key] = now_m
            if len(_monitoring_chat_trigger_last) > 600:
                for k, _ in sorted(_monitoring_chat_trigger_last.items(), key=lambda kv: kv[1])[:220]:
                    try:
                        del _monitoring_chat_trigger_last[k]
                    except KeyError:
                        pass
                _monitoring_chat_trigger_last[chat_gate_key] = now_m
        _monitoring_inflight_keys.add(debounce_key)
        if processed_stick:
            _processed_lark_message_ids.add(processed_stick)
        if im_event_id:
            _processed_lark_im_event_ids.add(im_event_id)
            if len(_processed_lark_im_event_ids) > _PROCESSED_IM_EVENT_IDS_CAP:
                _processed_lark_im_event_ids.clear()
                _processed_lark_im_event_ids.add(im_event_id)

    threading.Thread(
        target=_run_monitoring_background_job,
        args=(chat_id, open_id, mid, debounce_key, chat_aliases),
        daemon=True,
        name="monitoring-reply",
    ).start()


def _ws_log_message_snip(data: Dict[str, Any]) -> Tuple[Any, Any, str]:
    """Safe for ``event.message`` missing or null (``dict.get('message', {})`` returns None if key exists)."""
    ev = data.get("event") if isinstance(data.get("event"), dict) else {}
    msg = ev.get("message") or {}
    if not isinstance(msg, dict):
        msg = {}
    mid = _lark_im_message_dedupe_id(msg) or None
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
    bld = EventDispatcherHandler.builder(enc, ver).register_p2_im_message_receive_v1(
        _on_ws_p2_im_message_receive_v1
    )
    if _lark_env_truthy("LARK_WS_REGISTER_IM_MESSAGE_V2"):
        bld = bld.register_p2_customized_event(
            "im.message.receive_v2", _on_ws_im_message_p2_customized
        )
        logger.info("LARK_WS_REGISTER_IM_MESSAGE_V2=1 — also handling im.message.receive_v2")
    else:
        logger.info(
            "LARK_WS_REGISTER_IM_MESSAGE_V2=0 — not subscribing to im.message.receive_v2 (avoids duplicate v1+v2)."
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
        _v2 = (
            " + im.message.receive_v2"
            if _lark_env_truthy("LARK_WS_REGISTER_IM_MESSAGE_V2")
            else ""
        )
        logger.info(
            "Lark WebSocket client starting (domain=%s); WS IM: p2 im.message.receive_v1%s",
            dnorm,
            _v2,
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
    # Card interactions also require a fast 200; business logic should update the card asynchronously via Open API.
    if et_l.startswith("card.action"):
        try:
            _handle_monitoring_card_action(data)
        except Exception:
            logger.exception("card.action handler failed")
        return _lark_feishu_webhook_ack_immediate()

    if _lark_ack_only_event_type(et):
        return _lark_feishu_webhook_ack_immediate()

    if et in ("im.message.receive_v1", "im.message.receive_v2"):
        if _lark_skip_http_im_message_when_ws_mode():
            logger.info(
                "webhook: skip %s (HTTP IM ignored while LARK_EVENT_MODE=ws; set LARK_HTTP_IGNORE_IM_WHEN_EVENT_MODE_WS=0 to allow).",
                et,
            )
            return _lark_feishu_webhook_ack_immediate()
        return _handle_im_message_receive(data)

    logger.debug(
        "event ignored: event_type=%r keys=%s (subscribe 消息与群组 → 接收消息 v2.0)",
        et,
        list(data.keys())[:20],
    )
    return _lark_feishu_webhook_ack_immediate()


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
        data["httpAnalysis"] = _http_analysis_for_payload(data)
        return jsonify(data)
    except Exception as e:
        logger.exception("request-total-1m failed")
        return jsonify({"error": str(e)}), 500


def run_monitoring_bot() -> None:
    """
    Process entrypoint: HTTP-only, WebSocket-only, or WS + HTTP sidecar (see module docstring).
    Uses :data:`app`, :data:`logger`, :func:`start_lark_ws_client_blocking` from this module.
    """
    logger.info(
        "monitoring bot pid=%s — duplicate replies: check two processes (same APP_ID) or IM dedupe logs.",
        os.getpid(),
    )
    port = _cfg_listen_port(5002)
    if MONITORING_AT_MENTION_ENABLE and not (LARK_BOT_OPEN_ID or "").strip():
        logger.warning(
            "MONITORING_AT_MENTION_ENABLE is on but LARK_BOT_OPEN_ID is empty — @-only triggers will not match; set LARK_BOT_OPEN_ID to this bot's open_id."
        )
    if int(GRAFANA_QUERY_LOOKBACK_SECONDS) != 600:
        logger.warning(
            "GRAFANA_QUERY_LOOKBACK_SECONDS=%s (default 600 = 10m) — /monitoring Prometheus window is not 10 minutes",
            GRAFANA_QUERY_LOOKBACK_SECONDS,
        )
    raw_mode = _cfg_str("LARK_EVENT_MODE", "http").strip().lower()
    mode = raw_mode if raw_mode else "http"

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
        logger.info(
            "LARK_EVENT_MODE=http — **WebSocket disabled**; Feishu IM/events only via POST /webhook/event. "
            "Use Request URL mode in the developer console (not long-connection)."
        )
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
        raise SystemExit(f"Unknown LARK_EVENT_MODE={mode!r} (use ``http`` for webhook-only, or ``ws`` for long connection)")

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