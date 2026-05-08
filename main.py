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

群/at **本**机器人 + 监控命令（默认 ``/mo``）**或仅 @ 本机器人（无其它正文，见 ``MONITORING_AT_MENTION_*``）** → Grafana 摘要（默认最近 **15** 分钟）。默认 ``MONITORING_TRIGGER_REQUIRES_AT_BOT=1``：未 @ 本机器人时发 ``/mo`` 不会触发。
User-visible bot text is **English-only**. Short commands (configurable): ``@this_bot /mo`` monitoring (when ``MONITORING_TRIGGER_REQUIRES_AT_BOT=1``), ``/m`` mute card, ``/c`` cancel all mutes. **@this_bot + non-command text** → command list (see ``MONITORING_AT_MENTION_*``).
默认 ``MONITORING_MESSAGE_CARD_ENABLE=1``：用户侧 **一条** 交互卡片（``msg_type=interactive``），截图嵌在卡片内，不再跟一条独立 PNG。设 ``0`` 则仍为「纯文字 + 独立图片」两条消息。需在飞书应用开通「发送消息卡片」权限。

HTTP 回调先返回 ``{}`` 再后台处理。HTTP 跌幅告警命中时可额外转发到 ``MONITORING_ALERT_CHAT_ID``（群 ``chat_id``，如 ``oc_…``）。

可选 ``GRAFANA_SCREENSHOT_ENABLE=1``：与卡片同开时先截一张上传为 ``image_key`` 嵌入卡片；仅关卡片时仍为无头 Chromium 截图后单独发图（需 ``pip install playwright`` 与 ``playwright install chromium``）。
默认 ``GRAFANA_PERSISTENT_BROWSER=1``：进程启动时后台常驻一颗 Chromium，先预热 Grafana；``/monitoring`` 与告警截图复用该页，失败时自动回退为「每次新开浏览器」。
默认 ``GRAFANA_SCREENSHOT_FULL_PAGE=1`` 截整页滚动区域（长 dashboard 全部图表）；设为 ``0`` 则仅视口大小（易只拍到上半屏）。
多轮滚动 + Spinner 轮询见 ``GRAFANA_SCREENSHOT_STABILIZE_ROUNDS`` 等键；Prometheus 无数据/报错的格子无法被脚本「画出曲线」。
"""

import base64
import copy
import hashlib
import json
import logging
import math
import os
import queue
from urllib.parse import urlencode
from datetime import datetime
import re
import threading
import time
import warnings
from typing import Any, Dict, Generator, List, Optional, Set, Tuple

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
    "GRAFANA_PANEL_TITLE_9280": "9280 Connection",
    "MONITORING_9280_SERIES_KEYWORD": "9280 + Push",
    "GRAFANA_PANEL_TITLE_DEPOSIT": "主站充值 (Main Site Deposit)",
    "MONITORING_DEPOSIT_SERIES_KEYWORD": "createProposal",
    "GRAFANA_PANEL_TITLE_WITHDRAW": "提款 (Withdrawal)",
    "MONITORING_WITHDRAW_SERIES_KEYWORD": "InitiateWithdrawal",
    "GRAFANA_PANEL_TITLE_PROVIDER_JILI": "IGO Distributions of Providers JILI",
    "MONITORING_PROVIDER_JILI_SERIES_KEYWORD": "3201",
    "GRAFANA_PANEL_TITLE_PROVIDER_GENERAL": "IGO Distributions of Providers GENERAL",
    "MONITORING_PROVIDER_GENERAL_SERIES_KEYWORD": "3204",
    "GRAFANA_PANEL_TITLE_PROVIDER_INHOUSE": "IGO Distributions of Providers INHOUSE",
    "MONITORING_PROVIDER_INHOUSE_SERIES_KEYWORD": "3085",
    "GRAFANA_PANEL_TITLE_GAMES_JILI": "IGO Distributions of Games（Jili）",
    "MONITORING_GAMES_JILI_SERIES_KEYWORD": "49",
    "GRAFANA_PANEL_TITLE_GAMES_GENERAL": "IGO Distributions of Games（General）",
    "MONITORING_GAMES_GENERAL_SERIES_KEYWORD": "1492288",
    "GRAFANA_PANEL_TITLE_GAMES_INHOUSE": "IGO Distributions of Games（Inhouse）",
    "MONITORING_GAMES_INHOUSE_SERIES_KEYWORD": "6005",
    "GRAFANA_DASHBOARD_FROM": "now-30m",
    "GRAFANA_DASHBOARD_TO": "now",
    "GRAFANA_QUERY_STEP": 60,
    "GRAFANA_QUERY_LOOKBACK_SECONDS": 900,
    # Prometheus 最近分钟桶常未跑完；query_range 的 end 用「现在 − 该秒数」，最新点落在「约前两分钟」
    "GRAFANA_QUERY_END_LAG_SECONDS": 60,
    # 二者均 >0 且 START>END 时：不用 LOOKBACK+LAG，改用对齐窗口（见 MONITORING_TIME_BUCKET_TZ）
    # start = 当前日历分钟起点 − START 分钟，end = 当前日历分钟起点 − END 分钟（均为 …:00）。
    # 例 NOW=5:35:23 → cur_min=5:35:00，START=6 END=1 → start=5:29:00 end=5:34:00（最后一桶 5:34:00 非 5:34:23）
    # 设 START=0 或 END=0 则退回 GRAFANA_QUERY_LOOKBACK_SECONDS + GRAFANA_QUERY_END_LAG_SECONDS
    "MONITORING_QUERY_ALIGNED_START_OFFSET_MINUTES": "6",
    "MONITORING_QUERY_ALIGNED_END_OFFSET_MINUTES": "1",
    # 合并后再丢掉尾部 N 个「分钟桶」（最后一两分钟常为未完成 scrape / 延迟，易出现畸形偏低）；0=不丢
    "MONITORING_DROP_LAST_MERGED_MINUTES": "1",
    # /mo 与告警正文里的 time/value 表只展示「最新 N 行」（DROP/SPIKE 仍基于窗口内完整 merged 序列）
    "MONITORING_TABLE_TAIL_ROWS": "5",
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
    # 截图 URL 用 GRAFANA_DASHBOARD_FROM/TO（默认 now-15m / now）；0 则用 Prometheus 窗口的绝对毫秒时间戳
    "GRAFANA_SCREENSHOT_RELATIVE_RANGE": "1",
    # 截图 URL 追加 timezone=…（与 Grafana 时间栏一致）；设为 none / - 可省略该参数
    "GRAFANA_SCREENSHOT_TIMEZONE": "browser",
    # 数字面板分钟聚合 + 告警时间 ``mm-dd HH:MM`` 使用的 IANA 时区（如 Asia/Shanghai）。
    # 与 Grafana 面板时区不一致且 bot 跑在 UTC 上时，不设会导致同一分钟的值错桶（假 SPIKE）。
    # 空 / local / server = 使用进程本地时区。
    "MONITORING_TIME_BUCKET_TZ": "",
    # 1=进程内常驻 Playwright Chromium（启动时预热 Grafana；/monitoring 与告警截图复用，不必每次冷启动）
    "GRAFANA_PERSISTENT_BROWSER": "1",
    # 常驻浏览器在空闲时点击 Refresh 的间隔（秒），保持会话与图表较新
    "GRAFANA_PERSISTENT_BROWSER_IDLE_REFRESH_SECONDS": "120",
    # 单次截图任务在 keeper 队列中的最长等待（秒）
    "GRAFANA_PERSISTENT_BROWSER_JOB_TIMEOUT_SECONDS": "180",
    # 常驻浏览器每次截图前：不清空全部 cookie，只追加/覆盖新登录（减轻 SPA 主区闪空白）
    "GRAFANA_PERSISTENT_BROWSER_SOFT_COOKIE": "1",
    # 按快门前几毫秒：置顶滚动 + 等字体 + rAF，缓解 headless「已 ready 但 PNG 仍空壳」
    "GRAFANA_SCREENSHOT_PRE_CAPTURE_MS": "800",
    # 1=快门前再跑一轮整页滚动刷 canvas（更慢但更稳）
    "GRAFANA_SCREENSHOT_PRE_CAPTURE_RESCROLL": "0",
    # 等 #reactRoot 出现图表 DOM 的最长毫秒（过大会拖很久）
    "GRAFANA_SCREENSHOT_POPULATE_MAX_MS": 4500,
    # 整页截图稳定：默认 1 轮即可；仍无法保证 Prometheus「No data」有曲线
    "GRAFANA_SCREENSHOT_STABILIZE_ROUNDS": 1,
    "GRAFANA_SCREENSHOT_SCROLL_PAUSE_MS": 100,
    "GRAFANA_SCREENSHOT_SETTLE_MS": 300,
    "GRAFANA_SCREENSHOT_SPINNER_MAX_MS": 7000,
    # 至少等到 N 个 .react-grid-item（0=不等待；经典大屏可设 4–8；Scenes 布局可能为 0）
    "GRAFANA_SCREENSHOT_MIN_GRID_ITEMS": 0,
    # 截图前“全面板加载”门槛：已加载面板占比（含图或明确 No data）
    "GRAFANA_SCREENSHOT_PANEL_READY_RATIO": 0.92,
    # 截图前“全面板加载”最少面板数（防小屏/过滤时占比误判）
    "GRAFANA_SCREENSHOT_PANEL_READY_MIN": 8,
    # 全面板加载等待预算（毫秒）
    "GRAFANA_SCREENSHOT_PANEL_READY_MAX_MS": 12000,
    "GRAFANA_USER": "om_duty",
    "GRAFANA_PASSWORD": "5tgb%TGB094",
    "VERIFICATION_TOKEN": "QlZMYp7rogAS914dxxMVNgboUKxQP7jc",
    "APP_ID": "cli_a97fcc6df7615ed1",
    "APP_SECRET": "NwAi6xJxMYDHMFAQcTG8ZfJxpeTOibvy",
    "MONITORING_TRIGGER": "/mo",
    "MONITORING_MUTE_TRIGGER": "/m",
    "MONITORING_CANCELMUTE_TRIGGER": "/c",
    # 1=仅 @ 机器人且无其它正文也触发（与 MONITORING_TRIGGER 默认 /mo 同）；1+ANY=1 时 @ 且任意正文也跑监控（非命令且带字会先收到命令说明）
    "MONITORING_AT_MENTION_ENABLE": "0",
    "MONITORING_AT_MENTION_ANY_TEXT": "0",
    # 1=发 MONITORING_TRIGGER（如 /mo）时必须 @ 本机器人（LARK_BOT_OPEN_ID）；0=群内任意出现 /mo 即触发（旧行为）
    "MONITORING_TRIGGER_REQUIRES_AT_BOT": "1",
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
    "MONITORING_HTTP_DROP_ALERT_PCT": 10,
    "MONITORING_HTTP_CONTINUOUS_ALERT_PCT": 20,
    "MONITORING_9280_ENABLE": "1",
    "MONITORING_9280_ALERT_PCT": 15,
    "MONITORING_9280_CONTINUOUS_ALERT_PCT": 25,
    "MONITORING_DEPOSIT_ENABLE": "1",
    "MONITORING_DEPOSIT_ALERT_PCT": 80,
    "MONITORING_DEPOSIT_CONTINUOUS_ALERT_PCT": 120,
    "MONITORING_WITHDRAW_ENABLE": "1",
    "MONITORING_WITHDRAW_ALERT_PCT": 80,
    "MONITORING_WITHDRAW_CONTINUOUS_ALERT_PCT": 120,
    "MONITORING_PROVIDER_JILI_ENABLE": "1",
    "MONITORING_PROVIDER_JILI_ALERT_PCT": 15,
    "MONITORING_PROVIDER_JILI_CONTINUOUS_ALERT_PCT": 15,
    "MONITORING_PROVIDER_GENERAL_ENABLE": "1",
    "MONITORING_PROVIDER_GENERAL_ALERT_PCT": 15,
    "MONITORING_PROVIDER_GENERAL_CONTINUOUS_ALERT_PCT": 15,
    "MONITORING_PROVIDER_INHOUSE_ENABLE": "1",
    "MONITORING_PROVIDER_INHOUSE_ALERT_PCT": 15,
    "MONITORING_PROVIDER_INHOUSE_CONTINUOUS_ALERT_PCT": 15,
    "MONITORING_GAMES_JILI_ENABLE": "1",
    "MONITORING_GAMES_JILI_ALERT_PCT": 15,
    "MONITORING_GAMES_JILI_CONTINUOUS_ALERT_PCT": 15,
    "MONITORING_GAMES_GENERAL_ENABLE": "1",
    "MONITORING_GAMES_GENERAL_ALERT_PCT": 15,
    "MONITORING_GAMES_GENERAL_CONTINUOUS_ALERT_PCT": 15,
    "MONITORING_GAMES_INHOUSE_ENABLE": "1",
    "MONITORING_GAMES_INHOUSE_ALERT_PCT": 15,
    "MONITORING_GAMES_INHOUSE_CONTINUOUS_ALERT_PCT": 15,
    "MONITORING_ALERT_WINDOW_SECONDS": 120,
    # 1=alert text skips Fast/Continuous SPIKE/DROP lines; only a short time/value tail (Grafana-like).
    "MONITORING_SIMPLE_ALERT_TEXT": "0",
    # 1=/mo hides extra-panel ``within Xm drop/spike`` footer lines; tables only.
    "MONITORING_MO_HIDE_EXTRA_DROP_SPIKE_STATS": "0",
    "MONITORING_WATCH_ENABLE": "1",
    "MONITORING_WATCH_INTERVAL_SECONDS": "60",
    # 自动告警最短间隔（防刷屏）；默认 300=5 分钟
    "MONITORING_WATCH_ALERT_COOLDOWN_SECONDS": "300",
    # Watchdog：Prometheus 判窗对齐到整分钟，相对「当前分钟起点」向回偏移（分钟）。例：6 与 1 → 12:46:xx 只评 12:40:00..12:45:00
    "MONITORING_WATCH_EVAL_START_OFFSET_MINUTES": "6",
    "MONITORING_WATCH_EVAL_END_OFFSET_MINUTES": "1",
    # Watchdog 判警是否使用与 /monitoring 相同的拉数窗口（默认 0：窄窗口 eval；设为 1 则与报表一致，避免「报表有大波动但自动告警未扫到」）
    "MONITORING_WATCH_MATCH_REPORT_WINDOW": "0",
    # Watchdog 告警附带截图的 Grafana URL（相对时间）；与判窗数据窗口无关，默认最近 15 分钟整页
    "MONITORING_WATCH_SCREENSHOT_FROM": "now-15m",
    "MONITORING_WATCH_SCREENSHOT_TO": "now",
    "MONITORING_WATCH_SCREENSHOT_TIMEZONE": "browser",
    # 每日静默：该时段内不拉数、不判警（默认 23:59:00～次日 00:10:00 前一刻，跨午夜；本地机器时间）
    "MONITORING_WATCH_QUIET_WINDOW_ENABLE": "1",
    "MONITORING_WATCH_QUIET_START_HOUR": "23",
    "MONITORING_WATCH_QUIET_START_MINUTE": "59",
    "MONITORING_WATCH_QUIET_END_HOUR": "0",
    "MONITORING_WATCH_QUIET_END_MINUTE": "10",
    # Tag person
    "TARGET_USER_OPEN_ID": "ou_5f660c0fb0769d184aca635d02209272",
    "JUNCHEN": "ou_5f660c0fb0769d184aca635d02209272",
    # In which group
    "MONITORING_ALERT_CHAT_ID": "oc_9de3d63fc589df6feeb9b0bee9c45b72",
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
_monitoring_watch_last_alert_at: float = 0.0
_monitoring_watch_started: bool = False
# --- Alert mute (「告警静音」：进程内；按监控通道粒度，供 watchdog / 告警转发过滤) ---
_MONITORING_MUTE_UNTIL: Dict[str, float] = {}
_mute_pending_selections: Dict[str, Set[str]] = {}
_grafana_pw_keeper: Optional[Any] = None
_grafana_pw_keeper_lock = threading.Lock()
_grafana_pw_keeper_start_attempted: bool = False
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
GRAFANA_PANEL_TITLE_9280 = _cfg_str("GRAFANA_PANEL_TITLE_9280", "9280 Connection")
MONITORING_9280_SERIES_KEYWORD = _cfg_str("MONITORING_9280_SERIES_KEYWORD", "9280 + Push").strip()
GRAFANA_PANEL_TITLE_DEPOSIT = _cfg_str("GRAFANA_PANEL_TITLE_DEPOSIT", "主站充值 (Main Site Deposit)")
MONITORING_DEPOSIT_SERIES_KEYWORD = _cfg_str("MONITORING_DEPOSIT_SERIES_KEYWORD", "createProposal").strip()
GRAFANA_PANEL_TITLE_WITHDRAW = _cfg_str("GRAFANA_PANEL_TITLE_WITHDRAW", "提款 (Withdrawal)")
MONITORING_WITHDRAW_SERIES_KEYWORD = _cfg_str("MONITORING_WITHDRAW_SERIES_KEYWORD", "InitiateWithdrawal").strip()
GRAFANA_PANEL_TITLE_PROVIDER_JILI = _cfg_str(
    "GRAFANA_PANEL_TITLE_PROVIDER_JILI", "IGO Distributions of Providers JILI"
)
MONITORING_PROVIDER_JILI_SERIES_KEYWORD = _cfg_str(
    "MONITORING_PROVIDER_JILI_SERIES_KEYWORD", "3201"
).strip()
GRAFANA_PANEL_TITLE_PROVIDER_GENERAL = _cfg_str(
    "GRAFANA_PANEL_TITLE_PROVIDER_GENERAL", "IGO Distributions of Providers GENERAL"
)
MONITORING_PROVIDER_GENERAL_SERIES_KEYWORD = _cfg_str(
    "MONITORING_PROVIDER_GENERAL_SERIES_KEYWORD", "3204"
).strip()
GRAFANA_PANEL_TITLE_PROVIDER_INHOUSE = _cfg_str(
    "GRAFANA_PANEL_TITLE_PROVIDER_INHOUSE", "IGO Distributions of Providers INHOUSE"
)
MONITORING_PROVIDER_INHOUSE_SERIES_KEYWORD = _cfg_str(
    "MONITORING_PROVIDER_INHOUSE_SERIES_KEYWORD", "3085"
).strip()
GRAFANA_PANEL_TITLE_GAMES_JILI = _cfg_str(
    "GRAFANA_PANEL_TITLE_GAMES_JILI", "IGO Distributions of Games（Jili）"
)
MONITORING_GAMES_JILI_SERIES_KEYWORD = _cfg_str(
    "MONITORING_GAMES_JILI_SERIES_KEYWORD", "49"
).strip()
GRAFANA_PANEL_TITLE_GAMES_GENERAL = _cfg_str(
    "GRAFANA_PANEL_TITLE_GAMES_GENERAL", "IGO Distributions of Games（General）"
)
MONITORING_GAMES_GENERAL_SERIES_KEYWORD = _cfg_str(
    "MONITORING_GAMES_GENERAL_SERIES_KEYWORD", "1492288"
).strip()
GRAFANA_PANEL_TITLE_GAMES_INHOUSE = _cfg_str(
    "GRAFANA_PANEL_TITLE_GAMES_INHOUSE", "IGO Distributions of Games（Inhouse）"
)
MONITORING_GAMES_INHOUSE_SERIES_KEYWORD = _cfg_str(
    "MONITORING_GAMES_INHOUSE_SERIES_KEYWORD", "6005"
).strip()
# Browser URL time range for screenshots (default last 15 minutes, aligned with /monitoring tables).
GRAFANA_DASHBOARD_FROM = _cfg_str("GRAFANA_DASHBOARD_FROM", "now-15m")
GRAFANA_DASHBOARD_TO = _cfg_str("GRAFANA_DASHBOARD_TO", "now")
# Prometheus query_range step (seconds); 60 → up to 15 buckets in 15m when lookback=900
GRAFANA_QUERY_STEP = _cfg_int("GRAFANA_QUERY_STEP", 60)
GRAFANA_QUERY_LOOKBACK_SECONDS = _cfg_int("GRAFANA_QUERY_LOOKBACK_SECONDS", 900)
GRAFANA_QUERY_END_LAG_SECONDS = _cfg_int("GRAFANA_QUERY_END_LAG_SECONDS", 120)
MONITORING_QUERY_ALIGNED_START_OFFSET_MINUTES = max(
    0, _cfg_int("MONITORING_QUERY_ALIGNED_START_OFFSET_MINUTES", 0)
)
MONITORING_QUERY_ALIGNED_END_OFFSET_MINUTES = max(
    0, _cfg_int("MONITORING_QUERY_ALIGNED_END_OFFSET_MINUTES", 0)
)
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
GRAFANA_SCREENSHOT_PANEL_READY_RATIO = max(
    0.5, min(1.0, _cfg_float("GRAFANA_SCREENSHOT_PANEL_READY_RATIO", 0.92))
)
GRAFANA_SCREENSHOT_PANEL_READY_MIN = max(
    0, min(300, _cfg_int("GRAFANA_SCREENSHOT_PANEL_READY_MIN", 8))
)
GRAFANA_SCREENSHOT_PANEL_READY_MAX_MS = max(
    2000, min(120_000, _cfg_int("GRAFANA_SCREENSHOT_PANEL_READY_MAX_MS", 12000))
)
GRAFANA_SCREENSHOT_KIOSK = _cfg_str("GRAFANA_SCREENSHOT_KIOSK", "").strip()
GRAFANA_SCREENSHOT_RELATIVE_RANGE = _lark_env_truthy("GRAFANA_SCREENSHOT_RELATIVE_RANGE")
# Screenshot URL ``timezone=`` (e.g. browser); none / - / off → omit parameter
GRAFANA_SCREENSHOT_TIMEZONE = _cfg_str("GRAFANA_SCREENSHOT_TIMEZONE", "browser").strip()
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
MONITORING_TRIGGER = _cfg_str("MONITORING_TRIGGER", "/mo")
MONITORING_MUTE_TRIGGER = _cfg_str("MONITORING_MUTE_TRIGGER", "/m").strip()
MONITORING_CANCELMUTE_TRIGGER = _cfg_str("MONITORING_CANCELMUTE_TRIGGER", "/c").strip()
TARGET_USER_OPEN_ID = _cfg_str("TARGET_USER_OPEN_ID", _cfg_str("JUNCHEN", "")).strip()
MONITORING_HTTP_DROP_ALERT_PCT = _cfg_float("MONITORING_HTTP_DROP_ALERT_PCT", 10.0)
MONITORING_9280_ALERT_PCT = _cfg_float("MONITORING_9280_ALERT_PCT", 15.0)
MONITORING_HTTP_CONTINUOUS_ALERT_PCT = _cfg_float("MONITORING_HTTP_CONTINUOUS_ALERT_PCT", 20.0)
MONITORING_9280_CONTINUOUS_ALERT_PCT = _cfg_float("MONITORING_9280_CONTINUOUS_ALERT_PCT", 25.0)
MONITORING_DEPOSIT_ALERT_PCT = _cfg_float("MONITORING_DEPOSIT_ALERT_PCT", 60.0)
MONITORING_DEPOSIT_CONTINUOUS_ALERT_PCT = _cfg_float("MONITORING_DEPOSIT_CONTINUOUS_ALERT_PCT", 80.0)
MONITORING_WITHDRAW_ALERT_PCT = _cfg_float("MONITORING_WITHDRAW_ALERT_PCT", 60.0)
MONITORING_WITHDRAW_CONTINUOUS_ALERT_PCT = _cfg_float("MONITORING_WITHDRAW_CONTINUOUS_ALERT_PCT", 80.0)
MONITORING_PROVIDER_JILI_ALERT_PCT = _cfg_float("MONITORING_PROVIDER_JILI_ALERT_PCT", 15.0)
MONITORING_PROVIDER_JILI_CONTINUOUS_ALERT_PCT = _cfg_float(
    "MONITORING_PROVIDER_JILI_CONTINUOUS_ALERT_PCT", 15.0
)
MONITORING_PROVIDER_GENERAL_ALERT_PCT = _cfg_float("MONITORING_PROVIDER_GENERAL_ALERT_PCT", 15.0)
MONITORING_PROVIDER_GENERAL_CONTINUOUS_ALERT_PCT = _cfg_float(
    "MONITORING_PROVIDER_GENERAL_CONTINUOUS_ALERT_PCT", 15.0
)
MONITORING_PROVIDER_INHOUSE_ALERT_PCT = _cfg_float("MONITORING_PROVIDER_INHOUSE_ALERT_PCT", 15.0)
MONITORING_PROVIDER_INHOUSE_CONTINUOUS_ALERT_PCT = _cfg_float(
    "MONITORING_PROVIDER_INHOUSE_CONTINUOUS_ALERT_PCT", 15.0
)
MONITORING_GAMES_JILI_ALERT_PCT = _cfg_float("MONITORING_GAMES_JILI_ALERT_PCT", 15.0)
MONITORING_GAMES_JILI_CONTINUOUS_ALERT_PCT = _cfg_float(
    "MONITORING_GAMES_JILI_CONTINUOUS_ALERT_PCT", 15.0
)
MONITORING_GAMES_GENERAL_ALERT_PCT = _cfg_float("MONITORING_GAMES_GENERAL_ALERT_PCT", 15.0)
MONITORING_GAMES_GENERAL_CONTINUOUS_ALERT_PCT = _cfg_float(
    "MONITORING_GAMES_GENERAL_CONTINUOUS_ALERT_PCT", 15.0
)
MONITORING_GAMES_INHOUSE_ALERT_PCT = _cfg_float("MONITORING_GAMES_INHOUSE_ALERT_PCT", 15.0)
MONITORING_GAMES_INHOUSE_CONTINUOUS_ALERT_PCT = _cfg_float(
    "MONITORING_GAMES_INHOUSE_CONTINUOUS_ALERT_PCT", 15.0
)
MONITORING_ALERT_WINDOW_SECONDS = max(60, _cfg_int("MONITORING_ALERT_WINDOW_SECONDS", 120))
MONITORING_DROP_LAST_MERGED_MINUTES = max(
    0, min(60, _cfg_int("MONITORING_DROP_LAST_MERGED_MINUTES", 1))
)
MONITORING_TABLE_TAIL_ROWS = max(1, min(99, _cfg_int("MONITORING_TABLE_TAIL_ROWS", 5)))
MONITORING_SIMPLE_ALERT_TEXT = _lark_env_truthy("MONITORING_SIMPLE_ALERT_TEXT")
MONITORING_MO_HIDE_EXTRA_DROP_SPIKE_STATS = _lark_env_truthy(
    "MONITORING_MO_HIDE_EXTRA_DROP_SPIKE_STATS"
)
MONITORING_TIME_BUCKET_TZ = _cfg_str("MONITORING_TIME_BUCKET_TZ", "").strip()


def _parse_monitoring_zoneinfo() -> Optional[Any]:
    from zoneinfo import ZoneInfo

    tzn = MONITORING_TIME_BUCKET_TZ.strip()
    if not tzn or tzn.lower() in ("local", "server", "-", "none"):
        return None
    try:
        return ZoneInfo(tzn)
    except Exception:
        logger.warning(
            "MONITORING_TIME_BUCKET_TZ=%r invalid; using process local time for buckets",
            tzn,
        )
        return None


MONITORING_ZONEINFO: Optional[Any] = _parse_monitoring_zoneinfo()


def _monitoring_calendar_dt(ts: float) -> datetime:
    zi = MONITORING_ZONEINFO
    if zi is not None:
        return datetime.fromtimestamp(float(ts), tz=zi)
    return datetime.fromtimestamp(float(ts))


def _bucket_ts_monitoring_minute(ts: float) -> float:
    dt = _monitoring_calendar_dt(ts).replace(second=0, microsecond=0)
    return dt.timestamp()


def _snap_series_to_monitoring_minutes(
    points: List[Tuple[float, float]],
    *,
    how: str,
    tol_sec: float = 0.5,
) -> List[Tuple[float, float]]:
    """
    One point per calendar minute (``MONITORING_ZONEINFO`` / local), keyed at minute start ``b``.

    - If any sample lies within ``tol_sec`` of ``b``, prefer those (``max`` or ``sum`` of their values).
    - Otherwise fall back so Prometheus offsets (:30 / :45) still produce data: ``max`` → value at the
      timestamp **closest** to ``b``; ``sum`` → **sum every** sample in that minute (HTTP additive).
    """
    by_b: Dict[float, List[Tuple[float, float]]] = {}
    for ts, val in points:
        try:
            tsf = float(ts)
            v = float(val)
        except (TypeError, ValueError):
            continue
        if not math.isfinite(tsf) or not math.isfinite(v):
            continue
        b = _bucket_ts_monitoring_minute(tsf)
        by_b.setdefault(b, []).append((tsf, v))
    out: List[Tuple[float, float]] = []
    tol = float(tol_sec)
    for b in sorted(by_b.keys()):
        cand = by_b[b]
        near = [(t, v) for t, v in cand if abs(t - b) <= tol]
        if near:
            if how == "sum":
                out.append((b, sum(v for _, v in near)))
            else:
                out.append((b, max(v for _, v in near)))
        elif how == "sum":
            out.append((b, sum(v for _, v in cand)))
        else:
            _t_pick, v_pick = min(cand, key=lambda x: abs(x[0] - b))
            out.append((b, v_pick))
    return out


def _trim_trailing_minute_buckets(
    points: List[Tuple[float, float]],
    n: int,
) -> List[Tuple[float, float]]:
    """
    Drop the newest ``n`` minute buckets after snap — closing incomplete Prometheus tail rows.
    Keeps at least ``n + 3`` points when possible so short windows do not go empty.
    """
    if n <= 0 or not points:
        return points
    if len(points) <= n + 2:
        return points
    return points[:-n]


LARK_ENCRYPT_KEY = (
    _cfg_str("LARK_ENCRYPT_KEY")
    or _cfg_str("ENCRYPT_KEY")
    or _cfg_str("FEISHU_ENCRYPT_KEY")
    or ""
).strip()
LARK_BOT_OPEN_ID = _cfg_str("LARK_BOT_OPEN_ID", "").strip()
MONITORING_AT_MENTION_ENABLE = _lark_env_truthy("MONITORING_AT_MENTION_ENABLE")
MONITORING_AT_MENTION_ANY_TEXT = _lark_env_truthy("MONITORING_AT_MENTION_ANY_TEXT")
MONITORING_TRIGGER_REQUIRES_AT_BOT = _lark_env_truthy("MONITORING_TRIGGER_REQUIRES_AT_BOT")
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


def _im_command_matches(clean: str, cmd: str) -> bool:
    """
    True when ``clean`` is exactly ``cmd`` or starts with ``cmd`` + whitespace.
    Avoids ``/mo`` being mistaken for ``/m`` (prefix match without boundary).
    """
    c = re.sub(r"\s+", " ", (clean or "").strip().lower())
    tri = (cmd or "").strip().lower()
    if not tri or not c:
        return False
    if c == tri:
        return True
    return c.startswith(tri + " ") or c.startswith(tri + "\t")


def _text_has_monitoring_trigger(raw_text: str, clean: str) -> bool:
    _ = raw_text
    return _im_command_matches(clean or "", MONITORING_TRIGGER)


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

    When ``MONITORING_TRIGGER_REQUIRES_AT_BOT`` is true, an explicit trigger (e.g. ``/mo``) only
    runs if ``mentions`` includes this app's ``LARK_BOT_OPEN_ID``.
    """
    if _text_has_monitoring_trigger(raw_text, clean):
        if MONITORING_TRIGGER_REQUIRES_AT_BOT:
            return _lark_message_mentions_bot(mentions)
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
    Normalize IM debounce key so explicit commands / @-mention variants share one key
    (avoids two background workers when ``clean`` whitespace or mention markup differs slightly).
    """
    tri = (MONITORING_TRIGGER or "/mo").strip().lower()
    cl = re.sub(r"\s+", " ", (clean or "").strip().lower())
    if _im_command_matches(clean or "", MONITORING_TRIGGER):
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


def _fetch_library_panel_model(session: requests.Session, library_uid: str) -> Dict[str, Any]:
    """Best-effort fetch of Grafana library panel model by uid."""
    u = (library_uid or "").strip()
    if not u:
        return {}
    try:
        r = session.get(f"{GRAFANA_BASE_URL}/api/library-elements/{u}", params={"orgId": "1"}, timeout=60)
        r.raise_for_status()
        j = r.json() or {}
        # Common shapes: {"result":{"model":{...}}} or {"model":{...}}
        if isinstance(j.get("result"), dict) and isinstance(j["result"].get("model"), dict):
            return j["result"]["model"] or {}
        if isinstance(j.get("model"), dict):
            return j["model"] or {}
    except Exception:
        logger.exception("library panel fetch failed uid=%r", u[:32])
    return {}


def _panel_query_model(session: requests.Session, panel: Dict[str, Any]) -> Dict[str, Any]:
    """
    Return panel model carrying query targets.
    For library panels, Grafana dashboard JSON may only keep a lightweight ref with no targets.
    """
    if not isinstance(panel, dict):
        return {}
    targets = panel.get("targets") or []
    if isinstance(targets, list) and len(targets) > 0:
        return panel
    lp = panel.get("libraryPanel") if isinstance(panel.get("libraryPanel"), dict) else {}
    lib_uid = str(lp.get("uid") or "").strip()
    if lib_uid:
        m = _fetch_library_panel_model(session, lib_uid)
        if m:
            return m
    return panel


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


def _grafana_ds_query(
    session: requests.Session,
    datasource_uid: str,
    target: Dict[str, Any],
    start_unix: int,
    end_unix: int,
    step: int,
) -> Dict[str, Any]:
    """
    Generic Grafana datasource query fallback (for non-Prometheus targets, e.g. SQL/log-like queries).
    Returns Grafana /api/ds/query JSON.
    """
    q = copy.deepcopy(target or {})
    q["datasource"] = {"uid": datasource_uid}
    q["refId"] = str(q.get("refId") or "A")
    q["intervalMs"] = max(1000, int(step) * 1000)
    q["maxDataPoints"] = max(200, int((max(1, end_unix - start_unix) // max(1, step)) + 8))
    payload = {
        "from": str(int(start_unix) * 1000),
        "to": str(int(end_unix) * 1000),
        "queries": [q],
    }
    r = session.post(
        f"{GRAFANA_BASE_URL}/api/ds/query",
        json=payload,
        headers={"Content-Type": "application/json"},
        timeout=120,
    )
    r.raise_for_status()
    return r.json() or {}


def _ds_query_to_prometheus_like(raw: Dict[str, Any], ref_id: str) -> Dict[str, Any]:
    """
    Convert Grafana /api/ds/query frames into a Prometheus-like shape:
    {"data":{"result":[{"metric": {...}, "values":[[ts,val], ...]}, ...]}}
    so existing merge/analyze logic can stay unchanged.
    """
    out: List[Dict[str, Any]] = []
    results = raw.get("results") if isinstance(raw.get("results"), dict) else {}
    bucket = results.get(ref_id) if isinstance(results.get(ref_id), dict) else {}
    frames = bucket.get("frames") if isinstance(bucket.get("frames"), list) else []

    for fr in frames:
        if not isinstance(fr, dict):
            continue
        schema = fr.get("schema") if isinstance(fr.get("schema"), dict) else {}
        fields = schema.get("fields") if isinstance(schema.get("fields"), list) else []
        data = fr.get("data") if isinstance(fr.get("data"), dict) else {}
        cols = data.get("values") if isinstance(data.get("values"), list) else []
        if not fields or not cols:
            continue
        n = min(len(fields), len(cols))
        if n <= 0:
            continue

        names: List[str] = []
        field_objs: List[Dict[str, Any]] = []
        for i in range(n):
            f = fields[i] if isinstance(fields[i], dict) else {}
            field_objs.append(f)
            names.append(str(f.get("name") or f"f{i}"))

        def _field_series_name(idx: int, fallback: str) -> str:
            if idx < 0 or idx >= n:
                return fallback
            f = field_objs[idx] if isinstance(field_objs[idx], dict) else {}
            cfg = f.get("config") if isinstance(f.get("config"), dict) else {}
            labels = f.get("labels") if isinstance(f.get("labels"), dict) else {}
            for k in ("displayName", "displayNameFromDS"):
                v = cfg.get(k)
                if v is not None and str(v).strip():
                    return str(v).strip()
            for lk in ("series", "name", "providerid", "provider_id", "gameid", "game_id"):
                if lk in labels and str(labels.get(lk) or "").strip():
                    return str(labels.get(lk)).strip()
            if labels:
                if len(labels) == 1:
                    try:
                        only_v = next(iter(labels.values()))
                        if str(only_v or "").strip():
                            return str(only_v).strip()
                    except Exception:
                        pass
                compact = ",".join(
                    f"{str(k).strip()}={str(v).strip()}"
                    for k, v in labels.items()
                    if str(k).strip() and str(v).strip()
                ).strip(",")
                if compact:
                    return compact
            return fallback

        row_len = 0
        for i in range(n):
            c = cols[i]
            if isinstance(c, list):
                row_len = max(row_len, len(c))
        if row_len <= 0:
            continue

        ts_idx = -1
        val_idx = -1
        label_idx = -1
        for i, nm in enumerate(names):
            nl = nm.strip().lower()
            if ts_idx < 0 and nl in ("time", "t", "ts", "timestamp", "datetime"):
                ts_idx = i
            if val_idx < 0 and nl in ("value", "val", "count", "total"):
                val_idx = i
            if label_idx < 0 and nl in ("gameid", "game_id", "providerid", "provider_id", "name", "series"):
                label_idx = i
        if ts_idx < 0:
            for i in range(n):
                c = cols[i]
                if not isinstance(c, list) or not c:
                    continue
                vv = c[0]
                try:
                    fv = float(vv)
                except (TypeError, ValueError):
                    continue
                if fv > 1e9:
                    ts_idx = i
                    break
        if val_idx < 0:
            for i in range(n):
                if i == ts_idx:
                    continue
                c = cols[i]
                if not isinstance(c, list) or not c:
                    continue
                try:
                    float(c[0])
                    val_idx = i
                    break
                except (TypeError, ValueError):
                    continue
        if ts_idx < 0 or val_idx < 0:
            continue

        by_label: Dict[str, List[List[float]]] = {}
        numeric_idxs: List[int] = []
        for i in range(n):
            if i == ts_idx or i == label_idx:
                continue
            c = cols[i]
            if not isinstance(c, list) or not c:
                continue
            sample = None
            for sv in c:
                if sv is None or sv == "":
                    continue
                sample = sv
                break
            if sample is None:
                continue
            try:
                float(sample)
            except (TypeError, ValueError):
                continue
            numeric_idxs.append(i)
        wide_mode = label_idx < 0 and len(numeric_idxs) > 1

        for r_i in range(row_len):
            try:
                ts_raw = cols[ts_idx][r_i]
            except Exception:
                continue
            try:
                ts = float(ts_raw)
            except (TypeError, ValueError):
                continue
            if ts > 1e12:
                ts = ts / 1000.0
            if wide_mode:
                for vi in numeric_idxs:
                    try:
                        val = float(cols[vi][r_i])
                    except Exception:
                        continue
                    lbl = _field_series_name(vi, str(names[vi]).strip() or "value")
                    by_label.setdefault(lbl, []).append([ts, val])
            else:
                try:
                    val_raw = cols[val_idx][r_i]
                    val = float(val_raw)
                except Exception:
                    continue
                lbl = "value"
                if label_idx >= 0:
                    try:
                        lbl = str(cols[label_idx][r_i]).strip() or "value"
                    except Exception:
                        lbl = "value"
                elif 0 <= val_idx < len(names):
                    lbl = _field_series_name(val_idx, str(names[val_idx]).strip() or "value")
                by_label.setdefault(lbl, []).append([ts, val])

        for lbl, pairs in by_label.items():
            pairs.sort(key=lambda p: p[0])
            out.append({"metric": {"series": lbl, "name": lbl}, "values": pairs})

    return {"data": {"result": out}}


def _fetch_panel_series_by_title(
    panel_title: str,
    session: Optional[requests.Session] = None,
    start_unix: Optional[int] = None,
    end_unix: Optional[int] = None,
) -> Dict[str, Any]:
    if start_unix is not None and end_unix is not None:
        start = int(start_unix)
        end = int(end_unix)
        if start >= end:
            raise ValueError("query window: start_unix must be < end_unix")
    else:
        ao = int(MONITORING_QUERY_ALIGNED_START_OFFSET_MINUTES)
        bo = int(MONITORING_QUERY_ALIGNED_END_OFFSET_MINUTES)
        if ao > 0 and bo > 0 and ao > bo:
            cur_m = float(_bucket_ts_monitoring_minute(time.time()))
            end = int(cur_m - bo * 60)
            start = int(cur_m - ao * 60)
            if start >= end:
                raise ValueError(
                    "aligned query window: start must be < end "
                    f"(start_offset={ao} end_offset={bo} start={start} end={end})"
                )
        else:
            if ao > 0 or bo > 0:
                logger.warning(
                    "MONITORING_QUERY_ALIGNED_* ignored (need START>0, END>0, START>END); "
                    "using GRAFANA_QUERY_LOOKBACK_SECONDS + GRAFANA_QUERY_END_LAG_SECONDS"
                )
            lag = max(0, int(GRAFANA_QUERY_END_LAG_SECONDS))
            end = int(time.time()) - lag
            start = end - GRAFANA_QUERY_LOOKBACK_SECONDS
    sess = session or grafana_login_session()
    dash = _fetch_dashboard_model(sess, GRAFANA_DASHBOARD_UID)
    panel = _find_panel(dash, panel_title)
    if not panel:
        raise ValueError(f'Panel titled "{panel_title}" not found on dashboard {GRAFANA_DASHBOARD_UID}')

    q_panel = _panel_query_model(sess, panel)
    panel_ds = _datasource_uid(q_panel.get("datasource")) or _datasource_uid(panel.get("datasource"))
    series_out: List[Dict[str, Any]] = []
    for t in q_panel.get("targets") or []:
        expr = (
            (t.get("expr") or "")
            or (t.get("query") or "")
            or (t.get("rawSql") or "")
        )
        expr = str(expr).strip()
        if not expr:
            continue
        ds_uid = _datasource_uid(t.get("datasource")) or panel_ds
        if not ds_uid:
            logger.warning("skip target without datasource uid: %s", t.get("refId"))
            continue
        try:
            raw = _prometheus_query_range(sess, ds_uid, expr, start, end, GRAFANA_QUERY_STEP)
        except requests.HTTPError as e:
            # Non-Prometheus datasource often returns 405 on /api/v1/query_range.
            code = int(getattr(getattr(e, "response", None), "status_code", 0) or 0)
            if code in (400, 404, 405, 415, 422):
                logger.info(
                    'panel "%s" target %s query_range HTTP %s -> fallback api/ds/query',
                    panel_title,
                    t.get("refId"),
                    code,
                )
                raw_ds = _grafana_ds_query(sess, ds_uid, t, start, end, GRAFANA_QUERY_STEP)
                raw = _ds_query_to_prometheus_like(raw_ds, str(t.get("refId") or "A"))
            else:
                raise
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
        logger.warning(
            'No queryable Prometheus targets on panel "%s" (direct targets=%s, library_uid=%r)',
            panel_title,
            len(panel.get("targets") or []),
            ((panel.get("libraryPanel") or {}).get("uid") if isinstance(panel.get("libraryPanel"), dict) else ""),
        )
    return {
        "panelTitle": panel_title,
        "dashboardUid": GRAFANA_DASHBOARD_UID,
        "window": {"startUnix": start, "endUnix": end, "stepSeconds": GRAFANA_QUERY_STEP},
        "series": series_out,
    }


def fetch_request_total_1m_series(
    session: Optional[requests.Session] = None,
    start_unix: Optional[int] = None,
    end_unix: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Same data as Grafana panel「请求总数/1m」via Prometheus ``query_range`` (Grafana proxy).

    Default window (unless ``start_unix``/``end_unix`` passed, or watchdog overrides):

    - If ``MONITORING_QUERY_ALIGNED_START_OFFSET_MINUTES`` and ``…_END_…`` are both **> 0** and
      **START > END**: use minute-aligned bounds from :func:`_bucket_ts_monitoring_minute` —
      ``start = cur_min − START×60``, ``end = cur_min − END×60`` (both ``…:00`` in the configured TZ).
    - Else: ``end = now − GRAFANA_QUERY_END_LAG_SECONDS``, ``start = end − GRAFANA_QUERY_LOOKBACK_SECONDS``.

    Step is ``GRAFANA_QUERY_STEP``. Watchdog passes explicit ``start_unix``/``end_unix`` unchanged.
    """
    return _fetch_panel_series_by_title(
        GRAFANA_PANEL_TITLE,
        session=session,
        start_unix=start_unix,
        end_unix=end_unix,
    )


def _monitoring_watch_daily_quiet_tod_bounds() -> Tuple[int, int]:
    """
    Local-time quiet window as (start_tod_seconds, end_tod_seconds) with **end exclusive**:
    quiet when ``tod >= start`` OR ``tod < end`` (handles wrap past midnight).
    Default: [23:59:00, 00:10:00) → about 11 minutes.
    Returns ``(-1, -1)`` when ``MONITORING_WATCH_QUIET_WINDOW_ENABLE=0``.
    """
    if not _lark_env_truthy("MONITORING_WATCH_QUIET_WINDOW_ENABLE"):
        return -1, -1
    sh = max(0, min(23, _cfg_int("MONITORING_WATCH_QUIET_START_HOUR", 23)))
    sm = max(0, min(59, _cfg_int("MONITORING_WATCH_QUIET_START_MINUTE", 59)))
    eh = max(0, min(23, _cfg_int("MONITORING_WATCH_QUIET_END_HOUR", 0)))
    em = max(0, min(59, _cfg_int("MONITORING_WATCH_QUIET_END_MINUTE", 10)))
    start_sec = sh * 3600 + sm * 60
    end_sec = eh * 3600 + em * 60
    return start_sec, end_sec


def _monitoring_watch_in_daily_quiet_local(now: Optional[float] = None) -> bool:
    """True if current **local** time lies in the configured daily quiet window."""
    start_sec, end_sec = _monitoring_watch_daily_quiet_tod_bounds()
    if start_sec < 0:
        return False
    t = time.time() if now is None else float(now)
    lt = time.localtime(t)
    tod = lt.tm_hour * 3600 + lt.tm_min * 60 + lt.tm_sec
    if start_sec < end_sec:
        return start_sec <= tod < end_sec
    if start_sec == end_sec:
        return False
    return tod >= start_sec or tod < end_sec


def _monitoring_watch_eval_window_unix(now: Optional[float] = None) -> Tuple[int, int]:
    """
    Minute-aligned Prometheus window for **watchdog only** (exclude current incomplete minute by default).
    Defaults: start = floor_to_minute(now) − 6m, end = floor_to_minute(now) − 1m
    (e.g. at 12:46:30 → 12:40:00 .. 12:45:00 unix, inclusive for query_range with step 60).
    """
    t = time.time() if now is None else float(now)
    end_off = max(0, _cfg_int("MONITORING_WATCH_EVAL_END_OFFSET_MINUTES", 1))
    start_off = max(end_off + 1, _cfg_int("MONITORING_WATCH_EVAL_START_OFFSET_MINUTES", 6))
    t_floor = int(t // 60) * 60
    end_unix = t_floor - end_off * 60
    start_unix = t_floor - start_off * 60
    return start_unix, end_unix


def fetch_monitoring_payload(
    session: Optional[requests.Session] = None,
    *,
    for_watchdog: bool = False,
) -> Dict[str, Any]:
    sess = session or grafana_login_session()
    w_start: Optional[int] = None
    w_end: Optional[int] = None
    if for_watchdog:
        if _lark_env_truthy("MONITORING_WATCH_MATCH_REPORT_WINDOW"):
            logger.info(
                "fetch_monitoring_payload watchdog eval uses **report** window "
                "(MONITORING_WATCH_MATCH_REPORT_WINDOW=1; same lookback/lag as /monitoring)"
            )
        else:
            w_start, w_end = _monitoring_watch_eval_window_unix()
            logger.info(
                "fetch_monitoring_payload watchdog eval window unix %s..%s (aligned minutes)",
                w_start,
                w_end,
            )
    primary = fetch_request_total_1m_series(session=sess, start_unix=w_start, end_unix=w_end)
    extra: List[Dict[str, Any]] = []
    if _lark_env_truthy("MONITORING_9280_ENABLE"):
        try:
            p9280 = _fetch_panel_series_by_title(
                GRAFANA_PANEL_TITLE_9280,
                session=sess,
                start_unix=w_start,
                end_unix=w_end,
            )
            extra.append({"kind": "9280_push", "payload": p9280})
        except Exception:
            logger.exception("fetch 9280 panel failed (optional monitor)")
    if _lark_env_truthy("MONITORING_DEPOSIT_ENABLE"):
        try:
            pd = _fetch_panel_series_by_title(
                GRAFANA_PANEL_TITLE_DEPOSIT,
                session=sess,
                start_unix=w_start,
                end_unix=w_end,
            )
            extra.append({"kind": "deposit", "payload": pd})
        except Exception:
            logger.exception("fetch deposit panel failed (optional monitor)")
    if _lark_env_truthy("MONITORING_WITHDRAW_ENABLE"):
        try:
            pw = _fetch_panel_series_by_title(
                GRAFANA_PANEL_TITLE_WITHDRAW,
                session=sess,
                start_unix=w_start,
                end_unix=w_end,
            )
            extra.append({"kind": "withdraw", "payload": pw})
        except Exception:
            logger.exception("fetch withdraw panel failed (optional monitor)")
    if _lark_env_truthy("MONITORING_PROVIDER_JILI_ENABLE"):
        try:
            ppj = _fetch_panel_series_by_title(
                GRAFANA_PANEL_TITLE_PROVIDER_JILI,
                session=sess,
                start_unix=w_start,
                end_unix=w_end,
            )
            extra.append({"kind": "provider_jili", "payload": ppj})
        except Exception:
            logger.exception("fetch provider JILI panel failed (optional monitor)")
    if _lark_env_truthy("MONITORING_PROVIDER_GENERAL_ENABLE"):
        try:
            ppg = _fetch_panel_series_by_title(
                GRAFANA_PANEL_TITLE_PROVIDER_GENERAL,
                session=sess,
                start_unix=w_start,
                end_unix=w_end,
            )
            extra.append({"kind": "provider_general", "payload": ppg})
        except Exception:
            logger.exception("fetch provider GENERAL panel failed (optional monitor)")
    if _lark_env_truthy("MONITORING_PROVIDER_INHOUSE_ENABLE"):
        try:
            ppi = _fetch_panel_series_by_title(
                GRAFANA_PANEL_TITLE_PROVIDER_INHOUSE,
                session=sess,
                start_unix=w_start,
                end_unix=w_end,
            )
            extra.append({"kind": "provider_inhouse", "payload": ppi})
        except Exception:
            logger.exception("fetch provider INHOUSE panel failed (optional monitor)")
    if _lark_env_truthy("MONITORING_GAMES_JILI_ENABLE"):
        try:
            pgj = _fetch_panel_series_by_title(
                GRAFANA_PANEL_TITLE_GAMES_JILI,
                session=sess,
                start_unix=w_start,
                end_unix=w_end,
            )
            extra.append({"kind": "games_jili", "payload": pgj})
        except Exception:
            logger.exception("fetch games JILI panel failed (optional monitor)")
    if _lark_env_truthy("MONITORING_GAMES_GENERAL_ENABLE"):
        try:
            pgg = _fetch_panel_series_by_title(
                GRAFANA_PANEL_TITLE_GAMES_GENERAL,
                session=sess,
                start_unix=w_start,
                end_unix=w_end,
            )
            extra.append({"kind": "games_general", "payload": pgg})
        except Exception:
            logger.exception("fetch games GENERAL panel failed (optional monitor)")
    if _lark_env_truthy("MONITORING_GAMES_INHOUSE_ENABLE"):
        try:
            pgi = _fetch_panel_series_by_title(
                GRAFANA_PANEL_TITLE_GAMES_INHOUSE,
                session=sess,
                start_unix=w_start,
                end_unix=w_end,
            )
            extra.append({"kind": "games_inhouse", "payload": pgi})
        except Exception:
            logger.exception("fetch games INHOUSE panel failed (optional monitor)")
    if extra:
        primary["extraPanels"] = extra
    return primary


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


def _split_text_for_lark(text: str, max_chars: int = 3200) -> List[str]:
    """
    Split long text into multiple chunks to avoid platform length truncation.
    Prefer paragraph/line boundaries; hard-cut only when necessary.
    """
    raw = str(text or "")
    if max_chars <= 200:
        max_chars = 200
    if len(raw) <= max_chars:
        return [raw]

    chunks: List[str] = []
    cur = ""

    def _flush() -> None:
        nonlocal cur
        if cur:
            chunks.append(cur)
            cur = ""

    def _push_piece(piece: str, sep: str = "\n\n") -> None:
        nonlocal cur
        if not piece:
            return
        if len(piece) > max_chars:
            _flush()
            lines = piece.split("\n")
            buf = ""
            for ln in lines:
                if len(ln) > max_chars:
                    if buf:
                        chunks.append(buf)
                        buf = ""
                    i = 0
                    while i < len(ln):
                        chunks.append(ln[i : i + max_chars])
                        i += max_chars
                    continue
                trial = f"{buf}\n{ln}" if buf else ln
                if len(trial) <= max_chars:
                    buf = trial
                else:
                    if buf:
                        chunks.append(buf)
                    buf = ln
            if buf:
                chunks.append(buf)
            return

        trial = f"{cur}{sep}{piece}" if cur else piece
        if len(trial) <= max_chars:
            cur = trial
        else:
            _flush()
            cur = piece

    for para in raw.split("\n\n"):
        _push_piece(para, "\n\n")
    _flush()
    return chunks or [raw[:max_chars]]


def _lark_send_text_auto(receive_id_type: str, receive_id: str, text: str, max_chars: int = 3200) -> None:
    chunks = _split_text_for_lark(text, max_chars=max_chars)
    total = len(chunks)
    for i, c in enumerate(chunks, 1):
        body = c
        if total > 1:
            body = f"[{i}/{total}]\n{c}"
        _lark_send_text(receive_id_type, receive_id, body)


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
    s = (reply or "")[:3000]
    s = s.replace("```", "'''")
    return s


def _monitoring_card_body_md_strip_title(reply: str) -> str:
    r = (reply or "").strip()
    dup = f"[{GRAFANA_PANEL_TITLE}] graph"
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


# ---------------------------------------------------------------------------
# /mute — per-channel alert suppression (in-process)
# ---------------------------------------------------------------------------
_MUTE_DURATION_CHOICES: List[Tuple[str, int]] = [
    ("15 minutes", 900),
    ("30 minutes", 1800),
    ("1 hour", 3600),
    ("2 hours", 7200),
    ("3 hours", 10800),
    ("4 hours", 14400),
    ("8 hours", 28800),
    ("12 hours", 43200),
    ("1 day", 86400),
]


def _monitoring_mutable_channels() -> List[Tuple[str, str]]:
    """
    (channel_id, display_label) for enabled monitors. channel_id matches extraPanels ``kind`` or ``http``.
    """
    out: List[Tuple[str, str]] = [("http", f"HTTP · {GRAFANA_PANEL_TITLE}")]
    if _lark_env_truthy("MONITORING_9280_ENABLE"):
        out.append(("9280_push", f"9280 · {GRAFANA_PANEL_TITLE_9280}"))
    if _lark_env_truthy("MONITORING_DEPOSIT_ENABLE"):
        out.append(("deposit", f"Deposit · {GRAFANA_PANEL_TITLE_DEPOSIT}"))
    if _lark_env_truthy("MONITORING_WITHDRAW_ENABLE"):
        out.append(("withdraw", f"Withdraw · {GRAFANA_PANEL_TITLE_WITHDRAW}"))
    if _lark_env_truthy("MONITORING_PROVIDER_JILI_ENABLE"):
        out.append(("provider_jili", f"Provider JILI"))
    if _lark_env_truthy("MONITORING_PROVIDER_GENERAL_ENABLE"):
        out.append(("provider_general", "Provider GENERAL"))
    if _lark_env_truthy("MONITORING_PROVIDER_INHOUSE_ENABLE"):
        out.append(("provider_inhouse", "Provider INHOUSE"))
    if _lark_env_truthy("MONITORING_GAMES_JILI_ENABLE"):
        out.append(("games_jili", f"Games JILI"))
    if _lark_env_truthy("MONITORING_GAMES_GENERAL_ENABLE"):
        out.append(("games_general", "Games GENERAL"))
    if _lark_env_truthy("MONITORING_GAMES_INHOUSE_ENABLE"):
        out.append(("games_inhouse", "Games INHOUSE"))
    return out


def _monitoring_mutable_channel_ids() -> Set[str]:
    return {c for c, _ in _monitoring_mutable_channels()}


def _mute_session_key(chat_id: str, operator_open_id: str) -> str:
    c = (chat_id or "").strip()
    o = (operator_open_id or "").strip()
    return f"{c}\n{o}"


def _monitoring_alert_channel_muted(channel: str) -> bool:
    until = _MONITORING_MUTE_UNTIL.get((channel or "").strip(), 0.0)
    return time.time() < float(until or 0.0)


def _mute_purge_expired() -> None:
    now = time.time()
    dead = [k for k, t in _MONITORING_MUTE_UNTIL.items() if float(t or 0.0) <= now]
    for k in dead:
        try:
            del _MONITORING_MUTE_UNTIL[k]
        except KeyError:
            pass


def _mute_apply_channels(channels: Set[str], duration_sec: float) -> Dict[str, float]:
    """Returns channel -> expiry unix for confirmation text."""
    now = time.time()
    dur = max(1.0, float(duration_sec))
    applied: Dict[str, float] = {}
    with _monitoring_reply_dispatch_lock:
        allowed = _monitoring_mutable_channel_ids()
        for ch in channels:
            c = (ch or "").strip()
            if c not in allowed:
                continue
            exp = now + dur
            prev = float(_MONITORING_MUTE_UNTIL.get(c, 0.0) or 0.0)
            if prev > exp:
                exp = prev
            _MONITORING_MUTE_UNTIL[c] = exp
            applied[c] = exp
    return applied


def _mute_clear_all_locked() -> None:
    _MONITORING_MUTE_UNTIL.clear()
    _mute_pending_selections.clear()


def _mute_toast_response(content: str, toast_type: str = "info") -> Dict[str, Any]:
    """HTTP card.action synchronous response body (Feishu shows a small toast)."""
    return {"toast": {"type": toast_type, "content": (content or "")[:500]}}


def _mute_channel_display_label(ch: str) -> str:
    for cid, lbl in _monitoring_mutable_channels():
        if cid == ch:
            return lbl
    return ch


def _mute_selection_card_elements(rid_t: str, rid: str) -> List[Dict[str, Any]]:
    rt = (rid_t or "").strip()
    rv = (rid or "").strip()
    base_rid: Dict[str, Any] = {}
    if rt in ("chat_id", "open_id") and rv:
        base_rid = {"rid_t": rt, "rid": rv}

    elements: List[Dict[str, Any]] = [
        {
            "tag": "markdown",
            "content": (
                "**Mute alerts (multi-select)**\n\n"
                "1. Tap a monitor below repeatedly to **add/remove** it from the selection (see toast).\n"
                "2. **Mute all & duration** selects every monitor in this list at once.\n"
                "3. When ready, tap **Next: choose duration**.\n"
                "4. **Cancel** clears this selection session."
            ),
        }
    ]
    for ch, short_lbl in _monitoring_mutable_channels():
        payload = dict(base_rid)
        payload.update({"k": "mute_btn", "v": "toggle", "ch": ch})
        elements.append(
            _monitoring_card_v2_callback_button(
                short_lbl[:80],
                "default",
                _monitoring_card_callback_payload_strings(payload),
                element_id=f"mt_{hashlib.sha256(ch.encode()).hexdigest()[:10]}",
            )
        )

    row_advance = dict(base_rid)
    row_advance.update({"k": "mute_btn", "v": "next"})
    row_all = dict(base_rid)
    row_all.update({"k": "mute_btn", "v": "all"})
    row_cancel = dict(base_rid)
    row_cancel.update({"k": "mute_btn", "v": "cancel_sel"})
    elements.append(
        {
            "tag": "column_set",
            "flex_mode": "none",
            "horizontal_spacing": "default",
            "horizontal_align": "left",
            "columns": [
                {
                    "tag": "column",
                    "width": "weighted",
                    "weight": 1,
                    "elements": [
                        _monitoring_card_v2_callback_button(
                            "Next: choose duration",
                            "primary",
                            _monitoring_card_callback_payload_strings(row_advance),
                            element_id="mute_next",
                        )
                    ],
                },
                {
                    "tag": "column",
                    "width": "weighted",
                    "weight": 1,
                    "elements": [
                        _monitoring_card_v2_callback_button(
                            "Mute all & duration",
                            "danger",
                            _monitoring_card_callback_payload_strings(row_all),
                            element_id="mute_all",
                        )
                    ],
                },
            ],
        }
    )
    elements.append(
        _monitoring_card_v2_callback_button(
            "Cancel",
            "default",
            _monitoring_card_callback_payload_strings(row_cancel),
            element_id="mute_cancel",
        )
    )
    return elements


def _mute_duration_card_elements(
    rid_t: str, rid: str, operator_open_id: str, chat_id: str
) -> List[Dict[str, Any]]:
    rt = (rid_t or "").strip()
    rv = (rid or "").strip()
    oid = (operator_open_id or "").strip()
    cid = (chat_id or "").strip()
    base: Dict[str, Any] = {"k": "mute_btn", "oid": oid, "cid": cid}
    if rt in ("chat_id", "open_id") and rv:
        base["rid_t"] = rt
        base["rid"] = rv

    elements: List[Dict[str, Any]] = [
        {"tag": "markdown", "content": "**Choose mute duration** (applies to selected monitors)"},
    ]
    for label, secs in _MUTE_DURATION_CHOICES:
        pl = dict(base)
        pl["v"] = "apply"
        pl["sec"] = str(int(secs))
        elements.append(
            _monitoring_card_v2_callback_button(
                label,
                "primary",
                _monitoring_card_callback_payload_strings(pl),
                element_id=f"mute_d_{secs}"[:20],
            )
        )
    pl_cancel = dict(base)
    pl_cancel["v"] = "cancel_sel"
    elements.append(
        _monitoring_card_v2_callback_button(
            "Back / cancel selection",
            "default",
            _monitoring_card_callback_payload_strings(pl_cancel),
            element_id="mute_back",
        )
    )
    return elements


def _mute_selection_card_dict(rid_t: str, rid: str) -> Dict[str, Any]:
    return {
        "schema": "2.0",
        "config": {"update_multi": True, "wide_screen_mode": True},
        "header": {
            "template": "orange",
            "title": {"tag": "plain_text", "content": "Mute monitoring alerts"},
            "subtitle": {
                "tag": "plain_text",
                "content": (MONITORING_MUTE_TRIGGER or "/m").strip()[:190],
            },
        },
        "body": {"elements": _mute_selection_card_elements(rid_t, rid)},
    }


def _mute_duration_card_dict(rid_t: str, rid: str, operator_open_id: str, chat_id: str) -> Dict[str, Any]:
    return {
        "schema": "2.0",
        "config": {"update_multi": True, "wide_screen_mode": True},
        "header": {
            "template": "orange",
            "title": {"tag": "plain_text", "content": "Choose mute duration"},
            "subtitle": {
                "tag": "plain_text",
                "content": (MONITORING_MUTE_TRIGGER or "/m").strip()[:190],
            },
        },
        "body": {"elements": _mute_duration_card_elements(rid_t, rid, operator_open_id, chat_id)},
    }


def _mute_send_duration_card_async(
    rid_t: str, rid: str, operator_open_id: str, chat_id: str
) -> None:
    def _run() -> None:
        try:
            rt = (rid_t or "").strip()
            rv = (rid or "").strip()
            if rt not in ("chat_id", "open_id") or not rv:
                return
            card = _mute_duration_card_dict(rt, rv, operator_open_id, chat_id)
            _lark_send_interactive_card(rt, rv, card)
        except Exception:
            logger.exception("mute: send duration card failed")

    threading.Thread(target=_run, daemon=True, name="mute-duration-card").start()


def _mute_send_selection_card_worker(chat_id: str, open_id: str, debounce_key: str) -> None:
    try:
        rt = "chat_id" if (chat_id or "").strip() else "open_id"
        rv = (chat_id or open_id or "").strip()
        if not rv:
            logger.warning("mute: missing receive_id")
            return
        card = _mute_selection_card_dict(rt, rv)
        _lark_send_interactive_card(rt, rv, card)
    except Exception:
        logger.exception("mute: send selection card failed")
    finally:
        with _monitoring_reply_dispatch_lock:
            _monitoring_inflight_keys.discard(debounce_key)


def _cancelmute_worker(chat_id: str, open_id: str, debounce_key: str) -> None:
    try:
        rt = "chat_id" if (chat_id or "").strip() else "open_id"
        rv = (chat_id or open_id or "").strip()
        if not rv:
            return
        with _monitoring_reply_dispatch_lock:
            _mute_clear_all_locked()
        _lark_send_text(
            rt,
            rv,
            "**All** alert mutes have been cleared — new alerts will be delivered normally.\n"
            "(Mute state is kept in memory only and is lost when the process restarts.)",
        )
    except Exception:
        logger.exception("cancelmute failed")
    finally:
        with _monitoring_reply_dispatch_lock:
            _monitoring_inflight_keys.discard(debounce_key)


def _monitoring_at_mention_help_text() -> str:
    mo = (MONITORING_TRIGGER or "/mo").strip()
    m = (MONITORING_MUTE_TRIGGER or "/m").strip()
    c = (MONITORING_CANCELMUTE_TRIGGER or "/c").strip()
    return (
        "Commands:\n"
        f"- `{mo}` — Grafana monitoring summary\n"
        f"- `{m}` — mute alerts (interactive card)\n"
        f"- `{c}` — clear all mutes"
    )


def _monitoring_at_mention_help_worker(chat_id: str, open_id: str, debounce_key: str) -> None:
    try:
        rt = "chat_id" if (chat_id or "").strip() else "open_id"
        rv = (chat_id or open_id or "").strip()
        if not rv:
            return
        _lark_send_text(rt, rv, _monitoring_at_mention_help_text())
    except Exception:
        logger.exception("at-mention command help send failed")
    finally:
        with _monitoring_reply_dispatch_lock:
            _monitoring_inflight_keys.discard(debounce_key)


def _mute_card_action_dispatch(data: Dict[str, Any], val: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Return HTTP response dict for card.action when toast should be shown synchronously."""
    ev_id = _lark_im_payload_event_id(data)
    with _monitoring_reply_dispatch_lock:
        if ev_id and ev_id in _monitoring_card_action_event_ids:
            return _mute_toast_response("Duplicate click ignored", "info")
        if ev_id:
            _monitoring_card_action_event_ids.add(ev_id)
            if len(_monitoring_card_action_event_ids) > 2000:
                _monitoring_card_action_event_ids.clear()
                _monitoring_card_action_event_ids.add(ev_id)

    chat_id, open_id = _lark_card_action_target_ids(data)
    rid_t = _lark_dict_pick_str(val, "rid_t", "receive_id_type")
    rid = _lark_dict_pick_str(val, "rid", "receive_id")
    if rid_t == "chat_id" and rid:
        chat_id = rid
        open_id = ""
    elif rid_t == "open_id" and rid:
        open_id = rid

    op_open = open_id
    if not op_open:
        ev = data.get("event") if isinstance(data.get("event"), dict) else {}
        op = ev.get("operator") if isinstance(ev.get("operator"), dict) else {}
        op_id = op.get("operator_id") if isinstance(op.get("operator_id"), dict) else {}
        op_open = _lark_dict_pick_str(op_id, "open_id", "openId", "user_id", "userId")

    sk = _mute_session_key(chat_id, op_open or "")
    v = _lark_dict_pick_str(val, "v")
    allowed = _monitoring_mutable_channel_ids()

    if v == "toggle":
        ch = _lark_dict_pick_str(val, "ch").strip()
        if ch not in allowed:
            return _mute_toast_response("Unknown monitor", "warning")
        with _monitoring_reply_dispatch_lock:
            pend = _mute_pending_selections.setdefault(sk, set())
            if ch in pend:
                pend.discard(ch)
                msg = f"Removed: {_mute_channel_display_label(ch)} ({len(pend)} selected)"
            else:
                pend.add(ch)
                msg = f"Added: {_mute_channel_display_label(ch)} ({len(pend)} selected)"
        return _mute_toast_response(msg, "success")

    if v == "all":
        with _monitoring_reply_dispatch_lock:
            _mute_pending_selections[sk] = set(allowed)
        _mute_send_duration_card_async(rid_t, rid, op_open or "", chat_id)
        return _mute_toast_response("All monitors selected — pick a duration", "success")

    if v == "cancel_sel":
        with _monitoring_reply_dispatch_lock:
            _mute_pending_selections.pop(sk, None)
        return _mute_toast_response("Selection cleared", "info")

    if v == "next":
        with _monitoring_reply_dispatch_lock:
            pend = _mute_pending_selections.get(sk) or set()
            pend = set(pend)
        if not pend:
            return _mute_toast_response("Select at least one monitor first", "warning")
        _mute_send_duration_card_async(rid_t, rid, op_open or "", chat_id)
        return _mute_toast_response("Choose a duration", "success")

    if v == "apply":
        oid = _lark_dict_pick_str(val, "oid").strip()
        cid = _lark_dict_pick_str(val, "cid").strip()
        sk2 = _mute_session_key(cid, oid)
        try:
            sec = int(float(_lark_dict_pick_str(val, "sec") or "0"))
        except (TypeError, ValueError):
            sec = 0
        if sec <= 0:
            return _mute_toast_response("Invalid duration", "warning")
        with _monitoring_reply_dispatch_lock:
            pend = set(_mute_pending_selections.pop(sk2, set()) or set())
        if not pend:
            return _mute_toast_response(
                f"Nothing to mute — run {MONITORING_MUTE_TRIGGER} and select monitors again", "warning"
            )
        applied = _mute_apply_channels(pend, float(sec))
        if not applied:
            return _mute_toast_response("Could not apply mute", "warning")
        lines = [f"Alert mute enabled for {len(applied)} monitor(s):"]
        for ch, exp in sorted(applied.items(), key=lambda kv: kv[0]):
            lines.append(f"- {_mute_channel_display_label(ch)} until {_fmt_ts_short(exp)}")
        summary = "\n".join(lines)
        try:
            rt = (rid_t or "").strip()
            rv = (rid or "").strip()
            if rt in ("chat_id", "open_id") and rv:
                _lark_send_text(rt, rv, summary)
        except Exception:
            logger.exception("mute: confirmation text send failed")
        return _mute_toast_response("Mute applied", "success")

    return _mute_toast_response("Unknown action", "warning")


def _lark_dispatch_card_action(data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Route card.action; optional dict → merge into HTTP 200 JSON (toast)."""
    val = _lark_card_action_value(data)
    k = _lark_dict_pick_str(val, "k")
    if k == "mute_btn":
        _mute_purge_expired()
        return _mute_card_action_dispatch(data, val)
    if k == "monitoring_btn":
        _handle_monitoring_card_action(data)
        return None
    logger.info("card.action ignored value=%r", val or None)
    return None


def _monitoring_interactive_card_dict(
    reply: str,
    receive_id_type: str,
    receive_id: str,
    lark_img_key: Optional[str] = None,
) -> Dict[str, Any]:
    """Feishu card JSON v2 — markdown card, optional embedded PNG."""
    title = "📊 GRAFANA GRAPH"
    is_alert_card = (reply or "").lstrip().startswith("[ALERT]")
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
            "subtitle": {
                "tag": "plain_text",
                "content": "Alert Triggered" if is_alert_card else "Grafana · monitoring",
            },
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
    # Card markdown body has strict cap; for long replies, send split text messages directly.
    # Keep this threshold conservative to avoid card-side clipping.
    if len(reply or "") > 3000:
        _lark_send_text_auto(receive_id_type, rid, reply, max_chars=3200)
        return False, False
    if MONITORING_MESSAGE_CARD_ENABLE:
        try:
            card = _monitoring_interactive_card_dict(reply, receive_id_type, rid, lark_img_key)
            _lark_send_interactive_card(receive_id_type, rid, card)
            return True, bool((lark_img_key or "").strip())
        except Exception as e:
            logger.warning(
                "monitoring interactive card failed (%s) — fallback to plain text; "
                'check app permission "Send message cards".',
                e,
            )
    _lark_send_text_auto(receive_id_type, rid, reply, max_chars=3200)
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
    Grafana 12+：收起左侧 mega-menu，让主 dashboard 占满宽度。
    ``#dock-menu-button``（aria-label Dock menu）常在 ``[data-testid='data-testid navigation mega-menu']``
    对话框内；使用 ``visible`` 等待 + ``force=True`` 避免被遮罩/动画挡住导致 silent 失败。
    若仍无按钮则尝试打开 #mega-menu-toggle 后再点 Dock。
    """
    if not _lark_env_truthy("GRAFANA_SCREENSHOT_DOCK_NAV"):
        return
    t = min(25000, max(5000, int(timeout_ms)))
    try:
        page.locator("#reactRoot").wait_for(state="visible", timeout=t)
    except Exception:
        pass

    def _click_dock_js() -> bool:
        """Grafana/React 有时拦截 Playwright 合成点击；DOM 内连续两次完整 click（等同双击）。"""
        try:
            r = page.evaluate(
                """() => {
                  const mega = document.querySelector(
                    '[data-testid="data-testid navigation mega-menu"]'
                  );
                  let dock = mega ? mega.querySelector('#dock-menu-button') : null;
                  if (!dock) dock = document.querySelector('#dock-menu-button');
                  if (!dock) {
                    const all = Array.from(document.querySelectorAll('button[aria-label="Dock menu"]'));
                    dock = all[0] || null;
                  }
                  if (!dock) return 'missing';
                  try { dock.scrollIntoView({ block: 'center', inline: 'center' }); } catch (e) {}
                  try { dock.focus({ preventScroll: true }); } catch (e) {}
                  const v = window;
                  const o = { bubbles: true, cancelable: true, view: v };
                  const fireOnce = () => {
                    dock.dispatchEvent(new MouseEvent('pointerover', o));
                    dock.dispatchEvent(new MouseEvent('mouseover', o));
                    dock.dispatchEvent(new MouseEvent('pointerdown', o));
                    dock.dispatchEvent(new MouseEvent('mousedown', o));
                    dock.dispatchEvent(new MouseEvent('pointerup', o));
                    dock.dispatchEvent(new MouseEvent('mouseup', o));
                    dock.dispatchEvent(new MouseEvent('click', o));
                    if (typeof dock.click === 'function') dock.click();
                  };
                  fireOnce();
                  fireOnce();
                  try { dock.dispatchEvent(new MouseEvent('dblclick', o)); } catch (e2) {}
                  return 'ok';
                }"""
            )
            if r == "ok":
                logger.info(
                    "Grafana screenshot: Dock menu fired via in-page JS (double: two click cycles + dblclick)"
                )
                return True
        except Exception as ex:
            logger.info("Grafana screenshot: Dock JS click failed: %s", ex)
        return False

    def _click_dock() -> bool:
        # 侧栏处于「锁定」时会出现 Unlock menu，先点一次解除再 Dock（顺序因 Grafana 版本而异）
        for unlock_sel in (
            'button[aria-label="Unlock menu"]',
            '[aria-label="Unlock menu"]',
        ):
            ul = page.locator(unlock_sel).first
            try:
                if ul.count() > 0 and ul.is_visible():
                    ul.scroll_into_view_if_needed(timeout=3000)
                    ul.click(timeout=3000, force=True)
                    page.wait_for_timeout(200)
                    logger.info("Grafana screenshot: clicked %r before Dock", unlock_sel)
            except Exception:
                pass

        selectors = (
            '[data-testid="data-testid navigation mega-menu"] #dock-menu-button',
            '[data-testid="data-testid navigation mega-menu"] button[aria-label="Dock menu"]',
            "#dock-menu-button",
            'button[id="dock-menu-button"]',
            'button[aria-label="Dock menu"]',
        )
        for sel in selectors:
            loc = page.locator(sel).first
            try:
                if loc.count() == 0:
                    continue
                loc.wait_for(state="attached", timeout=5000)
                try:
                    loc.wait_for(state="visible", timeout=2500)
                except Exception:
                    pass
                loc.scroll_into_view_if_needed(timeout=5000)
                page.wait_for_timeout(140)
                try:
                    loc.hover(timeout=2500)
                    page.wait_for_timeout(80)
                except Exception:
                    pass
                try:
                    try:
                        loc.click(timeout=6000, force=True, delay=50, click_count=2)
                    except TypeError:
                        loc.dblclick(timeout=6000, force=True, delay=50)
                except Exception as e1:
                    logger.info(
                        "Grafana screenshot: Dock Playwright double-click failed %r (%s); trying dispatch_event",
                        sel,
                        e1,
                    )
                    try:
                        loc.dispatch_event("click")
                        page.wait_for_timeout(90)
                        loc.dispatch_event("click")
                    except Exception as e2:
                        logger.info("Grafana screenshot: Dock dispatch_event failed: %s", e2)
                        continue
                logger.info("Grafana screenshot: double-clicked Dock menu via %r", sel)
                return True
            except Exception:
                continue
        try:
            alt = page.get_by_role("button", name=re.compile(r"^\s*Dock menu\s*$", re.I)).first
            if alt.count() > 0:
                alt.wait_for(state="attached", timeout=4000)
                alt.scroll_into_view_if_needed(timeout=5000)
                try:
                    try:
                        alt.click(timeout=6000, force=True, delay=50, click_count=2)
                    except TypeError:
                        alt.dblclick(timeout=6000, force=True, delay=50)
                except Exception:
                    alt.dispatch_event("click")
                    page.wait_for_timeout(90)
                    alt.dispatch_event("click")
                logger.info("Grafana screenshot: double-clicked Dock menu (role=name)")
                return True
        except Exception:
            pass
        if _click_dock_js():
            return True
        return False

    try:
        if _click_dock():
            page.wait_for_timeout(320)
            return
        for open_sel in (
            "#mega-menu-toggle",
            '[data-testid="mega-menu-toggle"]',
            "button[aria-label*='Open menu']",
        ):
            mt = page.locator(open_sel).first
            try:
                if mt.count() == 0:
                    continue
                mt.click(timeout=2500, force=True)
                page.wait_for_timeout(450)
            except Exception:
                continue
            if _click_dock():
                page.wait_for_timeout(320)
                return
        logger.warning(
            "Grafana screenshot: could not click Dock menu — left nav may stay open "
            "(selectors tried: mega-menu #dock-menu-button, #dock-menu-button, aria-label)"
        )
    except Exception as e:
        logger.info("Grafana screenshot: dock nav optional step failed: %s", e)

    page.wait_for_timeout(200)


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
        _grafana_wait_loading_like_gone(page, spin_cap)
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


def _grafana_refresh_toolbar_busy(page: Any) -> bool:
    """
    Detect top-right query state: while loading, Grafana often shows a visible "Cancel" button;
    when idle it returns to "Refresh".
    """
    try:
        return bool(
            page.evaluate(
                """() => {
                  const visible = (el) => {
                    if (!el) return false;
                    const r = el.getBoundingClientRect();
                    if (r.width < 2 || r.height < 2) return false;
                    const st = window.getComputedStyle(el);
                    if (!st || st.display === "none" || st.visibility === "hidden") return false;
                    return true;
                  };
                  let hasCancel = false;
                  let hasRefresh = false;
                  for (const b of Array.from(document.querySelectorAll('button'))) {
                    if (!visible(b)) continue;
                    const t = (b.textContent || '').replace(/\\s+/g, ' ').trim().toLowerCase();
                    if (!t) continue;
                    if (t.includes('cancel') || t.includes('取消')) hasCancel = true;
                    if (t.includes('refresh') || t.includes('刷新')) hasRefresh = true;
                  }
                  return hasCancel && !hasRefresh;
                }"""
            )
        )
    except Exception:
        return False


def _grafana_wait_loading_like_gone(page: Any, budget_ms: int) -> None:
    """Poll until loading-like elements stay at 0 for a few ticks (queries + canvas paint)."""
    if budget_ms <= 0:
        return
    deadline = time.monotonic() + budget_ms / 1000.0
    stable = 0
    last_c = -1
    last_busy: Optional[bool] = None
    while time.monotonic() < deadline:
        c = _grafana_loading_like_count(page)
        busy = _grafana_refresh_toolbar_busy(page)
        if c != last_c or busy != last_busy:
            logger.debug("Grafana screenshot: loading-like count=%s toolbar_busy=%s", c, busy)
            last_c = c
            last_busy = busy
        if c == 0 and not busy:
            stable += 1
            if stable >= 2:
                return
        else:
            stable = 0
        page.wait_for_timeout(160)
    c = _grafana_loading_like_count(page)
    busy = _grafana_refresh_toolbar_busy(page)
    if c > 0 or busy:
        logger.warning(
            "Grafana screenshot: after %sms still busy (loading_count≈%s toolbar_busy=%s) — capture may be partial",
            budget_ms,
            c,
            busy,
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


def _grafana_panel_ready_stats(page: Any) -> Tuple[int, int]:
    """
    Return (total_panels, ready_panels).
    A panel is "ready" when it shows chart canvas/uplot, or explicit "No data"/error text.
    """
    try:
        r = page.evaluate(
            """() => {
              const panels = Array.from(
                document.querySelectorAll(
                  'section[data-testid^="data-testid Panel header"], section[data-testid*="Panel header"]'
                )
              );
              let ready = 0;
              for (const p of panels) {
                const root = p.querySelector('[data-testid="data-testid panel content"]') || p;
                const hasChart = !!(
                  root.querySelector('[data-testid="uplot-main-div"]') ||
                  root.querySelector('canvas') ||
                  root.querySelector('[class*="timeseries"]')
                );
                const txt = (root.textContent || '').toLowerCase();
                const hasExplicitNoData = txt.includes('no data') || txt.includes('n/a') || txt.includes('error');
                if (hasChart || hasExplicitNoData) ready += 1;
              }
              return { total: panels.length, ready };
            }"""
        )
        if isinstance(r, dict):
            return int(r.get("total") or 0), int(r.get("ready") or 0)
    except Exception:
        pass
    return 0, 0


def _grafana_wait_panels_fully_loaded(page: Any, budget_ms: int) -> None:
    """
    Wait until most dashboard panels are ready before screenshot.
    Uses panel ready ratio + loading-like elements stable at 0.
    """
    b = max(1000, int(budget_ms))
    deadline = time.monotonic() + b / 1000.0
    stable = 0
    while time.monotonic() < deadline:
        total, ready = _grafana_panel_ready_stats(page)
        loading = _grafana_loading_like_count(page)
        need = 0
        if total > 0:
            need = max(int(math.ceil(total * GRAFANA_SCREENSHOT_PANEL_READY_RATIO)), int(GRAFANA_SCREENSHOT_PANEL_READY_MIN))
        ok_panels = total > 0 and ready >= min(total, need)
        if ok_panels and loading == 0:
            stable += 1
            if stable >= 2:
                logger.info(
                    "Grafana screenshot: panel readiness reached ready=%s/%s (need=%s), loading=%s",
                    ready,
                    total,
                    need,
                    loading,
                )
                return
        else:
            stable = 0
        page.wait_for_timeout(220)
    total, ready = _grafana_panel_ready_stats(page)
    logger.warning(
        "Grafana screenshot: panel readiness timeout after %sms (ready=%s/%s ratio_target=%.2f min=%s)",
        b,
        ready,
        total,
        GRAFANA_SCREENSHOT_PANEL_READY_RATIO,
        GRAFANA_SCREENSHOT_PANEL_READY_MIN,
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


def _grafana_build_screenshot_dashboard_url(
    start_unix: int,
    end_unix: int,
    *,
    relative_from: Optional[str] = None,
    relative_to: Optional[str] = None,
    timezone_param: Optional[str] = None,
) -> str:
    params: List[Tuple[str, str]] = [("orgId", "1")]
    rf_ov = (relative_from or "").strip()
    rt_ov = (relative_to or "").strip()
    force_relative = bool(rf_ov or rt_ov)
    if GRAFANA_SCREENSHOT_RELATIVE_RANGE or force_relative:
        rf = rf_ov or (GRAFANA_DASHBOARD_FROM or "now-15m").strip()
        rt = rt_ov or (GRAFANA_DASHBOARD_TO or "now").strip()
        params.extend([("from", rf), ("to", rt)])
    else:
        params.extend(
            [
                ("from", str(int(start_unix) * 1000)),
                ("to", str(int(end_unix) * 1000)),
            ]
        )
    tz = (timezone_param or "").strip()
    if not tz:
        tz = (GRAFANA_SCREENSHOT_TIMEZONE or "").strip()
    if tz.lower() in ("none", "-", "off", "0", "false", "no"):
        tz = ""
    if tz:
        params.append(("timezone", tz))
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


def _grafana_persistent_browser_enabled() -> bool:
    return _lark_env_truthy("GRAFANA_SCREENSHOT_ENABLE") and _lark_env_truthy("GRAFANA_PERSISTENT_BROWSER")


def _grafana_playwright_pre_screenshot_paint_flush(page: Any) -> None:
    """
    Headless Grafana 有时「面板 ready 统计够了」但 uPlot/canvas 尚未合成进位图；快门前强制置顶、
    等字体与双 rAF，减少全页截图只有侧栏/顶栏、主区纯底色的情况。
    """
    try:
        page.evaluate(
            """() => {
              window.scrollTo(0, 0);
              const s = document.scrollingElement || document.documentElement;
              if (s) s.scrollTop = 0;
            }"""
        )
    except Exception:
        pass
    try:
        page.evaluate(
            """async () => {
              try {
                if (document.fonts && document.fonts.ready) await document.fonts.ready;
              } catch (e) {}
            }"""
        )
    except Exception:
        pass
    extra = max(0, min(5000, _cfg_int("GRAFANA_SCREENSHOT_PRE_CAPTURE_MS", 800)))
    page.wait_for_timeout(extra)
    try:
        page.evaluate(
            "() => new Promise((resolve) => {"
            "  requestAnimationFrame(() => { requestAnimationFrame(() => resolve(undefined)); });"
            "})"
        )
    except Exception:
        pass
    if _lark_env_truthy("GRAFANA_SCREENSHOT_PRE_CAPTURE_RESCROLL"):
        try:
            _grafana_scroll_paint_lazy_panels(page)
            page.evaluate(
                "() => { window.scrollTo(0, 0); const s = document.scrollingElement || document.documentElement; if (s) s.scrollTop = 0; }"
            )
            page.wait_for_timeout(220)
        except Exception:
            pass


def _grafana_playwright_render_dashboard_and_png(page: Any, url: str, timeout_ms: int) -> bytes:
    """
    Navigate ``page`` to dashboard ``url`` and return a PNG after the same wait/stabilize path
    as ephemeral screenshots (shared with :class:`GrafanaPlaywrightKeeper`).
    Caller must have injected Grafana cookies (and optional boot-warm root ``/``) beforehand.
    """
    page.goto(url, wait_until="load", timeout=timeout_ms)
    page.wait_for_timeout(200)
    _grafana_playwright_dock_nav_only(page, timeout_ms)
    _grafana_click_dashboard_refresh(page, timeout_ms)
    # Refresh 有时会重新弹出 mega-menu；再收一次侧栏
    _grafana_playwright_dock_nav_only(page, timeout_ms)
    _grafana_expand_collapsed_dashboard_rows(page)
    _grafana_wait_dashboard_ready(page, timeout_ms)
    _grafana_wait_dashboard_body_populated(page, int(GRAFANA_SCREENSHOT_POPULATE_MAX_MS))
    _grafana_stabilize_dashboard_render(page, timeout_ms)
    _grafana_wait_panels_fully_loaded(page, int(GRAFANA_SCREENSHOT_PANEL_READY_MAX_MS))
    page.wait_for_timeout(180)

    if not _grafana_dashboard_has_visual_content(page):
        _grafana_expand_collapsed_dashboard_rows(page)
        _grafana_click_dashboard_refresh(
            page,
            timeout_ms,
            spinner_budget_ms=min(1400, int(GRAFANA_SCREENSHOT_POST_REFRESH_SPINNER_MS)),
        )
        _grafana_playwright_dock_nav_only(page, timeout_ms)
        _grafana_wait_dashboard_body_populated(
            page, min(3200, int(GRAFANA_SCREENSHOT_POPULATE_MAX_MS))
        )
        _grafana_scroll_paint_lazy_panels(page)
        _grafana_wait_panels_fully_loaded(page, min(8000, int(GRAFANA_SCREENSHOT_PANEL_READY_MAX_MS)))
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
            _grafana_playwright_dock_nav_only(page, timeout_ms)
            _grafana_expand_collapsed_dashboard_rows(page)
            _grafana_wait_dashboard_ready(page, min(20000, timeout_ms // 2))
            _grafana_wait_dashboard_body_populated(
                page, min(8000, int(GRAFANA_SCREENSHOT_POPULATE_MAX_MS))
            )
            _grafana_stabilize_dashboard_render(page, timeout_ms, rounds=1)
            _grafana_wait_panels_fully_loaded(page, min(9000, int(GRAFANA_SCREENSHOT_PANEL_READY_MAX_MS)))
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
    _grafana_playwright_dock_nav_only(page, timeout_ms)
    _grafana_playwright_pre_screenshot_paint_flush(page)
    full_page = _lark_env_truthy("GRAFANA_SCREENSHOT_FULL_PAGE")
    try:
        return page.screenshot(type="png", full_page=full_page, animations="disabled")
    except TypeError:
        return page.screenshot(type="png", full_page=full_page)


class GrafanaPlaywrightKeeper:
    """
    One daemon thread owns Playwright + Chromium + a single dashboard ``page``.
    Screenshots are serialized via a queue so Flask / watchdog threads never touch Playwright directly.
    """

    def __init__(self) -> None:
        self._q: queue.Queue = queue.Queue()
        self._thread: Optional[threading.Thread] = None
        self._ready = threading.Event()
        self._fatal: Optional[BaseException] = None

    def start(self) -> None:
        if self._thread is not None:
            return
        t = threading.Thread(target=self._run, daemon=True, name="grafana-playwright-keeper")
        self._thread = t
        t.start()

    def wait_ready(self, timeout: float) -> bool:
        return self._ready.wait(timeout=timeout)

    def request_png(self, url: str, timeout_ms: int) -> bytes:
        warm_wait = max(120.0, float(timeout_ms) / 1000.0 + 45.0)
        if not self._ready.wait(timeout=warm_wait):
            raise TimeoutError("GrafanaPlaywrightKeeper not ready (warm-up still running or failed)")
        if self._fatal is not None:
            raise RuntimeError("GrafanaPlaywrightKeeper failed during warm-up") from self._fatal
        job_timeout = max(30.0, _cfg_float("GRAFANA_PERSISTENT_BROWSER_JOB_TIMEOUT_SECONDS", 180.0))
        ev = threading.Event()
        box: Dict[str, Any] = {}
        self._q.put({"op": "png", "url": url, "timeout_ms": int(timeout_ms), "ev": ev, "box": box})
        if not ev.wait(timeout=job_timeout):
            raise TimeoutError("GrafanaPlaywrightKeeper screenshot job timed out")
        err = box.get("err")
        if err is not None:
            if isinstance(err, BaseException):
                raise err
            raise RuntimeError(str(err))
        png = box.get("png")
        if not isinstance(png, (bytes, bytearray)):
            raise RuntimeError("keeper returned no PNG bytes")
        return bytes(png)

    def _run(self) -> None:
        try:
            from playwright.sync_api import sync_playwright
        except ImportError as e:
            self._fatal = e
            self._ready.set()
            logger.exception("GrafanaPlaywrightKeeper: Playwright not installed")
            return

        p = None
        browser = None
        try:
            p = sync_playwright().start()
            browser = p.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                    "--disable-dev-shm-usage",
                ],
            )
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
            page = context.new_page()
            try:
                page.add_init_script(
                    "try{Object.defineProperty(navigator,'webdriver',{get:()=>undefined});}catch(e){}"
                )
            except Exception:
                pass

            timeout_ms = max(5000, int(GRAFANA_SCREENSHOT_TIMEOUT_MS))
            base = str(GRAFANA_BASE_URL).rstrip("/")
            sess0 = grafana_login_session()
            context.add_cookies(_playwright_cookie_list(sess0))
            if _lark_env_truthy("GRAFANA_SCREENSHOT_BOOT_WARM"):
                page.goto(f"{base}/", wait_until="domcontentloaded", timeout=min(20000, timeout_ms))
                page.wait_for_timeout(220)

            warm_url = _grafana_build_screenshot_dashboard_url(0, 0)
            logger.info("GrafanaPlaywrightKeeper: warm-up load url=%s…", warm_url[:220])
            _ = _grafana_playwright_render_dashboard_and_png(page, warm_url, timeout_ms)
            logger.info(
                "GrafanaPlaywrightKeeper: warm-up finished — persistent Chromium stays open; "
                "screenshot jobs reuse this browser (see log line 'using persistent Playwright keeper')."
            )
            self._ready.set()

            idle_sec = max(30.0, _cfg_float("GRAFANA_PERSISTENT_BROWSER_IDLE_REFRESH_SECONDS", 120.0))
            while True:
                try:
                    job = self._q.get(timeout=idle_sec)
                except queue.Empty:
                    try:
                        _grafana_click_dashboard_refresh(page, timeout_ms)
                    except Exception as ex:
                        logger.debug("GrafanaPlaywrightKeeper idle refresh: %s", ex)
                    continue
                if job.get("op") == "stop":
                    break
                if job.get("op") != "png":
                    continue
                ev: threading.Event = job["ev"]
                box: Dict[str, Any] = job["box"]
                jurl = str(job.get("url") or "")
                jto = int(job.get("timeout_ms") or timeout_ms)
                try:
                    sess = grafana_login_session()
                    ck = _playwright_cookie_list(sess)
                    if _lark_env_truthy("GRAFANA_PERSISTENT_BROWSER_SOFT_COOKIE"):
                        try:
                            context.add_cookies(ck)
                        except Exception:
                            context.clear_cookies()
                            context.add_cookies(ck)
                    else:
                        context.clear_cookies()
                        context.add_cookies(ck)
                    box["png"] = _grafana_playwright_render_dashboard_and_png(page, jurl, max(5000, jto))
                except Exception as ex:
                    box["err"] = ex
                finally:
                    ev.set()
        except Exception as ex:
            self._fatal = ex
            logger.exception("GrafanaPlaywrightKeeper: crashed")
            self._ready.set()
        finally:
            try:
                if browser is not None:
                    browser.close()
            except Exception:
                pass
            try:
                if p is not None:
                    p.stop()
            except Exception:
                pass


def _start_grafana_playwright_keeper_if_enabled() -> None:
    """Start background Chromium once (non-blocking); screenshots wait on warm-up inside ``request_png``."""
    global _grafana_pw_keeper, _grafana_pw_keeper_start_attempted
    if not _grafana_persistent_browser_enabled():
        logger.info("Grafana persistent browser: off (GRAFANA_SCREENSHOT_ENABLE or GRAFANA_PERSISTENT_BROWSER=0)")
        return
    with _grafana_pw_keeper_lock:
        if _grafana_pw_keeper_start_attempted:
            return
        _grafana_pw_keeper_start_attempted = True
        try:
            k = GrafanaPlaywrightKeeper()
            k.start()
            _grafana_pw_keeper = k
            logger.info("GrafanaPlaywrightKeeper thread started (warm-up runs in background)")
        except Exception:
            logger.exception("GrafanaPlaywrightKeeper failed to start — ephemeral screenshots only")
            _grafana_pw_keeper = None


def _grafana_headless_screenshot_png(
    session: requests.Session,
    start_unix: int,
    end_unix: int,
    *,
    relative_from: Optional[str] = None,
    relative_to: Optional[str] = None,
    timezone_param: Optional[str] = None,
) -> bytes:
    """
    Headless Chromium (Playwright) opens the same dashboard URL as the UI, with session cookies.
    Requires: ``pip install playwright`` and ``playwright install chromium`` on the server.

    ``GRAFANA_SCREENSHOT_FULL_PAGE=1`` (default): ``page.screenshot(full_page=True)`` — full scroll height
    so KPI rows below the fold are included. ``0`` captures only the viewport (``WIDTH``×``HEIGHT``).

    Defaults favor **low latency** (short sleeps, tight spinner/populate caps). If captures go blank,
    raise ``GRAFANA_SCREENSHOT_POPULATE_MAX_MS`` and ``GRAFANA_SCREENSHOT_POST_REFRESH_SPINNER_MS`` first.

    Optional ``relative_from`` / ``relative_to`` / ``timezone_param`` override the URL query for this
    capture only (watchdog uses ``now-15m`` … ``now`` + ``timezone=browser`` while Prometheus uses a shorter eval window).

    When ``GRAFANA_PERSISTENT_BROWSER=1`` and the background :class:`GrafanaPlaywrightKeeper` is running,
    screenshots reuse one Chromium tab (warm at process start) instead of launching a new browser each time.
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as e:
        raise ImportError(
            "Playwright not installed — pip install playwright && playwright install chromium"
        ) from e

    url = _grafana_build_screenshot_dashboard_url(
        start_unix,
        end_unix,
        relative_from=relative_from,
        relative_to=relative_to,
        timezone_param=timezone_param,
    )
    cookies = _playwright_cookie_list(session)
    timeout_ms = max(5000, int(GRAFANA_SCREENSHOT_TIMEOUT_MS))
    rel_eff = GRAFANA_SCREENSHOT_RELATIVE_RANGE or bool(
        (relative_from or "").strip() or (relative_to or "").strip()
    )
    logger.info(
        "Grafana screenshot: relative_range=%s url=%s",
        rel_eff,
        url[:300] + ("…" if len(url) > 300 else ""),
    )

    k = _grafana_pw_keeper
    if k is not None and _grafana_persistent_browser_enabled():
        try:
            logger.info("Grafana screenshot: using persistent Playwright keeper")
            return k.request_png(url, timeout_ms)
        except Exception as e:
            logger.warning(
                "Grafana persistent keeper screenshot failed (%s); falling back to ephemeral browser",
                e,
            )

    with sync_playwright() as p:
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

            return _grafana_playwright_render_dashboard_and_png(page, url, timeout_ms)
        finally:
            browser.close()


def _grafana_watchdog_alert_screenshot_png(session: requests.Session) -> bytes:
    """
    Watchdog alert image: Grafana **browser** range ``now-15m`` … ``now`` (plus optional ``timezone``),
    independent of the shorter Prometheus eval window on the payload.
    """
    su, eu = _monitoring_watch_eval_window_unix()
    rf = _cfg_str("MONITORING_WATCH_SCREENSHOT_FROM", "now-15m").strip() or "now-15m"
    rt = _cfg_str("MONITORING_WATCH_SCREENSHOT_TO", "now").strip() or "now"
    tz = _cfg_str("MONITORING_WATCH_SCREENSHOT_TIMEZONE", "browser").strip()
    return _grafana_headless_screenshot_png(
        session,
        su,
        eu,
        relative_from=rf,
        relative_to=rt,
        timezone_param=tz or None,
    )


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


def _keyword_matches_series_labels(keyword: str, legend_format: str, metric: Dict[str, Any]) -> bool:
    """
    Match a panel ``keyword`` to a Prometheus-like series row.

    Purely **numeric** keywords (e.g. ``3201``, ``1492288``) use **token-boundary** matching so
    ``3201`` does **not** match ``13201`` / ``32012`` when those appear in legend or label text —
    substring-only matching caused bogus baselines (e.g. ~6096) vs Grafana's selected series (~21k).
    Non-numeric keywords keep substring behavior (e.g. ``9280 + Push``).
    """
    kw_raw = (keyword or "").strip()
    if not kw_raw:
        return True
    lg = str(legend_format or "").strip()
    metric_blob = " ".join(str(v) for v in metric.values() if v is not None)
    kw_cf = kw_raw.casefold()
    lg_cf = lg.casefold()
    mb_cf = metric_blob.casefold()
    if lg_cf == kw_cf or mb_cf.strip() == kw_cf:
        return True
    if kw_raw.isdigit():
        boundary = re.compile(r"(^|[^0-9])" + re.escape(kw_raw) + r"([^0-9]|$)")
        return bool(boundary.search(lg_cf) or boundary.search(mb_cf))
    return kw_cf in lg_cf or kw_cf in mb_cf


def _series_row_exact_keyword_id(keyword: str, legend_format: str, metric: Dict[str, Any]) -> bool:
    """
    True when this row is unambiguously the single-series id (e.g. Grafana legend ``3201``),
    not merely ``keyword`` appearing somewhere in a long label blob (which can still wrong-merge).

    If any exact-id rows exist, :func:`_merge_series_points_by_keyword` uses **only** those rows so
    alert numbers match the highlighted series (~21k) instead of accidental mixes (~14k).
    """
    kw_raw = (keyword or "").strip()
    if not kw_raw:
        return False
    kw_cf = kw_raw.casefold()
    lg = str(legend_format or "").strip().casefold()
    if lg == kw_cf:
        return True
    if not isinstance(metric, dict):
        return False
    for mk in (
        "series",
        "name",
        "provider",
        "providerid",
        "provider_id",
        "game",
        "gameid",
        "game_id",
        "label",
        "__series_id__",
    ):
        v = metric.get(mk)
        if v is None:
            continue
        if str(v).strip().casefold() == kw_cf:
            return True
    return False


def _prometheus_result_value_pairs(result: Dict[str, Any]) -> List[Tuple[float, float]]:
    """``values`` pairs from one Prometheus ``result`` row as ``(timestamp_unix, value)``."""
    out: List[Tuple[float, float]] = []
    for pair in result.get("values") or []:
        if len(pair) < 2:
            continue
        try:
            ts = float(pair[0])
            val = float(pair[1])
        except (TypeError, ValueError):
            continue
        out.append((ts, val))
    return out


def _median_positive_abs(values: List[float]) -> float:
    """Median of ``abs(v)`` for finite ``v > 0``; ``0.0`` if none."""
    xs = sorted(abs(float(v)) for v in values if math.isfinite(float(v)) and float(v) > 0.0)
    if not xs:
        return 0.0
    mid = len(xs) // 2
    if len(xs) % 2 == 1:
        return float(xs[mid])
    return float(xs[mid - 1] + xs[mid]) / 2.0


def _pick_best_exact_keyword_series(candidates: List[List[Tuple[float, float]]]) -> List[Tuple[float, float]]:
    """
    When Grafana shows one ``3201`` line but the snapshot carries several duplicate ``result`` rows,
    picking **longest** series can select a stale/partial target (longer scrape history at wrong levels),
    producing bogus fast drops (e.g. ~18k -> ~8k) and fake spikes from ~11k baselines.
    Prefer the row whose magnitudes match the **main** curve: highest median ``|v|``, then length, then mass.
    """
    best_pl: Optional[List[Tuple[float, float]]] = None
    best_key: Optional[Tuple[float, int, float]] = None
    for pl in candidates:
        vs = [v for _, v in pl]
        med = _median_positive_abs(vs)
        mass = sum(abs(float(v)) for v in vs if math.isfinite(float(v)))
        key = (med, len(pl), mass)
        if best_key is None or key > best_key:
            best_key = key
            best_pl = pl
    return best_pl or []


def _merge_digit_keyword_rows_max_bucketed(
    rows: List[List[Tuple[float, float]]],
    *,
    tol_sec: float = 0.5,
) -> List[Tuple[float, float]]:
    """
    Duplicate numeric-id rows: for each minute, prefer samples near ``…:00`` (``max`` across those);
    if none, use the value at the timestamp **closest** to minute start (still one row per minute).
    """
    by_b: Dict[float, List[Tuple[float, float]]] = {}
    tol = float(tol_sec)
    for row in rows:
        for ts, val in row:
            try:
                v = float(val)
            except (TypeError, ValueError):
                continue
            if not math.isfinite(v):
                continue
            tsf = float(ts)
            b = _bucket_ts_monitoring_minute(tsf)
            by_b.setdefault(b, []).append((tsf, v))
    out: List[Tuple[float, float]] = []
    for b in sorted(by_b.keys()):
        cand = by_b[b]
        near = [v for t, v in cand if abs(t - b) <= tol]
        if near:
            out.append((b, max(near)))
        else:
            _, v_pick = min(cand, key=lambda x: abs(x[0] - b))
            out.append((b, v_pick))
    return out


def _merge_result_rows_max_per_ts(rows: List[List[Tuple[float, float]]]) -> List[Tuple[float, float]]:
    """Combine several Prometheus ``result`` rows that describe the same legend; ``max`` per timestamp."""
    by_ts: Dict[float, float] = {}
    for row in rows:
        row_ts: Dict[float, float] = {}
        for ts, val in row:
            try:
                v = float(val)
            except (TypeError, ValueError):
                continue
            if not math.isfinite(v):
                continue
            row_ts[float(ts)] = v
        for ts, v in row_ts.items():
            prev = by_ts.get(ts)
            if prev is None or v > prev:
                by_ts[ts] = v
    return sorted(by_ts.items(), key=lambda x: x[0])


def _merge_9280_push_points(payload: Dict[str, Any]) -> List[Tuple[float, float]]:
    """
    Pick points for ``9280 + Push``.

    Substring keyword matching also hits ``9280 + Push - 7Days`` / ``9280 + Push...``; summing those
    with the highlighted series (~81k) yielded bogus totals (~238k). Prefer **exact** legend equality
    first; fall back to fuzzy match only if nothing matches exactly.
    """
    kw = (MONITORING_9280_SERIES_KEYWORD or "9280 + Push").strip()
    kw_cf = kw.casefold()

    exact_rows: List[List[Tuple[float, float]]] = []
    for s in payload.get("series") or []:
        lg = str(s.get("legendFormat") or "").strip()
        if lg.casefold() != kw_cf:
            continue
        prom = s.get("prometheus") or {}
        pdata = prom.get("data") or {}
        for r in pdata.get("result") or []:
            pts = _prometheus_result_value_pairs(r if isinstance(r, dict) else {})
            if pts:
                exact_rows.append(pts)

    if exact_rows:
        if len(exact_rows) == 1:
            by_one: Dict[float, float] = {}
            for ts, val in exact_rows[0]:
                try:
                    v = float(val)
                except (TypeError, ValueError):
                    continue
                if not math.isfinite(v):
                    continue
                by_one[float(ts)] = v
            return sorted(by_one.items(), key=lambda x: x[0])
        return _merge_result_rows_max_per_ts(exact_rows)

    by_ts: Dict[float, float] = {}
    for s in payload.get("series") or []:
        lg = str(s.get("legendFormat") or "")
        prom = s.get("prometheus") or {}
        pdata = prom.get("data") or {}
        for r in pdata.get("result") or []:
            metric = r.get("metric") or {}
            if kw and not _keyword_matches_series_labels(kw, lg, metric if isinstance(metric, dict) else {}):
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


def _merge_series_points_by_keyword(payload: Dict[str, Any], keyword: str) -> List[Tuple[float, float]]:
    kw = (keyword or "").strip()

    # Exact-id rows: duplicate Prometheus ``result`` rows (same legend ``3201``) must NOT be blindly
    # summed — sums like 5294+9483=14777 vs Grafana ~20k. Numeric ids: ``max`` per **minute** via
    # :func:`_bucket_ts_monitoring_minute` (``MONITORING_TIME_BUCKET_TZ``); even for a single
    # ``result`` row so sub-minute scrapes cannot leave a ghost low in the same minute as Grafana's
    # tooltip; non-numeric multi-row: pick one series by median magnitude.
    exact_candidates: List[List[Tuple[float, float]]] = []
    for s in payload.get("series") or []:
        lg = str(s.get("legendFormat") or "")
        prom = s.get("prometheus") or {}
        pdata = prom.get("data") or {}
        for r in pdata.get("result") or []:
            metric = r.get("metric") or {}
            md = metric if isinstance(metric, dict) else {}
            if kw and not _series_row_exact_keyword_id(kw, lg, md):
                continue
            pts = _prometheus_result_value_pairs(r if isinstance(r, dict) else {})
            if pts:
                exact_candidates.append(pts)
    if exact_candidates:
        if kw.isdigit():
            merged_pts = _merge_digit_keyword_rows_max_bucketed(exact_candidates)
        elif len(exact_candidates) == 1:
            merged_pts = exact_candidates[0]
        else:
            merged_pts = _pick_best_exact_keyword_series(exact_candidates)
        by_one: Dict[float, float] = {}
        for ts, val in merged_pts:
            by_one[ts] = val
        return sorted(by_one.items(), key=lambda x: x[0])

    def _accumulate_fuzzy() -> Dict[float, float]:
        digit_kw = bool(kw.isdigit())
        if digit_kw:
            digit_rows: List[List[Tuple[float, float]]] = []
            for s in payload.get("series") or []:
                lg = str(s.get("legendFormat") or "")
                prom = s.get("prometheus") or {}
                pdata = prom.get("data") or {}
                for r in pdata.get("result") or []:
                    metric = r.get("metric") or {}
                    md = metric if isinstance(metric, dict) else {}
                    if kw and not _keyword_matches_series_labels(kw, lg, md):
                        continue
                    pts = _prometheus_result_value_pairs(r if isinstance(r, dict) else {})
                    if pts:
                        digit_rows.append(pts)
            if digit_rows:
                return dict(_merge_digit_keyword_rows_max_bucketed(digit_rows))
            return {}

        by_ts_sum: Dict[float, float] = {}
        for s in payload.get("series") or []:
            lg = str(s.get("legendFormat") or "")
            prom = s.get("prometheus") or {}
            pdata = prom.get("data") or {}
            for r in pdata.get("result") or []:
                metric = r.get("metric") or {}
                md = metric if isinstance(metric, dict) else {}
                if kw and not _keyword_matches_series_labels(kw, lg, md):
                    continue
                row_ts: Dict[float, float] = {}
                for ts, val in _prometheus_result_value_pairs(r if isinstance(r, dict) else {}):
                    try:
                        v = float(val)
                    except (TypeError, ValueError):
                        continue
                    if not math.isfinite(v):
                        continue
                    row_ts[float(ts)] = v
                for ts, v in row_ts.items():
                    by_ts_sum[ts] = by_ts_sum.get(ts, 0.0) + v
        return by_ts_sum

    by_fuzzy = _accumulate_fuzzy()
    if not by_fuzzy:
        return []
    return sorted(by_fuzzy.items(), key=lambda x: x[0])


def _merge_deposit_points(payload: Dict[str, Any]) -> List[Tuple[float, float]]:
    return _merge_series_points_by_keyword(payload, MONITORING_DEPOSIT_SERIES_KEYWORD)


def _merge_withdraw_points(payload: Dict[str, Any]) -> List[Tuple[float, float]]:
    return _merge_series_points_by_keyword(payload, MONITORING_WITHDRAW_SERIES_KEYWORD)


def _filter_low_outlier_points(
    points: List[Tuple[float, float]],
    ratio_to_median: float = 0.10,
    min_abs_floor: float = 5.0,
) -> List[Tuple[float, float]]:
    """
    Remove tiny baseline outliers (e.g. 1/2/3/5) that can explode % change for
    high-volume series (30k~50k). This is used for Provider/Games keyword panels.
    """
    if len(points) < 4:
        return points
    vals = sorted(float(v) for _, v in points if float(v) > 0.0)
    if len(vals) < 3:
        return points
    mid = len(vals) // 2
    if len(vals) % 2 == 1:
        median_v = vals[mid]
    else:
        median_v = (vals[mid - 1] + vals[mid]) / 2.0
    floor = max(float(min_abs_floor), float(median_v) * float(ratio_to_median))
    filtered = [(t, v) for (t, v) in points if float(v) >= floor]
    # Keep original if filtering is too aggressive.
    if len(filtered) >= max(3, int(math.ceil(len(points) * 0.6))):
        return filtered
    return points


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
            cand = {
                "pct": round(pct, 2),
                "from_ts": ts[i],
                "to_ts": ts[j],
                "from_val": vals[i],
                "to_val": vals[j],
                "buckets": buckets,
            }
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
            cand = {
                "pct": round(pct, 2),
                "from_ts": ts[i],
                "to_ts": ts[j],
                "from_val": vals[i],
                "to_val": vals[j],
                "buckets": buckets,
            }
            if best is None or pct > float(best["pct"]) or (
                pct == float(best["pct"]) and buckets > int(best["buckets"])
            ):
                best = cand
        i = j + 1
    return best


def _http_drop_spike_analysis(
    points: List[Tuple[float, float]],
    fast_threshold_pct: float,
    continuous_threshold_pct: float,
    window_seconds: int = 120,
) -> Dict[str, Any]:
    """
    Alert rules:
    1) within ``window_seconds`` (default 2 minutes), drop/spike >= ``fast_threshold_pct``
    2) continuous monotonic run drop/spike >= ``continuous_threshold_pct``
    """
    out: Dict[str, Any] = {
        "pointCount": len(points),
        "hit_alert": False,
        "fast_threshold_pct": fast_threshold_pct,
        "continuous_threshold_pct": continuous_threshold_pct,
        "window_seconds": int(window_seconds),
        "consecutive_max_drop": None,
        "consecutive_max_spike": None,
        "window_max_drop": None,
        "window_max_spike": None,
    }
    if len(points) < 2:
        return out

    vals = [p[1] for p in points]
    ts = [p[0] for p in points]
    L = len(points)

    # Convert "within 2 minutes" to bucket span using median step.
    if L >= 2:
        diffs = [max(1.0, ts[i + 1] - ts[i]) for i in range(L - 1)]
        diffs.sort()
        step_sec = diffs[len(diffs) // 2]
    else:
        step_sec = 60.0
    span = max(1, int(round(float(window_seconds) / float(step_sec))))
    out["window_span_buckets"] = span + 1

    drop_run = _best_consecutive_drop_run(vals, ts)
    if drop_run is not None:
        out["consecutive_max_drop"] = {
            "pct": drop_run["pct"],
            "from_ts": drop_run["from_ts"],
            "to_ts": drop_run["to_ts"],
            "from_val": drop_run.get("from_val"),
            "to_val": drop_run.get("to_val"),
            "buckets": drop_run["buckets"],
        }
        if float(drop_run.get("pct") or 0.0) >= float(continuous_threshold_pct):
            out["hit_alert"] = True
    spike_run = _best_consecutive_spike_run(vals, ts)
    if spike_run is not None:
        out["consecutive_max_spike"] = {
            "pct": spike_run["pct"],
            "from_ts": spike_run["from_ts"],
            "to_ts": spike_run["to_ts"],
            "from_val": spike_run.get("from_val"),
            "to_val": spike_run.get("to_val"),
            "buckets": spike_run["buckets"],
        }
        if float(spike_run.get("pct") or 0.0) >= float(continuous_threshold_pct):
            out["hit_alert"] = True

    best_w_drop: Optional[Dict[str, Any]] = None
    best_w_spike: Optional[Dict[str, Any]] = None
    for i in range(0, L - span):
        j = i + span
        if vals[i] <= 0:
            continue
        pct = (vals[j] - vals[i]) / vals[i] * 100.0
        if pct < 0:
            cand_d = {
                "pct": round(abs(pct), 2),
                "from_ts": ts[i],
                "to_ts": ts[j],
                "from_val": vals[i],
                "to_val": vals[j],
                "window_seconds": int(round(ts[j] - ts[i])),
            }
            if best_w_drop is None or float(cand_d["pct"]) > float(best_w_drop["pct"]):
                best_w_drop = cand_d
        elif pct > 0:
            cand_s = {
                "pct": round(abs(pct), 2),
                "from_ts": ts[i],
                "to_ts": ts[j],
                "from_val": vals[i],
                "to_val": vals[j],
                "window_seconds": int(round(ts[j] - ts[i])),
            }
            if best_w_spike is None or float(cand_s["pct"]) > float(best_w_spike["pct"]):
                best_w_spike = cand_s
    out["window_max_drop"] = best_w_drop
    out["window_max_spike"] = best_w_spike
    if best_w_drop is not None and float(best_w_drop.get("pct") or 0.0) >= float(fast_threshold_pct):
        out["hit_alert"] = True
    if best_w_spike is not None and float(best_w_spike.get("pct") or 0.0) >= float(fast_threshold_pct):
        out["hit_alert"] = True
    return out


def _http_analysis_for_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    pts = _merge_http_timeseries_points(payload)
    pts = _snap_series_to_monitoring_minutes(pts, how="sum")
    pts = _trim_trailing_minute_buckets(pts, MONITORING_DROP_LAST_MERGED_MINUTES)
    a = _http_drop_spike_analysis(
        pts,
        MONITORING_HTTP_DROP_ALERT_PCT,
        MONITORING_HTTP_CONTINUOUS_ALERT_PCT,
        MONITORING_ALERT_WINDOW_SECONDS,
    )
    a["point_count"] = len(pts)
    a["merged_points"] = [[t, v] for t, v in pts]
    return a


def _analysis_for_9280_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    pts = _merge_9280_push_points(payload)
    pts = _snap_series_to_monitoring_minutes(pts, how="max")
    pts = _trim_trailing_minute_buckets(pts, MONITORING_DROP_LAST_MERGED_MINUTES)
    a = _http_drop_spike_analysis(
        pts,
        MONITORING_9280_ALERT_PCT,
        MONITORING_9280_CONTINUOUS_ALERT_PCT,
        MONITORING_ALERT_WINDOW_SECONDS,
    )
    a["point_count"] = len(pts)
    a["merged_points"] = [[t, v] for t, v in pts]
    return a


def _analysis_for_deposit_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    pts = _merge_deposit_points(payload)
    pts = _snap_series_to_monitoring_minutes(pts, how="sum")
    pts = _trim_trailing_minute_buckets(pts, MONITORING_DROP_LAST_MERGED_MINUTES)
    a = _http_drop_spike_analysis(
        pts,
        MONITORING_DEPOSIT_ALERT_PCT,
        MONITORING_DEPOSIT_CONTINUOUS_ALERT_PCT,
        MONITORING_ALERT_WINDOW_SECONDS,
    )
    a["point_count"] = len(pts)
    a["merged_points"] = [[t, v] for t, v in pts]
    return a


def _analysis_for_withdraw_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    pts = _merge_withdraw_points(payload)
    pts = _snap_series_to_monitoring_minutes(pts, how="sum")
    pts = _trim_trailing_minute_buckets(pts, MONITORING_DROP_LAST_MERGED_MINUTES)
    a = _http_drop_spike_analysis(
        pts,
        MONITORING_WITHDRAW_ALERT_PCT,
        MONITORING_WITHDRAW_CONTINUOUS_ALERT_PCT,
        MONITORING_ALERT_WINDOW_SECONDS,
    )
    a["point_count"] = len(pts)
    a["merged_points"] = [[t, v] for t, v in pts]
    return a


def _analysis_for_keyword_payload(
    payload: Dict[str, Any], keyword: str, fast_threshold_pct: float, continuous_threshold_pct: float
) -> Dict[str, Any]:
    pts = _merge_series_points_by_keyword(payload, keyword)
    # Drop ghost lows far below the typical level (e.g. ~4.5k vs median ~17k) before % rules.
    pts_filtered = _filter_low_outlier_points(pts, ratio_to_median=0.28)
    if len(pts_filtered) != len(pts):
        logger.info(
            "keyword baseline filter applied keyword=%r points=%s->%s",
            keyword,
            len(pts),
            len(pts_filtered),
        )
    pts_filtered = _snap_series_to_monitoring_minutes(pts_filtered, how="max")
    pts_filtered = _trim_trailing_minute_buckets(pts_filtered, MONITORING_DROP_LAST_MERGED_MINUTES)
    a = _http_drop_spike_analysis(
        pts_filtered,
        fast_threshold_pct,
        continuous_threshold_pct,
        MONITORING_ALERT_WINDOW_SECONDS,
    )
    a["point_count"] = len(pts_filtered)
    a["merged_points"] = [[t, v] for t, v in pts_filtered]
    if not pts:
        sample_labels: List[str] = []
        for s in payload.get("series") or []:
            prom = s.get("prometheus") if isinstance(s.get("prometheus"), dict) else {}
            pdata = prom.get("data") if isinstance(prom.get("data"), dict) else {}
            for r in pdata.get("result") or []:
                metric = r.get("metric") if isinstance(r.get("metric"), dict) else {}
                lbl = str(metric.get("series") or metric.get("name") or "").strip()
                if not lbl:
                    lbl = " ".join(str(v) for v in metric.values() if str(v).strip()).strip()
                if lbl and lbl not in sample_labels:
                    sample_labels.append(lbl)
                if len(sample_labels) >= 20:
                    break
            if len(sample_labels) >= 20:
                break
        logger.info(
            "keyword no match keyword=%r sample_series=%s",
            keyword,
            sample_labels[:20],
        )
    return a


def _analysis_for_provider_jili_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _analysis_for_keyword_payload(
        payload,
        MONITORING_PROVIDER_JILI_SERIES_KEYWORD,
        MONITORING_PROVIDER_JILI_ALERT_PCT,
        MONITORING_PROVIDER_JILI_CONTINUOUS_ALERT_PCT,
    )


def _analysis_for_provider_general_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _analysis_for_keyword_payload(
        payload,
        MONITORING_PROVIDER_GENERAL_SERIES_KEYWORD,
        MONITORING_PROVIDER_GENERAL_ALERT_PCT,
        MONITORING_PROVIDER_GENERAL_CONTINUOUS_ALERT_PCT,
    )


def _analysis_for_provider_inhouse_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _analysis_for_keyword_payload(
        payload,
        MONITORING_PROVIDER_INHOUSE_SERIES_KEYWORD,
        MONITORING_PROVIDER_INHOUSE_ALERT_PCT,
        MONITORING_PROVIDER_INHOUSE_CONTINUOUS_ALERT_PCT,
    )


def _analysis_for_games_jili_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _analysis_for_keyword_payload(
        payload,
        MONITORING_GAMES_JILI_SERIES_KEYWORD,
        MONITORING_GAMES_JILI_ALERT_PCT,
        MONITORING_GAMES_JILI_CONTINUOUS_ALERT_PCT,
    )


def _analysis_for_games_general_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _analysis_for_keyword_payload(
        payload,
        MONITORING_GAMES_GENERAL_SERIES_KEYWORD,
        MONITORING_GAMES_GENERAL_ALERT_PCT,
        MONITORING_GAMES_GENERAL_CONTINUOUS_ALERT_PCT,
    )


def _analysis_for_games_inhouse_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _analysis_for_keyword_payload(
        payload,
        MONITORING_GAMES_INHOUSE_SERIES_KEYWORD,
        MONITORING_GAMES_INHOUSE_ALERT_PCT,
        MONITORING_GAMES_INHOUSE_CONTINUOUS_ALERT_PCT,
    )


def _format_extra_analysis_lines(section_label: str, analysis: Dict[str, Any]) -> List[str]:
    if MONITORING_MO_HIDE_EXTRA_DROP_SPIKE_STATS:
        return []
    fast_thr = float(analysis.get("fast_threshold_pct") or MONITORING_9280_ALERT_PCT)
    cont_thr = float(analysis.get("continuous_threshold_pct") or MONITORING_9280_CONTINUOUS_ALERT_PCT)
    win_sec = int(analysis.get("window_seconds") or MONITORING_ALERT_WINDOW_SECONDS)
    lines: List[str] = [
        "",
        f"[{section_label}] alert when drop/spike > {fast_thr:g}% within {win_sec//60} minutes "
        f"or continuous drop/spike > {cont_thr:g}%",
    ]
    wd = analysis.get("window_max_drop")
    ws = analysis.get("window_max_spike")
    cd = analysis.get("consecutive_max_drop")
    cs = analysis.get("consecutive_max_spike")
    lines.append(
        f"within {win_sec//60}m drop/spike: -{(wd or {}).get('pct', 'n/a')}% / +{(ws or {}).get('pct', 'n/a')}%"
    )
    lines.append(
        f"continuous drop/spike : -{(cd or {}).get('pct', 'n/a')}% / +{(cs or {}).get('pct', 'n/a')}%"
    )
    return lines


def _format_trigger_lines(
    graph_label: str,
    series_label: str,
    analysis: Dict[str, Any],
    fast_threshold_pct: float,
    continuous_threshold_pct: float,
    window_seconds: int,
) -> List[str]:
    out: List[str] = []
    wd = analysis.get("window_max_drop")
    ws = analysis.get("window_max_spike")
    cd = analysis.get("consecutive_max_drop")
    cs = analysis.get("consecutive_max_spike")
    win_m = max(1, int(round(float(window_seconds) / 60.0)))

    def _pct_text(v: Any) -> str:
        try:
            f = float(v)
            if abs(f - round(f)) < 1e-6:
                return f"{int(round(f)):,}"
            return f"{f:,.2f}"
        except (TypeError, ValueError):
            return str(v)

    def _event_text(ev: Dict[str, Any], direction: str, threshold_pct: float) -> str:
        sign = "+" if direction == "SPIKE" else "-"
        pct = _pct_text(ev.get("pct"))
        return (
            f"{direction} {sign}{pct}% (>{threshold_pct:g}%) "
            f"{_fmt_num(ev.get('from_val'))} ({_fmt_ts_short(ev.get('from_ts'))}) -> "
            f"{_fmt_num(ev.get('to_val'))} ({_fmt_ts_short(ev.get('to_ts'))})"
        )

    fast_hits: List[str] = []
    if isinstance(wd, dict) and float(wd.get("pct") or 0.0) >= float(fast_threshold_pct):
        fast_hits.append(_event_text(wd, "DROP", fast_threshold_pct))
    if isinstance(ws, dict) and float(ws.get("pct") or 0.0) >= float(fast_threshold_pct):
        fast_hits.append(_event_text(ws, "SPIKE", fast_threshold_pct))

    cont_hits: List[str] = []
    if isinstance(cd, dict) and float(cd.get("pct") or 0.0) >= float(continuous_threshold_pct):
        cont_hits.append(_event_text(cd, "DROP", continuous_threshold_pct))
    if isinstance(cs, dict) and float(cs.get("pct") or 0.0) >= float(continuous_threshold_pct):
        cont_hits.append(_event_text(cs, "SPIKE", continuous_threshold_pct))

    if fast_hits or cont_hits:
        block: List[str] = [f"[{graph_label}] {series_label}"]
        if fast_hits:
            block.append(f"Fast ({win_m}m): {' | '.join(fast_hits)}")
        if cont_hits:
            block.append(f"Continuous: {' | '.join(cont_hits)}")
        out.append("\n".join(block))
    return out


def _format_alert_series_table_footer(
    graph_label: str,
    series_label: str,
    analysis: Dict[str, Any],
    *,
    max_rows: Optional[int] = None,
) -> str:
    """English-only: compact table tail so alert lines can be checked against the same merged series."""
    pts = analysis.get("merged_points") or []
    title = f"Recent points — [{graph_label}] {series_label}:"
    if not pts:
        return f"{title}\n(no points)"
    cap = MONITORING_TABLE_TAIL_ROWS if max_rows is None else max(1, min(99, int(max_rows)))
    tail = pts[-cap:]
    rows = ["```text", "time           value", "-------------  ------------"]
    for pair in tail:
        rows.append(f"{_fmt_ts_short(pair[0]):<13}  {_fmt_num(pair[1]):>12}")
    rows.append("```")
    return "\n".join([title, "\n".join(rows)])


def _format_simple_series_alert_block(
    graph_label: str,
    series_label: str,
    analysis: Dict[str, Any],
    *,
    max_rows: Optional[int] = None,
) -> str:
    """
    Grafana-style snippet: latest timestamps and values from the same merged series the bot analyzed.
    English-only user-visible lines (product convention).
    """
    pts = analysis.get("merged_points") or []
    head = [
        f"[{graph_label}] {series_label}",
        "Threshold exceeded. Recent points (bot merged series, oldest → newest):",
    ]
    if not pts:
        return "\n".join(head + ["(no points in window)"])
    cap = MONITORING_TABLE_TAIL_ROWS if max_rows is None else max(1, min(99, int(max_rows)))
    tail = pts[-cap:]
    rows = ["```text", "time           value", "-------------  ------------"]
    for pair in tail:
        rows.append(f"{_fmt_ts_short(pair[0]):<13}  {_fmt_num(pair[1]):>12}")
    rows.append("```")
    return "\n".join(head + rows)


def _format_trigger_fallback_line(
    graph_label: str,
    series_label: str,
    analysis: Dict[str, Any],
    fast_threshold_pct: float,
    continuous_threshold_pct: float,
    window_seconds: int,
) -> Optional[str]:
    """
    Fallback reason when concise consecutive drop/spike lines are empty but ``hit_alert`` is true.
    Uses avg-drop window details so alert messages are never reasonless.
    """
    win_m = max(1, int(round(float(window_seconds) / 60.0)))
    if bool(analysis.get("hit_alert")):
        return (
            f"[{graph_label}] {series_label} alert triggered "
            f"(rules: >{fast_threshold_pct:g}% within {win_m}m or continuous >{continuous_threshold_pct:g}%)"
        )
    return None


def _format_alert_trigger_reply(payload: Dict[str, Any]) -> str:
    """
    Alert-only concise content:
    which graph/series, spike or drop, from value/time -> to value/time.
    """
    _mute_purge_expired()
    lines: List[str] = [
        "[ALERT] Monitoring thresholds exceeded",
        "Fast = sharpest move within ~2 minutes; Continuous = longest steady climb or drop.",
        "",
    ]
    reason_blocks: List[str] = []
    a_http = _http_analysis_for_payload(payload)
    if not _monitoring_alert_channel_muted("http"):
        reasons = _format_trigger_lines(
            GRAFANA_PANEL_TITLE,
            "HTTP",
            a_http,
            MONITORING_HTTP_DROP_ALERT_PCT,
            MONITORING_HTTP_CONTINUOUS_ALERT_PCT,
            MONITORING_ALERT_WINDOW_SECONDS,
        )
        if not reasons:
            fb = _format_trigger_fallback_line(
                GRAFANA_PANEL_TITLE,
                "HTTP",
                a_http,
                MONITORING_HTTP_DROP_ALERT_PCT,
                MONITORING_HTTP_CONTINUOUS_ALERT_PCT,
                MONITORING_ALERT_WINDOW_SECONDS,
            )
            if fb:
                reasons.append(fb)
        if MONITORING_SIMPLE_ALERT_TEXT and (reasons or bool(a_http.get("hit_alert"))):
            reasons = [_format_simple_series_alert_block(GRAFANA_PANEL_TITLE, "HTTP", a_http)]
        elif reasons and not MONITORING_SIMPLE_ALERT_TEXT:
            reasons.append(_format_alert_series_table_footer(GRAFANA_PANEL_TITLE, "HTTP", a_http))
        if reasons:
            reason_blocks.append("\n\n".join(reasons))
    for ex in payload.get("extraPanels") or []:
        if not isinstance(ex, dict):
            continue
        kind = (ex.get("kind") or "")
        if _monitoring_alert_channel_muted(kind):
            continue
        p2 = ex.get("payload") if isinstance(ex.get("payload"), dict) else {}
        if kind == "9280_push":
            g_lbl = GRAFANA_PANEL_TITLE_9280
            s_lbl = MONITORING_9280_SERIES_KEYWORD
            a2 = _analysis_for_9280_payload(p2)
            fast2 = MONITORING_9280_ALERT_PCT
            cont2 = MONITORING_9280_CONTINUOUS_ALERT_PCT
        elif kind == "deposit":
            g_lbl = GRAFANA_PANEL_TITLE_DEPOSIT
            s_lbl = MONITORING_DEPOSIT_SERIES_KEYWORD
            a2 = _analysis_for_deposit_payload(p2)
            fast2 = MONITORING_DEPOSIT_ALERT_PCT
            cont2 = MONITORING_DEPOSIT_CONTINUOUS_ALERT_PCT
        elif kind == "withdraw":
            g_lbl = GRAFANA_PANEL_TITLE_WITHDRAW
            s_lbl = MONITORING_WITHDRAW_SERIES_KEYWORD
            a2 = _analysis_for_withdraw_payload(p2)
            fast2 = MONITORING_WITHDRAW_ALERT_PCT
            cont2 = MONITORING_WITHDRAW_CONTINUOUS_ALERT_PCT
        elif kind == "provider_jili":
            g_lbl = GRAFANA_PANEL_TITLE_PROVIDER_JILI
            s_lbl = MONITORING_PROVIDER_JILI_SERIES_KEYWORD
            a2 = _analysis_for_provider_jili_payload(p2)
            fast2 = MONITORING_PROVIDER_JILI_ALERT_PCT
            cont2 = MONITORING_PROVIDER_JILI_CONTINUOUS_ALERT_PCT
        elif kind == "provider_general":
            g_lbl = GRAFANA_PANEL_TITLE_PROVIDER_GENERAL
            s_lbl = MONITORING_PROVIDER_GENERAL_SERIES_KEYWORD
            a2 = _analysis_for_provider_general_payload(p2)
            fast2 = MONITORING_PROVIDER_GENERAL_ALERT_PCT
            cont2 = MONITORING_PROVIDER_GENERAL_CONTINUOUS_ALERT_PCT
        elif kind == "provider_inhouse":
            g_lbl = GRAFANA_PANEL_TITLE_PROVIDER_INHOUSE
            s_lbl = MONITORING_PROVIDER_INHOUSE_SERIES_KEYWORD
            a2 = _analysis_for_provider_inhouse_payload(p2)
            fast2 = MONITORING_PROVIDER_INHOUSE_ALERT_PCT
            cont2 = MONITORING_PROVIDER_INHOUSE_CONTINUOUS_ALERT_PCT
        elif kind == "games_jili":
            g_lbl = GRAFANA_PANEL_TITLE_GAMES_JILI
            s_lbl = MONITORING_GAMES_JILI_SERIES_KEYWORD
            a2 = _analysis_for_games_jili_payload(p2)
            fast2 = MONITORING_GAMES_JILI_ALERT_PCT
            cont2 = MONITORING_GAMES_JILI_CONTINUOUS_ALERT_PCT
        elif kind == "games_general":
            g_lbl = GRAFANA_PANEL_TITLE_GAMES_GENERAL
            s_lbl = MONITORING_GAMES_GENERAL_SERIES_KEYWORD
            a2 = _analysis_for_games_general_payload(p2)
            fast2 = MONITORING_GAMES_GENERAL_ALERT_PCT
            cont2 = MONITORING_GAMES_GENERAL_CONTINUOUS_ALERT_PCT
        elif kind == "games_inhouse":
            g_lbl = GRAFANA_PANEL_TITLE_GAMES_INHOUSE
            s_lbl = MONITORING_GAMES_INHOUSE_SERIES_KEYWORD
            a2 = _analysis_for_games_inhouse_payload(p2)
            fast2 = MONITORING_GAMES_INHOUSE_ALERT_PCT
            cont2 = MONITORING_GAMES_INHOUSE_CONTINUOUS_ALERT_PCT
        else:
            continue
        reasons2 = _format_trigger_lines(
            g_lbl,
            s_lbl,
            a2,
            fast2,
            cont2,
            MONITORING_ALERT_WINDOW_SECONDS,
        )
        if not reasons2:
            fb2 = _format_trigger_fallback_line(
                g_lbl,
                s_lbl,
                a2,
                fast2,
                cont2,
                MONITORING_ALERT_WINDOW_SECONDS,
            )
            if fb2:
                reasons2.append(fb2)
        if MONITORING_SIMPLE_ALERT_TEXT and (reasons2 or bool(a2.get("hit_alert"))):
            reasons2 = [_format_simple_series_alert_block(g_lbl, s_lbl, a2)]
        elif reasons2 and not MONITORING_SIMPLE_ALERT_TEXT:
            reasons2.append(_format_alert_series_table_footer(g_lbl, s_lbl, a2))
        if reasons2:
            reason_blocks.append("\n\n".join(reasons2))
    if not reason_blocks:
        lines.append("Alert fired but no panel matched text details (no analyzable points).")
    else:
        lines.append("\n────────\n".join(reason_blocks))
    if TARGET_USER_OPEN_ID:
        lines.append("")
        lines.append(f"<at id={TARGET_USER_OPEN_ID}></at>")
    return "\n".join(lines)


def _monitoring_payload_hit_alert(payload: Dict[str, Any]) -> bool:
    _mute_purge_expired()
    if not _monitoring_alert_channel_muted("http") and bool(
        _http_analysis_for_payload(payload).get("hit_alert")
    ):
        return True
    for ex in payload.get("extraPanels") or []:
        if not isinstance(ex, dict):
            continue
        k = (ex.get("kind") or "")
        if _monitoring_alert_channel_muted(k):
            continue
        p2 = ex.get("payload") if isinstance(ex.get("payload"), dict) else {}
        if k == "9280_push" and bool(_analysis_for_9280_payload(p2).get("hit_alert")):
            return True
        if k == "deposit" and bool(_analysis_for_deposit_payload(p2).get("hit_alert")):
            return True
        if k == "withdraw" and bool(_analysis_for_withdraw_payload(p2).get("hit_alert")):
            return True
        if k == "provider_jili" and bool(_analysis_for_provider_jili_payload(p2).get("hit_alert")):
            return True
        if k == "provider_general" and bool(_analysis_for_provider_general_payload(p2).get("hit_alert")):
            return True
        if k == "provider_inhouse" and bool(_analysis_for_provider_inhouse_payload(p2).get("hit_alert")):
            return True
        if k == "games_jili" and bool(_analysis_for_games_jili_payload(p2).get("hit_alert")):
            return True
        if k == "games_general" and bool(_analysis_for_games_general_payload(p2).get("hit_alert")):
            return True
        if k == "games_inhouse" and bool(_analysis_for_games_inhouse_payload(p2).get("hit_alert")):
            return True
    return False


def _fmt_ts_short(ts: Any) -> str:
    try:
        ft = _bucket_ts_monitoring_minute(float(ts))
        return _monitoring_calendar_dt(ft).strftime("%m-%d %H:%M")
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
    fast_thr = float(analysis.get("fast_threshold_pct") or MONITORING_HTTP_DROP_ALERT_PCT)
    cont_thr = float(analysis.get("continuous_threshold_pct") or MONITORING_HTTP_CONTINUOUS_ALERT_PCT)
    win_sec = int(analysis.get("window_seconds") or MONITORING_ALERT_WINDOW_SECONDS)
    lines: List[str] = [
        "",
        f"[HTTP] alert when drop/spike > {fast_thr:g}% within {win_sec//60} minutes "
        f"or continuous drop/spike > {cont_thr:g}%",
    ]

    wd = analysis.get("window_max_drop")
    ws = analysis.get("window_max_spike")
    cd = analysis.get("consecutive_max_drop")
    cs = analysis.get("consecutive_max_spike")
    lines.append(
        f"within {win_sec//60}m drop/spike: -{(wd or {}).get('pct', 'n/a')}% / +{(ws or {}).get('pct', 'n/a')}%"
    )
    lines.append(
        f"continuous drop/spike : -{(cd or {}).get('pct', 'n/a')}% / +{(cs or {}).get('pct', 'n/a')}%"
    )

    return lines


def _format_monitoring_reply(payload: Dict[str, Any], *, include_target_mention: bool = True) -> str:
    """
    Lark-friendly compact layout: ``[panel] graph`` + short ``Dashboard: …/d/{uid}`` + HTTP table + footer.

    When the caller prepends ``_format_alert_trigger_reply`` (already contains ``<at>``), pass
    ``include_target_mention=False`` to avoid duplicate mentions.
    """
    max_rows = MONITORING_TABLE_TAIL_ROWS
    uid = str(payload.get("dashboardUid") or GRAFANA_DASHBOARD_UID)
    base = str(GRAFANA_BASE_URL).rstrip("/")
    http_ex = _http_analysis_for_payload(payload)

    lines: List[str] = [
        f"[{payload.get('panelTitle')}] graph",
        f"Dashboard: {base}/d/{uid}",
    ]
    # Show optional monitored series first so they are not truncated away by long HTTP tables.
    for ex in payload.get("extraPanels") or []:
        if not isinstance(ex, dict):
            continue
        k = (ex.get("kind") or "")
        p2 = ex.get("payload") if isinstance(ex.get("payload"), dict) else {}
        if k == "9280_push":
            a2 = _analysis_for_9280_payload(p2)
            title = GRAFANA_PANEL_TITLE_9280
            series = MONITORING_9280_SERIES_KEYWORD
            extra_footer = _format_extra_analysis_lines("9280 + Push", a2)
        elif k == "deposit":
            a2 = _analysis_for_deposit_payload(p2)
            title = GRAFANA_PANEL_TITLE_DEPOSIT
            series = MONITORING_DEPOSIT_SERIES_KEYWORD
            extra_footer = _format_extra_analysis_lines("Deposit", a2)
        elif k == "withdraw":
            a2 = _analysis_for_withdraw_payload(p2)
            title = GRAFANA_PANEL_TITLE_WITHDRAW
            series = MONITORING_WITHDRAW_SERIES_KEYWORD
            extra_footer = _format_extra_analysis_lines("Withdraw", a2)
        elif k == "provider_jili":
            a2 = _analysis_for_provider_jili_payload(p2)
            title = GRAFANA_PANEL_TITLE_PROVIDER_JILI
            series = MONITORING_PROVIDER_JILI_SERIES_KEYWORD
            extra_footer = _format_extra_analysis_lines("Provider JILI", a2)
        elif k == "provider_general":
            a2 = _analysis_for_provider_general_payload(p2)
            title = GRAFANA_PANEL_TITLE_PROVIDER_GENERAL
            series = MONITORING_PROVIDER_GENERAL_SERIES_KEYWORD
            extra_footer = _format_extra_analysis_lines("Provider GENERAL", a2)
        elif k == "provider_inhouse":
            a2 = _analysis_for_provider_inhouse_payload(p2)
            title = GRAFANA_PANEL_TITLE_PROVIDER_INHOUSE
            series = MONITORING_PROVIDER_INHOUSE_SERIES_KEYWORD
            extra_footer = _format_extra_analysis_lines("Provider INHOUSE", a2)
        elif k == "games_jili":
            a2 = _analysis_for_games_jili_payload(p2)
            title = GRAFANA_PANEL_TITLE_GAMES_JILI
            series = MONITORING_GAMES_JILI_SERIES_KEYWORD
            extra_footer = _format_extra_analysis_lines("Games JILI", a2)
        elif k == "games_general":
            a2 = _analysis_for_games_general_payload(p2)
            title = GRAFANA_PANEL_TITLE_GAMES_GENERAL
            series = MONITORING_GAMES_GENERAL_SERIES_KEYWORD
            extra_footer = _format_extra_analysis_lines("Games GENERAL", a2)
        elif k == "games_inhouse":
            a2 = _analysis_for_games_inhouse_payload(p2)
            title = GRAFANA_PANEL_TITLE_GAMES_INHOUSE
            series = MONITORING_GAMES_INHOUSE_SERIES_KEYWORD
            extra_footer = _format_extra_analysis_lines("Games INHOUSE", a2)
        else:
            continue
        pts2 = a2.get("merged_points") or []
        lines.append("")
        lines.append(f"[{title}] series: {series}")
        if pts2:
            tail2 = pts2[-max_rows:]
            rows2: List[str] = ["time           value", "-------------  ------------"]
            for pair in tail2:
                rows2.append(f"{_fmt_ts_short(pair[0]):<13}  {_fmt_num(pair[1]):>12}")
            lines.append("```text")
            lines.extend(rows2)
            lines.append("```")
        else:
            lines.append(f"(no {series} points matched)")
        lines.extend(extra_footer)

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
        for r in http_results[:6]:
            m = r.get("metric") or {}
            legend = _compact_http_legend(m, str(ref))
            vals = r.get("values") or []
            if not vals:
                lines.append(f"[{ref}] {legend}: (empty)")
                continue
            lines.append("")
            lines.append(f"[{ref}] {legend}")
            tail = vals[-max_rows:]
            rows: List[str] = ["time           value", "-------------  ------------"]
            for pair in tail:
                rows.append(f"{_fmt_ts_short(pair[0]):<13}  {_fmt_num(pair[1]):>12}")
            lines.append("```text")
            lines.extend(rows)
            lines.append("```")

    if (
        include_target_mention
        and _monitoring_payload_hit_alert(payload)
        and TARGET_USER_OPEN_ID
    ):
        lines.append(f"<at id={TARGET_USER_OPEN_ID}></at>")
    lines.extend(_format_http_analysis_lines(http_ex))

    return "\n".join(lines)


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
        msg = f"Screenshot refresh failed: {e}"
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


def _monitoring_watchdog_loop() -> None:
    """Periodic Grafana check; alert chat on >= threshold drop/spike."""
    global _monitoring_watch_last_alert_at
    sec = max(15.0, _cfg_float("MONITORING_WATCH_INTERVAL_SECONDS", 60.0))
    cool = max(0.0, _cfg_float("MONITORING_WATCH_ALERT_COOLDOWN_SECONDS", 300.0))
    qs, qe = _monitoring_watch_daily_quiet_tod_bounds()
    if qs < 0:
        q_note = "daily_quiet=disabled"
    else:
        sh, sm = qs // 3600, (qs // 60) % 60
        eh, em = qe // 3600, (qe // 60) % 60
        q_note = (
            f"daily_quiet=on server-local {sh:02d}:{sm:02d}–{eh:02d}:{em:02d} "
            f"(end time exclusive; no fetch/alert)"
        )
    logger.info(
        "monitoring watchdog started interval=%.0fs cooldown=%.0fs alert_chat=%r target_user=%r %s",
        sec,
        cool,
        bool((MONITORING_ALERT_CHAT_ID or "").strip()),
        bool((TARGET_USER_OPEN_ID or "").strip()),
        q_note,
    )
    while True:
        try:
            alert_chat = (MONITORING_ALERT_CHAT_ID or "").strip()
            if not alert_chat:
                logger.warning("monitoring watchdog: MONITORING_ALERT_CHAT_ID empty — skip this cycle")
                time.sleep(sec)
                continue

            if _monitoring_watch_in_daily_quiet_local():
                logger.debug(
                    "monitoring watchdog: skip — daily quiet window (MONITORING_WATCH_QUIET_WINDOW_ENABLE=0 to disable)"
                )
                time.sleep(sec)
                continue

            sess = grafana_login_session()
            payload = fetch_monitoring_payload(session=sess, for_watchdog=True)
            if not _monitoring_payload_hit_alert(payload):
                time.sleep(sec)
                continue

            now_m = time.monotonic()
            with _monitoring_reply_dispatch_lock:
                prev = _monitoring_watch_last_alert_at
                if cool > 0 and prev > 0 and (now_m - prev) < cool:
                    logger.info(
                        "monitoring watchdog alert skipped by cooldown (%.0fs left)",
                        cool - (now_m - prev),
                    )
                    time.sleep(sec)
                    continue
                _monitoring_watch_last_alert_at = now_m

            reply = _format_alert_trigger_reply(payload)
            pre_key: Optional[str] = None
            if _lark_env_truthy("MONITORING_CARD_EMBED_SCREENSHOT") and _lark_env_truthy("GRAFANA_SCREENSHOT_ENABLE"):
                try:
                    pre_key = _lark_upload_png_image_key(_grafana_watchdog_alert_screenshot_png(sess))
                except Exception:
                    logger.exception("monitoring watchdog pre-screenshot failed")

            used_card, embedded = _lark_send_monitoring_user_message(
                "chat_id",
                alert_chat,
                reply,
                pre_key if _lark_env_truthy("MONITORING_CARD_EMBED_SCREENSHOT") else None,
            )
            logger.info(
                "monitoring watchdog alert sent chat_prefix=%s... card=%s embedded_png=%s",
                alert_chat[:16],
                used_card,
                embedded,
            )

            if _lark_env_truthy("GRAFANA_SCREENSHOT_ENABLE") and not embedded:
                if pre_key:
                    try:
                        _lark_send_image_message("chat_id", alert_chat, pre_key)
                        logger.info("monitoring watchdog screenshot sent via pre_key")
                    except Exception:
                        logger.exception("monitoring watchdog pre_key image send failed")
                else:
                    try:
                        png = _grafana_watchdog_alert_screenshot_png(sess)
                        key = _lark_upload_png_image_key(png)
                        _lark_send_image_message("chat_id", alert_chat, key)
                        logger.info("monitoring watchdog screenshot sent bytes=%s", len(png))
                    except Exception:
                        logger.exception("monitoring watchdog screenshot send failed")
        except Exception:
            logger.exception("monitoring watchdog cycle failed")
        time.sleep(sec)


def _start_monitoring_watchdog_if_enabled() -> None:
    global _monitoring_watch_started
    if not _lark_env_truthy("MONITORING_WATCH_ENABLE"):
        logger.info("monitoring watchdog disabled (MONITORING_WATCH_ENABLE=0)")
        return
    with _monitoring_reply_dispatch_lock:
        if _monitoring_watch_started:
            return
        _monitoring_watch_started = True
    threading.Thread(target=_monitoring_watchdog_loop, daemon=True, name="monitoring-watchdog").start()


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
        alert_hit = False
        try:
            grafana_session = grafana_login_session()
            payload = fetch_monitoring_payload(session=grafana_session)
            alert_hit = _monitoring_payload_hit_alert(payload)
            reply = _format_monitoring_reply(payload, include_target_mention=not alert_hit)
            if alert_hit and payload is not None:
                reply = _format_alert_trigger_reply(payload) + "\n\n---\n\n" + reply
        except Exception as e:
            logger.exception("monitoring fetch failed (background)")
            reply = f"Failed to fetch monitoring data: {e}"
            grafana_session = None
            payload = None
            alert_hit = False

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
            alert_reply = _format_alert_trigger_reply(payload) if alert_hit and payload is not None else reply
            src_alias = {str(x).strip() for x in (source_chat_aliases or []) if str(x).strip()}
            if (chat_id or "").strip():
                src_alias.add((chat_id or "").strip())
            suppress_alert_copy = alert_chat_id in src_alias
            if alert_hit and alert_chat_id and not suppress_alert_copy:
                try:
                    _lark_send_text_auto("chat_id", alert_chat_id, alert_reply, max_chars=3200)
                    logger.info(
                        "monitoring alert copy sent (background) alert_chat_id_prefix=%s... len=%s",
                        alert_chat_id[:16],
                        len(alert_reply),
                    )
                except Exception:
                    logger.exception(
                        "monitoring alert forward failed (background) alert_chat_id=%r",
                        alert_chat_id[:24],
                    )
            elif alert_hit and alert_chat_id and suppress_alert_copy:
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

    chat_id = chat_resolved
    open_id = sender_open
    chat_aliases = _lark_message_chat_id_aliases(msg)
    sender_debounce = _lark_im_sender_debounce_token(sender, open_id)
    im_event_id = _lark_im_payload_event_id(data)
    msg_time = _lark_im_message_time_token(msg)

    mute_tri = (MONITORING_MUTE_TRIGGER or "/m").strip().lower()
    cancel_tri = (MONITORING_CANCELMUTE_TRIGGER or "/c").strip().lower()

    sp_cmd: Optional[str] = None
    if _im_command_matches(clean or "", MONITORING_MUTE_TRIGGER):
        sp_cmd = "mute"
    elif _im_command_matches(clean or "", MONITORING_CANCELMUTE_TRIGGER):
        sp_cmd = "cancelmute"

    if sp_cmd:
        processed_stick_m = _monitoring_processed_stick(
            mid, im_event_id, chat_id or "", sender_debounce, msg_time
        )
        body_key_m = "__mute_cmd__" if sp_cmd == "mute" else "__cancelmute_cmd__"
        debounce_key_m = f"{(chat_id or '').strip()}\n{body_key_m}"
        now_mm = time.monotonic()
        with _monitoring_reply_dispatch_lock:
            if im_event_id and im_event_id in _processed_lark_im_event_ids:
                logger.info("duplicate IM event_id=%s — skip (%s)", im_event_id, sp_cmd)
                return
            if processed_stick_m and processed_stick_m in _processed_lark_message_ids:
                logger.info("duplicate %s dispatch stick=%r — skip", sp_cmd, processed_stick_m[:96])
                return
            if debounce_key_m in _monitoring_inflight_keys:
                logger.info("%s skip — already in flight", sp_cmd)
                return
            _monitoring_inflight_keys.add(debounce_key_m)
            if processed_stick_m:
                _processed_lark_message_ids.add(processed_stick_m)
            if im_event_id:
                _processed_lark_im_event_ids.add(im_event_id)
                if len(_processed_lark_im_event_ids) > _PROCESSED_IM_EVENT_IDS_CAP:
                    _processed_lark_im_event_ids.clear()
                    _processed_lark_im_event_ids.add(im_event_id)
        logger.info("%s command accepted chat=%r open=%r", sp_cmd, bool(chat_id), bool(open_id))
        if sp_cmd == "mute":
            threading.Thread(
                target=_mute_send_selection_card_worker,
                args=(chat_id, open_id, debounce_key_m),
                daemon=True,
                name="mute-selection-card",
            ).start()
        else:
            threading.Thread(
                target=_cancelmute_worker,
                args=(chat_id, open_id, debounce_key_m),
                daemon=True,
                name="cancelmute",
            ).start()
        return

    cn = re.sub(r"\s+", " ", (clean or "").strip().lower())
    if (
        (LARK_BOT_OPEN_ID or "").strip()
        and _lark_message_mentions_bot(mentions)
        and cn
        and not _im_command_matches(clean or "", MONITORING_TRIGGER)
        and not _im_command_matches(clean or "", MONITORING_MUTE_TRIGGER)
        and not _im_command_matches(clean or "", MONITORING_CANCELMUTE_TRIGGER)
    ):
        processed_h = _monitoring_processed_stick(
            mid, im_event_id, chat_id or "", sender_debounce, msg_time
        )
        debounce_key_h = f"{(chat_id or '').strip()}\n__cmd_help__"
        with _monitoring_reply_dispatch_lock:
            if im_event_id and im_event_id in _processed_lark_im_event_ids:
                logger.info("duplicate IM event_id=%s — skip (cmd help)", im_event_id)
                return
            if processed_h and processed_h in _processed_lark_message_ids:
                logger.info("duplicate cmd-help stick=%r — skip", processed_h[:96])
                return
            if debounce_key_h in _monitoring_inflight_keys:
                logger.info("cmd help skip — already in flight")
                return
            _monitoring_inflight_keys.add(debounce_key_h)
            if processed_h:
                _processed_lark_message_ids.add(processed_h)
            if im_event_id:
                _processed_lark_im_event_ids.add(im_event_id)
                if len(_processed_lark_im_event_ids) > _PROCESSED_IM_EVENT_IDS_CAP:
                    _processed_lark_im_event_ids.clear()
                    _processed_lark_im_event_ids.add(im_event_id)
        logger.info("at-mention non-command — sending cmd help chat=%r", bool(chat_id))
        threading.Thread(
            target=_monitoring_at_mention_help_worker,
            args=(chat_id, open_id, debounce_key_h),
            daemon=True,
            name="cmd-help",
        ).start()
        return

    if not _text_should_run_monitoring(raw_text, clean, mentions):
        logger.info(
            "im.message no trigger raw=%r clean=%r (mo/mute/cancel=%r/%r/%r require_at_bot_for_mo=%s; see MONITORING_AT_MENTION_*)",
            (raw_text or "")[:160],
            (clean or "")[:160],
            MONITORING_TRIGGER,
            MONITORING_MUTE_TRIGGER,
            MONITORING_CANCELMUTE_TRIGGER,
            MONITORING_TRIGGER_REQUIRES_AT_BOT,
        )
        return

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
            extra = _lark_dispatch_card_action(data)
            if isinstance(extra, dict) and extra:
                return _lark_min_json_response(extra)
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
    _start_grafana_playwright_keeper_if_enabled()
    _start_monitoring_watchdog_if_enabled()
    port = _cfg_listen_port(5002)
    if MONITORING_AT_MENTION_ENABLE and not (LARK_BOT_OPEN_ID or "").strip():
        logger.warning(
            "MONITORING_AT_MENTION_ENABLE is on but LARK_BOT_OPEN_ID is empty — @-only triggers will not match; set LARK_BOT_OPEN_ID to this bot's open_id."
        )
    if MONITORING_TRIGGER_REQUIRES_AT_BOT and not (LARK_BOT_OPEN_ID or "").strip():
        logger.warning(
            "MONITORING_TRIGGER_REQUIRES_AT_BOT is on but LARK_BOT_OPEN_ID is empty — explicit triggers like %r will never match; set LARK_BOT_OPEN_ID or set MONITORING_TRIGGER_REQUIRES_AT_BOT=0.",
            (MONITORING_TRIGGER or "/mo").strip(),
        )
    if int(GRAFANA_QUERY_LOOKBACK_SECONDS) != 900:
        logger.warning(
            "GRAFANA_QUERY_LOOKBACK_SECONDS=%s (default 900 = 15m) — /monitoring Prometheus window differs from default 15 minutes",
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