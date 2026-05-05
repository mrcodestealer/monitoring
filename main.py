#!/usr/bin/env python3
"""
最小 Lark 群聊机器人：仅在群内，满足以下任一条件则回复 ``hi``：
  - @ 机器人 且消息里含 ``hi`` / ``hello`` / ``hey``
  - 或整句只有 ``hi`` / ``hello`` / ``hey``（群内纯打招呼）

配置：只改下面 ``_CFG``。也可用 systemd ``Environment=KEY=value`` 覆盖（非空即覆盖）。

启动：``python main.py``（默认端口 **5002**，Flask ``threaded=True``，与 Chatbox 同类 HTTP 回调）

飞书后台：事件与回调 → Request URL → ``http://<公网IP>:5002/webhook/event``  
订阅：消息与群组 → 接收消息；权限需含发消息等。
"""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import os
import re
import threading
from typing import Any, Dict, List, Optional

import requests
from flask import Flask, jsonify, request

# ---------------------------------------------------------------------------
_CFG: Dict[str, Any] = {
    "PORT": 5002,
    "LARK_HOST": "https://open.larksuite.com",
    "VERIFICATION_TOKEN": "QlZMYp7rogAS914dxxMVNgboUKxQP7jc",
    "APP_ID": "cli_a97fcc6df7615ed1",
    "APP_SECRET": "NwAi6xJxMYDHMFAQcTG8ZfJxpeTOibvy",
    "LARK_ENCRYPT_KEY": "",
    # 若设置，忽略机器人自己发出的消息（open_id）
    "LARK_BOT_OPEN_ID": "",
}


def _cfg_raw(key: str) -> Any:
    if key in os.environ and str(os.environ.get(key, "")).strip() != "":
        return os.environ[key]
    return _CFG.get(key)


def _cfg_str(key: str, default: str = "") -> str:
    v = _cfg_raw(key)
    if v is None:
        return default
    s = str(v).strip()
    return s if s else default


def _cfg_listen_port(default: int = 5002) -> int:
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


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

LARK_HOST = _cfg_str("LARK_HOST", "https://open.feishu.cn").rstrip("/")
VERIFICATION_TOKEN = _cfg_str("VERIFICATION_TOKEN", "").strip()
APP_ID = _cfg_str("APP_ID", "").strip() or None
APP_SECRET = _cfg_str("APP_SECRET", "").strip() or None
LARK_ENCRYPT_KEY = (
    _cfg_str("LARK_ENCRYPT_KEY")
    or _cfg_str("ENCRYPT_KEY")
    or _cfg_str("FEISHU_ENCRYPT_KEY")
    or ""
).strip()
LARK_BOT_OPEN_ID = _cfg_str("LARK_BOT_OPEN_ID", "").strip()

if not APP_ID or not APP_SECRET:
    logger.warning(
        "APP_ID / APP_SECRET 为空：无法拉 tenant_access_token、也无法发消息。"
        "请填 ``_CFG`` 或在 systemd 里 ``Environment=APP_ID=...`` ``Environment=APP_SECRET=...``。"
    )

app = Flask(__name__)

_processed_message_ids: set = set()
_process_lock = threading.Lock()


def _lark_dict_pick_str(d: Any, *keys: str) -> str:
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


def _feishu_decrypt_encrypt_field(ciphertext_b64: str, encrypt_key: str) -> str:
    from Crypto.Cipher import AES

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
    if not isinstance(raw, dict) or "encrypt" not in raw:
        return raw
    if not LARK_ENCRYPT_KEY:
        logger.warning("Body has encrypt but LARK_ENCRYPT_KEY unset")
        return raw
    try:
        plain = _feishu_decrypt_encrypt_field(str(raw["encrypt"]), LARK_ENCRYPT_KEY)
        if plain.startswith("\ufeff"):
            plain = plain.lstrip("\ufeff")
        return json.loads(plain)
    except Exception as e:
        logger.exception("decrypt failed: %s", e)
        return raw


def _lark_safe_parse_json_body(req: Any) -> Optional[Dict[str, Any]]:
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


def _lark_header_event_type(data: Dict[str, Any]) -> str:
    h = data.get("header")
    if isinstance(h, dict) and h.get("event_type") is not None:
        return str(h.get("event_type")).strip()
    if data.get("event_type") is not None:
        return str(data.get("event_type")).strip()
    return ""


def _lark_extract_verification_token(data: Dict[str, Any]) -> Optional[str]:
    h = data.get("header")
    if isinstance(h, dict):
        for key in ("token", "Token", "verification_token"):
            t = h.get(key)
            if t is not None:
                return str(t).strip()
    if data.get("verification_token") is not None:
        return str(data.get("verification_token")).strip()
    t2 = data.get("token")
    if t2 is None:
        return None
    ts = str(t2).strip()
    if ts.startswith("c-") or ts.startswith("d-"):
        return None
    return ts


def _lark_coerce_event_dict(data: Dict[str, Any]) -> Dict[str, Any]:
    ev = data.get("event")
    if isinstance(ev, str):
        try:
            data["event"] = json.loads(ev)
        except Exception:
            data["event"] = {}
    if not isinstance(data.get("event"), dict):
        data["event"] = {}
    return data


def _lark_legacy_event_callback_message_to_v2(data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
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


def _lark_collect_post_text(obj: Any, out: List[str]) -> None:
    if isinstance(obj, dict):
        tag = obj.get("tag")
        if tag == "text" and "text" in obj:
            out.append(str(obj.get("text") or ""))
        elif tag in ("a", "code") and "text" in obj:
            out.append(str(obj.get("text") or ""))
        for v in obj.values():
            _lark_collect_post_text(v, out)
    elif isinstance(obj, list):
        for x in obj:
            _lark_collect_post_text(x, out)


def _lark_extract_plain_text_from_message(msg: Dict[str, Any]) -> str:
    if not isinstance(msg, dict):
        return ""
    raw_c = msg.get("content") or msg.get("Content") or msg.get("body")
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
    if mtype == "text":
        return str(obj.get("text") or "")
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
                            parts.append(str(cell.get("text") or ""))
                elif isinstance(row, dict) and row.get("tag") == "text":
                    parts.append(str(row.get("text") or ""))
            if parts:
                return "".join(parts)
        parts2: List[str] = []
        _lark_collect_post_text(obj, parts2)
        if parts2:
            return "".join(parts2)
    parts3: List[str] = []
    _lark_collect_post_text(obj, parts3)
    if parts3:
        return "".join(parts3)
    return str(obj.get("text") or "")


def _lark_message_chat_id(msg: Dict[str, Any]) -> str:
    cid = _lark_dict_pick_str(msg, "chat_id", "chatId", "open_chat_id", "openChatId")
    if cid:
        return cid
    c = msg.get("container")
    if isinstance(c, dict):
        return _lark_dict_pick_str(c, "chat_id", "chatId", "open_chat_id", "openChatId")
    return ""


def _is_group_chat(msg: Dict[str, Any]) -> bool:
    ct = (_lark_dict_pick_str(msg, "chat_type", "chatType") or "").lower()
    if ct == "group":
        return True
    cid = _lark_message_chat_id(msg)
    return cid.startswith("oc_")


def _clean_for_hi(raw: str, mentions: Any) -> str:
    text = raw or ""
    if isinstance(mentions, list):
        for m in mentions:
            if isinstance(m, dict) and m.get("key"):
                text = text.replace(str(m["key"]), "")
    text = re.sub(r"@_user_\d+", "", text)
    text = re.sub(r"<[^>]+>", "", text)
    text = re.sub(r"[\u200b\uFEFF\u00A0]", "", text)
    text = re.sub(r"\s+", " ", text).strip().lower()
    text = re.sub(r"[!,.。！？]+", "", text).strip()
    return text


def _has_mention(mentions: Any, raw_lower: str) -> bool:
    if isinstance(mentions, list) and len(mentions) > 0:
        return True
    if "<at" in raw_lower:
        return True
    return bool(re.search(r"(^|\s)@[^\s]+", raw_lower))


def _wants_hi_reply(raw_text: str, clean: str, mentions: Any) -> bool:
    raw_l = (raw_text or "").lower()
    if not re.search(r"\b(hi|hello|hey)\b", clean):
        return False
    # 纯打招呼：整句就是 hi/hello/hey
    if clean in ("hi", "hello", "hey"):
        return True
    # @ 机器人 + 含 hi 等
    return _has_mention(mentions, raw_l)


def _tenant_access_token() -> str:
    if not APP_ID or not APP_SECRET:
        raise ValueError("APP_ID and APP_SECRET required in _CFG")
    url = f"{LARK_HOST}/open-apis/auth/v3/tenant_access_token/internal"
    r = requests.post(
        url,
        json={"app_id": APP_ID, "app_secret": APP_SECRET},
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


def _send_text_to_chat(chat_id: str, text: str) -> None:
    tok = _tenant_access_token()
    url = f"{LARK_HOST}/open-apis/im/v1/messages"
    r = requests.post(
        url,
        params={"receive_id_type": "chat_id"},
        headers={
            "Authorization": f"Bearer {tok}",
            "Content-Type": "application/json; charset=utf-8",
        },
        json={
            "receive_id": chat_id,
            "msg_type": "text",
            "content": json.dumps({"text": text}, ensure_ascii=False),
        },
        timeout=30,
    )
    j = r.json()
    if r.status_code >= 400 or int(j.get("code", -1)) != 0:
        raise RuntimeError(f"send_message http={r.status_code} body={j}")


def _hi_worker(chat_id: str, mid: str) -> None:
    try:
        _send_text_to_chat(chat_id, "hi")
        logger.info("replied hi chat_id_prefix=%s mid=%r", chat_id[:16], mid)
    except Exception:
        logger.exception("send hi failed")


def _handle_im_message(data: Dict[str, Any]) -> None:
    data = _lark_coerce_event_dict(dict(data))
    event = data.get("event") if isinstance(data.get("event"), dict) else {}
    msg = event.get("message")
    if not isinstance(msg, dict):
        return
    if not _is_group_chat(msg):
        logger.debug("skip non-group message")
        return
    mid = _lark_dict_pick_str(msg, "message_id", "messageId")
    mtype = (_lark_dict_pick_str(msg, "message_type", "messageType") or "").lower()
    if mtype and mtype not in ("text", "post", ""):
        return

    send_wrap = event.get("sender")
    if not isinstance(send_wrap, dict):
        send_wrap = {}
    sid = send_wrap.get("sender_id") or send_wrap.get("senderId")
    sender = sid if isinstance(sid, dict) else {}
    sender_open = _lark_dict_pick_str(sender, "open_id", "openId", "user_id", "userId")
    if LARK_BOT_OPEN_ID and sender_open == LARK_BOT_OPEN_ID:
        return

    raw_text = _lark_extract_plain_text_from_message(msg)
    if not raw_text.strip():
        raw_text = _lark_dict_pick_str(
            event, "text_without_at_bot", "textWithoutAtBot", "text"
        )
    mentions_raw = msg.get("mentions") or []
    mentions = mentions_raw if isinstance(mentions_raw, list) else []
    clean = _clean_for_hi(raw_text, mentions)

    if not _wants_hi_reply(raw_text, clean, mentions):
        return

    chat_id = _lark_message_chat_id(msg)
    if not chat_id:
        logger.warning("no chat_id, cannot reply")
        return

    with _process_lock:
        if mid and mid in _processed_message_ids:
            return
        if mid:
            _processed_message_ids.add(mid)
        if len(_processed_message_ids) > 4000:
            _processed_message_ids.clear()

    threading.Thread(
        target=_hi_worker, args=(chat_id, mid), daemon=True, name="hi-reply"
    ).start()


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"ok": True})


@app.route("/webhook/event", methods=["POST", "GET", "OPTIONS", "HEAD"], strict_slashes=False)
def webhook_event():
    if request.method == "OPTIONS":
        return "", 204
    if request.method == "HEAD":
        return "", 200
    if request.method == "GET":
        return jsonify(
            {
                "ok": True,
                "service": "monitoring_hi_bot",
                "hint": "Feishu POSTs events to this URL. Port default 5002.",
            }
        )

    print(
        "[lark] webhook POST len=%s path=%s ct=%s"
        % (
            request.content_length,
            request.path,
            (request.headers.get("Content-Type") or "")[:80],
        ),
        flush=True,
    )

    raw_in = _lark_safe_parse_json_body(request)
    if raw_in is None:
        return jsonify({"error": "invalid json"}), 400

    if isinstance(raw_in, dict) and raw_in.get("type") == "url_verification":
        tok = raw_in.get("token")
        if VERIFICATION_TOKEN and str(tok or "").strip() != VERIFICATION_TOKEN:
            return jsonify({"error": "Invalid token"}), 403
        return jsonify({"challenge": raw_in.get("challenge", "")})

    data_any = _feishu_maybe_decrypt_webhook_payload(raw_in)
    if not isinstance(data_any, dict):
        return jsonify({"error": "invalid payload"}), 400
    data = data_any

    if isinstance(raw_in, dict) and raw_in.get("encrypt") is not None and data is raw_in:
        return jsonify({"error": "decrypt failed"}), 403

    data = _lark_normalize_webhook(data)
    data = _lark_coerce_event_dict(data)

    if isinstance(data, dict) and data.get("type") == "url_verification":
        tok = data.get("token")
        if VERIFICATION_TOKEN and str(tok or "").strip() != VERIFICATION_TOKEN:
            return jsonify({"error": "Invalid token"}), 403
        return jsonify({"challenge": data.get("challenge", "")})

    if VERIFICATION_TOKEN:
        got = _lark_extract_verification_token(data)
        if got != VERIFICATION_TOKEN:
            logger.warning("token mismatch got=%r", got)
            return jsonify({"error": "Invalid token"}), 403

    et = _lark_header_event_type(data)
    if et in ("im.message.receive_v1", "im.message.receive_v2"):
        ref = dict(data)

        def _run() -> None:
            try:
                _handle_im_message(ref)
            except Exception:
                logger.exception("im.message handler failed")

        threading.Thread(target=_run, daemon=True, name="im-worker").start()
        return jsonify({}), 200

    return jsonify({}), 200


def main() -> None:
    port = _cfg_listen_port(5002)
    logger.info("Listening 0.0.0.0:%s POST /webhook/event (Flask threaded=True)", port)
    app.run(host="0.0.0.0", port=port, threaded=True, use_reloader=False, debug=False)


if __name__ == "__main__":
    main()
