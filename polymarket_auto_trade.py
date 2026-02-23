#!/usr/bin/env python3
"""
Polymarket BTC 15分钟自动交易脚本 (WebSocket版本)
功能: 实时监控市场 → 检查条件 → 自动下单 → 止损管理
使用 WebSocket 获取实时价格数据,延迟更低
"""
import os
import sys
import time
import json
import threading
import requests
from datetime import datetime, timezone
from urllib.parse import urlencode
from dotenv import load_dotenv
from flask import Flask, jsonify, send_from_directory

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(BASE_DIR, "static")

# 加载配置
load_dotenv(os.path.join(BASE_DIR, "config.env"))

try:
    from py_clob_client.client import ClobClient
    from py_clob_client.clob_types import OrderArgs
    from py_clob_client.order_builder.constants import BUY, SELL
    HAS_CLOB = True
except:
    HAS_CLOB = False
    print("⚠️  请安装: pip install py-clob-client")
    sys.exit(1)

try:
    import websocket
    HAS_WS = True
except:
    HAS_WS = False
    print("⚠️  请安装: pip install websocket-client")
    sys.exit(1)

try:
    from web3 import Web3
    HAS_WEB3 = True
except:
    HAS_WEB3 = False

# ============== 配置 ==============
GAMMA_API = "https://gamma-api.polymarket.com"
CRYPTO_PRICE_API = "https://polymarket.com/api/crypto/crypto-price"
BINANCE_WSS = "wss://stream.binance.com:9443/ws/btcusdt@trade"
POLYMARKET_WSS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
CLOB_API = "https://clob.polymarket.com"
RTDS_WS = "wss://ws-live-data.polymarket.com"  # Chainlink价格WebSocket
DATA_API = "https://data-api.polymarket.com"
CTF_CONTRACT = "0x4d97dcd97ec945f40cf65f87097ace5ea0476045"
USDC_E_CONTRACT = "0x2791bca1f2de4661ed88a30c99a7a9449aa84174"

# 代理配置
HTTP_PROXY = os.getenv("HTTP_PROXY", "")
HTTPS_PROXY = os.getenv("HTTPS_PROXY", "")

# 构建代理字典
PROXIES = {}
if HTTP_PROXY:
    PROXIES["http"] = HTTP_PROXY
if HTTPS_PROXY:
    PROXIES["https"] = HTTPS_PROXY

def parse_proxy_url(url):
    """解析代理URL，返回 (host, port, auth)"""
    if not url:
        return None, None, None
    try:
        from urllib.parse import urlparse
        parsed = urlparse(url)
        username = parsed.username
        password = parsed.password
        # 处理某些情况下username包含特殊字符被错误解析的问题
        if not username and '@' in parsed.netloc:
             # 手动解析
             auth_part, host_part = parsed.netloc.split('@', 1)
             if ':' in auth_part:
                 username, password = auth_part.split(':', 1)
             else:
                 username = auth_part
             
             if ':' in host_part:
                 hostname, port = host_part.split(':', 1)
                 try:
                     port = int(port)
                 except:
                     port = 80
             else:
                 hostname = host_part
                 port = 80
             return hostname, port, (username, password)
        
        return parsed.hostname, parsed.port, username and password and (username, password)
    except Exception as e:
        log(f"代理解析失败: {e}", "ERR")
        return None, None, None

def check_proxy_ip():
    """检查当前IP (验证代理是否生效)"""
    try:
        log("正在检查当前IP...", "INFO")
        r = requests.get("https://httpbin.org/ip", proxies=PROXIES if PROXIES else None, timeout=10)
        if r.status_code == 200:
            ip = r.json().get("origin")
            log(f"当前IP: {ip}", "INFO", force=True)
            if PROXIES:
                log(f"代理已配置: {PROXIES}", "INFO")
        else:
            log(f"IP检查失败: {r.status_code}", "WARN")
    except Exception as e:
        log(f"IP检查异常: {e}", "WARN")

# 交易配置
AUTO_TRADE = os.getenv("AUTO_TRADE", "false").lower() == "true"
TRADE_AMOUNT = float(os.getenv("TRADE_AMOUNT", "5"))

# 条件配置
C1_TIME = int(os.getenv("CONDITION_1_TIME", "40"))
C1_DIFF = float(os.getenv("CONDITION_1_DIFF", "60"))
C2_TIME = int(os.getenv("CONDITION_2_TIME", "60"))
C2_DIFF = float(os.getenv("CONDITION_2_DIFF", "80"))
C3_TIME = int(os.getenv("CONDITION_3_TIME", "120"))
C3_DIFF = float(os.getenv("CONDITION_3_DIFF", "180"))

# 风控配置
STOP_LOSS_DIFF = float(os.getenv("STOP_LOSS_DIFF", "40"))
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "2"))

AUTO_REDEEM = os.getenv("AUTO_REDEEM", "true").lower() == "true"
POLYGON_RPC_URL = os.getenv("POLYGON_RPC_URL", "")
REDEEM_SCAN_INTERVAL = max(3, int(os.getenv("REDEEM_SCAN_INTERVAL", "15")))
REDEEM_RETRY_INTERVAL = max(10, int(os.getenv("REDEEM_RETRY_INTERVAL", "120")))
REDEEM_MAX_PER_SCAN = max(1, int(os.getenv("REDEEM_MAX_PER_SCAN", "2")))
REDEEM_PENDING_LOG_INTERVAL = max(10, int(os.getenv("REDEEM_PENDING_LOG_INTERVAL", "30")))
POLY_BUILDER_API_KEY = os.getenv("POLY_BUILDER_API_KEY", "")
POLY_BUILDER_SECRET = os.getenv("POLY_BUILDER_SECRET", "")
POLY_BUILDER_PASSPHRASE = os.getenv("POLY_BUILDER_PASSPHRASE", "")
RELAYER_URL = os.getenv("RELAYER_URL", "https://relayer-v2.polymarket.com")
RELAYER_TX_TYPE = os.getenv("RELAYER_TX_TYPE", "SAFE").upper()
DASHBOARD_ACCOUNT_SYNC_SEC = max(10, int(os.getenv("DASHBOARD_ACCOUNT_SYNC_SEC", "20")))

WEB_ENABLED = os.getenv("WEB_ENABLED", "true").lower() == "true"
WEB_HOST = os.getenv("WEB_HOST", "0.0.0.0")
WEB_PORT = int(os.getenv("WEB_PORT", "5080"))

# 状态文件
STATE_FILE = os.path.join(BASE_DIR, "state.json")

# 全局价格数据
price_data = {
    "btc": None,           # Chainlink BTC价格 (交易依据)
    "binance": None,       # 币安BTC价格 (仅参考)
    "ptb": None,           # Price to Beat
    "up_price": None,      # UP token价格
    "down_price": None,    # DOWN token价格
    "last_update": None,
}

dashboard_lock = threading.Lock()
dashboard_state = {
    "updated_at": None,
    "market": {},
    "prices": {},
    "position": {},
    "pending_order": {},
    "last_order": {},
    "trade_history": [],
    "wallet_positions": [],
    "wallet_history": [],
    "live_trades": [],
    "live_positions_count": 0,
    "live_realized_pnl": 0.0,
    "live_unrealized_pnl": 0.0,
    "live_total_pnl": 0.0,
    "auto_redeem": {},
    "activity": [],
}

app = Flask(__name__, static_folder=STATIC_DIR)


def _dashboard_set(**kwargs):
    with dashboard_lock:
        for k, v in kwargs.items():
            dashboard_state[k] = v
        dashboard_state["updated_at"] = datetime.now().isoformat()


@app.route("/")
def dashboard_index():
    return send_from_directory(STATIC_DIR, "dashboard.html")


@app.route("/api/status")
def dashboard_status():
    with dashboard_lock:
        return jsonify(dict(dashboard_state))


@app.route("/api/logs")
def dashboard_logs():
    with dashboard_lock:
        return jsonify({"items": list(dashboard_state.get("activity") or [])[-300:]})


@app.route("/api/history")
def dashboard_history():
    with dashboard_lock:
        live_items = list(dashboard_state.get("live_trades") or [])
        if live_items:
            return jsonify({"items": live_items[-300:]})
        local_items = list(dashboard_state.get("trade_history") or [])
        wallet_items = list(dashboard_state.get("wallet_history") or [])
        return jsonify({"items": (local_items + wallet_items)[-300:]})


def start_web_server():
    if not WEB_ENABLED:
        return

    def run():
        app.run(host=WEB_HOST, port=WEB_PORT, threaded=True, use_reloader=False)

    t = threading.Thread(target=run, daemon=True)
    t.start()

# ============== 工具函数 ==============
def log(msg, level="INFO", force=False):
    """日志输出"""
    if force or level in ["OK", "ERR", "WARN", "TRADE"]:
        icons = {"INFO": "ℹ️", "OK": "✅", "ERR": "❌", "WARN": "⚠️", "TRADE": "💰"}
        icon = icons.get(level, "ℹ️")
        ts = datetime.now().strftime("%H:%M:%S")
        log_msg = f"[{ts}] {icon} {msg}"
        print(log_msg)

        with dashboard_lock:
            arr = dashboard_state.get("activity") or []
            arr.append({
                "time": ts,
                "level": level,
                "message": str(msg),
            })
            if len(arr) > 400:
                arr = arr[-400:]
            dashboard_state["activity"] = arr
            dashboard_state["updated_at"] = datetime.now().isoformat()
        
        # 只写入重要日志到文件: TRADE(交易)和ERR(错误)
        if level in ["TRADE", "ERR"]:
            try:
                with open("trade.log", "a", encoding="utf-8") as f:
                    f.write(log_msg + "\n")
            except:
                pass

def get_binance_btc_price():
    """从币安API获取BTC价格"""
    try:
        r = requests.get("https://api.binance.com/api/v3/ticker/price", 
                        params={"symbol": "BTCUSDT"}, 
                        proxies=PROXIES if PROXIES else None,
                        timeout=5)
        if r.status_code == 200:
            return float(r.json().get("price"))
    except:
        pass
    return None



def get_crypto_price_api(start_time, end_time):
    """
    从 Polymarket crypto-price API 获取 PTB
    返回: {"openPrice": PTB, "closePrice": 当前价格或None, "completed": bool}
    """
    try:
        # 如果是字符串,直接使用;如果是datetime,转换为字符串
        if isinstance(start_time, str):
            start_str = start_time.replace("Z", "+00:00")
            if "+" in start_str:
                start_str = start_str.split("+")[0] + "Z"
            else:
                start_str = start_time
        else:
            start_str = start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        
        if isinstance(end_time, str):
            end_str = end_time.replace("Z", "+00:00")
            if "+" in end_str:
                end_str = end_str.split("+")[0] + "Z"
            else:
                end_str = end_time
        else:
            end_str = end_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        
        params = {
            "symbol": "BTC",
            "eventStartTime": start_str,
            "variant": "fifteen",
            "endDate": end_str
        }
        
        # 添加请求头,模拟浏览器
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json",
            "Referer": "https://polymarket.com/"
        }
        
        log(f"请求PTB: {CRYPTO_PRICE_API}?{urlencode(params)}", "INFO")
        r = requests.get(CRYPTO_PRICE_API, params=params, headers=headers, 
                        proxies=PROXIES if PROXIES else None, timeout=10)
        
        log(f"PTB响应状态: {r.status_code}", "INFO")
        
        if r.status_code == 200:
            data = r.json()
            log(f"PTB数据: {data}", "INFO")
            return data
        else:
            log(f"PTB请求失败: HTTP {r.status_code} - {r.text[:200]}", "ERR")
    except Exception as e:
        log(f"获取 crypto-price 失败: {type(e).__name__}: {str(e)}", "ERR")
    return {}

def get_current_slug():
    """根据当前时间计算slug"""
    ts = int(time.time())
    current_15m = (ts // 900) * 900
    return f"btc-updown-15m-{current_15m}"

def get_next_slug():
    """根据下一个15分钟时间计算slug"""
    ts = int(time.time())
    next_15m = ((ts // 900) + 1) * 900
    return f"btc-updown-15m-{next_15m}"

def get_active_market():
    """获取当前活跃的15分钟BTC市场"""
    try:
        # 先尝试当前15分钟周期的市场
        current_slug = get_current_slug()
        market = fetch_market_by_slug(current_slug)
        if market and market["remaining"] > 0:
            log(f"找到当前市场: {current_slug[:40]}... (剩余{market['remaining']//60}分{market['remaining']%60}秒)", "OK")
            return market
        
        # 如果当前市场已结束或不存在,尝试下一个周期
        next_slug = get_next_slug()
        market = fetch_market_by_slug(next_slug)
        if market and market["remaining"] > 0:
            log(f"找到下一市场: {next_slug[:40]}... (剩余{market['remaining']//60}分{market['remaining']%60}秒)", "OK")
            return market
        
        log("当前和下一周期都没有活跃市场", "WARN")
        
    except Exception as e:
        log(f"获取市场失败: {e}", "ERR")
        import traceback
        traceback.print_exc()
    return None

def fetch_market_by_slug(slug):
    """根据slug获取市场数据"""
    try:
        r = requests.get(f"{GAMMA_API}/events", params={"slug": slug}, 
                        proxies=PROXIES if PROXIES else None, timeout=10)
        data = r.json()
        
        if not data:
            return None
        
        event = data[0]
        
        # 检查市场是否关闭
        if event.get("closed", False):
            return None
        
        end_str = event.get("endDate", "")
        start_str = event.get("startTime", "")
        if not end_str or not start_str:
            return None
        
        # 计算剩余时间 (使用time.time()以获得更准确的UTC时间戳)
        now = time.time()
        end_ts = datetime.fromisoformat(end_str.replace("Z", "+00:00")).timestamp()
        remaining_time = int(end_ts - now)
        
        if remaining_time <= 0:
            return None
        
        # 解析市场数据
        markets = event.get("markets", [])
        if not markets:
            return None
        
        m = markets[0]
        outcomes = json.loads(m.get("outcomes", "[]")) if isinstance(m.get("outcomes"), str) else m.get("outcomes", [])
        prices = json.loads(m.get("outcomePrices", "[]")) if isinstance(m.get("outcomePrices"), str) else m.get("outcomePrices", [])
        tokens = json.loads(m.get("clobTokenIds", "[]")) if isinstance(m.get("clobTokenIds"), str) else m.get("clobTokenIds", [])
        
        # 假设第一个是UP,第二个是DOWN
        up_price = float(prices[0]) if len(prices) > 0 else None
        down_price = float(prices[1]) if len(prices) > 1 else None
        up_token = tokens[0] if len(tokens) > 0 else None
        down_token = tokens[1] if len(tokens) > 1 else None
        
        return {
            "slug": slug,
            "start": start_str,
            "end": end_str,
            "remaining": remaining_time,
            "up_price": up_price,
            "down_price": down_price,
            "up_token": up_token,
            "down_token": down_token
        }
    except Exception as e:
        # 静默失败,可能是市场不存在
        return None

def get_ptb(start_time, end_time):
    """获取Price to Beat"""
    try:
        params = {
            "symbol": "BTC",
            "eventStartTime": start_time,
            "variant": "fifteen",
            "endDate": end_time
        }
        r = requests.get(CRYPTO_PRICE_API, params=params, timeout=10)
        if r.status_code == 200:
            data = r.json()
            return float(data.get("openPrice")) if data.get("openPrice") else None
    except:
        pass
    return None


def _normalize_state(state):
    if not isinstance(state, dict):
        state = {}
    if not isinstance(state.get("position"), dict):
        state["position"] = {}
    if not isinstance(state.get("pending_order"), dict):
        state["pending_order"] = {}
    if not isinstance(state.get("last_order"), dict):
        state["last_order"] = {}
    if not isinstance(state.get("trade_history"), list):
        state["trade_history"] = []
    return state


def _append_trade_history(state, item):
    state = _normalize_state(state)
    hist = list(state.get("trade_history") or [])
    hist.append(item)
    if len(hist) > 300:
        hist = hist[-300:]
    state["trade_history"] = hist
    _dashboard_set(trade_history=list(hist))
    return state


def _to_float(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return float(default)


def _maybe_float(value):
    try:
        return float(value)
    except Exception:
        return None


def _to_bool(value):
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    s = str(value).strip().lower()
    return s in ("1", "true", "yes", "y", "on")


def _data_api_get(path, params=None):
    try:
        r = requests.get(
            f"{DATA_API}{path}",
            params=params or {},
            proxies=PROXIES if PROXIES else None,
            timeout=12,
        )
        if r.status_code == 200:
            return r.json()
    except Exception:
        return None
    return None


def _text_scalar(v):
    if isinstance(v, (str, int, float, bool)):
        return str(v).strip()
    return ""


def _normalize_outcome_label(v):
    s = str(v or "").upper()
    if "UP" in s or s == "YES":
        return "UP"
    if "DOWN" in s or s == "NO":
        return "DOWN"
    return s or "-"


def _trade_pick_field(tr, *keys):
    if not isinstance(tr, dict):
        return ""
    sources = [tr]
    market = tr.get("market")
    if isinstance(market, dict):
        sources.append(market)
    event = tr.get("event")
    if isinstance(event, dict):
        sources.append(event)
    for src in sources:
        for k in keys:
            if k not in src:
                continue
            s = _text_scalar(src.get(k))
            if s:
                return s
    return ""


def _trade_event_kind(tr):
    typ = str((tr or {}).get("type") or "").upper().strip()
    side = str((tr or {}).get("side") or "").upper().strip()
    if typ == "REDEEM":
        return "REDEEM"
    if typ in ["DEPOSIT", "WITHDRAW", "WITHDRAWAL", "TRANSFER"]:
        return "IGNORE"
    if side in ["BUY", "SELL"]:
        return side
    return "IGNORE"


def _trade_ts_ms(tr):
    v = (tr or {}).get("matchtime") or (tr or {}).get("match_time") or (tr or {}).get("timestamp") or (tr or {}).get("created_at") or (tr or {}).get("time")
    if isinstance(v, (int, float)):
        n = float(v)
        return int(n if n > 1e12 else n * 1000)
    s = str(v or "").strip()
    if not s:
        return 0
    if s.isdigit():
        n = int(s)
        return n if n > 1e12 else n * 1000
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return int(dt.timestamp() * 1000)
    except Exception:
        return 0


def _trade_usdc_size(tr):
    usdc = _maybe_float((tr or {}).get("usdcSize") or (tr or {}).get("usdc_size"))
    if usdc is not None:
        return abs(usdc)
    price = _maybe_float((tr or {}).get("price"))
    size = _maybe_float((tr or {}).get("size_matched") or (tr or {}).get("size") or (tr or {}).get("original_size"))
    if price is not None and size is not None:
        return abs(price * size)
    return 0.0


def _trade_market_key(tr):
    cond = _trade_pick_field(tr, "conditionId", "condition_id", "market", "market_id")
    slug = _trade_pick_field(tr, "eventSlug", "slug")
    if cond:
        return cond
    if slug:
        return slug
    asset = _trade_pick_field(tr, "asset_id", "asset", "token_id")
    return asset or "market"


def _resolve_trade_reason(tr):
    title = _trade_pick_field(tr, "title", "eventTitle", "name", "question")
    if title:
        return title
    slug = _trade_pick_field(tr, "eventSlug", "slug")
    if slug:
        return slug
    return "市场"


def _fetch_trade_activity(user, limit=500):
    if not user:
        return []
    lim = min(max(int(limit), 50), 1000)
    param_sets = [
        {"user": user, "limit": lim, "offset": 0},
        {"user": user},
        {"address": user, "limit": lim, "offset": 0},
        {"wallet": user, "limit": lim, "offset": 0},
    ]

    rows = []
    seen = set()
    for params in param_sets:
        data = _data_api_get("/activity", params)
        if not isinstance(data, list):
            continue
        for item in data:
            if not isinstance(item, dict):
                continue
            kind = _trade_event_kind(item)
            if kind == "IGNORE":
                continue
            tid = _text_scalar(item.get("id") or item.get("tradeID") or item.get("transaction_hash") or item.get("transactionHash"))
            if not tid:
                tid = f"act-{kind}-{_trade_ts_ms(item)}-{_trade_usdc_size(item):.6f}-{_trade_market_key(item)}"
            if tid in seen:
                continue
            seen.add(tid)
            norm = dict(item)
            if norm.get("type") is not None:
                norm["type"] = str(norm.get("type")).upper()
            if norm.get("side") is not None:
                norm["side"] = str(norm.get("side")).upper()
            norm["id"] = tid
            rows.append(norm)
        if rows:
            break

    rows.sort(key=_trade_ts_ms)
    return rows


def _build_market_aggregated_trades(raw_trades):
    groups = {}
    for tr in sorted((raw_trades or []), key=_trade_ts_ms):
        if not isinstance(tr, dict):
            continue
        kind = _trade_event_kind(tr)
        if kind == "IGNORE":
            continue

        price = _maybe_float(tr.get("price"))
        size = _maybe_float(tr.get("size_matched") or tr.get("size") or tr.get("original_size"))
        usdc_size = _trade_usdc_size(tr)
        if kind in ["BUY", "SELL"] and (price is None or size is None or size <= 0):
            continue
        if kind == "REDEEM" and usdc_size <= 0:
            continue

        key = _trade_market_key(tr)
        ts = tr.get("matchtime") or tr.get("match_time") or tr.get("timestamp") or tr.get("created_at") or tr.get("time")
        ts_ms = _trade_ts_ms(tr)
        g = groups.get(key)
        if g is None:
            g = {
                "id": f"agg-{key}",
                "direction": _normalize_outcome_label(tr.get("outcome") or tr.get("direction")),
                "outcomes": set(),
                "reason": _resolve_trade_reason(tr),
                "buy_count": 0,
                "sell_count": 0,
                "redeem_count": 0,
                "buy_size": 0.0,
                "sell_size": 0.0,
                "buy_notional": 0.0,
                "sell_notional": 0.0,
                "redeem_notional": 0.0,
                "first_ts": ts,
                "last_ts": ts,
                "first_ts_ms": ts_ms,
                "last_ts_ms": ts_ms,
            }
            groups[key] = g

        if ts_ms and ts_ms < g["first_ts_ms"]:
            g["first_ts_ms"] = ts_ms
            g["first_ts"] = ts
        if ts_ms and ts_ms >= g["last_ts_ms"]:
            g["last_ts_ms"] = ts_ms
            g["last_ts"] = ts

        outcome = _normalize_outcome_label(tr.get("outcome") or tr.get("direction"))
        if outcome and outcome != "-":
            g["outcomes"].add(outcome)

        if kind == "BUY":
            g["buy_count"] += 1
            g["buy_size"] += float(size)
            g["buy_notional"] += float(usdc_size)
        elif kind == "SELL":
            g["sell_count"] += 1
            g["sell_size"] += float(size)
            g["sell_notional"] += float(usdc_size)
        elif kind == "REDEEM":
            g["redeem_count"] += 1
            g["redeem_notional"] += float(usdc_size)

    rows = []
    for g in groups.values():
        if (g["buy_count"] + g["sell_count"] + g["redeem_count"]) <= 0:
            continue
        buy_avg = (g["buy_notional"] / g["buy_size"]) if g["buy_size"] > 1e-9 else None
        sell_avg = (g["sell_notional"] / g["sell_size"]) if g["sell_size"] > 1e-9 else None
        matched_size = min(g["buy_size"], g["sell_size"])
        pnl = g["sell_notional"] + g["redeem_notional"] - g["buy_notional"]

        if len(g["outcomes"]) == 1:
            g["direction"] = list(g["outcomes"])[0]
        elif len(g["outcomes"]) > 1:
            g["direction"] = "MIX"

        result = "CLOSED" if (g["sell_count"] > 0 or g["redeem_count"] > 0) else "OPEN"
        rows.append({
            "id": g["id"],
            "pair_id": g["id"],
            "direction": g["direction"],
            "reason": g["reason"],
            "buy_count": g["buy_count"],
            "sell_count": g["sell_count"],
            "redeem_count": g["redeem_count"],
            "buy_usdc": g["buy_notional"],
            "sell_usdc": g["sell_notional"],
            "redeem_usdc": g["redeem_notional"],
            "size": matched_size if matched_size > 1e-9 else max(g["buy_size"], g["sell_size"]),
            "entry_price_quote": buy_avg,
            "exit_price_quote": sell_avg,
            "order_time": g["first_ts"],
            "settle_time": g["last_ts"],
            "profit": pnl,
            "result": result,
            "status": "AGG",
        })

    rows.sort(key=lambda x: _trade_ts_ms({"timestamp": x.get("settle_time")}) if isinstance(x, dict) else 0)
    return rows


def _compute_wallet_realized_pnl(rows):
    realized = 0.0
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        rp = _maybe_float(row.get("realizedPnl") if row.get("realizedPnl") is not None else row.get("realized_pnl"))
        if rp is not None:
            realized += rp
    return float(realized)


def _compute_wallet_unrealized_pnl(rows):
    unrealized = 0.0
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        mark = _maybe_float(row.get("curPrice") if row.get("curPrice") is not None else row.get("cur_price"))
        avg = _maybe_float(row.get("avgPrice") if row.get("avgPrice") is not None else row.get("avg_price"))
        size = _maybe_float(row.get("size"))
        if mark is None or avg is None or size is None:
            continue
        unrealized += (mark - avg) * size
    return float(unrealized)


def get_usdc_balance(address):
    """获取USDC余额"""
    if not address or not HAS_WEB3 or not POLYGON_RPC_URL:
        return None
    try:
        request_kwargs = {}
        if PROXIES:
             request_kwargs = {"proxies": PROXIES}
        
        w3 = Web3(Web3.HTTPProvider(POLYGON_RPC_URL, request_kwargs=request_kwargs))
        if not w3.is_connected():
            return None
        
        # 简单ABI
        abi = [{
            "constant": True,
            "inputs": [{"name": "_owner", "type": "address"}],
            "name": "balanceOf",
            "outputs": [{"name": "balance", "type": "uint256"}],
            "type": "function"
        }]
        contract = w3.eth.contract(address=Web3.to_checksum_address(USDC_E_CONTRACT), abi=abi)
        balance_wei = contract.functions.balanceOf(Web3.to_checksum_address(address)).call()
        return float(balance_wei) / 1e6
    except Exception:
        return None


def _sync_dashboard_account_snapshot(user):
    u = str(user or "").strip().lower()
    if not u:
        return False
    wallet_positions = _fetch_wallet_positions(u)
    wallet_closed = _fetch_wallet_closed_positions(u)
    wallet_history = _build_wallet_history_items(wallet_closed)
    raw_activity = _fetch_trade_activity(u, limit=500)
    agg_trades = _build_market_aggregated_trades(raw_activity)
    realized_pnl = _compute_wallet_realized_pnl(wallet_closed)
    unrealized_pnl = _compute_wallet_unrealized_pnl(wallet_positions)
    balance = get_usdc_balance(u)
    _dashboard_set(
        wallet_positions=list(wallet_positions)[:120],
        wallet_history=list(wallet_history)[:200],
        live_trades=list(agg_trades)[-300:],
        live_positions_count=len(wallet_positions),
        live_realized_pnl=float(realized_pnl),
        live_unrealized_pnl=float(unrealized_pnl),
        live_total_pnl=float(realized_pnl + unrealized_pnl),
        wallet_balance=balance,
    )
    return True


def _fetch_wallet_positions(user):
    if not user:
        return []
    try:
        r = requests.get(
            f"{DATA_API}/positions",
            params={"user": user, "sizeThreshold": 0},
            proxies=PROXIES if PROXIES else None,
            timeout=12,
        )
        if r.status_code == 200:
            rows = r.json()
            if isinstance(rows, list):
                out = []
                for row in rows:
                    if not isinstance(row, dict):
                        continue
                    size = _to_float(row.get("size"), 0)
                    if size <= 0:
                        continue
                    if _to_bool(row.get("redeemable")) or _to_bool(row.get("mergeable")):
                        continue
                    out.append(row)
                return out
    except Exception:
        pass
    return []


def _fetch_wallet_closed_positions(user):
    if not user:
        return []
    try:
        r = requests.get(
            f"{DATA_API}/closed-positions",
            params={
                "user": user,
                "limit": 200,
                "offset": 0,
                "sortBy": "TIMESTAMP",
                "sortDirection": "DESC",
            },
            proxies=PROXIES if PROXIES else None,
            timeout=12,
        )
        if r.status_code == 200:
            rows = r.json()
            if isinstance(rows, list):
                return rows
    except Exception:
        pass
    return []


def _build_wallet_history_items(rows):
    items = []
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        side = row.get("outcome") or row.get("side") or row.get("positionSide") or "-"
        item = {
            "time": row.get("endDate") or row.get("timestamp") or row.get("updatedAt") or "-",
            "slug": row.get("slug") or row.get("marketSlug") or row.get("question") or "-",
            "action": "CLOSE",
            "side": side,
            "price": row.get("avgPrice") if row.get("avgPrice") is not None else row.get("avg_price"),
            "amount": row.get("size"),
            "order_id": row.get("transactionHash") or row.get("id") or "",
            "status": "closed",
            "reason": "wallet_sync",
            "pnl": row.get("realizedPnl") if row.get("realizedPnl") is not None else row.get("realized_pnl"),
        }
        items.append(item)
    return items[:200]

def load_state():
    """加载交易状态"""
    if not os.path.exists(STATE_FILE):
        return _normalize_state({})
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return _normalize_state(json.load(f))
    except:
        return _normalize_state({})

def save_state(state):
    """保存交易状态"""
    try:
        state = _normalize_state(state)
        # 添加实时价格数据
        state["ptb"] = price_data.get("ptb")
        state["chainlink"] = price_data.get("btc")
        state["binance"] = price_data.get("binance")
        state["up_price"] = price_data.get("up_price")
        state["down_price"] = price_data.get("down_price")
        state["last_update"] = datetime.now().isoformat()
        
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2)
    except Exception as e:
        log(f"保存状态失败: {e}", "ERR")

# ============== WebSocket 价格监听 ==============
class BTCPriceListener:
    """监听币安BTC价格 (WebSocket)"""
    def __init__(self):
        self.ws = None
        self.running = False
    
    def on_message(self, ws, message):
        try:
            data = json.loads(message)
            if "p" in data:  # 价格字段
                price_data["btc"] = float(data["p"])
                price_data["last_update"] = time.time()
        except:
            pass
    
    def on_error(self, ws, error):
        log(f"BTC WS Error: {error}", "ERR")
    
    def on_close(self, ws, close_status_code, close_msg):
        if self.running:
            log(f"BTC价格连接断开 ({close_status_code}: {close_msg}), 5秒后重连...", "WARN")
            time.sleep(5)
            self.start()
    
    def on_open(self, ws):
        log("BTC价格WebSocket已连接", "OK")
    
    def start(self):
        self.running = True
        self.ws = websocket.WebSocketApp(
            BINANCE_WSS,
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close
        )
        proxy_host, proxy_port, proxy_auth = parse_proxy_url(HTTP_PROXY or HTTPS_PROXY)
        log(f"BTC WS 代理参数: host={proxy_host} port={proxy_port} auth={'***' if proxy_auth else 'None'}", "INFO")
        threading.Thread(target=self.ws.run_forever, kwargs={
            "http_proxy_host": proxy_host, 
            "http_proxy_port": proxy_port, 
            "http_proxy_auth": proxy_auth,
            "proxy_type": "http"
        }, daemon=True).start()
    
    def stop(self):
        self.running = False
        if self.ws:
            self.ws.close()

class ChainlinkPriceListener:
    """监听Chainlink BTC价格 (WebSocket)"""
    def __init__(self):
        self.ws = None
        self.running = False

    def on_message(self, ws, message):
        try:
            data = json.loads(message)
            if "data" in data and "value" in data["data"]:
                # Chainlink价格通常是带小数点的字符串，需要转换为float
                price_data["btc"] = float(data["data"]["value"]) / 1e8 # Chainlink价格通常是8位小数
                price_data["last_update"] = time.time()
        except:
            pass

    def on_error(self, ws, error):
        log(f"Chainlink WS Error: {error}", "ERR")

    def on_close(self, ws, close_status_code, close_msg):
        if self.running:
            log(f"Chainlink价格连接断开 ({close_status_code}: {close_msg}), 5秒后重连...", "WARN")
            time.sleep(5)
            self.start()

    def on_open(self, ws):
        log("Chainlink价格WebSocket已连接", "OK")

    def start(self):
        self.running = True
        self.ws = websocket.WebSocketApp(
            CHAINLINK_WSS,
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close
        )
        proxy_host, proxy_port, proxy_auth = parse_proxy_url(HTTP_PROXY or HTTPS_PROXY)
        log(f"Chainlink WS 代理参数: host={proxy_host} port={proxy_port} auth={'***' if proxy_auth else 'None'}", "INFO")
        threading.Thread(target=self.ws.run_forever, kwargs={
            "http_proxy_host": proxy_host,
            "http_proxy_port": proxy_port,
            "http_proxy_auth": proxy_auth,
            "proxy_type": "http"
        }, daemon=True).start()

    def stop(self):
        self.running = False
        if self.ws:
            self.ws.close()

class ChainlinkPriceListener:
    """监听Chainlink BTC价格 (WebSocket) - 持久连接"""
    def __init__(self):
        self.ws = None
        self.running = False
    
    def on_message(self, ws, message):
        try:
            data = json.loads(message)
            if data.get("topic") == "crypto_prices" and data.get("payload"):
                payload = data["payload"]
                if "data" in payload and payload.get("symbol") == "btc/usd":
                    prices = payload["data"]
                    if prices:
                        price = float(prices[-1]["value"])
                        price_data["btc"] = price
                        price_data["last_update"] = time.time()
                elif "value" in payload:
                    price = float(payload["value"])
                    price_data["btc"] = price
                    price_data["last_update"] = time.time()
        except:
            pass

    def on_error(self, ws, error):
        log(f"Chainlink WS Error: {error}", "ERR")
    
    def on_close(self, ws, close_status_code, close_msg):
        if self.running:
            log(f"Chainlink连接断开 ({close_status_code}: {close_msg}), 5秒后重连...", "WARN")
            time.sleep(5)
            self.start()

    def on_open(self, ws):
        sub_msg = {
            "action": "subscribe",
            "subscriptions": [{
                "topic": "crypto_prices_chainlink",
                "type": "*",
                "filters": "{\"symbol\":\"btc/usd\"}"
            }]
        }
        ws.send(json.dumps(sub_msg))
        log("Chainlink WebSocket已连接", "OK")

    def start(self):
        self.running = True
        self.ws = websocket.WebSocketApp(
            RTDS_WS,
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close
        )
        proxy_host, proxy_port, proxy_auth = parse_proxy_url(HTTP_PROXY or HTTPS_PROXY)
        log(f"Chainlink WS 代理参数: host={proxy_host} port={proxy_port} auth={'***' if proxy_auth else 'None'}", "INFO")
        threading.Thread(target=self.ws.run_forever, kwargs={
            "http_proxy_host": proxy_host, 
            "http_proxy_port": proxy_port, 
            "http_proxy_auth": proxy_auth,
            "proxy_type": "http"
        }, daemon=True).start()

    def stop(self):
        self.running = False
        if self.ws:
            self.ws.close()

class MarketPriceListener:
    """监听市场UP/DOWN价格 (WebSocket)"""
    def __init__(self, up_token, down_token):
        self.up_token = up_token
        self.down_token = down_token
        self.ws = None
        self.running = False
    
    def on_message(self, ws, message):
        try:
            data = json.loads(message)
            
            # 处理列表或字典
            items = data if isinstance(data, list) else [data]
            
            for item in items:
                if not isinstance(item, dict):
                    continue
                
                event_type = item.get("event_type")
                asset_id = item.get("asset_id")
                
                # 处理订单簿数据
                if event_type == "book":
                    bids = item.get("bids") or []
                    asks = item.get("asks") or []
                    
                    if bids and asks:
                        best_bid = max([float(b["price"]) for b in bids], default=0)
                        best_ask = min([float(a["price"]) for a in asks], default=0)
                        mid_price = (best_bid + best_ask) / 2
                        
                        if asset_id == self.up_token:
                            price_data["up_price"] = mid_price
                            price_data["up_best_bid"] = best_bid
                            price_data["up_best_ask"] = best_ask
                        elif asset_id == self.down_token:
                            price_data["down_price"] = mid_price
                            price_data["down_best_bid"] = best_bid
                            price_data["down_best_ask"] = best_ask
                
                # 处理价格变化数据
                elif event_type == "price_change":
                    price_changes = item.get("price_changes", [])
                    if price_changes:
                        pc = price_changes[0]
                        best_bid = float(pc.get("best_bid", 0))
                        best_ask = float(pc.get("best_ask", 0))
                        
                        if best_bid > 0 and best_ask > 0:
                            mid_price = (best_bid + best_ask) / 2
                            
                            if asset_id == self.up_token:
                                price_data["up_price"] = mid_price
                                price_data["up_best_bid"] = best_bid
                                price_data["up_best_ask"] = best_ask
                            elif asset_id == self.down_token:
                                price_data["down_price"] = mid_price
                                price_data["down_best_bid"] = best_bid
                                price_data["down_best_ask"] = best_ask
        except:
            pass
    
    def on_error(self, ws, error):
        log(f"Market WS Error: {error}", "ERR")
    
    def on_close(self, ws, close_status_code, close_msg):
        if self.running:
            log(f"市场价格连接断开 ({close_status_code}: {close_msg}), 5秒后重连...", "WARN")
            time.sleep(5)
            self.start()
    
    def on_open(self, ws):
        # 订阅UP和DOWN的市场数据
        ws.send(json.dumps({
            "assets_ids": [self.up_token, self.down_token],
            "type": "market"
        }))
        log("市场价格WebSocket已连接", "OK")
    
    def start(self):
        self.running = True
        self.ws = websocket.WebSocketApp(
            POLYMARKET_WSS,
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close
        )
        proxy_host, proxy_port, proxy_auth = parse_proxy_url(HTTP_PROXY or HTTPS_PROXY)
        log(f"Market WS 代理参数: host={proxy_host} port={proxy_port} auth={'***' if proxy_auth else 'None'}", "INFO")
        threading.Thread(target=self.ws.run_forever, kwargs={
            "http_proxy_host": proxy_host, 
            "http_proxy_port": proxy_port, 
            "http_proxy_auth": proxy_auth,
            "proxy_type": "http"
        }, daemon=True).start()
    
    def stop(self):
        self.running = False
        if self.ws:
            self.ws.close()

# ============== 交易客户端 ==============
class Trader:
    def __init__(self):
        self.client = None
        self.connected = False
        self.address = None
    
    def connect(self):
        """连接交易客户端"""
        pk = os.getenv("PRIVATE_KEY")
        if not pk:
            log("未配置PRIVATE_KEY", "ERR")
            return False
        
        try:
            if not pk.startswith("0x"):
                pk = "0x" + pk
            
            log("连接交易客户端...")
            temp = ClobClient(host="https://clob.polymarket.com", chain_id=137, key=pk)
            self.address = temp.get_address()
            log(f"钱包: {self.address}")
            
            creds = temp.create_or_derive_api_creds()
            funder = os.getenv("FUNDER_ADDRESS") or self.address
            sig_type = int(os.getenv("SIGNATURE_TYPE", "2"))
            
            self.client = ClobClient(
                host="https://clob.polymarket.com",
                chain_id=137,
                key=pk,
                creds=creds,
                signature_type=sig_type,
                funder=funder
            )
            self.connected = True
            log("交易客户端已连接", "OK")
            return True
        except Exception as e:
            log(f"连接失败: {e}", "ERR")
            return False
    
    def place_order(self, token_id, side, price, size):
        """下单"""
        if not self.connected:
            log("未连接交易客户端", "ERR")
            return None
        
        try:
            log(f"下单: {side} ${size} @ {price:.3f}", "TRADE")
            
            order_args = OrderArgs(
                token_id=token_id,
                price=price,
                size=size,
                side=BUY if side == "BUY" else SELL
            )
            
            signed_order = self.client.create_order(order_args)
            resp = self.client.post_order(signed_order)
            
            if resp and resp.get("orderID"):
                order_id = resp.get("orderID")
                log(f"下单成功! 订单ID: {order_id}", "OK")
                return order_id
            else:
                log("下单失败", "ERR")
                return None
        except Exception as e:
            log(f"下单异常: {e}", "ERR")
            return None
    
    def get_order_status(self, order_id):
        """获取订单状态"""
        if not self.connected or not order_id:
            return None
        
        try:
            order = self.client.get_order(order_id)
            if order:
                status = order.get("status", "").upper()
                original_size = float(order.get("original_size", 0) or 0)
                size_matched = float(order.get("size_matched", 0) or 0)
                
                return {
                    "status": status,
                    "original_size": original_size,
                    "size_matched": size_matched,
                    "filled": size_matched >= original_size if original_size > 0 else False
                }
        except Exception as e:
            log(f"获取订单状态失败: {e}", "WARN")
        return None
    
    def cancel_order(self, order_id):
        """撤销订单"""
        if not self.connected or not order_id:
            return False
        
        try:
            log(f"撤销订单: {order_id}", "WARN")
            resp = self.client.cancel(order_id)
            if resp:
                log("订单已撤销", "OK")
                return True
            else:
                log("撤销失败", "ERR")
                return False
        except Exception as e:
            log(f"撤销异常: {e}", "ERR")
            return False

class AutoRedeemer:
    def __init__(self, private_key, funder_address):
        self.enabled = bool(AUTO_REDEEM)
        self.private_key = (private_key or "").strip()
        if self.private_key and not self.private_key.startswith("0x"):
            self.private_key = "0x" + self.private_key
        self.funder_address = (funder_address or "").strip()
        self.scan_addresses = []
        self.last_try_by_condition = {}
        self.last_pending_signature = ""
        self.last_pending_log_ts = 0.0
        self.running = False
        self.thread = None
        self.relayer_client = None
        self.relayer_error = ""
        self.last_pending_count = 0
        self.last_claimable_count = 0
        self.last_result = {}
        self.last_error = ""

        if not self.enabled:
            _dashboard_set(auto_redeem={"enabled": False, "pending_count": 0, "claimable_count": 0, "last_result": {}, "last_error": ""})
            return
        if not HAS_WEB3:
            log("自动领取已禁用: 缺少web3依赖", "WARN", force=True)
            self.enabled = False
            _dashboard_set(auto_redeem={"enabled": False, "pending_count": 0, "claimable_count": 0, "last_result": {}, "last_error": "缺少web3依赖"})
            return
        if not self.private_key:
            log("自动领取已禁用: 缺少PRIVATE_KEY", "WARN", force=True)
            self.enabled = False
            _dashboard_set(auto_redeem={"enabled": False, "pending_count": 0, "claimable_count": 0, "last_result": {}, "last_error": "缺少PRIVATE_KEY"})
            return
        if not self.funder_address:
            log("自动领取已禁用: 缺少FUNDER_ADDRESS(代理钱包)", "WARN", force=True)
            self.enabled = False
            _dashboard_set(auto_redeem={"enabled": False, "pending_count": 0, "claimable_count": 0, "last_result": {}, "last_error": "缺少FUNDER_ADDRESS"})
            return
        if not (POLY_BUILDER_API_KEY and POLY_BUILDER_SECRET and POLY_BUILDER_PASSPHRASE):
            log("自动领取已禁用: 缺少POLY_BUILDER_API_KEY/SECRET/PASSPHRASE", "WARN", force=True)
            self.enabled = False
            _dashboard_set(auto_redeem={"enabled": False, "pending_count": 0, "claimable_count": 0, "last_result": {}, "last_error": "缺少Builder凭据"})
            return

        self.scan_addresses = [self.funder_address]

        client, err = self._create_relayer_client()
        if client is None:
            log(f"自动领取已禁用: Relayer初始化失败 {err}", "ERR", force=True)
            self.enabled = False
            _dashboard_set(auto_redeem={"enabled": False, "pending_count": 0, "claimable_count": 0, "last_result": {}, "last_error": str(err)})
            return
        self.relayer_client = client

    def _normalize_condition_id(self, value):
        s = str(value or "").strip().lower()
        if not s:
            return ""
        if s.startswith("0x"):
            s = s[2:]
        if len(s) != 64:
            return ""
        try:
            int(s, 16)
        except Exception:
            return ""
        return "0x" + s

    def _fetch_positions(self, user):
        try:
            r = requests.get(
                f"{DATA_API}/positions",
                params={"user": user, "sizeThreshold": 0},
                proxies=PROXIES if PROXIES else None,
                timeout=12,
            )
            if r.status_code == 200:
                data = r.json()
                if isinstance(data, list):
                    return data
        except Exception:
            pass
        return []

    def _create_relayer_client(self):
        try:
            import inspect
            import py_builder_relayer_client.client as rel_mod
            from py_builder_relayer_client.client import RelayClient
            try:
                from py_builder_signing_sdk import BuilderConfig, BuilderApiKeyCreds
            except Exception:
                from py_builder_signing_sdk.config import BuilderConfig, BuilderApiKeyCreds

            cfg = BuilderConfig(
                local_builder_creds=BuilderApiKeyCreds(
                    key=POLY_BUILDER_API_KEY,
                    secret=POLY_BUILDER_SECRET,
                    passphrase=POLY_BUILDER_PASSPHRASE,
                )
            )

            args = [RELAYER_URL, 137, self.private_key, cfg]
            init_params = inspect.signature(RelayClient.__init__).parameters
            if len(init_params) >= 6:
                tx_enum = getattr(rel_mod, "RelayerTxType", None) or getattr(rel_mod, "TransactionType", None)
                tx_value = None
                if tx_enum is not None:
                    if RELAYER_TX_TYPE == "PROXY" and hasattr(tx_enum, "PROXY"):
                        tx_value = getattr(tx_enum, "PROXY")
                    elif hasattr(tx_enum, "SAFE"):
                        tx_value = getattr(tx_enum, "SAFE")
                    elif hasattr(tx_enum, "SAFE_CREATE"):
                        tx_value = getattr(tx_enum, "SAFE_CREATE")
                if tx_value is not None:
                    args.append(tx_value)

            return RelayClient(*args), ""
        except Exception as e:
            return None, str(e)

    def _collect_redeemable(self):
        pending = []
        seen = set()
        claimable = []

        for owner in self.scan_addresses:
            rows = self._fetch_positions(owner)
            owner_l = owner.lower()
            for row in rows:
                if not isinstance(row, dict):
                    continue
                size = row.get("size")
                try:
                    size_f = float(size or 0)
                except Exception:
                    size_f = 0.0
                if size_f <= 0:
                    continue

                redeemable = bool(row.get("redeemable") or row.get("mergeable"))
                if not redeemable:
                    continue

                cid = self._normalize_condition_id(
                    row.get("conditionId") or row.get("condition_id")
                )
                if not cid:
                    continue

                key = owner_l + "|" + cid
                if key in seen:
                    continue
                seen.add(key)
                pending.append({"owner": owner, "condition_id": cid})

                if owner_l == self.funder_address.lower() and cid not in claimable:
                    claimable.append(cid)

        return pending, claimable

    def _redeem_condition(self, condition_id):
        try:
            from py_builder_relayer_client.models import SafeTransaction, OperationType

            request_kwargs = {}
            if PROXIES:
                request_kwargs = {"proxies": PROXIES}
            
            ctf_addr = Web3.to_checksum_address(CTF_CONTRACT)
            usdc_addr = Web3.to_checksum_address(USDC_E_CONTRACT)
            contract = Web3(Web3.HTTPProvider(POLYGON_RPC_URL, request_kwargs=request_kwargs)).eth.contract(
                address=ctf_addr,
                abi=[{
                    "name": "redeemPositions",
                    "type": "function",
                    "stateMutability": "nonpayable",
                    "inputs": [
                        {"name": "collateralToken", "type": "address"},
                        {"name": "parentCollectionId", "type": "bytes32"},
                        {"name": "conditionId", "type": "bytes32"},
                        {"name": "indexSets", "type": "uint256[]"},
                    ],
                    "outputs": [],
                }],
            )
            cond_bytes = bytes.fromhex(condition_id[2:])
            data = contract.encode_abi(
                abi_element_identifier="redeemPositions",
                args=[usdc_addr, b"\x00" * 32, cond_bytes, [1, 2]],
            )
            op_call = getattr(OperationType, "Call", None)
            if op_call is None:
                op_call = list(OperationType)[0]
            tx = SafeTransaction(to=str(ctf_addr), operation=op_call, data=str(data), value="0")

            def execute_once():
                resp = self.relayer_client.execute([tx], f"Redeem {condition_id}")
                result = resp.wait()
                txh = str(getattr(resp, "transaction_hash", "") or "")
                state = ""
                if isinstance(result, dict):
                    txh = str(result.get("transaction_hash") or result.get("transactionHash") or txh)
                    state = str(result.get("state") or "")
                else:
                    txh = str(getattr(result, "transaction_hash", "") or getattr(result, "transactionHash", "") or txh)
                    state = str(getattr(result, "state", "") or "")
                if result is None:
                    return False, txh, "relayer_not_confirmed"
                if state and state not in ["STATE_CONFIRMED", "STATE_MINED", "STATE_EXECUTED"]:
                    return False, txh, f"state={state}"
                return True, txh, ""

            try:
                return execute_once()
            except Exception as e:
                msg = str(e)
                low = msg.lower()
                if "expected safe" in low and "not deployed" in low:
                    dep = self.relayer_client.deploy()
                    dep.wait()
                    return execute_once()
                return False, "", msg
        except Exception as e:
            return False, "", str(e)

    def scan_once(self):
        if not self.enabled:
            return

        pending, claimable = self._collect_redeemable()
        now = time.time()
        self.last_pending_count = len(pending)
        self.last_claimable_count = len(claimable)
        _dashboard_set(auto_redeem={
            "enabled": self.enabled,
            "pending_count": self.last_pending_count,
            "claimable_count": self.last_claimable_count,
            "last_result": dict(self.last_result or {}),
            "last_error": self.last_error,
            "scan_interval": REDEEM_SCAN_INTERVAL,
        })

        if pending:
            signature = "|".join([f"{x['owner']}:{x['condition_id']}" for x in pending])
            if signature != self.last_pending_signature or (now - self.last_pending_log_ts) >= REDEEM_PENDING_LOG_INTERVAL:
                self.last_pending_signature = signature
                self.last_pending_log_ts = now
                owners = sorted(list({x["owner"] for x in pending}))
                owner_text = ", ".join(owners[:3])
                if len(owners) > 3:
                    owner_text += f" 等{len(owners)}个地址"
                log(f"检测到可领取未领取 {len(pending)} 条, 代理自动领取 {len(claimable)} 条, 地址: {owner_text}", "WARN", force=True)

        if not claimable:
            return

        processed = 0
        for cid in claimable:
            t0 = self.last_try_by_condition.get(cid, 0)
            if now - t0 < REDEEM_RETRY_INTERVAL:
                continue
            self.last_try_by_condition[cid] = now

            ok, tx_hash, err = self._redeem_condition(cid)
            if ok:
                log(f"代理钱包自动领取成功: {cid} | tx {tx_hash}", "TRADE", force=True)
                self.last_error = ""
                self.last_result = {
                    "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "ok": True,
                    "condition_id": cid,
                    "tx": tx_hash,
                    "message": "ok",
                }
            else:
                log(f"代理钱包自动领取失败: {cid} | {err}", "ERR", force=True)
                self.last_error = str(err)
                self.last_result = {
                    "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "ok": False,
                    "condition_id": cid,
                    "tx": tx_hash,
                    "message": str(err),
                }

            _dashboard_set(auto_redeem={
                "enabled": self.enabled,
                "pending_count": self.last_pending_count,
                "claimable_count": self.last_claimable_count,
                "last_result": dict(self.last_result or {}),
                "last_error": self.last_error,
                "scan_interval": REDEEM_SCAN_INTERVAL,
            })
            _sync_dashboard_account_snapshot(self.funder_address)

            processed += 1
            if processed >= REDEEM_MAX_PER_SCAN:
                break

    def _loop(self):
        while self.running:
            try:
                self.scan_once()
            except Exception as e:
                log(f"自动领取扫描异常: {e}", "ERR", force=True)
            for _ in range(REDEEM_SCAN_INTERVAL):
                if not self.running:
                    break
                time.sleep(1)

    def start(self):
        if not self.enabled:
            return
        if self.running:
            return
        self.running = True
        self.thread = threading.Thread(target=self._loop, daemon=True)
        self.thread.start()
        log(f"代理钱包自动领取已开启: 每{REDEEM_SCAN_INTERVAL}s扫描", "OK", force=True)
        _dashboard_set(auto_redeem={
            "enabled": self.enabled,
            "pending_count": self.last_pending_count,
            "claimable_count": self.last_claimable_count,
            "last_result": dict(self.last_result or {}),
            "last_error": self.last_error,
            "scan_interval": REDEEM_SCAN_INTERVAL,
        })

    def stop(self):
        self.running = False

# ============== 主循环 ==============
def main():
    start_web_server()
    if WEB_ENABLED:
        log(f"前端面板已启动: http://{WEB_HOST}:{WEB_PORT}", "OK", force=True)

    print("\n" + "="*60)
    print("  ₿ Polymarket BTC 15分钟自动交易脚本")
    print("="*60)
    print(f"  自动下单: {'开启' if AUTO_TRADE else '关闭'}")
    print(f"  自动领取: {'开启' if AUTO_REDEEM else '关闭'}")
    print(f"  下单金额: ${TRADE_AMOUNT}")
    print(f"  条件1: 剩余≤{C1_TIME}秒 且 价差≥${C1_DIFF}")
    print(f"  条件2: 剩余≤{C2_TIME}秒 且 价差≥${C2_DIFF}")
    print(f"  条件3: 剩余≤{C3_TIME}秒 且 价差≥${C3_DIFF}")
    print(f"  止损线: 价差<${STOP_LOSS_DIFF}")
    print("="*60 + "\n")
    
    check_proxy_ip()
    
    trader = Trader()
    redeemer = AutoRedeemer(os.getenv("PRIVATE_KEY"), os.getenv("FUNDER_ADDRESS"))
    if AUTO_TRADE:
        if not trader.connect():
            log("无法连接交易客户端,退出", "ERR", force=True)
            return
    redeemer.start()

    init_state = load_state()
    _dashboard_set(
        position=dict(init_state.get("position") or {}),
        pending_order=dict(init_state.get("pending_order") or {}),
        last_order=dict(init_state.get("last_order") or {}),
        trade_history=list(init_state.get("trade_history") or []),
        wallet_positions=[],
        wallet_history=[],
        live_trades=[],
        live_positions_count=0,
        live_realized_pnl=0.0,
        live_unrealized_pnl=0.0,
        live_total_pnl=0.0,
    )
    


    log("启动价格监听...", "INFO", force=True)
    
    btc_listener = BTCPriceListener()
    btc_listener.start()
    
    chainlink_listener = ChainlinkPriceListener()
    chainlink_listener.start()
    
    last_slug = None
    market_listener = None
    first_display = True
    last_chainlink_update = 0
    last_account_sync = 0.0
    last_market_fetch = 0.0
    market_data_cache = None
    last_binance_fetch = 0.0
    
    dashboard_user = (os.getenv("FUNDER_ADDRESS", "") or "").strip().lower()
    if not dashboard_user:
        dashboard_user = (os.getenv("PRIVATE_KEY_ADDRESS", "") or "").strip().lower()
    if AUTO_TRADE and trader.address:
        dashboard_user = ((os.getenv("FUNDER_ADDRESS", "") or trader.address) or "").strip().lower()
    
    try:
        while True:
            # 每0.1秒循环一次
            now = time.time()
            
            # 定期更新市场元数据 (每5秒或没有缓存时)
            if not market_data_cache or now - last_market_fetch >= 5:
                market_data_cache = get_active_market()
                last_market_fetch = now
                if not market_data_cache:
                    # 如果没有活跃市场，可以稍微sleep多一点避免死循环频繁请求吗？
                    # 但是get_active_market本身是阻塞的，所以这里还好。
                    pass

            market = None
            if market_data_cache:
                 # 本地更新剩余时间
                 end_ts = datetime.fromisoformat(market_data_cache["end"].replace("Z", "+00:00")).timestamp()
                 remaining = int(end_ts - now)
                 if remaining <= 0:
                     # 市场已结束，立即强制刷新
                     last_market_fetch = 0
                     market_data_cache = None
                 else:
                     market = market_data_cache.copy()
                     market["remaining"] = remaining
            
            # 获取币安价格(仅参考), 每2秒更新一次, 避免阻塞
            if now - last_binance_fetch > 2:
                binance_price = get_binance_btc_price()
                if binance_price:
                    price_data["binance"] = binance_price
                last_binance_fetch = now

            if now - last_account_sync >= DASHBOARD_ACCOUNT_SYNC_SEC:
                _sync_dashboard_account_snapshot(dashboard_user)
                last_account_sync = now

            if not market:
                state_snapshot = load_state()
                _dashboard_set(
                    market={"slug": "", "remaining": 0, "status": "waiting"},
                    prices={
                        "ptb": price_data.get("ptb"),
                        "chainlink_btc": price_data.get("btc"),
                        "binance_btc": price_data.get("binance"),
                        "up_price": price_data.get("up_price"),
                        "down_price": price_data.get("down_price"),
                        "diff": None,
                        "diff_abs": None,
                    },
                    position=dict(state_snapshot.get("position") or {}),
                    pending_order=dict(state_snapshot.get("pending_order") or {}),
                    last_order=dict(state_snapshot.get("last_order") or {}),
                    trade_history=list(state_snapshot.get("trade_history") or []),
                )
                if first_display:
                    print("\n⏳ 等待活跃市场...")
                    if price_data["btc"]:
                        print(f"当前BTC价格(Chainlink): ${price_data['btc']:,.2f}")
                if first_display:
                    print("\n⏳ 等待活跃市场...")
                    if price_data["btc"]:
                        print(f"当前BTC价格(Chainlink): ${price_data['btc']:,.2f}")
                time.sleep(0.5)
                continue
            
            slug = market["slug"]
            remaining = market["remaining"]
            
            # 检测市场切换
            if last_slug and slug != last_slug:
                # 停止旧的市场监听
                if market_listener:
                    market_listener.stop()
                
                # 清除状态
                state = load_state()
                state.pop("position", None)
                state.pop("last_order", None)
                save_state(state)
                
                # 启动新的市场监听
                market_listener = MarketPriceListener(market["up_token"], market["down_token"])
                market_listener.start()
                
                # 清空PTB缓存
                price_data["ptb"] = None
                
                # 标记需要重新显示
                first_display = True
                
                # 等待获取市场价格
                time.sleep(2)
            
            elif not last_slug:
                # 首次启动市场监听
                market_listener = MarketPriceListener(market["up_token"], market["down_token"])
                market_listener.start()
                time.sleep(2)
            
            last_slug = slug
            
            # 获取PTB (使用crypto-price API)
            if not price_data["ptb"]:
                crypto_data = get_crypto_price_api(market["start"], market["end"])
                if crypto_data.get("openPrice"):
                    price_data["ptb"] = crypto_data["openPrice"]
            
            # 从WebSocket获取的实时数据
            btc = price_data["btc"] or 0  # 如果Chainlink获取失败,使用0
            ptb = price_data["ptb"] or 0
            up_price = price_data["up_price"] or market["up_price"]
            down_price = price_data["down_price"] or market["down_price"]
            
            # 计算价差
            diff = btc - ptb if (btc > 0 and ptb > 0) else 0
            diff_abs = abs(diff)
            _dashboard_set(
                market={
                    "slug": slug,
                    "remaining": remaining,
                    "remaining_text": f"{remaining//60}分{remaining%60}秒",
                    "start": market.get("start"),
                    "end": market.get("end"),
                    "status": "active",
                },
                prices={
                    "ptb": ptb if ptb > 0 else None,
                    "chainlink_btc": btc if btc > 0 else None,
                    "binance_btc": (price_data.get("binance") or None),
                    "up_price": up_price,
                    "down_price": down_price,
                    "diff": diff if (btc > 0 and ptb > 0) else None,
                    "diff_abs": diff_abs if (btc > 0 and ptb > 0) else None,
                    "updated_ts": time.time(),
                },
            )

            state_snapshot = load_state()
            _dashboard_set(
                position=dict(state_snapshot.get("position") or {}),
                pending_order=dict(state_snapshot.get("pending_order") or {}),
                last_order=dict(state_snapshot.get("last_order") or {}),
                trade_history=list(state_snapshot.get("trade_history") or []),
            )
            
            # 首次显示完整界面
            if first_display:
                print("\n" + "="*90)
                print(f"📊 市场: {slug}")
                print(f"⏱️  剩余时间: {remaining//60}分{remaining%60}秒")
                print()
                print("┌────────────────────────┬────────────────────────┬────────────────────────┐")
                print("│ 标定价 (PTB)           │ Chainlink 现价 (依据)  │ 币安现价 (参考)        │")
                ptb_display = f"${ptb:,.2f}" if ptb > 0 else "获取中..."
                btc_display = f"${btc:,.2f}" if btc > 0 else "获取中..."
                binance = price_data.get("binance") or 0
                binance_display = f"${binance:,.2f}" if binance > 0 else "获取中..."
                print(f"│ {ptb_display:22s} │ {btc_display:22s} │ {binance_display:22s} │")
                print("├────────────────────────┴────────────────────────┴────────────────────────┤")
                print("│ 市场现价                                                                 │")
                print(f"│ UP: {up_price*100:.2f}%  DOWN: {down_price*100:.2f}%                                                │")
                print("├──────────────────────────────────────────────────────────────────────────┤")
                print("│ 实时价差 (Chainlink - PTB)                                               │")
                if btc > 0 and ptb > 0:
                    diff_display = f"{diff:+.0f} USD"
                else:
                    diff_display = "等待价格数据..."
                print(f"│ {diff_display:72s} │")
                print("└──────────────────────────────────────────────────────────────────────────┘")
                print()
                print("="*90)
                print("实时日志:")
                print("="*90)
                first_display = False
            
            # 后续只更新状态行 (每0.5秒刷新一次print以避免闪烁，但dashboard状态是实时的)
            ptb_str = f"${ptb:,.0f}" if ptb > 0 else "获取中"
            btc_str = f"${btc:,.0f}" if btc > 0 else "获取中"
            binance = price_data.get("binance") or 0
            binance_str = f"${binance:,.0f}" if binance > 0 else "N/A"
            diff_str = f"{diff:+.0f}" if (btc > 0 and ptb > 0) else "N/A"
            
            # 使用\r覆盖当前行
            status = f"[{datetime.now().strftime('%H:%M:%S')}] 剩余:{remaining//60:02d}分{remaining%60:02d}秒 | Chainlink:{btc_str} | 币安:{binance_str} | PTB:{ptb_str} | 价差:{diff_str} | UP:{up_price*100:.1f}% DOWN:{down_price*100:.1f}%"
            print(f"\r{status}" + " "*10, end="", flush=True)
            
            # 检查触发条件
            triggered = False
            condition = None
            side = None
            price = None
            token = None
            
            if remaining <= C1_TIME and diff_abs >= C1_DIFF:
                triggered = True
                condition = f"条件1: 剩余≤{C1_TIME}s 且 价差≥${C1_DIFF}"
            elif remaining <= C2_TIME and diff_abs >= C2_DIFF:
                triggered = True
                condition = f"条件2: 剩余≤{C2_TIME}s 且 价差≥${C2_DIFF}"
            elif remaining <= C3_TIME and diff_abs >= C3_DIFF:
                triggered = True
                condition = f"条件3: 剩余≤{C3_TIME}s 且 价差≥${C3_DIFF}"
            
            if triggered:
                side = "UP" if diff > 0 else "DOWN"
                # 使用 best_ask 价格下单 (taker order, 立即成交)
                # 如果没有 best_ask 数据，回退到 mid_price
                if side == "UP":
                    price = price_data.get("up_best_ask") or up_price
                else:
                    price = price_data.get("down_best_ask") or down_price
                token = market["up_token"] if side == "UP" else market["down_token"]
                log(f"下单价格: best_ask={price:.4f} (mid={up_price if side=='UP' else down_price:.4f})", "TRADE")
                
                # 检查是否已下单
                state = load_state()
                last_order = state.get("last_order", {})
                order_key = f"{slug}|{side}"
                
                # 检查是否有未完成的订单需要监控
                pending_order = state.get("pending_order")
                _dashboard_set(
                    position=dict(state.get("position") or {}),
                    pending_order=dict(pending_order or {}),
                    last_order=dict(last_order or {}),
                )
                if pending_order:
                    order_id = pending_order.get("order_id")
                    order_time = pending_order.get("time")
                    
                    # 检查订单是否超过10秒
                    if order_time:
                        elapsed = (datetime.now() - datetime.fromisoformat(order_time)).total_seconds()
                        if elapsed > 10:
                            # 检查订单状态
                            order_status = trader.get_order_status(order_id)
                            if order_status and not order_status.get("filled"):
                                # 订单未成交,撤销并重试
                                log(f"订单超时未成交,撤销重试 (订单ID: {order_id})", "TRADE")
                                trader.cancel_order(order_id)
                                state.pop("pending_order", None)
                                save_state(state)
                                _dashboard_set(
                                    position=dict(state.get("position") or {}),
                                    pending_order={},
                                    last_order=dict(state.get("last_order") or {}),
                                )
                            elif order_status and order_status.get("filled"):
                                # 订单已成交
                                filled_side = pending_order.get("side") or side
                                filled_price = float(pending_order.get("price") or price or 0)
                                filled_slug = pending_order.get("slug") or slug
                                log(f"订单已成交! {filled_side} @ {filled_price*100:.2f}% (市场: {filled_slug})", "TRADE")
                                state.pop("pending_order", None)
                                state["position"] = {
                                    "slug": filled_slug,
                                    "side": filled_side,
                                    "entry_price": filled_price,
                                    "entry_diff": diff_abs
                                }
                                state = _append_trade_history(state, {
                                    "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                    "slug": filled_slug,
                                    "action": "BUY",
                                    "side": filled_side,
                                    "price": filled_price,
                                    "amount": TRADE_AMOUNT,
                                    "order_id": order_id,
                                    "status": "filled",
                                    "reason": "pending_filled",
                                    "diff": diff,
                                })
                                save_state(state)
                                _dashboard_set(
                                    position=dict(state.get("position") or {}),
                                    pending_order={},
                                    last_order=dict(state.get("last_order") or {}),
                                    trade_history=list(state.get("trade_history") or []),
                                )
                                _sync_dashboard_account_snapshot(dashboard_user)
                
                # 如果没有pending订单且未记录过此订单,则下单
                if not pending_order and last_order.get("key") != order_key:
                    log(f"触发条件: {condition} → {side} @ {price*100:.1f}%", "TRADE")
                    
                    if AUTO_TRADE and trader.connected:
                        order_id = trader.place_order(token, "BUY", price, TRADE_AMOUNT)
                        
                        if order_id:
                            # 记录pending订单,开始监控
                            state["pending_order"] = {
                                "order_id": order_id,
                                "time": datetime.now().isoformat(),
                                "slug": slug,
                                "side": side,
                                "price": price
                            }
                            state["last_order"] = {"key": order_key, "time": datetime.now().isoformat()}
                            state = _append_trade_history(state, {
                                "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                "slug": slug,
                                "action": "BUY",
                                "side": side,
                                "price": price,
                                "amount": TRADE_AMOUNT,
                                "order_id": order_id,
                                "status": "submitted",
                                "reason": condition,
                                "diff": diff,
                            })
                            save_state(state)
                            _dashboard_set(
                                pending_order=dict(state.get("pending_order") or {}),
                                last_order=dict(state.get("last_order") or {}),
                                trade_history=list(state.get("trade_history") or []),
                            )
                            _sync_dashboard_account_snapshot(dashboard_user)
                            log(f"订单已提交,开始监控 (订单ID: {order_id})", "TRADE")
                        else:
                            # 下单失败,记录避免重复尝试
                            log(f"下单失败: {side} @ {price*100:.1f}%", "ERR")
                            state["last_order"] = {"key": order_key, "time": datetime.now().isoformat()}
                            state = _append_trade_history(state, {
                                "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                "slug": slug,
                                "action": "BUY",
                                "side": side,
                                "price": price,
                                "amount": TRADE_AMOUNT,
                                "order_id": "",
                                "status": "failed",
                                "reason": condition,
                                "diff": diff,
                            })
                            save_state(state)
                            _dashboard_set(
                                last_order=dict(state.get("last_order") or {}),
                                trade_history=list(state.get("trade_history") or []),
                            )
                            _sync_dashboard_account_snapshot(dashboard_user)
                    else:
                        log(f"提醒模式: 建议买入 {side} @ {price*100:.1f}%", "TRADE")
                        state["last_order"] = {"key": order_key, "time": datetime.now().isoformat()}
                        save_state(state)
                        _dashboard_set(last_order=dict(state.get("last_order") or {}))
            
            # 止损检查
            state = load_state()
            pos = state.get("position")
            if pos and pos.get("slug") == slug:
                if diff_abs < STOP_LOSS_DIFF:
                    log(f"止损触发! 价差${diff_abs:.0f} < ${STOP_LOSS_DIFF}", "TRADE")
                    
                    if AUTO_TRADE and trader.connected:
                        pos_side = pos.get("side")
                        # 止损卖出使用 best_bid (买一价) 以确保立即成交
                        if pos_side == "UP":
                            sell_price = price_data.get("up_best_bid") or up_price
                        else:
                            sell_price = price_data.get("down_best_bid") or down_price
                        sell_token = market["up_token"] if pos_side == "UP" else market["down_token"]
                        log(f"止损卖出价格: best_bid={sell_price:.4f}", "TRADE")
                        sell_order_id = trader.place_order(sell_token, "SELL", sell_price, TRADE_AMOUNT)
                        state = _append_trade_history(state, {
                            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            "slug": slug,
                            "action": "SELL",
                            "side": pos_side,
                            "price": sell_price,
                            "amount": TRADE_AMOUNT,
                            "order_id": sell_order_id or "",
                            "status": "submitted" if sell_order_id else "failed",
                            "reason": "stop_loss",
                            "diff": diff,
                        })
                        state.pop("position", None)
                        save_state(state)
                        _dashboard_set(position={}, trade_history=list(state.get("trade_history") or []))
                        _sync_dashboard_account_snapshot(dashboard_user)
                        log(f"止损卖出完成: {pos_side} @ {sell_price*100:.2f}%", "TRADE")
            
            time.sleep(0.1)  # 缩短循环间隔到0.1秒
            
    except KeyboardInterrupt:
        print("\n\n退出监控")
        if market_listener:
            market_listener.stop()
        redeemer.stop()

if __name__ == "__main__":
    main()
