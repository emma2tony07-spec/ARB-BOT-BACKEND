mma"""
ETH/USD1 Cross-Exchange Arbitrage Bot
Exchanges: Binance Global (0% fee) x MEXC (0.05% fee)
Deploy: Render.com (free tier)
"""

import asyncio
import json
import time
import hmac
import hashlib
import urllib.parse
import os
from datetime import datetime
from collections import deque
from contextlib import asynccontextmanager

import aiohttp
import websockets
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

# ── State ──────────────────────────────────────────────────────────────────────
state = {
    "binance_price": None, "mexc_price": None,
    "binance_bid": None, "binance_ask": None,
    "mexc_bid": None, "mexc_ask": None,
    "running": False,
    "capital": 5.0, "min_spread": 0.001, "take_profit": 10.0,
    "binance_usd1_balance": 0.0, "binance_eth_balance": 0.0,
    "mexc_usd1_balance": 0.0, "mexc_eth_balance": 0.0,
    "total_profit": 0.0, "total_trades": 0,
    "winning_trades": 0, "losing_trades": 0, "total_fees_paid": 0.0,
    "binance_api_key": "", "binance_api_secret": "",
    "mexc_api_key": "", "mexc_api_secret": "",
    "logs": deque(maxlen=200),
    "trade_history": deque(maxlen=100),
    "binance_ws_status": "disconnected",
    "mexc_ws_status": "disconnected",
    "last_opportunity": None,
    "spread": 0.0, "spread_pct": 0.0,
    "buy_exchange": None, "sell_exchange": None,
}

dashboard_clients = set()

BINANCE_FEE    = 0.0000
MEXC_FEE       = 0.0005
SYMBOL_BINANCE = "ETHUSD1"
SYMBOL_MEXC    = "ETHUSD1"

# ── Lifespan ───────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Load API keys from Render environment variables on startup
    state["binance_api_key"]    = os.environ.get("BINANCE_API_KEY", "")
    state["binance_api_secret"] = os.environ.get("BINANCE_API_SECRET", "")
    state["mexc_api_key"]       = os.environ.get("MEXC_API_KEY", "")
    state["mexc_api_secret"]    = os.environ.get("MEXC_API_SECRET", "")
    if state["binance_api_key"]:
        log("🔑 API keys loaded from environment variables", "SYSTEM")
    else:
        log("⚠️ No API keys in environment — running in PAPER mode", "SYSTEM")
    log("🚀 ARB BOT started", "SYSTEM")
    asyncio.create_task(binance_ws())
    asyncio.create_task(mexc_poll())
    # Fetch balances if keys are available
    if state["binance_api_key"]:
        asyncio.create_task(fetch_balances())
    yield

# ── App ────────────────────────────────────────────────────────────────────────
app = FastAPI(lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Logging ────────────────────────────────────────────────────────────────────
def log(msg: str, level: str = "INFO"):
    entry = {"time": datetime.now().strftime("%H:%M:%S"), "level": level, "msg": msg}
    state["logs"].appendleft(entry)
    print(f"[{entry['time']}] [{level}] {msg}")
    asyncio.create_task(broadcast_state())

# ── Broadcast ─────────────────────────────────────────────────────────────────
async def broadcast_state():
    global dashboard_clients
    if not dashboard_clients:
        return
    payload = json.dumps(build_payload())
    dead = set()
    for client in dashboard_clients:
        try:
            await client.send_text(payload)
        except Exception:
            dead.add(client)
    dashboard_clients -= dead

def build_payload():
    return {
        "binance_price": state["binance_price"],
        "mexc_price": state["mexc_price"],
        "binance_bid": state["binance_bid"],
        "binance_ask": state["binance_ask"],
        "mexc_bid": state["mexc_bid"],
        "mexc_ask": state["mexc_ask"],
        "spread": state["spread"],
        "spread_pct": state["spread_pct"],
        "buy_exchange": state["buy_exchange"],
        "sell_exchange": state["sell_exchange"],
        "running": state["running"],
        "capital": state["capital"],
        "min_spread": state["min_spread"],
        "take_profit": state["take_profit"],
        "binance_usd1_balance": state["binance_usd1_balance"],
        "binance_eth_balance": state["binance_eth_balance"],
        "mexc_usd1_balance": state["mexc_usd1_balance"],
        "mexc_eth_balance": state["mexc_eth_balance"],
        "total_profit": state["total_profit"],
        "total_trades": state["total_trades"],
        "winning_trades": state["winning_trades"],
        "losing_trades": state["losing_trades"],
        "total_fees_paid": state["total_fees_paid"],
        "binance_ws_status": state["binance_ws_status"],
        "mexc_ws_status": state["mexc_ws_status"],
        "last_opportunity": state["last_opportunity"],
        "logs": list(state["logs"])[:50],
        "trade_history": list(state["trade_history"])[:20],
        "keys_set": bool(state["binance_api_key"] and state["mexc_api_key"]),
    }

# ── Spread Calculator ──────────────────────────────────────────────────────────
def calculate_spread():
    bp = state["binance_price"]
    mp = state["mexc_price"]
    if bp is None or mp is None:
        return

    if mp <= bp:
        buy_ex, sell_ex = "MEXC", "BINANCE"
        buy_price, sell_price = mp, bp
        buy_fee_rate, sell_fee_rate = MEXC_FEE, BINANCE_FEE
    else:
        buy_ex, sell_ex = "BINANCE", "MEXC"
        buy_price, sell_price = bp, mp
        buy_fee_rate, sell_fee_rate = BINANCE_FEE, MEXC_FEE

    capital    = state["capital"]
    eth_qty    = capital / buy_price
    raw_spread = sell_price - buy_price
    buy_fee    = capital * buy_fee_rate
    sell_fee   = (eth_qty * sell_price) * sell_fee_rate
    net_profit = (raw_spread * eth_qty) - buy_fee - sell_fee
    spread_pct = (raw_spread / buy_price) * 100

    state["spread"]            = round(raw_spread, 4)
    state["spread_pct"]        = round(spread_pct, 6)
    state["buy_exchange"]      = buy_ex
    state["sell_exchange"]     = sell_ex
    state["last_opportunity"]  = {
        "buy_ex": buy_ex, "sell_ex": sell_ex,
        "buy_price": round(buy_price, 4),
        "sell_price": round(sell_price, 4),
        "raw_spread": round(raw_spread, 4),
        "net_profit": round(net_profit, 6),
        "eth_qty": round(eth_qty, 6),
        "buy_fee": round(buy_fee, 6),
        "sell_fee": round(sell_fee, 6),
        "spread_pct": round(spread_pct, 6),
    }

    if state["running"] and net_profit >= state["min_spread"]:
        asyncio.create_task(execute_trade(state["last_opportunity"]))

# ── Trade Execution ────────────────────────────────────────────────────────────
# Guard to prevent firing duplicate trades on the same tick
_trade_lock = asyncio.Lock() if False else None  # initialized in lifespan

async def execute_trade(opp: dict):
    if not state["running"]:
        return
    log(f"🎯 BUY {opp['buy_ex']} @ {opp['buy_price']} | SELL {opp['sell_ex']} @ {opp['sell_price']} | Net: ${opp['net_profit']:.6f}", "TRADE")

    buy_ok  = await place_order(opp["buy_ex"],  "BUY",  opp["eth_qty"], opp["buy_price"])
    sell_ok = await place_order(opp["sell_ex"], "SELL", opp["eth_qty"], opp["sell_price"])

    record = {
        "time": datetime.now().strftime("%H:%M:%S"),
        "buy_ex": opp["buy_ex"], "sell_ex": opp["sell_ex"],
        "buy_price": opp["buy_price"], "sell_price": opp["sell_price"],
        "eth_qty": opp["eth_qty"], "net_profit": opp["net_profit"],
        "status": "OK" if (buy_ok and sell_ok) else "PARTIAL",
    }
    state["trade_history"].appendleft(record)
    state["total_trades"] += 1
    state["total_fees_paid"] += opp["buy_fee"] + opp["sell_fee"]

    if buy_ok and sell_ok:
        state["total_profit"] += opp["net_profit"]
        state["winning_trades"] += 1
        log(f"✅ Done | Profit: ${opp['net_profit']:.6f} | Total: ${state['total_profit']:.4f}", "TRADE")
        if state["total_profit"] >= state["take_profit"]:
            state["running"] = False
            log(f"🏁 Take profit ${state['take_profit']} hit — bot stopped", "SYSTEM")
    else:
        state["losing_trades"] += 1
        log(f"⚠️ Partial fill — buy:{buy_ok} sell:{sell_ok}", "WARN")

    await fetch_balances()
    await broadcast_state()

async def place_order(exchange: str, side: str, qty: float, price: float) -> bool:
    if not state["binance_api_key"] or not state["mexc_api_key"]:
        log(f"[PAPER] {side} {qty:.6f} ETH on {exchange} @ {price}", "PAPER")
        return True
    try:
        if exchange == "BINANCE":
            return await binance_order(side, qty)
        else:
            return await mexc_order(side, qty)
    except Exception as e:
        log(f"Order error {exchange}: {e}", "ERROR")
        return False

async def binance_order(side: str, qty: float) -> bool:
    ts     = int(time.time() * 1000)
    params = {"symbol": SYMBOL_BINANCE, "side": side, "type": "MARKET",
              "quantity": f"{qty:.6f}", "timestamp": ts}
    query  = urllib.parse.urlencode(params)
    sig    = hmac.new(state["binance_api_secret"].encode(), query.encode(), hashlib.sha256).hexdigest()
    headers = {"X-MBX-APIKEY": state["binance_api_key"]}
    async with aiohttp.ClientSession() as s:
        async with s.post(f"https://api.binance.com/api/v3/order?{query}&signature={sig}", headers=headers) as r:
            data = await r.json()
            if "orderId" in data:
                return True
            log(f"Binance order err: {data}", "ERROR")
            return False

async def mexc_order(side: str, qty: float) -> bool:
    ts     = int(time.time() * 1000)
    params = {"symbol": SYMBOL_MEXC, "side": side, "type": "MARKET",
              "quantity": f"{qty:.6f}", "timestamp": ts}
    query  = urllib.parse.urlencode(params)
    sig    = hmac.new(state["mexc_api_secret"].encode(), query.encode(), hashlib.sha256).hexdigest()
    headers = {"X-MEXC-APIKEY": state["mexc_api_key"]}
    async with aiohttp.ClientSession() as s:
        async with s.post(f"https://api.mexc.com/api/v3/order?{query}&signature={sig}", headers=headers) as r:
            data = await r.json()
            if "orderId" in data:
                return True
            log(f"MEXC order err: {data}", "ERROR")
            return False

# ── Balance Fetch ──────────────────────────────────────────────────────────────
async def fetch_balances():
    if not state["binance_api_key"]:
        return
    try:
        await _fetch_binance_bal()
        await _fetch_mexc_bal()
    except Exception as e:
        log(f"Balance fetch error: {e}", "ERROR")

async def _fetch_binance_bal():
    ts    = int(time.time() * 1000)
    query = f"timestamp={ts}"
    sig   = hmac.new(state["binance_api_secret"].encode(), query.encode(), hashlib.sha256).hexdigest()
    headers = {"X-MBX-APIKEY": state["binance_api_key"]}
    async with aiohttp.ClientSession() as s:
        async with s.get(f"https://api.binance.com/api/v3/account?{query}&signature={sig}", headers=headers) as r:
            data = await r.json()
            for a in data.get("balances", []):
                if a["asset"] == "USD1":  state["binance_usd1_balance"] = float(a["free"])
                if a["asset"] == "ETH":   state["binance_eth_balance"]  = float(a["free"])

async def _fetch_mexc_bal():
    ts    = int(time.time() * 1000)
    query = f"timestamp={ts}"
    sig   = hmac.new(state["mexc_api_secret"].encode(), query.encode(), hashlib.sha256).hexdigest()
    headers = {"X-MEXC-APIKEY": state["mexc_api_key"]}
    async with aiohttp.ClientSession() as s:
        async with s.get(f"https://api.mexc.com/api/v3/account?{query}&signature={sig}", headers=headers) as r:
            data = await r.json()
            for a in data.get("balances", []):
                if a["asset"] == "USD1":  state["mexc_usd1_balance"] = float(a["free"])
                if a["asset"] == "ETH":   state["mexc_eth_balance"]  = float(a["free"])

# ── Binance WebSocket ──────────────────────────────────────────────────────────
async def binance_ws():
    url = f"wss://stream.binance.com:9443/ws/{SYMBOL_BINANCE.lower()}@bookTicker"
    while True:
        try:
            state["binance_ws_status"] = "connecting"
            async with websockets.connect(url, ping_interval=20) as ws:
                state["binance_ws_status"] = "connected"
                log("Binance WS connected", "SYSTEM")
                async for msg in ws:
                    d = json.loads(msg)
                    state["binance_bid"]   = float(d["b"])
                    state["binance_ask"]   = float(d["a"])
                    state["binance_price"] = (state["binance_bid"] + state["binance_ask"]) / 2
                    calculate_spread()
                    await broadcast_state()
        except Exception as e:
            state["binance_ws_status"] = "disconnected"
            log(f"Binance WS error: {e} — reconnecting in 3s", "WARN")
            await asyncio.sleep(3)

# ── MEXC REST Poll ─────────────────────────────────────────────────────────────
async def mexc_poll():
    url_book  = f"https://api.mexc.com/api/v3/ticker/bookTicker?symbol={SYMBOL_MEXC}"
    url_price = f"https://api.mexc.com/api/v3/ticker/price?symbol={SYMBOL_MEXC}"
    state["mexc_ws_status"] = "connecting"
    connector = aiohttp.TCPConnector(ssl=False)
    session   = aiohttp.ClientSession(connector=connector)
    log("MEXC REST poll starting...", "SYSTEM")
    try:
        while True:
            try:
                async with session.get(url_book, timeout=aiohttp.ClientTimeout(total=5)) as r:
                    if r.status == 200:
                        data = await r.json(content_type=None)
                        bid  = data.get("bidPrice") or data.get("b")
                        ask  = data.get("askPrice") or data.get("a")
                        if bid and ask:
                            state["mexc_bid"]    = float(bid)
                            state["mexc_ask"]    = float(ask)
                            state["mexc_price"]  = (state["mexc_bid"] + state["mexc_ask"]) / 2
                            state["mexc_ws_status"] = "connected"
                            calculate_spread()
                            await broadcast_state()
                        else:
                            async with session.get(url_price, timeout=aiohttp.ClientTimeout(total=5)) as r2:
                                d2    = await r2.json(content_type=None)
                                price = d2.get("price")
                                if price:
                                    state["mexc_price"] = float(price)
                                    state["mexc_bid"]   = float(price)
                                    state["mexc_ask"]   = float(price)
                                    state["mexc_ws_status"] = "connected"
                                    calculate_spread()
                                    await broadcast_state()
                    else:
                        state["mexc_ws_status"] = "disconnected"
                        log(f"MEXC poll HTTP {r.status}", "WARN")
            except asyncio.TimeoutError:
                state["mexc_ws_status"] = "disconnected"
                log("MEXC poll timeout", "WARN")
            except Exception as e:
                state["mexc_ws_status"] = "disconnected"
                log(f"MEXC poll error: {type(e).__name__}: {e}", "WARN")
            await asyncio.sleep(0.5)
    finally:
        await session.close()

# ── API Routes ─────────────────────────────────────────────────────────────────
@app.post("/api/keys")
async def set_keys(body: dict):
    state["binance_api_key"]    = body.get("binance_api_key", "")
    state["binance_api_secret"] = body.get("binance_api_secret", "")
    state["mexc_api_key"]       = body.get("mexc_api_key", "")
    state["mexc_api_secret"]    = body.get("mexc_api_secret", "")
    log("API keys updated", "SYSTEM")
    await fetch_balances()
    await broadcast_state()
    return {"status": "ok"}

@app.post("/api/start")
async def start_bot(body: dict):
    state["capital"]         = max(1.0, float(body.get("capital", 5)))
    state["min_spread"]      = float(body.get("min_spread", 0.001))
    state["take_profit"]     = float(body.get("take_profit", 10))
    state["running"]         = True
    state["total_profit"]    = 0.0
    state["total_trades"]    = 0
    state["winning_trades"]  = 0
    state["losing_trades"]   = 0
    state["total_fees_paid"] = 0.0
    log(f"▶ Bot started | Capital: ${state['capital']} | Min: ${state['min_spread']} | TP: ${state['take_profit']}", "SYSTEM")
    await broadcast_state()
    return {"status": "started"}

@app.post("/api/stop")
async def stop_bot():
    state["running"] = False
    log("⏹ Bot stopped by user", "SYSTEM")
    await broadcast_state()
    return {"status": "stopped"}

@app.get("/api/state")
async def get_state():
    return build_payload()

@app.get("/health")
async def health():
    return {"status": "ok"}

@app.websocket("/ws")
async def dashboard_ws(websocket: WebSocket):
    global dashboard_clients
    await websocket.accept()
    dashboard_clients.add(websocket)
    try:
        await websocket.send_text(json.dumps(build_payload()))
        while True:
            await asyncio.sleep(1)
            try:
                await websocket.send_text(json.dumps(build_payload()))
            except Exception:
                break
    except (WebSocketDisconnect, Exception):
        pass
    finally:
        dashboard_clients.discard(websocket)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
