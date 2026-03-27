"""
ETH/USD1 Cross-Exchange Arbitrage Bot - PRODUCTION READY
Exchanges: Binance Global (0% fee) x MEXC (0.05% fee)
Deploy: Render.com (paid tier recommended)
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
from typing import Optional, Dict, Any
import uuid

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
    "capital": 5.0, 
    "min_spread": 0.001, 
    "take_profit": 10.0,
    "slippage_buffer": 0.20,  # 20% buffer for slippage
    "binance_usd1_balance": 0.0, 
    "binance_eth_balance": 0.0,
    "mexc_usd1_balance": 0.0, 
    "mexc_eth_balance": 0.0,
    "total_profit": 0.0, 
    "total_trades": 0,
    "winning_trades": 0, 
    "losing_trades": 0, 
    "total_fees_paid": 0.0,
    "binance_api_key": "", 
    "binance_api_secret": "",
    "mexc_api_key": "", 
    "mexc_api_secret": "",
    "logs": deque(maxlen=200),
    "trade_history": deque(maxlen=100),
    "pending_orders": {},  # Track pending orders for confirmation
    "binance_ws_status": "disconnected",
    "mexc_ws_status": "disconnected",
    "last_opportunity": None,
    "spread": 0.0, 
    "spread_pct": 0.0,
    "buy_exchange": None, 
    "sell_exchange": None,
    "last_trade_time": 0,  # Cooldown tracking
    "trade_cooldown": 2.0,  # Seconds between trades
}

dashboard_clients = set()

BINANCE_FEE = 0.0000
MEXC_FEE = 0.0005
SYMBOL_BINANCE = "ETHUSD1"
SYMBOL_MEXC = "ETHUSD1"

# Global locks for critical sections
_trade_lock = asyncio.Lock()
_order_locks = {
    "BINANCE": asyncio.Lock(),
    "MEXC": asyncio.Lock()
}

# ── Lifespan ───────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Load API keys from Render environment variables on startup
    state["binance_api_key"] = os.environ.get("BINANCE_API_KEY", "")
    state["binance_api_secret"] = os.environ.get("BINANCE_API_SECRET", "")
    state["mexc_api_key"] = os.environ.get("MEXC_API_KEY", "")
    state["mexc_api_secret"] = os.environ.get("MEXC_API_SECRET", "")
    
    if state["binance_api_key"]:
        log("API keys loaded from environment variables", "SYSTEM")
        # Validate API keys on startup
        asyncio.create_task(validate_api_keys())
    else:
        log("No API keys in environment - running in PAPER mode", "SYSTEM")
    
    log("ARB BOT started", "SYSTEM")
    asyncio.create_task(binance_ws())
    asyncio.create_task(mexc_ws())  # Now using WebSocket instead of REST
    
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
        "slippage_buffer": state["slippage_buffer"],
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

# ── API Key Validation ─────────────────────────────────────────────────────────
async def validate_api_keys():
    """Validate API keys on startup"""
    if state["binance_api_key"]:
        valid = await validate_binance_key()
        if not valid:
            log("Binance API keys invalid - running in PAPER mode", "WARN")
            state["binance_api_key"] = ""
            state["binance_api_secret"] = ""
    
    if state["mexc_api_key"]:
        valid = await validate_mexc_key()
        if not valid:
            log("MEXC API keys invalid - running in PAPER mode", "WARN")
            state["mexc_api_key"] = ""
            state["mexc_api_secret"] = ""

async def validate_binance_key() -> bool:
    try:
        ts = int(time.time() * 1000)
        query = f"timestamp={ts}"
        sig = hmac.new(state["binance_api_secret"].encode(), query.encode(), hashlib.sha256).hexdigest()
        headers = {"X-MBX-APIKEY": state["binance_api_key"]}
        async with aiohttp.ClientSession() as s:
            async with s.get(f"https://api.binance.com/api/v3/account?{query}&signature={sig}", headers=headers) as r:
                return r.status == 200
    except:
        return False

async def validate_mexc_key() -> bool:
    try:
        ts = int(time.time() * 1000)
        query = f"timestamp={ts}"
        sig = hmac.new(state["mexc_api_secret"].encode(), query.encode(), hashlib.sha256).hexdigest()
        headers = {"X-MEXC-APIKEY": state["mexc_api_key"]}
        async with aiohttp.ClientSession() as s:
            async with s.get(f"https://api.mexc.com/api/v3/account?{query}&signature={sig}", headers=headers) as r:
                return r.status == 200
    except:
        return False

# ── Spread Calculator with Slippage Protection ─────────────────────────────────
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

    capital = state["capital"]
    eth_qty = capital / buy_price
    raw_spread = sell_price - buy_price
    buy_fee = capital * buy_fee_rate
    sell_fee = (eth_qty * sell_price) * sell_fee_rate
    gross_profit = raw_spread * eth_qty
    net_profit = gross_profit - buy_fee - sell_fee
    
    # Apply slippage buffer (reduce profit by buffer percentage)
    adjusted_net_profit = net_profit * (1 - state["slippage_buffer"])
    spread_pct = (raw_spread / buy_price) * 100

    state["spread"] = round(raw_spread, 4)
    state["spread_pct"] = round(spread_pct, 6)
    state["buy_exchange"] = buy_ex
    state["sell_exchange"] = sell_ex
    state["last_opportunity"] = {
        "buy_ex": buy_ex, 
        "sell_ex": sell_ex,
        "buy_price": round(buy_price, 4),
        "sell_price": round(sell_price, 4),
        "raw_spread": round(raw_spread, 4),
        "net_profit": round(adjusted_net_profit, 6),
        "gross_profit": round(net_profit, 6),
        "eth_qty": round(eth_qty, 6),
        "buy_fee": round(buy_fee, 6),
        "sell_fee": round(sell_fee, 6),
        "spread_pct": round(spread_pct, 6),
        "slippage_adjusted": True,
    }

    # Check if opportunity is valid with cooldown
    current_time = time.time()
    if (state["running"] and 
        adjusted_net_profit >= state["min_spread"] and
        current_time - state["last_trade_time"] >= state["trade_cooldown"]):
        asyncio.create_task(execute_trade(state["last_opportunity"]))

# ── Trade Execution with Balance Check and Lock ────────────────────────────────
async def execute_trade(opp: dict):
    """Execute trade with proper locking and balance verification"""
    async with _trade_lock:  # Prevent duplicate trades
        if not state["running"]:
            return
        
        # Cooldown check again inside lock
        if time.time() - state["last_trade_time"] < state["trade_cooldown"]:
            log("Trade skipped: cooldown period", "WARN")
            return
        
        # Check balances before trading
        await fetch_balances()
        
        # Verify sufficient funds
        if not await verify_balances(opp):
            log("Insufficient funds for trade", "ERROR")
            return
        
        log(f"TRADE BUY {opp['buy_ex']} @ {opp['buy_price']} | SELL {opp['sell_ex']} @ {opp['sell_price']} | Net: ${opp['net_profit']:.6f}", "TRADE")
        
        # Generate unique trade ID for tracking
        trade_id = str(uuid.uuid4())[:8]
        
        # Place limit orders instead of market orders
        buy_order_id = await place_limit_order(
            opp["buy_ex"], "BUY", opp["eth_qty"], 
            opp["buy_price"], trade_id
        )
        
        if not buy_order_id:
            log(f"Failed to place BUY order on {opp['buy_ex']}", "ERROR")
            return
        
        # Small delay to ensure buy order is processed
        await asyncio.sleep(0.5)
        
        sell_order_id = await place_limit_order(
            opp["sell_ex"], "SELL", opp["eth_qty"], 
            opp["sell_price"], trade_id
        )
        
        if not sell_order_id:
            log(f"Failed to place SELL order on {opp['sell_ex']}", "ERROR")
            # Cancel buy order if sell fails
            await cancel_order(opp["buy_ex"], buy_order_id)
            return
        
        # Track pending orders
        state["pending_orders"][trade_id] = {
            "buy": {"exchange": opp["buy_ex"], "order_id": buy_order_id},
            "sell": {"exchange": opp["sell_ex"], "order_id": sell_order_id},
            "timestamp": time.time(),
            "opp": opp
        }
        
        # Confirm orders after delay
        asyncio.create_task(confirm_orders(trade_id))
        
        state["last_trade_time"] = time.time()

async def verify_balances(opp: dict) -> bool:
    """Verify sufficient balance before trading"""
    buy_exchange = opp["buy_ex"]
    sell_exchange = opp["sell_ex"]
    eth_qty = opp["eth_qty"]
    buy_price = opp["buy_price"]
    sell_price = opp["sell_price"]
    
    if buy_exchange == "BINANCE":
        required_usd1 = eth_qty * buy_price
        if state["binance_usd1_balance"] < required_usd1 * 1.01:  # 1% buffer
            log(f"Binance insufficient USD1: {state['binance_usd1_balance']:.4f} < {required_usd1:.4f}", "WARN")
            return False
    else:  # MEXC
        required_usd1 = eth_qty * buy_price
        if state["mexc_usd1_balance"] < required_usd1 * 1.01:
            log(f"MEXC insufficient USD1: {state['mexc_usd1_balance']:.4f} < {required_usd1:.4f}", "WARN")
            return False
    
    if sell_exchange == "BINANCE":
        if state["binance_eth_balance"] < eth_qty * 1.01:
            log(f"Binance insufficient ETH: {state['binance_eth_balance']:.6f} < {eth_qty:.6f}", "WARN")
            return False
    else:  # MEXC
        if state["mexc_eth_balance"] < eth_qty * 1.01:
            log(f"MEXC insufficient ETH: {state['mexc_eth_balance']:.6f} < {eth_qty:.6f}", "WARN")
            return False
    
    return True

async def place_limit_order(exchange: str, side: str, qty: float, price: float, trade_id: str) -> Optional[str]:
    """Place limit order with price protection"""
    if not state["binance_api_key"] or not state["mexc_api_key"]:
        log(f"[PAPER] {side} {qty:.6f} ETH on {exchange} @ {price}", "PAPER")
        return f"paper_{trade_id}"
    
    async with _order_locks.get(exchange, asyncio.Lock()):
        try:
            if exchange == "BINANCE":
                return await binance_limit_order(side, qty, price)
            else:
                return await mexc_limit_order(side, qty, price)
        except Exception as e:
            log(f"Order error {exchange}: {e}", "ERROR")
            return None

async def binance_limit_order(side: str, qty: float, price: float) -> Optional[str]:
    """Place limit order on Binance"""
    ts = int(time.time() * 1000)
    params = {
        "symbol": SYMBOL_BINANCE, 
        "side": side, 
        "type": "LIMIT",
        "timeInForce": "GTC",  # Good 'til cancelled
        "quantity": f"{qty:.6f}", 
        "price": f"{price:.2f}",
        "timestamp": ts
    }
    query = urllib.parse.urlencode(params)
    sig = hmac.new(state["binance_api_secret"].encode(), query.encode(), hashlib.sha256).hexdigest()
    headers = {"X-MBX-APIKEY": state["binance_api_key"]}
    
    async with aiohttp.ClientSession() as s:
        async with s.post(f"https://api.binance.com/api/v3/order?{query}&signature={sig}", headers=headers) as r:
            data = await r.json()
            if "orderId" in data:
                return str(data["orderId"])
            log(f"Binance order err: {data}", "ERROR")
            return None

async def mexc_limit_order(side: str, qty: float, price: float) -> Optional[str]:
    """Place limit order on MEXC"""
    ts = int(time.time() * 1000)
    params = {
        "symbol": SYMBOL_MEXC, 
        "side": side, 
        "type": "LIMIT_ORDER",
        "quantity": f"{qty:.6f}", 
        "price": f"{price:.2f}",
        "timestamp": ts
    }
    query = urllib.parse.urlencode(params)
    sig = hmac.new(state["mexc_api_secret"].encode(), query.encode(), hashlib.sha256).hexdigest()
    headers = {"X-MEXC-APIKEY": state["mexc_api_key"]}
    
    async with aiohttp.ClientSession() as s:
        async with s.post(f"https://api.mexc.com/api/v3/order?{query}&signature={sig}", headers=headers) as r:
            data = await r.json()
            if "orderId" in data:
                return str(data["orderId"])
            log(f"MEXC order err: {data}", "ERROR")
            return None

async def confirm_orders(trade_id: str):
    """Confirm order status and update trade record"""
    await asyncio.sleep(3)  # Wait for orders to process
    
    pending = state["pending_orders"].get(trade_id)
    if not pending:
        return
    
    opp = pending["opp"]
    buy_ok = await check_order_status(pending["buy"]["exchange"], pending["buy"]["order_id"])
    sell_ok = await check_order_status(pending["sell"]["exchange"], pending["sell"]["order_id"])
    
    record = {
        "time": datetime.now().strftime("%H:%M:%S"),
        "trade_id": trade_id,
        "buy_ex": opp["buy_ex"], 
        "sell_ex": opp["sell_ex"],
        "buy_price": opp["buy_price"], 
        "sell_price": opp["sell_price"],
        "eth_qty": opp["eth_qty"], 
        "net_profit": opp["net_profit"],
        "status": "FILLED" if (buy_ok and sell_ok) else "PARTIAL",
    }
    
    state["trade_history"].appendleft(record)
    state["total_trades"] += 1
    state["total_fees_paid"] += opp["buy_fee"] + opp["sell_fee"]
    
    if buy_ok and sell_ok:
        state["total_profit"] += opp["net_profit"]
        state["winning_trades"] += 1
        log(f"Trade {trade_id} FILLED | Profit: ${opp['net_profit']:.6f} | Total: ${state['total_profit']:.4f}", "TRADE")
        
        if state["total_profit"] >= state["take_profit"]:
            state["running"] = False
            log(f"Take profit ${state['take_profit']} hit — bot stopped", "SYSTEM")
    else:
        state["losing_trades"] += 1
        log(f"Trade {trade_id} PARTIAL — buy:{buy_ok} sell:{sell_ok}", "WARN")
    
    # Clean up
    del state["pending_orders"][trade_id]
    await fetch_balances()
    await broadcast_state()

async def check_order_status(exchange: str, order_id: str) -> bool:
    """Check if order was filled"""
    if order_id.startswith("paper_"):
        return True
    
    try:
        if exchange == "BINANCE":
            return await check_binance_order(order_id)
        else:
            return await check_mexc_order(order_id)
    except Exception as e:
        log(f"Order check error {exchange}: {e}", "ERROR")
        return False

async def check_binance_order(order_id: str) -> bool:
    ts = int(time.time() * 1000)
    params = {"symbol": SYMBOL_BINANCE, "orderId": order_id, "timestamp": ts}
    query = urllib.parse.urlencode(params)
    sig = hmac.new(state["binance_api_secret"].encode(), query.encode(), hashlib.sha256).hexdigest()
    headers = {"X-MBX-APIKEY": state["binance_api_key"]}
    
    async with aiohttp.ClientSession() as s:
        async with s.get(f"https://api.binance.com/api/v3/order?{query}&signature={sig}", headers=headers) as r:
            data = await r.json()
            return data.get("status") == "FILLED"

async def check_mexc_order(order_id: str) -> bool:
    ts = int(time.time() * 1000)
    params = {"symbol": SYMBOL_MEXC, "orderId": order_id, "timestamp": ts}
    query = urllib.parse.urlencode(params)
    sig = hmac.new(state["mexc_api_secret"].encode(), query.encode(), hashlib.sha256).hexdigest()
    headers = {"X-MEXC-APIKEY": state["mexc_api_key"]}
    
    async with aiohttp.ClientSession() as s:
        async with s.get(f"https://api.mexc.com/api/v3/order?{query}&signature={sig}", headers=headers) as r:
            data = await r.json()
            return data.get("status") == "FILLED"

async def cancel_order(exchange: str, order_id: str):
    """Cancel an order if needed"""
    if order_id.startswith("paper_"):
        return
    
    try:
        if exchange == "BINANCE":
            await cancel_binance_order(order_id)
        else:
            await cancel_mexc_order(order_id)
    except Exception as e:
        log(f"Order cancel error {exchange}: {e}", "ERROR")

async def cancel_binance_order(order_id: str):
    ts = int(time.time() * 1000)
    params = {"symbol": SYMBOL_BINANCE, "orderId": order_id, "timestamp": ts}
    query = urllib.parse.urlencode(params)
    sig = hmac.new(state["binance_api_secret"].encode(), query.encode(), hashlib.sha256).hexdigest()
    headers = {"X-MBX-APIKEY": state["binance_api_key"]}
    
    async with aiohttp.ClientSession() as s:
        async with s.delete(f"https://api.binance.com/api/v3/order?{query}&signature={sig}", headers=headers) as r:
            pass

async def cancel_mexc_order(order_id: str):
    ts = int(time.time() * 1000)
    params = {"symbol": SYMBOL_MEXC, "orderId": order_id, "timestamp": ts}
    query = urllib.parse.urlencode(params)
    sig = hmac.new(state["mexc_api_secret"].encode(), query.encode(), hashlib.sha256).hexdigest()
    headers = {"X-MEXC-APIKEY": state["mexc_api_key"]}
    
    async with aiohttp.ClientSession() as s:
        async with s.delete(f"https://api.mexc.com/api/v3/order?{query}&signature={sig}", headers=headers) as r:
            pass

# ── Balance Fetch with Error Handling ──────────────────────────────────────────
async def fetch_balances():
    if not state["binance_api_key"]:
        return
    try:
        await _fetch_binance_bal()
        await _fetch_mexc_bal()
    except Exception as e:
        log(f"Balance fetch error: {e}", "ERROR")

async def _fetch_binance_bal():
    ts = int(time.time() * 1000)
    query = f"timestamp={ts}"
    sig = hmac.new(state["binance_api_secret"].encode(), query.encode(), hashlib.sha256).hexdigest()
    headers = {"X-MBX-APIKEY": state["binance_api_key"]}
    async with aiohttp.ClientSession() as s:
        async with s.get(f"https://api.binance.com/api/v3/account?{query}&signature={sig}", headers=headers) as r:
            data = await r.json()
            for a in data.get("balances", []):
                if a["asset"] == "USD1":
                    state["binance_usd1_balance"] = float(a["free"])
                if a["asset"] == "ETH":
                    state["binance_eth_balance"] = float(a["free"])

async def _fetch_mexc_bal():
    ts = int(time.time() * 1000)
    query = f"timestamp={ts}"
    sig = hmac.new(state["mexc_api_secret"].encode(), query.encode(), hashlib.sha256).hexdigest()
    headers = {"X-MEXC-APIKEY": state["mexc_api_key"]}
    async with aiohttp.ClientSession() as s:
        async with s.get(f"https://api.mexc.com/api/v3/account?{query}&signature={sig}", headers=headers) as r:
            data = await r.json()
            for a in data.get("balances", []):
                if a["asset"] == "USD1":
                    state["mexc_usd1_balance"] = float(a["free"])
                if a["asset"] == "ETH":
                    state["mexc_eth_balance"] = float(a["free"])

# ── Binance WebSocket ──────────────────────────────────────────────────────────
async def binance_ws():
    url = f"wss://data-stream.binance.vision/ws/{SYMBOL_BINANCE.lower()}@bookTicker"
    while True:
        try:
            state["binance_ws_status"] = "connecting"
            async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
                state["binance_ws_status"] = "connected"
                log("Binance WS connected", "SYSTEM")
                async for msg in ws:
                    d = json.loads(msg)
                    state["binance_bid"] = float(d["b"])
                    state["binance_ask"] = float(d["a"])
                    state["binance_price"] = (state["binance_bid"] + state["binance_ask"]) / 2
                    calculate_spread()
                    await broadcast_state()
        except Exception as e:
            state["binance_ws_status"] = "disconnected"
            log(f"Binance WS error: {e} — reconnecting in 3s", "WARN")
            await asyncio.sleep(3)

# ── MEXC WebSocket (REPLACED REST POLL) ────────────────────────────────────────
async def mexc_ws():
    """MEXC WebSocket connection for real-time data"""
    url = f"wss://wbs.mexc.com/ws"
    
    while True:
        try:
            state["mexc_ws_status"] = "connecting"
            async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
                # Subscribe to bookTicker
                subscribe_msg = {
                    "method": "SUBSCRIPTION",
                    "params": [f"{SYMBOL_MEXC.lower()}@bookTicker"],
                    "id": 1
                }
                await ws.send(json.dumps(subscribe_msg))
                
                state["mexc_ws_status"] = "connected"
                log("MEXC WS connected", "SYSTEM")
                
                async for msg in ws:
                    try:
                        data = json.loads(msg)
                        if "d" in data:  # Data payload
                            d = data["d"]
                            if "b" in d and "a" in d:  # bookTicker format
                                state["mexc_bid"] = float(d["b"])
                                state["mexc_ask"] = float(d["a"])
                                state["mexc_price"] = (state["mexc_bid"] + state["mexc_ask"]) / 2
                                calculate_spread()
                                await broadcast_state()
                    except json.JSONDecodeError:
                        continue
                        
        except Exception as e:
            state["mexc_ws_status"] = "disconnected"
            log(f"MEXC WS error: {e} — reconnecting in 3s", "WARN")
            await asyncio.sleep(3)

# ── API Routes ─────────────────────────────────────────────────────────────────
@app.post("/api/keys")
async def set_keys(body: dict):
    state["binance_api_key"] = body.get("binance_api_key", "")
    state["binance_api_secret"] = body.get("binance_api_secret", "")
    state["mexc_api_key"] = body.get("mexc_api_key", "")
    state["mexc_api_secret"] = body.get("mexc_api_secret", "")
    log("API keys updated", "SYSTEM")
    
    # Validate new keys
    if state["binance_api_key"]:
        valid = await validate_binance_key()
        if not valid:
            log("Binance API keys invalid", "ERROR")
            state["binance_api_key"] = ""
            state["binance_api_secret"] = ""
    
    if state["mexc_api_key"]:
        valid = await validate_mexc_key()
        if not valid:
            log("MEXC API keys invalid", "ERROR")
            state["mexc_api_key"] = ""
            state["mexc_api_secret"] = ""
    
    await fetch_balances()
    await broadcast_state()
    return {"status": "ok"}

@app.post("/api/start")
async def start_bot(body: dict):
    state["capital"] = max(1.0, float(body.get("capital", 5)))
    state["min_spread"] = float(body.get("min_spread", 0.001))
    state["take_profit"] = float(body.get("take_profit", 10))
    state["slippage_buffer"] = float(body.get("slippage_buffer", 0.20))
    state["running"] = True
    state["total_profit"] = 0.0
    state["total_trades"] = 0
    state["winning_trades"] = 0
    state["losing_trades"] = 0
    state["total_fees_paid"] = 0.0
    state["last_trade_time"] = 0
    log(f"Bot started | Capital: ${state['capital']} | Min: ${state['min_spread']} | TP: ${state['take_profit']} | Slippage buffer: {state['slippage_buffer']*100}%", "SYSTEM")
    await broadcast_state()
    return {"status": "started"}

@app.post("/api/stop")
async def stop_bot():
    state["running"] = False
    log("Bot stopped by user", "SYSTEM")
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