import os
import threading
import logging
from flask import Flask, request, jsonify

from alpaca.trading.client import TradingClient
from alpaca.trading.requests import LimitOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce

from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockLatestTradeRequest

# =======================
# CONFIG
# =======================

BOTS = {
    "GLD": {
        "lower": 365.76,
        "upper": 436.84,
        "grid_pct": 0.005,          # 0.5%
        "mode": "arithmetic",        # "geometric" or "arithmetic"
        "order_usd": 1000,
        "max_capital": 35000
    },
    "SLV": {
        "lower": 63.00,
        "upper": 77.48,
        "grid_pct": 0.005,          # 0.5%
        "mode": "arithmetic",        # "geometric" or "arithmetic"
        "order_usd": 1500,
        "max_capital": 60000
    }
}

RUN_TOKEN = os.getenv("RUN_TOKEN")

# Use the env var names you were using before (Render):
ALPACA_KEY = os.getenv("ALPACA_KEY") or os.getenv("ALPACA_API_KEY")
ALPACA_SECRET = os.getenv("ALPACA_SECRET") or os.getenv("ALPACA_SECRET_KEY")

PAPER = True

if not ALPACA_KEY or not ALPACA_SECRET:
    raise RuntimeError("Missing ALPACA_KEY/ALPACA_SECRET in environment variables")

# =======================
# LOGGING
# =======================

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("leo")

# =======================
# INIT
# =======================

app = Flask(__name__)
trading = TradingClient(ALPACA_KEY, ALPACA_SECRET, paper=PAPER)
data = StockHistoricalDataClient(ALPACA_KEY, ALPACA_SECRET)

run_lock = threading.Lock()

OPEN_STATUSES = {"new", "accepted", "partially_filled"}

# =======================
# HELPERS
# =======================

def get_last_price(symbol: str) -> float:
    req = StockLatestTradeRequest(symbol_or_symbols=symbol)
    trade = data.get_stock_latest_trade(req)[symbol]
    return float(trade.price)

def get_open_orders(symbol: str):
    # Note: get_orders() returns recent orders; filter by symbol + open statuses
    orders = trading.get_orders()
    return [o for o in orders if o.symbol == symbol and str(o.status) in OPEN_STATUSES]

def get_open_order_prices(symbol: str):
    prices = set()
    for o in get_open_orders(symbol):
        try:
            if o.limit_price is not None:
                prices.add(round(float(o.limit_price), 2))
        except:
            pass
    return prices

def get_open_order_sides(symbol: str):
    sides = set()
    for o in get_open_orders(symbol):
        try:
            sides.add(str(o.side).lower())
        except:
            pass
    return sides

def get_position_qty(symbol: str) -> int:
    try:
        pos = trading.get_open_position(symbol)
        # equities qty is string, often integer-looking
        return int(float(pos.qty))
    except:
        return 0

def round_price(p: float) -> float:
    return round(p, 2)

def grid_levels(lower: float, upper: float, pct: float, mode: str):
    lo, hi = (lower, upper) if lower < upper else (upper, lower)

    levels = []
    if mode.lower() == "arithmetic":
        step = lo * pct
        p = lo
        while p <= hi:
            levels.append(round_price(p))
            p += step
    else:
        p = lo
        while p <= hi:
            levels.append(round_price(p))
            p *= (1 + pct)
    return levels

def within_capital(symbol: str, cfg: dict, qty: int, price: float) -> bool:
    # Simple cap check: used capital = current position market value
    # (Stateless; avoids DB)
    owned = get_position_qty(symbol)
    used = owned * price
    new_cost = qty * price
    return (used + new_cost) <= cfg["max_capital"]

# =======================
# CORE LOGIC
# =======================

def run_bot(symbol: str, cfg: dict):
    price = get_last_price(symbol)
    log.info(f"📈 {symbol} PRICE = {price}")

    lo = min(cfg["lower"], cfg["upper"])
    hi = max(cfg["lower"], cfg["upper"])

    if not (lo <= price <= hi):
        log.info(f"⏸️ {symbol} outside band [{lo}, {hi}] — no action")
        return

    levels = grid_levels(cfg["lower"], cfg["upper"], cfg["grid_pct"], cfg.get("mode", "geometric"))
    open_prices = get_open_order_prices(symbol)
    open_sides = get_open_order_sides(symbol)

    owned_qty = get_position_qty(symbol)

    # We only allow ONE open order at a time per symbol (prevents BUY+SELL conflict errors)
    if len(get_open_orders(symbol)) > 0:
        log.info(f"🧱 {symbol} has open order(s) — skipping this run to avoid conflicts")
        return

    # Decide one action near the current price:
    # - Place BUY at the nearest grid below price
    # - Place SELL at the nearest grid above price (only if you own shares)

    below = [p for p in levels if p < price]
    above = [p for p in levels if p > price]

    buy_price = max(below) if below else None
    sell_price = min(above) if above else None

    # BUY
    if buy_price is not None and buy_price not in open_prices:
        qty = int(cfg["order_usd"] // buy_price)
        if qty <= 0:
            log.info(f"⚠️ {symbol} BUY qty=0 (order_usd too small)")
            return
        if not within_capital(symbol, cfg, qty, price):
            log.info(f"💰 {symbol} max_capital reached — BUY skipped")
            return

        log.info(f"🟢 BUY | {symbol} | Qty: {qty} @ {buy_price}")
        trading.submit_order(
            LimitOrderRequest(
                symbol=symbol,
                qty=qty,
                side=OrderSide.BUY,
                limit_price=buy_price,
                time_in_force=TimeInForce.GTC
            )
        )
        return

    # SELL
    if sell_price is not None and sell_price not in open_prices:
        # sell same size as buy would be at that level, but limited by what we own
        qty = int(cfg["order_usd"] // sell_price)
        if qty <= 0:
            log.info(f"⚠️ {symbol} SELL qty=0 (order_usd too small)")
            return
        if owned_qty <= 0:
            log.info(f"📦 {symbol} no position — SELL skipped")
            return
        qty = min(qty, owned_qty)

        log.info(f"🔴 SELL | {symbol} | Qty: {qty} @ {sell_price}")
        trading.submit_order(
            LimitOrderRequest(
                symbol=symbol,
                qty=qty,
                side=OrderSide.SELL,
                limit_price=sell_price,
                time_in_force=TimeInForce.GTC
            )
        )
        return

    log.info(f"😴 {symbol} nothing to do this run")

# =======================
# ROUTES
# =======================

@app.route("/run", methods=["GET"])
def run():
    if RUN_TOKEN and request.headers.get("X-RUN-TOKEN") != RUN_TOKEN:
        return jsonify({"error": "Unauthorized /run attempt blocked"}), 401

    if not run_lock.acquire(blocking=False):
        return jsonify({"status": "already running"}), 200

    try:
        for symbol, cfg in BOTS.items():
            run_bot(symbol, cfg)
        return jsonify({"bots": list(BOTS.keys()), "status": "ok"})
    finally:
        run_lock.release()

@app.route("/healthz")
def health():
    return "ok", 200

# =======================
# START
# =======================

if __name__ == "__main__":
    log.info("🦁 Leo started (Paper Trading)")
    port = int(os.getenv("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
