import os
import math
import logging
from datetime import datetime, timezone, date
from decimal import Decimal, ROUND_HALF_UP

import requests
import psycopg2
from psycopg2.extras import RealDictCursor

from flask import Flask, request, jsonify

from alpaca.trading.client import TradingClient
from alpaca.trading.requests import GetOrdersRequest, LimitOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce, QueryOrderStatus
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockLatestTradeRequest


# =======================
# LOGGING
# =======================
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("leo")


# =======================
# CONFIG (EDIT THIS)
# =======================
BOTS = {
    "GLD": {
        "lower": 365.76,
        "upper": 436.84,
        "grid_pct": 0.005,      # 0.5% geometric spacing
        "order_usd": 10000,      # you can change later
        "max_capital": 90000
    },
    "SLV": {
        "lower": 70,
        "upper": 81.82,
        "grid_pct": 0.005,      # 0.5% geometric spacing
        "order_usd": 15000,      # you can change later
        "max_capital": 90000
    }
}

PAPER = os.getenv("PAPER_TRADING", "true").lower() == "true"
TRADING_ENABLED = os.getenv("TRADING_ENABLED", "true").lower() == "true"

RUN_TOKEN = os.getenv("RUN_TOKEN", "")  # required header X-RUN-TOKEN
DATABASE_URL = os.getenv("DATABASE_URL")  # from Render Postgres

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

# Send daily summary after this UTC hour (0-23). (Example: 20 = 20:00 UTC)
DAILY_SUMMARY_HOUR_UTC = int(os.getenv("DAILY_SUMMARY_HOUR_UTC", "20"))


# =======================
# ALPACA KEYS (support both naming styles)
# =======================
ALPACA_KEY = os.getenv("ALPACA_API_KEY") or os.getenv("ALPACA_KEY")
ALPACA_SECRET = os.getenv("ALPACA_SECRET_KEY") or os.getenv("ALPACA_SECRET")

if not ALPACA_KEY or not ALPACA_SECRET:
    raise RuntimeError("Missing Alpaca keys. Set ALPACA_API_KEY + ALPACA_SECRET_KEY (or ALPACA_KEY + ALPACA_SECRET).")

if not DATABASE_URL:
    raise RuntimeError("Missing DATABASE_URL. Add Render Postgres and connect it to this service.")

trading = TradingClient(ALPACA_KEY, ALPACA_SECRET, paper=PAPER)
data_client = StockHistoricalDataClient(ALPACA_KEY, ALPACA_SECRET)

app = Flask(__name__)


# =======================
# SMALL UTILITIES
# =======================
def d2(x: float) -> float:
    """Round price to 2 decimals."""
    return float(Decimal(str(x)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))

def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def utc_day() -> date:
    return now_utc().date()

def tg_enabled() -> bool:
    return bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)

def tg_send(text: str) -> None:
    if not tg_enabled():
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": text}, timeout=10)
    except Exception as e:
        log.warning(f"Telegram send failed: {e}")

def pg_conn():
    # Use a short statement timeout to avoid hanging
    conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
    conn.autocommit = False
    return conn


# =======================
# POSTGRES INIT
# =======================
def init_db():
    conn = pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS bot_state (
                    k TEXT PRIMARY KEY,
                    v TEXT NOT NULL
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS daily_equity (
                    day DATE PRIMARY KEY,
                    start_equity NUMERIC NOT NULL,
                    summary_sent BOOLEAN NOT NULL DEFAULT FALSE
                );
            """)
        conn.commit()
        log.info("✅ Postgres initialized")
    finally:
        conn.close()


# =======================
# GLOBAL RUN LOCK (prevents double /run due to retries)
# =======================
def acquire_global_lock(conn) -> bool:
    # Arbitrary lock key. Same key = same lock.
    lock_key = 99112233
    with conn.cursor() as cur:
        cur.execute("SELECT pg_try_advisory_lock(%s) AS locked;", (lock_key,))
        return bool(cur.fetchone()["locked"])

def release_global_lock(conn) -> None:
    lock_key = 99112233
    with conn.cursor() as cur:
        cur.execute("SELECT pg_advisory_unlock(%s);", (lock_key,))


# =======================
# PRICE + ACCOUNT
# =======================
def get_last_price(symbol: str) -> float:
    req = StockLatestTradeRequest(symbol_or_symbols=symbol)
    trade = data_client.get_stock_latest_trade(req)[symbol]
    return float(trade.price)

def get_account_equity() -> float:
    acct = trading.get_account()
    return float(acct.equity)

def get_position_qty(symbol: str) -> float:
    try:
        pos = trading.get_open_position(symbol)
        return float(pos.qty)
    except Exception:
        return 0.0

def get_open_orders(symbol: str):
    # Pull open orders from Alpaca (NEW/ACCEPTED/PARTIALLY_FILLED)
    req = GetOrdersRequest(
        status=QueryOrderStatus.OPEN,
        symbols=[symbol]
    )
    return trading.get_orders(filter=req)


# =======================
# GRID (GEOMETRIC)
# =======================
def build_geometric_levels(lower: float, upper: float, grid_pct: float):
    """
    Levels: lower, lower*(1+g), lower*(1+g)^2, ... <= upper
    """
    if lower <= 0 or upper <= 0 or grid_pct <= 0:
        return []

    levels = []
    p = lower
    # safety max iterations
    for _ in range(5000):
        if p > upper + 1e-9:
            break
        levels.append(d2(p))
        p = p * (1.0 + grid_pct)
    return levels

def nearest_buy_level(levels, price: float):
    # highest level strictly below price
    below = [lv for lv in levels if lv < price]
    return max(below) if below else None

def nearest_sell_level(levels, price: float):
    # lowest level strictly above price
    above = [lv for lv in levels if lv > price]
    return min(above) if above else None


# =======================
# CAPITAL RULE (prevents runaway ordering)
# =======================
def capital_used(symbol: str, last_price: float, open_orders):
    """
    "Capital used" = position market value + reserved value of open BUY orders
    """
    pos_qty = get_position_qty(symbol)
    pos_value = pos_qty * last_price

    reserved_buy = 0.0
    for o in open_orders:
        if o.side.value.lower() == "buy":
            reserved_buy += float(o.qty) * float(o.limit_price)

    return pos_value + reserved_buy


# =======================
# SAFE ORDER PLACEMENT
# =======================
def has_open_order_at(open_orders, side: str, price: float) -> bool:
    # compare by rounded cents
    p = d2(price)
    for o in open_orders:
        if o.side.value.lower() == side.lower():
            try:
                if d2(float(o.limit_price)) == p:
                    return True
            except Exception:
                continue
    return False

def open_sell_qty(open_orders) -> float:
    qty = 0.0
    for o in open_orders:
        if o.side.value.lower() == "sell":
            qty += float(o.qty)
    return qty

def place_limit(symbol: str, side: str, qty: float, price: float):
    if not TRADING_ENABLED:
        msg = f"⛔️ TRADING DISABLED | blocked {side.upper()} {symbol}"
        log.info(msg)
        tg_send(msg)
        return None

    req = LimitOrderRequest(
        symbol=symbol,
        qty=qty,
        side=OrderSide.BUY if side.lower() == "buy" else OrderSide.SELL,
        limit_price=price,
        time_in_force=TimeInForce.GTC
    )
    return trading.submit_order(req)


# =======================
# DAILY SUMMARY
# =======================
def maybe_daily_summary(conn):
    day = utc_day()
    hour = now_utc().hour

    # Only attempt summary when we are past the configured hour
    if hour < DAILY_SUMMARY_HOUR_UTC:
        return

    equity = get_account_equity()

    with conn.cursor() as cur:
        cur.execute("SELECT * FROM daily_equity WHERE day=%s;", (day,))
        row = cur.fetchone()

        if row is None:
            # First time we see this day, store start equity now
            cur.execute(
                "INSERT INTO daily_equity(day, start_equity, summary_sent) VALUES(%s, %s, false);",
                (day, equity)
            )
            conn.commit()
            return

        if row["summary_sent"]:
            return

        start_equity = float(row["start_equity"])
        pnl = equity - start_equity

        text = f"📊 DAILY P&L (UTC {day})\nEquity: ${equity:,.2f}\nPnL: ${pnl:,.2f}"
        log.info(text.replace("\n", " | "))
        tg_send(text)

        cur.execute("UPDATE daily_equity SET summary_sent=true WHERE day=%s;", (day,))
        conn.commit()


# =======================
# CORE BOT (one action per symbol per /run, but can accumulate many grids over time)
# =======================
def run_symbol(symbol: str, cfg: dict):
    lower = float(cfg["lower"])
    upper = float(cfg["upper"])
    grid_pct = float(cfg["grid_pct"])
    order_usd = float(cfg["order_usd"])
    max_capital = float(cfg["max_capital"])

    if lower >= upper:
        msg = f"⚠️ {symbol} config invalid: lower({lower}) must be < upper({upper})"
        log.error(msg)
        tg_send(msg)
        return {"symbol": symbol, "action": "error", "reason": "bad_config"}

    last_price = get_last_price(symbol)
    log.info(f"📈 {symbol} PRICE = {last_price}")

    # outside band => do nothing
    if not (lower <= last_price <= upper):
        log.info(f"🟡 {symbol} outside band [{lower}, {upper}]")
        return {"symbol": symbol, "action": "none", "reason": "outside_band", "price": last_price}

    levels = build_geometric_levels(lower, upper, grid_pct)
    if not levels:
        return {"symbol": symbol, "action": "none", "reason": "no_levels"}

    open_orders = get_open_orders(symbol)

    used = capital_used(symbol, last_price, open_orders)
    pos_qty = get_position_qty(symbol)
    sell_reserved = open_sell_qty(open_orders)
    free_qty = max(0.0, pos_qty - sell_reserved)

    # Decide BUY level & SELL level
    buy_level = nearest_buy_level(levels, last_price)
    sell_level = nearest_sell_level(levels, last_price)

    # 1) Prefer SELL if we have inventory and a sell level exists and no open sell at that level
    if sell_level is not None:
        # quantity in shares (ETFs allow fractional? Alpaca usually supports whole shares for equities.
        # We'll use whole shares to be safe.)
        sell_qty = math.floor(order_usd / sell_level)
        if sell_qty > 0 and free_qty >= sell_qty:
            if not has_open_order_at(open_orders, "sell", sell_level):
                try:
                    o = place_limit(symbol, "sell", sell_qty, sell_level)
                    if o:
                        msg = f"🔴 SELL | {symbol}\nQty: {sell_qty} @ {sell_level}"
                        log.info(msg.replace("\n", " | "))
                        tg_send(msg)
                        return {"symbol": symbol, "action": "sell", "qty": sell_qty, "price": sell_level}
                except Exception as e:
                    msg = f"❌ SELL failed | {symbol} | {e}"
                    log.error(msg)
                    tg_send(msg)
                    return {"symbol": symbol, "action": "error", "reason": "sell_failed"}
        # else: cannot sell now

    # 2) BUY if within capital and no open buy at that level
    if buy_level is not None:
        buy_qty = math.floor(order_usd / buy_level)
        if buy_qty <= 0:
            return {"symbol": symbol, "action": "none", "reason": "buy_qty_zero"}

        projected = used + (buy_qty * buy_level)
        if projected > max_capital:
            log.info(f"🟠 {symbol} BUY blocked (capital) used≈${used:,.2f} projected≈${projected:,.2f} max=${max_capital:,.2f}")
            return {"symbol": symbol, "action": "none", "reason": "max_capital", "used": used, "price": last_price}

        if not has_open_order_at(open_orders, "buy", buy_level):
            try:
                o = place_limit(symbol, "buy", buy_qty, buy_level)
                if o:
                    msg = f"🟢 BUY | {symbol}\nQty: {buy_qty} @ {buy_level}"
                    log.info(msg.replace("\n", " | "))
                    tg_send(msg)
                    return {"symbol": symbol, "action": "buy", "qty": buy_qty, "price": buy_level}
            except Exception as e:
                msg = f"❌ BUY failed | {symbol} | {e}"
                log.error(msg)
                tg_send(msg)
                return {"symbol": symbol, "action": "error", "reason": "buy_failed"}

    # Nothing to do
    return {"symbol": symbol, "action": "none", "reason": "no_signal", "price": last_price}


# =======================
# ROUTES
# =======================
@app.route("/healthz", methods=["GET"])
def healthz():
    return "ok", 200


@app.route("/run", methods=["GET"])
def run():
    # Auth
    token = request.headers.get("X-RUN-TOKEN", "")
    if not RUN_TOKEN or token != RUN_TOKEN:
        return jsonify({"error": "Unauthorized /run attempt blocked"}), 401

    conn = pg_conn()
    try:
        if not acquire_global_lock(conn):
            # another run is in progress (cron retry etc.)
            return jsonify({"status": "already running"}), 200

        results = []
        for sym, cfg in BOTS.items():
            results.append(run_symbol(sym, cfg))

        # daily summary (fires only once/day when /run happens after summary hour)
        maybe_daily_summary(conn)

        conn.commit()
        return jsonify({"status": "ok", "results": results})
    except Exception as e:
        conn.rollback()
        msg = f"❌ /run crashed: {e}"
        log.exception(msg)
        tg_send(msg)
        return jsonify({"status": "error", "error": str(e)}), 500
    finally:
        try:
            release_global_lock(conn)
        except Exception:
            pass
        conn.close()


# Optional: simple Telegram webhook endpoint (read-only)
@app.route("/telegram", methods=["POST"])
def telegram_webhook():
    # This only replies; no trading commands.
    try:
        data = request.get_json(force=True, silent=True) or {}
        msg = (data.get("message") or {}).get("text") or ""
        chat_id = (data.get("message") or {}).get("chat") or {}
        chat_id = str(chat_id.get("id") or "")

        if not TELEGRAM_CHAT_ID or chat_id != str(TELEGRAM_CHAT_ID):
            return "ok", 200

        if msg.strip().lower() == "/status":
            lines = [f"🦁 Leo status | Paper={PAPER} | TradingEnabled={TRADING_ENABLED}"]
            for sym, cfg in BOTS.items():
                p = get_last_price(sym)
                pos = get_position_qty(sym)
                lines.append(f"{sym} price={p:.2f} pos={pos}")
            tg_send("\n".join(lines))
        return "ok", 200
    except Exception as e:
        log.warning(f"telegram webhook error: {e}")
        return "ok", 200


# =======================
# START
# =======================
if __name__ == "__main__":
    init_db()
    log.info(f"🦁 Leo started ({'Paper' if PAPER else 'Live'} Trading)")
    port = int(os.getenv("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
