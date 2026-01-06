import os
import math
import sqlite3
import logging
from datetime import datetime, timezone
from flask import Flask, request, jsonify

from alpaca.trading.client import TradingClient
from alpaca.trading.requests import LimitOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockLatestTradeRequest

# =========================
# LOGGING (RENDER SAFE)
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger(__name__)

# =========================
# CONFIG (EDIT ONLY THIS)
# =========================
SYMBOL = "GLD"
LOWER_BAND = 380
UPPER_BAND = 430
GRID_PERCENT = 0.006
ORDER_USD = 500
MAX_CAPITAL = 10000

PAPER_TRADING = True
DB_FILE = f"gridbot_{SYMBOL}.db"

# =========================
# ALPACA CLIENTS
# =========================
ALPACA_KEY = os.getenv("ALPACA_KEY")
ALPACA_SECRET = os.getenv("ALPACA_SECRET")

if not ALPACA_KEY or not ALPACA_SECRET:
    raise RuntimeError("Missing ALPACA_KEY or ALPACA_SECRET")

trading = TradingClient(ALPACA_KEY, ALPACA_SECRET, paper=PAPER_TRADING)
data_client = StockHistoricalDataClient(ALPACA_KEY, ALPACA_SECRET)

app = Flask(__name__)
# =========================
# HEALTH CHECK (RENDER)
# =========================
@app.route("/healthz", methods=["GET"])
def healthz():
    return "ok", 200

# =========================
# DATABASE
# =========================
def db():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = db()
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS lots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT,
        buy_order_id TEXT,
        buy_status TEXT,
        buy_limit_price REAL,
        buy_filled_price REAL,
        qty INTEGER,
        buy_created_at TEXT,
        sell_order_id TEXT,
        sell_status TEXT,
        sell_limit_price REAL,
        sell_filled_price REAL,
        sell_created_at TEXT
    )
    """)
    conn.commit()
    conn.close()
    log.info("Database initialized")

def now():
    return datetime.now(timezone.utc).isoformat()

def round_price(p):
    return round(p, 2)

# =========================
# PRICE
# =========================
def get_price(symbol):
    req = StockLatestTradeRequest(symbol_or_symbols=symbol)
    trade = data_client.get_stock_latest_trade(req)[symbol]
    price = float(trade.price)

    log.info(f"{symbol} | Last price fetched: {price}")

    return price


# =========================
# ORDER SYNC
# =========================
def sync_orders():
    conn = db()
    cur = conn.cursor()

    cur.execute("SELECT * FROM lots WHERE buy_status='BUY_SUBMITTED'")
    for r in cur.fetchall():
        try:
            o = trading.get_order_by_id(r["buy_order_id"])
            if o.status == "filled":
                cur.execute(
                    "UPDATE lots SET buy_status='BOUGHT', buy_filled_price=? WHERE id=?",
                    (float(o.filled_avg_price), r["id"])
                )
                log.info(f"BUY FILLED @ {o.filled_avg_price}")
        except:
            pass

    cur.execute("SELECT * FROM lots WHERE sell_status='SELL_SUBMITTED'")
    for r in cur.fetchall():
        try:
            o = trading.get_order_by_id(r["sell_order_id"])
            if o.status == "filled":
                cur.execute(
                    "UPDATE lots SET sell_status='SOLD', sell_filled_price=? WHERE id=?",
                    (float(o.filled_avg_price), r["id"])
                )
                log.info(f"SELL FILLED @ {o.filled_avg_price}")
        except:
            pass

    conn.commit()
    conn.close()

def deployed_capital():
    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT COALESCE(SUM(buy_filled_price * qty), 0) FROM lots WHERE buy_status='BOUGHT' AND sell_status IS NULL")
    used = cur.fetchone()[0]
    cur.execute("SELECT COALESCE(SUM(buy_limit_price * qty), 0) FROM lots WHERE buy_status='BUY_SUBMITTED'")
    reserved = cur.fetchone()[0]
    conn.close()
    return used + reserved

# =========================
# MAIN CYCLE
# =========================
def run_cycle():
    price = get_price(SYMBOL)
    log.info(f"Price: {price}")

    sync_orders()

    conn = db()
    cur = conn.cursor()

    # SELL
    cur.execute("SELECT * FROM lots WHERE buy_status='BOUGHT' AND (sell_status IS NULL OR sell_status!='SOLD')")
    for r in cur.fetchall():
        target = round_price(r["buy_filled_price"] * (1 + GRID_PERCENT))
        if price >= target:
            try:
                o = trading.submit_order(
                    LimitOrderRequest(
                        symbol=SYMBOL,
                        qty=r["qty"],
                        side=OrderSide.SELL,
                        limit_price=target,
                        time_in_force=TimeInForce.DAY
                    )
                )
                cur.execute(
                    "UPDATE lots SET sell_status='SELL_SUBMITTED', sell_order_id=?, sell_limit_price=?, sell_created_at=? WHERE id=?",
                    (o.id, target, now(), r["id"])
                )
                log.info(f"SELL ORDER @ {target}")
            except:
                pass

    capital = deployed_capital()

    if LOWER_BAND <= price <= UPPER_BAND and capital + ORDER_USD <= MAX_CAPITAL:
        cur.execute("SELECT buy_filled_price FROM lots WHERE buy_status='BOUGHT' ORDER BY id DESC LIMIT 1")
        row = cur.fetchone()
        anchor = row[0] if row else price
        buy_price = round_price(anchor * (1 - GRID_PERCENT))

        if price <= buy_price:
            qty = int(ORDER_USD // buy_price)
            if qty > 0:
                try:
                    o = trading.submit_order(
                        LimitOrderRequest(
                            symbol=SYMBOL,
                            qty=qty,
                            side=OrderSide.BUY,
                            limit_price=buy_price,
                            time_in_force=TimeInForce.DAY
                        )
                    )
                    cur.execute(
                        "INSERT INTO lots (symbol, buy_order_id, buy_status, buy_limit_price, qty, buy_created_at) VALUES (?, ?, 'BUY_SUBMITTED', ?, ?, ?)",
                        (SYMBOL, o.id, buy_price, qty, now())
                    )
                    log.info(f"BUY ORDER @ {buy_price}")
                except:
                    pass

    conn.commit()
    conn.close()

# =========================
# ROUTES
# =========================
@app.route("/run")
def run():
    run_cycle()
    return jsonify({"status": "ok", "symbol": SYMBOL})

# =========================
# START
# =========================
if __name__ == "__main__":
    init_db()
    port = int(os.getenv("PORT", 10000))
    log.info("Bot started")
    app.run(host="0.0.0.0", port=port)
