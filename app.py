import os
import math
import logging
from datetime import datetime, timezone, date, timedelta  # >>> NEW/CHANGED
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
    "GLD": {"lower": 403.28, "upper": 436.84, "grid_pct": 0.004, "order_usd": 2500, "max_capital": 30000},
    "SLV": {"lower": 70.80, "upper": 81.82, "grid_pct": 0.006, "order_usd": 2500, "max_capital": 30000},
    "MARA": {"lower": 9.0, "upper": 16.50, "grid_pct": 0.015, "order_usd": 2500, "max_capital": 30000},
    "MSTR": {"lower": 148.00, "upper": 177.00, "grid_pct": 0.015, "order_usd": 2500, "max_capital": 30000},
    "BTBT": {"lower": 1.87, "upper": 3.37, "grid_pct": 0.015, "order_usd": 2500, "max_capital": 30000},
    "RSP": {"lower": 191.15, "upper": 204.76, "grid_pct": 0.005, "order_usd": 2500, "max_capital": 30000},
    "GOOG": {"lower": 295.00, "upper": 360.96, "grid_pct": 0.005, "order_usd": 2500, "max_capital": 30000},
    "AAPL": {"lower": 250.00, "upper": 298.00, "grid_pct": 0.005, "order_usd": 2500, "max_capital": 30000},
    "MSFT": {"lower": 446.00, "upper": 526.00, "grid_pct": 0.005, "order_usd": 2500, "max_capital": 30000},
    "AMZN": {"lower": 200.00, "upper": 265.96, "grid_pct": 0.005, "order_usd": 2500, "max_capital": 30000},
}

MIN_TICK = 0.01  # 🔒 minimum price difference to avoid buy/sell at same level

PAPER = os.getenv("PAPER_TRADING", "true").lower() == "true"
TRADING_ENABLED = os.getenv("TRADING_ENABLED", "true").lower() == "true"

RUN_TOKEN = os.getenv("RUN_TOKEN", "")  # required header X-RUN-TOKEN
DATABASE_URL = os.getenv("DATABASE_URL")  # from Render Postgres

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

# Send daily summary after this UTC hour (0-23). (Example: 20 = 20:00 UTC)
DAILY_SUMMARY_HOUR_UTC = int(os.getenv("DAILY_SUMMARY_HOUR_UTC", "20"))


# =======================
# ALPACA KEYS
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

            # >>> NEW/CHANGED: grid memory table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS grid_lots (
                    symbol TEXT NOT NULL,
                    buy_level NUMERIC NOT NULL,
                    sell_level NUMERIC NOT NULL,
                    qty NUMERIC NOT NULL,
                    state TEXT NOT NULL, -- buy_open | owned | sell_open
                    buy_order_id TEXT,
                    sell_order_id TEXT,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    PRIMARY KEY(symbol, buy_level)
                );
            """)

        conn.commit()
        log.info("✅ Postgres initialized")
    finally:
        conn.close()


# =======================
# GLOBAL RUN LOCK
# =======================
def acquire_global_lock(conn) -> bool:
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
    req = GetOrdersRequest(
        status=QueryOrderStatus.OPEN,
        symbols=[symbol]
    )
    return trading.get_orders(filter=req)

# >>> NEW/CHANGED: helper to fetch recent CLOSED orders (to know if a previously-open order filled vs canceled)
def get_recent_closed_orders(symbol: str, days: int = 14):
    try:
        after = now_utc() - timedelta(days=days)
        req = GetOrdersRequest(
            status=QueryOrderStatus.CLOSED,
            symbols=[symbol],
            after=after  # alpaca-py supports datetime here; if your version errors, remove `after`
        )
        return trading.get_orders(filter=req)
    except Exception as e:
        # fallback if `after` isn't supported in your installed alpaca-py
        try:
            req = GetOrdersRequest(
                status=QueryOrderStatus.CLOSED,
                symbols=[symbol]
            )
            return trading.get_orders(filter=req)
        except Exception:
            log.warning(f"{symbol} closed order fetch failed: {e}")
            return []


# =======================
# GRID (GEOMETRIC)
# =======================
def build_geometric_levels(lower: float, upper: float, grid_pct: float):
    if lower <= 0 or upper <= 0 or grid_pct <= 0:
        return []

    levels = []
    p = lower
    for _ in range(5000):
        if p > upper + 1e-9:
            break
        levels.append(d2(p))
        p = p * (1.0 + grid_pct)
    return levels

def nearest_buy_level(levels, price: float):
    below = [lv for lv in levels if lv < price]
    return max(below) if below else None

def nearest_sell_level(levels, price: float):
    above = [lv for lv in levels if lv > price]
    return min(above) if above else None

# >>> NEW/CHANGED: next grid level above a specific level (deterministic sell target)
def next_level_above(levels, level: float):
    try:
        idx = levels.index(d2(level))
    except ValueError:
        # level not found (shouldn’t happen if we computed it from levels)
        return None
    if idx + 1 >= len(levels):
        return None
    return levels[idx + 1]


# =======================
# CAPITAL RULE
# =======================
def capital_used(symbol: str, last_price: float, open_orders):
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
# >>> NEW/CHANGED: GRID MEMORY (DB HELPERS)
# =======================
def db_list_lots(conn, symbol: str):
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM grid_lots WHERE symbol=%s;", (symbol,))
        return cur.fetchall() or []

def db_get_lot(conn, symbol: str, buy_level: float):
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM grid_lots WHERE symbol=%s AND buy_level=%s;", (symbol, d2(buy_level)))
        return cur.fetchone()

def db_upsert_lot(conn, symbol: str, buy_level: float, sell_level: float, qty: float, state: str, buy_order_id=None, sell_order_id=None):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO grid_lots(symbol, buy_level, sell_level, qty, state, buy_order_id, sell_order_id, updated_at)
            VALUES(%s,%s,%s,%s,%s,%s,%s,now())
            ON CONFLICT(symbol, buy_level)
            DO UPDATE SET
                sell_level=EXCLUDED.sell_level,
                qty=EXCLUDED.qty,
                state=EXCLUDED.state,
                buy_order_id=COALESCE(EXCLUDED.buy_order_id, grid_lots.buy_order_id),
                sell_order_id=COALESCE(EXCLUDED.sell_order_id, grid_lots.sell_order_id),
                updated_at=now();
        """, (symbol, d2(buy_level), d2(sell_level), float(qty), state, buy_order_id, sell_order_id))

def db_delete_lot(conn, symbol: str, buy_level: float):
    with conn.cursor() as cur:
        cur.execute("DELETE FROM grid_lots WHERE symbol=%s AND buy_level=%s;", (symbol, d2(buy_level)))

def reconcile_lots(conn, symbol: str, open_orders):
    """
    If an order we tracked as buy_open/sell_open is no longer open,
    check recent CLOSED orders to see if it filled or was canceled.
    """
    open_ids = set()
    for o in open_orders:
        try:
            open_ids.add(str(o.id))
        except Exception:
            pass

    lots = db_list_lots(conn, symbol)
    if not lots:
        return

    closed = get_recent_closed_orders(symbol)
    closed_by_id = {}
    for o in closed:
        try:
            closed_by_id[str(o.id)] = o
        except Exception:
            continue

    for lot in lots:
        state = lot["state"]
        buy_oid = lot.get("buy_order_id")
        sell_oid = lot.get("sell_order_id")

        # BUY open -> decide filled vs canceled
        if state == "buy_open" and buy_oid and str(buy_oid) not in open_ids:
            o = closed_by_id.get(str(buy_oid))
            if o and getattr(o, "status", "").lower() == "filled":
                db_upsert_lot(conn, symbol, float(lot["buy_level"]), float(lot["sell_level"]), float(lot["qty"]), "owned",
                              buy_order_id=str(buy_oid), sell_order_id=None)
            else:
                # not filled (canceled/rejected/expired/unknown) => forget it
                db_delete_lot(conn, symbol, float(lot["buy_level"]))

        # SELL open -> if filled, remove lot (level can be re-bought)
        if state == "sell_open" and sell_oid and str(sell_oid) not in open_ids:
            o = closed_by_id.get(str(sell_oid))
            if o and getattr(o, "status", "").lower() == "filled":
                db_delete_lot(conn, symbol, float(lot["buy_level"]))
            else:
                # sell didn't fill -> revert to owned
                db_upsert_lot(conn, symbol, float(lot["buy_level"]), float(lot["sell_level"]), float(lot["qty"]), "owned",
                              buy_order_id=buy_oid, sell_order_id=None)


# =======================
# DAILY SUMMARY
# =======================
def maybe_daily_summary(conn):
    day = utc_day()
    hour = now_utc().hour

    if hour < DAILY_SUMMARY_HOUR_UTC:
        return

    equity = get_account_equity()

    with conn.cursor() as cur:
        cur.execute("SELECT * FROM daily_equity WHERE day=%s;", (day,))
        row = cur.fetchone()

        if row is None:
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
# CORE BOT
# =======================
# >>> NEW/CHANGED: accept conn so we can read/write memory
def run_symbol(conn, symbol: str, cfg: dict):
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

    if not (lower <= last_price <= upper):
        log.info(f"🟡 {symbol} outside band [{lower}, {upper}]")
        tg_send(f"🟡 {symbol} outside band [{lower}, {upper}] | price={last_price:.2f}")
        return {"symbol": symbol, "action": "none", "reason": "outside_band", "price": last_price}

    levels = build_geometric_levels(lower, upper, grid_pct)
    if not levels:
        return {"symbol": symbol, "action": "none", "reason": "no_levels"}

    open_orders = get_open_orders(symbol)

    # >>> NEW/CHANGED: reconcile our DB memory with Alpaca order reality
    reconcile_lots(conn, symbol, open_orders)

    used = capital_used(symbol, last_price, open_orders)
    pos_qty = get_position_qty(symbol)
    sell_reserved = open_sell_qty(open_orders)
    free_qty = max(0.0, pos_qty - sell_reserved)

    buy_level = nearest_buy_level(levels, last_price)
    sell_level = nearest_sell_level(levels, last_price)

    if buy_level is not None and sell_level is not None:
        if sell_level - buy_level < MIN_TICK:
            log.info(f"🟡 {symbol} buy/sell too close (buy={buy_level}, sell={sell_level}), skipping")
            return {"symbol": symbol, "action": "none", "reason": "min_tick_guard", "price": last_price}

    # 1) SELL (only if we actually own the corresponding buy_level lot)
    if sell_level is not None:
        sell_qty = math.floor(order_usd / sell_level)
        if sell_qty > 0 and free_qty >= sell_qty:
            # the lot we should be selling is the grid step immediately below this sell_level
            source_buy = nearest_buy_level(levels, sell_level)
            if source_buy is not None:
                lot = db_get_lot(conn, symbol, source_buy)
                if lot and lot["state"] == "owned" and float(lot["qty"]) >= sell_qty:
                    if not has_open_order_at(open_orders, "sell", sell_level):
                        try:
                            o = place_limit(symbol, "sell", sell_qty, sell_level)
                            if o:
                                # mark that lot as being sold
                                db_upsert_lot(
                                    conn, symbol,
                                    float(lot["buy_level"]),
                                    float(lot["sell_level"]),
                                    sell_qty,
                                    "sell_open",
                                    buy_order_id=lot.get("buy_order_id"),
                                    sell_order_id=str(o.id)
                                )
                                msg = f"🔴 SELL | {symbol}\nQty: {sell_qty} @ {sell_level}"
                                log.info(msg.replace("\n", " | "))
                                tg_send(msg)
                                return {"symbol": symbol, "action": "sell", "qty": sell_qty, "price": sell_level, "from_buy_level": float(lot["buy_level"])}
                        except Exception as e:
                            msg = f"❌ SELL failed | {symbol} | {e}"
                            log.error(msg)
                            tg_send(msg)
                            return {"symbol": symbol, "action": "error", "reason": "sell_failed"}
            # else: no matching owned lot => skip selling this step

    # 2) BUY (only if we do NOT already own / have pending at that buy_level)
    if buy_level is not None:
        buy_qty = math.floor(order_usd / buy_level)
        if buy_qty <= 0:
            return {"symbol": symbol, "action": "none", "reason": "buy_qty_zero"}

        # >>> NEW/CHANGED: DB-based "memory" check (this is the real fix)
        existing = db_get_lot(conn, symbol, buy_level)
        if existing and existing["state"] in ("buy_open", "owned", "sell_open"):
            log.info(f"🟡 {symbol} already has memory at buy_level={buy_level} (state={existing['state']}), skipping BUY")
            return {"symbol": symbol, "action": "none", "reason": "level_already_tracked", "buy_level": buy_level, "price": last_price}

        projected = used + (buy_qty * buy_level)
        if projected > max_capital:
            log.info(
                f"🟠 {symbol} BUY blocked (capital) used≈${used:,.2f} projected≈${projected:,.2f} max=${max_capital:,.2f}"
            )
            return {"symbol": symbol, "action": "none", "reason": "max_capital", "used": used, "price": last_price}

        if not has_open_order_at(open_orders, "buy", buy_level):
            try:
                o = place_limit(symbol, "buy", buy_qty, buy_level)
                if o:
                    sell_target = next_level_above(levels, buy_level)
                    if sell_target is None:
                        # no next level to sell into; don't buy
                        return {"symbol": symbol, "action": "none", "reason": "no_sell_target"}

                    # >>> NEW/CHANGED: write memory (pending buy)
                    db_upsert_lot(
                        conn, symbol,
                        buy_level,
                        sell_target,
                        buy_qty,
                        "buy_open",
                        buy_order_id=str(o.id),
                        sell_order_id=None
                    )

                    msg = f"🟢 BUY | {symbol}\nQty: {buy_qty} @ {buy_level}"
                    log.info(msg.replace("\n", " | "))
                    tg_send(msg)
                    return {"symbol": symbol, "action": "buy", "qty": buy_qty, "price": buy_level}
            except Exception as e:
                msg = f"❌ BUY failed | {symbol} | {e}"
                log.error(msg)
                tg_send(msg)
                return {"symbol": symbol, "action": "error", "reason": "buy_failed"}

    return {"symbol": symbol, "action": "none", "reason": "no_signal", "price": last_price}


# =======================
# ROUTES
# =======================
@app.route("/healthz", methods=["GET"])
def healthz():
    return "ok", 200


@app.route("/run", methods=["GET"])
def run():
    token = request.headers.get("X-RUN-TOKEN", "")
    if not RUN_TOKEN or token != RUN_TOKEN:
        return jsonify({"error": "Unauthorized /run attempt blocked"}), 401

    conn = pg_conn()
    try:
        if not acquire_global_lock(conn):
            return jsonify({"status": "already running"}), 200

        results = []
        for sym, cfg in BOTS.items():
            # >>> NEW/CHANGED: pass conn
            results.append(run_symbol(conn, sym, cfg))

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


@app.route("/telegram", methods=["POST"])
def telegram_webhook():
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
