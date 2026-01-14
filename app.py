# Leo Grid Bot v4.1.1
# Minimal fix: BUY-side reconcile fallback uses position truth when Alpaca parent BUY order is missing.

import os
import math
import logging
from datetime import datetime, timezone, date, timedelta
from decimal import Decimal, ROUND_HALF_UP

import requests
import psycopg2
from psycopg2.extras import RealDictCursor

from flask import Flask, request, jsonify

from alpaca.trading.client import TradingClient
from alpaca.trading.requests import GetOrdersRequest, LimitOrderRequest

# ✅ Bracket support (A = Take-profit + Stop-loss)
# alpaca-py versions vary a bit; these imports are the canonical ones.
try:
    from alpaca.trading.requests import TakeProfitRequest  # type: ignore
except Exception:
    TakeProfitRequest = None  # handled later

try:
    from alpaca.trading.requests import StopLossRequest  # type: ignore
except Exception:
    StopLossRequest = None  # handled later

try:
    from alpaca.trading.enums import OrderSide, TimeInForce, QueryOrderStatus, OrderClass  # type: ignore
except Exception:
    from alpaca.trading.enums import OrderSide, TimeInForce, QueryOrderStatus  # type: ignore
    OrderClass = None  # handled later

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
# GLOBAL "ONE ORDER" MODE
# =======================
GLOBAL_ONE_ORDER_AT_A_TIME = os.getenv("GLOBAL_ONE_ORDER_AT_A_TIME", "false").lower() == "true"
MAX_NEW_ORDERS_PER_RUN = int(os.getenv("MAX_NEW_ORDERS_PER_RUN", "1"))

# =======================
# BUY/SELL ON TOUCH (CROSSING) MODE
# =======================
TOUCH_MODE = os.getenv("TOUCH_MODE", "true").lower() == "true"

# =======================
# WIDE STOP LOSS CONFIG (AUTOMATED)
# =======================
# This sets the stop-loss *below* the buy price by a percentage.
# Example: STOP_LOSS_PCT=0.35 means stop at 35% below the buy (very wide).
STOP_LOSS_PCT = float(os.getenv("STOP_LOSS_PCT", "0.35"))
# Optional extra guard: never set stop above this fraction of lower band (keeps stop from being too high in some edge cases)
# If unset/empty, ignored.
STOP_LOSS_MIN_FACTOR_OF_LOWER = os.getenv("STOP_LOSS_MIN_FACTOR_OF_LOWER", "").strip()


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
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS bot_state (
                    k TEXT PRIMARY KEY,
                    v TEXT NOT NULL
                );
            """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS daily_equity (
                    day DATE PRIMARY KEY,
                    start_equity NUMERIC NOT NULL,
                    summary_sent BOOLEAN NOT NULL DEFAULT FALSE
                );
            """
            )
            # ✅ NEW: daily wins summary (count + pnl)
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS daily_wins (
                    day DATE PRIMARY KEY,
                    wins INTEGER NOT NULL DEFAULT 0,
                    wins_pnl NUMERIC NOT NULL DEFAULT 0,
                    summary_sent BOOLEAN NOT NULL DEFAULT FALSE
                );
            """
            )
            cur.execute(
                """
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
            """
            )

        conn.commit()
        log.info("✅ Postgres initialized")
    finally:
        conn.close()


# =======================
# BOT STATE HELPERS
# =======================
def db_get_state(conn, k: str, default: str = "") -> str:
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT v FROM bot_state WHERE k=%s;", (k,))
            row = cur.fetchone()
            if row and row.get("v") is not None:
                return str(row["v"])
    except Exception:
        pass
    return default


def db_set_state(conn, k: str, v: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO bot_state(k, v) VALUES(%s, %s)
            ON CONFLICT(k) DO UPDATE SET v=EXCLUDED.v;
        """,
            (k, str(v)),
        )


# =======================
# DAILY WINS HELPERS (NEW)
# =======================
def db_record_tp_win(conn, day: date, pnl_usd: float) -> None:
    """Accumulate TP wins and TP PnL for the day (UTC)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO daily_wins(day, wins, wins_pnl, summary_sent)
            VALUES(%s, 1, %s, false)
            ON CONFLICT(day) DO UPDATE SET
                wins = daily_wins.wins + 1,
                wins_pnl = daily_wins.wins_pnl + EXCLUDED.wins_pnl;
            """,
            (day, float(pnl_usd)),
        )


def db_get_daily_wins(conn, day: date):
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM daily_wins WHERE day=%s;", (day,))
        return cur.fetchone()


def db_mark_daily_wins_sent(conn, day: date) -> None:
    with conn.cursor() as cur:
        cur.execute("UPDATE daily_wins SET summary_sent=true WHERE day=%s;", (day,))


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
    req = GetOrdersRequest(status=QueryOrderStatus.OPEN, symbols=[symbol])
    return trading.get_orders(filter=req)


def get_recent_closed_orders(symbol: str, days: int = 14):
    try:
        after = now_utc() - timedelta(days=days)
        req = GetOrdersRequest(status=QueryOrderStatus.CLOSED, symbols=[symbol], after=after)
        return trading.get_orders(filter=req)
    except Exception as e:
        try:
            req = GetOrdersRequest(status=QueryOrderStatus.CLOSED, symbols=[symbol])
            return trading.get_orders(filter=req)
        except Exception:
            log.warning(f"{symbol} closed order fetch failed: {e}")
            return []


# =======================
# GLOBAL OPEN ORDERS + STRONGER ORDER LOOKUP
# =======================
def get_all_open_orders():
    try:
        req = GetOrdersRequest(status=QueryOrderStatus.OPEN)
        return trading.get_orders(filter=req) or []
    except Exception as e:
        log.warning(f"Global open order fetch failed: {e}")
        all_o = []
        for sym in BOTS.keys():
            try:
                all_o.extend(get_open_orders(sym))
            except Exception:
                pass
        return all_o


def get_order_by_id_safe(order_id: str):
    if not order_id:
        return None
    try:
        return trading.get_order_by_id(order_id)
    except Exception:
        return None


def order_status_string(order_obj) -> str:
    try:
        return str(getattr(order_obj, "status", "") or "").lower()
    except Exception:
        return ""


def order_filled_qty(order_obj) -> float:
    try:
        fq = getattr(order_obj, "filled_qty", None)
        if fq is None:
            return 0.0
        return float(fq)
    except Exception:
        return 0.0


def order_filled_avg_price(order_obj) -> float:
    """Best-effort filled average price from Alpaca order object."""
    try:
        p = getattr(order_obj, "filled_avg_price", None)
        if p is None or p == "":
            return 0.0
        return float(p)
    except Exception:
        return 0.0


# =======================
# TP/SL CLASSIFIER (NEW)
# =======================
def classify_sell_fill(order_obj) -> str:
    """
    Classifies a filled SELL leg.
    Returns:
      'tp' -> take-profit filled (usually LIMIT sell)
      'sl' -> stop-loss triggered (STOP / STOP_LIMIT)
      'sell' -> normal/unknown sell
    """
    try:
        typ = str(getattr(order_obj, "type", "") or "").lower()
        if "stop" in typ:
            return "sl"
        if typ == "limit":
            return "tp"
    except Exception:
        pass
    return "sell"


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


def next_level_above(levels, level: float):
    try:
        idx = levels.index(d2(level))
    except ValueError:
        return None
    if idx + 1 >= len(levels):
        return None
    return levels[idx + 1]


# =======================
# TOUCH/CROSSING HELPERS
# =======================
def pick_touched_buy_level(levels, prev_price: float, curr_price: float):
    if prev_price is None:
        return None
    if curr_price > prev_price:
        return None
    lo = float(curr_price)
    hi = float(prev_price)
    crossed = [lv for lv in levels if lo <= float(lv) <= hi]
    if not crossed:
        return None
    return max(crossed)


def pick_touched_sell_level(levels, prev_price: float, curr_price: float):
    if prev_price is None:
        return None
    if curr_price < prev_price:
        return None
    lo = float(prev_price)
    hi = float(curr_price)
    crossed = [lv for lv in levels if lo <= float(lv) <= hi]
    if not crossed:
        return None
    return min(crossed)


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
        time_in_force=TimeInForce.GTC,
    )
    return trading.submit_order(req)


def compute_wide_stop_price(buy_price: float, lower_band: float) -> float:
    """
    Automated wide stop:
      stop = buy * (1 - STOP_LOSS_PCT)
    Plus optional extra guard based on lower band.

    It must be strictly below buy_price (Alpaca requirement).
    """
    pct = float(STOP_LOSS_PCT)
    if pct < 0.01:
        pct = 0.01
    if pct > 0.95:
        pct = 0.95

    stop = buy_price * (1.0 - pct)

    if STOP_LOSS_MIN_FACTOR_OF_LOWER:
        try:
            factor = float(STOP_LOSS_MIN_FACTOR_OF_LOWER)
            if factor > 0:
                stop = min(stop, float(lower_band) * factor)
        except Exception:
            pass

    stop = d2(stop)

    if stop >= d2(buy_price):
        stop = d2(buy_price - MIN_TICK)

    if stop <= 0:
        stop = d2(max(MIN_TICK, buy_price * 0.01))

    return stop


# ✅ UPDATED: BRACKET BUY (TP + WIDE STOP-LOSS)
def place_bracket_buy(symbol: str, qty: float, buy_price: float, take_profit_price: float, stop_price: float):
    """
    Places one parent BUY limit + two children:
      - TAKE-PROFIT sell limit
      - STOP-LOSS sell stop

    Your Alpaca environment requires stop_loss.stop_price for bracket orders.
    We compute a wide automated stop so it's hard to hit.
    """
    if not TRADING_ENABLED:
        msg = f"⛔️ TRADING DISABLED | blocked BRACKET BUY {symbol}"
        log.info(msg)
        tg_send(msg)
        return None

    if TakeProfitRequest is None or StopLossRequest is None or OrderClass is None:
        raise RuntimeError(
            "Your installed alpaca-py does not expose TakeProfitRequest/StopLossRequest/OrderClass. Upgrade alpaca-py."
        )

    buy_price = d2(buy_price)
    take_profit_price = d2(take_profit_price)
    stop_price = d2(stop_price)

    if stop_price >= buy_price:
        stop_price = d2(buy_price - MIN_TICK)

    req = LimitOrderRequest(
        symbol=symbol,
        qty=qty,
        side=OrderSide.BUY,
        limit_price=buy_price,
        time_in_force=TimeInForce.GTC,
        order_class=OrderClass.BRACKET,
        take_profit=TakeProfitRequest(limit_price=take_profit_price),
        stop_loss=StopLossRequest(stop_price=stop_price),
    )
    return trading.submit_order(req)


def extract_take_profit_leg_id(order_obj) -> str:
    """
    Best-effort: Alpaca bracket parent may return legs.
    We try to find the SELL leg (take-profit) ID.
    """
    if not order_obj:
        return ""
    try:
        legs = getattr(order_obj, "legs", None) or []
        for leg in legs:
            try:
                side = str(getattr(leg, "side", "") or "").lower()
                if side == "sell":
                    return str(getattr(leg, "id", "") or "")
            except Exception:
                continue
    except Exception:
        pass
    return ""


# =======================
# GRID MEMORY (DB HELPERS)
# =======================
def db_list_lots(conn, symbol: str):
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM grid_lots WHERE symbol=%s;", (symbol,))
        return cur.fetchall() or []


def db_get_lot(conn, symbol: str, buy_level: float):
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM grid_lots WHERE symbol=%s AND buy_level=%s;", (symbol, d2(buy_level)))
        return cur.fetchone()


def db_upsert_lot(
    conn,
    symbol: str,
    buy_level: float,
    sell_level: float,
    qty: float,
    state: str,
    buy_order_id=None,
    sell_order_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
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
        """,
            (symbol, d2(buy_level), d2(sell_level), float(qty), state, buy_order_id, sell_order_id),
        )


def db_delete_lot(conn, symbol: str, buy_level: float):
    with conn.cursor() as cur:
        cur.execute("DELETE FROM grid_lots WHERE symbol=%s AND buy_level=%s;", (symbol, d2(buy_level)))


def db_get_seeded_lot_for_sell_level(conn, symbol: str, sell_level: float):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT *
            FROM grid_lots
            WHERE symbol=%s
              AND state='owned'
              AND sell_level=%s
              AND (buy_order_id IS NULL OR buy_order_id='')
            ORDER BY buy_level DESC
            LIMIT 1;
        """,
            (symbol, d2(sell_level)),
        )
        return cur.fetchone()


# =======================
# ROBUST RECONCILIATION (WITH PARTIAL-FILL + BRACKET-AWARE + TP/SL ALERTS + WIN TRACKING)
# =======================
def reconcile_lots(conn, symbol: str, open_orders):
    """
    Robust reconciliation:
    - If tracked order no longer open:
        - Prefer fetching by ID (truth)
        - Else use recent CLOSED list
        - If still unknown: DO NOT DELETE; keep and warn

    Partial-fill + cancel cases:
      - buy_open: if filled_qty > 0, becomes owned or sell_open depending on bracket leg existence
      - sell_open: if partially filled, reduce qty and revert to owned

    ✅ BRACKET-AWARE:
      - If buy_open order filled, and we can see a TP leg id, we transition straight to sell_open.
      - If sell_open leg fills, we delete lot as before.

    ✅ NEW:
      - Telegram alert when TP fills (with PnL)
      - Telegram alert when SL triggers (with PnL)
      - Daily wins counter + PnL accumulation for TP fills

    v4.1.1 fix:
      - If BUY parent order is missing from Alpaca but position exists, assume buy filled and promote lot.
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

    def lookup_order_truth(oid: str):
        if not oid:
            return None
        o = get_order_by_id_safe(str(oid))
        if o is not None:
            return o
        return closed_by_id.get(str(oid))

    def lot_qty_float(lot_row) -> float:
        try:
            return float(lot_row.get("qty") or 0.0)
        except Exception:
            return 0.0

    for lot in lots:
        state = lot["state"]
        buy_oid = lot.get("buy_order_id")
        sell_oid = lot.get("sell_order_id")

        # -------------------------
        # BUY open -> filled/canceled/unknown (WITH partial-fill truth)
        # -------------------------
        if state == "buy_open" and buy_oid and str(buy_oid) not in open_ids:
            o = lookup_order_truth(str(buy_oid))
            st = order_status_string(o) if o else ""
            fq = order_filled_qty(o) if o else 0.0

            if fq > 0.0:
                tp_leg_id = ""
                try:
                    tp_leg_id = extract_take_profit_leg_id(o)
                except Exception:
                    tp_leg_id = ""

                if tp_leg_id:
                    db_upsert_lot(
                        conn,
                        symbol,
                        float(lot["buy_level"]),
                        float(lot["sell_level"]),
                        float(fq),
                        "sell_open",
                        buy_order_id=str(buy_oid),
                        sell_order_id=str(tp_leg_id),
                    )
                    log.info(f"🧠 {symbol} reconcile: BUY filled_qty={fq} -> BRACKET TP leg detected -> sell_open")
                else:
                    db_upsert_lot(
                        conn,
                        symbol,
                        float(lot["buy_level"]),
                        float(lot["sell_level"]),
                        float(fq),
                        "owned",
                        buy_order_id=str(buy_oid),
                        sell_order_id=None,
                    )
                    if st and st != "filled":
                        log.warning(f"🟠 {symbol} reconcile: BUY {buy_oid} status={st} but filled_qty={fq} -> keeping as owned")

            elif st == "filled":
                fallback_qty = lot_qty_float(lot)
                tp_leg_id = ""
                try:
                    tp_leg_id = extract_take_profit_leg_id(o)
                except Exception:
                    tp_leg_id = ""

                if tp_leg_id:
                    db_upsert_lot(
                        conn,
                        symbol,
                        float(lot["buy_level"]),
                        float(lot["sell_level"]),
                        float(fallback_qty),
                        "sell_open",
                        buy_order_id=str(buy_oid),
                        sell_order_id=str(tp_leg_id),
                    )
                    log.info(f"🧠 {symbol} reconcile: BUY status=filled -> BRACKET TP leg -> sell_open")
                else:
                    db_upsert_lot(
                        conn,
                        symbol,
                        float(lot["buy_level"]),
                        float(lot["sell_level"]),
                        float(fallback_qty),
                        "owned",
                        buy_order_id=str(buy_oid),
                        sell_order_id=None,
                    )

            elif st in ("canceled", "cancelled", "rejected", "expired"):
                db_delete_lot(conn, symbol, float(lot["buy_level"]))

            # ✅ v4.1.1 FIX: position-truth fallback when Alpaca "forgets" the parent buy
            elif get_position_qty(symbol) > 0:
                expected_qty = lot_qty_float(lot)
                if expected_qty <= 0:
                    expected_qty = get_position_qty(symbol)

                tp_leg_id = ""
                try:
                    tp_leg_id = extract_take_profit_leg_id(o)
                except Exception:
                    pass

                if tp_leg_id:
                    db_upsert_lot(
                        conn,
                        symbol,
                        float(lot["buy_level"]),
                        float(lot["sell_level"]),
                        float(expected_qty),
                        "sell_open",
                        buy_order_id=str(buy_oid),
                        sell_order_id=str(tp_leg_id),
                    )
                else:
                    db_upsert_lot(
                        conn,
                        symbol,
                        float(lot["buy_level"]),
                        float(lot["sell_level"]),
                        float(expected_qty),
                        "owned",
                        buy_order_id=str(buy_oid),
                        sell_order_id=None,
                    )

                log.info(f"🧠 {symbol} reconcile fallback: BUY assumed filled via position qty={expected_qty}")

            else:
                log.warning(f"🟠 {symbol} reconcile: buy_open order {buy_oid} not open, truth unknown -> keeping lot")

        # -------------------------
        # SELL open -> filled/canceled/unknown (WITH partial-fill truth) + TP/SL ALERTS
        # -------------------------
        if state == "sell_open" and sell_oid and str(sell_oid) not in open_ids:
            o = lookup_order_truth(str(sell_oid))
            st = order_status_string(o) if o else ""
            fq = order_filled_qty(o) if o else 0.0
            lot_qty = lot_qty_float(lot)

            if st == "filled":
                # ✅ NEW: classify TP vs SL, compute PnL, alert Telegram, record daily wins on TP
                fill_type = classify_sell_fill(o)

                buy_level = float(lot["buy_level"])
                target_level = float(lot["sell_level"])
                qty = float(lot_qty)

                filled_px = order_filled_avg_price(o)
                sell_px = float(filled_px) if filled_px > 0 else float(target_level)

                pnl = (sell_px - buy_level) * qty

                if fill_type == "tp":
                    msg = (
                        f"🎉 TAKE-PROFIT FILLED | {symbol}\n"
                        f"Qty: {qty:g}\n"
                        f"Buy: {buy_level:.2f}\n"
                        f"Sell (TP): {sell_px:.2f}\n"
                        f"PnL: ${pnl:,.2f}"
                    )
                    tg_send(msg)
                    log.info(msg.replace("\n", " | "))

                    # record win stats (UTC day)
                    db_record_tp_win(conn, utc_day(), pnl)

                elif fill_type == "sl":
                    msg = (
                        f"🚨 STOP-LOSS HIT | {symbol}\n"
                        f"Qty: {qty:g}\n"
                        f"Buy: {buy_level:.2f}\n"
                        f"Exit (SL): {sell_px:.2f}\n"
                        f"PnL: ${pnl:,.2f}"
                    )
                    tg_send(msg)
                    log.warning(msg.replace("\n", " | "))

                else:
                    msg = f"ℹ️ SELL FILLED | {symbol}\nQty: {qty:g}\nExit: {sell_px:.2f}\nPnL: ${pnl:,.2f}"
                    tg_send(msg)

                db_delete_lot(conn, symbol, float(lot["buy_level"]))

            elif fq > 0.0:
                remaining = max(0.0, float(lot_qty) - float(fq))
                if remaining <= 0.0:
                    db_delete_lot(conn, symbol, float(lot["buy_level"]))
                else:
                    db_upsert_lot(
                        conn,
                        symbol,
                        float(lot["buy_level"]),
                        float(lot["sell_level"]),
                        float(remaining),
                        "owned",
                        buy_order_id=buy_oid,
                        sell_order_id=None,
                    )
                if st and st not in ("filled",):
                    log.warning(f"🟠 {symbol} reconcile: SELL {sell_oid} status={st} but filled_qty={fq} -> adjusted remaining={remaining}")

            elif st in ("canceled", "cancelled", "rejected", "expired"):
                db_upsert_lot(
                    conn,
                    symbol,
                    float(lot["buy_level"]),
                    float(lot["sell_level"]),
                    float(lot_qty),
                    "owned",
                    buy_order_id=buy_oid,
                    sell_order_id=None,
                )

            else:
                log.warning(f"🟠 {symbol} reconcile: sell_open order {sell_oid} not open, truth unknown -> keeping lot")


# =======================
# SIMPLE SEED (UNCHANGED)
# =======================
def seed_one_lot_if_needed(conn, symbol: str, levels, open_orders):
    try:
        existing_lots = db_list_lots(conn, symbol)
        if existing_lots:
            return
        if open_orders:
            return

        qty = get_position_qty(symbol)
        if qty <= 0.0:
            return

        try:
            pos = trading.get_open_position(symbol)
            avg_price = float(pos.avg_entry_price)
        except Exception:
            return

        if not levels:
            return

        buy_level = min(levels, key=lambda lv: abs(float(lv) - float(avg_price)))
        sell_target = next_level_above(levels, buy_level)
        if sell_target is None:
            return

        db_upsert_lot(conn, symbol, float(buy_level), float(sell_target), float(qty), "owned", buy_order_id=None, sell_order_id=None)
        log.info(f"🧩 Seeded {symbol} memory: owned lot at buy_level={buy_level} qty={qty} (avg_entry≈{avg_price:.2f})")
    except Exception as e:
        log.warning(f"{symbol} seed failed: {e}")


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
        # ---- equity summary (existing) ----
        cur.execute("SELECT * FROM daily_equity WHERE day=%s;", (day,))
        row = cur.fetchone()

        if row is None:
            cur.execute("INSERT INTO daily_equity(day, start_equity, summary_sent) VALUES(%s, %s, false);", (day, equity))
            conn.commit()
            # also ensure daily_wins exists
            cur.execute("INSERT INTO daily_wins(day, wins, wins_pnl, summary_sent) VALUES(%s, 0, 0, false) ON CONFLICT(day) DO NOTHING;", (day,))
            conn.commit()
            return

        if not row["summary_sent"]:
            start_equity = float(row["start_equity"])
            pnl = equity - start_equity

            text = f"📊 DAILY P&L (UTC {day})\nEquity: ${equity:,.2f}\nPnL: ${pnl:,.2f}"
            log.info(text.replace("\n", " | "))
            tg_send(text)

            cur.execute("UPDATE daily_equity SET summary_sent=true WHERE day=%s;", (day,))
            conn.commit()

        # ---- wins summary (NEW) ----
        wins_row = db_get_daily_wins(conn, day)
        if wins_row is None:
            cur.execute("INSERT INTO daily_wins(day, wins, wins_pnl, summary_sent) VALUES(%s, 0, 0, false);", (day,))
            conn.commit()
            return

        if not wins_row["summary_sent"]:
            wins = int(wins_row["wins"])
            wins_pnl = float(wins_row["wins_pnl"])

            msg = f"🏆 DAILY WINS (UTC {day})\nTP wins: {wins}\nTP PnL: ${wins_pnl:,.2f}"
            log.info(msg.replace("\n", " | "))
            tg_send(msg)

            db_mark_daily_wins_sent(conn, day)
            conn.commit()


# =======================
# CORE BOT
# =======================
def run_symbol(conn, symbol: str, cfg: dict, allow_new_order: bool = True):
    lower = float(cfg["lower"])
    upper = float(cfg["upper"])
    grid_pct = float(cfg["grid_pct"])
    order_usd = float(cfg["order_usd"])
    max_capital = float(cfg["max_capital"])

    state_key = f"last_price:{symbol}"

    def finish(res: dict, last_price_value: float = None):
        try:
            if last_price_value is not None:
                db_set_state(conn, state_key, str(float(last_price_value)))
        except Exception as e:
            log.warning(f"{symbol} state write failed: {e}")
        return res

    if lower >= upper:
        msg = f"⚠️ {symbol} config invalid: lower({lower}) must be < upper({upper})"
        log.error(msg)
        tg_send(msg)
        return finish({"symbol": symbol, "action": "error", "reason": "bad_config"})

    last_price = get_last_price(symbol)
    log.info(f"📈 {symbol} PRICE = {last_price}")

    prev_raw = db_get_state(conn, state_key, "")
    prev_price = None
    try:
        prev_price = float(prev_raw) if prev_raw != "" else None
    except Exception:
        prev_price = None

    if not (lower <= last_price <= upper):
        log.info(f"🟡 {symbol} outside band [{lower}, {upper}]")
        tg_send(f"🟡 {symbol} outside band [{lower}, {upper}] | price={last_price:.2f}")
        return finish({"symbol": symbol, "action": "none", "reason": "outside_band", "price": last_price}, last_price)

    levels = build_geometric_levels(lower, upper, grid_pct)
    if not levels:
        return finish({"symbol": symbol, "action": "none", "reason": "no_levels"}, last_price)

    open_orders = get_open_orders(symbol)

    can_place = bool(allow_new_order)

    seed_one_lot_if_needed(conn, symbol, levels, open_orders)
    reconcile_lots(conn, symbol, open_orders)

    used = capital_used(symbol, last_price, open_orders)
    pos_qty = get_position_qty(symbol)
    sell_reserved = open_sell_qty(open_orders)
    free_qty = max(0.0, pos_qty - sell_reserved)

    if TOUCH_MODE:
        buy_level = pick_touched_buy_level(levels, prev_price, last_price)
        sell_level = pick_touched_sell_level(levels, prev_price, last_price)
    else:
        buy_level = nearest_buy_level(levels, last_price)
        sell_level = nearest_sell_level(levels, last_price)

    if buy_level is not None and sell_level is not None:
        if float(sell_level) - float(buy_level) < MIN_TICK:
            log.info(f"🟡 {symbol} buy/sell too close (buy={buy_level}, sell={sell_level}), skipping")
            return finish({"symbol": symbol, "action": "none", "reason": "min_tick_guard", "price": last_price}, last_price)

    if not can_place:
        log.info(f"🧊 {symbol} new orders blocked (global/rate limit mode)")
        return finish({"symbol": symbol, "action": "none", "reason": "new_orders_blocked", "price": last_price}, last_price)

    # =========================
    # 1) SELL (for SEEDED positions only; bracket buys manage their own TP sells)
    # =========================
    if sell_level is not None:
        seeded_lot = db_get_seeded_lot_for_sell_level(conn, symbol, sell_level)
        if seeded_lot:
            seeded_qty = math.floor(float(seeded_lot["qty"]))
            sell_qty = min(seeded_qty, math.floor(free_qty))
            if sell_qty > 0 and not has_open_order_at(open_orders, "sell", sell_level):
                try:
                    o = place_limit(symbol, "sell", sell_qty, float(sell_level))
                    if o:
                        db_upsert_lot(
                            conn,
                            symbol,
                            float(seeded_lot["buy_level"]),
                            float(seeded_lot["sell_level"]),
                            sell_qty,
                            "sell_open",
                            buy_order_id=seeded_lot.get("buy_order_id"),
                            sell_order_id=str(o.id),
                        )
                        msg = f"🔴 SELL (SEEDED) | {symbol}\nQty: {sell_qty} @ {float(sell_level):.2f}"
                        log.info(msg.replace("\n", " | "))
                        tg_send(msg)
                        return finish({"symbol": symbol, "action": "sell", "qty": sell_qty, "price": float(sell_level), "seeded": True}, last_price)
                except Exception as e:
                    msg = f"❌ SELL failed | {symbol} | {e}"
                    log.error(msg)
                    tg_send(msg)
                    return finish({"symbol": symbol, "action": "error", "reason": "sell_failed"}, last_price)

    # =========================
    # 2) BUY (BRACKET: buy + take-profit + WIDE stop-loss)
    # =========================
    if buy_level is not None:
        buy_qty = math.floor(order_usd / float(buy_level))
        if buy_qty <= 0:
            return finish({"symbol": symbol, "action": "none", "reason": "buy_qty_zero"}, last_price)

        existing = db_get_lot(conn, symbol, float(buy_level))
        if existing and existing["state"] in ("buy_open", "owned", "sell_open"):
            log.info(f"🟡 {symbol} already has memory at buy_level={float(buy_level):.2f} (state={existing['state']}), skipping BUY")
            return finish({"symbol": symbol, "action": "none", "reason": "level_already_tracked", "buy_level": float(buy_level), "price": last_price}, last_price)

        projected = used + (buy_qty * float(buy_level))
        if projected > max_capital:
            log.info(f"🟠 {symbol} BUY blocked (capital) used≈${used:,.2f} projected≈${projected:,.2f} max=${max_capital:,.2f}")
            return finish({"symbol": symbol, "action": "none", "reason": "max_capital", "used": used, "price": last_price}, last_price)

        sell_target = next_level_above(levels, float(buy_level))
        if sell_target is None:
            return finish({"symbol": symbol, "action": "none", "reason": "no_sell_target"}, last_price)

        if float(sell_target) - float(buy_level) < MIN_TICK:
            return finish({"symbol": symbol, "action": "none", "reason": "min_tick_guard_bracket"}, last_price)

        if not has_open_order_at(open_orders, "buy", buy_level):
            try:
                stop_price = compute_wide_stop_price(float(buy_level), lower_band=lower)

                o = place_bracket_buy(symbol, buy_qty, float(buy_level), float(sell_target), float(stop_price))
                if o:
                    tp_leg_id = extract_take_profit_leg_id(o)

                    db_upsert_lot(
                        conn,
                        symbol,
                        float(buy_level),
                        float(sell_target),
                        buy_qty,
                        "buy_open",
                        buy_order_id=str(o.id),
                        sell_order_id=(tp_leg_id or None),
                    )

                    msg = (
                        f"🟢 BUY (BRACKET) | {symbol}\n"
                        f"Qty: {buy_qty} @ {float(buy_level):.2f}\n"
                        f"TP: {float(sell_target):.2f}\n"
                        f"SL (wide): {float(stop_price):.2f}  (pct={STOP_LOSS_PCT:.2%})"
                    )
                    log.info(msg.replace("\n", " | "))
                    tg_send(msg)

                    return finish(
                        {
                            "symbol": symbol,
                            "action": "buy",
                            "qty": buy_qty,
                            "price": float(buy_level),
                            "take_profit": float(sell_target),
                            "stop_loss": float(stop_price),
                            "stop_loss_pct": float(STOP_LOSS_PCT),
                            "bracket": True,
                        },
                        last_price,
                    )
            except Exception as e:
                msg = f"❌ BUY failed | {symbol} | {e}"
                log.error(msg)
                tg_send(msg)
                return finish({"symbol": symbol, "action": "error", "reason": "buy_failed"}, last_price)

    if TOUCH_MODE and prev_price is None:
        log.info(f"🧠 {symbol} touch-mode warmup: stored first last_price, no trade this run")
        return finish({"symbol": symbol, "action": "none", "reason": "touch_warmup", "price": last_price}, last_price)

    return finish({"symbol": symbol, "action": "none", "reason": "no_signal", "price": last_price}, last_price)


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

        global_open = get_all_open_orders()
        open_exists = len(global_open) > 0

        new_orders_left = MAX_NEW_ORDERS_PER_RUN

        allow_new = True
        if GLOBAL_ONE_ORDER_AT_A_TIME and open_exists:
            allow_new = False

        for sym, cfg in BOTS.items():
            res = run_symbol(conn, sym, cfg, allow_new_order=(allow_new and (new_orders_left > 0)))
            results.append(res)

            if res.get("action") in ("buy", "sell"):
                new_orders_left -= 1

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


@app.route("/board", methods=["GET"])
def board():
    token = request.headers.get("X-RUN-TOKEN", "")
    if RUN_TOKEN and token != RUN_TOKEN:
        return "Unauthorized", 401

    lines = [f"🦁 Leo Board | Paper={PAPER} | TradingEnabled={TRADING_ENABLED} | {now_utc().isoformat()}"]
    try:
        global_open = get_all_open_orders()
        lines.append(f"📌 Open orders (global): {len(global_open)}")
    except Exception:
        pass

    for sym in BOTS.keys():
        try:
            p = get_last_price(sym)
            pos = get_position_qty(sym)
            oo = get_open_orders(sym)
            lines.append(f"📈 {sym} | price={p:.2f} | pos={pos} | open_orders={len(oo)}")
        except Exception as e:
            lines.append(f"⚠️ {sym} board error: {e}")

    return "\n".join(lines), 200


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
            lines = [f"🦁 Leo status | Paper={PAPER} | TradingEnabled={TRADING_ENABLED} | SL_PCT={STOP_LOSS_PCT:.2%}"]
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
    log.info(f"🦁 Leo started v4.1.1 ({'Paper' if PAPER else 'Live'} Trading)")
    tg_send(f"🦁 Leo started v4.1.1 ({'Paper' if PAPER else 'Live'} Trading) | SL_PCT={STOP_LOSS_PCT:.2%}")
    port = int(os.getenv("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
