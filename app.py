# Leo Grid Bot v4.2.1_PATCH_2026-01-16
# Updates in this patched version:
#   1) Live-mode hardening:
#        - RUN_TOKEN is REQUIRED when PAPER_TRADING=false (live)
#        - /board also requires token in live
#        - Live-mode file logging to Render Disk (/var/data by default)
#   2) Logging:
#        - Stream + Rotating file logs (when enabled)
#        - More detailed reconcile + order lifecycle logs
#   3) DB initialization/migrations:
#        - Adds ALTER TABLE ... ADD COLUMN IF NOT EXISTS for all known columns
#   4) Reconcile logic:
#        - Safer BUY "position-truth" fallback: only promotes when there is enough
#          *untracked* position qty for that lot, and only once per run per symbol.
#   5) Stop-loss default widened to 60% (STOP_LOSS_PCT default = 0.60)
#
# Notes:
#   - Per-symbol max_capital is preserved (no global account cap added).
#   - This bot uses Alpaca bracket orders: BUY parent + TP limit sell + SL stop.

import os
import math
import logging
from logging.handlers import RotatingFileHandler
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
# CONFIG (EDIT THIS)
# =======================
BOTS = {
    "GLD": {"lower": 404.66, "upper": 440.74, "grid_pct": 0.0026, "order_usd": 2000, "max_capital": 26000},
    "SLV": {"lower": 67.74, "upper": 92.06, "grid_pct": 0.0026, "order_usd": 2000, "max_capital": 26000},
}

MIN_TICK = 0.01  # 🔒 minimum price difference to avoid buy/sell at same level


# =======================
# QTY PRECISION / FRACTIONALS
# =======================
# Decision:
#   - BUY: whole shares only (stable grid sizing)
#   - SELL: allow fractional (handles partial fills / seeded positions that may become fractional)
BUY_WHOLE_SHARES = os.getenv("BUY_WHOLE_SHARES", "true").lower() == "true"
ALLOW_FRACTIONAL_SELLS = os.getenv("ALLOW_FRACTIONAL_SELLS", "true").lower() == "true"

# Qty step used when we allow fractional sells. Alpaca commonly supports 0.0001 share increments for fractionals.
QTY_STEP = Decimal(os.getenv("QTY_STEP", "0.0001"))


# =======================
# ENV / MODES
# =======================
PAPER = os.getenv("PAPER_TRADING", "true").lower() == "true"
TRADING_ENABLED = os.getenv("TRADING_ENABLED", "true").lower() == "true"

# Required header X-RUN-TOKEN for /run (and /board in live)
RUN_TOKEN = os.getenv("RUN_TOKEN", "")

# Render Postgres connection string
DATABASE_URL = os.getenv("DATABASE_URL")

# Telegram
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

# Reporting-only baseline capital (never used for trading decisions)
BASELINE_CAPITAL = float(os.getenv("BASELINE_CAPITAL", "50000"))

# Send daily summary after this UTC hour (0-23). (Example: 20 = 20:00 UTC)
DAILY_SUMMARY_HOUR_UTC = int(os.getenv("DAILY_SUMMARY_HOUR_UTC", "20"))


# =======================
# HEALTH / ALERT CONFIG
# =======================
LOT_STUCK_MINUTES = int(os.getenv("LOT_STUCK_MINUTES", "180"))
HEALTH_RECENT_ERROR_HOURS = int(os.getenv("HEALTH_RECENT_ERROR_HOURS", "24"))
HEALTH_RECENT_AUTOHEAL_HOURS = int(os.getenv("HEALTH_RECENT_AUTOHEAL_HOURS", "24"))
HEALTH_RECENT_CAPSAT_HOURS = int(os.getenv("HEALTH_RECENT_CAPSAT_HOURS", "6"))
HEALTH_ALERT_COOLDOWN_MINUTES = int(os.getenv("HEALTH_ALERT_COOLDOWN_MINUTES", "60"))


# =======================
# GLOBAL "ONE ORDER" MODE (kept)
# =======================
GLOBAL_ONE_ORDER_AT_A_TIME = os.getenv("GLOBAL_ONE_ORDER_AT_A_TIME", "false").lower() == "true"
MAX_NEW_ORDERS_PER_RUN = int(os.getenv("MAX_NEW_ORDERS_PER_RUN", "1"))


# =======================
# BUY/SELL ON TOUCH (CROSSING) MODE
# =======================
TOUCH_MODE = os.getenv("TOUCH_MODE", "true").lower() == "true"


# =======================
# WIDE STOP LOSS CONFIG (DEFAULT NOW 60%)
# =======================
# This sets the stop-loss *below* the buy price by a percentage.
# Example: STOP_LOSS_PCT=0.60 means stop at 60% below the buy (very wide).
STOP_LOSS_PCT = float(os.getenv("STOP_LOSS_PCT", "0.60"))

# Optional extra guard: never set stop above this fraction of lower band
# If unset/empty, ignored.
STOP_LOSS_MIN_FACTOR_OF_LOWER = os.getenv("STOP_LOSS_MIN_FACTOR_OF_LOWER", "").strip()


# =======================
# LOGGING (Render Disk)
# =======================
# Render Persistent Disk commonly mounts at /var/data.
# Set LOG_DIR to your disk path (Render: /var/data) to persist logs across deploys.
LOG_DIR = os.getenv("LOG_DIR", "/var/data")
LOG_TO_FILE = os.getenv("LOG_TO_FILE", "true").lower() == "true"
LOG_FILE_NAME = os.getenv("LOG_FILE_NAME", "leo_grid_bot.log")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()


def _configure_logging() -> logging.Logger:
    logger = logging.getLogger("leo")
    logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    # Always log to stdout (Render captures this)
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    # In LIVE mode, we strongly prefer file logs as well.
    if LOG_TO_FILE and (not PAPER or os.getenv("FORCE_FILE_LOG", "false").lower() == "true"):
        try:
            os.makedirs(LOG_DIR, exist_ok=True)
            fp = os.path.join(LOG_DIR, LOG_FILE_NAME)
            fh = RotatingFileHandler(fp, maxBytes=5_000_000, backupCount=5)
            fh.setFormatter(fmt)
            logger.addHandler(fh)
            logger.info(f"📝 File logging enabled: {fp}")
        except Exception as e:
            # Do not crash for logging issues; fall back to stdout.
            logger.warning(f"File logging could not be enabled: {e}")

    logger.propagate = False
    return logger


log = _configure_logging()


# =======================
# ALPACA KEYS
# =======================
ALPACA_KEY = os.getenv("ALPACA_API_KEY") or os.getenv("ALPACA_KEY")
ALPACA_SECRET = os.getenv("ALPACA_SECRET_KEY") or os.getenv("ALPACA_SECRET")

if not ALPACA_KEY or not ALPACA_SECRET:
    raise RuntimeError("Missing Alpaca keys. Set ALPACA_API_KEY + ALPACA_SECRET_KEY (or ALPACA_KEY + ALPACA_SECRET).")

if not DATABASE_URL:
    raise RuntimeError("Missing DATABASE_URL. Add Render Postgres and connect it to this service.")

# ✅ Force RUN_TOKEN in LIVE mode
if not PAPER and not RUN_TOKEN:
    raise RuntimeError("LIVE mode detected (PAPER_TRADING=false) but RUN_TOKEN is missing. Set RUN_TOKEN before going live.")

trading = TradingClient(ALPACA_KEY, ALPACA_SECRET, paper=PAPER)
data_client = StockHistoricalDataClient(ALPACA_KEY, ALPACA_SECRET)

app = Flask(__name__)


# =======================
# SMALL UTILITIES
# =======================

def d2(x: float) -> float:
    """Round price to 2 decimals."""
    return float(Decimal(str(x)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def normalize_qty(qty: float, *, allow_fractional: bool) -> float:
    """Normalize quantity for order submission.

    - If allow_fractional=False: return whole shares (floor) and never negative.
    - If allow_fractional=True: quantize DOWN to QTY_STEP.

    Important: we always round DOWN to avoid over-selling.
    """
    try:
        q = Decimal(str(qty))
    except Exception:
        return 0.0

    if q <= 0:
        return 0.0

    if not allow_fractional:
        return float(int(q))

    step = QTY_STEP if QTY_STEP > 0 else Decimal("0.0001")
    q = (q / step).to_integral_value(rounding="ROUND_FLOOR") * step
    return float(q)


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


# =======================
# TELEGRAM OUTBOX (EXACTLY-ONCE) + PNL PERSISTENCE
# =======================

# Notes:
# - We keep tg_send() as the low-level sender for non-critical/legacy alerts.
# - For lot-closing alerts and strategy summaries we use a DB-backed outbox
#   (telegram_events) to guarantee exactly-once even across restarts.

def tg_enqueue_event(conn, event_id: str, kind: str, text: str) -> bool:
    """Insert an event into telegram_events.

    Returns True if inserted (i.e., not already present).
    """
    if not tg_enabled() or not event_id:
        return False
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO telegram_events(event_id, kind, text, sent)
            VALUES(%s, %s, %s, false)
            ON CONFLICT(event_id) DO NOTHING;
            """,
            (event_id, kind, text),
        )
        return cur.rowcount == 1


def tg_flush_outbox(conn, limit: int = 25) -> None:
    """Attempt to send unsent telegram events."""
    if not tg_enabled():
        return
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT event_id, text
                FROM telegram_events
                WHERE sent=false
                ORDER BY created_at ASC
                LIMIT %s;
                """,
                (int(limit),),
            )
            rows = cur.fetchall() or []

        for r in rows:
            eid = str(r.get('event_id') or '')
            msg = str(r.get('text') or '')
            if not eid or not msg:
                continue
            try:
                tg_send(msg)
                with conn.cursor() as cur2:
                    cur2.execute(
                        "UPDATE telegram_events SET sent=true, sent_at=now() WHERE event_id=%s AND sent=false;",
                        (eid,),
                    )
                conn.commit()
            except Exception as e:
                # Leave unsent for next run
                conn.rollback()
                log.warning(f"Telegram outbox send failed for {eid}: {e}")
                break
    except Exception as e:
        log.warning(f"Telegram outbox flush failed: {e}")


def strategy_get_cum_realized_pnl(conn) -> float:
    return db_get_state_float(conn, 'strategy:cumulative_realized_pnl_usd', 0.0)


def strategy_add_realized_pnl(conn, delta_usd: float) -> float:
    curr = strategy_get_cum_realized_pnl(conn)
    curr = float(curr) + float(delta_usd)
    db_set_state(conn, 'strategy:cumulative_realized_pnl_usd', str(curr))
    return curr


def build_strategy_performance_text(conn) -> str:
    baseline = float(BASELINE_CAPITAL) if float(BASELINE_CAPITAL) > 0 else 0.0
    cum = strategy_get_cum_realized_pnl(conn)
    net = baseline + cum
    pct = (cum / baseline * 100.0) if baseline > 0 else 0.0
    return (
        'STRATEGY PERFORMANCE (Realized)\n'
        f'Baseline: ${baseline:,.2f}\n'
        f'Cumulative realized PnL: ${cum:,.2f}\n'
        f'Net strategy equity: ${net:,.2f}\n'
        f'Return vs baseline: {pct:+.2f}%'
    )


def tg_send_strategy_performance(conn, reason: str = '') -> None:
    eid = f"strategy_perf:{utc_day().isoformat()}:{reason or 'manual'}"
    txt = build_strategy_performance_text(conn)
    tg_enqueue_event(conn, eid, 'strategy', txt)


def db_mark_lot_closed_once(
    conn,
    symbol: str,
    buy_level: float,
    outcome: str,
    qty: float,
    entry_px: float,
    exit_px: float,
    pnl_usd: float,
    pnl_pct: float,
    close_order_id: str = '',
) -> bool:
    """Persist a lot closure record. Returns True only the first time."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO lot_closures(symbol, buy_level, outcome, qty, entry_px, exit_px, pnl_usd, pnl_pct, close_order_id, closed_at)
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now())
            ON CONFLICT(symbol, buy_level) DO NOTHING;
            """,
            (
                symbol,
                d2(float(buy_level)),
                str(outcome),
                float(qty),
                float(entry_px),
                float(exit_px),
                float(pnl_usd),
                float(pnl_pct),
                str(close_order_id or ''),
            ),
        )
        return cur.rowcount == 1
def pg_conn():
    conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
    conn.autocommit = False
    return conn


# =======================
# POSTGRES INIT + MIGRATIONS
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
                CREATE TABLE IF NOT EXISTS telegram_events (
                    event_id TEXT PRIMARY KEY,
                    kind TEXT NOT NULL,
                    text TEXT NOT NULL,
                    sent BOOLEAN NOT NULL DEFAULT FALSE,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    sent_at TIMESTAMPTZ
                );
            """
            )

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS lot_closures (
                    symbol TEXT NOT NULL,
                    buy_level NUMERIC NOT NULL,
                    outcome TEXT NOT NULL,
                    qty NUMERIC NOT NULL,
                    entry_px NUMERIC NOT NULL,
                    exit_px NUMERIC NOT NULL,
                    pnl_usd NUMERIC NOT NULL,
                    pnl_pct NUMERIC NOT NULL,
                    close_order_id TEXT,
                    closed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    PRIMARY KEY(symbol, buy_level)
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
                    tp_order_id TEXT,
                    sl_order_id TEXT,
                    buy_fill_px NUMERIC,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    PRIMARY KEY(symbol, buy_level)
                );
            """
            )

            # ✅ Backwards-compatible migrations for existing databases
            # (CREATE TABLE IF NOT EXISTS does NOT add missing columns.)
            cur.execute("ALTER TABLE grid_lots ADD COLUMN IF NOT EXISTS buy_order_id TEXT;")
            cur.execute("ALTER TABLE grid_lots ADD COLUMN IF NOT EXISTS sell_order_id TEXT;")
            cur.execute("ALTER TABLE grid_lots ADD COLUMN IF NOT EXISTS tp_order_id TEXT;")
            cur.execute("ALTER TABLE grid_lots ADD COLUMN IF NOT EXISTS sl_order_id TEXT;")
            cur.execute("ALTER TABLE grid_lots ADD COLUMN IF NOT EXISTS buy_fill_px NUMERIC;")
            cur.execute("ALTER TABLE grid_lots ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();")

            # If someone created a minimal grid_lots table, ensure required columns exist.
            cur.execute("ALTER TABLE grid_lots ADD COLUMN IF NOT EXISTS symbol TEXT;")
            cur.execute("ALTER TABLE grid_lots ADD COLUMN IF NOT EXISTS buy_level NUMERIC;")
            cur.execute("ALTER TABLE grid_lots ADD COLUMN IF NOT EXISTS sell_level NUMERIC;")
            cur.execute("ALTER TABLE grid_lots ADD COLUMN IF NOT EXISTS qty NUMERIC;")
            cur.execute("ALTER TABLE grid_lots ADD COLUMN IF NOT EXISTS state TEXT;")

        conn.commit()
        log.info("✅ Postgres initialized + migrations applied")
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
# HEALTH / EVENTS
# =======================

def _iso_now() -> str:
    return now_utc().isoformat()


def _parse_iso(ts: str) -> datetime:
    try:
        return datetime.fromisoformat(ts)
    except Exception:
        return datetime.fromtimestamp(0, tz=timezone.utc)


def db_get_state_int(conn, k: str, default: int = 0) -> int:
    raw = db_get_state(conn, k, "")
    try:
        return int(raw)
    except Exception:
        return int(default)


def db_get_state_float(conn, k: str, default: float = 0.0) -> float:
    raw = db_get_state(conn, k, "")
    try:
        return float(raw)
    except Exception:
        return float(default)


def db_inc_state(conn, k: str, delta: int = 1) -> int:
    curr = db_get_state_int(conn, k, 0)
    curr += int(delta)
    db_set_state(conn, k, str(curr))
    return curr


def _recent_within_hours(ts_iso: str, hours: int) -> bool:
    if not ts_iso:
        return False
    try:
        t = _parse_iso(ts_iso)
        return (now_utc() - t) <= timedelta(hours=float(hours))
    except Exception:
        return False


def record_auto_heal(conn, symbol: str, detail: str) -> None:
    db_inc_state(conn, "health:auto_heal_count", 1)
    db_set_state(conn, "health:auto_heal_last_ts", _iso_now())
    db_set_state(conn, "health:auto_heal_last", f"{symbol}:{detail}")


def record_capital_saturation(conn, symbol: str, detail: str) -> None:
    db_inc_state(conn, "health:cap_sat_count", 1)
    db_set_state(conn, "health:cap_sat_last_ts", _iso_now())
    db_set_state(conn, "health:cap_sat_last", f"{symbol}:{detail}")


def record_error(conn, detail: str) -> None:
    db_inc_state(conn, "health:error_count", 1)
    db_set_state(conn, "health:error_last_ts", _iso_now())
    db_set_state(conn, "health:error_last", str(detail)[:500])

# =======================
# ALERT AGGREGATION (HOURLY)
# =======================

def _hour_bucket_key(prefix: str) -> str:
    t = now_utc()
    return f"{prefix}:{t.strftime('%Y%m%d%H')}"

def record_warning(conn, detail: str) -> None:
    # Aggregate warnings to send at most once per hour.
    try:
        b = _hour_bucket_key('warn')
        db_inc_state(conn, f'{b}:count', 1)
        # Keep a small sample set for context (max 5).
        sample_key = f'{b}:samples'
        raw = db_get_state(conn, sample_key, '')
        samples = [x for x in raw.split('||') if x] if raw else []
        if len(samples) < 5:
            samples.append(str(detail)[:200])
            db_set_state(conn, sample_key, '||'.join(samples))
    except Exception:
        pass

def record_nonfatal_error(conn, detail: str) -> None:
    # Count as error (existing health counters) and also as a warning for hourly summary.
    try:
        record_error(conn, detail)
    except Exception:
        pass
    try:
        record_warning(conn, detail)
    except Exception:
        pass

def maybe_send_hourly_warning_summary(conn) -> None:
    # Send at most one warning summary per hour (only if warnings occurred).
    try:
        b = _hour_bucket_key('warn')
        sent_key = f'{b}:sent'
        if db_get_state(conn, sent_key, '') == '1':
            return
        count = db_get_state_int(conn, f'{b}:count', 0)
        if count <= 0:
            return
        samples_raw = db_get_state(conn, f'{b}:samples', '')
        samples = [x for x in samples_raw.split('||') if x] if samples_raw else []
        lines = [f'⚠️ BOT WARNINGS (last hour UTC)', f'Count: {count}']
        for s in samples[:5]:
            lines.append(f'- {s}')
        # Exactly-once via outbox
        eid = f'warn_summary:{b}'
        tg_enqueue_event(conn, eid, 'warning', '\n'.join(lines))
        db_set_state(conn, sent_key, '1')
    except Exception as e:
        log.warning(f'Hourly warning summary failed: {e}')


def detect_stuck_lots(conn, minutes: int):
    mins = int(minutes)
    if mins <= 0:
        return []
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT symbol, buy_level, sell_level, qty, state, updated_at
            FROM grid_lots
            WHERE state IN ('buy_open', 'sell_open')
              AND updated_at < (now() - (%s * interval '1 minute'))
            ORDER BY updated_at ASC;
            """,
            (mins,),
        )
        return cur.fetchall() or []


def maybe_alert_lot_stuck(conn) -> None:
    try:
        stuck = detect_stuck_lots(conn, LOT_STUCK_MINUTES)
        if not stuck:
            return

        last_alert_ts = db_get_state(conn, "health:lot_stuck_alert_ts", "")
        cooldown_hours = max(1, int(HEALTH_ALERT_COOLDOWN_MINUTES / 60))
        if last_alert_ts and _recent_within_hours(last_alert_ts, cooldown_hours):
            return

        db_inc_state(conn, "health:lot_stuck_count", len(stuck))
        db_set_state(conn, "health:lot_stuck_last_ts", _iso_now())
        db_set_state(conn, "health:lot_stuck_last", f"{stuck[0]['symbol']}:{stuck[0]['state']}")

        top = stuck[:5]
        lines = ["2️⃣ 🕒 Lot stuck — needs attention"]
        for r in top:
            try:
                age_min = int((now_utc() - r["updated_at"]).total_seconds() / 60)
            except Exception:
                age_min = -1
            lines.append(
                f"{r['symbol']} {r['state']} | buy={float(r['buy_level']):.2f} sell={float(r['sell_level']):.2f} qty={float(r['qty']):g} | age≈{age_min}m"
            )
        if len(stuck) > len(top):
            lines.append(f"...and {len(stuck) - len(top)} more")

        tg_send("\n".join(lines))
        db_set_state(conn, "health:lot_stuck_alert_ts", _iso_now())
    except Exception as e:
        log.warning(f"lot-stuck check failed: {e}")


def total_pnl_since_start(conn) -> float:
    equity = get_account_equity()
    start = db_get_state_float(conn, "health:start_equity", 0.0)
    if start <= 0.0:
        db_set_state(conn, "health:start_equity", str(float(equity)))
        return 0.0
    return float(equity) - float(start)


def compute_confidence_score(conn) -> int:
    score = 100

    err_ts = db_get_state(conn, "health:error_last_ts", "")
    if _recent_within_hours(err_ts, HEALTH_RECENT_ERROR_HOURS):
        score -= 50

    cap_ts = db_get_state(conn, "health:cap_sat_last_ts", "")
    if _recent_within_hours(cap_ts, HEALTH_RECENT_CAPSAT_HOURS):
        score -= 15

    ah_ts = db_get_state(conn, "health:auto_heal_last_ts", "")
    if _recent_within_hours(ah_ts, HEALTH_RECENT_AUTOHEAL_HOURS):
        score -= 5

    try:
        stuck = detect_stuck_lots(conn, LOT_STUCK_MINUTES)
        if stuck:
            score -= 20
    except Exception:
        pass

    score = max(0, min(100, score))
    db_set_state(conn, "health:confidence_last", str(int(score)))
    db_set_state(conn, "health:confidence_last_ts", _iso_now())
    return int(score)


# =======================
# DAILY WINS HELPERS
# =======================

def db_record_tp_win(conn, day: date, pnl_usd: float) -> None:
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
# GLOBAL OPEN ORDERS + ORDER LOOKUP
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
    try:
        p = getattr(order_obj, "filled_avg_price", None)
        if p is None or p == "":
            return 0.0
        return float(p)
    except Exception:
        return 0.0


# =======================
# TP/SL CLASSIFIER
# =======================

def classify_sell_fill(order_obj) -> str:
    """Return 'tp' for limit sells, 'sl' for stop sells, else 'sell'."""
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
# CAPITAL RULE (per-symbol)
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
    """Automated wide stop: stop = buy * (1 - STOP_LOSS_PCT), with optional guard."""
    pct = float(STOP_LOSS_PCT)
    pct = min(max(pct, 0.01), 0.95)

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


def place_bracket_buy(symbol: str, qty: float, buy_price: float, take_profit_price: float, stop_price: float):
    """Places parent BUY limit + TP limit + SL stop (bracket)."""
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


def extract_bracket_leg_ids(order_obj):
    """Return (tp_leg_id, sl_leg_id) for a BRACKET parent order."""
    tp_id, sl_id = "", ""
    if not order_obj:
        return tp_id, sl_id

    try:
        legs = getattr(order_obj, "legs", None) or []
    except Exception:
        legs = []

    for leg in legs or []:
        try:
            side = str(getattr(leg, "side", "") or "").lower()
            if side != "sell":
                continue
            leg_id = str(getattr(leg, "id", "") or "")
            typ = str(getattr(leg, "type", "") or "").lower()

            if "stop" in typ:
                if not sl_id:
                    sl_id = leg_id
            elif typ == "limit":
                if not tp_id:
                    tp_id = leg_id
            else:
                if not tp_id:
                    tp_id = leg_id
        except Exception:
            continue

    return tp_id, sl_id


def extract_take_profit_leg_id(order_obj) -> str:
    tp_id, _ = extract_bracket_leg_ids(order_obj)
    return tp_id


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
    tp_order_id=None,
    sl_order_id=None,
    buy_fill_px=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO grid_lots(symbol, buy_level, sell_level, qty, state, buy_order_id, sell_order_id, tp_order_id, sl_order_id, buy_fill_px, updated_at)
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,now())
            ON CONFLICT(symbol, buy_level)
            DO UPDATE SET
                sell_level=EXCLUDED.sell_level,
                qty=EXCLUDED.qty,
                state=EXCLUDED.state,
                buy_order_id=COALESCE(EXCLUDED.buy_order_id, grid_lots.buy_order_id),
                sell_order_id=COALESCE(EXCLUDED.sell_order_id, grid_lots.sell_order_id),
                tp_order_id=COALESCE(EXCLUDED.tp_order_id, grid_lots.tp_order_id),
                sl_order_id=COALESCE(EXCLUDED.sl_order_id, grid_lots.sl_order_id),
                buy_fill_px=COALESCE(EXCLUDED.buy_fill_px, grid_lots.buy_fill_px),
                updated_at=now();
        """,
            (
                symbol,
                d2(buy_level),
                d2(sell_level),
                float(qty),
                state,
                buy_order_id,
                sell_order_id,
                tp_order_id,
                sl_order_id,
                buy_fill_px,
            ),
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
# RECONCILIATION (ROBUST + SAFER BUY FALLBACK)
# =======================

def reconcile_lots(conn, symbol: str, open_orders):
    """Reconcile DB lots with Alpaca reality.

    Key behaviors:
      - When a tracked order is not open, attempt truth lookup by ID, then recent CLOSED.
      - Partial-fill handling for both buy_open and sell_open.
      - Bracket-aware: sell_open is treated as "either TP or SL leg may fill".

    Safer BUY fallback (patch):
      - Only promotes a missing BUY when there is enough *untracked* position qty
        to justify that specific lot.
      - Only one fallback promotion per symbol per run.

    This prevents the serious failure mode: symbol has some position (manual/seeded/other lot)
    and we accidentally "promote" multiple missing buys into owned/sell_open.
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

    # Build a lookup of recent closed orders for truth fallback.
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

    # Compute "tracked" position qty from lots that represent owned inventory.
    # - owned: yes
    # - sell_open: yes (still owned until filled)
    # buy_open: no (not owned yet)
    tracked_owned_qty = 0.0
    for lot in lots:
        st = str(lot.get("state") or "")
        if st in ("owned", "sell_open"):
            tracked_owned_qty += max(0.0, lot_qty_float(lot))

    pos_qty_total = get_position_qty(symbol)
    untracked_pos_qty = max(0.0, float(pos_qty_total) - float(tracked_owned_qty))

    fallback_promoted = False

    for lot in lots:
        state = lot["state"]
        buy_oid = lot.get("buy_order_id")
        sell_oid = lot.get("sell_order_id")
        tp_oid = lot.get("tp_order_id") or ""
        sl_oid = lot.get("sl_order_id") or ""

        # -------------------------
        # BUY open -> filled/canceled/unknown
        # -------------------------
        if state == "buy_open" and buy_oid and str(buy_oid) not in open_ids:
            o = lookup_order_truth(str(buy_oid))
            st = order_status_string(o) if o else ""
            fq = order_filled_qty(o) if o else 0.0

            # Detailed logging (live debugging)
            log.info(
                f"🧾 {symbol} reconcile BUY | lot buy_level={float(lot['buy_level']):.2f} | oid={buy_oid} | truth_status={st or 'n/a'} | truth_filled_qty={fq:g}"
            )

            if fq > 0.0:
                tp_leg_id, sl_leg_id = ("", "")
                try:
                    tp_leg_id, sl_leg_id = extract_bracket_leg_ids(o)
                except Exception:
                    tp_leg_id, sl_leg_id = ("", "")

                if tp_leg_id or sl_leg_id:
                    db_upsert_lot(
                        conn,
                        symbol,
                        float(lot["buy_level"]),
                        float(lot["sell_level"]),
                        float(fq),
                        "sell_open",
                        buy_order_id=str(buy_oid),
                        sell_order_id=str(tp_leg_id) if tp_leg_id else None,
                        tp_order_id=str(tp_leg_id) if tp_leg_id else None,
                        sl_order_id=str(sl_leg_id) if sl_leg_id else None,
                    )
                    log.info(f"🧠 {symbol} reconcile: BUY partial/filled qty={fq:g} -> BRACKET legs -> sell_open")
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
                        log.warning(f"🟠 {symbol} reconcile: BUY {buy_oid} status={st} but filled_qty={fq:g} -> owned")

            elif st == "filled":
                fallback_qty = lot_qty_float(lot)
                tp_leg_id, sl_leg_id = ("", "")
                try:
                    tp_leg_id, sl_leg_id = extract_bracket_leg_ids(o)
                except Exception:
                    tp_leg_id, sl_leg_id = ("", "")

                if tp_leg_id or sl_leg_id:
                    db_upsert_lot(
                        conn,
                        symbol,
                        float(lot["buy_level"]),
                        float(lot["sell_level"]),
                        float(fallback_qty),
                        "sell_open",
                        buy_order_id=str(buy_oid),
                        sell_order_id=str(tp_leg_id) if tp_leg_id else None,
                        tp_order_id=str(tp_leg_id) if tp_leg_id else None,
                        sl_order_id=str(sl_leg_id) if sl_leg_id else None,
                    )
                    log.info(f"🧠 {symbol} reconcile: BUY status=filled -> BRACKET legs -> sell_open")
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
                log.info(f"🧹 {symbol} reconcile: BUY {buy_oid} {st} -> deleted lot")

            # ✅ SAFER position-truth fallback
            elif (not fallback_promoted) and untracked_pos_qty > 0.0:
                expected_qty = lot_qty_float(lot)
                # If expected is missing/zero, use a conservative amount: ALL remaining untracked.
                if expected_qty <= 0.0:
                    expected_qty = float(untracked_pos_qty)

                # Promote only if there is enough untracked position to cover this lot.
                # Use a tiny tolerance for float/rounding.
                tol = 1e-6
                if float(untracked_pos_qty) + tol >= float(expected_qty):
                    # We cannot reliably fetch legs if the parent is missing; treat as owned.
                    # (If legs exist, a later reconcile when Alpaca returns the parent/legs will advance it.)
                    db_upsert_lot(
                        conn,
                        symbol,
                        float(lot["buy_level"]),
                        float(lot["sell_level"]),
                        float(expected_qty),
                        "owned",
                        buy_order_id=str(buy_oid),
                        sell_order_id=None,
                        tp_order_id=None,
                        sl_order_id=None,
                    )

                    untracked_pos_qty = max(0.0, float(untracked_pos_qty) - float(expected_qty))
                    fallback_promoted = True

                    log.info(
                        f"🧠 {symbol} reconcile fallback: BUY parent missing; promoted ONE lot via untracked position qty={expected_qty:g} (remaining_untracked={untracked_pos_qty:g})"
                    )
                    try:
                        record_auto_heal(conn, symbol, "buy_position_fallback_safe")
                    except Exception:
                        pass
                else:
                    log.warning(
                        f"🟠 {symbol} reconcile: BUY parent missing; NOT promoting lot (expected={expected_qty:g} > untracked_pos={untracked_pos_qty:g}). Keeping as buy_open."
                    )

            else:
                log.warning(
                    f"🟠 {symbol} reconcile: buy_open order {buy_oid} not open, truth unknown -> keeping lot"
                )

        # -------------------------
        # SELL open -> filled/canceled/unknown (BRACKET-AWARE: TP vs SL)
        # -------------------------

        # BRACKET RECOVERY: if tp/sl leg IDs were not captured at submit time,
        # attempt to recover them by scanning recent orders for child legs whose
        # parent_order_id matches this lot's buy_order_id.
        if state == 'sell_open' and buy_oid and (not tp_oid) and (not sl_oid):
            try:
                parent_id = str(buy_oid)
                candidates = []

                def _side_val(oobj):
                    try:
                        s = getattr(oobj, 'side', None)
                        return str(getattr(s, 'value', s) or '').lower()
                    except Exception:
                        return ''

                for oo in list(open_orders) + list(closed):
                    try:
                        if _side_val(oo) != 'sell':
                            continue
                        po = str(getattr(oo, 'parent_order_id', '') or '')
                        if po and po == parent_id:
                            candidates.append(oo)
                    except Exception:
                        continue

                rec_tp, rec_sl = '', ''
                for oo in candidates:
                    try:
                        typ = str(getattr(oo, 'type', '') or '').lower()
                        oid = str(getattr(oo, 'id', '') or '')
                        if not oid:
                            continue
                        if 'stop' in typ and not rec_sl:
                            rec_sl = oid
                        elif typ == 'limit' and not rec_tp:
                            rec_tp = oid
                        elif not rec_tp:
                            rec_tp = oid
                    except Exception:
                        continue

                if rec_tp or rec_sl:
                    db_upsert_lot(
                        conn,
                        symbol,
                        float(lot['buy_level']),
                        float(lot['sell_level']),
                        float(lot_qty_float(lot)),
                        'sell_open',
                        buy_order_id=str(buy_oid),
                        sell_order_id=(rec_tp or None),
                        tp_order_id=(rec_tp or None),
                        sl_order_id=(rec_sl or None),
                        buy_fill_px=lot.get('buy_fill_px'),
                    )
                    tp_oid = rec_tp or tp_oid
                    sl_oid = rec_sl or sl_oid
                    sell_oid = rec_tp or sell_oid
                    log.info(f"{symbol} bracket recovery: backfilled legs tp={rec_tp or '-'} sl={rec_sl or '-'} for parent={parent_id}")
            except Exception as e:
                log.warning(f"{symbol} bracket recovery failed: {e}")

        tracked_sell_ids = []
        if tp_oid or sl_oid:
            if tp_oid:
                tracked_sell_ids.append(str(tp_oid))
            if sl_oid:
                tracked_sell_ids.append(str(sl_oid))
        elif sell_oid:
            tracked_sell_ids.append(str(sell_oid))

        any_leg_open = any((oid and str(oid) in open_ids) for oid in tracked_sell_ids)

        if state == "sell_open" and tracked_sell_ids and not any_leg_open:
            # Look up leg truths
            leg_truth = []
            for oid in tracked_sell_ids:
                oo = lookup_order_truth(str(oid))
                leg_truth.append((oid, oo, order_status_string(oo) if oo else "", order_filled_qty(oo) if oo else 0.0))

            filled_leg = next((t for t in leg_truth if t[2] == "filled"), None)
            if filled_leg is None:
                filled_leg = next((t for t in leg_truth if (t[3] or 0.0) > 0.0), None)

            oid_used, o, st, fq = ("", None, "", 0.0)
            if filled_leg is not None:
                oid_used, o, st, fq = filled_leg
            else:
                oid_used, o, st, fq = leg_truth[0]

            st = order_status_string(o) if o else ""
            fq = order_filled_qty(o) if o else 0.0
            lot_qty = lot_qty_float(lot)

            log.info(
                f"🧾 {symbol} reconcile SELL | lot buy_level={float(lot['buy_level']):.2f} | tracked={tracked_sell_ids} | used={oid_used} | truth_status={st or 'n/a'} | truth_filled_qty={fq:g}"
            )

            if st == "filled":
                # classify TP vs SL
                if tp_oid or sl_oid:
                    if sl_oid and str(oid_used) == str(sl_oid):
                        fill_type = "sl"
                    elif tp_oid and str(oid_used) == str(tp_oid):
                        fill_type = "tp"
                    else:
                        fill_type = classify_sell_fill(o)
                else:
                    fill_type = classify_sell_fill(o)

                buy_level = float(lot["buy_level"])
                target_level = float(lot["sell_level"])
                qty = float(lot_qty)

                filled_px = order_filled_avg_price(o)
                sell_px = float(filled_px) if filled_px > 0 else float(target_level)
                pnl = (sell_px - buy_level) * qty
                # Compute true realized PnL (use stored buy fill avg if available)
                entry_px = float(lot.get('buy_fill_px') or 0.0)
                if entry_px <= 0.0:
                    entry_px = float(buy_level)

                pnl = (sell_px - entry_px) * qty
                cost = entry_px * qty
                pnl_pct = (pnl / cost * 100.0) if cost > 0 else 0.0

                outcome = 'tp' if fill_type == 'tp' else ('sl' if fill_type == 'sl' else 'sell')

                # Exactly-once close record (persistent dedupe)
                first_close = db_mark_lot_closed_once(
                    conn,
                    symbol=symbol,
                    buy_level=buy_level,
                    outcome=outcome,
                    qty=qty,
                    entry_px=entry_px,
                    exit_px=sell_px,
                    pnl_usd=pnl,
                    pnl_pct=pnl_pct,
                    close_order_id=str(oid_used or ''),
                )

                if first_close:
                    # Update lifetime realized PnL
                    strategy_add_realized_pnl(conn, pnl)

                    # Lot-level completion (exactly-once)
                    title = 'TAKE-PROFIT' if outcome == 'tp' else ('STOP-LOSS' if outcome == 'sl' else 'SELL')
                    msg = (
                        f"LOT CLOSED ({title}) | {symbol}\n"
                        f"Qty: {qty:g}\n"
                        f"Entry: {entry_px:.2f}\n"
                        f"Exit: {sell_px:.2f}\n"
                        f"Realized PnL: ${pnl:,.2f} ({pnl_pct:+.2f}%)"
                    )
                    tg_enqueue_event(conn, f"lot_closed:{symbol}:{d2(buy_level):.2f}", 'lot', msg)

                    # Daily TP wins counter preserved (existing behavior, now driven by confirmed closure)
                    if outcome == 'tp':
                        db_record_tp_win(conn, utc_day(), pnl)

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
                        tp_order_id=None,
                        sl_order_id=None,
                    )
                if st and st != "filled":
                    log.warning(
                        f"🟠 {symbol} reconcile: SELL {oid_used} status={st} but filled_qty={fq:g} -> adjusted remaining={remaining:g}"
                    )

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
                    tp_order_id=None,
                    sl_order_id=None,
                )
                log.info(f"🧠 {symbol} reconcile: SELL legs {tracked_sell_ids} {st} -> owned")

            else:
                pos_qty = get_position_qty(symbol)
                if o is None and pos_qty > 0.0:
                    expected_qty = float(lot_qty) if float(lot_qty) > 0.0 else float(pos_qty)
                    db_upsert_lot(
                        conn,
                        symbol,
                        float(lot["buy_level"]),
                        float(lot["sell_level"]),
                        float(expected_qty),
                        "owned",
                        buy_order_id=buy_oid,
                        sell_order_id=None,
                        tp_order_id=None,
                        sl_order_id=None,
                    )
                    log.info(
                        f"🧠 {symbol} reconcile auto-heal: phantom SELL legs {tracked_sell_ids} cleared via position qty={expected_qty:g}"
                    )
                    try:
                        record_auto_heal(conn, symbol, "phantom_sell_cleared")
                    except Exception:
                        pass
                elif o is None and pos_qty <= 0.0:
                    db_delete_lot(conn, symbol, float(lot["buy_level"]))
                    log.info(
                        f"🧹 {symbol} reconcile auto-heal: removed stale lot (no position, missing SELL legs {tracked_sell_ids})"
                    )
                    try:
                        record_auto_heal(conn, symbol, "stale_lot_removed")
                    except Exception:
                        pass
                else:
                    log.warning(
                        f"🟠 {symbol} reconcile: sell_open legs {tracked_sell_ids} not open, truth unknown -> keeping lot"
                    )


# =======================
# SIMPLE SEED
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
        log.info(
            f"🧩 Seeded {symbol} memory: owned lot at buy_level={buy_level} qty={qty:g} (avg_entry≈{avg_price:.2f})"
        )
    except Exception as e:
        log.warning(f"{symbol} seed failed: {e}")


# =======================
# DAILY SUMMARY
# =======================

def maybe_daily_strategy_summary(conn):
    """Daily Telegram summary based on REALIZED PnL (lot_closures) - exactly once per UTC day."""
    day = utc_day()
    hour = now_utc().hour

    # Wait until configured UTC hour
    if hour < DAILY_SUMMARY_HOUR_UTC:
        return

    # Exactly-once per day via outbox id
    eid = f"daily_strategy:{day.isoformat()}"

    # If already enqueued, skip building/sending again.
    if tg_enabled():
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM telegram_events WHERE event_id=%s;", (eid,))
                if cur.fetchone() is not None:
                    return
        except Exception:
            # If we cannot check, fail silent here - do not risk spam.
            return

    # Daily realized PnL and win/loss counts based on closures
    daily_pnl = 0.0
    wins = 0
    losses = 0
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT outcome, SUM(pnl_usd) AS pnl, COUNT(*) AS n
                FROM lot_closures
                WHERE symbol IS NOT NULL
                  AND (closed_at AT TIME ZONE 'UTC')::date = %s
                GROUP BY outcome;
                """,
                (day,),
            )
            rows = cur.fetchall() or []
        for r in rows:
            try:
                out = str(r.get('outcome') or '')
                pnl = float(r.get('pnl') or 0.0)
                n = int(r.get('n') or 0)
            except Exception:
                continue
            daily_pnl += pnl
            if out == 'tp':
                wins += n
            elif out == 'sl':
                losses += n
            else:
                # Treat other outcomes as neutral; keep counts separate from tp/sl.
                pass
    except Exception as e:
        try:
            record_warning(conn, f"daily summary query failed: {e}")
        except Exception:
            pass
        return

    total_trades = wins + losses
    win_rate = (wins / total_trades * 100.0) if total_trades > 0 else 0.0

    baseline = float(BASELINE_CAPITAL) if float(BASELINE_CAPITAL) > 0 else 0.0
    cum = strategy_get_cum_realized_pnl(conn)
    net = baseline + cum
    pct = (cum / baseline * 100.0) if baseline > 0 else 0.0

    txt = (
        f"📊 DAILY STRATEGY SUMMARY (UTC {day})\n\n"
        f"Daily realized PnL: ${daily_pnl:,.2f}\n"
        f"Daily wins/losses: {wins}W / {losses}L\n"
        f"Daily win rate: {win_rate:.1f}%\n\n"
        f"Baseline capital: ${baseline:,.2f}\n"
        f"Cumulative realized PnL: ${cum:,.2f}\n"
        f"Net strategy equity: ${net:,.2f}\n"
        f"Return vs baseline: {pct:+.2f}%"
    )

    tg_enqueue_event(conn, eid, 'strategy', txt)


# =======================
# CORE BOT
# =======================
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
        try:
            record_error(conn, msg)
        except Exception:
            pass
        return finish({"symbol": symbol, "action": "error", "reason": "bad_config"})

    last_price = get_last_price(symbol)
    log.info(f"📈 {symbol} PRICE = {last_price:.4f}")

    prev_raw = db_get_state(conn, state_key, "")
    prev_price = None
    try:
        prev_price = float(prev_raw) if prev_raw != "" else None
    except Exception:
        prev_price = None

    if not (lower <= last_price <= upper):
        log.info(f"🟡 {symbol} outside band [{lower}, {upper}]")
        # Telegram outside-band alerts disabled (log only)
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
        try:
            record_capital_saturation(conn, symbol, "lock_rules")
        except Exception:
            pass
        return finish({"symbol": symbol, "action": "none", "reason": "new_orders_blocked", "price": last_price}, last_price)

    # =========================
    # 1) SELL (seeded positions only; bracket buys manage their own TP sells)
    # =========================
    if sell_level is not None:
        seeded_lot = db_get_seeded_lot_for_sell_level(conn, symbol, sell_level)
        if seeded_lot:
            seeded_qty = float(seeded_lot["qty"])
            max_sell = min(seeded_qty, float(free_qty))
            sell_qty = normalize_qty(max_sell, allow_fractional=ALLOW_FRACTIONAL_SELLS)

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
                    try:
                        record_nonfatal_error(conn, msg)
                    except Exception:
                        pass
                    return finish({"symbol": symbol, "action": "error", "reason": "sell_failed"}, last_price)

    # =========================
    # 2) BUY (BRACKET)
    # =========================
    if buy_level is not None:
        raw_qty = order_usd / float(buy_level)
        buy_qty = normalize_qty(raw_qty, allow_fractional=not BUY_WHOLE_SHARES)
        if buy_qty <= 0:
            return finish({"symbol": symbol, "action": "none", "reason": "buy_qty_zero"}, last_price)

        existing = db_get_lot(conn, symbol, float(buy_level))
        if existing and existing["state"] in ("buy_open", "owned", "sell_open"):
            log.info(
                f"🟡 {symbol} already tracked at buy_level={float(buy_level):.2f} (state={existing['state']}), skipping BUY"
            )
            return finish(
                {"symbol": symbol, "action": "none", "reason": "level_already_tracked", "buy_level": float(buy_level), "price": last_price},
                last_price,
            )

        projected = used + (float(buy_qty) * float(buy_level))
        if projected > max_capital:
            log.info(
                f"🟠 {symbol} BUY blocked (capital) used≈${used:,.2f} projected≈${projected:,.2f} max=${max_capital:,.2f}"
            )
            try:
                record_capital_saturation(conn, symbol, f"used≈{used:.2f} projected≈{projected:.2f} max≈{max_capital:.2f}")
            except Exception:
                pass

            # Optional alert (rate-limited)
            try:
                last_alert_ts = db_get_state(conn, "health:cap_sat_alert_ts", "")
                cooldown_hours = max(1, int(HEALTH_ALERT_COOLDOWN_MINUTES / 60))
                if (not last_alert_ts) or (not _recent_within_hours(last_alert_ts, cooldown_hours)):
                    tg_send(
                        f"3️⃣ 🧊 Capital saturation | {symbol}\nused≈${used:,.2f} projected≈${projected:,.2f} max=${max_capital:,.2f}"
                    )
                    db_set_state(conn, "health:cap_sat_alert_ts", _iso_now())
            except Exception:
                pass

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
                try:
                    record_nonfatal_error(conn, msg)
                except Exception:
                    pass
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

        maybe_alert_lot_stuck(conn)
        compute_confidence_score(conn)
        maybe_daily_strategy_summary(conn)
        maybe_send_hourly_warning_summary(conn)
        tg_flush_outbox(conn)

        conn.commit()
        return jsonify({"status": "ok", "results": results})
    except Exception as e:
        conn.rollback()
        msg = f"❌ /run crashed: {e}"
        log.exception(msg)
        try:
            # Critical alert (enqueue for exactly-once); fallback to direct send if needed.
            tg_enqueue_event(conn, f'critical:run_crash:{utc_day().isoformat()}:{now_utc().hour}', 'critical', msg)
            tg_flush_outbox(conn)
        except Exception:
            try:
                tg_send(msg)
            except Exception:
                pass
        try:
            record_error(conn, msg)
            conn.commit()
        except Exception:
            pass
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

    # ✅ In LIVE mode, /board also requires the token (no accidental exposure).
    if not PAPER:
        if not RUN_TOKEN or token != RUN_TOKEN:
            return "Unauthorized", 401
    else:
        # In paper mode, if RUN_TOKEN is set, require it.
        if RUN_TOKEN and token != RUN_TOKEN:
            return "Unauthorized", 401

    lines = [f"🦁 Leo Board | Paper={PAPER} | TradingEnabled={TRADING_ENABLED} | {now_utc().isoformat()} | SL_PCT={STOP_LOSS_PCT:.2%}"]

    try:
        conn = pg_conn()
        try:
            score = compute_confidence_score(conn)
            pnl_total = total_pnl_since_start(conn)
            err_last = db_get_state(conn, "health:error_last", "")
            cap_last = db_get_state(conn, "health:cap_sat_last", "")
            ah_last = db_get_state(conn, "health:auto_heal_last", "")
            stuck_now = detect_stuck_lots(conn, LOT_STUCK_MINUTES)

            lines.append(f"5️⃣ 🧠 Confidence: {score}/100")
            lines.append(f"6️⃣ 📊 Total P/L (since start): ${pnl_total:,.2f}")
            if stuck_now:
                lines.append(f"2️⃣ 🕒 Lot stuck: {len(stuck_now)} (>{LOT_STUCK_MINUTES}m)")
            if cap_last:
                lines.append(f"3️⃣ 🧊 Capital saturation: {cap_last}")
            if ah_last:
                lines.append(f"1️⃣ 🧠 Auto-heal: {ah_last}")
            if err_last:
                lines.append(f"4️⃣ 🚨 Error: {err_last}")
        finally:
            conn.close()
    except Exception:
        pass

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
            lines.append(f"📈 {sym} | price={p:.2f} | pos={pos:g} | open_orders={len(oo)}")
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

        if msg.strip().lower() in ("/performance", "/pnl"):
            conn = pg_conn()
            try:
                tg_enqueue_event(conn, f"strategy_perf_manual:{now_utc().isoformat()}", 'strategy', build_strategy_performance_text(conn))
                tg_flush_outbox(conn)
            finally:
                conn.close()

        if msg.strip().lower() == "/status":
            lines = [
                f"🦁 Leo status | Paper={PAPER} | TradingEnabled={TRADING_ENABLED} | SL_PCT={STOP_LOSS_PCT:.2%} | FileLog={'on' if (LOG_TO_FILE and (not PAPER)) else 'off'}"
            ]

            try:
                conn = pg_conn()
                try:
                    score = compute_confidence_score(conn)
                    pnl_total = total_pnl_since_start(conn)
                    err_last = db_get_state(conn, "health:error_last", "")
                    cap_last = db_get_state(conn, "health:cap_sat_last", "")
                    ah_last = db_get_state(conn, "health:auto_heal_last", "")
                    stuck_now = detect_stuck_lots(conn, LOT_STUCK_MINUTES)

                    lines.append(f"5️⃣ 🧠 Confidence: {score}/100")
                    lines.append(f"6️⃣ 📊 Total P/L (since start): ${pnl_total:,.2f}")
                    if stuck_now:
                        lines.append(f"2️⃣ 🕒 Lot stuck: {len(stuck_now)} (>{LOT_STUCK_MINUTES}m)")
                    if cap_last:
                        lines.append(f"3️⃣ 🧊 Capital saturation: {cap_last}")
                    if ah_last:
                        lines.append(f"1️⃣ 🧠 Auto-heal: {ah_last}")
                    if err_last:
                        lines.append(f"4️⃣ 🚨 Error: {err_last}")
                finally:
                    conn.close()
            except Exception:
                pass

            for sym in BOTS.keys():
                try:
                    p = get_last_price(sym)
                    pos = get_position_qty(sym)
                    lines.append(f"{sym} price={p:.2f} pos={pos:g}")
                except Exception:
                    pass

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
    mode = "Paper" if PAPER else "Live"
    log.info(f"🦁 Leo started v4.2.1_PATCH ({mode} Trading) | SL_PCT={STOP_LOSS_PCT:.2%}")
    tg_send(f"🦁 Leo started v4.2.1_PATCH ({mode} Trading) | SL_PCT={STOP_LOSS_PCT:.2%}")
    port = int(os.getenv("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
