"""
backend/main.py — ES·TBBO Suite FastAPI Backend
Improvements vs v1:
  - FIX: Commission double-charge bug (entry already paid at open; close was charging 2×)
  - FIX: Better NULL handling in _safe()
  - ADD: /health endpoint (faster than /docs for readiness check)
  - ADD: /bars universal endpoint (routes to correct bar builder)
  - ADD: /export/trades endpoint (CSV download)
  - ADD: session filter (RTH / ETH / ALL) in date_where
  - ADD: avg_trade_duration in stats
  - ADD: better validation & error messages
  - IMPROVED: Monte Carlo now returns full percentile band data
  - IMPROVED: stats now includes initial_capital, session, bar info
"""

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import Optional, Literal
import duckdb, os, math, traceback, csv, io
import numpy as np

app = FastAPI(title="ES·TBBO Backend", version="2.0")
app.add_middleware(CORSMiddleware,
    allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
con = duckdb.connect()

# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────
def ensure_file(path: str):
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail=f"File not found: {path}")

def _safe(v):
    """Return float or None; suppress NaN/Inf/errors."""
    if v is None:
        return None
    try:
        f = float(v)
        return None if (math.isnan(f) or math.isinf(f)) else f
    except Exception:
        return None

def safe_round(v, n: int = 4):
    s = _safe(v)
    return round(s, n) if s is not None else None


# ─────────────────────────────────────────────────────────────────────────────
# Column definitions for OF aggregations
# ─────────────────────────────────────────────────────────────────────────────
OF_COLS = """
    SUM(size)                                                       AS vol,
    AVG(bid_px_00)                                                  AS bid,
    AVG(ask_px_00)                                                  AS ask,
    SUM(price * size) / NULLIF(SUM(size), 0)                       AS vwap,
    SUM(CASE WHEN side='B' THEN size ELSE 0 END)                   AS buy_vol,
    SUM(CASE WHEN side='A' THEN size ELSE 0 END)                   AS sell_vol,
    AVG(bid_sz_00)                                                  AS avg_bid_sz,
    AVG(ask_sz_00)                                                  AS avg_ask_sz,
    MAX(bid_sz_00)                                                  AS max_bid_sz,
    MAX(ask_sz_00)                                                  AS max_ask_sz
"""

# Session hour ranges (ET)
SESSION_HOURS = {
    "RTH": ("09:30", "16:15"),
    "ETH": ("18:00", "09:29"),   # overnight / globex
    "ALL": None,
}


def date_where(symbol: str, date_from, date_to, session: str = "ALL") -> str:
    """Build a safe WHERE clause. Uses parameterised quoting via string escaping."""
    # Sanitise symbol (strip dangerous chars)
    sym = symbol.replace("'", "''")
    w = f"WHERE symbol='{sym}' AND action='T'"

    if date_from:
        df = str(date_from).replace("'", "")
        w += f" AND ts_recv >= '{df} 00:00:00+00'"
    if date_to:
        dt = str(date_to).replace("'", "")
        w += f" AND ts_recv <= '{dt} 23:59:59+00'"

    if session == "RTH":
        w += " AND time_part('hour', ts_recv AT TIME ZONE 'America/New_York') * 60 + time_part('minute', ts_recv AT TIME ZONE 'America/New_York') BETWEEN 570 AND 975"
    elif session == "ETH":
        w += " AND (time_part('hour', ts_recv AT TIME ZONE 'America/New_York') * 60 + time_part('minute', ts_recv AT TIME ZONE 'America/New_York') >= 1080 OR time_part('hour', ts_recv AT TIME ZONE 'America/New_York') * 60 + time_part('minute', ts_recv AT TIME ZONE 'America/New_York') < 570)"

    return w


def clean_ohlcv(rows) -> list:
    """
    Expected columns per row:
    0:time  1:open  2:high  3:low  4:close  5:volume
    6:bid   7:ask   8:vwap
    9:buy_vol  10:sell_vol
    11:avg_bid_sz  12:avg_ask_sz  13:max_bid_sz  14:max_ask_sz
    """
    result = []
    for r in rows:
        if any(_safe(r[i]) is None for i in range(1, 5)):
            continue

        vol      = _safe(r[5])  or 0.0
        buy_vol  = _safe(r[9])  or 0.0
        sell_vol = _safe(r[10]) or 0.0
        delta    = round(buy_vol - sell_vol, 2)

        b_sz = safe_round(r[11], 2)
        a_sz = safe_round(r[12], 2)
        size_imb = None
        if b_sz and a_sz and (b_sz + a_sz) > 0:
            size_imb = round((b_sz - a_sz) / (b_sz + a_sz), 4)

        result.append({
            "time":       int(r[0]),
            "open":       round(float(r[1]), 4),
            "high":       round(float(r[2]), 4),
            "low":        round(float(r[3]), 4),
            "close":      round(float(r[4]), 4),
            "volume":     round(vol, 2),
            "bid":        safe_round(r[6], 4),
            "ask":        safe_round(r[7], 4),
            "vwap":       safe_round(r[8], 4),
            "buy_vol":    round(buy_vol, 2),
            "sell_vol":   round(sell_vol, 2),
            "delta":      delta,
            "delta_pct":  round(delta / vol * 100, 2) if vol > 0 else 0.0,
            "avg_bid_sz": b_sz,
            "avg_ask_sz": a_sz,
            "max_bid_sz": safe_round(r[13], 2),
            "max_ask_sz": safe_round(r[14], 2),
            "size_imb":   size_imb,
            "cum_delta":  0.0,   # filled below
        })

    # Cumulative delta
    cd = 0.0
    for bar in result:
        cd += bar["delta"]
        bar["cum_delta"] = round(cd, 2)
    return result


def _fetch_ticks(path: str, w: str):
    return con.execute(f"""
        SELECT ts_recv, price, size, side,
               bid_px_00, ask_px_00, bid_sz_00, ask_sz_00
        FROM read_parquet('{path}') {w}
        ORDER BY ts_recv
    """).fetchall()


def _agg(rows, mode: str, threshold: float) -> list:
    """Aggregate raw ticks into volume / tick / range bars."""
    if not rows:
        return []

    bars = []

    def _make(t, o, h, l, c, vol, ticks, bv, sv, sb, sa, sbsz, sasz, mbsz, masz):
        ts_int   = int(t.timestamp()) if hasattr(t, "timestamp") else int(t)
        vol      = vol  or 1e-9
        bv       = bv   or 0.0
        sv       = sv   or 0.0
        delta    = round(bv - sv, 2)
        n        = ticks or 1
        b_sz     = round(sbsz / n, 2)
        a_sz     = round(sasz / n, 2)
        mb       = round(mbsz, 2) if mbsz else None
        ma       = round(masz, 2) if masz else None
        size_imb = None
        if b_sz and a_sz and (b_sz + a_sz) > 0:
            size_imb = round((b_sz - a_sz) / (b_sz + a_sz), 4)
        return {
            "time":       ts_int,
            "open":  o, "high": h, "low": l, "close": c,
            "volume":     round(vol, 2),
            "bid":        round(sb / n, 4),
            "ask":        round(sa / n, 4),
            "vwap":       None,
            "buy_vol":    round(bv,  2),
            "sell_vol":   round(sv,  2),
            "delta":      delta,
            "delta_pct":  round(delta / vol * 100, 2),
            "avg_bid_sz": b_sz, "avg_ask_sz": a_sz,
            "max_bid_sz": mb,   "max_ask_sz": ma,
            "size_imb":   size_imb,
            "cum_delta":  0.0,
        }

    ts0, p0, *_ = rows[0]
    o = h = l = c = float(p0)
    vol = ticks = bv = sv = sb = sa = sbsz = sasz = 0.0
    mbsz = masz = 0.0
    t = ts0

    for ts, price, size, side, bid, ask, bsz, asz in rows:
        price = float(price)
        s     = float(size or 0)
        h     = max(h, price)
        l     = min(l, price)
        c     = price
        vol  += s
        ticks += 1
        if side == 'B':
            bv += s
        else:
            sv += s
        sb   += float(bid  or 0)
        sa   += float(ask  or 0)
        sbsz += float(bsz  or 0)
        sasz += float(asz  or 0)
        mbsz  = max(mbsz, float(bsz or 0))
        masz  = max(masz, float(asz or 0))

        done = (
            (mode == "volume" and vol   >= threshold) or
            (mode == "tick"   and ticks >= threshold) or
            (mode == "range"  and (h - l) >= threshold)
        )
        if done:
            bars.append(_make(t, o, h, l, c, vol, ticks, bv, sv, sb, sa, sbsz, sasz, mbsz, masz))
            o = h = l = c = price
            vol = ticks = bv = sv = sb = sa = sbsz = sasz = 0.0
            mbsz = masz = 0.0
            t = ts

    if ticks > 0:
        bars.append(_make(t, o, h, l, c, vol, ticks, bv, sv, sb, sa, sbsz, sasz, mbsz, masz))

    cd = 0.0
    for bar in bars:
        cd += bar["delta"]
        bar["cum_delta"] = round(cd, 2)
    return bars


INTERVAL_MAP = {
    "1min":  "1 minute",
    "5min":  "5 minutes",
    "15min": "15 minutes",
    "30min": "30 minutes",
    "1h":    "1 hour",
}


def _build_time_bars(path: str, w: str, interval: str) -> list:
    di  = INTERVAL_MAP.get(interval, "5 minutes")
    raw = con.execute(f"""
        SELECT
            epoch(time_bucket(INTERVAL '{di}', ts_recv::TIMESTAMPTZ)) AS t,
            FIRST(price ORDER BY ts_recv) AS o,
            MAX(price) AS h, MIN(price) AS l,
            LAST(price ORDER BY ts_recv) AS c,
            {OF_COLS}
        FROM read_parquet('{path}') {w}
        GROUP BY 1 ORDER BY 1
    """).fetchall()
    return clean_ohlcv(raw)


# ─────────────────────────────────────────────────────────────────────────────
# Endpoints
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/load")
def load_file(path: str = Query(...)):
    ensure_file(path)
    try:
        info = con.execute(f"""
            SELECT COUNT(*), MIN(ts_recv), MAX(ts_recv), COUNT(DISTINCT symbol)
            FROM read_parquet('{path}')
        """).fetchone()
        return {
            "rows":    info[0],
            "first":   str(info[1]),
            "last":    str(info[2]),
            "symbols": info[3],
            "path":    path,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/symbols")
def get_symbols(path: str = Query(...)):
    ensure_file(path)
    try:
        rows = con.execute(f"""
            SELECT symbol, COUNT(*) AS cnt
            FROM read_parquet('{path}')
            WHERE action='T'
            GROUP BY symbol
            ORDER BY 2 DESC
        """).fetchall()
        return [{"symbol": r[0], "count": r[1]} for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── Universal bars endpoint ───────────────────────────────────────────────
@app.get("/bars")
def get_bars(
    path:      str           = Query(...),
    symbol:    str           = Query("ESM4"),
    bar_type:  str           = Query("time"),
    interval:  str           = Query("5min"),
    threshold: float         = Query(1000),
    date_from: Optional[str] = Query(None),
    date_to:   Optional[str] = Query(None),
    session:   str           = Query("ALL"),
):
    ensure_file(path)
    w = date_where(symbol, date_from, date_to, session)
    try:
        if bar_type == "time":
            return _build_time_bars(path, w, interval)
        else:
            return _agg(_fetch_ticks(path, w), bar_type, threshold)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Legacy individual bar endpoints kept for backward compat
@app.get("/bars/time")
def time_bars(path: str=Query(...), symbol: str=Query("ESM4"), interval: str=Query("5min"),
              date_from: Optional[str]=Query(None), date_to: Optional[str]=Query(None),
              session: str=Query("ALL")):
    ensure_file(path)
    return _build_time_bars(path, date_where(symbol, date_from, date_to, session), interval)

@app.get("/bars/volume")
def volume_bars(path: str=Query(...), symbol: str=Query("ESM4"), threshold: int=Query(1000),
                date_from: Optional[str]=Query(None), date_to: Optional[str]=Query(None),
                session: str=Query("ALL")):
    ensure_file(path)
    return _agg(_fetch_ticks(path, date_where(symbol, date_from, date_to, session)), "volume", float(threshold))

@app.get("/bars/tick")
def tick_bars(path: str=Query(...), symbol: str=Query("ESM4"), threshold: int=Query(500),
              date_from: Optional[str]=Query(None), date_to: Optional[str]=Query(None),
              session: str=Query("ALL")):
    ensure_file(path)
    return _agg(_fetch_ticks(path, date_where(symbol, date_from, date_to, session)), "tick", float(threshold))

@app.get("/bars/range")
def range_bars(path: str=Query(...), symbol: str=Query("ESM4"), threshold: float=Query(4.0),
               date_from: Optional[str]=Query(None), date_to: Optional[str]=Query(None),
               session: str=Query("ALL")):
    ensure_file(path)
    return _agg(_fetch_ticks(path, date_where(symbol, date_from, date_to, session)), "range", threshold)


# ─────────────────────────────────────────────────────────────────────────────
# Backtest
# ─────────────────────────────────────────────────────────────────────────────
class BacktestRequest(BaseModel):
    path:            str
    symbol:          str   = "ESM4"
    bar_type:        str   = "time"
    interval:        str   = "5min"
    threshold:       float = 1000
    date_from:       Optional[str] = None
    date_to:         Optional[str] = None
    session:         str   = "ALL"
    strategy:        str
    initial_capital: float = 100_000
    commission:      float = 2.0
    slippage:        float = 0.25
    contract_size:   float = 50.0


@app.post("/backtest")
def run_backtest(req: BacktestRequest):
    try:
        ensure_file(req.path)
        w = date_where(req.symbol, req.date_from, req.date_to, req.session)

        if req.bar_type == "time":
            bars = _build_time_bars(req.path, w, req.interval)
        else:
            bars = _agg(_fetch_ticks(req.path, w), req.bar_type, req.threshold)

        if not bars:
            return {"success": False, "error": "No bars generated. Check symbol, date range, or threshold."}

        trades, equity, err = _run_strategy(bars, req)
        if err:
            return {"success": False, "error": err}

        stats = _stats(trades, equity, req.initial_capital)
        mc    = _monte_carlo(trades, req.initial_capital) if len(trades) >= 2 else {}

        return {
            "success":      True,
            "trades":       trades,
            "equity":       equity,
            "stats":        stats,
            "monte_carlo":  mc,
            "bar_count":    len(bars),
        }

    except HTTPException:
        raise
    except Exception:
        return {"success": False, "error": traceback.format_exc()}


# ── Safe builtins for strategy sandbox ───────────────────────────────────
SAFE_BUILTINS = {
    k: v for k, v in __builtins__.items()
    if k in {
        "abs","round","min","max","sum","len","range","enumerate","zip",
        "list","dict","tuple","set","int","float","str","bool",
        "print","isinstance","hasattr","getattr","setattr",
        "any","all","sorted","reversed","map","filter",
        "True","False","None",
        "ValueError","TypeError","IndexError","KeyError",
    }
} if isinstance(__builtins__, dict) else {
    "abs": abs, "round": round, "min": min, "max": max,
    "sum": sum, "len": len, "range": range,
    "enumerate": enumerate, "zip": zip,
    "list": list, "dict": dict, "tuple": tuple, "set": set,
    "int": int, "float": float, "str": str, "bool": bool,
    "print": print, "isinstance": isinstance,
    "hasattr": hasattr, "getattr": getattr, "setattr": setattr,
    "any": any, "all": all,
    "sorted": sorted, "reversed": reversed,
    "map": map, "filter": filter,
    "True": True, "False": False, "None": None,
    "ValueError": ValueError, "TypeError": TypeError,
}


def _run_strategy(bars: list, req: BacktestRequest):
    trades  = []
    equity  = []
    cash    = req.initial_capital
    pos     = 0          # +N long, -N short, 0 flat
    ep      = 0.0        # entry price
    et      = 0          # entry time (unix)
    sl      = req.slippage
    cs      = req.contract_size
    comm    = req.commission   # $/side/contract

    class Ctx:
        __slots__ = ("position","entry_price","cash","data","bars_seen")
        def __init__(self):
            self.position    = 0
            self.entry_price = 0.0
            self.cash        = req.initial_capital
            self.data        = {}
            self.bars_seen   = 0

    ctx = Ctx()
    g   = {"__builtins__": SAFE_BUILTINS, "np": np, "math": math}

    try:
        exec(req.strategy, g)
    except SyntaxError as e:
        return [], [], f"Syntax error in strategy:\n  {e}"
    except Exception:
        return [], [], f"Compile error:\n{traceback.format_exc()}"

    init_fn   = g.get("initialize")
    on_bar_fn = g.get("on_bar")

    if not on_bar_fn:
        return [], [], "Function `on_bar(bar, ctx, history)` not found in strategy."

    if init_fn:
        try:
            init_fn(ctx)
        except Exception as e:
            return [], [], f"initialize() error: {e}"

    history = []
    for i, bar in enumerate(bars):
        ctx.bars_seen = i
        history.append(bar)

        try:
            sig = on_bar_fn(bar, ctx, history)
        except Exception:
            return [], [], f"on_bar() error at bar {i} (t={bar.get('time')}):\n{traceback.format_exc()}"

        # ── Parse signal ─────────────────────────────────────────────
        if sig is None:
            action, size = None, 1
        elif isinstance(sig, (tuple, list)) and len(sig) >= 1:
            action = sig[0]
            size   = max(1, int(abs(sig[1]))) if len(sig) > 1 and sig[1] else 1
        else:
            action, size = sig, 1

        size  = max(1, int(size or 1))
        close = float(bar["close"])

        # ─────────────────────────────────────────────────────────────
        # BUY / LONG
        # ─────────────────────────────────────────────────────────────
        if action in ("BUY", "LONG") and pos <= 0:
            if pos < 0:
                # Close existing short
                fill = close + sl
                # FIX: only exit commission here — entry comm was paid when shorting
                pnl  = (ep - fill) * abs(pos) * cs - comm * abs(pos)
                cash += pnl
                trades.append({
                    "entry_time": et, "exit_time": bar["time"],
                    "side": "SHORT", "entry": ep,
                    "exit": fill, "size": abs(pos),
                    "pnl": round(pnl, 2),
                })
                pos = 0

            # Open long
            fill = close + sl
            pos  = size
            ep   = fill
            et   = bar["time"]
            cash -= comm * size   # entry commission
            ctx.position    = pos
            ctx.entry_price = ep

        # ─────────────────────────────────────────────────────────────
        # SELL / SHORT
        # ─────────────────────────────────────────────────────────────
        elif action in ("SELL", "SHORT") and pos >= 0:
            if pos > 0:
                # Close existing long
                fill = close - sl
                # FIX: only exit commission here — entry comm was paid when buying
                pnl  = (fill - ep) * pos * cs - comm * pos
                cash += pnl
                trades.append({
                    "entry_time": et, "exit_time": bar["time"],
                    "side": "LONG", "entry": ep,
                    "exit": fill, "size": pos,
                    "pnl": round(pnl, 2),
                })
                pos = 0

            # Open short
            fill = close - sl
            pos  = -size
            ep   = fill
            et   = bar["time"]
            cash -= comm * size   # entry commission
            ctx.position    = pos
            ctx.entry_price = ep

        # ─────────────────────────────────────────────────────────────
        # CLOSE
        # ─────────────────────────────────────────────────────────────
        elif action == "CLOSE" and pos != 0:
            if pos > 0:
                fill = close - sl
                # FIX: only exit commission — entry was already charged
                pnl  = (fill - ep) * pos * cs - comm * pos
                side = "LONG"
            else:
                fill = close + sl
                pnl  = (ep - fill) * abs(pos) * cs - comm * abs(pos)
                side = "SHORT"

            cash += pnl
            trades.append({
                "entry_time": et, "exit_time": bar["time"],
                "side": side, "entry": ep,
                "exit": fill, "size": abs(pos),
                "pnl": round(pnl, 2),
            })
            pos = 0
            ep  = 0.0
            ctx.position    = 0
            ctx.entry_price = 0.0

        # ── Open PnL for equity curve ────────────────────────────────
        open_pnl = 0.0
        if   pos > 0:  open_pnl = (close - ep) * pos       * cs
        elif pos < 0:  open_pnl = (ep - close)  * abs(pos) * cs

        ctx.cash = cash
        equity.append({"time": bar["time"], "equity": round(cash + open_pnl, 2)})

    # Close any open position at end of data
    if pos != 0 and bars:
        last = bars[-1]
        close = float(last["close"])
        if pos > 0:
            fill = close - sl
            pnl  = (fill - ep) * pos * cs - comm * pos
            side = "LONG"
        else:
            fill = close + sl
            pnl  = (ep - fill) * abs(pos) * cs - comm * abs(pos)
            side = "SHORT"
        cash += pnl
        trades.append({
            "entry_time": et, "exit_time": last["time"],
            "side": side, "entry": ep,
            "exit": fill, "size": abs(pos),
            "pnl": round(pnl, 2),
            "forced_close": True,
        })

    return trades, equity, None


# ─────────────────────────────────────────────────────────────────────────────
# Statistics
# ─────────────────────────────────────────────────────────────────────────────
def _stats(trades: list, equity: list, initial: float) -> dict:
    if not trades:
        return {"total_trades": 0, "initial_capital": initial, "note": "No trades executed."}

    pnls   = np.array([t["pnl"] for t in trades], dtype=float)
    wins   = pnls[pnls > 0]
    losses = pnls[pnls <= 0]
    eqs    = np.array([e["equity"] for e in equity], dtype=float)

    # Drawdown
    peak   = np.maximum.accumulate(np.maximum(eqs, initial))
    dd_arr = np.where(peak > 0, (peak - eqs) / peak * 100, 0)
    max_dd = float(dd_arr.max())

    final_eq  = float(eqs[-1]) if len(eqs) else initial
    wr        = float(len(wins) / len(pnls))
    gp        = float(wins.sum())        if len(wins)   else 0.0
    gl        = float(abs(losses.sum())) if len(losses) else 0.0
    aw        = float(wins.mean())       if len(wins)   else 0.0
    al        = float(losses.mean())     if len(losses) else 0.0
    total_ret = (final_eq - initial) / initial

    # Annualised Sharpe (per-trade)
    pnl_std = float(pnls.std(ddof=1)) if len(pnls) > 1 else 1e-9
    sharpe  = round(float(pnls.mean()) / pnl_std * math.sqrt(252), 3)

    # Sortino
    neg_pnls = pnls[pnls < 0]
    dstd     = float(neg_pnls.std(ddof=1)) if len(neg_pnls) > 1 else 1e-9
    sortino  = round(float(pnls.mean()) / dstd * math.sqrt(252), 3)

    # Calmar
    calmar = round(total_ret * 100 / max_dd, 3) if max_dd > 0 else 999.0

    # Avg trade duration (seconds)
    durations = [t["exit_time"] - t["entry_time"] for t in trades if t.get("exit_time") and t.get("entry_time")]
    avg_dur   = int(sum(durations) / len(durations)) if durations else 0

    return {
        "total_trades":       len(trades),
        "winning_trades":     int(len(wins)),
        "losing_trades":      int(len(losses)),
        "win_rate":           round(wr * 100, 2),
        "total_pnl":          round(float(pnls.sum()), 2),
        "gross_profit":       round(gp, 2),
        "gross_loss":         round(gl, 2),
        "profit_factor":      round(gp / gl, 3) if gl > 0 else 999.0,
        "avg_win":            round(aw, 2),
        "avg_loss":           round(al, 2),
        "rr_ratio":           round(abs(aw / al), 3) if al else 0.0,
        "expectancy":         round(wr * aw + (1 - wr) * al, 2),
        "max_drawdown_pct":   round(max_dd, 2),
        "sharpe_ratio":       sharpe,
        "sortino_ratio":      sortino,
        "calmar_ratio":       calmar,
        "total_return_pct":   round(total_ret * 100, 2),
        "final_equity":       round(final_eq, 2),
        "initial_capital":    initial,
        "best_trade":         round(float(pnls.max()), 2),
        "worst_trade":        round(float(pnls.min()), 2),
        "avg_trade":          round(float(pnls.mean()), 2),
        "avg_trade_duration": avg_dur,
        "max_consec_wins":    _consec(pnls, True),
        "max_consec_losses":  _consec(pnls, False),
    }


def _consec(pnls: np.ndarray, wins: bool) -> int:
    mx = cur = 0
    for p in pnls:
        hit = (wins and p > 0) or (not wins and p <= 0)
        cur = cur + 1 if hit else 0
        mx  = max(mx, cur)
    return int(mx)


# ─────────────────────────────────────────────────────────────────────────────
# Monte Carlo
# ─────────────────────────────────────────────────────────────────────────────
def _monte_carlo(trades: list, initial: float, n_sim: int = 500) -> dict:
    if len(trades) < 2:
        return {}

    pnls = np.array([t["pnl"] for t in trades], dtype=float)
    n    = len(pnls)
    rng  = np.random.default_rng()

    final_eq = np.empty(n_sim)
    max_dd   = np.empty(n_sim)
    paths    = []   # first 100 full paths

    for s in range(n_sim):
        r     = rng.choice(pnls, size=n, replace=True)
        eq    = np.empty(n + 1)
        eq[0] = initial
        np.cumsum(r, out=eq[1:])
        eq[1:] += initial

        pk   = np.maximum.accumulate(eq)
        safe = np.where(pk > 0, pk, 1.0)
        dd   = float(((pk - eq) / safe * 100).max())

        final_eq[s] = float(eq[-1])
        max_dd[s]   = dd

        if s < 100:
            paths.append([round(x, 2) for x in eq.tolist()])

    return {
        "n_simulations":   n_sim,
        "paths":           paths,
        "final_eq_p5":     round(float(np.percentile(final_eq,  5)), 2),
        "final_eq_p25":    round(float(np.percentile(final_eq, 25)), 2),
        "final_eq_median": round(float(np.percentile(final_eq, 50)), 2),
        "final_eq_p75":    round(float(np.percentile(final_eq, 75)), 2),
        "final_eq_p95":    round(float(np.percentile(final_eq, 95)), 2),
        "max_dd_p25":      round(float(np.percentile(max_dd, 25)), 2),
        "max_dd_p50":      round(float(np.percentile(max_dd, 50)), 2),
        "max_dd_p75":      round(float(np.percentile(max_dd, 75)), 2),
        "max_dd_p95":      round(float(np.percentile(max_dd, 95)), 2),
        "prob_profit":     round(float((final_eq > initial).mean() * 100), 2),
        "prob_ruin":       round(float((final_eq < initial * 0.5).mean() * 100), 2),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Export
# ─────────────────────────────────────────────────────────────────────────────
class ExportRequest(BaseModel):
    trades: list
    stats:  dict


@app.post("/export/trades")
def export_trades_csv(req: ExportRequest):
    """Stream trades as CSV for download."""
    buf = io.StringIO()
    w   = csv.writer(buf)

    w.writerow(["#","Side","Entry Time","Exit Time","Entry Price","Exit Price","Size","PnL ($)"])
    for i, t in enumerate(req.trades, 1):
        entry_dt = _fmt_dt(t.get("entry_time", 0))
        exit_dt  = _fmt_dt(t.get("exit_time",  0))
        w.writerow([
            i,
            t.get("side", ""),
            entry_dt,
            exit_dt,
            t.get("entry", ""),
            t.get("exit", ""),
            t.get("size", ""),
            t.get("pnl", ""),
        ])

    w.writerow([])
    w.writerow(["STATS"])
    for k, v in req.stats.items():
        w.writerow([k, v])

    buf.seek(0)
    return StreamingResponse(
        iter([buf.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=trades.csv"},
    )


def _fmt_dt(ts: int) -> str:
    from datetime import datetime, timezone
    try:
        return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(ts)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8765, log_level="info")