from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
import duckdb, os, math, traceback
import numpy as np

app = FastAPI()
app.add_middleware(CORSMiddleware,
    allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
con = duckdb.connect()

# ── Helpers ───────────────────────────────────────────────────────────────
def ensure_file(path: str):
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail=f"File not found: {path}")

def _safe(v):
    if v is None: return None
    try:
        f = float(v)
        return None if (math.isnan(f) or math.isinf(f)) else f
    except Exception:
        return None

def safe_round(v, n=4):
    s = _safe(v)
    return round(s, n) if s is not None else None

def clean_ohlcv(rows):
    """
    Expected columns per row:
    0:time 1:open 2:high 3:low 4:close 5:volume
    6:bid 7:ask 8:vwap
    9:buy_vol 10:sell_vol
    11:avg_bid_sz 12:avg_ask_sz 13:max_bid_sz 14:max_ask_sz
    """
    result = []
    for r in rows:
        # Skip bars with invalid OHLC
        if any(_safe(r[i]) is None for i in range(1, 5)):
            continue

        vol      = _safe(r[5]) or 0.0
        buy_vol  = _safe(r[9])  or 0.0
        sell_vol = _safe(r[10]) or 0.0
        delta    = round(buy_vol - sell_vol, 2)

        b_sz = safe_round(r[11], 2)
        a_sz = safe_round(r[12], 2)
        size_imb = None
        if b_sz and a_sz and (b_sz + a_sz) > 0:
            size_imb = round((b_sz - a_sz) / (b_sz + a_sz), 4)

        bar = {
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
            "cum_delta":  0.0,  # filled below
        }
        result.append(bar)

    # Cumulative delta
    cd = 0.0
    for bar in result:
        cd += bar["delta"]
        bar["cum_delta"] = round(cd, 2)

    return result


def date_where(symbol: str, date_from, date_to):
    # Always filter action='T' (trades only)
    w = f"WHERE symbol='{symbol}' AND action='T'"
    if date_from:
        w += f" AND ts_recv >= '{date_from} 00:00:00+00'"
    if date_to:
        w += f" AND ts_recv <= '{date_to} 23:59:59+00'"
    return w


OF_COLS = """
    SUM(size)                                                   AS vol,
    AVG(bid_px_00)                                              AS bid,
    AVG(ask_px_00)                                              AS ask,
    SUM(price * size) / NULLIF(SUM(size), 0)                   AS vwap,
    SUM(CASE WHEN side='B' THEN size ELSE 0 END)               AS buy_vol,
    SUM(CASE WHEN side='A' THEN size ELSE 0 END)               AS sell_vol,
    AVG(bid_sz_00)                                              AS avg_bid_sz,
    AVG(ask_sz_00)                                              AS avg_ask_sz,
    MAX(bid_sz_00)                                              AS max_bid_sz,
    MAX(ask_sz_00)                                              AS max_ask_sz
"""

# ── LOAD / SYMBOLS ────────────────────────────────────────────────────────
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


# ── TIME BARS ─────────────────────────────────────────────────────────────
@app.get("/bars/time")
def time_bars(
    path:      str           = Query(...),
    symbol:    str           = Query("ESM4"),
    interval:  str           = Query("5min"),
    date_from: Optional[str] = Query(None),
    date_to:   Optional[str] = Query(None),
):
    ensure_file(path)
    interval_map = {
        "1min":  "1 minute",
        "5min":  "5 minutes",
        "15min": "15 minutes",
        "30min": "30 minutes",
        "1h":    "1 hour",
    }
    di   = interval_map.get(interval, "5 minutes")
    w    = date_where(symbol, date_from, date_to)
    rows = con.execute(f"""
        SELECT
            epoch(time_bucket(INTERVAL '{di}', ts_recv::TIMESTAMPTZ)) AS t,
            FIRST(price ORDER BY ts_recv)  AS o,
            MAX(price)                     AS h,
            MIN(price)                     AS l,
            LAST(price ORDER BY ts_recv)   AS c,
            {OF_COLS}
        FROM read_parquet('{path}') {w}
        GROUP BY 1
        ORDER BY 1
    """).fetchall()
    return clean_ohlcv(rows)


# ── VOLUME / TICK / RANGE BARS ────────────────────────────────────────────
def _fetch_ticks(path: str, w: str):
    return con.execute(f"""
        SELECT ts_recv, price, size, side,
               bid_px_00, ask_px_00, bid_sz_00, ask_sz_00
        FROM read_parquet('{path}') {w}
        ORDER BY ts_recv
    """).fetchall()


def _agg(rows, mode: str, threshold: float):
    """Aggregate tick data into volume / tick / range bars."""
    if not rows:
        return []

    bars = []

    def make_bar(t, o, h, l, c, vol, ticks,
                 buy_v, sell_v, sum_bid, sum_ask,
                 sum_bsz, sum_asz, max_bsz, max_asz):
        ts_int   = int(t.timestamp()) if hasattr(t, "timestamp") else int(t)
        vol      = vol  or 1e-9
        buy_v    = buy_v  or 0.0
        sell_v   = sell_v or 0.0
        delta    = round(buy_v - sell_v, 2)
        n        = ticks or 1
        bid      = round(sum_bid / n, 4)
        ask      = round(sum_ask / n, 4)
        ab_sz    = round(sum_bsz / n, 2)
        aa_sz    = round(sum_asz / n, 2)
        mb_sz    = round(max_bsz, 2) if max_bsz else None
        ma_sz    = round(max_asz, 2) if max_asz else None
        size_imb = None
        if ab_sz and aa_sz and (ab_sz + aa_sz) > 0:
            size_imb = round((ab_sz - aa_sz) / (ab_sz + aa_sz), 4)
        return {
            "time":       ts_int,
            "open":       o, "high": h, "low": l, "close": c,
            "volume":     round(vol, 2),
            "bid":        bid, "ask": ask, "vwap": None,
            "buy_vol":    round(buy_v,  2),
            "sell_vol":   round(sell_v, 2),
            "delta":      delta,
            "delta_pct":  round(delta / vol * 100, 2),
            "avg_bid_sz": ab_sz, "avg_ask_sz": aa_sz,
            "max_bid_sz": mb_sz, "max_ask_sz": ma_sz,
            "size_imb":   size_imb,
            "cum_delta":  0.0,
        }

    # Initialise from first tick
    ts0, p0, sz0, sd0, b0, a0, bsz0, asz0 = rows[0]
    o = h = l = c = float(p0)
    vol = ticks = buy_v = sell_v = 0
    sum_bid = sum_ask = sum_bsz = sum_asz = 0.0
    max_bsz = max_asz = 0.0
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
            buy_v  += s
        else:
            sell_v += s

        sum_bid += float(bid  or 0)
        sum_ask += float(ask  or 0)
        sum_bsz += float(bsz  or 0)
        sum_asz += float(asz  or 0)
        max_bsz  = max(max_bsz, float(bsz or 0))
        max_asz  = max(max_asz, float(asz or 0))

        done = (
            (mode == "volume" and vol   >= threshold) or
            (mode == "tick"   and ticks >= threshold) or
            (mode == "range"  and (h - l) >= threshold)
        )
        if done:
            bars.append(make_bar(t, o, h, l, c, vol, ticks,
                                  buy_v, sell_v, sum_bid, sum_ask,
                                  sum_bsz, sum_asz, max_bsz, max_asz))
            # Reset for next bar — start with this tick's price
            o = h = l = c = price
            vol = ticks = buy_v = sell_v = 0
            sum_bid = sum_ask = sum_bsz = sum_asz = 0.0
            max_bsz = max_asz = 0.0
            t = ts

    # Flush last incomplete bar
    if ticks > 0:
        bars.append(make_bar(t, o, h, l, c, vol, ticks,
                              buy_v, sell_v, sum_bid, sum_ask,
                              sum_bsz, sum_asz, max_bsz, max_asz))

    # Cumulative delta
    cd = 0.0
    for bar in bars:
        cd += bar["delta"]
        bar["cum_delta"] = round(cd, 2)

    return bars


@app.get("/bars/volume")
def volume_bars(
    path:      str           = Query(...),
    symbol:    str           = Query("ESM4"),
    threshold: int           = Query(1000),
    date_from: Optional[str] = Query(None),
    date_to:   Optional[str] = Query(None),
):
    ensure_file(path)
    return _agg(_fetch_ticks(path, date_where(symbol, date_from, date_to)),
                "volume", float(threshold))


@app.get("/bars/tick")
def tick_bars(
    path:      str           = Query(...),
    symbol:    str           = Query("ESM4"),
    threshold: int           = Query(500),
    date_from: Optional[str] = Query(None),
    date_to:   Optional[str] = Query(None),
):
    ensure_file(path)
    return _agg(_fetch_ticks(path, date_where(symbol, date_from, date_to)),
                "tick", float(threshold))


@app.get("/bars/range")
def range_bars(
    path:      str           = Query(...),
    symbol:    str           = Query("ESM4"),
    threshold: float         = Query(4.0),
    date_from: Optional[str] = Query(None),
    date_to:   Optional[str] = Query(None),
):
    ensure_file(path)
    return _agg(_fetch_ticks(path, date_where(symbol, date_from, date_to)),
                "range", threshold)


# ── BACKTEST ──────────────────────────────────────────────────────────────
class BacktestRequest(BaseModel):
    path:            str
    symbol:          str   = "ESM4"
    bar_type:        str   = "time"
    interval:        str   = "5min"
    threshold:       float = 1000
    date_from:       Optional[str] = None
    date_to:         Optional[str] = None
    strategy:        str
    initial_capital: float = 100_000
    commission:      float = 2.0
    slippage:        float = 0.25
    contract_size:   float = 50.0


@app.post("/backtest")
def run_backtest(req: BacktestRequest):
    try:
        ensure_file(req.path)
        w = date_where(req.symbol, req.date_from, req.date_to)

        # Build bars
        if req.bar_type == "time":
            im = {
                "1min":  "1 minute",  "5min":  "5 minutes",
                "15min": "15 minutes","30min": "30 minutes",
                "1h":    "1 hour",
            }
            di  = im.get(req.interval, "5 minutes")
            raw = con.execute(f"""
                SELECT
                    epoch(time_bucket(INTERVAL '{di}', ts_recv::TIMESTAMPTZ)) AS t,
                    FIRST(price ORDER BY ts_recv) AS o,
                    MAX(price) AS h, MIN(price) AS l,
                    LAST(price ORDER BY ts_recv) AS c,
                    {OF_COLS}
                FROM read_parquet('{req.path}') {w}
                GROUP BY 1 ORDER BY 1
            """).fetchall()
            bars = clean_ohlcv(raw)
        else:
            bars = _agg(
                _fetch_ticks(req.path, w),
                req.bar_type,
                req.threshold,
            )

        if not bars:
            return {
                "success": False,
                "error":   "No bars generated. Check symbol, date range, or threshold.",
            }

        trades, equity, err = _run_strategy(bars, req)
        if err:
            return {"success": False, "error": err}

        stats = _stats(trades, equity, req.initial_capital)
        mc    = _monte_carlo(trades, req.initial_capital) if len(trades) >= 2 else {}
        return {
            "success": True,
            "trades":  trades,
            "equity":  equity,
            "stats":   stats,
            "monte_carlo": mc,
        }

    except HTTPException:
        raise
    except Exception:
        return {"success": False, "error": traceback.format_exc()}


# ── Safe builtins for exec ────────────────────────────────────────────────
SAFE_BUILTINS = {
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


def _run_strategy(bars, req: BacktestRequest):
    trades  = []
    equity  = []
    cash    = req.initial_capital
    position    = 0
    entry_price = 0.0
    entry_time  = 0

    class Ctx:
        def __init__(self):
            self.position    = 0
            self.entry_price = 0.0
            self.cash        = req.initial_capital
            self.data        = {}
            self.bars_seen   = 0

    ctx = Ctx()
    g   = {"__builtins__": SAFE_BUILTINS, "np": np, "math": math}

    # Compile strategy code
    try:
        exec(req.strategy, g)
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
            return [], [], f"on_bar() error at bar {i}:\n{traceback.format_exc()}"

        # Parse signal
        if sig is None:
            action, size = None, 1
        elif isinstance(sig, (tuple, list)):
            action = sig[0] if len(sig) > 0 else None
            size   = int(abs(sig[1])) if len(sig) > 1 and sig[1] else 1
        else:
            action, size = sig, 1

        size  = max(1, size or 1)
        close = float(bar["close"])
        slip  = req.slippage
        cs    = req.contract_size
        comm  = req.commission

        # ── BUY / LONG ──────────────────────────────────────────────
        if action in ("BUY", "LONG") and position <= 0:
            # Close short first
            if position < 0:
                fill = close + slip
                pnl  = (entry_price - fill) * abs(position) * cs - comm * 2 * abs(position)
                cash += pnl
                trades.append({
                    "entry_time": entry_time, "exit_time": bar["time"],
                    "side": "SHORT", "entry": entry_price,
                    "exit": fill, "size": abs(position),
                    "pnl": round(pnl, 2),
                })
                position = 0

            fill        = close + slip
            position    = size
            entry_price = fill
            entry_time  = bar["time"]
            cash       -= comm * size
            ctx.position    = position
            ctx.entry_price = entry_price

        # ── SELL / SHORT ─────────────────────────────────────────────
        elif action in ("SELL", "SHORT") and position >= 0:
            # Close long first
            if position > 0:
                fill = close - slip
                pnl  = (fill - entry_price) * position * cs - comm * 2 * position
                cash += pnl
                trades.append({
                    "entry_time": entry_time, "exit_time": bar["time"],
                    "side": "LONG", "entry": entry_price,
                    "exit": fill, "size": position,
                    "pnl": round(pnl, 2),
                })
                position = 0

            fill        = close - slip
            position    = -size
            entry_price = fill
            entry_time  = bar["time"]
            cash       -= comm * size
            ctx.position    = position
            ctx.entry_price = entry_price

        # ── CLOSE ────────────────────────────────────────────────────
        elif action == "CLOSE" and position != 0:
            if position > 0:
                fill = close - slip
                pnl  = (fill - entry_price) * position * cs - comm * 2 * position
                side = "LONG"
            else:
                fill = close + slip
                pnl  = (entry_price - fill) * abs(position) * cs - comm * 2 * abs(position)
                side = "SHORT"

            cash += pnl
            trades.append({
                "entry_time": entry_time, "exit_time": bar["time"],
                "side": side, "entry": entry_price,
                "exit": fill, "size": abs(position),
                "pnl": round(pnl, 2),
            })
            position    = 0
            entry_price = 0.0
            ctx.position    = 0
            ctx.entry_price = 0.0

        # Open PnL for equity curve
        open_pnl = 0.0
        if   position > 0: open_pnl = (close - entry_price) * position      * cs
        elif position < 0: open_pnl = (entry_price - close) * abs(position) * cs

        ctx.cash = cash
        equity.append({"time": bar["time"], "equity": round(cash + open_pnl, 2)})

    return trades, equity, None


# ── STATISTICS ────────────────────────────────────────────────────────────
def _stats(trades, equity, initial: float) -> dict:
    if not trades:
        return {"total_trades": 0, "note": "No trades executed."}

    pnls   = np.array([t["pnl"] for t in trades], dtype=float)
    wins   = pnls[pnls > 0]
    losses = pnls[pnls <= 0]
    eqs    = np.array([e["equity"] for e in equity], dtype=float)

    # Drawdown
    peak     = np.maximum.accumulate(np.maximum(eqs, initial))
    dd_arr   = (peak - eqs) / np.where(peak > 0, peak, 1) * 100
    max_dd   = float(dd_arr.max())

    final_eq = float(eqs[-1])
    wr       = float(len(wins) / len(pnls))
    gp       = float(wins.sum())        if len(wins)   else 0.0
    gl       = float(abs(losses.sum())) if len(losses) else 0.0
    aw       = float(wins.mean())       if len(wins)   else 0.0
    al       = float(losses.mean())     if len(losses) else 0.0

    total_ret = (final_eq - initial) / initial

    # Sharpe (annualised, treat each trade as one period)
    pnl_std   = float(pnls.std()) if pnls.std() > 0 else 1e-9
    sharpe    = round(float(pnls.mean()) / pnl_std * math.sqrt(252), 3)

    # Sortino
    neg_pnls  = pnls[pnls < 0]
    dstd      = float(neg_pnls.std()) if len(neg_pnls) > 1 else 1e-9
    sortino   = round(float(pnls.mean()) / dstd * math.sqrt(252), 3)

    calmar    = round(total_ret * 100 / max_dd, 3) if max_dd > 0 else 999.0

    return {
        "total_trades":    len(trades),
        "winning_trades":  int(len(wins)),
        "losing_trades":   int(len(losses)),
        "win_rate":        round(wr * 100, 2),
        "total_pnl":       round(float(pnls.sum()), 2),
        "gross_profit":    round(gp, 2),
        "gross_loss":      round(gl, 2),
        "profit_factor":   round(gp / gl, 3) if gl > 0 else 999.0,
        "avg_win":         round(aw, 2),
        "avg_loss":        round(al, 2),
        "rr_ratio":        round(abs(aw / al), 3) if al else 0.0,
        "expectancy":      round(wr * aw + (1 - wr) * al, 2),
        "max_drawdown_pct":round(max_dd, 2),
        "sharpe_ratio":    sharpe,
        "sortino_ratio":   sortino,
        "calmar_ratio":    calmar,
        "total_return_pct":round(total_ret * 100, 2),
        "final_equity":    round(final_eq, 2),
        "initial_capital": initial,
        "best_trade":      round(float(pnls.max()), 2),
        "worst_trade":     round(float(pnls.min()), 2),
        "avg_trade":       round(float(pnls.mean()), 2),
        "max_consec_wins": _consec(pnls, True),
        "max_consec_losses": _consec(pnls, False),
    }


def _consec(pnls: np.ndarray, wins: bool) -> int:
    mx = cur = 0
    for p in pnls:
        hit = (wins and p > 0) or (not wins and p <= 0)
        cur = cur + 1 if hit else 0
        mx  = max(mx, cur)
    return int(mx)


# ── MONTE CARLO ───────────────────────────────────────────────────────────
def _monte_carlo(trades: list, initial: float, n_sim: int = 500) -> dict:
    if len(trades) < 2:
        return {}

    pnls = np.array([t["pnl"] for t in trades], dtype=float)
    n    = len(pnls)

    final_eq = []
    max_dd   = []
    paths    = []          # store first 100 paths

    rng = np.random.default_rng()  # modern RNG (fixes deprecation)

    for s in range(n_sim):
        r    = rng.choice(pnls, size=n, replace=True)
        eq   = np.empty(n + 1)
        eq[0] = initial
        np.cumsum(r, out=eq[1:])
        eq[1:] += initial

        pk   = np.maximum.accumulate(eq)
        safe = np.where(pk > 0, pk, 1.0)
        dd   = float(((pk - eq) / safe * 100).max())

        final_eq.append(float(eq[-1]))
        max_dd.append(dd)
        if s < 100:
            paths.append([round(x, 2) for x in eq.tolist()])

    fe = np.array(final_eq)
    md = np.array(max_dd)

    return {
        "n_simulations":   n_sim,
        "paths":           paths,
        "final_eq_p5":     round(float(np.percentile(fe,  5)), 2),
        "final_eq_p25":    round(float(np.percentile(fe, 25)), 2),
        "final_eq_median": round(float(np.percentile(fe, 50)), 2),
        "final_eq_p75":    round(float(np.percentile(fe, 75)), 2),
        "final_eq_p95":    round(float(np.percentile(fe, 95)), 2),
        "max_dd_p50":      round(float(np.percentile(md, 50)), 2),
        "max_dd_p95":      round(float(np.percentile(md, 95)), 2),
        "prob_profit":     round(float((fe > initial).mean() * 100), 2),
        "prob_ruin":       round(float((fe < initial * 0.5).mean() * 100), 2),
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8765, log_level="info")