from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
import duckdb, os, math, traceback
import numpy as np
import builtins

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
con = duckdb.connect()

def ensure_file(path):
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail=f"File tidak ditemukan: {path}")

def clean_ohlcv(rows, has_extras=False):
    result = []
    for r in rows:
        if any(v is None or (isinstance(v, float) and math.isnan(v)) for v in r[:5]):
            continue
        bar = {
            "time":   int(r[0]),
            "open":   float(r[1]),
            "high":   float(r[2]),
            "low":    float(r[3]),
            "close":  float(r[4]),
            "volume": float(r[5] or 0),
        }
        if has_extras and len(r) > 6:
            bar["bid"]  = round(float(r[6]), 4) if r[6] is not None and not math.isnan(float(r[6])) else None
            bar["ask"]  = round(float(r[7]), 4) if r[7] is not None and not math.isnan(float(r[7])) else None
            bar["vwap"] = round(float(r[8]), 4) if r[8] is not None and not math.isnan(float(r[8])) else None
        result.append(bar)
    return result

def date_where(symbol, date_from, date_to, extra=""):
    # FIX: filter action='T' (trades only — exclude quote-only rows)
    w = f"WHERE symbol='{symbol}' AND action='T'{extra}"
    if date_from:
        w += f" AND ts_recv >= '{date_from} 00:00:00+00'"
    if date_to:
        w += f" AND ts_recv <= '{date_to} 23:59:59+00'"
    return w

# ── FIX: was @app.post — frontend calls GET ──────────────────────────────
@app.get("/load")
def load_file(path: str = Query(...)):
    ensure_file(path)
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

@app.get("/symbols")
def get_symbols(path: str = Query(...)):
    ensure_file(path)
    rows = con.execute(f"""
        SELECT symbol, COUNT(*) as cnt
        FROM read_parquet('{path}')
        WHERE action='T'
        GROUP BY symbol ORDER BY 2 DESC
    """).fetchall()
    return [{"symbol": r[0], "count": r[1]} for r in rows]

@app.get("/bars/time")
def time_bars(
    path:      str           = Query(...),
    symbol:    str           = Query("ESM4"),
    interval:  str           = Query("5min"),
    date_from: Optional[str] = Query(None),
    date_to:   Optional[str] = Query(None),
):
    ensure_file(path)
    w  = date_where(symbol, date_from, date_to)
    im = {"1min":"1 minute","5min":"5 minutes","15min":"15 minutes","30min":"30 minutes","1h":"1 hour"}
    di = im.get(interval, "5 minutes")

    rows = con.execute(f"""
        SELECT
            epoch(time_bucket(INTERVAL '{di}', ts_recv::TIMESTAMPTZ)) AS t,
            FIRST(price ORDER BY ts_recv)                               AS o,
            MAX(price)                                                  AS h,
            MIN(price)                                                  AS l,
            LAST(price ORDER BY ts_recv)                                AS c,
            SUM(size)                                                   AS vol,
            AVG(bid_px_00)                                              AS bid,
            AVG(ask_px_00)                                              AS ask,
            SUM(price * size) / NULLIF(SUM(size), 0)                   AS vwap
        FROM read_parquet('{path}') {w}
        GROUP BY 1 ORDER BY 1
    """).fetchall()
    return clean_ohlcv(rows, has_extras=True)

@app.get("/bars/volume")
def volume_bars(
    path:      str           = Query(...),
    symbol:    str           = Query("ESM4"),
    threshold: int           = Query(1000),
    date_from: Optional[str] = Query(None),
    date_to:   Optional[str] = Query(None),
):
    ensure_file(path)
    w = date_where(symbol, date_from, date_to)
    rows = con.execute(f"""
        SELECT ts_recv, price, size, bid_px_00, ask_px_00
        FROM read_parquet('{path}') {w} ORDER BY ts_recv
    """).fetchall()
    return _agg(rows, "volume", threshold)

@app.get("/bars/tick")
def tick_bars(
    path:      str           = Query(...),
    symbol:    str           = Query("ESM4"),
    threshold: int           = Query(500),
    date_from: Optional[str] = Query(None),
    date_to:   Optional[str] = Query(None),
):
    ensure_file(path)
    w = date_where(symbol, date_from, date_to)
    rows = con.execute(f"""
        SELECT ts_recv, price, size, bid_px_00, ask_px_00
        FROM read_parquet('{path}') {w} ORDER BY ts_recv
    """).fetchall()
    return _agg(rows, "tick", threshold)

@app.get("/bars/range")
def range_bars(
    path:      str           = Query(...),
    symbol:    str           = Query("ESM4"),
    threshold: float         = Query(4.0),
    date_from: Optional[str] = Query(None),
    date_to:   Optional[str] = Query(None),
):
    ensure_file(path)
    w = date_where(symbol, date_from, date_to)
    rows = con.execute(f"""
        SELECT ts_recv, price, size, bid_px_00, ask_px_00
        FROM read_parquet('{path}') {w} ORDER BY ts_recv
    """).fetchall()
    return _agg(rows, "range", threshold)

def _agg(rows, mode, threshold):
    if not rows:
        return []
    bars = []
    ts0, p0, s0, b0, a0 = rows[0]
    o = h = l = c = p0
    vol = ticks = 0
    sum_bids = sum_asks = 0.0
    t = ts0

    for ts, price, size, bid, ask in rows:
        h = max(h, price); l = min(l, price); c = price
        vol += (size or 0); ticks += 1
        sum_bids += (bid or 0); sum_asks += (ask or 0)

        done = (
            (mode == "volume" and vol >= threshold) or
            (mode == "tick"   and ticks >= threshold) or
            (mode == "range"  and (h - l) >= threshold)
        )
        if done:
            ts_int = int(t.timestamp()) if hasattr(t, "timestamp") else int(t)
            bars.append({
                "time":   ts_int,
                "open":   o,
                "high":   h,
                "low":    l,
                "close":  c,
                "volume": vol,
                "bid":    round(sum_bids / ticks, 4),
                "ask":    round(sum_asks / ticks, 4),
                "vwap":   None,
            })
            o = h = l = c = price
            vol = ticks = 0
            sum_bids = sum_asks = 0.0
            t = ts

    # flush last partial bar
    if ticks > 0:
        ts_int = int(t.timestamp()) if hasattr(t, "timestamp") else int(t)
        bars.append({
            "time":   ts_int,
            "open":   o,
            "high":   h,
            "low":    l,
            "close":  c,
            "volume": vol,
            "bid":    round(sum_bids / ticks, 4),
            "ask":    round(sum_asks / ticks, 4),
            "vwap":   None,
        })
    return bars

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
    initial_capital: float = 100000
    commission:      float = 2.0
    slippage:        float = 0.25
    contract_size:   float = 50

@app.post("/backtest")
def run_backtest(req: BacktestRequest):
    try:
        ensure_file(req.path)
        w = date_where(req.symbol, req.date_from, req.date_to)

        if req.bar_type == "time":
            im = {"1min":"1 minute","5min":"5 minutes","15min":"15 minutes","30min":"30 minutes","1h":"1 hour"}
            di = im.get(req.interval, "5 minutes")
            raw = con.execute(f"""
                SELECT
                    epoch(time_bucket(INTERVAL '{di}', ts_recv::TIMESTAMPTZ)),
                    FIRST(price ORDER BY ts_recv), MAX(price), MIN(price),
                    LAST(price ORDER BY ts_recv), SUM(size),
                    AVG(bid_px_00), AVG(ask_px_00),
                    SUM(price * size) / NULLIF(SUM(size), 0)
                FROM read_parquet('{req.path}') {w}
                GROUP BY 1 ORDER BY 1
            """).fetchall()
            bars = clean_ohlcv(raw, has_extras=True)
        else:
            raw = con.execute(f"""
                SELECT ts_recv, price, size, bid_px_00, ask_px_00
                FROM read_parquet('{req.path}') {w} ORDER BY ts_recv
            """).fetchall()
            bars = _agg(raw, req.bar_type, req.threshold)

        if not bars:
            return {"success": False, "error": "No bars found. Coba perluas date range atau cek symbol."}

        trades, equity, err = _run_strategy(bars, req)
        if err:
            return {"success": False, "error": err}

        stats = _stats(trades, equity, req.initial_capital)
        mc    = _monte_carlo(trades, req.initial_capital)
        return {
            "success": True,
            "trades":  trades,
            "equity":  equity,
            "stats":   stats,
            "monte_carlo": mc,
        }

    except Exception:
        return {"success": False, "error": traceback.format_exc()}

# FIX: use builtins module directly instead of __builtins__ dict hack
SAFE_BUILTINS = {
    "abs": abs, "round": round, "min": min, "max": max, "sum": sum,
    "len": len, "range": range, "enumerate": enumerate, "zip": zip,
    "list": list, "dict": dict, "tuple": tuple, "set": set,
    "int": int, "float": float, "str": str, "bool": bool,
    "print": print, "isinstance": isinstance, "hasattr": hasattr,
    "getattr": getattr, "setattr": setattr, "any": any, "all": all,
    "sorted": sorted, "reversed": reversed, "map": map, "filter": filter,
    "True": True, "False": False, "None": None,
    "ValueError": ValueError, "TypeError": TypeError,
}

def _run_strategy(bars, req):
    trades  = []
    equity  = []
    cash    = req.initial_capital
    position  = 0
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

    try:
        exec(req.strategy, g)
    except Exception:
        return [], [], f"Compile error:\n{traceback.format_exc()}"

    init_fn   = g.get("initialize")
    on_bar_fn = g.get("on_bar")
    if not on_bar_fn:
        return [], [], "`on_bar(bar, ctx, history)` function not found in strategy."

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

        action, size = (sig if isinstance(sig, (tuple, list)) else (sig, 1)) if sig else (None, 1)
        size  = max(1, abs(int(size or 1)))
        close = bar["close"]
        slip  = req.slippage
        comm  = req.commission
        cs    = req.contract_size

        if action in ("BUY", "LONG") and position <= 0:
            if position < 0:
                fill = close + slip
                pnl  = (entry_price - fill) * abs(position) * cs - comm * 2 * abs(position)
                cash += pnl
                trades.append({"entry_time": entry_time, "exit_time": bar["time"],
                                "side": "SHORT", "entry": entry_price, "exit": fill,
                                "size": abs(position), "pnl": round(pnl, 2)})
                position = 0
            fill = close + slip
            position = size; entry_price = fill; entry_time = bar["time"]
            cash -= comm * size
            ctx.position = position; ctx.entry_price = entry_price

        elif action in ("SELL", "SHORT") and position >= 0:
            if position > 0:
                fill = close - slip
                pnl  = (fill - entry_price) * position * cs - comm * 2 * position
                cash += pnl
                trades.append({"entry_time": entry_time, "exit_time": bar["time"],
                                "side": "LONG", "entry": entry_price, "exit": fill,
                                "size": position, "pnl": round(pnl, 2)})
                position = 0
            fill = close - slip
            position = -size; entry_price = fill; entry_time = bar["time"]
            cash -= comm * size
            ctx.position = position; ctx.entry_price = entry_price

        elif action == "CLOSE" and position != 0:
            if position > 0:
                fill = close - slip; pnl = (fill - entry_price) * position * cs; side = "LONG"
            else:
                fill = close + slip; pnl = (entry_price - fill) * abs(position) * cs; side = "SHORT"
            pnl -= comm * 2 * abs(position)
            cash += pnl
            trades.append({"entry_time": entry_time, "exit_time": bar["time"],
                           "side": side, "entry": entry_price, "exit": fill,
                           "size": abs(position), "pnl": round(pnl, 2)})
            position = 0; entry_price = 0
            ctx.position = 0; ctx.entry_price = 0

        open_pnl = 0
        if   position > 0: open_pnl = (close - entry_price) * position * cs
        elif position < 0: open_pnl = (entry_price - close) * abs(position) * cs
        ctx.cash = cash
        equity.append({"time": bar["time"], "equity": round(cash + open_pnl, 2)})

    return trades, equity, None

def _stats(trades, equity, initial):
    if not trades:
        return {"total_trades": 0, "note": "No trades executed."}
    pnls   = np.array([t["pnl"] for t in trades])
    wins   = pnls[pnls > 0]
    losses = pnls[pnls <= 0]
    eqs    = np.array([e["equity"] for e in equity])
    peak   = np.maximum.accumulate(np.maximum(eqs, initial))
    dd     = (peak - eqs) / peak * 100
    final  = float(eqs[-1])
    wr     = len(wins) / len(pnls)
    gp     = float(wins.sum())  if len(wins)   else 0.0
    gl     = float(abs(losses.sum())) if len(losses) else 0.0
    aw     = float(wins.mean()) if len(wins)   else 0.0
    al     = float(losses.mean()) if len(losses) else 0.0

    # Calmar = annualized return / max drawdown
    total_return = (final - initial) / initial
    max_dd_pct   = float(dd.max())
    calmar = round(total_return * 100 / max_dd_pct, 3) if max_dd_pct > 0 else 999

    # Sortino (downside std)
    neg_pnls = pnls[pnls < 0]
    downside_std = float(neg_pnls.std()) if len(neg_pnls) > 1 else 1e-9
    sortino = round(float(pnls.mean()) / downside_std * np.sqrt(252), 3) if downside_std > 0 else 0

    return {
        "total_trades":      len(trades),
        "winning_trades":    int(len(wins)),
        "losing_trades":     int(len(losses)),
        "win_rate":          round(wr * 100, 2),
        "total_pnl":         round(float(pnls.sum()), 2),
        "gross_profit":      round(gp, 2),
        "gross_loss":        round(gl, 2),
        "profit_factor":     round(gp / gl, 3) if gl else 999,
        "avg_win":           round(aw, 2),
        "avg_loss":          round(al, 2),
        "rr_ratio":          round(abs(aw / al), 3) if al else 0,
        "expectancy":        round(wr * aw + (1 - wr) * al, 2),
        "max_drawdown_pct":  round(max_dd_pct, 2),
        "sharpe_ratio":      round(float(pnls.mean() / pnls.std() * np.sqrt(252)), 3) if pnls.std() > 0 else 0,
        "sortino_ratio":     sortino,
        "calmar_ratio":      calmar,
        "total_return_pct":  round(total_return * 100, 2),
        "final_equity":      round(final, 2),
        "initial_capital":   initial,
        "best_trade":        round(float(pnls.max()), 2),
        "worst_trade":       round(float(pnls.min()), 2),
        "avg_trade":         round(float(pnls.mean()), 2),
        "max_consec_wins":   _consec(pnls, True),
        "max_consec_losses": _consec(pnls, False),
    }

def _consec(pnls, wins):
    mx = cur = 0
    for p in pnls:
        if (wins and p > 0) or (not wins and p <= 0):
            cur += 1; mx = max(mx, cur)
        else:
            cur = 0
    return mx

def _monte_carlo(trades, initial, n_sim=500):
    if len(trades) < 2:
        return {}
    pnls      = np.array([t["pnl"] for t in trades])
    n         = len(pnls)
    final_eq  = []
    max_dd    = []
    paths     = []

    for s in range(n_sim):
        r   = np.random.choice(pnls, size=n, replace=True)
        eq  = np.insert(initial + np.cumsum(r), 0, initial)
        pk  = np.maximum.accumulate(eq)
        final_eq.append(float(eq[-1]))
        max_dd.append(float(((pk - eq) / pk * 100).max()))
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