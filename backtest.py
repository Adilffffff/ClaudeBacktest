here"""
Strategy G — EMA Crossover + EMA50 Slope + ADX(14)
8 VARIANTS: 4 rules × 2 timeframes (15m and 30m)

ADX IMPLEMENTATION: matches GMaxV1.py live bot exactly.
  - Requires len(closes) >= period*3 to return non-zero values
  - Uses SMA seed for first `period` bars, then Wilder smoothing
  - Same ws() helper, same guard, same pdi/mdi/dx calc

TIMEFRAMES:
  15m candles: max_hold = 960 bars = 10 calendar days
  30m candles: max_hold = 480 bars = 10 calendar days (same real time)

VARIANTS (per timeframe):
  VAR_B   — TP 2.0% / SL 18.0% / no max hold  (15m only — unlimited hold)
  VAR_C   — TP 2.0% / SL 18.0% / 10-day auto-close
  VAR_D   — TP 2.0% / SL 15.0% / 10-day auto-close
  VAR_NEW — TP 1.2% / SL 10.0% / 10-day auto-close  (replaces old VAR_E 1.5%/11%)

8 total: VAR_B_15m, VAR_C_15m, VAR_D_15m, VAR_NEW_15m,
         VAR_B_30m, VAR_C_30m, VAR_D_30m, VAR_NEW_30m

PARALLELISM:
  Phase 1 (download): 50 workers via ProcessPoolExecutor
  Phase 3 (simulate): 8 workers via ProcessPoolExecutor — one per variant

P&L FORMULA (correct):
  pnl = FIXED_MARGIN * net_pct
  net_pct = raw_ret * LEVERAGE - ROUND_TRIP_COST
  (NOT notional * net_pct — that double-applies leverage)

Coin universe : WHITELIST_G (56 coins)
Period        : Jul 2024 - Jun 2026 (24 months)
Capital       : $100 starting, $1 fixed margin per trade
Max concurrent: 6 positions portfolio-wide
Fees          : 0.05%/side  Slippage: 0.02%/side
Entry         : close of signal bar (closed candle, no lookahead)
Exit          : TP or SL hit intra-bar (high/low test)
              : auto-close at bar limit if no TP/SL (VAR_C/D/NEW)
              : End of data — force-close any remaining open positions
"""

import urllib.request, zipfile, csv, io, json, math
from datetime import datetime, timezone
from concurrent.futures import ProcessPoolExecutor, as_completed

# -- WHITELIST (56 coins) -------------------------------------------------------
COINS = [
    "1000000BOBUSDT","1000CATUSDT","1000RATSUSDT","A2ZUSDT","AIOTUSDT",
    "ALGOUSDT","ALPINEUSDT","ASTERUSDT","AUSDT","BASEDUSDT","BELUSDT",
    "BIDUSDT","BMTUSDT","BTRUSDT","CFXUSDT","CHIPUSDT","CRCLUSDT",
    "DAMUSDT","DEXEUSDT","DIAUSDT","EPTUSDT","ETHUSDT","FLNCUSDT",
    "FUNUSDT","GLMUSDT","GUAUSDT","ICXUSDT","IOUSDT","LIGHTUSDT",
    "MOODENGUSDT","NFPUSDT","NMRUSDT","NOTUSDT","ORBSUSDT","PEOPLEUSDT",
    "PIPPINUSDT","POWERUSDT","POWRUSDT","RAVEUSDT","RESOLVUSDT","RVVUSDT",
    "SEIUSDT","SIGNUSDT","SKRUSDT","SNDKUSDT","SOMIUSDT","SPELLUSDT",
    "TRUTHUSDT","TURBOUSDT","VANRYUSDT","VINEUSDT","VVVUSDT","XEMUSDT",
    "XRPUSDT","ZECUSDT","ZEREBROUSDT",
]

START_YEAR,  START_MONTH = 2024, 7
END_YEAR,    END_MONTH   = 2026, 6

# -- STRATEGY PARAMS (shared) ---------------------------------------------------
LEVERAGE        = 5
START_CAPITAL   = 100.0
FIXED_MARGIN    = 1.0
FEE_RATE        = 0.0005
SLIP_RATE       = 0.0002
ROUND_TRIP_COST = (FEE_RATE + SLIP_RATE) * 2
MAX_CONCURRENT  = 6
WARMUP_BARS     = 70   # >= 70 matches live bot's `if len(closes) < 70` guard

# -- INDICATOR PARAMS -----------------------------------------------------------
EMA_FAST   = 9
EMA_SLOW   = 21
EMA_TREND  = 50
SLOPE_BARS = 10
SLOPE_MIN  = 0.0005   # 0.05% matches live bot's slope_pct > 0.05
ADX_PERIOD = 14
ADX_MIN    = 22.0

# -- VARIANT DEFINITIONS --------------------------------------------------------
BASE_VARIANTS = [
    {"name": "VAR_B",   "tp_pct": 0.020, "sl_pct": 0.18, "has_hold_limit": False},
    {"name": "VAR_C",   "tp_pct": 0.020, "sl_pct": 0.18, "has_hold_limit": True},
    {"name": "VAR_D",   "tp_pct": 0.020, "sl_pct": 0.15, "has_hold_limit": True},
    {"name": "VAR_NEW", "tp_pct": 0.012, "sl_pct": 0.10, "has_hold_limit": True},
]

TIMEFRAMES = [
    {"interval": "15m", "hold_bars": 960, "label": "15m"},
    {"interval": "30m", "hold_bars": 480, "label": "30m"},
]

def build_variants():
    variants = []
    for tf in TIMEFRAMES:
        for bv in BASE_VARIANTS:
            max_hold = tf["hold_bars"] if bv["has_hold_limit"] else None
            variants.append({
                "name":          f"{bv['name']}_{tf['label']}",
                "tp_pct":        bv["tp_pct"],
                "sl_pct":        bv["sl_pct"],
                "max_hold_bars": max_hold,
                "interval":      tf["interval"],
                "hold_days":     10 if bv["has_hold_limit"] else None,
            })
    return variants

VARIANTS = build_variants()

BASE_URL = "https://data.binance.vision/data/futures/um/monthly/klines"

# -- DATA -----------------------------------------------------------------------
def month_range(sy, sm, ey, em):
    y, m = sy, sm
    while (y, m) <= (ey, em):
        yield y, m
        m += 1
        if m > 12:
            m = 1; y += 1

def fetch_symbol(args):
    symbol, interval = args
    rows = []
    for y, m in month_range(START_YEAR, START_MONTH, END_YEAR, END_MONTH):
        url = f"{BASE_URL}/{symbol}/{interval}/{symbol}-{interval}-{y}-{m:02d}.zip"
        try:
            with urllib.request.urlopen(url, timeout=30) as resp:
                data = resp.read()
            with zipfile.ZipFile(io.BytesIO(data)) as z:
                with z.open(z.namelist()[0]) as f:
                    for row in csv.reader(io.TextIOWrapper(f)):
                        if not row or not row[0].isdigit():
                            continue
                        ts = int(row[0])
                        if ts > 10**14:
                            ts //= 1000
                        rows.append({
                            "ts":    ts,
                            "open":  float(row[1]),
                            "high":  float(row[2]),
                            "low":   float(row[3]),
                            "close": float(row[4]),
                        })
        except Exception:
            pass
    rows.sort(key=lambda r: r["ts"])
    seen, out = set(), []
    for r in rows:
        if r["ts"] not in seen:
            seen.add(r["ts"]); out.append(r)
    return out

# -- INDICATORS -----------------------------------------------------------------
def calc_ema(values, period):
    k = 2.0 / (period + 1)
    ema = [values[0]]
    for v in values[1:]:
        ema.append(v * k + ema[-1] * (1 - k))
    return ema

def adx_calc(highs, lows, closes, period=14):
    """
    EXACT match of GMaxV1.py live bot adx_calc().
    Returns (adx, pdi, mdi) — same as live bot.
    Returns (0.0, 0.0, 0.0) if not enough data.
    """
    if len(closes) < period * 3:
        return 0.0, 0.0, 0.0
    pdm, mdm, trs = [], [], []
    for i in range(1, len(closes)):
        up   = highs[i] - highs[i-1]
        down = lows[i-1] - lows[i]
        pdm.append(up   if up > down   and up   > 0 else 0.0)
        mdm.append(down if down > up   and down > 0 else 0.0)
        trs.append(max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i-1]),
            abs(lows[i]  - closes[i-1]),
        ))
    def ws(v, p):
        if len(v) < p:
            return []
        r = [sum(v[:p])]
        for x in v[p:]:
            r.append(r[-1] - r[-1]/p + x)
        return r
    st = ws(trs, period)
    sp = ws(pdm, period)
    sm = ws(mdm, period)
    if not st:
        return 0.0, 0.0, 0.0
    pdi = [100*p/t if t else 0 for p, t in zip(sp, st)]
    mdi = [100*m/t if t else 0 for m, t in zip(sm, st)]
    dx  = [100*abs(p-m)/(p+m) if (p+m) else 0 for p, m in zip(pdi, mdi)]
    if len(dx) < period:
        return 0.0, pdi[-1], mdi[-1]
    adx = sum(dx[:period]) / period
    for d in dx[period:]:
        adx = (adx * (period-1) + d) / period
    return max(0.0, min(100.0, adx)), pdi[-1], mdi[-1]

def calc_adx_series(bars, period=14):
    """
    Compute per-bar ADX for the full bar array.
    At each bar i, calls adx_calc on bars[0..i] to get the ADX value
    that the live bot would see at that point in time.
    Returns list of floats, one per bar.
    """
    n = len(bars)
    adx_vals = [0.0] * n
    # Pre-extract arrays for speed
    highs  = [b["high"]  for b in bars]
    lows   = [b["low"]   for b in bars]
    closes = [b["close"] for b in bars]
    for i in range(period * 3, n):
        adx_v, _, _ = adx_calc(highs[:i+1], lows[:i+1], closes[:i+1], period)
        adx_vals[i] = adx_v
    return adx_vals

# -- PORTFOLIO SIMULATION (designed to run in a subprocess) --------------------
def _simulate_variant(args):
    """
    Worker function for ProcessPoolExecutor.
    Receives all data it needs as plain dicts/lists (picklable).
    Returns (variant_name, trades, final_equity, rej)
    """
    v, sym_inds_plain = args
    vname         = v["name"]
    tp_pct        = v["tp_pct"]
    sl_pct        = v["sl_pct"]
    max_hold_bars = v["max_hold_bars"]

    NOTIONAL = FIXED_MARGIN * LEVERAGE

    # Build event list
    events = []
    for sym, inds in sym_inds_plain.items():
        for i in range(len(inds["bars"])):
            events.append((inds["bars"][i]["ts"], sym, i))
    events.sort()

    equity   = START_CAPITAL
    open_pos = {}
    closed   = []

    rej = {
        "warmup":         0,
        "sym_open":       0,
        "max_concurrent": 0,
        "slope":          0,
        "no_cross":       0,
        "adx":            0,
        "insuff_cap":     0,
        "executed":       0,
    }

    progress_step = max(1, len(events) // 10)

    for step, (ts, sym, i) in enumerate(events):
        if step % progress_step == 0:
            pct = step / len(events) * 100
            print(f"  [{vname}] {pct:.0f}%  eq=${equity:,.2f}  open={len(open_pos)}  trades={len(closed)}", flush=True)

        inds = sym_inds_plain[sym]
        bars = inds["bars"]
        bar  = bars[i]

        # -- EXIT CHECK ---------------------------------------------------------
        if sym in open_pos:
            pos    = open_pos[sym]
            side   = pos["side"]
            tp     = pos["tp"]
            sl     = pos["sl"]
            held   = i - pos["entry_bar"]
            hit_tp = hit_sl = timed_out = False

            if max_hold_bars is not None and held >= max_hold_bars:
                timed_out = True
            elif side == "LONG":
                if bar["high"] >= tp:   hit_tp = True
                elif bar["low"] <= sl:  hit_sl = True
            else:
                if bar["low"] <= tp:    hit_tp = True
                elif bar["high"] >= sl: hit_sl = True

            if hit_tp or hit_sl or timed_out:
                exit_px = bar["close"] if timed_out else (tp if hit_tp else sl)
                ep      = pos["entry"]
                raw_ret = (exit_px - ep)/ep if side == "LONG" else (ep - exit_px)/ep
                net_pct = raw_ret * LEVERAGE - ROUND_TRIP_COST
                pnl     = FIXED_MARGIN * net_pct
                pnl     = max(pnl, -FIXED_MARGIN)

                equity += pnl
                closed.append({
                    "symbol":    sym,
                    "side":      side,
                    "entry_ts":  pos["entry_ts"],
                    "exit_ts":   ts,
                    "entry_px":  ep,
                    "exit_px":   exit_px,
                    "notional":  NOTIONAL,
                    "margin":    FIXED_MARGIN,
                    "pnl":       pnl,
                    "win":       pnl > 0,
                    "duration":  held,
                    "tp_hit":    hit_tp,
                    "timed_out": timed_out,
                    "forced":    False,
                })
                del open_pos[sym]

            if sym in open_pos:
                continue

        # -- WARMUP GUARD -------------------------------------------------------
        if i < WARMUP_BARS:
            rej["warmup"] += 1
            continue

        if sym in open_pos:
            rej["sym_open"] += 1
            continue

        if len(open_pos) >= MAX_CONCURRENT:
            rej["max_concurrent"] += 1
            continue

        # -- ENTRY FILTERS (matches live bot check_signal_G order) --------------
        ema50   = inds["ema50"]
        ema9    = inds["ema9"]
        ema21   = inds["ema21"]
        adx_arr = inds["adx"]

        # Filter 1: EMA50 slope (same formula as live bot)
        slope_pct = (ema50[i] - ema50[i-SLOPE_BARS]) / ema50[i-SLOPE_BARS] * 100
        trend_up   = slope_pct >  0.05
        trend_down = slope_pct < -0.05
        if not trend_up and not trend_down:
            rej["slope"] += 1
            continue

        # Filter 2: EMA9/21 crossover on closed bar (i-1 = previous bar)
        cross_long  = ema9[i] > ema21[i] and ema9[i-1] <= ema21[i-1]
        cross_short = ema9[i] < ema21[i] and ema9[i-1] >= ema21[i-1]

        if cross_long and trend_up:
            direction = "LONG"
        elif cross_short and trend_down:
            direction = "SHORT"
        else:
            rej["no_cross"] += 1
            continue

        # Filter 3: ADX >= 22 (same threshold as live bot)
        if adx_arr[i] < ADX_MIN:
            rej["adx"] += 1
            continue

        if equity < FIXED_MARGIN:
            rej["insuff_cap"] += 1
            continue

        # -- OPEN POSITION ------------------------------------------------------
        entry_px = bar["close"]
        if direction == "LONG":
            tp_px = entry_px * (1 + tp_pct)
            sl_px = entry_px * (1 - sl_pct)
        else:
            tp_px = entry_px * (1 - tp_pct)
            sl_px = entry_px * (1 + sl_pct)

        open_pos[sym] = {
            "entry":      entry_px,
            "entry_ts":   ts,
            "entry_bar":  i,
            "side":       direction,
            "tp":         tp_px,
            "sl":         sl_px,
        }
        rej["executed"] += 1

    # -- FORCE-CLOSE remaining open positions at end of data --------------------
    force_closed = 0
    for sym, pos in open_pos.items():
        bars    = sym_inds_plain[sym]["bars"]
        last    = bars[-1]
        exit_px = last["close"]
        ep      = pos["entry"]
        side    = pos["side"]
        held    = len(bars) - 1 - pos["entry_bar"]
        raw_ret = (exit_px - ep)/ep if side == "LONG" else (ep - exit_px)/ep
        net_pct = raw_ret * LEVERAGE - ROUND_TRIP_COST
        pnl     = FIXED_MARGIN * net_pct
        pnl     = max(pnl, -FIXED_MARGIN)

        equity += pnl
        closed.append({
            "symbol":    sym,
            "side":      side,
            "entry_ts":  pos["entry_ts"],
            "exit_ts":   last["ts"],
            "entry_px":  ep,
            "exit_px":   exit_px,
            "notional":  NOTIONAL,
            "margin":    FIXED_MARGIN,
            "pnl":       pnl,
            "win":       pnl > 0,
            "duration":  held,
            "tp_hit":    False,
            "timed_out": False,
            "forced":    True,
        })
        force_closed += 1

    timed_out_count = sum(1 for t in closed if t.get("timed_out"))
    print(f"  [{vname}] Done. Equity=${equity:,.2f}  Trades={len(closed)}  ForceClose={force_closed}  TimedOut={timed_out_count}", flush=True)
    return vname, closed, equity, rej

# -- STATISTICS -----------------------------------------------------------------
def compute_stats(trades, start_cap, final_eq):
    if not trades:
        return None
    wins   = [t for t in trades if t["win"]]
    losses = [t for t in trades if not t["win"]]
    forced = [t for t in trades if t.get("forced")]
    timed  = [t for t in trades if t.get("timed_out")]
    n, nw, nl = len(trades), len(wins), len(losses)

    gross_win  = sum(t["pnl"] for t in wins)
    gross_loss = abs(sum(t["pnl"] for t in losses))
    pf  = gross_win / gross_loss if gross_loss else float("inf")
    wr  = nw / n
    exp = sum(t["pnl"] for t in trades) / n

    durations = [t["duration"] for t in trades]
    avg_dur   = sum(durations) / n
    max_dur   = max(durations)
    min_dur   = min(durations)

    trades_sorted = sorted(trades, key=lambda t: t["exit_ts"])
    eq = start_cap
    equity_curve = [eq]
    for t in trades_sorted:
        eq += t["pnl"]
        equity_curve.append(eq)

    peak = start_cap; max_dd = 0.0
    for v in equity_curve:
        peak = max(peak, v)
        dd = (peak - v) / peak
        max_dd = max(max_dd, dd)

    monthly = {}
    for t in trades_sorted:
        dt  = datetime.fromtimestamp(t["exit_ts"]/1000, tz=timezone.utc)
        key = f"{dt.year}-{dt.month:02d}"
        monthly[key] = monthly.get(key, 0) + t["pnl"]

    daily = {}
    for t in trades_sorted:
        dt  = datetime.fromtimestamp(t["exit_ts"]/1000, tz=timezone.utc)
        key = (dt.year, dt.month, dt.day)
        daily[key] = daily.get(key, 0) + t["pnl"]
    days = list(daily.values())
    sharpe = sortino = 0.0
    if len(days) > 1:
        mu  = sum(days)/len(days)
        std = math.sqrt(sum((d-mu)**2 for d in days)/len(days))
        if std:
            sharpe = mu/std * math.sqrt(365)
        neg_sq = sum(d**2 for d in days if d < 0)/len(days)
        if neg_sq:
            sortino = mu/math.sqrt(neg_sq) * math.sqrt(365)

    bws = bloss = cw = cl = 0
    for t in trades_sorted:
        if t["win"]:
            cw += 1; cl = 0
        else:
            cl += 1; cw = 0
        bws   = max(bws,   cw)
        bloss = max(bloss, cl)

    longs  = [t for t in trades if t["side"] == "LONG"]
    shorts = [t for t in trades if t["side"] == "SHORT"]
    lwr = sum(1 for t in longs  if t["win"])/len(longs)  if longs  else 0
    swr = sum(1 for t in shorts if t["win"])/len(shorts) if shorts else 0

    return {
        "total_trades":      n,
        "wins":              nw,
        "losses":            nl,
        "forced_closed":     len(forced),
        "timed_out":         len(timed),
        "win_rate":          wr,
        "profit_factor":     pf,
        "net_pnl":           final_eq - start_cap,
        "final_equity":      final_eq,
        "max_drawdown":      max_dd,
        "sharpe":            sharpe,
        "sortino":           sortino,
        "avg_win":           gross_win/nw   if nw else 0,
        "avg_loss":          -gross_loss/nl if nl else 0,
        "expectancy":        exp,
        "avg_duration_bars": avg_dur,
        "max_duration_bars": max_dur,
        "min_duration_bars": min_dur,
        "long_trades":       len(longs),
        "long_wr":           lwr,
        "short_trades":      len(shorts),
        "short_wr":          swr,
        "best_win_streak":   bws,
        "best_loss_streak":  bloss,
        "monthly_pnl":       dict(sorted(monthly.items())),
    }

def per_coin_stats(trades):
    by_sym = {}
    for t in trades:
        by_sym.setdefault(t["symbol"], []).append(t)
    rows = []
    for sym, st in by_sym.items():
        w  = sum(1 for t in st if t["win"])
        l  = len(st) - w
        gw = sum(t["pnl"] for t in st if t["win"])
        gl = abs(sum(t["pnl"] for t in st if not t["win"]))
        pf = gw/gl if gl else float("inf")
        rows.append({
            "symbol": sym, "trades": len(st),
            "wins": w, "losses": l,
            "win_rate": w/len(st),
            "profit_factor": pf,
            "net_pnl": sum(t["pnl"] for t in st),
        })
    rows.sort(key=lambda r: -r["profit_factor"])
    return rows

# -- OUTPUT ---------------------------------------------------------------------
def write_summary(v, stats, coin_table, rej, loaded):
    vname         = v["name"]
    tp_pct        = v["tp_pct"]
    sl_pct        = v["sl_pct"]
    max_hold_bars = v["max_hold_bars"]
    interval      = v["interval"]
    mins          = int(interval.replace("m", ""))

    lines = []
    a = lines.append

    hold_label = (f"{max_hold_bars} bars ({v['hold_days']}d calendar)"
                  if max_hold_bars else "unlimited")

    a("=" * 70)
    a(f"STRATEGY G — {vname}  |  TP {tp_pct*100:.1f}%  SL {sl_pct*100:.1f}%  5x Isolated  [{interval}]")
    a(f"Max hold : {hold_label}")
    a(f"Sizing   : ${FIXED_MARGIN:.2f} margin x {LEVERAGE}x = ${FIXED_MARGIN*LEVERAGE:.2f} notional  (FIXED, P&L: margin x leveraged-return%)")
    a(f"Period   : {START_YEAR}-{START_MONTH:02d} to {END_YEAR}-{END_MONTH:02d}  |  Capital: ${START_CAPITAL:.2f}")
    a(f"Universe : {len(COINS)} whitelisted coins | {loaded} loaded with data")
    a(f"ADX      : matches GMaxV1.py live bot (period*3 guard, SMA seed, Wilder smooth)")
    a("=" * 70)

    if stats is None:
        a("NO TRADES EXECUTED"); return "\n".join(lines)

    verdict = ("USABLE (candidate)"
               if stats["profit_factor"] >= 1.5 and stats["win_rate"] >= 0.42
               else "DOES NOT MEET TARGETS")

    a(f"Total Trades     : {stats['total_trades']:,}")
    a(f"Wins / Losses    : {stats['wins']:,} / {stats['losses']:,}")
    a(f"Force-closed EoD : {stats['forced_closed']:,}  (end of data)")
    if max_hold_bars:
        a(f"Timed-out ({v['hold_days']}d)  : {stats['timed_out']:,}  (hit {max_hold_bars}-bar limit)")
    a(f"Win Rate         : {stats['win_rate']*100:.2f}%")
    a(f"Profit Factor    : {stats['profit_factor']:.4f}")
    a(f"Net PnL          : ${stats['net_pnl']:,.2f}  (on ${START_CAPITAL:.2f} starting capital)")
    a(f"Final Equity     : ${stats['final_equity']:,.2f}")
    a(f"Max Drawdown     : {stats['max_drawdown']*100:.2f}%")
    a(f"Sharpe           : {stats['sharpe']:.3f}")
    a(f"Sortino          : {stats['sortino']:.3f}")
    a(f"Avg Win          : ${stats['avg_win']:.4f}")
    a(f"Avg Loss         : ${stats['avg_loss']:.4f}")
    a(f"Expectancy       : ${stats['expectancy']:.4f} per trade")
    a(f"Duration — Avg   : {stats['avg_duration_bars']:.1f} bars  ({stats['avg_duration_bars']*mins/60:.1f} hours)")
    a(f"Duration — Max   : {stats['max_duration_bars']} bars  ({stats['max_duration_bars']*mins/60:.1f} hours  /  {stats['max_duration_bars']*mins/60/24:.1f} days)")
    a(f"Duration — Min   : {stats['min_duration_bars']} bars  ({stats['min_duration_bars']*mins/60:.2f} hours)")
    a(f"Long  Trades     : {stats['long_trades']:,}  WR {stats['long_wr']*100:.2f}%")
    a(f"Short Trades     : {stats['short_trades']:,}  WR {stats['short_wr']*100:.2f}%")
    a(f"Best Win Streak  : {stats['best_win_streak']}")
    a(f"Best Loss Streak : {stats['best_loss_streak']}")
    a(f"VERDICT          : {verdict}")
    a("")
    a("── Scale reference ─────────────────────────────────────────────────")
    a(f"  $1/trade  → Net PnL ${stats['net_pnl']:,.2f} over 24 months")
    a(f"  $10/trade → ~${stats['net_pnl']*10:,.0f}")
    a(f"  $50/trade → ~${stats['net_pnl']*50:,.0f}")
    a("")
    a("── Filter Rejection Stats ───────────────────────────────────────────")
    total = sum(rej.values())
    a(f"  Warmup                   : {rej['warmup']:>9,}")
    a(f"  Symbol already open      : {rej['sym_open']:>9,}")
    a(f"  Max concurrent ({MAX_CONCURRENT}) hit   : {rej['max_concurrent']:>9,}")
    a(f"  Slope filter             : {rej['slope']:>9,}")
    a(f"  No valid crossover       : {rej['no_cross']:>9,}")
    a(f"  ADX < {ADX_MIN:.0f}               : {rej['adx']:>9,}")
    a(f"  Insufficient capital     : {rej['insuff_cap']:>9,}")
    a(f"  EXECUTED                 : {rej['executed']:>9,}")
    a(f"  Total bar-events         : {total:>9,}")
    a("")
    a("── Monthly PnL ─────────────────────────────────────────────────────")
    for ym, pnl in stats["monthly_pnl"].items():
        sign = "+" if pnl >= 0 else ""
        a(f"  {ym}: ${sign}{pnl:,.4f}")
    a("")
    a("── Per-Coin Table (sorted by Profit Factor) ─────────────────────────")
    a(f"  {'Symbol':<24} {'PF':>6}  {'WR':>7}  {'Trades':>7}  {'Net PnL':>12}")
    for r in coin_table:
        pf_s = f"{r['profit_factor']:.3f}" if r["profit_factor"] != float("inf") else "  INF"
        a(f"  {r['symbol']:<24} {pf_s:>6}  {r['win_rate']*100:>6.1f}%  {r['trades']:>7,}  ${r['net_pnl']:>11,.4f}")

    return "\n".join(lines)

# -- MAIN -----------------------------------------------------------------------
def main():
    print(f"Strategy G — 8 variants (4 rules × 2 TFs: 15m + 30m) | 50 download workers | 8 sim workers")
    print(f"ADX: exact match to GMaxV1.py live bot (period*3={ADX_PERIOD*3} bar guard)")
    print(f"Variants: {[v['name'] for v in VARIANTS]}")
    print(f"Period: {START_YEAR}-{START_MONTH:02d} → {END_YEAR}-{END_MONTH:02d}  |  {len(COINS)} coins")
    print()

    # -- Phase 1: Download (50 parallel workers) --------------------------------
    download_tasks = [(sym, tf["interval"]) for sym in COINS for tf in TIMEFRAMES]
    print(f"Phase 1: Downloading {len(download_tasks)} symbol×timeframe combos (50 workers)...")
    all_bars = {"15m": {}, "30m": {}}
    with ProcessPoolExecutor(max_workers=50) as ex:
        futs = {ex.submit(fetch_symbol, task): task for task in download_tasks}
        for fut in as_completed(futs):
            sym, interval = futs[fut]
            try:
                bars = fut.result()
                if bars:
                    all_bars[interval][sym] = bars
                    print(f"  OK {sym} [{interval}]: {len(bars):,} bars")
                else:
                    print(f"  MISS {sym} [{interval}]: no data")
            except Exception as e:
                print(f"  ERR {sym} [{interval}]: {e}")

    for interval in ["15m", "30m"]:
        loaded = len(all_bars[interval])
        print(f"Loaded [{interval}]: {loaded}/{len(COINS)} symbols")
        if loaded == 0:
            print(f"FATAL: 0 symbols loaded for {interval}.")
        elif loaded < len(COINS) * 0.5:
            print(f"WARNING [{interval}]: less than half the whitelist loaded.")

    # -- Phase 2: Compute indicators --------------------------------------------
    print("\nPhase 2: Computing indicators (both timeframes)...")
    sym_inds = {"15m": {}, "30m": {}}
    for interval in ["15m", "30m"]:
        for sym, bars in all_bars[interval].items():
            if len(bars) < WARMUP_BARS + 5:
                print(f"  SKIP {sym} [{interval}]: only {len(bars)} bars")
                continue
            closes = [b["close"] for b in bars]
            highs  = [b["high"]  for b in bars]
            lows   = [b["low"]   for b in bars]
            sym_inds[interval][sym] = {
                "ema9":  calc_ema(closes, EMA_FAST),
                "ema21": calc_ema(closes, EMA_SLOW),
                "ema50": calc_ema(closes, EMA_TREND),
                "adx":   calc_adx_series(bars, ADX_PERIOD),
                "bars":  bars,
            }
        print(f"  [{interval}] {len(sym_inds[interval])} symbols ready.")

    # -- Phase 3: Simulate all 8 variants in parallel (8 workers) --------------
    print("\nPhase 3: Simulating 8 variants in parallel (8 workers)...")
    sim_tasks = []
    for v in VARIANTS:
        interval = v["interval"]
        loaded   = len(sym_inds[interval])
        if loaded == 0:
            print(f"  SKIP {v['name']}: no data for {interval}")
            continue
        # Pass a plain dict copy — must be picklable for subprocess
        sim_tasks.append((v, sym_inds[interval]))

    all_results = {}
    with ProcessPoolExecutor(max_workers=8) as ex:
        futs = {ex.submit(_simulate_variant, task): task[0]["name"] for task in sim_tasks}
        for fut in as_completed(futs):
            vname = futs[fut]
            try:
                vname_out, trades, final_eq, rej = fut.result()
                all_results[vname_out] = {
                    "trades": trades, "final_eq": final_eq, "rej": rej,
                }
                print(f"  [{vname_out}] simulation complete — {len(trades)} trades")
            except Exception as e:
                print(f"  ERR {vname}: {e}")

    # -- Compute stats & summaries ----------------------------------------------
    summaries = []
    for v in VARIANTS:
        vname    = v["name"]
        interval = v["interval"]
        loaded   = len(sym_inds[interval])
        r        = all_results.get(vname)
        if r is None:
            continue
        stats      = compute_stats(r["trades"], START_CAPITAL, r["final_eq"])
        coin_table = per_coin_stats(r["trades"])
        summary    = write_summary(v, stats, coin_table, r["rej"], loaded)
        all_results[vname]["stats"]      = stats
        all_results[vname]["coin_table"] = coin_table
        all_results[vname]["summary"]    = summary
        summaries.append(summary)
        print("\n" + summary)

    # -- Write outputs ----------------------------------------------------------
    combined = ("\n\n" + ("=" * 70) + "\n\n").join(summaries)
    with open("backtest_summary.txt", "w") as f:
        f.write(combined + "\n")

    report = {
        "meta": {
            "strategy":       "Strategy G (GMaxV1 ADX, 8 variants, 15m+30m, parallel sim)",
            "variants":       [v["name"] for v in VARIANTS],
            "sizing":         "fixed",
            "fixed_margin":   FIXED_MARGIN,
            "leverage":       LEVERAGE,
            "period":         f"{START_YEAR}-{START_MONTH:02d} to {END_YEAR}-{END_MONTH:02d}",
            "start_capital":  START_CAPITAL,
            "fee_pct":        FEE_RATE,
            "slip_pct":       SLIP_RATE,
            "max_concurrent": MAX_CONCURRENT,
            "coins_listed":   len(COINS),
            "adx_note":       "matches GMaxV1.py live bot adx_calc() exactly",
        },
    }
    for v in VARIANTS:
        vname = v["name"]
        r     = all_results.get(vname, {})
        report[vname] = {
            "interval":      v["interval"],
            "tp_pct":        v["tp_pct"],
            "sl_pct":        v["sl_pct"],
            "max_hold_bars": v["max_hold_bars"],
            "coins_loaded":  len(sym_inds.get(v["interval"], {})),
            "aggregate":     r.get("stats"),
            "filter_stats":  r.get("rej"),
            "per_coin":      r.get("coin_table"),
            "trades":        r.get("trades"),
        }

    with open("backtest_report.json", "w") as f:
        json.dump(report, f, indent=2, default=str)

    # -- Quick comparison table -------------------------------------------------
    print("\n" + "=" * 80)
    print("QUICK COMPARISON TABLE")
    print("=" * 80)
    print(f"{'Variant':<18} {'TF':>4} {'TP%':>5} {'SL%':>5} {'Trades':>7} {'WR%':>7} {'PF':>7} {'DD%':>7} {'Net PnL':>10}  Verdict")
    print("-" * 90)
    for v in VARIANTS:
        vname = v["name"]
        r = all_results.get(vname, {})
        s = r.get("stats")
        if s:
            verdict = "PASS" if s["profit_factor"] >= 1.5 and s["win_rate"] >= 0.42 else "fail"
            print(f"{vname:<18} {v['interval']:>4} {v['tp_pct']*100:>4.1f}% {v['sl_pct']*100:>4.1f}% "
                  f"{s['total_trades']:>7,} {s['win_rate']*100:>6.2f}% {s['profit_factor']:>7.4f} "
                  f"{s['max_drawdown']*100:>6.2f}% ${s['net_pnl']:>9,.2f}  {verdict}")
        else:
            print(f"{vname:<18} {v['interval']:>4} {v['tp_pct']*100:>4.1f}% {v['sl_pct']*100:>4.1f}%   NO DATA")

    print("\nOutputs: backtest_summary.txt  backtest_report.json")
    print("Done")

if __name__ == "__main__":
    main()
