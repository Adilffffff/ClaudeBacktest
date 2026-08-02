"""
Strategy G — EMA Crossover + EMA50 Slope + ADX(14)
8 VARIANTS: 4 rules x 2 timeframes (15m and 30m)

SPEED FIXES vs prior version:
  - Downloads: ThreadPoolExecutor(100) not ProcessPoolExecutor
    Threads are correct for I/O-bound work. No OS process spawn overhead.
  - ADX: O(n) single-pass array, not O(n^2) per-bar recompute.
    Old code called adx_calc() once per bar, reprocessing all prior bars.
    This version runs one pass over all bars and maps values to indices.
  - Indicator phase: ThreadPoolExecutor(50) for parallel per-coin compute.
  - Simulation: ProcessPoolExecutor(8), one worker per variant.
    Uses Pool initializer to avoid pickling large sym_inds data to each worker.

ADX: exact match to GMaxV1.py live bot logic.
  Same ws() Wilder smoothing for TR/+DM/-DM.
  Same SMA seed -> Wilder for final ADX.
  Same period*3 guard before trusting values.

VARIANTS:
  VAR_B   TP 2.0% / SL 18.0% / no hold limit
  VAR_C   TP 2.0% / SL 18.0% / 10-day auto-close
  VAR_D   TP 2.0% / SL 15.0% / 10-day auto-close
  VAR_NEW TP 1.2% / SL 10.0% / 10-day auto-close

Each variant runs on both 15m and 30m = 8 total.
15m max_hold = 960 bars = 10 days
30m max_hold = 480 bars = 10 days

P&L: pnl = FIXED_MARGIN * net_pct  (net_pct = raw_ret * LEVERAGE - costs)
     NOT notional * net_pct (that double-applies leverage).

Universe : 56 whitelisted coins
Period   : Jul 2024 - Jun 2026 (24 months)
Capital  : $100 start, $1 fixed margin per trade, never compounds
Fees     : 0.05%/side  Slippage: 0.02%/side
Max open : 6 concurrent positions portfolio-wide
Entry    : close of signal bar (no lookahead)
Exit     : TP/SL intra-bar high/low test, timeout close, or end-of-data force
"""

import urllib.request, zipfile, csv, io, json, math, multiprocessing
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed

# -- WHITELIST ------------------------------------------------------------------
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

# -- PARAMS ---------------------------------------------------------------------
LEVERAGE        = 5
START_CAPITAL   = 100.0
FIXED_MARGIN    = 1.0
FEE_RATE        = 0.0005
SLIP_RATE       = 0.0002
ROUND_TRIP_COST = (FEE_RATE + SLIP_RATE) * 2
MAX_CONCURRENT  = 6
WARMUP_BARS     = 70      # matches live bot len(closes) < 70 guard

EMA_FAST   = 9
EMA_SLOW   = 21
EMA_TREND  = 50
SLOPE_BARS = 10
ADX_PERIOD = 14
ADX_MIN    = 22.0

BASE_VARIANTS = [
    {"name": "VAR_B",   "tp_pct": 0.020, "sl_pct": 0.18, "hold": False},
    {"name": "VAR_C",   "tp_pct": 0.020, "sl_pct": 0.18, "hold": True},
    {"name": "VAR_D",   "tp_pct": 0.020, "sl_pct": 0.15, "hold": True},
    {"name": "VAR_NEW", "tp_pct": 0.012, "sl_pct": 0.10, "hold": True},
]
TIMEFRAMES = [
    {"interval": "15m", "hold_bars": 960},
    {"interval": "30m", "hold_bars": 480},
]

def build_variants():
    out = []
    for tf in TIMEFRAMES:
        for bv in BASE_VARIANTS:
            out.append({
                "name":          f"{bv['name']}_{tf['interval']}",
                "tp_pct":        bv["tp_pct"],
                "sl_pct":        bv["sl_pct"],
                "max_hold_bars": tf["hold_bars"] if bv["hold"] else None,
                "interval":      tf["interval"],
                "hold_days":     10 if bv["hold"] else None,
            })
    return out

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

def calc_adx_array(bars, period=14):
    """
    O(n) ADX array. Exact match to GMaxV1.py live bot adx_calc() logic.

    Live bot uses:
      ws(arr, p)  -> Wilder smoother for TR / +DM / -DM (running sum form)
      SMA seed -> Wilder for final DX -> ADX

    This function runs that algorithm once over all bars and maps each
    result back to its bar index, instead of calling adx_calc() per bar.

    result[i] == 0.0 for i < period*3 - 1  (matches live bot's guard)
    result[i] == ADX value the live bot would compute at bar i onwards
    """
    n = len(bars)
    if n < period * 3:
        return [0.0] * n

    highs  = [b["high"]  for b in bars]
    lows   = [b["low"]   for b in bars]
    closes = [b["close"] for b in bars]

    # Raw +DM, -DM, TR (length n-1)
    pdm_raw, mdm_raw, tr_raw = [], [], []
    for i in range(1, n):
        up   = highs[i] - highs[i-1]
        down = lows[i-1] - lows[i]
        pdm_raw.append(up   if up > down   and up   > 0 else 0.0)
        mdm_raw.append(down if down > up   and down > 0 else 0.0)
        tr_raw.append(max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i-1]),
            abs(lows[i]  - closes[i-1]),
        ))

    # ws() Wilder smoother — same as live bot ws(v, p)
    def ws(arr, p):
        if len(arr) < p:
            return []
        r = [sum(arr[:p])]
        for x in arr[p:]:
            r.append(r[-1] - r[-1] / p + x)
        return r

    st = ws(tr_raw,  period)   # length n - period
    sp = ws(pdm_raw, period)
    sm = ws(mdm_raw, period)

    if not st:
        return [0.0] * n

    # pdi, mdi, dx (length n - period)
    pdi_arr = [100.0 * p / t if t else 0.0 for p, t in zip(sp, st)]
    mdi_arr = [100.0 * m / t if t else 0.0 for m, t in zip(sm, st)]
    dx_arr  = [100.0 * abs(p - m) / (p + m) if (p + m) else 0.0
               for p, m in zip(pdi_arr, mdi_arr)]

    if len(dx_arr) < period:
        return [0.0] * n

    # ADX: SMA seed then Wilder — same as live bot's final adx loop
    # adx_rolling[k] = ADX value using dx_arr[0 .. period-1+k]
    # Maps to bar index: 2*period - 1 + k
    # We only expose values from bar index period*3 - 1 (live bot guard)
    # which corresponds to k = period (since 2p-1+p = 3p-1)
    adx_val = sum(dx_arr[:period]) / period
    adx_rolling = [adx_val]
    for d in dx_arr[period:]:
        adx_val = (adx_val * (period - 1) + d) / period
        adx_rolling.append(adx_val)

    result = [0.0] * n
    guard_k = period  # adx_rolling[period] is at bar 3*period-1
    for k in range(guard_k, len(adx_rolling)):
        bar_idx = 2 * period - 1 + k
        if bar_idx < n:
            result[bar_idx] = max(0.0, min(100.0, adx_rolling[k]))

    return result

def compute_indicators(args):
    sym, bars = args
    if len(bars) < WARMUP_BARS + 5:
        return sym, None
    closes = [b["close"] for b in bars]
    return sym, {
        "ema9":  calc_ema(closes, EMA_FAST),
        "ema21": calc_ema(closes, EMA_SLOW),
        "ema50": calc_ema(closes, EMA_TREND),
        "adx":   calc_adx_array(bars, ADX_PERIOD),
        "bars":  bars,
    }

# -- SIMULATION (subprocess worker) --------------------------------------------
# sym_inds is set once per worker via Pool initializer to avoid
# pickling the large indicator dataset for every task.
_WORKER_SYM_INDS = {}

def _init_sim_worker(sym_inds_dict):
    global _WORKER_SYM_INDS
    _WORKER_SYM_INDS = sym_inds_dict

def _sim_task(v):
    return _run_variant(v, _WORKER_SYM_INDS)

def _run_variant(v, sym_inds):
    vname         = v["name"]
    tp_pct        = v["tp_pct"]
    sl_pct        = v["sl_pct"]
    max_hold_bars = v["max_hold_bars"]
    NOTIONAL      = FIXED_MARGIN * LEVERAGE

    events = []
    for sym, inds in sym_inds.items():
        for i in range(len(inds["bars"])):
            events.append((inds["bars"][i]["ts"], sym, i))
    events.sort()

    equity   = START_CAPITAL
    open_pos = {}
    closed   = []
    rej = {"warmup":0,"sym_open":0,"max_concurrent":0,
           "slope":0,"no_cross":0,"adx":0,"insuff_cap":0,"executed":0}

    progress_step = max(1, len(events) // 10)

    for step, (ts, sym, i) in enumerate(events):
        if step % progress_step == 0:
            print(f"  [{vname}] {step/len(events)*100:.0f}%  "
                  f"eq=${equity:,.2f}  open={len(open_pos)}  trades={len(closed)}", flush=True)

        inds = sym_inds[sym]
        bars = inds["bars"]
        bar  = bars[i]

        # EXIT CHECK
        if sym in open_pos:
            pos   = open_pos[sym]
            side  = pos["side"]
            tp    = pos["tp"]
            sl    = pos["sl"]
            held  = i - pos["entry_bar"]
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
                raw_ret = (exit_px-ep)/ep if side=="LONG" else (ep-exit_px)/ep
                net_pct = raw_ret * LEVERAGE - ROUND_TRIP_COST
                pnl     = max(FIXED_MARGIN * net_pct, -FIXED_MARGIN)
                equity += pnl
                closed.append({
                    "symbol":sym,"side":side,
                    "entry_ts":pos["entry_ts"],"exit_ts":ts,
                    "entry_px":ep,"exit_px":exit_px,
                    "notional":NOTIONAL,"margin":FIXED_MARGIN,
                    "pnl":pnl,"win":pnl>0,"duration":held,
                    "tp_hit":hit_tp,"timed_out":timed_out,"forced":False,
                })
                del open_pos[sym]

            if sym in open_pos:
                continue

        # WARMUP
        if i < WARMUP_BARS:
            rej["warmup"] += 1; continue
        if sym in open_pos:
            rej["sym_open"] += 1; continue
        if len(open_pos) >= MAX_CONCURRENT:
            rej["max_concurrent"] += 1; continue

        # FILTERS — same order as live bot check_signal_G
        ema50 = inds["ema50"]
        slope_pct = (ema50[i] - ema50[i-SLOPE_BARS]) / ema50[i-SLOPE_BARS] * 100
        trend_up   = slope_pct >  0.05
        trend_down = slope_pct < -0.05
        if not trend_up and not trend_down:
            rej["slope"] += 1; continue

        ema9  = inds["ema9"]
        ema21 = inds["ema21"]
        cross_long  = ema9[i] > ema21[i] and ema9[i-1] <= ema21[i-1]
        cross_short = ema9[i] < ema21[i] and ema9[i-1] >= ema21[i-1]

        if   cross_long  and trend_up:   direction = "LONG"
        elif cross_short and trend_down: direction = "SHORT"
        else:
            rej["no_cross"] += 1; continue

        if inds["adx"][i] < ADX_MIN:
            rej["adx"] += 1; continue
        if equity < FIXED_MARGIN:
            rej["insuff_cap"] += 1; continue

        # OPEN
        entry_px = bar["close"]
        tp_px = entry_px * (1 + tp_pct) if direction=="LONG" else entry_px * (1 - tp_pct)
        sl_px = entry_px * (1 - sl_pct) if direction=="LONG" else entry_px * (1 + sl_pct)
        open_pos[sym] = {
            "entry":entry_px,"entry_ts":ts,"entry_bar":i,
            "side":direction,"tp":tp_px,"sl":sl_px,
        }
        rej["executed"] += 1

    # FORCE-CLOSE
    fc = 0
    for sym, pos in open_pos.items():
        bars    = sym_inds[sym]["bars"]
        last    = bars[-1]
        exit_px = last["close"]
        ep      = pos["entry"]
        side    = pos["side"]
        held    = len(bars) - 1 - pos["entry_bar"]
        raw_ret = (exit_px-ep)/ep if side=="LONG" else (ep-exit_px)/ep
        net_pct = raw_ret * LEVERAGE - ROUND_TRIP_COST
        pnl     = max(FIXED_MARGIN * net_pct, -FIXED_MARGIN)
        equity += pnl
        closed.append({
            "symbol":sym,"side":side,
            "entry_ts":pos["entry_ts"],"exit_ts":last["ts"],
            "entry_px":ep,"exit_px":exit_px,
            "notional":FIXED_MARGIN*LEVERAGE,"margin":FIXED_MARGIN,
            "pnl":pnl,"win":pnl>0,"duration":held,
            "tp_hit":False,"timed_out":False,"forced":True,
        })
        fc += 1

    timed = sum(1 for t in closed if t.get("timed_out"))
    print(f"  [{vname}] DONE  eq=${equity:,.2f}  trades={len(closed)}  "
          f"force={fc}  timeout={timed}  adx_rej={rej['adx']}", flush=True)
    return vname, closed, equity, rej

# -- STATS ----------------------------------------------------------------------
def compute_stats(trades, start_cap, final_eq):
    if not trades:
        return None
    wins   = [t for t in trades if t["win"]]
    losses = [t for t in trades if not t["win"]]
    n, nw, nl = len(trades), len(wins), len(losses)
    gw = sum(t["pnl"] for t in wins)
    gl = abs(sum(t["pnl"] for t in losses))
    pf = gw / gl if gl else float("inf")
    wr = nw / n
    exp= sum(t["pnl"] for t in trades) / n
    dur= [t["duration"] for t in trades]
    ts = sorted(trades, key=lambda t: t["exit_ts"])
    eq = start_cap; curve=[eq]
    for t in ts:
        eq += t["pnl"]; curve.append(eq)
    peak=start_cap; mdd=0.0
    for v in curve:
        peak=max(peak,v); mdd=max(mdd,(peak-v)/peak)
    monthly={}
    for t in ts:
        dt=datetime.fromtimestamp(t["exit_ts"]/1000,tz=timezone.utc)
        k=f"{dt.year}-{dt.month:02d}"; monthly[k]=monthly.get(k,0)+t["pnl"]
    daily={}
    for t in ts:
        dt=datetime.fromtimestamp(t["exit_ts"]/1000,tz=timezone.utc)
        k=(dt.year,dt.month,dt.day); daily[k]=daily.get(k,0)+t["pnl"]
    days=list(daily.values()); sharpe=sortino=0.0
    if len(days)>1:
        mu=sum(days)/len(days)
        std=math.sqrt(sum((d-mu)**2 for d in days)/len(days))
        if std: sharpe=mu/std*math.sqrt(365)
        ns=sum(d**2 for d in days if d<0)/len(days)
        if ns: sortino=mu/math.sqrt(ns)*math.sqrt(365)
    bws=bloss=cw=cl=0
    for t in ts:
        if t["win"]: cw+=1;cl=0
        else: cl+=1;cw=0
        bws=max(bws,cw); bloss=max(bloss,cl)
    lo=[t for t in trades if t["side"]=="LONG"]
    sh=[t for t in trades if t["side"]=="SHORT"]
    return {
        "total_trades":n,"wins":nw,"losses":nl,
        "forced_closed":sum(1 for t in trades if t.get("forced")),
        "timed_out":sum(1 for t in trades if t.get("timed_out")),
        "win_rate":wr,"profit_factor":pf,"net_pnl":final_eq-start_cap,
        "final_equity":final_eq,"max_drawdown":mdd,"sharpe":sharpe,"sortino":sortino,
        "avg_win":gw/nw if nw else 0,"avg_loss":-gl/nl if nl else 0,"expectancy":exp,
        "avg_duration_bars":sum(dur)/n,"max_duration_bars":max(dur),"min_duration_bars":min(dur),
        "long_trades":len(lo),"long_wr":sum(1 for t in lo if t["win"])/len(lo) if lo else 0,
        "short_trades":len(sh),"short_wr":sum(1 for t in sh if t["win"])/len(sh) if sh else 0,
        "best_win_streak":bws,"best_loss_streak":bloss,
        "monthly_pnl":dict(sorted(monthly.items())),
    }

def per_coin_stats(trades):
    by_sym={}
    for t in trades: by_sym.setdefault(t["symbol"],[]).append(t)
    rows=[]
    for sym,st in by_sym.items():
        w=sum(1 for t in st if t["win"]); l=len(st)-w
        gw=sum(t["pnl"] for t in st if t["win"])
        gl=abs(sum(t["pnl"] for t in st if not t["win"]))
        rows.append({"symbol":sym,"trades":len(st),"wins":w,"losses":l,
                     "win_rate":w/len(st),"profit_factor":gw/gl if gl else float("inf"),
                     "net_pnl":sum(t["pnl"] for t in st)})
    rows.sort(key=lambda r:-r["profit_factor"]); return rows

def write_summary(v, stats, coin_table, rej, loaded):
    vname=v["name"]; mins=int(v["interval"].replace("m",""))
    a=[]; w=a.append
    hold_label=(f"{v['max_hold_bars']} bars ({v['hold_days']}d)" if v["max_hold_bars"] else "unlimited")
    w("="*70)
    w(f"STRATEGY G — {vname}  |  TP {v['tp_pct']*100:.1f}%  SL {v['sl_pct']*100:.1f}%  5x  [{v['interval']}]")
    w(f"Max hold : {hold_label}")
    w(f"Period   : {START_YEAR}-{START_MONTH:02d} to {END_YEAR}-{END_MONTH:02d}  Capital: ${START_CAPITAL:.2f}")
    w(f"Universe : {len(COINS)} coins | {loaded} loaded")
    w(f"ADX      : GMaxV1.py exact match (period*3 guard, ws() Wilder, SMA->Wilder ADX)")
    w("="*70)
    if stats is None:
        w("NO TRADES EXECUTED"); return "\n".join(a)
    ok = stats["profit_factor"]>=1.5 and stats["win_rate"]>=0.42
    verdict = "USABLE (candidate)" if ok else "DOES NOT MEET TARGETS"
    w(f"Total Trades     : {stats['total_trades']:,}")
    w(f"Wins / Losses    : {stats['wins']:,} / {stats['losses']:,}")
    w(f"Force-closed EoD : {stats['forced_closed']:,}")
    if v["max_hold_bars"]:
        w(f"Timed-out ({v['hold_days']}d)  : {stats['timed_out']:,}")
    w(f"Win Rate         : {stats['win_rate']*100:.2f}%")
    w(f"Profit Factor    : {stats['profit_factor']:.4f}")
    w(f"Net PnL          : ${stats['net_pnl']:,.2f}  (on ${START_CAPITAL:.2f})")
    w(f"Final Equity     : ${stats['final_equity']:,.2f}")
    w(f"Max Drawdown     : {stats['max_drawdown']*100:.2f}%")
    w(f"Sharpe           : {stats['sharpe']:.3f}")
    w(f"Sortino          : {stats['sortino']:.3f}")
    w(f"Avg Win          : ${stats['avg_win']:.4f}")
    w(f"Avg Loss         : ${stats['avg_loss']:.4f}")
    w(f"Expectancy       : ${stats['expectancy']:.4f}/trade")
    w(f"Avg Duration     : {stats['avg_duration_bars']:.1f} bars ({stats['avg_duration_bars']*mins/60:.1f}h)")
    w(f"Max Duration     : {stats['max_duration_bars']} bars ({stats['max_duration_bars']*mins/60/24:.1f}d)")
    w(f"Long  Trades     : {stats['long_trades']:,}  WR {stats['long_wr']*100:.2f}%")
    w(f"Short Trades     : {stats['short_trades']:,}  WR {stats['short_wr']*100:.2f}%")
    w(f"Win Streak Best  : {stats['best_win_streak']}")
    w(f"Loss Streak Best : {stats['best_loss_streak']}")
    w(f"VERDICT          : {verdict}")
    w("")
    w("-- Scale reference ---")
    w(f"  $1/trade  -> ${stats['net_pnl']:,.2f}")
    w(f"  $10/trade -> ~${stats['net_pnl']*10:,.0f}")
    w(f"  $50/trade -> ~${stats['net_pnl']*50:,.0f}")
    w("")
    w("-- Filter Rejections ---")
    total=sum(rej.values())
    w(f"  Warmup           : {rej['warmup']:>9,}")
    w(f"  Sym already open : {rej['sym_open']:>9,}")
    w(f"  Max concurrent   : {rej['max_concurrent']:>9,}")
    w(f"  Slope            : {rej['slope']:>9,}")
    w(f"  No crossover     : {rej['no_cross']:>9,}")
    w(f"  ADX < {ADX_MIN:.0f}         : {rej['adx']:>9,}")
    w(f"  Insuff capital   : {rej['insuff_cap']:>9,}")
    w(f"  EXECUTED         : {rej['executed']:>9,}")
    w(f"  Total bar-events : {total:>9,}")
    w("")
    w("-- Monthly PnL ---")
    for ym,pnl in stats["monthly_pnl"].items():
        w(f"  {ym}: ${'+'if pnl>=0 else ''}{pnl:,.4f}")
    w("")
    w("-- Per-Coin (by PF) ---")
    w(f"  {'Symbol':<24} {'PF':>6}  {'WR':>7}  {'Trades':>7}  {'Net PnL':>12}")
    for r in coin_table:
        pf_s=f"{r['profit_factor']:.3f}" if r["profit_factor"]!=float("inf") else "  INF"
        w(f"  {r['symbol']:<24} {pf_s:>6}  {r['win_rate']*100:>6.1f}%  {r['trades']:>7,}  ${r['net_pnl']:>11,.4f}")
    return "\n".join(a)

# -- MAIN -----------------------------------------------------------------------
def main():
    print(f"Strategy G | 8 variants | ThreadPoolExecutor downloads | O(n) ADX | 8 parallel sims")
    print(f"Variants: {[v['name'] for v in VARIANTS]}")
    print(f"Period  : {START_YEAR}-{START_MONTH:02d} -> {END_YEAR}-{END_MONTH:02d} | {len(COINS)} coins")
    print()

    # PHASE 1: Download — ThreadPoolExecutor (I/O bound, no process overhead)
    tasks = [(sym, tf["interval"]) for sym in COINS for tf in TIMEFRAMES]
    print(f"Phase 1: Downloading {len(tasks)} symbol x timeframe combos (100 threads)...")
    all_bars = {"15m": {}, "30m": {}}
    with ThreadPoolExecutor(max_workers=100) as ex:
        futs = {ex.submit(fetch_symbol, t): t for t in tasks}
        for fut in as_completed(futs):
            sym, interval = futs[fut]
            try:
                bars = fut.result()
                if bars:
                    all_bars[interval][sym] = bars
                    print(f"  OK {sym} [{interval}]: {len(bars):,} bars")
                else:
                    print(f"  MISS {sym} [{interval}]")
            except Exception as e:
                print(f"  ERR {sym} [{interval}]: {e}")

    for iv in ["15m","30m"]:
        n = len(all_bars[iv])
        print(f"Loaded [{iv}]: {n}/{len(COINS)}")
        if n == 0: print(f"FATAL: 0 symbols for {iv}")

    # PHASE 2: Indicators — ThreadPoolExecutor (parallel per coin, O(n) ADX)
    print("\nPhase 2: Computing indicators (50 threads, O(n) ADX)...")
    sym_inds = {"15m": {}, "30m": {}}
    for iv in ["15m","30m"]:
        ind_tasks = [(sym, bars) for sym, bars in all_bars[iv].items()
                     if len(bars) >= WARMUP_BARS + 5]
        with ThreadPoolExecutor(max_workers=50) as ex:
            futs = {ex.submit(compute_indicators, t): t[0] for t in ind_tasks}
            for fut in as_completed(futs):
                sym = futs[fut]
                try:
                    sym_out, inds = fut.result()
                    if inds:
                        sym_inds[iv][sym_out] = inds
                except Exception as e:
                    print(f"  IND ERR {sym} [{iv}]: {e}")
        print(f"  [{iv}] {len(sym_inds[iv])} symbols ready.")

    # PHASE 3: Simulate 8 variants in parallel — ProcessPoolExecutor
    # Pool initializer sets sym_inds once per worker (no re-pickling per task)
    print("\nPhase 3: Simulating 8 variants in parallel (8 workers)...")
    all_results = {}

    for iv in ["15m","30m"]:
        iv_variants = [v for v in VARIANTS if v["interval"] == iv]
        if not sym_inds[iv]:
            print(f"  SKIP {iv}: no data"); continue

        ctx = multiprocessing.get_context("fork")
        with ctx.Pool(
            processes=4,
            initializer=_init_sim_worker,
            initargs=(sym_inds[iv],)
        ) as pool:
            results = pool.map(_sim_task, iv_variants)

        for vname_out, trades, final_eq, rej in results:
            all_results[vname_out] = {"trades":trades,"final_eq":final_eq,"rej":rej}
            print(f"  [{vname_out}] {len(trades)} trades, eq=${final_eq:,.2f}")

    # PHASE 4: Stats + output
    print("\nPhase 4: Computing stats and writing output...")
    summaries = []
    report = {
        "meta": {
            "strategy":       "Strategy G (GMaxV1 ADX exact, 8 variants, 15m+30m)",
            "variants":       [v["name"] for v in VARIANTS],
            "sizing":         "fixed $1 margin",
            "leverage":       LEVERAGE,
            "period":         f"{START_YEAR}-{START_MONTH:02d} to {END_YEAR}-{END_MONTH:02d}",
            "start_capital":  START_CAPITAL,
            "fee_pct":        FEE_RATE,
            "slip_pct":       SLIP_RATE,
            "max_concurrent": MAX_CONCURRENT,
            "coins_listed":   len(COINS),
            "adx_note":       "O(n) array, exact match to GMaxV1.py adx_calc()",
        }
    }

    for v in VARIANTS:
        vname    = v["name"]
        iv       = v["interval"]
        loaded   = len(sym_inds.get(iv, {}))
        r        = all_results.get(vname)
        if not r:
            continue
        stats      = compute_stats(r["trades"], START_CAPITAL, r["final_eq"])
        coin_table = per_coin_stats(r["trades"])
        summary    = write_summary(v, stats, coin_table, r["rej"], loaded)
        summaries.append(summary)
        print("\n" + summary)
        report[vname] = {
            "interval":v["interval"],"tp_pct":v["tp_pct"],"sl_pct":v["sl_pct"],
            "max_hold_bars":v["max_hold_bars"],"coins_loaded":loaded,
            "aggregate":stats,"filter_stats":r["rej"],
            "per_coin":coin_table,"trades":r["trades"],
        }

    with open("backtest_summary.txt","w") as f:
        f.write(("\n\n"+"="*70+"\n\n").join(summaries)+"\n")
    with open("backtest_report.json","w") as f:
        json.dump(report,f,indent=2,default=str)

    # Quick comparison table
    print("\n"+"="*80)
    print("QUICK COMPARISON")
    print("="*80)
    print(f"{'Variant':<18} {'TF':>4} {'TP':>5} {'SL':>5} {'Trades':>7} {'WR%':>7} {'PF':>7} {'DD%':>7} {'PnL':>9}  Result")
    print("-"*85)
    for v in VARIANTS:
        r = all_results.get(v["name"],{})
        s = compute_stats(r.get("trades",[]), START_CAPITAL, r.get("final_eq",START_CAPITAL)) if r else None
        if s:
            ok="PASS" if s["profit_factor"]>=1.5 and s["win_rate"]>=0.42 else "fail"
            print(f"{v['name']:<18} {v['interval']:>4} {v['tp_pct']*100:>4.1f}% {v['sl_pct']*100:>4.1f}% "
                  f"{s['total_trades']:>7,} {s['win_rate']*100:>6.2f}% {s['profit_factor']:>7.4f} "
                  f"{s['max_drawdown']*100:>6.2f}% ${s['net_pnl']:>8,.2f}  {ok}")
        else:
            print(f"{v['name']:<18} {v['interval']:>4} {v['tp_pct']*100:>4.1f}% {v['sl_pct']*100:>4.1f}%   NO DATA")

    print("\nOutputs: backtest_summary.txt  backtest_report.json")
    print("Done")

if __name__ == "__main__":
    main()
