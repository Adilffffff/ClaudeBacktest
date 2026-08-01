"""
Strategy G — EMA Crossover + EMA50 Slope + ADX(14) — 15m candles
Fixed sizing: $1 margin × 5x leverage = $5 notional per trade

TWO VARIANTS run in one script (single download pass):

  VAR_B — original rules, no time limit
           TP 2.0%  |  SL 18.0%  |  5x isolated  |  no max hold

  VAR_C — identical to VAR_B + 10-day auto-close rule
           If a position is still open after 960 bars (10 days × 24h × 4bars/h),
           it is force-closed at the next bar's close regardless of P&L.

Coin universe : 144 coins (full list)
Period        : Jul 2024 – Jun 2026 (24 months)
Capital       : $100 starting, $1 fixed margin per trade (never compounds)
Max concurrent: 6 positions portfolio-wide
Fees          : 0.05%/side  Slippage: 0.02%/side
Entry         : close of signal bar (closed candle only, no lookahead)
Exit          : TP or SL hit intra-bar (high/low test)
              : VAR_C only — auto-close at bar 960 if neither TP nor SL hit
              : End of data — force-close any remaining open positions

RULES ENFORCED (both variants):
  - One position per coin at a time — a coin cannot open a new trade while
    it already has one open; it only becomes eligible again once that
    position is closed (TP, SL, timeout, or end-of-data force-close).
  - Portfolio-wide cap of MAX_CONCURRENT open positions across all coins.
"""

import urllib.request, zipfile, csv, io, json, math
from datetime import datetime, timezone
from concurrent.futures import ProcessPoolExecutor, as_completed

# ── UNIVERSE (144 coins) ───────────────────────────────────────────────────────
COINS = [
    "0GUSDT","1000000BOBUSDT","1000BONKUSDT","1000CATUSDT","1000RATSUSDT",
    "1000SATSUSDT","A2ZUSDT","ACHUSDT","AI16ZUSDT","AINUSDT","AIOTUSDT",
    "ALGOUSDT","ALICEUSDT","ALPINEUSDT","ANKRUSDT","ARKMUSDT","ASRUSDT",
    "ASTERUSDT","ATAUSDT","AUSDT","AWEUSDT","AXLUSDT","BANDUSDT","BANKUSDT",
    "BASEDUSDT","BASUSDT","BATUSDT","BDXNUSDT","BELUSDT","BIDUSDT","BMTUSDT",
    "BTRUSDT","CFXUSDT","CHIPUSDT","COAIUSDT","COMBOUSDT","COMMONUSDT",
    "COTIUSDT","CRCLUSDT","CUSDT","DAMUSDT","DEFIUSDT","DEXEUSDT","DIAUSDT",
    "DMCUSDT","EIGENUSDT","ELSAUSDT","ENAUSDT","EPICUSDT","EPTUSDT","ESPUSDT",
    "ETCUSDT","ETHUSDT","EVAAUSDT","FIOUSDT","FLNCUSDT","FLUXUSDT","FOLKSUSDT",
    "FUNUSDT","FXSUSDT","GLMUSDT","GRIFFAINUSDT","GUAUSDT","HANAUSDT",
    "HEMIUSDT","ICPUSDT","ICXUSDT","INITUSDT","IOSTUSDT","IOUSDT","IPUSDT",
    "KITEUSDT","LABUSDT","LIGHTUSDT","LRCUSDT","LYNUSDT","MAGICUSDT","MAVUSDT",
    "MEGAUSDT","MILKUSDT","MOODENGUSDT","MTLUSDT","NFPUSDT","NMRUSDT",
    "NOMUSDT","NOTUSDT","OBOLUSDT","OMGUSDT","OPENUSDT","OPNUSDT","ORBSUSDT",
    "PEOPLEUSDT","PIPPINUSDT","PIXELUSDT","PLUMEUSDT","POLUSDT","POWERUSDT",
    "POWRUSDT","PROMPTUSDT","PTBUSDT","PUMPBTCUSDT","PUNDIXUSDT","QUICKUSDT",
    "RAVEUSDT","REEFUSDT","RESOLVUSDT","REZUSDT","RLSUSDT","RVVUSDT",
    "SAGAUSDT","SAHARAUSDT","SANTOSUSDT","SEIUSDT","SIGNUSDT","SKRUSDT",
    "SNDKUSDT","SOMIUSDT","SPELLUSDT","SPKUSDT","STABLEUSDT","STBLUSDT",
    "STXUSDT","TNSRUSDT","TRBUSDT","TRUTHUSDT","TURBOUSDT","UBUSDT",
    "USUALUSDT","UXLINKUSDT","VANRYUSDT","VINEUSDT","VIRTUALUSDT","VVVUSDT",
    "WAXPUSDT","WLDUSDT","XCNUSDT","XEMUSDT","XLMUSDT","XRPUSDT","YBUSDT",
    "ZECUSDT","ZENUSDT","ZEREBROUSDT","ZKJUSDT",
]

INTERVAL        = "15m"
START_YEAR, START_MONTH = 2024, 7
END_YEAR,   END_MONTH   = 2026, 6

# ── STRATEGY PARAMS (same for both variants) ───────────────────────────────────
LEVERAGE        = 5
TP_PCT          = 0.020       # 2.0%
SL_PCT          = 0.180       # 18.0%
START_CAPITAL   = 100.0
FIXED_MARGIN    = 1.0         # $1 per trade — change to scale
FEE_RATE        = 0.0005
SLIP_RATE       = 0.0002
ROUND_TRIP_COST = (FEE_RATE + SLIP_RATE) * 2
MAX_CONCURRENT  = 6
WARMUP_BARS     = 60

# ── VARIANT PARAMS ─────────────────────────────────────────────────────────────
# VAR_B: no time limit (None = disabled)
# VAR_C: auto-close after 10 days = 10d × 24h × 60min / 15min = 960 bars
VARIANTS = [
    {"name": "VAR_B", "max_hold_bars": None},
    {"name": "VAR_C", "max_hold_bars": 960},
]

# ── INDICATOR PARAMS ──────────────────────────────────────────────────────────
EMA_FAST    = 9
EMA_SLOW    = 21
EMA_TREND   = 50
SLOPE_BARS  = 10
SLOPE_MIN   = 0.0005
ADX_PERIOD  = 14
ADX_MIN     = 22.0

BASE_URL = "https://data.binance.vision/data/futures/um/monthly/klines"

# ── DATA ───────────────────────────────────────────────────────────────────────
def month_range(sy, sm, ey, em):
    y, m = sy, sm
    while (y, m) <= (ey, em):
        yield y, m
        m += 1
        if m > 12:
            m = 1; y += 1

def fetch_symbol(symbol):
    rows = []
    for y, m in month_range(START_YEAR, START_MONTH, END_YEAR, END_MONTH):
        url = f"{BASE_URL}/{symbol}/{INTERVAL}/{symbol}-{INTERVAL}-{y}-{m:02d}.zip"
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

# ── INDICATORS ─────────────────────────────────────────────────────────────────
def calc_ema(values, period):
    k = 2.0 / (period + 1)
    ema = [0.0] * len(values)
    ema[0] = values[0]
    for i in range(1, len(values)):
        ema[i] = values[i] * k + ema[i-1] * (1 - k)
    return ema

def calc_adx(bars, period=14):
    n = len(bars)
    pdm = [0.0]*n; ndm = [0.0]*n; tr = [0.0]*n
    for i in range(1, n):
        up   = bars[i]["high"] - bars[i-1]["high"]
        down = bars[i-1]["low"] - bars[i]["low"]
        pdm[i] = up   if up > down   and up   > 0 else 0.0
        ndm[i] = down if down > up   and down > 0 else 0.0
        tr[i]  = max(
            bars[i]["high"] - bars[i]["low"],
            abs(bars[i]["high"] - bars[i-1]["close"]),
            abs(bars[i]["low"]  - bars[i-1]["close"]),
        )
    def wilder(arr):
        s = [0.0]*n
        s[period] = sum(arr[1:period+1])
        for i in range(period+1, n):
            s[i] = s[i-1] - s[i-1]/period + arr[i]
        return s
    spdm = wilder(pdm); sndm = wilder(ndm); str_ = wilder(tr)
    dx = [0.0]*n
    for i in range(period, n):
        if str_[i] == 0:
            continue
        pdi = 100 * spdm[i] / str_[i]
        ndi = 100 * sndm[i] / str_[i]
        d   = pdi + ndi
        if d:
            dx[i] = 100 * abs(pdi - ndi) / d
    return wilder(dx)

# ── PORTFOLIO SIMULATION ───────────────────────────────────────────────────────
def run_portfolio(sym_inds, max_hold_bars, variant_name):
    NOTIONAL = FIXED_MARGIN * LEVERAGE

    events = []
    for sym, inds in sym_inds.items():
        for i, bar in enumerate(inds["bars"]):
            events.append((bar["ts"], sym, i))
    events.sort()
    print(f"\n[{variant_name}] Processing {len(events):,} bar events...")

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

    progress_step = max(1, len(events) // 20)

    for step, (ts, sym, i) in enumerate(events):
        if step % progress_step == 0:
            pct = step / len(events) * 100
            print(f"  [{variant_name}] {pct:.0f}%  equity=${equity:,.2f}  open={len(open_pos)}  trades={len(closed)}")

        inds = sym_inds[sym]
        bars = inds["bars"]
        bar  = bars[i]

        # ── EXIT CHECK ──────────────────────────────────────────────────────────
        if sym in open_pos:
            pos    = open_pos[sym]
            side   = pos["side"]
            tp     = pos["tp"]
            sl     = pos["sl"]
            held   = i - pos["entry_bar"]
            hit_tp = hit_sl = timed_out = False

            # VAR_C: 10-day auto-close fires before TP/SL check
            if max_hold_bars is not None and held >= max_hold_bars:
                timed_out = True
            elif side == "LONG":
                if bar["high"] >= tp:  hit_tp = True
                elif bar["low"] <= sl: hit_sl = True
            else:
                if bar["low"] <= tp:   hit_tp = True
                elif bar["high"] >= sl: hit_sl = True

            if hit_tp or hit_sl or timed_out:
                if timed_out:
                    exit_px = bar["close"]   # close at current bar's close
                else:
                    exit_px = tp if hit_tp else sl

                ep      = pos["entry"]
                raw_ret = (exit_px - ep)/ep if side == "LONG" else (ep - exit_px)/ep
                net_pct = raw_ret * LEVERAGE - ROUND_TRIP_COST
                pnl     = NOTIONAL * net_pct
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

        # ── WARMUP GUARD ───────────────────────────────────────────────────────
        if i < WARMUP_BARS:
            rej["warmup"] += 1
            continue

        if sym in open_pos:
            rej["sym_open"] += 1
            continue

        if len(open_pos) >= MAX_CONCURRENT:
            rej["max_concurrent"] += 1
            continue

        # ── ENTRY FILTERS ──────────────────────────────────────────────────────
        ema50    = inds["ema50"]
        slope    = (ema50[i] - ema50[i-SLOPE_BARS]) / ema50[i-SLOPE_BARS]
        long_ok  = slope >  SLOPE_MIN
        short_ok = slope < -SLOPE_MIN
        if not long_ok and not short_ok:
            rej["slope"] += 1
            continue

        ema9  = inds["ema9"]
        ema21 = inds["ema21"]
        cross_long  = ema9[i] > ema21[i] and ema9[i-1] <= ema21[i-1]
        cross_short = ema9[i] < ema21[i] and ema9[i-1] >= ema21[i-1]

        if cross_long and long_ok:
            direction = "LONG"
        elif cross_short and short_ok:
            direction = "SHORT"
        else:
            rej["no_cross"] += 1
            continue

        if inds["adx"][i] < ADX_MIN:
            rej["adx"] += 1
            continue

        if equity < FIXED_MARGIN:
            rej["insuff_cap"] += 1
            continue

        # ── OPEN POSITION ──────────────────────────────────────────────────────
        entry_px = bar["close"]
        if direction == "LONG":
            tp_px = entry_px * (1 + TP_PCT)
            sl_px = entry_px * (1 - SL_PCT)
        else:
            tp_px = entry_px * (1 - TP_PCT)
            sl_px = entry_px * (1 + SL_PCT)

        open_pos[sym] = {
            "entry":      entry_px,
            "entry_ts":   ts,
            "entry_bar":  i,
            "side":       direction,
            "tp":         tp_px,
            "sl":         sl_px,
        }
        rej["executed"] += 1

    # ── FORCE-CLOSE any remaining open positions at end of data ────────────────
    force_closed = 0
    for sym, pos in open_pos.items():
        bars    = sym_inds[sym]["bars"]
        last    = bars[-1]
        exit_px = last["close"]
        ep      = pos["entry"]
        side    = pos["side"]
        held    = len(bars) - 1 - pos["entry_bar"]
        raw_ret = (exit_px - ep)/ep if side == "LONG" else (ep - exit_px)/ep
        net_pct = raw_ret * LEVERAGE - ROUND_TRIP_COST
        pnl     = NOTIONAL * net_pct
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

    if force_closed:
        print(f"  [{variant_name}] Force-closed {force_closed} positions at end of data.")

    timed_out_count = sum(1 for t in closed if t.get("timed_out"))
    print(f"  [{variant_name}] Done. Equity=${equity:,.2f}  Trades={len(closed)}  TimedOut={timed_out_count}")
    return closed, equity, rej

# ── STATISTICS ─────────────────────────────────────────────────────────────────
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

    durations   = [t["duration"] for t in trades]
    avg_dur     = sum(durations) / n
    max_dur     = max(durations)
    min_dur     = min(durations)

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
        "total_trades":     n,
        "wins":             nw,
        "losses":           nl,
        "forced_closed":    len(forced),
        "timed_out":        len(timed),
        "win_rate":         wr,
        "profit_factor":    pf,
        "net_pnl":          final_eq - start_cap,
        "final_equity":     final_eq,
        "max_drawdown":     max_dd,
        "sharpe":           sharpe,
        "sortino":          sortino,
        "avg_win":          gross_win/nw   if nw else 0,
        "avg_loss":         -gross_loss/nl if nl else 0,
        "expectancy":       exp,
        "avg_duration_bars": avg_dur,
        "max_duration_bars": max_dur,
        "min_duration_bars": min_dur,
        "long_trades":      len(longs),
        "long_wr":          lwr,
        "short_trades":     len(shorts),
        "short_wr":         swr,
        "best_win_streak":  bws,
        "best_loss_streak": bloss,
        "monthly_pnl":      dict(sorted(monthly.items())),
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

# ── OUTPUT ─────────────────────────────────────────────────────────────────────
def write_summary(variant_name, max_hold_bars, stats, coin_table, rej, loaded):
    lines = []
    a = lines.append

    hold_label = f"{max_hold_bars} bars ({max_hold_bars*15//60//24}d)" if max_hold_bars else "unlimited"

    a("=" * 70)
    a(f"STRATEGY G — {variant_name}  |  TP 2.0%  SL 18.0%  5x Isolated")
    a(f"Max hold : {hold_label}")
    a(f"Sizing   : ${FIXED_MARGIN:.2f} margin × {LEVERAGE}x = ${FIXED_MARGIN*LEVERAGE:.2f} notional  (FIXED)")
    a(f"Period   : {START_YEAR}-{START_MONTH:02d} to {END_YEAR}-{END_MONTH:02d}  |  Capital: ${START_CAPITAL:.2f}")
    a(f"Universe : {len(COINS)} coins listed | {loaded} loaded with data")
    a("=" * 70)

    if stats is None:
        a("NO TRADES EXECUTED"); return "\n".join(lines)

    verdict = "USABLE ✓" if (stats["profit_factor"] >= 1.5 and stats["win_rate"] >= 0.42) else "DOES NOT MEET TARGETS ✗"

    a(f"Total Trades     : {stats['total_trades']:,}")
    a(f"Wins / Losses    : {stats['wins']:,} / {stats['losses']:,}")
    a(f"Force-closed EoD : {stats['forced_closed']:,}  (end of data)")
    if max_hold_bars:
        a(f"Timed-out (10d)  : {stats['timed_out']:,}  (hit {max_hold_bars}-bar limit)")
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
    a(f"Duration — Avg   : {stats['avg_duration_bars']:.1f} bars  ({stats['avg_duration_bars']*15/60:.1f} hours)")
    a(f"Duration — Max   : {stats['max_duration_bars']} bars  ({stats['max_duration_bars']*15/60:.1f} hours  /  {stats['max_duration_bars']*15/60/24:.1f} days)")
    a(f"Duration — Min   : {stats['min_duration_bars']} bars  ({stats['min_duration_bars']*15/60:.2f} hours)")
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

# ── MAIN ───────────────────────────────────────────────────────────────────────
def main():
    print(f"Strategy G — TP {TP_PCT*100:.1f}%  SL {SL_PCT*100:.1f}%  {LEVERAGE}x Isolated  |  Fixed ${FIXED_MARGIN}/trade")
    print(f"Variants: {[v['name'] for v in VARIANTS]}")
    print(f"Period: {START_YEAR}-{START_MONTH:02d} → {END_YEAR}-{END_MONTH:02d}  |  {len(COINS)} coins")
    print()

    # ── Phase 1: Download once, reuse for both variants ──
    print(f"Phase 1: Downloading {len(COINS)} symbols (50 workers)...")
    all_bars = {}
    with ProcessPoolExecutor(max_workers=50) as ex:
        futs = {ex.submit(fetch_symbol, sym): sym for sym in COINS}
        for fut in as_completed(futs):
            sym = futs[fut]
            try:
                bars = fut.result()
                if bars:
                    all_bars[sym] = bars
                    print(f"  ✓ {sym}: {len(bars):,} bars")
                else:
                    print(f"  ✗ {sym}: no data")
            except Exception as e:
                print(f"  ✗ {sym}: {e}")

    loaded = len(all_bars)
    print(f"\nLoaded {loaded}/{len(COINS)} symbols")
    if loaded == 0:
        print("FATAL: 0 symbols loaded."); return
    if loaded < len(COINS) * 0.1:
        print("FATAL: <10% loaded — probable network block."); return

    # ── Phase 2: Precompute indicators once ──
    print("\nPhase 2: Computing indicators...")
    sym_inds = {}
    for sym, bars in all_bars.items():
        if len(bars) < WARMUP_BARS + 5:
            continue
        closes = [b["close"] for b in bars]
        sym_inds[sym] = {
            "ema9":  calc_ema(closes, EMA_FAST),
            "ema21": calc_ema(closes, EMA_SLOW),
            "ema50": calc_ema(closes, EMA_TREND),
            "adx":   calc_adx(bars, ADX_PERIOD),
            "bars":  bars,
        }
    print(f"  {len(sym_inds)} symbols ready.")

    # ── Phase 3: Run both variants ──
    all_results = {}
    for v in VARIANTS:
        vname     = v["name"]
        max_hold  = v["max_hold_bars"]
        print(f"\nPhase 3 [{vname}]: Simulating (max_hold={max_hold})...")
        trades, final_eq, rej = run_portfolio(sym_inds, max_hold, vname)
        stats      = compute_stats(trades, START_CAPITAL, final_eq)
        coin_table = per_coin_stats(trades)
        summary    = write_summary(vname, max_hold, stats, coin_table, rej, loaded)
        all_results[vname] = {
            "trades": trades, "final_eq": final_eq,
            "rej": rej, "stats": stats,
            "coin_table": coin_table, "summary": summary,
        }
        print("\n" + summary)

    # ── Write outputs ──
    combined_summary = "\n\n" + ("=" * 70) + "\n\n".join(
        all_results[v["name"]]["summary"] for v in VARIANTS
    )
    with open("backtest_summary.txt", "w") as f:
        f.write(combined_summary + "\n")

    report = {
        "meta": {
            "strategy":       "Strategy G",
            "variants":       [v["name"] for v in VARIANTS],
            "sizing":         "fixed",
            "fixed_margin":   FIXED_MARGIN,
            "notional":       FIXED_MARGIN * LEVERAGE,
            "tp_pct":         TP_PCT,
            "sl_pct":         SL_PCT,
            "leverage":       LEVERAGE,
            "margin_type":    "isolated",
            "period":         f"{START_YEAR}-{START_MONTH:02d} to {END_YEAR}-{END_MONTH:02d}",
            "start_capital":  START_CAPITAL,
            "fee_pct":        FEE_RATE,
            "slip_pct":       SLIP_RATE,
            "max_concurrent": MAX_CONCURRENT,
            "coins_listed":   len(COINS),
            "coins_loaded":   loaded,
        },
    }
    for v in VARIANTS:
        vname = v["name"]
        r     = all_results[vname]
        report[vname] = {
            "max_hold_bars": v["max_hold_bars"],
            "aggregate":     r["stats"],
            "filter_stats":  r["rej"],
            "per_coin":      r["coin_table"],
            "trades":        r["trades"],
        }

    with open("backtest_report.json", "w") as f:
        json.dump(report, f, indent=2, default=str)

    print("\nOutputs: backtest_summary.txt  backtest_report.json")
    print("Done ✓")

if __name__ == "__main__":
    main()

