"""
Strategy G — EMA Crossover + EMA50 Slope + ADX(14) — 15m candles
THREE VARIANTS (all 5x leverage, isolated margin):
  BASE : TP 3.0%, SL 15.0%
  VAR_A: TP 1.5%, SL 10.0%
  VAR_B: TP 2.0%, SL 18.0%

Isolated margin means each position's max loss = margin posted for that trade.
Liquidation occurs when price moves against position by ~(1/leverage) = 20%.
At 5x, liquidation price ≈ entry ± 20% (before fees). Since SL is tighter
than liq distance in all variants, SL always fires first — no liq risk here.

Coin universe: 56-coin whitelist from Variant G v11.0
Period: Jul 2024 – Jun 2026 (24 months)
Capital: $10,000 shared, compounding 0.75% risk per trade
Max concurrent: 6 positions
Fees: 0.05%/side, Slippage: 0.02%/side
"""

import urllib.request
import zipfile
import csv
import io
import json
import os
import math
from datetime import datetime, timezone
from concurrent.futures import ProcessPoolExecutor, as_completed

# ── COIN UNIVERSE ──────────────────────────────────────────────────────────────
COINS = [
    "1000000BOBUSDT","1000CATUSDT","1000RATSUSDT","A2ZUSDT","AIOTUSDT",
    "ALGOUSDT","ALPINEUSDT","ASTERUSDT","AUSDT","BASEDUSDT","BELUSDT",
    "BIDUSDT","BMTUSDT","BTRUSDT","CFXUSDT","CHIPUSDT","CRCLUSDT","DAMUSDT",
    "DEXEUSDT","DIAUSDT","EPTUSDT","ETHUSDT","FLNCUSDT","FUNUSDT","GLMUSDT",
    "GUAUSDT","ICXUSDT","IOUSDT","LIGHTUSDT","MOODENGUSDT","NFPUSDT",
    "NMRUSDT","NOTUSDT","ORBSUSDT","PEOPLEUSDT","PIPPINUSDT","POWERUSDT",
    "POWRUSDT","RAVEUSDT","RESOLVUSDT","RVVUSDT","SEIUSDT","SIGNUSDT",
    "SKRUSDT","SNDKUSDT","SOMIUSDT","SPELLUSDT","TRUTHUSDT","TURBOUSDT",
    "VANRYUSDT","VINEUSDT","VVVUSDT","XEMUSDT","XRPUSDT","ZECUSDT",
    "ZEREBROUSDT",
]

INTERVAL   = "15m"
START_YEAR, START_MONTH = 2024, 7
END_YEAR,   END_MONTH   = 2026, 6

# ── STRATEGY PARAMETERS ────────────────────────────────────────────────────────
LEVERAGE       = 5
RISK_PCT       = 0.0075   # 0.75% of equity per trade
FEE_RATE       = 0.0005   # 0.05% per side
SLIP_RATE      = 0.0002   # 0.02% per side
ROUND_TRIP     = (FEE_RATE + SLIP_RATE) * 2  # total cost

MAX_CONCURRENT = 6
START_CAPITAL  = 10_000.0

WARMUP_BARS    = 60

EMA_FAST   = 9
EMA_SLOW   = 21
EMA_TREND  = 50
SLOPE_BARS = 10
SLOPE_MIN  = 0.0005   # 0.05%
ADX_PERIOD = 14
ADX_MIN    = 22.0

# Variants: (label, tp_pct, sl_pct)
VARIANTS = [
    ("BASE",  0.030, 0.150),
    ("VAR_A", 0.015, 0.100),
    ("VAR_B", 0.020, 0.180),
]

# ── DATA FETCH ─────────────────────────────────────────────────────────────────
BASE_URL = "https://data.binance.vision/data/futures/um/monthly/klines"

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
                name = z.namelist()[0]
                with z.open(name) as f:
                    reader = csv.reader(io.TextIOWrapper(f))
                    for row in reader:
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
            pass  # 404 = symbol not listed yet that month
    rows.sort(key=lambda r: r["ts"])
    # deduplicate
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
    plus_dm  = [0.0] * n
    minus_dm = [0.0] * n
    tr_arr   = [0.0] * n

    for i in range(1, n):
        up   = bars[i]["high"] - bars[i-1]["high"]
        down = bars[i-1]["low"] - bars[i]["low"]
        plus_dm[i]  = up   if up > down and up > 0   else 0.0
        minus_dm[i] = down if down > up and down > 0 else 0.0
        tr_arr[i]   = max(
            bars[i]["high"] - bars[i]["low"],
            abs(bars[i]["high"] - bars[i-1]["close"]),
            abs(bars[i]["low"]  - bars[i-1]["close"]),
        )

    # Wilder smooth
    def wilder(arr, p):
        s = [0.0] * n
        s[p] = sum(arr[1:p+1])
        for i in range(p+1, n):
            s[i] = s[i-1] - s[i-1]/p + arr[i]
        return s

    sp_tr  = wilder(tr_arr,   period)
    sp_pdm = wilder(plus_dm,  period)
    sp_ndm = wilder(minus_dm, period)

    dx = [0.0] * n
    for i in range(period, n):
        if sp_tr[i] == 0:
            continue
        pdi = 100 * sp_pdm[i] / sp_tr[i]
        ndi = 100 * sp_ndm[i] / sp_tr[i]
        denom = pdi + ndi
        if denom:
            dx[i] = 100 * abs(pdi - ndi) / denom

    adx = wilder(dx, period)
    return adx

# ── PER-SYMBOL BACKTEST ────────────────────────────────────────────────────────
def backtest_symbol(args):
    symbol, bars = args
    if len(bars) < WARMUP_BARS + 5:
        return symbol, []

    closes = [b["close"] for b in bars]
    ema9  = calc_ema(closes, EMA_FAST)
    ema21 = calc_ema(closes, EMA_SLOW)
    ema50 = calc_ema(closes, EMA_TREND)
    adx   = calc_adx(bars, ADX_PERIOD)

    trades = []  # list of dicts with variant label

    # open positions per variant: {variant_label: trade_dict or None}
    open_pos = {v[0]: None for v in VARIANTS}

    for i in range(WARMUP_BARS, len(bars)):
        bar = bars[i]

        for vlabel, tp_pct, sl_pct in VARIANTS:
            pos = open_pos[vlabel]

            # ── CHECK EXITS ──
            if pos is not None:
                ep    = pos["entry"]
                side  = pos["side"]
                tp    = pos["tp"]
                sl    = pos["sl"]
                hit_tp = hit_sl = False
                if side == "LONG":
                    if bar["high"] >= tp:  hit_tp = True
                    elif bar["low"] <= sl: hit_sl = True
                else:
                    if bar["low"] <= tp:   hit_tp = True
                    elif bar["high"] >= sl: hit_sl = True

                if hit_tp or hit_sl:
                    exit_price = tp if hit_tp else sl
                    raw_ret = (exit_price - ep) / ep if side == "LONG" else (ep - exit_price) / ep
                    # apply leverage to raw price move
                    lev_ret = raw_ret * LEVERAGE
                    # cost = round-trip fees on notional
                    cost = ROUND_TRIP
                    net_pct = lev_ret - cost
                    pnl = pos["risk_dollar"] / (sl_pct * LEVERAGE) * notional_size_factor(pos, sl_pct) * net_pct
                    # simpler: risk_dollar is amount at risk per 1x move of sl_pct
                    # position notional = risk_dollar / sl_pct (at 1x)
                    # with leverage, margin = notional / leverage
                    # pnl = notional * net_pct (leverage already in net_pct via lev_ret)
                    notional = pos["risk_dollar"] / sl_pct
                    pnl = notional * net_pct

                    # isolated: max loss = margin = notional / leverage
                    margin = notional / LEVERAGE
                    # cap loss at margin (liquidation floor)
                    if pnl < -margin:
                        pnl = -margin

                    trades.append({
                        "variant":    vlabel,
                        "symbol":     symbol,
                        "side":       side,
                        "entry_ts":   pos["entry_ts"],
                        "exit_ts":    bar["ts"],
                        "entry":      ep,
                        "exit":       exit_price,
                        "pnl":        pnl,
                        "win":        pnl > 0,
                        "duration":   i - pos["entry_bar"],
                        "tp_hit":     hit_tp,
                    })
                    open_pos[vlabel] = None
                    pos = None

            # ── CHECK ENTRIES ──
            if pos is not None:
                continue  # already in trade

            # Indicators
            if adx[i] is None or adx[i-1] is None:
                continue

            # Filter 1: EMA50 slope
            if i < SLOPE_BARS:
                continue
            slope = (ema50[i] - ema50[i-SLOPE_BARS]) / ema50[i-SLOPE_BARS]

            long_ok  = slope >  SLOPE_MIN
            short_ok = slope < -SLOPE_MIN
            if not long_ok and not short_ok:
                continue

            # Filter 2: EMA9/21 crossover
            cross_long  = ema9[i] > ema21[i] and ema9[i-1] <= ema21[i-1]
            cross_short = ema9[i] < ema21[i] and ema9[i-1] >= ema21[i-1]

            if cross_long and long_ok:
                direction = "LONG"
            elif cross_short and short_ok:
                direction = "SHORT"
            else:
                continue

            # Filter 3: ADX
            if adx[i] < ADX_MIN:
                continue

            # Signal passes — entry at close
            entry_price = bar["close"]
            if direction == "LONG":
                tp_price = entry_price * (1 + tp_pct)
                sl_price = entry_price * (1 - sl_pct)
            else:
                tp_price = entry_price * (1 - tp_pct)
                sl_price = entry_price * (1 + sl_pct)

            open_pos[vlabel] = {
                "entry":      entry_price,
                "entry_ts":   bar["ts"],
                "entry_bar":  i,
                "side":       direction,
                "tp":         tp_price,
                "sl":         sl_price,
                "tp_pct":     tp_pct,
                "sl_pct":     sl_pct,
                "risk_dollar": None,  # set by portfolio manager
            }

    return symbol, trades

def notional_size_factor(pos, sl_pct):
    return 1.0  # placeholder, actual calc in portfolio runner

# ── PORTFOLIO RUNNER ───────────────────────────────────────────────────────────
def run_portfolio(all_symbol_bars):
    """
    Event-driven portfolio simulation. Processes all bars chronologically
    across all symbols, managing shared equity and max 6 concurrent positions.
    """
    # Build unified timeline
    all_events = []  # (ts, symbol, bar_index)
    for symbol, bars in all_symbol_bars.items():
        for i, bar in enumerate(bars):
            all_events.append((bar["ts"], symbol, i))
    all_events.sort()

    # Per-variant state
    variant_states = {}
    for vlabel, tp_pct, sl_pct in VARIANTS:
        variant_states[vlabel] = {
            "equity":   START_CAPITAL,
            "open":     {},   # symbol -> position dict
            "trades":   [],
            "tp_pct":   tp_pct,
            "sl_pct":   sl_pct,
        }

    # Precompute indicators per symbol
    print("Computing indicators...")
    sym_indicators = {}
    for symbol, bars in all_symbol_bars.items():
        if len(bars) < WARMUP_BARS + 5:
            continue
        closes = [b["close"] for b in bars]
        sym_indicators[symbol] = {
            "ema9":  calc_ema(closes, EMA_FAST),
            "ema21": calc_ema(closes, EMA_SLOW),
            "ema50": calc_ema(closes, EMA_TREND),
            "adx":   calc_adx(bars, ADX_PERIOD),
        }

    print(f"Processing {len(all_events):,} bar events...")
    processed = 0
    for ts, symbol, i in all_events:
        processed += 1
        if processed % 500_000 == 0:
            print(f"  {processed:,}/{len(all_events):,} events...")

        if symbol not in sym_indicators:
            continue
        if i < WARMUP_BARS:
            continue

        inds  = sym_indicators[symbol]
        bars  = all_symbol_bars[symbol]
        bar   = bars[i]

        ema9  = inds["ema9"]
        ema21 = inds["ema21"]
        ema50 = inds["ema50"]
        adx   = inds["adx"]

        for vlabel, tp_pct, sl_pct in VARIANTS:
            state = variant_states[vlabel]
            equity = state["equity"]
            open_pos = state["open"]

            # ── EXIT CHECK ──
            if symbol in open_pos:
                pos = open_pos[symbol]
                ep   = pos["entry"]
                side = pos["side"]
                tp   = pos["tp"]
                sl   = pos["sl"]
                notional = pos["notional"]
                margin   = pos["margin"]
                hit_tp = hit_sl = False

                if side == "LONG":
                    if bar["high"] >= tp:  hit_tp = True
                    elif bar["low"] <= sl: hit_sl = True
                else:
                    if bar["low"] <= tp:   hit_tp = True
                    elif bar["high"] >= sl: hit_sl = True

                if hit_tp or hit_sl:
                    exit_price = tp if hit_tp else sl
                    raw_ret = (exit_price - ep) / ep if side == "LONG" else (ep - exit_price) / ep
                    lev_ret = raw_ret * LEVERAGE
                    net_pct = lev_ret - ROUND_TRIP
                    pnl = notional * net_pct
                    pnl = max(pnl, -margin)  # isolated: floor at margin loss

                    state["equity"] = equity + pnl
                    state["trades"].append({
                        "variant":   vlabel,
                        "symbol":    symbol,
                        "side":      side,
                        "entry_ts":  pos["entry_ts"],
                        "exit_ts":   ts,
                        "pnl":       pnl,
                        "win":       pnl > 0,
                        "duration":  i - pos["entry_bar"],
                        "tp_hit":    hit_tp,
                    })
                    del open_pos[symbol]

            # ── ENTRY CHECK ──
            if symbol in open_pos:
                continue
            if len(open_pos) >= MAX_CONCURRENT:
                continue

            # Slope filter
            if i < SLOPE_BARS:
                continue
            slope = (ema50[i] - ema50[i-SLOPE_BARS]) / ema50[i-SLOPE_BARS]
            long_ok  = slope >  SLOPE_MIN
            short_ok = slope < -SLOPE_MIN
            if not long_ok and not short_ok:
                continue

            # Crossover
            cross_long  = ema9[i] > ema21[i] and ema9[i-1] <= ema21[i-1]
            cross_short = ema9[i] < ema21[i] and ema9[i-1] >= ema21[i-1]
            if cross_long and long_ok:
                direction = "LONG"
            elif cross_short and short_ok:
                direction = "SHORT"
            else:
                continue

            # ADX
            if adx[i] < ADX_MIN:
                continue

            # Position sizing
            eq = state["equity"]
            risk_dollar = eq * RISK_PCT
            notional    = risk_dollar / sl_pct  # 1x notional
            margin      = notional / LEVERAGE   # capital locked

            if eq < margin:
                continue  # insufficient capital

            entry_price = bar["close"]
            if direction == "LONG":
                tp_price = entry_price * (1 + tp_pct)
                sl_price = entry_price * (1 - sl_pct)
            else:
                tp_price = entry_price * (1 - tp_pct)
                sl_price = entry_price * (1 + sl_pct)

            open_pos[symbol] = {
                "entry":      entry_price,
                "entry_ts":   ts,
                "entry_bar":  i,
                "side":       direction,
                "tp":         tp_price,
                "sl":         sl_price,
                "notional":   notional,
                "margin":     margin,
            }

    return variant_states

# ── STATISTICS ─────────────────────────────────────────────────────────────────
def compute_stats(trades, variant_label, start_cap):
    if not trades:
        return None

    wins   = [t for t in trades if t["win"]]
    losses = [t for t in trades if not t["win"]]
    n      = len(trades)
    nw     = len(wins)
    nl     = len(losses)

    avg_win  = sum(t["pnl"] for t in wins)  / nw if nw else 0
    avg_loss = sum(t["pnl"] for t in losses)/ nl if nl else 0

    gross_win  = sum(t["pnl"] for t in wins)
    gross_loss = abs(sum(t["pnl"] for t in losses))
    pf = gross_win / gross_loss if gross_loss else float("inf")

    net_pnl = sum(t["pnl"] for t in trades)
    wr = nw / n if n else 0
    exp = net_pnl / n if n else 0
    avg_dur = sum(t["duration"] for t in trades) / n if n else 0

    # Equity curve for drawdown/Sharpe
    eq = start_cap
    equity_curve = [eq]
    for t in trades:
        eq += t["pnl"]
        equity_curve.append(eq)

    peak = start_cap
    max_dd = 0.0
    for v in equity_curve:
        peak = max(peak, v)
        dd = (peak - v) / peak
        max_dd = max(max_dd, dd)

    # Monthly PnL
    monthly = {}
    for t in trades:
        dt = datetime.fromtimestamp(t["exit_ts"] / 1000, tz=timezone.utc)
        key = f"{dt.year}-{dt.month:02d}"
        monthly[key] = monthly.get(key, 0) + t["pnl"]

    # Sharpe / Sortino (daily returns)
    daily = {}
    for t in trades:
        dt = datetime.fromtimestamp(t["exit_ts"] / 1000, tz=timezone.utc)
        key = (dt.year, dt.month, dt.day)
        daily[key] = daily.get(key, 0) + t["pnl"]
    days = list(daily.values())
    if len(days) > 1:
        mean_d = sum(days) / len(days)
        var_d  = sum((d - mean_d)**2 for d in days) / len(days)
        std_d  = math.sqrt(var_d)
        sharpe = (mean_d / std_d * math.sqrt(365)) if std_d else 0
        neg    = [d for d in days if d < 0]
        down_v = sum(d**2 for d in neg) / len(days)
        sortino = (mean_d / math.sqrt(down_v) * math.sqrt(365)) if down_v else 0
    else:
        sharpe = sortino = 0

    # Streaks
    best_win_streak = best_loss_streak = cur_w = cur_l = 0
    for t in trades:
        if t["win"]:
            cur_w += 1; cur_l = 0
        else:
            cur_l += 1; cur_w = 0
        best_win_streak  = max(best_win_streak,  cur_w)
        best_loss_streak = max(best_loss_streak, cur_l)

    longs  = [t for t in trades if t["side"] == "LONG"]
    shorts = [t for t in trades if t["side"] == "SHORT"]
    lwr = sum(1 for t in longs  if t["win"]) / len(longs)  if longs  else 0
    swr = sum(1 for t in shorts if t["win"]) / len(shorts) if shorts else 0

    return {
        "variant":          variant_label,
        "total_trades":     n,
        "wins":             nw,
        "losses":           nl,
        "win_rate":         wr,
        "profit_factor":    pf,
        "net_pnl":          net_pnl,
        "final_equity":     start_cap + net_pnl,
        "max_drawdown":     max_dd,
        "sharpe":           sharpe,
        "sortino":          sortino,
        "avg_win":          avg_win,
        "avg_loss":         avg_loss,
        "expectancy":       exp,
        "avg_duration":     avg_dur,
        "long_trades":      len(longs),
        "long_wr":          lwr,
        "short_trades":     len(shorts),
        "short_wr":         swr,
        "best_win_streak":  best_win_streak,
        "best_loss_streak": best_loss_streak,
        "monthly_pnl":      dict(sorted(monthly.items())),
    }

def per_coin_stats(trades, vlabel):
    by_sym = {}
    for t in trades:
        by_sym.setdefault(t["symbol"], []).append(t)
    rows = []
    for sym, strades in by_sym.items():
        w = sum(1 for t in strades if t["win"])
        l = len(strades) - w
        gw = sum(t["pnl"] for t in strades if t["win"])
        gl = abs(sum(t["pnl"] for t in strades if not t["win"]))
        pf = gw / gl if gl else float("inf")
        rows.append({
            "symbol": sym, "trades": len(strades),
            "wins": w, "losses": l,
            "win_rate": w/len(strades),
            "profit_factor": pf,
            "net_pnl": sum(t["pnl"] for t in strades),
        })
    rows.sort(key=lambda r: -r["profit_factor"])
    return rows

# ── REPORTING ──────────────────────────────────────────────────────────────────
def print_summary(stats, coin_table, variant_cfg):
    vlabel, tp_pct, sl_pct = variant_cfg
    print(f"\n{'='*64}")
    print(f"VARIANT: {vlabel}  |  5x Isolated  |  TP {tp_pct*100:.1f}%  SL {sl_pct*100:.1f}%")
    print(f"{'='*64}")
    if stats is None:
        print("  No trades executed.")
        return

    pf_pass = stats["profit_factor"] >= 1.5
    wr_pass = stats["win_rate"] >= 0.42
    verdict = "✅ USABLE" if (pf_pass and wr_pass) else "❌ DOES NOT MEET TARGETS"

    print(f"Total Trades    : {stats['total_trades']:,}")
    print(f"Wins / Losses   : {stats['wins']:,} / {stats['losses']:,}")
    print(f"Win Rate        : {stats['win_rate']*100:.2f}%")
    print(f"Profit Factor   : {stats['profit_factor']:.4f}")
    print(f"Net PnL         : ${stats['net_pnl']:,.2f}")
    print(f"Final Equity    : ${stats['final_equity']:,.2f}")
    print(f"Starting Capital: ${START_CAPITAL:,.2f}")
    print(f"Max Drawdown    : {stats['max_drawdown']*100:.2f}%")
    print(f"Sharpe          : {stats['sharpe']:.3f}")
    print(f"Sortino         : {stats['sortino']:.3f}")
    print(f"Avg Win         : ${stats['avg_win']:,.2f}")
    print(f"Avg Loss        : ${stats['avg_loss']:,.2f}")
    print(f"Expectancy      : ${stats['expectancy']:,.2f} per trade")
    print(f"Avg Duration    : {stats['avg_duration']:.1f} bars ({stats['avg_duration']*15/60:.1f} hours)")
    print(f"Long Trades     : {stats['long_trades']:,}  |  WR {stats['long_wr']*100:.2f}%")
    print(f"Short Trades    : {stats['short_trades']:,}  |  WR {stats['short_wr']*100:.2f}%")
    print(f"Best Win Streak : {stats['best_win_streak']}")
    print(f"Best Loss Streak: {stats['best_loss_streak']}")
    print(f"\nLeverage Notes  : 5x Isolated — Liq at ~20% adverse move")
    print(f"  BASE  SL 15% < 20% liq distance ✓")
    print(f"  VAR_A SL 10% < 20% liq distance ✓")
    print(f"  VAR_B SL 18% < 20% liq distance ✓")
    print(f"\nVERDICT: {verdict}")

    print(f"\nMonthly PnL:")
    for ym, pnl in stats["monthly_pnl"].items():
        sign = "+" if pnl >= 0 else ""
        print(f"  {ym}: ${sign}{pnl:,.2f}")

    print(f"\nPer-Coin Table (sorted by PF):")
    print(f"  {'Symbol':<24} {'PF':>6}  {'WR':>7}  {'Trades':>7}  {'Net PnL':>12}")
    for row in coin_table:
        pf_str = f"{row['profit_factor']:.3f}" if row["profit_factor"] != float("inf") else "  INF"
        print(f"  {row['symbol']:<24} {pf_str:>6}  {row['win_rate']*100:>6.1f}%  {row['trades']:>7,}  ${row['net_pnl']:>11,.2f}")

# ── MAIN ───────────────────────────────────────────────────────────────────────
def main():
    print("Strategy G — 5x Isolated Leverage Backtest")
    print(f"Period: {START_YEAR}-{START_MONTH:02d} to {END_YEAR}-{END_MONTH:02d}")
    print(f"Universe: {len(COINS)} coins")
    print(f"Variants: {[v[0] for v in VARIANTS]}")
    print()

    # Phase 1: Download data
    print(f"Phase 1: Downloading {len(COINS)} symbols...")
    all_bars = {}
    failed   = 0

    with ProcessPoolExecutor(max_workers=50) as ex:
        futures = {ex.submit(fetch_symbol, sym): sym for sym in COINS}
        for fut in as_completed(futures):
            sym = futures[fut]
            try:
                bars = fut.result()
                if bars:
                    all_bars[sym] = bars
                    print(f"  ✓ {sym}: {len(bars):,} bars")
                else:
                    print(f"  ✗ {sym}: no data")
                    failed += 1
            except Exception as e:
                print(f"  ✗ {sym}: {e}")
                failed += 1

    loaded = len(all_bars)
    print(f"\nLoaded {loaded}/{len(COINS)} symbols ({failed} failed/no data)")

    if loaded == 0:
        print("FATAL: No data loaded — check network / data source.")
        return

    # Phase 2: Portfolio simulation
    print("\nPhase 2: Portfolio simulation (all variants)...")
    variant_states = run_portfolio(all_bars)

    # Phase 3: Stats & reporting
    summary_lines = []
    all_stats = {}
    report = {"meta": {
        "strategy": "Strategy G",
        "leverage": LEVERAGE,
        "margin_type": "isolated",
        "period": f"{START_YEAR}-{START_MONTH:02d} to {END_YEAR}-{END_MONTH:02d}",
        "coins": COINS,
        "fee_pct": FEE_RATE,
        "slip_pct": SLIP_RATE,
        "risk_pct": RISK_PCT,
        "max_concurrent": MAX_CONCURRENT,
        "start_capital": START_CAPITAL,
    }, "variants": {}}

    for vlabel, tp_pct, sl_pct in VARIANTS:
        state  = variant_states[vlabel]
        trades = state["trades"]
        stats  = compute_stats(trades, vlabel, START_CAPITAL)
        coin_t = per_coin_stats(trades, vlabel)
        print_summary(stats, coin_t, (vlabel, tp_pct, sl_pct))
        all_stats[vlabel] = stats

        report["variants"][vlabel] = {
            "config": {"tp_pct": tp_pct, "sl_pct": sl_pct, "leverage": LEVERAGE},
            "aggregate": stats,
            "per_coin": coin_t,
            "trades": trades,
        }

    # Write summary text
    summary_path = "backtest_summary.txt"
    with open(summary_path, "w") as f:
        f.write("Strategy G — 5x Isolated Leverage — Backtest Summary\n")
        f.write(f"Period: {START_YEAR}-{START_MONTH:02d} to {END_YEAR}-{END_MONTH:02d}\n")
        f.write(f"Universe: {len(COINS)} coins | Loaded: {loaded}\n\n")
        for vlabel, tp_pct, sl_pct in VARIANTS:
            stats = all_stats[vlabel]
            f.write(f"{'='*60}\n")
            f.write(f"VARIANT {vlabel}: TP {tp_pct*100:.1f}% / SL {sl_pct*100:.1f}% / 5x Isolated\n")
            f.write(f"{'='*60}\n")
            if stats:
                pf_pass = stats["profit_factor"] >= 1.5
                wr_pass = stats["win_rate"] >= 0.42
                verdict = "USABLE" if (pf_pass and wr_pass) else "DOES NOT MEET TARGETS"
                f.write(f"Trades: {stats['total_trades']:,} | WR: {stats['win_rate']*100:.2f}% | PF: {stats['profit_factor']:.4f}\n")
                f.write(f"Net PnL: ${stats['net_pnl']:,.2f} | Final Equity: ${stats['final_equity']:,.2f}\n")
                f.write(f"Max DD: {stats['max_drawdown']*100:.2f}% | Sharpe: {stats['sharpe']:.3f} | Sortino: {stats['sortino']:.3f}\n")
                f.write(f"Expectancy: ${stats['expectancy']:,.2f}/trade | Avg Duration: {stats['avg_duration']:.1f} bars\n")
                f.write(f"VERDICT: {verdict}\n\n")
                f.write("Monthly PnL:\n")
                for ym, pnl in stats["monthly_pnl"].items():
                    sign = "+" if pnl >= 0 else ""
                    f.write(f"  {ym}: ${sign}{pnl:,.2f}\n")
                f.write("\n")
            else:
                f.write("  No trades.\n\n")

    # Write JSON report
    report_path = "backtest_report.json"
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2, default=str)

    print(f"\nOutputs written: {summary_path}, {report_path}")
    print("\nDone ✓")

if __name__ == "__main__":
    main()
