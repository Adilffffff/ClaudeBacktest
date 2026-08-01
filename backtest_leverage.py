"""
Strategy G — Leverage Variants Backtest
Tests 2x through 10x isolated leverage on the 56-coin whitelist
Jul 2024 – Jun 2026, 15m candles
stdlib-only, parallel workers
"""

import os, sys, json, csv, zipfile, io, math, time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError

# ─────────────────────────────────────────────────────────────────────
# SETTINGS
# ─────────────────────────────────────────────────────────────────────
START_YEAR, START_MONTH = 2024, 7
END_YEAR,   END_MONTH   = 2026, 6
INTERVAL    = "15m"
STARTING_CAPITAL = 10_000.0
RISK_PCT    = 0.0075          # 0.75% per trade
TP_PCT      = 0.03            # 3%
SL_PCT      = 0.15            # 15%
FEE_SIDE    = 0.0005          # 0.05%
SLIP_SIDE   = 0.0002          # 0.02%
TOTAL_COST  = (FEE_SIDE + SLIP_SIDE) * 2   # 0.14%
WARMUP_BARS = 60
LEVERAGE_VARIANTS = [2, 3, 4, 5, 6, 7, 8, 9, 10]
MAX_WORKERS = 20              # parallel data-fetch workers

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

# ─────────────────────────────────────────────────────────────────────
# DATA FETCHING
# ─────────────────────────────────────────────────────────────────────
BASE_URL = "https://data.binance.vision/data/futures/um/monthly/klines"

def fetch_month(symbol, year, month):
    ym = f"{year}-{month:02d}"
    url = f"{BASE_URL}/{symbol}/{INTERVAL}/{symbol}-{INTERVAL}-{ym}.zip"
    try:
        req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urlopen(req, timeout=30) as r:
            data = r.read()
        with zipfile.ZipFile(io.BytesIO(data)) as z:
            name = z.namelist()[0]
            with z.open(name) as f:
                rows = []
                reader = csv.reader(io.TextIOWrapper(f, "utf-8"))
                for row in reader:
                    if not row or not row[0].isdigit():
                        continue
                    ts = int(row[0])
                    if ts > 10**14:
                        ts //= 1000
                    rows.append({
                        "t": ts,
                        "o": float(row[1]),
                        "h": float(row[2]),
                        "l": float(row[3]),
                        "c": float(row[4]),
                    })
                return rows
    except HTTPError as e:
        if e.code == 404:
            return []
        raise
    except Exception:
        return []

def fetch_symbol_data(symbol):
    months = []
    for y in range(START_YEAR, END_YEAR + 1):
        sm = START_MONTH if y == START_YEAR else 1
        em = END_MONTH   if y == END_YEAR   else 12
        for m in range(sm, em + 1):
            months.append((y, m))

    all_bars = []
    for (y, m) in months:
        bars = fetch_month(symbol, y, m)
        all_bars.extend(bars)

    all_bars.sort(key=lambda x: x["t"])
    # deduplicate
    seen, deduped = set(), []
    for b in all_bars:
        if b["t"] not in seen:
            seen.add(b["t"])
            deduped.append(b)
    return deduped

# ─────────────────────────────────────────────────────────────────────
# INDICATORS
# ─────────────────────────────────────────────────────────────────────
def calc_ema(closes, period):
    k = 2 / (period + 1)
    ema = [0.0] * len(closes)
    ema[0] = closes[0]
    for i in range(1, len(closes)):
        ema[i] = closes[i] * k + ema[i-1] * (1 - k)
    return ema

def calc_adx(bars, period=14):
    n = len(bars)
    plus_dm  = [0.0] * n
    minus_dm = [0.0] * n
    tr_list  = [0.0] * n

    for i in range(1, n):
        up   = bars[i]["h"] - bars[i-1]["h"]
        down = bars[i-1]["l"] - bars[i]["l"]
        plus_dm[i]  = up   if up > down and up > 0   else 0.0
        minus_dm[i] = down if down > up and down > 0 else 0.0
        tr_list[i]  = max(
            bars[i]["h"] - bars[i]["l"],
            abs(bars[i]["h"] - bars[i-1]["c"]),
            abs(bars[i]["l"] - bars[i-1]["c"]),
        )

    def wilder(arr):
        s = [0.0] * n
        if n < period + 1:
            return s
        s[period] = sum(arr[1:period+1])
        for i in range(period+1, n):
            s[i] = s[i-1] - s[i-1] / period + arr[i]
        return s

    s_tr  = wilder(tr_list)
    s_pdm = wilder(plus_dm)
    s_ndm = wilder(minus_dm)

    dx_arr = [0.0] * n
    for i in range(period, n):
        if s_tr[i] == 0:
            continue
        pdi = 100 * s_pdm[i] / s_tr[i]
        ndi = 100 * s_ndm[i] / s_tr[i]
        denom = pdi + ndi
        if denom == 0:
            continue
        dx_arr[i] = 100 * abs(pdi - ndi) / denom

    adx = [0.0] * n
    if n >= period * 2:
        adx[period*2-1] = sum(dx_arr[period:period*2]) / period
        for i in range(period*2, n):
            adx[i] = (adx[i-1] * (period-1) + dx_arr[i]) / period

    return adx

# ─────────────────────────────────────────────────────────────────────
# BACKTEST CORE — per symbol
# ─────────────────────────────────────────────────────────────────────
def backtest_symbol(symbol, bars, leverage, starting_equity):
    """
    Returns list of trade dicts. Equity is passed in (portfolio-level shared).
    For leverage variant: position size uses isolated margin.
    
    Isolated leverage means:
      - margin = risk_dollar / sl_pct   (same $ margin as before)
      - leveraged_notional = margin * leverage
      - actual contracts = leveraged_notional / entry_price
      - BUT max loss is still capped at margin (isolated = you only lose the margin posted)
      - So effective risk per trade stays risk_dollar (0.75% equity)
      - The DIFFERENCE is that TP hit earns leverage * normal_gain on the margin
    
    In isolated leverage:
      raw_pnl on TP = margin * leverage * tp_pct   (long profits amplified)
      raw_pnl on SL = -margin                       (you lose only the margin posted)
      
    So TP pnl   = margin * leverage * TP_PCT
       SL pnl   = -margin  (isolated, no liquidation beyond margin)
       Fees/slip = (FEE_SIDE+SLIP_SIDE)*2 * notional
                 = TOTAL_COST * margin * leverage
    """
    closes = [b["c"] for b in bars]
    highs  = [b["h"] for b in bars]
    lows   = [b["l"] for b in bars]
    times  = [b["t"] for b in bars]
    n = len(bars)

    if n < WARMUP_BARS + 5:
        return []

    ema9  = calc_ema(closes, 9)
    ema21 = calc_ema(closes, 21)
    ema50 = calc_ema(closes, 50)
    adx   = calc_adx(bars, 14)

    trades = []
    open_trade = None
    equity = starting_equity  # reference, actual equity maintained at portfolio level

    for i in range(WARMUP_BARS, n):
        # --- Check exit for open trade ---
        if open_trade is not None:
            entry  = open_trade["entry"]
            direct = open_trade["direction"]
            tp_p   = open_trade["tp"]
            sl_p   = open_trade["sl"]
            margin = open_trade["margin"]
            notional = open_trade["notional"]

            hit_tp = (direct == "long"  and highs[i] >= tp_p) or \
                     (direct == "short" and lows[i]  <= tp_p)
            hit_sl = (direct == "long"  and lows[i]  <= sl_p) or \
                     (direct == "short" and highs[i] >= sl_p)

            if hit_tp or hit_sl:
                if hit_tp:
                    raw_pnl = margin * leverage * TP_PCT
                    result  = "tp"
                else:
                    raw_pnl = -margin  # isolated: lose only margin
                    result  = "sl"

                fee_cost = TOTAL_COST * notional
                net_pnl  = raw_pnl - fee_cost
                duration = i - open_trade["entry_bar"]

                open_trade["exit_bar"]  = i
                open_trade["exit_ts"]   = times[i]
                open_trade["result"]    = result
                open_trade["raw_pnl"]   = raw_pnl
                open_trade["net_pnl"]   = net_pnl
                open_trade["duration"]  = duration
                open_trade["leverage"]  = leverage

                trades.append(open_trade)
                open_trade = None

        # --- Entry signal check ---
        if open_trade is not None:
            continue  # one trade per symbol at a time

        # Filter 1: EMA50 slope
        if i < 10:
            continue
        slope = (ema50[i] - ema50[i-10]) / ema50[i-10] * 100
        if abs(slope) < 0.05:
            continue
        direction = "long" if slope > 0 else "short"

        # Filter 2: EMA9/21 crossover
        cross_long  = ema9[i] > ema21[i] and ema9[i-1] <= ema21[i-1]
        cross_short = ema9[i] < ema21[i] and ema9[i-1] >= ema21[i-1]
        if direction == "long"  and not cross_long:  continue
        if direction == "short" and not cross_short: continue

        # Filter 3: ADX >= 22
        if adx[i] < 22:
            continue

        # Entry
        entry_price = closes[i]

        # Isolated margin sizing
        # risk_dollar = equity * RISK_PCT  — passed in via shared equity at call time
        # But in this per-symbol function we don't have live equity.
        # We'll store margin=None and resolve at portfolio level — actually
        # simpler: store the bar index + direction and let portfolio engine size it.
        # For per-symbol backtest (independent), assume starting_equity constant.
        risk_dollar = equity * RISK_PCT
        margin      = risk_dollar / SL_PCT   # isolated margin posted
        notional    = margin * leverage       # leveraged position size in $

        if direction == "long":
            tp_price = entry_price * (1 + TP_PCT)
            sl_price = entry_price * (1 - SL_PCT)
        else:
            tp_price = entry_price * (1 - TP_PCT)
            sl_price = entry_price * (1 + SL_PCT)

        open_trade = {
            "symbol":     symbol,
            "direction":  direction,
            "entry":      entry_price,
            "tp":         tp_price,
            "sl":         sl_price,
            "entry_bar":  i,
            "entry_ts":   times[i],
            "margin":     margin,
            "notional":   notional,
        }

    return trades

# ─────────────────────────────────────────────────────────────────────
# PORTFOLIO ENGINE — runs all symbols together with shared equity
# ─────────────────────────────────────────────────────────────────────
def portfolio_backtest(all_bars_map, leverage):
    """
    Event-driven portfolio backtest.
    all_bars_map: {symbol: [bars...]}
    Shared equity across all coins. Isolated margin per trade.
    """
    # Merge all events (entry signals + exit checks) by timestamp
    # Build per-symbol indicator arrays first
    sym_data = {}
    for sym, bars in all_bars_map.items():
        if len(bars) < WARMUP_BARS + 5:
            continue
        closes = [b["c"] for b in bars]
        ema9   = calc_ema(closes, 9)
        ema21  = calc_ema(closes, 21)
        ema50  = calc_ema(closes, 50)
        adx    = calc_adx(bars, 14)
        sym_data[sym] = {
            "bars": bars,
            "closes": closes,
            "highs":  [b["h"] for b in bars],
            "lows":   [b["l"] for b in bars],
            "times":  [b["t"] for b in bars],
            "ema9":   ema9,
            "ema21":  ema21,
            "ema50":  ema50,
            "adx":    adx,
            "open_trade": None,
            "bar_idx": 0,
        }

    # Collect all unique timestamps across all symbols
    all_ts = set()
    for sd in sym_data.values():
        for b in sd["bars"]:
            all_ts.add(b["t"])
    all_ts = sorted(all_ts)

    # Build per-symbol ts->bar_index map
    for sym, sd in sym_data.items():
        sd["ts_map"] = {b["t"]: i for i, b in enumerate(sd["bars"])}

    equity = STARTING_CAPITAL
    trades = []
    equity_curve = []  # (ts, equity)
    open_trades_by_sym = {}  # sym -> trade_dict

    for ts in all_ts:
        equity_curve.append((ts, equity))

        for sym, sd in sym_data.items():
            if ts not in sd["ts_map"]:
                continue
            i = sd["ts_map"][ts]
            if i < WARMUP_BARS:
                continue

            bars   = sd["bars"]
            highs  = sd["highs"]
            lows   = sd["lows"]
            ema9   = sd["ema9"]
            ema21  = sd["ema21"]
            ema50  = sd["ema50"]
            adx    = sd["adx"]
            closes = sd["closes"]
            times  = sd["times"]

            # Check exit
            if sym in open_trades_by_sym:
                ot = open_trades_by_sym[sym]
                hit_tp = (ot["direction"] == "long"  and highs[i] >= ot["tp"]) or \
                         (ot["direction"] == "short" and lows[i]  <= ot["tp"])
                hit_sl = (ot["direction"] == "long"  and lows[i]  <= ot["sl"]) or \
                         (ot["direction"] == "short" and highs[i] >= ot["sl"])

                if hit_tp or hit_sl:
                    margin   = ot["margin"]
                    notional = ot["notional"]
                    if hit_tp:
                        raw_pnl = margin * leverage * TP_PCT
                        result  = "tp"
                    else:
                        raw_pnl = -margin
                        result  = "sl"

                    fee_cost = TOTAL_COST * notional
                    net_pnl  = raw_pnl - fee_cost
                    equity  += net_pnl
                    if equity < 0:
                        equity = 0.0

                    ot.update({
                        "exit_ts": ts,
                        "exit_bar": i,
                        "result": result,
                        "raw_pnl": raw_pnl,
                        "fee_cost": fee_cost,
                        "net_pnl": net_pnl,
                        "duration": i - ot["entry_bar"],
                        "leverage": leverage,
                        "exit_equity": equity,
                    })
                    trades.append(ot)
                    del open_trades_by_sym[sym]

            # Check entry (only if no open trade for this symbol)
            if sym in open_trades_by_sym:
                continue

            # Filter 1: slope
            if i < 10:
                continue
            slope = (ema50[i] - ema50[i-10]) / ema50[i-10] * 100
            if abs(slope) < 0.05:
                continue
            direction = "long" if slope > 0 else "short"

            # Filter 2: crossover
            cross_long  = ema9[i] > ema21[i] and ema9[i-1] <= ema21[i-1]
            cross_short = ema9[i] < ema21[i] and ema9[i-1] >= ema21[i-1]
            if direction == "long"  and not cross_long:  continue
            if direction == "short" and not cross_short: continue

            # Filter 3: ADX
            if adx[i] < 22:
                continue

            # Size trade
            risk_dollar = equity * RISK_PCT
            if risk_dollar <= 0:
                continue
            margin   = risk_dollar / SL_PCT
            notional = margin * leverage

            entry_price = closes[i]
            if direction == "long":
                tp_price = entry_price * (1 + TP_PCT)
                sl_price = entry_price * (1 - SL_PCT)
            else:
                tp_price = entry_price * (1 - TP_PCT)
                sl_price = entry_price * (1 + SL_PCT)

            open_trades_by_sym[sym] = {
                "symbol":    sym,
                "direction": direction,
                "entry":     entry_price,
                "tp":        tp_price,
                "sl":        sl_price,
                "entry_bar": i,
                "entry_ts":  ts,
                "margin":    margin,
                "notional":  notional,
            }

    return trades, equity, equity_curve

# ─────────────────────────────────────────────────────────────────────
# STATS
# ─────────────────────────────────────────────────────────────────────
def calc_stats(trades, final_equity, equity_curve, leverage):
    n = len(trades)
    if n == 0:
        return {"leverage": leverage, "trades": 0}

    wins  = [t for t in trades if t["result"] == "tp"]
    loses = [t for t in trades if t["result"] == "sl"]
    win_n = len(wins)
    los_n = len(loses)
    wr    = win_n / n * 100

    gross_win  = sum(t["net_pnl"] for t in wins)
    gross_loss = abs(sum(t["net_pnl"] for t in loses))
    pf = gross_win / gross_loss if gross_loss > 0 else float("inf")
    net_pnl = sum(t["net_pnl"] for t in trades)

    avg_win  = gross_win  / win_n if win_n else 0
    avg_loss = gross_loss / los_n if los_n else 0
    expectancy = net_pnl / n

    # drawdown from equity curve
    peak, max_dd = STARTING_CAPITAL, 0.0
    for (ts, eq) in equity_curve:
        if eq > peak:
            peak = eq
        dd = (peak - eq) / peak * 100
        if dd > max_dd:
            max_dd = dd
    # also check final equity
    if final_equity < peak:
        dd = (peak - final_equity) / peak * 100
        if dd > max_dd:
            max_dd = dd

    # Sharpe / Sortino — monthly returns
    monthly = {}
    for t in trades:
        dt = datetime.utcfromtimestamp(t["entry_ts"] / 1000)
        k  = f"{dt.year}-{dt.month:02d}"
        monthly.setdefault(k, 0.0)
        monthly[k] += t["net_pnl"]
    m_vals = list(monthly.values())
    if len(m_vals) > 1:
        mean_m = sum(m_vals) / len(m_vals)
        std_m  = math.sqrt(sum((v - mean_m)**2 for v in m_vals) / len(m_vals))
        sharpe  = (mean_m / std_m * math.sqrt(12)) if std_m > 0 else 0
        neg     = [v for v in m_vals if v < 0]
        std_neg = math.sqrt(sum(v**2 for v in neg) / len(neg)) if neg else 0
        sortino = (mean_m / std_neg * math.sqrt(12)) if std_neg > 0 else 0
    else:
        sharpe = sortino = 0.0

    # long/short split
    longs  = [t for t in trades if t["direction"] == "long"]
    shorts = [t for t in trades if t["direction"] == "short"]
    lw = sum(1 for t in longs  if t["result"] == "tp")
    sw = sum(1 for t in shorts if t["result"] == "tp")

    # streaks
    best_win_streak = best_los_streak = 0
    cur_w = cur_l = 0
    for t in trades:
        if t["result"] == "tp":
            cur_w += 1; cur_l = 0
        else:
            cur_l += 1; cur_w = 0
        best_win_streak = max(best_win_streak, cur_w)
        best_los_streak = max(best_los_streak, cur_l)

    avg_dur = sum(t["duration"] for t in trades) / n

    # per-coin
    per_coin = {}
    for t in trades:
        s = t["symbol"]
        per_coin.setdefault(s, {"trades":0,"wins":0,"net_pnl":0.0,"gross_win":0.0,"gross_loss":0.0})
        per_coin[s]["trades"]  += 1
        per_coin[s]["net_pnl"] += t["net_pnl"]
        if t["result"] == "tp":
            per_coin[s]["wins"]      += 1
            per_coin[s]["gross_win"] += t["net_pnl"]
        else:
            per_coin[s]["gross_loss"] += abs(t["net_pnl"])
    for s, d in per_coin.items():
        d["wr"] = d["wins"] / d["trades"] * 100 if d["trades"] else 0
        d["pf"] = d["gross_win"] / d["gross_loss"] if d["gross_loss"] > 0 else float("inf")

    # monthly pnl
    monthly_pnl = dict(sorted(monthly.items()))

    return {
        "leverage":        leverage,
        "trades":          n,
        "wins":            win_n,
        "losses":          los_n,
        "win_rate":        round(wr, 2),
        "profit_factor":   round(pf, 4),
        "net_pnl":         round(net_pnl, 2),
        "final_equity":    round(final_equity, 2),
        "max_drawdown_pct":round(max_dd, 2),
        "sharpe":          round(sharpe, 3),
        "sortino":         round(sortino, 3),
        "avg_win":         round(avg_win, 2),
        "avg_loss":        round(avg_loss, 2),
        "expectancy":      round(expectancy, 2),
        "avg_duration_bars":round(avg_dur, 1),
        "avg_duration_hrs": round(avg_dur * 15 / 60, 1),
        "long_trades":     len(longs),
        "long_wr":         round(lw/len(longs)*100,2) if longs else 0,
        "short_trades":    len(shorts),
        "short_wr":        round(sw/len(shorts)*100,2) if shorts else 0,
        "best_win_streak": best_win_streak,
        "best_loss_streak":best_los_streak,
        "monthly_pnl":     monthly_pnl,
        "per_coin":        per_coin,
    }

# ─────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────
def main():
    print("=" * 70)
    print("STRATEGY G — LEVERAGE VARIANTS BACKTEST (2x–10x Isolated)")
    print(f"Coins: {len(COINS)} | Period: Jul 2024 – Jun 2026 | 15m candles")
    print("=" * 70)

    # ── Phase 1: Fetch all data in parallel ──────────────────────────
    print(f"\n[Phase 1] Fetching data for {len(COINS)} coins using {MAX_WORKERS} workers...")
    t0 = time.time()
    all_bars_map = {}
    failed = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        fut_map = {ex.submit(fetch_symbol_data, sym): sym for sym in COINS}
        done = 0
        for fut in as_completed(fut_map):
            sym = fut_map[fut]
            done += 1
            try:
                bars = fut.result()
                if bars:
                    all_bars_map[sym] = bars
                    print(f"  [{done:3d}/{len(COINS)}] {sym}: {len(bars)} bars")
                else:
                    failed.append(sym)
                    print(f"  [{done:3d}/{len(COINS)}] {sym}: NO DATA")
            except Exception as e:
                failed.append(sym)
                print(f"  [{done:3d}/{len(COINS)}] {sym}: ERROR {e}")

    print(f"\nData fetch done in {time.time()-t0:.1f}s")
    print(f"Loaded: {len(all_bars_map)} coins | Failed: {len(failed)}")
    if len(all_bars_map) == 0:
        print("ABORT: All symbols failed — data source blocked or unavailable.")
        sys.exit(1)

    # ── Phase 2: Run backtest for each leverage ───────────────────────
    print(f"\n[Phase 2] Running {len(LEVERAGE_VARIANTS)} leverage variants...")
    all_results = []

    for lev in LEVERAGE_VARIANTS:
        print(f"\n  ── {lev}x Leverage ──")
        t1 = time.time()
        trades, final_eq, eq_curve = portfolio_backtest(all_bars_map, lev)
        elapsed = time.time() - t1
        stats = calc_stats(trades, final_eq, eq_curve, lev)
        all_results.append(stats)
        print(f"    Trades: {stats['trades']} | WR: {stats['win_rate']}% | "
              f"PF: {stats['profit_factor']} | Net PnL: ${stats['net_pnl']:,.2f} | "
              f"MaxDD: {stats['max_drawdown_pct']}% | Done in {elapsed:.1f}s")

    # ── Phase 3: Write outputs ────────────────────────────────────────
    print("\n[Phase 3] Writing reports...")

    # Summary text
    lines = []
    lines.append("=" * 70)
    lines.append("STRATEGY G — LEVERAGE VARIANTS BACKTEST RESULTS")
    lines.append(f"Period  : Jul 2024 – Jun 2026 (24 months)")
    lines.append(f"Coins   : {len(all_bars_map)} loaded ({len(failed)} failed)")
    lines.append(f"Capital : ${STARTING_CAPITAL:,.0f} | Risk/trade: {RISK_PCT*100:.2f}%")
    lines.append(f"TP: {TP_PCT*100:.1f}% | SL: {SL_PCT*100:.1f}% | Fees+Slip: {TOTAL_COST*100:.2f}% RT")
    lines.append(f"Margin type: ISOLATED (loss capped at margin)")
    lines.append("=" * 70)

    # Comparison table
    lines.append("\n── AGGREGATE COMPARISON TABLE ──")
    hdr = f"{'Lev':>4} {'Trades':>7} {'WR%':>7} {'PF':>7} {'Net PnL':>14} {'FinalEq':>14} {'MaxDD%':>8} {'Sharpe':>8} {'Sortino':>8}"
    lines.append(hdr)
    lines.append("-" * len(hdr))
    for s in all_results:
        lines.append(
            f"{s['leverage']:>3}x "
            f"{s['trades']:>7} "
            f"{s['win_rate']:>6.2f}% "
            f"{s['profit_factor']:>7.4f} "
            f"${s['net_pnl']:>13,.2f} "
            f"${s['final_equity']:>13,.2f} "
            f"{s['max_drawdown_pct']:>7.2f}% "
            f"{s['sharpe']:>8.3f} "
            f"{s['sortino']:>8.3f}"
        )

    # Per-leverage detail
    for s in all_results:
        lev = s["leverage"]
        lines.append(f"\n{'='*70}")
        lines.append(f"LEVERAGE: {lev}x ISOLATED")
        lines.append(f"{'='*70}")
        if s["trades"] == 0:
            lines.append("  No trades.")
            continue
        lines.append(f"Total Trades    : {s['trades']}")
        lines.append(f"Wins / Losses   : {s['wins']} / {s['losses']}")
        lines.append(f"Win Rate        : {s['win_rate']}%")
        lines.append(f"Profit Factor   : {s['profit_factor']}")
        lines.append(f"Net PnL         : ${s['net_pnl']:,.2f}")
        lines.append(f"Final Equity    : ${s['final_equity']:,.2f}")
        lines.append(f"Starting Capital: ${STARTING_CAPITAL:,.2f}")
        lines.append(f"Max Drawdown    : {s['max_drawdown_pct']}%")
        lines.append(f"Sharpe          : {s['sharpe']}")
        lines.append(f"Sortino         : {s['sortino']}")
        lines.append(f"Avg Win         : ${s['avg_win']:,.2f}")
        lines.append(f"Avg Loss        : ${s['avg_loss']:,.2f}")
        lines.append(f"Expectancy      : ${s['expectancy']:,.2f} per trade")
        lines.append(f"Avg Duration    : {s['avg_duration_bars']} bars ({s['avg_duration_hrs']} hrs)")
        lines.append(f"Long  Trades    : {s['long_trades']}  |  WR {s['long_wr']}%")
        lines.append(f"Short Trades    : {s['short_trades']}  |  WR {s['short_wr']}%")
        lines.append(f"Best Win Streak : {s['best_win_streak']}")
        lines.append(f"Best Loss Streak: {s['best_loss_streak']}")

        lines.append(f"\nMonthly PnL ({lev}x):")
        for mo, pnl in s["monthly_pnl"].items():
            sign = "+" if pnl >= 0 else ""
            lines.append(f"  {mo}: ${sign}{pnl:,.2f}")

        lines.append(f"\nPer-Coin Results ({lev}x) — sorted by PF:")
        pc_sorted = sorted(s["per_coin"].items(), key=lambda x: x[1]["pf"], reverse=True)
        lines.append(f"  {'Symbol':<22} {'PF':>7} {'WR%':>7} {'Trades':>7} {'Net PnL':>14}")
        lines.append(f"  {'-'*22} {'-'*7} {'-'*7} {'-'*7} {'-'*14}")
        for sym, d in pc_sorted:
            pf_str = f"{d['pf']:.3f}" if d['pf'] != float('inf') else "∞"
            lines.append(f"  {sym:<22} {pf_str:>7} {d['wr']:>6.1f}% {d['trades']:>7} ${d['net_pnl']:>13,.2f}")

    if failed:
        lines.append(f"\n── FAILED SYMBOLS (no data) ──")
        for f in failed:
            lines.append(f"  {f}")

    summary_text = "\n".join(lines)

    with open("backtest_leverage_summary.txt", "w") as f:
        f.write(summary_text)
    print("  Wrote backtest_leverage_summary.txt")

    # JSON report
    report = {
        "meta": {
            "strategy": "G",
            "period": "Jul 2024 – Jun 2026",
            "coins_loaded": len(all_bars_map),
            "coins_failed": failed,
            "starting_capital": STARTING_CAPITAL,
            "risk_pct": RISK_PCT,
            "tp_pct": TP_PCT,
            "sl_pct": SL_PCT,
            "fee_side": FEE_SIDE,
            "slip_side": SLIP_SIDE,
            "margin_type": "isolated",
            "leverage_variants": LEVERAGE_VARIANTS,
        },
        "results": all_results,
    }
    with open("backtest_leverage_report.json", "w") as f:
        json.dump(report, f, indent=2)
    print("  Wrote backtest_leverage_report.json")

    print("\n" + "=" * 70)
    print("DONE.")
    print(summary_text[:3000])  # print first part to stdout for GH Actions log

if __name__ == "__main__":
    main()
