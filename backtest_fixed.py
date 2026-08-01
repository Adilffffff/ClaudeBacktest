"""
Strategy G — Fixed Amount Backtest
144 coins, Jul 2024 – Jun 2026, 15m candles

Variant A: $1 risk per trade, no leverage, 3% TP / 15% SL
Variant B: $1 margin per trade, 5x leverage, 3% TP / 8% SL

Fixed amount = no compounding. Same $1 every single trade.
One position per coin at a time.
stdlib-only, 25 parallel workers for data fetch.
"""

import os, sys, json, csv, zipfile, io, math, time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.request import urlopen, Request
from urllib.error import HTTPError

# ─────────────────────────────────────────────────────────────────────
# SETTINGS
# ─────────────────────────────────────────────────────────────────────
START_YEAR, START_MONTH = 2024, 7
END_YEAR,   END_MONTH   = 2026, 6
INTERVAL    = "15m"
WARMUP_BARS = 60
MAX_WORKERS = 25
FEE_SIDE    = 0.0005   # 0.05%
SLIP_SIDE   = 0.0002   # 0.02%
TOTAL_COST  = (FEE_SIDE + SLIP_SIDE) * 2  # 0.14% round trip

VARIANTS = [
    {
        "name":      "A",
        "label":     "Variant A — 5x Leverage | TP 3% / SL 15%",
        "leverage":  5,
        "tp_pct":    0.03,
        "sl_pct":    0.15,
        "fixed_margin": 1.0,   # $1 margin posted per trade (isolated)
    },
    {
        "name":      "B",
        "label":     "Variant B — 5x Leverage | TP 3% / SL 8%",
        "leverage":  5,
        "tp_pct":    0.03,
        "sl_pct":    0.08,
        "fixed_margin": 1.0,   # $1 margin posted per trade (isolated)
    },
]

# 144-coin full universe from Strategy G
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

# ─────────────────────────────────────────────────────────────────────
# DATA FETCH
# ─────────────────────────────────────────────────────────────────────
BASE_URL = "https://data.binance.vision/data/futures/um/monthly/klines"

def fetch_month(symbol, year, month):
    ym  = f"{year}-{month:02d}"
    url = f"{BASE_URL}/{symbol}/{INTERVAL}/{symbol}-{INTERVAL}-{ym}.zip"
    try:
        req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urlopen(req, timeout=30) as r:
            data = r.read()
        with zipfile.ZipFile(io.BytesIO(data)) as z:
            with z.open(z.namelist()[0]) as f:
                rows = []
                for row in csv.reader(io.TextIOWrapper(f, "utf-8")):
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

def fetch_symbol(symbol):
    months = []
    for y in range(START_YEAR, END_YEAR + 1):
        sm = START_MONTH if y == START_YEAR else 1
        em = END_MONTH   if y == END_YEAR   else 12
        for m in range(sm, em + 1):
            months.append((y, m))
    all_bars = []
    for (y, m) in months:
        all_bars.extend(fetch_month(symbol, y, m))
    all_bars.sort(key=lambda x: x["t"])
    seen, out = set(), []
    for b in all_bars:
        if b["t"] not in seen:
            seen.add(b["t"])
            out.append(b)
    return out

# ─────────────────────────────────────────────────────────────────────
# INDICATORS
# ─────────────────────────────────────────────────────────────────────
def ema(closes, period):
    k  = 2 / (period + 1)
    e  = [0.0] * len(closes)
    e[0] = closes[0]
    for i in range(1, len(closes)):
        e[i] = closes[i] * k + e[i-1] * (1 - k)
    return e

def adx14(bars):
    n = len(bars)
    pdm = [0.0]*n; ndm = [0.0]*n; tr = [0.0]*n
    for i in range(1, n):
        up   = bars[i]["h"] - bars[i-1]["h"]
        dn   = bars[i-1]["l"] - bars[i]["l"]
        pdm[i] = up if up > dn and up > 0 else 0.0
        ndm[i] = dn if dn > up and dn > 0 else 0.0
        tr[i]  = max(bars[i]["h"]-bars[i]["l"],
                     abs(bars[i]["h"]-bars[i-1]["c"]),
                     abs(bars[i]["l"]-bars[i-1]["c"]))
    def wilder(arr, p=14):
        s = [0.0]*n
        if n < p+1: return s
        s[p] = sum(arr[1:p+1])
        for i in range(p+1, n):
            s[i] = s[i-1] - s[i-1]/p + arr[i]
        return s
    str_ = wilder(tr); spdm = wilder(pdm); sndm = wilder(ndm)
    dx = [0.0]*n
    for i in range(14, n):
        if str_[i] == 0: continue
        pdi = 100*spdm[i]/str_[i]; ndi = 100*sndm[i]/str_[i]
        d   = pdi+ndi
        if d: dx[i] = 100*abs(pdi-ndi)/d
    adx_ = [0.0]*n
    if n >= 28:
        adx_[27] = sum(dx[14:28])/14
        for i in range(28, n):
            adx_[i] = (adx_[i-1]*13 + dx[i])/14
    return adx_

# ─────────────────────────────────────────────────────────────────────
# PORTFOLIO BACKTEST — event-driven, fixed $ per trade
# ─────────────────────────────────────────────────────────────────────
def run_variant(all_bars_map, variant):
    lev       = variant["leverage"]
    tp_pct    = variant["tp_pct"]
    sl_pct    = variant["sl_pct"]
    margin    = variant["fixed_margin"]          # always $1
    notional  = margin * lev                     # leveraged notional
    fee_cost  = TOTAL_COST * notional            # fixed fee per trade

    # Pre-compute indicators
    sym_data = {}
    for sym, bars in all_bars_map.items():
        if len(bars) < WARMUP_BARS + 5:
            continue
        closes = [b["c"] for b in bars]
        sym_data[sym] = {
            "bars":   bars,
            "closes": closes,
            "highs":  [b["h"] for b in bars],
            "lows":   [b["l"] for b in bars],
            "times":  [b["t"] for b in bars],
            "ema9":   ema(closes, 9),
            "ema21":  ema(closes, 21),
            "ema50":  ema(closes, 50),
            "adx":    adx14(bars),
            "ts_map": {b["t"]: i for i, b in enumerate(bars)},
        }

    # All unique timestamps
    all_ts = sorted({b["t"] for sd in sym_data.values() for b in sd["bars"]})

    trades          = []
    open_trades     = {}       # sym -> trade dict
    pnl_running     = 0.0      # cumulative net pnl (no compounding)
    peak_pnl        = 0.0
    max_dd          = 0.0
    equity_snapshots = []      # (ts, cumulative_pnl)

    for ts in all_ts:
        equity_snapshots.append((ts, pnl_running))

        for sym, sd in sym_data.items():
            if ts not in sd["ts_map"]:
                continue
            i = sd["ts_map"][ts]
            if i < WARMUP_BARS:
                continue

            highs  = sd["highs"]
            lows   = sd["lows"]
            ema9_  = sd["ema9"]
            ema21_ = sd["ema21"]
            ema50_ = sd["ema50"]
            adx_   = sd["adx"]
            closes = sd["closes"]
            times  = sd["times"]

            # ── Exit check ──
            if sym in open_trades:
                ot = open_trades[sym]
                hit_tp = (ot["dir"] == "long"  and highs[i] >= ot["tp"]) or \
                         (ot["dir"] == "short" and lows[i]  <= ot["tp"])
                hit_sl = (ot["dir"] == "long"  and lows[i]  <= ot["sl"]) or \
                         (ot["dir"] == "short" and highs[i] >= ot["sl"])

                if hit_tp or hit_sl:
                    if hit_tp:
                        raw   = margin * lev * tp_pct
                        res   = "tp"
                    else:
                        raw   = -margin          # isolated: lose only margin
                        res   = "sl"

                    net = raw - fee_cost
                    pnl_running += net

                    # drawdown on cumulative pnl curve
                    if pnl_running > peak_pnl:
                        peak_pnl = pnl_running
                    dd = peak_pnl - pnl_running
                    if dd > max_dd:
                        max_dd = dd

                    ot.update({
                        "exit_ts":  ts,
                        "exit_bar": i,
                        "result":   res,
                        "raw_pnl":  raw,
                        "fee":      fee_cost,
                        "net_pnl":  net,
                        "duration": i - ot["entry_bar"],
                    })
                    trades.append(ot)
                    del open_trades[sym]

            # ── Entry check ──
            if sym in open_trades:
                continue

            if i < 10:
                continue

            # Filter 1: EMA50 slope
            slope = (ema50_[i] - ema50_[i-10]) / ema50_[i-10] * 100
            if abs(slope) < 0.05:
                continue
            direction = "long" if slope > 0 else "short"

            # Filter 2: EMA9/21 crossover
            cl  = ema9_[i] > ema21_[i] and ema9_[i-1] <= ema21_[i-1]
            cs  = ema9_[i] < ema21_[i] and ema9_[i-1] >= ema21_[i-1]
            if direction == "long"  and not cl: continue
            if direction == "short" and not cs: continue

            # Filter 3: ADX >= 22
            if adx_[i] < 22:
                continue

            ep = closes[i]
            if direction == "long":
                tp_p = ep * (1 + tp_pct)
                sl_p = ep * (1 - sl_pct)
            else:
                tp_p = ep * (1 - tp_pct)
                sl_p = ep * (1 + sl_pct)

            open_trades[sym] = {
                "symbol":    sym,
                "dir":       direction,
                "entry":     ep,
                "tp":        tp_p,
                "sl":        sl_p,
                "entry_bar": i,
                "entry_ts":  ts,
                "margin":    margin,
                "notional":  notional,
            }

    return trades, pnl_running, max_dd, equity_snapshots

# ─────────────────────────────────────────────────────────────────────
# STATS
# ─────────────────────────────────────────────────────────────────────
def calc_stats(trades, net_pnl, max_dd_abs, variant):
    n = len(trades)
    if n == 0:
        return {"variant": variant["name"], "trades": 0}

    wins  = [t for t in trades if t["result"] == "tp"]
    loses = [t for t in trades if t["result"] == "sl"]
    wn    = len(wins); ln = len(loses)
    wr    = wn / n * 100

    gw  = sum(t["net_pnl"] for t in wins)
    gl  = abs(sum(t["net_pnl"] for t in loses))
    pf  = gw / gl if gl > 0 else float("inf")
    exp = net_pnl / n

    avg_win  = gw / wn if wn else 0
    avg_loss = gl / ln if ln else 0

    # DD as % — relative to total $ at risk if all trades were open simultaneously
    # More meaningful: express as % of total gross win potential
    # Since fixed $1/trade, express max_dd in $ (absolute) and also as % of net_pnl
    max_dd_pct = (max_dd_abs / (max_dd_abs + max(net_pnl, 0.01))) * 100 if max_dd_abs > 0 else 0

    # Monthly pnl
    monthly = {}
    for t in trades:
        dt = datetime.utcfromtimestamp(t["entry_ts"] / 1000)
        k  = f"{dt.year}-{dt.month:02d}"
        monthly.setdefault(k, 0.0)
        monthly[k] += t["net_pnl"]
    mv = list(monthly.values())

    # Sharpe / Sortino (monthly)
    if len(mv) > 1:
        mean_m = sum(mv) / len(mv)
        std_m  = math.sqrt(sum((v-mean_m)**2 for v in mv) / len(mv))
        sharpe = (mean_m / std_m * math.sqrt(12)) if std_m > 0 else 0
        neg    = [v for v in mv if v < 0]
        std_d  = math.sqrt(sum(v**2 for v in neg) / len(neg)) if neg else 0
        sortino= (mean_m / std_d * math.sqrt(12)) if std_d > 0 else 0
    else:
        sharpe = sortino = 0.0

    longs  = [t for t in trades if t["dir"] == "long"]
    shorts = [t for t in trades if t["dir"] == "short"]
    lw = sum(1 for t in longs  if t["result"] == "tp")
    sw = sum(1 for t in shorts if t["result"] == "tp")

    bws = bls = cw = cl = 0
    for t in trades:
        if t["result"] == "tp": cw+=1; cl=0
        else:                   cl+=1; cw=0
        bws = max(bws, cw); bls = max(bls, cl)

    avg_dur = sum(t["duration"] for t in trades) / n

    # Per-coin
    per_coin = {}
    for t in trades:
        s = t["symbol"]
        per_coin.setdefault(s, {"trades":0,"wins":0,"net_pnl":0.0,"gw":0.0,"gl":0.0})
        per_coin[s]["trades"]  += 1
        per_coin[s]["net_pnl"] += t["net_pnl"]
        if t["result"] == "tp":
            per_coin[s]["wins"] += 1
            per_coin[s]["gw"]   += t["net_pnl"]
        else:
            per_coin[s]["gl"] += abs(t["net_pnl"])
    for s, d in per_coin.items():
        d["wr"] = d["wins"]/d["trades"]*100 if d["trades"] else 0
        d["pf"] = d["gw"]/d["gl"] if d["gl"] > 0 else float("inf")

    return {
        "variant":          variant["name"],
        "label":            variant["label"],
        "leverage":         variant["leverage"],
        "tp_pct":           variant["tp_pct"]*100,
        "sl_pct":           variant["sl_pct"]*100,
        "fixed_margin_usd": variant["fixed_margin"],
        "trades":           n,
        "wins":             wn,
        "losses":           ln,
        "win_rate":         round(wr, 2),
        "profit_factor":    round(pf, 4),
        "net_pnl":          round(net_pnl, 4),
        "max_dd_abs":       round(max_dd_abs, 4),
        "max_dd_pct":       round(max_dd_pct, 2),
        "sharpe":           round(sharpe, 3),
        "sortino":          round(sortino, 3),
        "avg_win":          round(avg_win, 4),
        "avg_loss":         round(avg_loss, 4),
        "expectancy":       round(exp, 4),
        "avg_dur_bars":     round(avg_dur, 1),
        "avg_dur_hrs":      round(avg_dur*15/60, 1),
        "long_trades":      len(longs),
        "long_wr":          round(lw/len(longs)*100,2) if longs else 0,
        "short_trades":     len(shorts),
        "short_wr":         round(sw/len(shorts)*100,2) if shorts else 0,
        "best_win_streak":  bws,
        "best_loss_streak": bls,
        "monthly_pnl":      dict(sorted(monthly.items())),
        "per_coin":         per_coin,
    }

# ─────────────────────────────────────────────────────────────────────
# REPORT WRITER
# ─────────────────────────────────────────────────────────────────────
def write_report(all_stats, loaded, failed):
    lines = []
    lines.append("=" * 72)
    lines.append("STRATEGY G — FIXED $1/TRADE BACKTEST")
    lines.append("Period  : Jul 2024 – Jun 2026 (24 months) | 15m candles")
    lines.append(f"Universe: 144 coins | Loaded: {loaded} | Failed: {len(failed)}")
    lines.append("Sizing  : Fixed $1 margin per trade (NO compounding)")
    lines.append("=" * 72)

    # Comparison header
    lines.append("\n── SIDE-BY-SIDE COMPARISON ──")
    lines.append(f"{'Metric':<28} {'Variant A':>20} {'Variant B':>20}")
    lines.append("-" * 70)

    def row(label, ka, kb, fmt=None):
        a = all_stats[0].get(ka, "N/A")
        b = all_stats[1].get(kb or ka, "N/A")
        if fmt and isinstance(a, float): a = fmt.format(a)
        if fmt and isinstance(b, float): b = fmt.format(b)
        lines.append(f"{label:<28} {str(a):>20} {str(b):>20}")

    a, B = all_stats[0], all_stats[1]
    lines.append(f"{'Config':<28} {'1x | TP3% SL15%':>20} {'5x | TP3% SL8%':>20}")
    lines.append(f"{'Margin per trade':<28} {'$1.00':>20} {'$1.00':>20}")
    lines.append(f"{'Total Trades':<28} {a['trades']:>20} {B['trades']:>20}")
    lines.append(f"{'Wins / Losses':<28} {str(a['wins'])+' / '+str(a['losses']):>20} {str(B['wins'])+' / '+str(B['losses']):>20}")
    lines.append(f"{'Win Rate':<28} {a['win_rate']:>19}% {B['win_rate']:>19}%")
    lines.append(f"{'Profit Factor':<28} {a['profit_factor']:>20} {B['profit_factor']:>20}")
    lines.append(f"{'Net PnL ($)':<28} ${a['net_pnl']:>19.4f} ${B['net_pnl']:>19.4f}")
    lines.append(f"{'Max DD ($)':<28} ${a['max_dd_abs']:>19.4f} ${B['max_dd_abs']:>19.4f}")
    lines.append(f"{'Max DD (%)':<28} {a['max_dd_pct']:>19}% {B['max_dd_pct']:>19}%")
    lines.append(f"{'Sharpe':<28} {a['sharpe']:>20} {B['sharpe']:>20}")
    lines.append(f"{'Sortino':<28} {a['sortino']:>20} {B['sortino']:>20}")
    lines.append(f"{'Avg Win ($)':<28} ${a['avg_win']:>19.4f} ${B['avg_win']:>19.4f}")
    lines.append(f"{'Avg Loss ($)':<28} ${a['avg_loss']:>19.4f} ${B['avg_loss']:>19.4f}")
    lines.append(f"{'Expectancy ($/trade)':<28} ${a['expectancy']:>19.4f} ${B['expectancy']:>19.4f}")
    lines.append(f"{'Avg Duration (hrs)':<28} {a['avg_dur_hrs']:>20} {B['avg_dur_hrs']:>20}")
    lines.append(f"{'Long Trades | WR':<28} {str(a['long_trades'])+' | '+str(a['long_wr'])+'%':>20} {str(B['long_trades'])+' | '+str(B['long_wr'])+'%':>20}")
    lines.append(f"{'Short Trades | WR':<28} {str(a['short_trades'])+' | '+str(a['short_wr'])+'%':>20} {str(B['short_trades'])+' | '+str(B['short_wr'])+'%':>20}")
    lines.append(f"{'Best Win Streak':<28} {a['best_win_streak']:>20} {B['best_win_streak']:>20}")
    lines.append(f"{'Best Loss Streak':<28} {a['best_loss_streak']:>20} {B['best_loss_streak']:>20}")

    # Per-variant detail
    for s in all_stats:
        lines.append(f"\n{'='*72}")
        lines.append(s["label"])
        lines.append(f"{'='*72}")
        if s["trades"] == 0:
            lines.append("  No trades generated.")
            continue

        lines.append(f"\nMonthly PnL:")
        for mo, pnl in s["monthly_pnl"].items():
            sign = "+" if pnl >= 0 else ""
            lines.append(f"  {mo}: ${sign}{pnl:.4f}")

        lines.append(f"\nPer-Coin Results — sorted by Profit Factor:")
        lines.append(f"  {'Symbol':<22} {'PF':>8} {'WR%':>7} {'Trades':>7} {'Net PnL':>12}")
        lines.append(f"  {'-'*22} {'-'*8} {'-'*7} {'-'*7} {'-'*12}")
        pc_sorted = sorted(s["per_coin"].items(), key=lambda x: x[1]["pf"], reverse=True)
        for sym, d in pc_sorted:
            pf_str = f"{d['pf']:.3f}" if d["pf"] != float("inf") else "inf"
            lines.append(f"  {sym:<22} {pf_str:>8} {d['wr']:>6.1f}% {d['trades']:>7} ${d['net_pnl']:>11.4f}")

    if failed:
        lines.append(f"\n── FAILED SYMBOLS ──")
        for f in failed:
            lines.append(f"  {f}")

    return "\n".join(lines)

# ─────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────
def main():
    print("=" * 72)
    print("STRATEGY G — FIXED $1/TRADE | 144 COINS | Jul 2024 – Jun 2026")
    print("Variant A: 1x, TP 3%, SL 15%")
    print("Variant B: 5x isolated, TP 3%, SL 8%")
    print("=" * 72)

    # Phase 1: Fetch data
    print(f"\n[Phase 1] Fetching {len(COINS)} coins with {MAX_WORKERS} workers...")
    t0 = time.time()
    all_bars_map = {}
    failed = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        fut_map = {ex.submit(fetch_symbol, sym): sym for sym in COINS}
        done = 0
        for fut in as_completed(fut_map):
            sym  = fut_map[fut]
            done += 1
            try:
                bars = fut.result()
                if bars:
                    all_bars_map[sym] = bars
                    print(f"  [{done:3d}/{len(COINS)}] {sym}: {len(bars)} bars")
                else:
                    failed.append(sym)
                    print(f"  [{done:3d}/{len(COINS)}] {sym}: NO DATA (404/missing)")
            except Exception as e:
                failed.append(sym)
                print(f"  [{done:3d}/{len(COINS)}] {sym}: ERROR {e}")

    print(f"\nFetch done in {time.time()-t0:.1f}s | Loaded: {len(all_bars_map)} | Failed: {len(failed)}")
    if not all_bars_map:
        print("ABORT: No data loaded.")
        sys.exit(1)

    # Phase 2: Run variants
    all_stats = []
    for variant in VARIANTS:
        print(f"\n[Phase 2] Running {variant['label']}...")
        t1 = time.time()
        trades, net_pnl, max_dd, eq_curve = run_variant(all_bars_map, variant)
        elapsed = time.time() - t1
        stats = calc_stats(trades, net_pnl, max_dd, variant)
        all_stats.append(stats)
        print(f"  Done in {elapsed:.1f}s | Trades: {stats['trades']} | "
              f"WR: {stats['win_rate']}% | PF: {stats['profit_factor']} | "
              f"Net PnL: ${stats['net_pnl']:.4f} | MaxDD: ${stats['max_dd_abs']:.4f}")

    # Phase 3: Write output
    print("\n[Phase 3] Writing reports...")
    report_text = write_report(all_stats, len(all_bars_map), failed)

    with open("backtest_fixed_summary.txt", "w") as f:
        f.write(report_text)

    report_json = {
        "meta": {
            "strategy": "G",
            "period": "Jul 2024 – Jun 2026",
            "coins_attempted": len(COINS),
            "coins_loaded": len(all_bars_map),
            "coins_failed": failed,
            "sizing": "fixed $1 margin per trade, no compounding",
        },
        "variants": all_stats,
    }
    with open("backtest_fixed_report.json", "w") as f:
        json.dump(report_json, f, indent=2)

    print("  Wrote backtest_fixed_summary.txt")
    print("  Wrote backtest_fixed_report.json")
    print("\n" + "=" * 72)
    print(report_text[:4000])

if __name__ == "__main__":
    main()
