"""
Strategy G — VAR_D Stress Test Suite
=====================================
Timeframe  : 15m candles
Entry      : EMA50 slope filter + EMA9/21 crossover + ADX(14) >= 22
Exit       : TP 3.0% / SL 15.0% (VAR_D from fable5 brief, updated TP to 3%)
Hold cap   : 10 days (960 bars on 15m) — same symbol cannot re-enter while open
Sizing     : 0.75% of current equity compounding (risk_dollar = equity * 0.0075)
             position_size = risk_dollar / (entry_price * 0.15)
Leverage   : 5x (notional = position_size * entry_price; reported in output)
             margin_used = notional / LEVERAGE
Fees       : 0.05% per side
Slippage   : varies by test (normal 0.02%, stress 0.05%)
Universe   : 144 coins (v12 list from fable5 brief)
Period     : Jul 2024 – Jun 2026  (24 months in-sample)

5 TEST VARIANTS
---------------
TEST_1  BASELINE         full period Jul24–Jun26, slip=0.02%, no concurrent cap
TEST_2  WALK_FORWARD     out-of-sample Jan26–Jun26, slip=0.02%   (6 months)
TEST_3  SLIP_STRESS      full period, slip=0.05% (2.5× normal)
TEST_4  BEAR_REGIME      Jul24–Mar25 (9 months bear/chop), slip=0.02%
TEST_5  LIQUIDATION      full period, slip=0.02%, forced liquidation at -90% equity

What "same symbol cannot re-enter while open" means:
  - Each symbol tracks its own open position independently
  - While sym is in open_pos, no new entry for that sym
  - This is the correct fix for the "long hold period" issue — the position IS
    tracked as open for the full duration, blocking that symbol from re-entry

Leverage reporting:
  - Margin used per trade = (position_size * entry_price) / LEVERAGE
  - Notional = position_size * entry_price
  - PnL = (raw_return * position_size * entry_price) - round_trip_cost_in_dollars
    where round_trip_cost = notional * ROUND_TRIP_COST_RATE
  - Capped at -margin (can't lose more than margin per trade)
  
ADX: exact GMaxV1.py logic — O(n) single-pass array, period*3 guard,
     ws() Wilder smoothing, SMA seed -> Wilder ADX.

Data: data.binance.vision futures monthly archives — no API key, geo-block free.
"""

import urllib.request, zipfile, csv, io, json, math, multiprocessing
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

# ── COIN UNIVERSE (144 coins from fable5 brief) ─────────────────────────────
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

# ── TEST DEFINITIONS ─────────────────────────────────────────────────────────
TESTS = [
    {
        "name":        "TEST_1_BASELINE",
        "label":       "Baseline (full period, normal slip)",
        "start":       (2024, 7),
        "end":         (2026, 6),
        "slip_rate":   0.0002,
        "liq_test":    False,
        "pass_pf":     1.5,
        "pass_wr":     0.42,
    },
    {
        "name":        "TEST_2_WALK_FORWARD",
        "label":       "Walk-Forward OOS (Jan–Jun 2026)",
        "start":       (2026, 1),
        "end":         (2026, 6),
        "slip_rate":   0.0002,
        "liq_test":    False,
        "pass_pf":     1.2,
        "pass_wr":     0.42,
    },
    {
        "name":        "TEST_3_SLIP_STRESS",
        "label":       "Slippage Stress (full period, 0.05% slip)",
        "start":       (2024, 7),
        "end":         (2026, 6),
        "slip_rate":   0.0005,
        "liq_test":    False,
        "pass_pf":     1.2,
        "pass_wr":     0.42,
    },
    {
        "name":        "TEST_4_BEAR_REGIME",
        "label":       "Bear Regime (Jul 2024 – Mar 2025)",
        "start":       (2024, 7),
        "end":         (2025, 3),
        "slip_rate":   0.0002,
        "liq_test":    False,
        "pass_pf":     1.0,
        "pass_wr":     0.42,
    },
    {
        "name":        "TEST_5_LIQUIDATION",
        "label":       "Liquidation Stress (forced liq at -90% equity)",
        "start":       (2024, 7),
        "end":         (2026, 6),
        "slip_rate":   0.0002,
        "liq_test":    True,
        "pass_pf":     1.0,
        "pass_wr":     0.42,
    },
]

# ── STRATEGY PARAMS ──────────────────────────────────────────────────────────
INTERVAL        = "15m"
LEVERAGE        = 5
START_CAPITAL   = 10_000.0
RISK_PCT        = 0.0075          # 0.75% of equity per trade
FEE_RATE        = 0.0005          # 0.05% per side
MAX_HOLD_BARS   = 960             # 10 days on 15m
WARMUP_BARS     = 70

TP_PCT          = 0.030           # 3.0%
SL_PCT          = 0.150           # 15.0%

EMA_FAST        = 9
EMA_SLOW        = 21
EMA_TREND       = 50
SLOPE_BARS      = 10
ADX_PERIOD      = 14
ADX_MIN         = 22.0
SLOPE_MIN       = 0.05            # % threshold for slope direction

LIQ_THRESHOLD  = 0.10            # liquidate all positions if equity < 10% of start

BASE_URL = "https://data.binance.vision/data/futures/um/monthly/klines"

# ── DATA ─────────────────────────────────────────────────────────────────────
def month_range(sy, sm, ey, em):
    y, m = sy, sm
    while (y, m) <= (ey, em):
        yield y, m
        m += 1
        if m > 12:
            m = 1; y += 1

def fetch_symbol(args):
    symbol, sy, sm, ey, em = args
    rows = []
    for y, m in month_range(sy, sm, ey, em):
        url = f"{BASE_URL}/{symbol}/{INTERVAL}/{symbol}-{INTERVAL}-{y}-{m:02d}.zip"
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=30) as resp:
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

# ── INDICATORS ───────────────────────────────────────────────────────────────
def calc_ema(values, period):
    k = 2.0 / (period + 1)
    ema = [values[0]]
    for v in values[1:]:
        ema.append(v * k + ema[-1] * (1 - k))
    return ema

def calc_adx_array(bars, period=14):
    """
    O(n) ADX. Exact GMaxV1.py match:
      ws() Wilder smoother (running-sum form)
      SMA seed -> Wilder for final ADX
      period*3 guard before trusting values
    """
    n = len(bars)
    if n < period * 3:
        return [0.0] * n

    highs  = [b["high"]  for b in bars]
    lows   = [b["low"]   for b in bars]
    closes = [b["close"] for b in bars]

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

    def ws(arr, p):
        if len(arr) < p:
            return []
        r = [sum(arr[:p])]
        for x in arr[p:]:
            r.append(r[-1] - r[-1] / p + x)
        return r

    st = ws(tr_raw,  period)
    sp = ws(pdm_raw, period)
    sm = ws(mdm_raw, period)

    if not st:
        return [0.0] * n

    pdi_arr = [100.0 * p / t if t else 0.0 for p, t in zip(sp, st)]
    mdi_arr = [100.0 * m / t if t else 0.0 for m, t in zip(sm, st)]
    dx_arr  = [100.0 * abs(p - m) / (p + m) if (p + m) else 0.0
               for p, m in zip(pdi_arr, mdi_arr)]

    if len(dx_arr) < period:
        return [0.0] * n

    adx_val = sum(dx_arr[:period]) / period
    adx_rolling = [adx_val]
    for d in dx_arr[period:]:
        adx_val = (adx_val * (period - 1) + d) / period
        adx_rolling.append(adx_val)

    result = [0.0] * n
    guard_k = period
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

# ── SIMULATION ───────────────────────────────────────────────────────────────
def run_test(test_cfg, sym_inds):
    tname       = test_cfg["name"]
    slip_rate   = test_cfg["slip_rate"]
    liq_test    = test_cfg["liq_test"]
    ROUND_TRIP  = (FEE_RATE + slip_rate) * 2

    # Build global event list: (ts, sym, bar_index)
    events = []
    for sym, inds in sym_inds.items():
        for i, bar in enumerate(inds["bars"]):
            events.append((bar["ts"], sym, i))
    events.sort()

    equity    = START_CAPITAL
    open_pos  = {}    # sym -> position dict
    closed    = []
    liquidated = False

    rej = {
        "warmup":       0,
        "sym_open":     0,
        "slope":        0,
        "no_cross":     0,
        "adx":          0,
        "insuff_cap":   0,
        "executed":     0,
    }

    progress_step = max(1, len(events) // 20)

    for step, (ts, sym, i) in enumerate(events):
        if step % progress_step == 0:
            print(f"  [{tname}] {step/len(events)*100:.0f}%  "
                  f"eq=${equity:,.0f}  open={len(open_pos)}  trades={len(closed)}", flush=True)

        # ── LIQUIDATION CHECK ────────────────────────────────────────────────
        if liq_test and not liquidated:
            if equity < START_CAPITAL * LIQ_THRESHOLD:
                print(f"  [{tname}] *** LIQUIDATION TRIGGERED at ${equity:,.2f} ***", flush=True)
                # Force-close ALL open positions at current bar's close
                for lsym, lpos in list(open_pos.items()):
                    linds = sym_inds[lsym]
                    # find bar index closest to current ts
                    lbars = linds["bars"]
                    li    = lpos["entry_bar"]
                    # use latest available bar for this symbol
                    while li + 1 < len(lbars) and lbars[li+1]["ts"] <= ts:
                        li += 1
                    lbar   = lbars[li]
                    exit_px = lbar["close"]
                    ep      = lpos["entry"]
                    side    = lpos["side"]
                    sz      = lpos["size"]
                    notional = sz * ep
                    raw_ret = (exit_px - ep) / ep if side == "LONG" else (ep - exit_px) / ep
                    pnl     = raw_ret * notional - notional * ROUND_TRIP
                    margin  = notional / LEVERAGE
                    pnl     = max(pnl, -margin)
                    equity += pnl
                    closed.append({
                        "symbol": lsym, "side": side,
                        "entry_ts": lpos["entry_ts"], "exit_ts": ts,
                        "entry_px": ep, "exit_px": exit_px,
                        "size": sz, "notional": notional, "margin": margin,
                        "pnl": pnl, "win": pnl > 0, "duration": li - lpos["entry_bar"],
                        "tp_hit": False, "timed_out": False, "forced": True, "liquidated": True,
                    })
                open_pos.clear()
                liquidated = True
                break   # stop trading after liquidation

        inds = sym_inds[sym]
        bars = inds["bars"]
        bar  = bars[i]

        # ── EXIT CHECK ───────────────────────────────────────────────────────
        if sym in open_pos:
            pos    = open_pos[sym]
            side   = pos["side"]
            tp     = pos["tp"]
            sl     = pos["sl"]
            sz     = pos["size"]
            ep     = pos["entry"]
            notional = sz * ep
            margin   = notional / LEVERAGE
            held   = i - pos["entry_bar"]

            hit_tp = hit_sl = timed_out = False

            if MAX_HOLD_BARS is not None and held >= MAX_HOLD_BARS:
                timed_out = True
            elif side == "LONG":
                if bar["high"] >= tp:  hit_tp = True
                elif bar["low"]  <= sl: hit_sl = True
            else:
                if bar["low"]  <= tp:  hit_tp = True
                elif bar["high"] >= sl: hit_sl = True

            if hit_tp or hit_sl or timed_out:
                exit_px  = bar["close"] if timed_out else (tp if hit_tp else sl)
                raw_ret  = (exit_px - ep) / ep if side == "LONG" else (ep - exit_px) / ep
                pnl      = raw_ret * notional - notional * ROUND_TRIP
                pnl      = max(pnl, -margin)
                equity  += pnl
                closed.append({
                    "symbol": sym, "side": side,
                    "entry_ts": pos["entry_ts"], "exit_ts": ts,
                    "entry_px": ep, "exit_px": exit_px,
                    "size": sz, "notional": notional, "margin": margin,
                    "pnl": pnl, "win": pnl > 0, "duration": held,
                    "tp_hit": hit_tp, "timed_out": timed_out, "forced": False, "liquidated": False,
                })
                del open_pos[sym]

            if sym in open_pos:
                continue   # still in position — skip entry

        # ── WARMUP GUARD ─────────────────────────────────────────────────────
        if i < WARMUP_BARS:
            rej["warmup"] += 1
            continue

        # ── ENTRY BLOCKED: sym already open ──────────────────────────────────
        if sym in open_pos:
            rej["sym_open"] += 1
            continue

        # ── FILTER 1: EMA50 SLOPE ────────────────────────────────────────────
        ema50     = inds["ema50"]
        if i < SLOPE_BARS:
            rej["slope"] += 1
            continue
        slope_pct = (ema50[i] - ema50[i - SLOPE_BARS]) / ema50[i - SLOPE_BARS] * 100
        trend_up   = slope_pct >  SLOPE_MIN
        trend_down = slope_pct < -SLOPE_MIN
        if not trend_up and not trend_down:
            rej["slope"] += 1
            continue

        # ── FILTER 2: EMA9/21 CROSSOVER ──────────────────────────────────────
        ema9  = inds["ema9"]
        ema21 = inds["ema21"]
        cross_long  = ema9[i] > ema21[i] and ema9[i-1] <= ema21[i-1]
        cross_short = ema9[i] < ema21[i] and ema9[i-1] >= ema21[i-1]

        if   cross_long  and trend_up:   direction = "LONG"
        elif cross_short and trend_down: direction = "SHORT"
        else:
            rej["no_cross"] += 1
            continue

        # ── FILTER 3: ADX ────────────────────────────────────────────────────
        if inds["adx"][i] < ADX_MIN:
            rej["adx"] += 1
            continue

        # ── CAPITAL CHECK ─────────────────────────────────────────────────────
        entry_px   = bar["close"]
        risk_dollar = equity * RISK_PCT
        pos_size   = risk_dollar / (entry_px * SL_PCT)  # contracts
        notional   = pos_size * entry_px
        margin     = notional / LEVERAGE
        if equity < margin:
            rej["insuff_cap"] += 1
            continue

        # ── OPEN POSITION ─────────────────────────────────────────────────────
        tp_px = entry_px * (1 + TP_PCT) if direction == "LONG" else entry_px * (1 - TP_PCT)
        sl_px = entry_px * (1 - SL_PCT) if direction == "LONG" else entry_px * (1 + SL_PCT)

        open_pos[sym] = {
            "entry":     entry_px,
            "entry_ts":  ts,
            "entry_bar": i,
            "side":      direction,
            "tp":        tp_px,
            "sl":        sl_px,
            "size":      pos_size,
        }
        rej["executed"] += 1

    # ── FORCE-CLOSE remaining open positions at end of data ──────────────────
    for sym, pos in open_pos.items():
        bars    = sym_inds[sym]["bars"]
        last    = bars[-1]
        exit_px = last["close"]
        ep      = pos["entry"]
        side    = pos["side"]
        sz      = pos["size"]
        notional = sz * ep
        margin   = notional / LEVERAGE
        held    = len(bars) - 1 - pos["entry_bar"]
        raw_ret = (exit_px - ep) / ep if side == "LONG" else (ep - exit_px) / ep
        pnl     = raw_ret * notional - notional * ROUND_TRIP
        pnl     = max(pnl, -margin)
        equity += pnl
        closed.append({
            "symbol": sym, "side": side,
            "entry_ts": pos["entry_ts"], "exit_ts": last["ts"],
            "entry_px": ep, "exit_px": exit_px,
            "size": sz, "notional": notional, "margin": margin,
            "pnl": pnl, "win": pnl > 0, "duration": held,
            "tp_hit": False, "timed_out": False, "forced": True, "liquidated": False,
        })

    print(f"  [{tname}] DONE  eq=${equity:,.2f}  trades={len(closed)}", flush=True)
    return closed, equity, rej

# ── STATS ─────────────────────────────────────────────────────────────────────
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

    ts_sorted = sorted(trades, key=lambda t: t["exit_ts"])
    eq = start_cap; curve = [eq]
    for t in ts_sorted:
        eq += t["pnl"]; curve.append(eq)
    peak = start_cap; mdd = 0.0
    for v in curve:
        peak = max(peak, v); mdd = max(mdd, (peak - v) / peak if peak else 0)

    monthly = {}
    for t in ts_sorted:
        dt = datetime.fromtimestamp(t["exit_ts"] / 1000, tz=timezone.utc)
        k  = f"{dt.year}-{dt.month:02d}"
        monthly[k] = monthly.get(k, 0) + t["pnl"]

    daily = {}
    for t in ts_sorted:
        dt = datetime.fromtimestamp(t["exit_ts"] / 1000, tz=timezone.utc)
        k  = (dt.year, dt.month, dt.day)
        daily[k] = daily.get(k, 0) + t["pnl"]
    days = list(daily.values())
    sharpe = sortino = 0.0
    if len(days) > 1:
        mu  = sum(days) / len(days)
        std = math.sqrt(sum((d - mu) ** 2 for d in days) / len(days))
        if std:    sharpe  = mu / std * math.sqrt(365)
        ns = sum(d ** 2 for d in days if d < 0) / len(days)
        if ns:     sortino = mu / math.sqrt(ns) * math.sqrt(365)

    dur = [t["duration"] for t in trades]
    bws = bloss = cw = cl = 0
    for t in ts_sorted:
        if t["win"]: cw += 1; cl = 0
        else:        cl += 1; cw = 0
        bws   = max(bws, cw)
        bloss = max(bloss, cl)

    lo = [t for t in trades if t["side"] == "LONG"]
    sh = [t for t in trades if t["side"] == "SHORT"]

    avg_notional = sum(t["notional"] for t in trades) / n
    avg_margin   = sum(t["margin"]   for t in trades) / n

    return {
        "total_trades":        n,
        "wins":                nw,
        "losses":              nl,
        "forced_closed":       sum(1 for t in trades if t.get("forced")),
        "timed_out":           sum(1 for t in trades if t.get("timed_out")),
        "liquidated_event":    any(t.get("liquidated") for t in trades),
        "win_rate":            wr,
        "profit_factor":       pf,
        "net_pnl":             final_eq - start_cap,
        "final_equity":        final_eq,
        "max_drawdown":        mdd,
        "sharpe":              sharpe,
        "sortino":             sortino,
        "avg_win":             gw / nw if nw else 0,
        "avg_loss":            -gl / nl if nl else 0,
        "expectancy":          exp,
        "avg_duration_bars":   sum(dur) / n,
        "max_duration_bars":   max(dur),
        "avg_notional":        avg_notional,
        "avg_margin":          avg_margin,
        "leverage_used":       LEVERAGE,
        "long_trades":         len(lo),
        "long_wr":             sum(1 for t in lo if t["win"]) / len(lo) if lo else 0,
        "short_trades":        len(sh),
        "short_wr":            sum(1 for t in sh if t["win"]) / len(sh) if sh else 0,
        "best_win_streak":     bws,
        "best_loss_streak":    bloss,
        "monthly_pnl":         dict(sorted(monthly.items())),
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
        rows.append({
            "symbol":      sym,
            "trades":      len(st),
            "wins":        w,
            "losses":      l,
            "win_rate":    w / len(st),
            "profit_factor": gw / gl if gl else float("inf"),
            "net_pnl":     sum(t["pnl"] for t in st),
        })
    rows.sort(key=lambda r: -r["profit_factor"])
    return rows

# ── SUMMARY WRITER ────────────────────────────────────────────────────────────
def write_summary(test_cfg, stats, coin_table, rej, loaded):
    tname = test_cfg["name"]
    label = test_cfg["label"]
    sy, sm = test_cfg["start"]
    ey, em = test_cfg["end"]
    slip_r = test_cfg["slip_rate"]

    a = []; w = a.append
    w("=" * 72)
    w(f"STRATEGY G — VAR_D  |  TP {TP_PCT*100:.1f}%  SL {SL_PCT*100:.1f}%  {LEVERAGE}x  [15m]")
    w(f"TEST    : {tname}")
    w(f"Label   : {label}")
    w(f"Period  : {sy}-{sm:02d} to {ey}-{em:02d}")
    w(f"Slip    : {slip_r*100:.3f}% per side  |  Fee: {FEE_RATE*100:.3f}% per side")
    w(f"Capital : ${START_CAPITAL:,.0f}  Risk: {RISK_PCT*100:.2f}%/trade  Leverage: {LEVERAGE}x")
    w(f"Hold cap: {MAX_HOLD_BARS} bars (10 days)  |  Universe: {len(COINS)} coins  |  Loaded: {loaded}")
    w(f"Liq test: {'YES — forced close at <10% equity' if test_cfg['liq_test'] else 'NO'}")
    w("=" * 72)

    if stats is None:
        w("NO TRADES EXECUTED")
        return "\n".join(a)

    ok = stats["profit_factor"] >= test_cfg["pass_pf"] and stats["win_rate"] >= test_cfg["pass_wr"]
    verdict = f"PASS (PF≥{test_cfg['pass_pf']} WR≥{int(test_cfg['pass_wr']*100)}%)" if ok else "FAIL"

    if stats.get("liquidated_event"):
        w("*** LIQUIDATION EVENT OCCURRED during this test ***")

    w(f"Total Trades     : {stats['total_trades']:,}")
    w(f"Wins / Losses    : {stats['wins']:,} / {stats['losses']:,}")
    w(f"Force-closed EoD : {stats['forced_closed']:,}")
    w(f"Timed-out (10d)  : {stats['timed_out']:,}")
    w(f"Win Rate         : {stats['win_rate']*100:.2f}%")
    w(f"Profit Factor    : {stats['profit_factor']:.4f}")
    w(f"Net PnL          : ${stats['net_pnl']:,.2f}  (on ${START_CAPITAL:,.0f})")
    w(f"Final Equity     : ${stats['final_equity']:,.2f}")
    w(f"Max Drawdown     : {stats['max_drawdown']*100:.2f}%")
    w(f"Sharpe           : {stats['sharpe']:.3f}")
    w(f"Sortino          : {stats['sortino']:.3f}")
    w(f"Avg Win          : ${stats['avg_win']:,.2f}")
    w(f"Avg Loss         : ${stats['avg_loss']:,.2f}")
    w(f"Expectancy       : ${stats['expectancy']:,.4f}/trade")
    w(f"Avg Duration     : {stats['avg_duration_bars']:.1f} bars ({stats['avg_duration_bars']*15/60:.1f}h)")
    w(f"Max Duration     : {stats['max_duration_bars']} bars ({stats['max_duration_bars']*15/60/24:.1f}d)")
    w(f"Avg Notional     : ${stats['avg_notional']:,.2f}  |  Avg Margin: ${stats['avg_margin']:,.2f}")
    w(f"Leverage         : {stats['leverage_used']}x")
    w(f"Long  Trades     : {stats['long_trades']:,}  WR {stats['long_wr']*100:.2f}%")
    w(f"Short Trades     : {stats['short_trades']:,}  WR {stats['short_wr']*100:.2f}%")
    w(f"Win Streak Best  : {stats['best_win_streak']}")
    w(f"Loss Streak Best : {stats['best_loss_streak']}")
    w(f"VERDICT          : {verdict}")
    w("")
    w("-- Scale reference (approx, compounding) ---")
    w(f"  $10K start  -> ${stats['net_pnl']:,.2f}")
    w(f"  $50K start  -> ~${stats['net_pnl']*5:,.0f}  (linear approx)")
    w("")
    w("-- Filter Rejections ---")
    total = sum(rej.values())
    w(f"  Warmup           : {rej['warmup']:>9,}")
    w(f"  Sym already open : {rej['sym_open']:>9,}")
    w(f"  Slope            : {rej['slope']:>9,}")
    w(f"  No crossover     : {rej['no_cross']:>9,}")
    w(f"  ADX < {ADX_MIN:.0f}         : {rej['adx']:>9,}")
    w(f"  Insuff capital   : {rej['insuff_cap']:>9,}")
    w(f"  EXECUTED         : {rej['executed']:>9,}")
    w(f"  Total bar-events : {total:>9,}")
    w("")
    w("-- Monthly PnL ---")
    for ym, pnl in stats["monthly_pnl"].items():
        bar_str = "+" if pnl >= 0 else ""
        w(f"  {ym}: ${bar_str}{pnl:,.2f}")
    w("")
    w("-- Per-Coin (sorted by PF) ---")
    w(f"  {'Symbol':<24} {'PF':>7}  {'WR':>7}  {'Trades':>7}  {'Net PnL':>14}")
    for r in coin_table:
        pf_s = f"{r['profit_factor']:.3f}" if r["profit_factor"] != float("inf") else "   INF"
        w(f"  {r['symbol']:<24} {pf_s:>7}  {r['win_rate']*100:>6.1f}%  {r['trades']:>7,}  ${r['net_pnl']:>13,.2f}")
    return "\n".join(a)

# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    print(f"Strategy G | VAR_D | 5 Stress Tests | 144 coins | 15m | {LEVERAGE}x leverage")
    print(f"TP {TP_PCT*100:.1f}%  SL {SL_PCT*100:.1f}%  Risk {RISK_PCT*100:.2f}%/trade  Hold cap {MAX_HOLD_BARS} bars")
    print(f"Coins: {len(COINS)}")
    print()

    # Determine widest date range needed across all tests
    # (download once, slice per test)
    all_starts = [t["start"] for t in TESTS]
    all_ends   = [t["end"]   for t in TESTS]
    GLOBAL_START = min(all_starts)
    GLOBAL_END   = max(all_ends)

    print(f"Phase 1: Downloading {len(COINS)} coins [{INTERVAL}] "
          f"{GLOBAL_START[0]}-{GLOBAL_START[1]:02d} → {GLOBAL_END[0]}-{GLOBAL_END[1]:02d} ...")
    all_bars = {}
    tasks = [(sym, GLOBAL_START[0], GLOBAL_START[1], GLOBAL_END[0], GLOBAL_END[1])
             for sym in COINS]

    ok_count  = 0
    miss_count = 0
    with ThreadPoolExecutor(max_workers=100) as ex:
        futs = {ex.submit(fetch_symbol, t): t[0] for t in tasks}
        for fut in as_completed(futs):
            sym = futs[fut]
            try:
                bars = fut.result()
                if bars:
                    all_bars[sym] = bars
                    ok_count += 1
                    print(f"  OK  {sym}: {len(bars):,} bars")
                else:
                    miss_count += 1
                    print(f"  MISS {sym}")
            except Exception as e:
                miss_count += 1
                print(f"  ERR  {sym}: {e}")

    print(f"\nLoaded: {ok_count}/{len(COINS)}  Missing: {miss_count}")
    if ok_count == 0:
        print("FATAL: 0 symbols loaded — likely geo-block or network issue. Aborting.")
        return

    # Phase 2: Indicators (compute once on full date range)
    print("\nPhase 2: Computing indicators ...")
    sym_inds_full = {}
    ind_tasks = [(sym, bars) for sym, bars in all_bars.items()
                 if len(bars) >= WARMUP_BARS + 5]
    with ThreadPoolExecutor(max_workers=50) as ex:
        futs = {ex.submit(compute_indicators, t): t[0] for t in ind_tasks}
        for fut in as_completed(futs):
            sym = futs[fut]
            try:
                sym_out, inds = fut.result()
                if inds:
                    sym_inds_full[sym_out] = inds
            except Exception as e:
                print(f"  IND ERR {sym}: {e}")
    print(f"  {len(sym_inds_full)} symbols with indicators ready.")

    # Phase 3: Run each test
    summaries  = []
    report     = {
        "meta": {
            "strategy":     "Strategy G VAR_D (5 Stress Tests)",
            "tp_pct":       TP_PCT,
            "sl_pct":       SL_PCT,
            "leverage":     LEVERAGE,
            "risk_pct":     RISK_PCT,
            "start_capital": START_CAPITAL,
            "fee_pct":      FEE_RATE,
            "max_hold_bars": MAX_HOLD_BARS,
            "coins_listed": len(COINS),
            "adx_min":      ADX_MIN,
            "slope_min_pct": SLOPE_MIN,
        }
    }

    all_test_results = {}

    for test_cfg in TESTS:
        tname  = test_cfg["name"]
        sy, sm = test_cfg["start"]
        ey, em = test_cfg["end"]

        print(f"\nPhase 3 [{tname}]: {test_cfg['label']}")

        # Slice bars to this test's date range
        start_ts = int(datetime(sy, sm, 1, tzinfo=timezone.utc).timestamp() * 1000)
        if em == 12:
            end_ts = int(datetime(ey + 1, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
        else:
            end_ts = int(datetime(ey, em + 1, 1, tzinfo=timezone.utc).timestamp() * 1000)

        sym_inds_slice = {}
        for sym, inds in sym_inds_full.items():
            bars_slice = [b for b in inds["bars"] if start_ts <= b["ts"] < end_ts]
            if len(bars_slice) < WARMUP_BARS + 5:
                continue
            closes = [b["close"] for b in bars_slice]
            sym_inds_slice[sym] = {
                "ema9":  calc_ema(closes, EMA_FAST),
                "ema21": calc_ema(closes, EMA_SLOW),
                "ema50": calc_ema(closes, EMA_TREND),
                "adx":   calc_adx_array(bars_slice, ADX_PERIOD),
                "bars":  bars_slice,
            }

        loaded = len(sym_inds_slice)
        print(f"  Symbols in range: {loaded}")

        trades, final_eq, rej = run_test(test_cfg, sym_inds_slice)
        stats      = compute_stats(trades, START_CAPITAL, final_eq)
        coin_table = per_coin_stats(trades)

        summary = write_summary(test_cfg, stats, coin_table, rej, loaded)
        summaries.append(summary)
        print("\n" + summary)

        all_test_results[tname] = {
            "stats":      stats,
            "coin_table": coin_table,
            "trades":     len(trades),
            "final_eq":   final_eq,
        }

        report[tname] = {
            "label":       test_cfg["label"],
            "period":      f"{sy}-{sm:02d} to {ey}-{em:02d}",
            "slip_rate":   test_cfg["slip_rate"],
            "liq_test":    test_cfg["liq_test"],
            "coins_loaded": loaded,
            "aggregate":   stats,
            "per_coin":    coin_table,
            "pass_pf":     test_cfg["pass_pf"],
            "pass_wr":     test_cfg["pass_wr"],
        }

    # Phase 4: Quick comparison table
    print("\n" + "=" * 85)
    print("COMPARISON ACROSS ALL 5 TESTS")
    print("=" * 85)
    print(f"{'Test':<28} {'Trades':>7} {'WR%':>7} {'PF':>7} {'DD%':>7} {'PnL':>14}  Verdict")
    print("-" * 85)
    for test_cfg in TESTS:
        tname = test_cfg["name"]
        r = all_test_results.get(tname, {})
        s = r.get("stats")
        if s:
            ok = s["profit_factor"] >= test_cfg["pass_pf"] and s["win_rate"] >= test_cfg["pass_wr"]
            verdict = "PASS" if ok else "FAIL"
            liq_flag = " [LIQ]" if s.get("liquidated_event") else ""
            print(f"{tname:<28} {s['total_trades']:>7,} {s['win_rate']*100:>6.2f}% "
                  f"{s['profit_factor']:>7.4f} {s['max_drawdown']*100:>6.2f}% "
                  f"${s['net_pnl']:>13,.2f}  {verdict}{liq_flag}")
        else:
            print(f"{tname:<28}  NO DATA")

    # Write outputs
    sep = "\n\n" + "=" * 72 + "\n\n"
    with open("backtest_summary.txt", "w") as f:
        f.write(sep.join(summaries) + "\n")
    with open("backtest_report.json", "w") as f:
        json.dump(report, f, indent=2, default=str)

    print("\nOutputs: backtest_summary.txt  backtest_report.json")
    print("Done.")

if __name__ == "__main__":
    main()
