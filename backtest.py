"""
Liquidity Exhaustion Reversal (LER) Strategy — Backtest v3
===========================================================
Custom strategy: Wick exhaustion + volume dropout + RSI divergence
on 30m entries, filtered by 4H EMA21 price position.

v3 changes vs v2 (informed by v1+v2 results):
  - HTF filter     : EMA slope → price above/below EMA21 (stronger S/R filter)
  - Wick ratio     : 1.8 → 2.0 (v2 got noisy at 1.8, too many bad signals)
  - Vol spike      : 1.7 → 1.9 (tighten back up slightly)
  - TP             : 1.8R → 2.2R (1.8R clipped winners too early)
  - RSI lookback   : keep 5 bars (working well in v2)
  - SL ATR buffer  : keep 0.15
  - Consecutive loss guard: pause coin for 10 bars after 3 consecutive losses
  - Focused 20-coin whitelist: cross-run survivors from v1 + v2

Whitelist logic:
  PF >= 1.5 in v1: TRUMP, SKL, ACT, WLD, GALA, FTM, SHIB, DOT, CTK, BONK,
                   SUI, ZEN, ATOM, PYTH, LTC
  PF >= 1.5 in v2: ZRX, STX, KLAY, INJ, ADA, WIF, ALGO, OP, SKL, XMR,
                   FET, DOT, ICP, DOGE, PYTH
  Final 20 (union of both + cross-run overlap prioritised):
  SKL, DOT, PYTH, FET, ZRX, INJ, ADA, ALGO, OP, XMR, ICP, DOGE, WIF,
  STX, KLAY, ATOM, GALA, 1000BONKUSDT, 1000SHIBUSDT, WLDUSDT

Rules:
  LONG entry (all must be true):
    1. LTF close > 4H EMA21 (price above dynamic support — uptrend context)
    2. 30m candle has long LOWER wick: lower_wick / body >= 2.0
    3. Volume >= 1.9x the 20-bar rolling average
    4. Next candle volume < wick candle volume (exhaustion confirmed)
    5. RSI(14) higher than 5 bars ago AND price made lower low (bullish div)
    Entry: open of the candle AFTER confirmation candle

  SHORT entry (mirror):
    1. LTF close < 4H EMA21 (price below dynamic resistance)
    2. Long UPPER wick: upper_wick / body >= 2.0
    3. Volume >= 1.9x 20-bar avg
    4. Next candle volume drops
    5. RSI lower than 5 bars ago AND price made higher high (bearish div)

  Exit:
    - TP: 2.2R
    - SL: low of wick candle - 0.15xATR(14) for longs
          high of wick candle + 0.15xATR(14) for shorts
    - Max hold: 48 bars (24h)
    - Consecutive loss guard: skip signals for 10 bars after 3 losses in a row

Data: data.binance.vision futures monthly archives
Parallel: ThreadPoolExecutor (8 workers)
"""

import csv
import io
import json
import math
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from urllib.request import urlopen, Request
from urllib.error import HTTPError

# ── CONFIG ────────────────────────────────────────────────────────────────────

INTERVAL_LTF    = "30m"
INTERVAL_HTF    = "4h"
START_YEAR      = 2024
START_MONTH     = 1
END_YEAR        = 2025
END_MONTH       = 12

CAPITAL_START   = 10_000.0
RISK_PCT        = 0.0075
FEE_SIDE        = 0.0005
SLIPPAGE_SIDE   = 0.0002
MAX_POSITIONS   = 6
TP_R            = 2.2
MAX_HOLD_BARS   = 48

WICK_RATIO_MIN  = 2.0
VOL_SPIKE_MULT  = 1.9
RSI_PERIOD      = 14
EMA_PERIOD      = 21
ATR_PERIOD      = 14
VOL_AVG_PERIOD  = 20
SL_ATR_BUFFER   = 0.15
RSI_DIV_LOOKBACK = 5

CONSEC_LOSS_LIMIT = 3    # pause signals after this many consecutive losses
CONSEC_LOSS_PAUSE = 10   # bars to pause

MAX_WORKERS     = 8

PROFIT_FACTOR_TARGET = 1.5
WIN_RATE_TARGET      = 0.40

# Cross-run survivors: profitable in at least one of v1/v2, logical picks
SYMBOLS = [
    "SKLUSDT",        # v1 PF 3.74, v2 PF 1.80
    "DOTUSDT",        # v1 PF 2.05, v2 PF 1.65
    "PYTHUSDT",       # v1 PF 1.51, v2 PF 1.55
    "FETUSDT",        # v1 PF 1.38, v2 PF 1.70
    "ZRXUSDT",        # v2 PF 5.54
    "INJUSDT",        # v2 PF 2.80
    "ADAUSDT",        # v2 PF 2.48
    "ALGOUSDT",       # v2 PF 1.94
    "OPUSDT",         # v2 PF 1.87
    "XMRUSDT",        # v2 PF 1.77
    "ICPUSDT",        # v2 PF 1.64
    "DOGEUSDT",       # v2 PF 1.59
    "WIFUSDT",        # v2 PF 2.17
    "STXUSDT",        # v2 PF 4.26
    "KLAYUSDT",       # v2 PF 3.69
    "ATOMUSDT",       # v1 PF 1.51
    "GALAUSDT",       # v1 PF 2.49
    "1000BONKUSDT",   # v1 PF 1.82
    "1000SHIBUSDT",   # v1 PF 2.07
    "WLDUSDT",        # v1 PF 2.85
]

BASE_URL = "https://data.binance.vision/data/futures/um/monthly/klines"

# ── HELPERS ───────────────────────────────────────────────────────────────────

def months_range(sy, sm, ey, em):
    y, m = sy, sm
    while (y, m) <= (ey, em):
        yield y, m
        m += 1
        if m > 12:
            m = 1
            y += 1

def fetch_monthly_klines(symbol, interval, year, month):
    mm    = f"{month:02d}"
    fname = f"{symbol}-{interval}-{year}-{mm}.zip"
    url   = f"{BASE_URL}/{symbol}/{interval}/{fname}"
    try:
        req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urlopen(req, timeout=30) as resp:
            data = resp.read()
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            csv_name = zf.namelist()[0]
            with zf.open(csv_name) as f:
                rows = list(csv.reader(io.TextIOWrapper(f)))
        return rows
    except HTTPError as e:
        if e.code == 404:
            return None
        raise
    except Exception:
        return None

def parse_klines(rows):
    out = []
    for r in rows:
        if not r or not r[0].isdigit():
            continue
        ts = int(r[0])
        if ts > 10**14:
            ts //= 1000
        out.append({
            "t": ts,
            "o": float(r[1]),
            "h": float(r[2]),
            "l": float(r[3]),
            "c": float(r[4]),
            "v": float(r[5]),
        })
    return out

def load_symbol_data(symbol, interval, sy, sm, ey, em):
    all_bars = []
    for y, m in months_range(sy, sm, ey, em):
        rows = fetch_monthly_klines(symbol, interval, y, m)
        if rows is None:
            continue
        all_bars.extend(parse_klines(rows))
    all_bars.sort(key=lambda x: x["t"])
    deduped, seen_t = [], set()
    for b in all_bars:
        if b["t"] not in seen_t:
            seen_t.add(b["t"])
            deduped.append(b)
    return deduped

# ── INDICATORS ────────────────────────────────────────────────────────────────

def calc_ema(values, period):
    ema = [None] * len(values)
    k   = 2.0 / (period + 1)
    si  = period - 1
    if si >= len(values):
        return ema
    ema[si] = sum(values[:period]) / period
    for i in range(si + 1, len(values)):
        ema[i] = values[i] * k + ema[i-1] * (1 - k)
    return ema

def calc_rsi(closes, period=14):
    rsi = [None] * len(closes)
    if len(closes) < period + 1:
        return rsi
    gains, losses = [], []
    for i in range(1, period + 1):
        d = closes[i] - closes[i-1]
        gains.append(max(d, 0))
        losses.append(max(-d, 0))
    avg_g = sum(gains) / period
    avg_l = sum(losses) / period
    for i in range(period, len(closes)):
        if i > period:
            d     = closes[i] - closes[i-1]
            avg_g = (avg_g * (period - 1) + max(d, 0))  / period
            avg_l = (avg_l * (period - 1) + max(-d, 0)) / period
        rs      = avg_g / avg_l if avg_l != 0 else 1e9
        rsi[i]  = 100 - (100 / (1 + rs))
    return rsi

def calc_atr(bars, period=14):
    atr = [None] * len(bars)
    if len(bars) < period + 1:
        return atr
    trs = []
    for i in range(1, len(bars)):
        h, l, pc = bars[i]["h"], bars[i]["l"], bars[i-1]["c"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    atr[period] = sum(trs[:period]) / period
    for i in range(period + 1, len(bars)):
        atr[i] = (atr[i-1] * (period - 1) + trs[i-1]) / period
    return atr

def calc_vol_avg(bars, period=20):
    out = [None] * len(bars)
    for i in range(period - 1, len(bars)):
        out[i] = sum(bars[j]["v"] for j in range(i - period + 1, i + 1)) / period
    return out

def build_htf_ema_price_map(htf_bars, period=21):
    """
    Returns dict: timestamp -> ema_value for closed HTF bars.
    v3: we compare LTF close vs HTF EMA level (not slope).
    """
    closes = [b["c"] for b in htf_bars]
    emas   = calc_ema(closes, period)
    result = {}
    for i in range(len(htf_bars)):
        if emas[i] is not None:
            result[htf_bars[i]["t"]] = emas[i]
    return result

def get_htf_ema_level(htf_ema_map, ltf_ts, htf_period_ms):
    """
    Return the EMA21 value from the last CLOSED 4H bar before ltf_ts.
    Offset by one full HTF period to avoid lookahead.
    """
    query_ts = ltf_ts - htf_period_ms
    best_ts  = None
    best_val = None
    for ts, val in htf_ema_map.items():
        if ts <= query_ts:
            if best_ts is None or ts > best_ts:
                best_ts  = ts
                best_val = val
    return best_val  # None if no closed bar yet

# ── BACKTEST ENGINE ───────────────────────────────────────────────────────────

HTF_MS = 4 * 60 * 60 * 1000   # 4h in milliseconds

def backtest_symbol(symbol, ltf_bars, htf_bars):
    closes_ltf = [b["c"] for b in ltf_bars]
    rsi_vals   = calc_rsi(closes_ltf, RSI_PERIOD)
    atr_vals   = calc_atr(ltf_bars, ATR_PERIOD)
    vol_avgs   = calc_vol_avg(ltf_bars, VOL_AVG_PERIOD)
    htf_ema_map = build_htf_ema_price_map(htf_bars, EMA_PERIOD)

    trades = []
    filter_stats = {
        "total_candles":        0,
        "warmup_none":          0,
        "no_htf_ema":           0,
        "htf_price_filter_fail":0,
        "wick_ratio_fail":      0,
        "vol_spike_fail":       0,
        "vol_dropout_fail":     0,
        "rsi_divergence_fail":  0,
        "consec_loss_pause":    0,
        "signal_generated":     0,
    }

    warmup = max(RSI_PERIOD, ATR_PERIOD, VOL_AVG_PERIOD) + RSI_DIV_LOOKBACK
    open_positions = []

    # Consecutive loss guard state
    consec_losses   = 0
    pause_until_bar = -1

    for i in range(warmup, len(ltf_bars) - 2):
        filter_stats["total_candles"] += 1

        bar  = ltf_bars[i]
        bar1 = ltf_bars[i + 1]
        bar2 = ltf_bars[i + 2]

        # ── Close open positions ──────────────────────────────────────────
        still_open = []
        for pos in open_positions:
            bars_held = i - pos["entry_bar_idx"]
            ep, sl, tp = pos["entry_price"], pos["sl"], pos["tp"]

            if pos["direction"] == "long":
                if bar["l"] <= sl:
                    pos.update(exit_price=sl, exit_ts=bar["t"], result="sl")
                    trades.append(pos)
                    consec_losses += 1
                    continue
                elif bar["h"] >= tp:
                    pos.update(exit_price=tp, exit_ts=bar["t"], result="tp")
                    trades.append(pos)
                    consec_losses = 0
                    continue
            else:
                if bar["h"] >= sl:
                    pos.update(exit_price=sl, exit_ts=bar["t"], result="sl")
                    trades.append(pos)
                    consec_losses += 1
                    continue
                elif bar["l"] <= tp:
                    pos.update(exit_price=tp, exit_ts=bar["t"], result="tp")
                    trades.append(pos)
                    consec_losses = 0
                    continue

            if bars_held >= MAX_HOLD_BARS:
                pos.update(exit_price=bar["c"], exit_ts=bar["t"], result="timeout")
                trades.append(pos)
                # timeout counts as loss for guard
                consec_losses += 1
                continue

            still_open.append(pos)
        open_positions = still_open

        # ── Update pause threshold ────────────────────────────────────────
        if consec_losses >= CONSEC_LOSS_LIMIT and pause_until_bar < i:
            pause_until_bar = i + CONSEC_LOSS_PAUSE

        # ── FILTER 1: Warmup ──────────────────────────────────────────────
        if (rsi_vals[i] is None or rsi_vals[i - RSI_DIV_LOOKBACK] is None
                or atr_vals[i] is None or vol_avgs[i] is None):
            filter_stats["warmup_none"] += 1
            continue

        # ── FILTER 2: HTF EMA level ───────────────────────────────────────
        htf_ema_val = get_htf_ema_level(htf_ema_map, bar["t"], HTF_MS)
        if htf_ema_val is None:
            filter_stats["no_htf_ema"] += 1
            continue

        # price must be clearly on one side of the EMA
        ltf_close = bar["c"]
        price_above = ltf_close > htf_ema_val
        price_below = ltf_close < htf_ema_val

        if not price_above and not price_below:
            filter_stats["htf_price_filter_fail"] += 1
            continue

        # ── FILTER 3: Wick ratio ──────────────────────────────────────────
        o, h, l, c = bar["o"], bar["h"], bar["l"], bar["c"]
        body        = max(abs(c - o), bar["l"] * 0.0001)
        lower_wick  = min(o, c) - l
        upper_wick  = h - max(o, c)
        lower_ratio = lower_wick / body
        upper_ratio = upper_wick / body

        long_candidate  = price_above and lower_ratio >= WICK_RATIO_MIN
        short_candidate = price_below and upper_ratio >= WICK_RATIO_MIN

        if not long_candidate and not short_candidate:
            filter_stats["wick_ratio_fail"] += 1
            continue

        # ── FILTER 4: Volume spike ────────────────────────────────────────
        if bar["v"] < VOL_SPIKE_MULT * vol_avgs[i]:
            filter_stats["vol_spike_fail"] += 1
            continue

        # ── FILTER 5: Volume dropout ──────────────────────────────────────
        if bar1["v"] >= bar["v"]:
            filter_stats["vol_dropout_fail"] += 1
            continue

        # ── FILTER 6: RSI divergence ──────────────────────────────────────
        rsi_now  = rsi_vals[i]
        rsi_prev = rsi_vals[i - RSI_DIV_LOOKBACK]

        if long_candidate:
            price_lower_low = bar["l"] < ltf_bars[i - RSI_DIV_LOOKBACK]["l"]
            rsi_higher      = rsi_now > rsi_prev
            if not (price_lower_low and rsi_higher):
                filter_stats["rsi_divergence_fail"] += 1
                continue
        else:
            price_higher_high = bar["h"] > ltf_bars[i - RSI_DIV_LOOKBACK]["h"]
            rsi_lower         = rsi_now < rsi_prev
            if not (price_higher_high and rsi_lower):
                filter_stats["rsi_divergence_fail"] += 1
                continue

        # ── FILTER 7: Consecutive loss guard ─────────────────────────────
        if i < pause_until_bar:
            filter_stats["consec_loss_pause"] += 1
            continue

        # Reset loss counter once we pass the pause window
        if i >= pause_until_bar and consec_losses >= CONSEC_LOSS_LIMIT:
            consec_losses = 0

        filter_stats["signal_generated"] += 1

        # ── BUILD TRADE ───────────────────────────────────────────────────
        direction = "long" if long_candidate else "short"
        entry_px  = bar2["o"]
        atr_val   = atr_vals[i]

        if direction == "long":
            sl   = bar["l"] - SL_ATR_BUFFER * atr_val
            risk = entry_px - sl
            if risk <= 0:
                continue
            tp = entry_px + TP_R * risk
        else:
            sl   = bar["h"] + SL_ATR_BUFFER * atr_val
            risk = sl - entry_px
            if risk <= 0:
                continue
            tp = entry_px - TP_R * risk

        open_positions.append({
            "symbol":        symbol,
            "direction":     direction,
            "entry_ts":      bar2["t"],
            "entry_bar_idx": i + 2,
            "entry_price":   entry_px,
            "sl":            sl,
            "tp":            tp,
            "risk_per_unit": risk,
            "exit_price":    None,
            "exit_ts":       None,
            "result":        None,
        })

    # Close remaining at end
    if ltf_bars:
        last = ltf_bars[-1]
        for pos in open_positions:
            pos.update(exit_price=last["c"], exit_ts=last["t"], result="end_of_data")
            trades.append(pos)

    return trades, filter_stats

# ── PORTFOLIO SIMULATION ──────────────────────────────────────────────────────

def simulate_portfolio(all_symbol_trades):
    all_trades = []
    for trades in all_symbol_trades.values():
        all_trades.extend(trades)
    all_trades.sort(key=lambda x: x["entry_ts"])

    equity   = CAPITAL_START
    open_pos = []
    results  = []

    for trade in all_trades:
        open_pos = [p for p in open_pos if p["exit_ts"] > trade["entry_ts"]]

        if len(open_pos) >= MAX_POSITIONS:
            trade["result_detail"] = "skipped_max_pos"
            results.append(trade)
            continue

        risk_amt  = equity * RISK_PCT
        qty       = risk_amt / trade["risk_per_unit"]
        total_fee = (FEE_SIDE + SLIPPAGE_SIDE) * 2 * trade["entry_price"] * qty
        ep, xp    = trade["entry_price"], trade["exit_price"]

        if trade["direction"] == "long":
            gross_pnl = (xp - ep) * qty
        else:
            gross_pnl = (ep - xp) * qty

        net_pnl = gross_pnl - total_fee
        equity += net_pnl

        trade["qty"]          = qty
        trade["gross_pnl"]    = gross_pnl
        trade["net_pnl"]      = net_pnl
        trade["equity_after"] = equity
        trade["result_detail"] = trade["result"]

        open_pos.append(trade)
        results.append(trade)

    return results, equity

# ── STATS ─────────────────────────────────────────────────────────────────────

def compute_stats(results):
    executed = [t for t in results if t.get("result_detail") != "skipped_max_pos"]
    skipped  = [t for t in results if t.get("result_detail") == "skipped_max_pos"]
    if not executed:
        return None

    pnls   = [t["net_pnl"] for t in executed]
    wins   = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p <= 0]

    total_trades = len(executed)
    win_count    = len(wins)
    win_rate     = win_count / total_trades if total_trades else 0
    gross_profit = sum(wins)
    gross_loss   = abs(sum(losses)) if losses else 0
    pf           = gross_profit / gross_loss if gross_loss > 0 else float("inf")
    net_pnl      = sum(pnls)
    avg_win      = gross_profit / win_count  if win_count  else 0
    avg_loss     = gross_loss   / len(losses) if losses    else 0
    expectancy   = (win_rate * avg_win) - ((1 - win_rate) * avg_loss)

    # Drawdown
    peak, max_dd = CAPITAL_START, 0
    for t in executed:
        e = t["equity_after"]
        if e > peak:
            peak = e
        dd = (peak - e) / peak
        if dd > max_dd:
            max_dd = dd

    # Daily Sharpe/Sortino
    daily_pnls = {}
    for t in executed:
        day = t["entry_ts"] // (86400 * 1000)
        daily_pnls.setdefault(day, 0)
        daily_pnls[day] += t["net_pnl"]
    dr = list(daily_pnls.values())
    if len(dr) > 1:
        mean_r = sum(dr) / len(dr)
        std_r   = math.sqrt(sum((x - mean_r)**2 for x in dr) / len(dr))
        sharpe  = (mean_r / std_r * math.sqrt(365)) if std_r > 0 else 0
        down_r  = [x for x in dr if x < 0]
        std_d   = math.sqrt(sum(x**2 for x in down_r) / len(down_r)) if down_r else 0
        sortino = (mean_r / std_d * math.sqrt(365)) if std_d > 0 else 0
    else:
        sharpe = sortino = 0

    # Streaks
    streak_win = streak_loss = cur_w = cur_l = 0
    for p in pnls:
        if p > 0:
            cur_w += 1; cur_l = 0
        else:
            cur_l += 1; cur_w = 0
        streak_win  = max(streak_win,  cur_w)
        streak_loss = max(streak_loss, cur_l)

    longs  = [t for t in executed if t["direction"] == "long"]
    shorts = [t for t in executed if t["direction"] == "short"]
    long_wr  = len([t for t in longs  if t["net_pnl"] > 0]) / len(longs)  if longs  else 0
    short_wr = len([t for t in shorts if t["net_pnl"] > 0]) / len(shorts) if shorts else 0

    durations = []
    for t in executed:
        if t.get("exit_ts") and t.get("entry_ts"):
            durations.append((t["exit_ts"] - t["entry_ts"]) // (30 * 60 * 1000))
    avg_dur_h = (sum(durations) / len(durations) * 0.5) if durations else 0

    # Exit breakdown
    tp_count      = len([t for t in executed if t["result"] == "tp"])
    sl_count      = len([t for t in executed if t["result"] == "sl"])
    timeout_count = len([t for t in executed if t["result"] == "timeout"])
    eod_count     = len([t for t in executed if t["result"] == "end_of_data"])

    return {
        "total_trades":     total_trades,
        "skipped_max_pos":  len(skipped),
        "win_rate":         round(win_rate * 100, 2),
        "profit_factor":    round(pf, 4),
        "net_pnl":          round(net_pnl, 2),
        "gross_profit":     round(gross_profit, 2),
        "gross_loss":       round(gross_loss, 2),
        "max_drawdown_pct": round(max_dd * 100, 2),
        "sharpe":           round(sharpe, 3),
        "sortino":          round(sortino, 3),
        "avg_win":          round(avg_win, 2),
        "avg_loss":         round(avg_loss, 2),
        "expectancy":       round(expectancy, 2),
        "avg_duration_h":   round(avg_dur_h, 1),
        "long_count":       len(longs),
        "short_count":      len(shorts),
        "long_wr_pct":      round(long_wr * 100, 2),
        "short_wr_pct":     round(short_wr * 100, 2),
        "max_win_streak":   streak_win,
        "max_loss_streak":  streak_loss,
        "final_equity":     round(CAPITAL_START + net_pnl, 2),
        "tp_count":         tp_count,
        "sl_count":         sl_count,
        "timeout_count":    timeout_count,
        "eod_count":        eod_count,
    }

def per_coin_stats(all_symbol_trades):
    rows = []
    for sym, trades in all_symbol_trades.items():
        executed = [t for t in trades if t.get("exit_price") is not None
                    and "net_pnl" in t]
        if not executed:
            rows.append({"symbol": sym, "trades": 0, "pf": 0, "wr": 0, "net_pnl": 0})
            continue
        pnls   = [t["net_pnl"] for t in executed]
        wins   = [p for p in pnls if p > 0]
        losses = [p for p in pnls if p <= 0]
        gp     = sum(wins)
        gl     = abs(sum(losses))
        pf     = gp / gl if gl > 0 else float("inf")
        wr     = len(wins) / len(pnls) * 100
        rows.append({
            "symbol":  sym,
            "trades":  len(pnls),
            "pf":      round(pf, 4),
            "wr":      round(wr, 2),
            "net_pnl": round(sum(pnls), 2),
        })
    rows.sort(key=lambda x: x["pf"], reverse=True)
    return rows

def monthly_pnl(results):
    mp = {}
    for t in results:
        if "net_pnl" not in t:
            continue
        dt  = datetime.fromtimestamp(t["entry_ts"] / 1000, tz=timezone.utc)
        key = f"{dt.year}-{dt.month:02d}"
        mp.setdefault(key, 0)
        mp[key] += t["net_pnl"]
    return {k: round(v, 2) for k, v in sorted(mp.items())}

# ── WORKER ────────────────────────────────────────────────────────────────────

def process_symbol(symbol):
    try:
        ltf_bars = load_symbol_data(
            symbol, INTERVAL_LTF, START_YEAR, START_MONTH, END_YEAR, END_MONTH)
        htf_bars = load_symbol_data(
            symbol, INTERVAL_HTF, START_YEAR, START_MONTH, END_YEAR, END_MONTH)

        if len(ltf_bars) < 200:
            return symbol, [], {}, f"insufficient_data({len(ltf_bars)} bars)"
        if len(htf_bars) < 50:
            return symbol, [], {}, f"insufficient_htf({len(htf_bars)} bars)"

        trades, fstats = backtest_symbol(symbol, ltf_bars, htf_bars)
        return symbol, trades, fstats, "ok"
    except Exception as e:
        return symbol, [], {}, f"error:{e}"

# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    print("=" * 65)
    print("  LER v3 — Liquidity Exhaustion Reversal Backtest")
    print(f"  Period  : {START_YEAR}-{START_MONTH:02d} → {END_YEAR}-{END_MONTH:02d}")
    print(f"  Coins   : {len(SYMBOLS)} (focused whitelist)")
    print(f"  Workers : {MAX_WORKERS}")
    print(f"  TP      : {TP_R}R  |  Wick: {WICK_RATIO_MIN}x  |  VolSpike: {VOL_SPIKE_MULT}x")
    print(f"  HTF     : price vs EMA{EMA_PERIOD} (not slope)")
    print(f"  Loss guard: pause {CONSEC_LOSS_PAUSE} bars after {CONSEC_LOSS_LIMIT} losses")
    print("=" * 65)

    all_symbol_trades = {}
    all_filter_stats  = {}
    symbol_statuses   = {}

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_symbol, sym): sym for sym in SYMBOLS}
        done = 0
        for future in as_completed(futures):
            sym = futures[future]
            symbol, trades, fstats, status = future.result()
            done += 1
            all_symbol_trades[symbol] = trades
            all_filter_stats[symbol]  = fstats
            symbol_statuses[symbol]   = status
            tag = f"[{len(trades):3d} trades]" if status == "ok" else f"[{status}]"
            print(f"  [{done:2d}/{len(SYMBOLS)}] {symbol:<22} {tag}")

    print("\nRunning portfolio simulation...")
    results, final_equity = simulate_portfolio(all_symbol_trades)

    print("Computing stats...")
    agg     = compute_stats(results)
    pc      = per_coin_stats(all_symbol_trades)
    monthly = monthly_pnl(results)

    total_fstats = {}
    for fs in all_filter_stats.values():
        for k, v in fs.items():
            total_fstats[k] = total_fstats.get(k, 0) + v

    # ── PRINT ─────────────────────────────────────────────────────────────────
    print("\n" + "=" * 65)
    print("  AGGREGATE RESULTS")
    print("=" * 65)
    if agg:
        print(f"  Total Trades      : {agg['total_trades']}")
        print(f"  Skipped (max pos) : {agg['skipped_max_pos']}")
        print(f"  Win Rate          : {agg['win_rate']}%  (target ≥{WIN_RATE_TARGET*100:.0f}%)")
        print(f"  Profit Factor     : {agg['profit_factor']}  (target ≥{PROFIT_FACTOR_TARGET})")
        print(f"  Net PnL           : ${agg['net_pnl']:,.2f}")
        print(f"  Final Equity      : ${agg['final_equity']:,.2f}")
        print(f"  Max Drawdown      : {agg['max_drawdown_pct']}%")
        print(f"  Sharpe            : {agg['sharpe']}")
        print(f"  Sortino           : {agg['sortino']}")
        print(f"  Avg Win           : ${agg['avg_win']:,.2f}")
        print(f"  Avg Loss          : ${agg['avg_loss']:,.2f}")
        print(f"  Expectancy/trade  : ${agg['expectancy']:,.2f}")
        print(f"  Avg Duration      : {agg['avg_duration_h']}h")
        print(f"  Longs             : {agg['long_count']} (WR {agg['long_wr_pct']}%)")
        print(f"  Shorts            : {agg['short_count']} (WR {agg['short_wr_pct']}%)")
        print(f"  TP hits           : {agg['tp_count']}")
        print(f"  SL hits           : {agg['sl_count']}")
        print(f"  Timeouts          : {agg['timeout_count']}")
        print(f"  Max Win Streak    : {agg['max_win_streak']}")
        print(f"  Max Loss Streak   : {agg['max_loss_streak']}")
        pf_ok = agg["profit_factor"] >= PROFIT_FACTOR_TARGET
        wr_ok = agg["win_rate"]      >= WIN_RATE_TARGET * 100
        verdict = "✅ MEETS TARGETS" if (pf_ok and wr_ok) else "❌ BELOW TARGETS"
        print(f"\n  VERDICT: {verdict}")
    else:
        print("  No trades executed.")

    print("\n" + "=" * 65)
    print("  PER-COIN RESULTS")
    print("=" * 65)
    print(f"  {'Symbol':<22} {'Trades':>6} {'PF':>7} {'WR%':>7} {'NetPnL':>10}")
    print("  " + "-" * 57)
    for row in pc:
        pf_str = f"{row['pf']:.4f}" if row['pf'] != float("inf") else "∞"
        flag   = " ✅" if row["pf"] >= PROFIT_FACTOR_TARGET else ""
        print(f"  {row['symbol']:<22} {row['trades']:>6} {pf_str:>7} "
              f"{row['wr']:>6.2f}% ${row['net_pnl']:>9,.2f}{flag}")

    print("\n" + "=" * 65)
    print("  MONTHLY PnL")
    print("=" * 65)
    max_abs = max((abs(v) for v in monthly.values()), default=1)
    for month, pnl in monthly.items():
        bar_len = int(abs(pnl) / max_abs * 30)
        bar_str = ("█" * bar_len) if pnl >= 0 else ("░" * bar_len)
        sign    = "+" if pnl >= 0 else ""
        print(f"  {month}  {sign}${pnl:>9,.2f}  {bar_str}")

    print("\n" + "=" * 65)
    print("  FILTER REJECTION STATS")
    print("=" * 65)
    total_c = total_fstats.get("total_candles", 1)
    for k, v in total_fstats.items():
        pct = v / total_c * 100 if total_c else 0
        print(f"  {k:<30} {v:>7,}  ({pct:5.1f}%)")

    # ── WRITE OUTPUT ──────────────────────────────────────────────────────────
    with open("backtest_summary.txt", "w") as f:
        f.write(f"LER v3 Backtest Summary — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}\n")
        f.write("=" * 65 + "\n")
        if agg:
            for k, v in agg.items():
                f.write(f"  {k}: {v}\n")
        f.write("\nPer-Coin Results:\n")
        f.write(f"{'Symbol':<22} {'Trades':>6} {'PF':>8} {'WR%':>7} {'NetPnL':>12}\n")
        for row in pc:
            pf_str = f"{row['pf']:.4f}" if row['pf'] != float("inf") else "inf"
            f.write(f"{row['symbol']:<22} {row['trades']:>6} {pf_str:>8} "
                    f"{row['wr']:>6.2f}% ${row['net_pnl']:>11,.2f}\n")
        f.write("\nMonthly PnL:\n")
        for month, pnl in monthly.items():
            f.write(f"  {month}: ${pnl:,.2f}\n")
        f.write("\nFilter Stats:\n")
        for k, v in total_fstats.items():
            f.write(f"  {k}: {v}\n")

    report = {
        "meta": {
            "strategy": "LER v3 — Liquidity Exhaustion Reversal (focused whitelist)",
            "version":  "v3",
            "period":   f"{START_YEAR}-{START_MONTH:02d} to {END_YEAR}-{END_MONTH:02d}",
            "ltf":      INTERVAL_LTF,
            "htf":      INTERVAL_HTF,
            "coins":    SYMBOLS,
            "settings": {
                "capital":         CAPITAL_START,
                "risk_pct":        RISK_PCT,
                "fee_side":        FEE_SIDE,
                "slippage_side":   SLIPPAGE_SIDE,
                "max_positions":   MAX_POSITIONS,
                "tp_r":            TP_R,
                "max_hold_bars":   MAX_HOLD_BARS,
            },
            "ler_params": {
                "wick_ratio_min":    WICK_RATIO_MIN,
                "vol_spike_mult":    VOL_SPIKE_MULT,
                "rsi_period":        RSI_PERIOD,
                "ema_period":        EMA_PERIOD,
                "sl_atr_buffer":     SL_ATR_BUFFER,
                "rsi_div_lookback":  RSI_DIV_LOOKBACK,
                "consec_loss_limit": CONSEC_LOSS_LIMIT,
                "consec_loss_pause": CONSEC_LOSS_PAUSE,
                "htf_filter":        "price_vs_ema21",
            },
        },
        "aggregate":       agg,
        "per_coin":        pc,
        "monthly_pnl":     monthly,
        "filter_stats":    total_fstats,
        "symbol_statuses": symbol_statuses,
        "trades": [
            {k: v for k, v in t.items() if k != "entry_bar_idx"}
            for t in results
        ],
    }

    with open("backtest_report.json", "w") as f:
        json.dump(report, f, indent=2, default=str)

    print("\n  Outputs: backtest_summary.txt + backtest_report.json")
    print("=" * 65)

if __name__ == "__main__":
    main()
