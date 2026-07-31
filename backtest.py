"""
Liquidity Exhaustion Reversal (LER) Strategy — Backtest v2
===========================================================
Custom strategy: Wick exhaustion + volume dropout + RSI divergence
on 30m entries, filtered by 4H trend direction.

v2 tuning vs v1 (informed by v1 results):
  - Wick ratio      : 2.5 → 1.8  (v1 too restrictive: 2203 signals from 3.3M bars)
  - TP              : 2.5R → 1.8R (avg duration was 5.7h, price rarely ran 2.5R)
  - Vol spike mult  : 2.0 → 1.7  (slightly looser, still meaningful)
  - RSI div lookback: 3 bars → 5 bars (wider window catches more valid divergences)
  - SL ATR buffer   : 0.1 → 0.15 (give trades more breathing room)
  - All 100 coins kept (removing coins post-hoc = curve fitting)

Rules:
  LONG entry (all must be true):
    1. 4H EMA21 slope is UP (current 4H EMA21 > prev 4H EMA21)
    2. 30m candle has a long LOWER wick: lower_wick / body >= 1.8
    3. 30m volume on that wick candle >= 1.7x the 20-bar rolling avg
    4. Next 30m candle's volume < wick candle's volume (exhaustion confirmed)
    5. RSI(14) on 30m makes a HIGHER reading vs 5 bars ago, while price
       made a LOWER low (bullish divergence — local momentum shift)
    Entry: open of the candle AFTER confirmation candle closes

  SHORT entry (mirror):
    1. 4H EMA21 slope is DOWN
    2. Long UPPER wick: upper_wick / body >= 1.8
    3. Volume >= 1.7x 20-bar avg on wick candle
    4. Next candle volume drops
    5. RSI makes LOWER reading vs 5 bars ago, price made HIGHER high

  Exit:
    - TP: 1.8R
    - SL: low of wick candle - 0.15xATR(14) for longs
          high of wick candle + 0.15xATR(14) for shorts
    - Max hold: 48 bars (24h on 30m)

Data: data.binance.vision futures monthly archives
Parallel: ThreadPoolExecutor (network-bound, threads are fine)
"""

import csv
import io
import json
import math
import os
import sys
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError

# ── CONFIG ────────────────────────────────────────────────────────────────────

INTERVAL_LTF   = "30m"
INTERVAL_HTF   = "4h"
START_YEAR      = 2024
START_MONTH     = 1
END_YEAR        = 2025
END_MONTH       = 12

CAPITAL_START   = 10_000.0
RISK_PCT        = 0.0075          # 0.75% per trade
FEE_SIDE        = 0.0005          # 0.05% per side
SLIPPAGE_SIDE   = 0.0002          # 0.02% per side
MAX_POSITIONS   = 6
TP_R            = 1.8
MAX_HOLD_BARS   = 48              # 24h on 30m

WICK_RATIO_MIN  = 1.8
VOL_SPIKE_MULT  = 1.7
RSI_PERIOD      = 14
EMA_PERIOD      = 21
ATR_PERIOD      = 14
VOL_AVG_PERIOD  = 20
SL_ATR_BUFFER   = 0.15
RSI_DIV_LOOKBACK = 5             # v1 used 3, widening to catch more divergences

MAX_WORKERS     = 8               # parallel coin downloaders

PROFIT_FACTOR_TARGET = 1.5
WIN_RATE_TARGET      = 0.42

SYMBOLS = [
    # Majors
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
    "ADAUSDT", "AVAXUSDT", "DOGEUSDT", "DOTUSDT", "LINKUSDT",
    "LTCUSDT", "UNIUSDT", "ATOMUSDT", "NEARUSDT", "APTUSDT",
    "ARBUSDT", "OPUSDT", "INJUSDT", "AAVEUSDT", "SUIUSDT",
    "HBARUSDT", "ICPUSDT", "FILUSDT", "LDOUSDT", "STXUSDT",
    "RUNEUSDT", "SEIUSDT", "TIAUSDT", "JUPUSDT", "PYTHUSDT",
    # Mid-cap alts
    "POLUSDT", "WLDUSDT", "FETUSDT", "RENDERUSDT", "GRTUSDT",
    "SANDUSDT", "MANAUSDT", "AXSUSDT", "ENJUSDT", "GALAUSDT",
    "FTMUSDT", "CRVUSDT", "COMPUSDT", "MKRUSDT", "YFIUSDT",
    "SNXUSDT", "BALUSDT", "SUSHIUSDT", "ZRXUSDT", "IOSTUSDT",
    "KAVAUSDT", "BANDUSDT", "RLCUSDT", "SKLUSDT", "CTKUSDT",
    "BLZUSDT", "CKBUSDT", "SFPUSDT", "CELRUSDT", "HOTUSDT",
    "ZILUSDT", "ONTUSDT", "VETUSDT", "IOTAUSDT", "XTZUSDT",
    "EOSUSDT", "XLMUSDT", "TRXUSDT", "ETCUSDT", "BCHUSDT",
    "ALGOUSDT", "DASHUSDT", "ZECUSDT", "XMRUSDT", "NEOUSDT",
    "QTUMUSDT", "WAVESUSDT", "BATUSDT", "ZENUSDT", "ANKRUSDT",
    # Memes & newer
    "1000BONKUSDT", "1000PEPEUSDT", "1000SHIBUSDT", "1000FLOKIUSDT",
    "WIFUSDT", "BOMEUSDT", "NEIROUSDT", "TRUMPUSDT", "MOODENGUSDT",
    "1000RATSUSDT", "POPCATUSDT", "ACTUSDT", "PNUTUSDT", "CHILLGUYUSDT",
    # DeFi / ecosystem
    "ARUSDT", "THETAUSDT", "FLOWUSDT", "EGLDUSDT", "KLAYUSDT",
    "HNTUSDT", "CKBUSDT", "ORDIUSDT", "SATSUSDT", "1000XECUSDT",
    "RONINUSDT", "ALTUSDT", "JUPUSDT", "WUSDT", "ZROUSDT",
]
# Deduplicate while preserving order
seen = set()
SYMBOLS_DEDUP = []
for s in SYMBOLS:
    if s not in seen:
        seen.add(s)
        SYMBOLS_DEDUP.append(s)
SYMBOLS = SYMBOLS_DEDUP[:100]

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
    mm = f"{month:02d}"
    fname = f"{symbol}-{interval}-{year}-{mm}.zip"
    url = f"{BASE_URL}/{symbol}/{interval}/{fname}"
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
            return None   # symbol didn't exist yet or delisted
        raise
    except Exception:
        return None

def parse_klines(rows):
    """Returns list of dicts with OHLCV, timestamp in ms."""
    out = []
    for r in rows:
        if not r or not r[0].isdigit():
            continue
        ts = int(r[0])
        # Guard microseconds (Binance changed spot; futures may follow)
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
    skipped_months = 0
    for y, m in months_range(sy, sm, ey, em):
        rows = fetch_monthly_klines(symbol, interval, y, m)
        if rows is None:
            skipped_months += 1
            continue
        all_bars.extend(parse_klines(rows))
    # Sort and deduplicate by timestamp
    all_bars.sort(key=lambda x: x["t"])
    deduped = []
    seen_t = set()
    for b in all_bars:
        if b["t"] not in seen_t:
            seen_t.add(b["t"])
            deduped.append(b)
    return deduped, skipped_months

# ── INDICATORS ────────────────────────────────────────────────────────────────

def calc_ema(values, period):
    """Returns list of EMA values, None for warmup bars."""
    ema = [None] * len(values)
    k = 2.0 / (period + 1)
    # seed with SMA
    seed_idx = period - 1
    if seed_idx >= len(values):
        return ema
    ema[seed_idx] = sum(values[i] for i in range(period)) / period
    for i in range(seed_idx + 1, len(values)):
        ema[i] = values[i] * k + ema[i-1] * (1 - k)
    return ema

def calc_rsi(closes, period=14):
    """Returns list of RSI values, None for warmup."""
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
            d = closes[i] - closes[i-1]
            g = max(d, 0)
            l = max(-d, 0)
            avg_g = (avg_g * (period - 1) + g) / period
            avg_l = (avg_l * (period - 1) + l) / period
        rs = avg_g / avg_l if avg_l != 0 else 1e9
        rsi[i] = 100 - (100 / (1 + rs))
    return rsi

def calc_atr(bars, period=14):
    """Returns list of ATR values, None for warmup."""
    atr = [None] * len(bars)
    if len(bars) < period + 1:
        return atr
    trs = []
    for i in range(1, len(bars)):
        h, l, pc = bars[i]["h"], bars[i]["l"], bars[i-1]["c"]
        tr = max(h - l, abs(h - pc), abs(l - pc))
        trs.append(tr)
    # seed ATR
    atr[period] = sum(trs[:period]) / period
    for i in range(period + 1, len(bars)):
        tr = trs[i-1]
        atr[i] = (atr[i-1] * (period - 1) + tr) / period
    return atr

def calc_vol_avg(bars, period=20):
    """Returns list of rolling volume average."""
    out = [None] * len(bars)
    for i in range(period - 1, len(bars)):
        out[i] = sum(bars[j]["v"] for j in range(i - period + 1, i + 1)) / period
    return out

def build_htf_ema(htf_bars, period=21):
    """Returns dict: timestamp -> (ema_val, prev_ema_val) for closed HTF bars."""
    closes = [b["c"] for b in htf_bars]
    emas = calc_ema(closes, period)
    result = {}
    for i in range(1, len(htf_bars)):
        if emas[i] is not None and emas[i-1] is not None:
            result[htf_bars[i]["t"]] = (emas[i], emas[i-1])
    return result

def get_htf_signal(htf_ema_map, ltf_ts, htf_period_ms):
    """
    Return 'up', 'down', or None for the HTF trend at ltf_ts.
    Offset by one full HTF period to avoid lookahead (only use closed HTF bars).
    """
    query_ts = ltf_ts - htf_period_ms
    # Find the latest HTF bar whose open_time <= query_ts
    best_ts = None
    best_val = None
    for ts, val in htf_ema_map.items():
        if ts <= query_ts:
            if best_ts is None or ts > best_ts:
                best_ts = ts
                best_val = val
    if best_val is None:
        return None
    ema_now, ema_prev = best_val
    if ema_now > ema_prev:
        return "up"
    elif ema_now < ema_prev:
        return "down"
    return None

# ── BACKTEST ENGINE ───────────────────────────────────────────────────────────

HTF_PERIOD_MS = {
    "4h": 4 * 60 * 60 * 1000,
    "1h": 60 * 60 * 1000,
}

def backtest_symbol(symbol, ltf_bars, htf_bars):
    """
    Run LER strategy on one symbol.
    Returns dict with trades list and filter_stats.
    """

    # Precompute indicators on LTF
    closes_ltf = [b["c"] for b in ltf_bars]
    rsi_vals   = calc_rsi(closes_ltf, RSI_PERIOD)
    atr_vals   = calc_atr(ltf_bars, ATR_PERIOD)
    vol_avgs   = calc_vol_avg(ltf_bars, VOL_AVG_PERIOD)

    # Precompute HTF EMA map
    htf_ema_map = build_htf_ema(htf_bars, EMA_PERIOD)
    htf_ms      = HTF_PERIOD_MS["4h"]

    trades = []
    filter_stats = {
        "total_candles":       0,
        "warmup_none":         0,
        "no_htf_signal":       0,
        "wick_ratio_fail":     0,
        "vol_spike_fail":      0,
        "vol_dropout_fail":    0,
        "rsi_divergence_fail": 0,
        "signal_generated":    0,
        "skipped_max_pos":     0,   # tracked at portfolio level, placeholder
    }

    # We need at least RSI_PERIOD + RSI_DIV_LOOKBACK bars warmup
    warmup = max(RSI_PERIOD, ATR_PERIOD, VOL_AVG_PERIOD) + RSI_DIV_LOOKBACK

    # We iterate up to len-2 because we need bar[i+1] for volume dropout
    # and bar[i+2] for entry (open of bar after confirmation)
    open_positions = []   # list of dicts

    for i in range(warmup, len(ltf_bars) - 2):
        filter_stats["total_candles"] += 1

        bar   = ltf_bars[i]
        bar1  = ltf_bars[i + 1]   # confirmation candle (vol dropout)
        bar2  = ltf_bars[i + 2]   # entry candle (entry at open)

        # ── Close any open positions for THIS symbol ──────────────────────
        still_open = []
        for pos in open_positions:
            entry_bar_idx = pos["entry_bar_idx"]
            bars_held = i - entry_bar_idx
            ep = pos["entry_price"]
            sl = pos["sl"]
            tp = pos["tp"]
            direction = pos["direction"]

            # Check if current bar hits TP or SL
            if direction == "long":
                if bar["l"] <= sl:
                    pos["exit_price"] = sl
                    pos["exit_ts"]    = bar["t"]
                    pos["result"]     = "sl"
                    trades.append(pos)
                    continue
                elif bar["h"] >= tp:
                    pos["exit_price"] = tp
                    pos["exit_ts"]    = bar["t"]
                    pos["result"]     = "tp"
                    trades.append(pos)
                    continue
            else:  # short
                if bar["h"] >= sl:
                    pos["exit_price"] = sl
                    pos["exit_ts"]    = bar["t"]
                    pos["result"]     = "sl"
                    trades.append(pos)
                    continue
                elif bar["l"] <= tp:
                    pos["exit_price"] = tp
                    pos["exit_ts"]    = bar["t"]
                    pos["result"]     = "tp"
                    trades.append(pos)
                    continue

            # Max hold check
            if bars_held >= MAX_HOLD_BARS:
                pos["exit_price"] = bar["c"]
                pos["exit_ts"]    = bar["t"]
                pos["result"]     = "timeout"
                trades.append(pos)
                continue

            still_open.append(pos)
        open_positions = still_open

        # ── FILTER 1: Warmup check ────────────────────────────────────────
        if (rsi_vals[i] is None or rsi_vals[i-RSI_DIV_LOOKBACK] is None or
                atr_vals[i] is None or vol_avgs[i] is None):
            filter_stats["warmup_none"] += 1
            continue

        # ── FILTER 2: HTF trend ───────────────────────────────────────────
        htf_dir = get_htf_signal(htf_ema_map, bar["t"], htf_ms)
        if htf_dir is None:
            filter_stats["no_htf_signal"] += 1
            continue

        # ── Determine wick ratios ─────────────────────────────────────────
        o, h, l, c = bar["o"], bar["h"], bar["l"], bar["c"]
        body = abs(c - o)
        body = max(body, bar["l"] * 0.0001)  # avoid /0 on doji

        lower_wick = min(o, c) - l
        upper_wick = h - max(o, c)

        lower_ratio = lower_wick / body
        upper_ratio = upper_wick / body

        # ── FILTER 3: Wick ratio ──────────────────────────────────────────
        long_candidate  = htf_dir == "up"   and lower_ratio >= WICK_RATIO_MIN
        short_candidate = htf_dir == "down" and upper_ratio >= WICK_RATIO_MIN

        if not long_candidate and not short_candidate:
            filter_stats["wick_ratio_fail"] += 1
            continue

        # ── FILTER 4: Volume spike on wick candle ─────────────────────────
        vol_avg = vol_avgs[i]
        if bar["v"] < VOL_SPIKE_MULT * vol_avg:
            filter_stats["vol_spike_fail"] += 1
            continue

        # ── FILTER 5: Volume dropout on next candle ───────────────────────
        if bar1["v"] >= bar["v"]:
            filter_stats["vol_dropout_fail"] += 1
            continue

        # ── FILTER 6: RSI divergence ──────────────────────────────────────
        rsi_now  = rsi_vals[i]
        rsi_prev = rsi_vals[i - RSI_DIV_LOOKBACK]

        if long_candidate:
            # Bullish div: price made lower low, RSI made higher reading
            price_lower_low = bar["l"] < ltf_bars[i - RSI_DIV_LOOKBACK]["l"]
            rsi_higher      = rsi_now > rsi_prev
            if not (price_lower_low and rsi_higher):
                filter_stats["rsi_divergence_fail"] += 1
                continue
        else:
            # Bearish div: price made higher high, RSI made lower reading
            price_higher_high = bar["h"] > ltf_bars[i - RSI_DIV_LOOKBACK]["h"]
            rsi_lower         = rsi_now < rsi_prev
            if not (price_higher_high and rsi_lower):
                filter_stats["rsi_divergence_fail"] += 1
                continue

        filter_stats["signal_generated"] += 1

        # ── BUILD TRADE ───────────────────────────────────────────────────
        direction  = "long" if long_candidate else "short"
        entry_px   = bar2["o"]
        atr_val    = atr_vals[i]

        if direction == "long":
            sl = bar["l"] - SL_ATR_BUFFER * atr_val
            risk = entry_px - sl
            if risk <= 0:
                continue
            tp = entry_px + TP_R * risk
        else:
            sl = bar["h"] + SL_ATR_BUFFER * atr_val
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

    # Close any remaining open positions at last bar close
    if ltf_bars:
        last = ltf_bars[-1]
        for pos in open_positions:
            pos["exit_price"] = last["c"]
            pos["exit_ts"]    = last["t"]
            pos["result"]     = "end_of_data"
            trades.append(pos)

    return trades, filter_stats

# ── PORTFOLIO SIMULATION ──────────────────────────────────────────────────────

def simulate_portfolio(all_symbol_trades):
    """
    Merge all symbol trades, apply portfolio-level position cap,
    simulate equity curve, compute PnL per trade.
    """
    # Flatten and sort by entry_ts
    all_trades = []
    for trades in all_symbol_trades.values():
        all_trades.extend(trades)
    all_trades.sort(key=lambda x: x["entry_ts"])

    equity    = CAPITAL_START
    open_pos  = []   # currently active trades (by entry_ts order)
    results   = []

    for trade in all_trades:
        # Remove positions that exited before this trade's entry
        still_open = [p for p in open_pos if p["exit_ts"] > trade["entry_ts"]]
        open_pos   = still_open

        if len(open_pos) >= MAX_POSITIONS:
            trade["result_detail"] = "skipped_max_pos"
            results.append(trade)
            continue

        # Size the trade based on current equity
        risk_amt   = equity * RISK_PCT
        qty        = risk_amt / trade["risk_per_unit"]
        total_fee  = (FEE_SIDE + SLIPPAGE_SIDE) * 2 * trade["entry_price"] * qty

        result = trade["result"]
        ep     = trade["entry_price"]
        xp     = trade["exit_price"]

        if trade["direction"] == "long":
            gross_pnl = (xp - ep) * qty
        else:
            gross_pnl = (ep - xp) * qty

        net_pnl = gross_pnl - total_fee
        equity += net_pnl

        trade["qty"]        = qty
        trade["gross_pnl"]  = gross_pnl
        trade["net_pnl"]    = net_pnl
        trade["equity_after"] = equity
        trade["result_detail"] = result

        open_pos.append(trade)
        results.append(trade)

    return results, equity

# ── STATS ─────────────────────────────────────────────────────────────────────

def compute_stats(results):
    executed = [t for t in results if t.get("result_detail") not in ("skipped_max_pos",)]
    skipped  = [t for t in results if t.get("result_detail") == "skipped_max_pos"]

    if not executed:
        return None

    pnls    = [t["net_pnl"] for t in executed]
    wins    = [p for p in pnls if p > 0]
    losses  = [p for p in pnls if p <= 0]

    total_trades = len(executed)
    win_count    = len(wins)
    win_rate     = win_count / total_trades if total_trades else 0

    gross_profit = sum(wins)   if wins   else 0
    gross_loss   = abs(sum(losses)) if losses else 0
    pf           = gross_profit / gross_loss if gross_loss > 0 else float("inf")

    net_pnl      = sum(pnls)
    avg_win      = gross_profit / win_count  if win_count  else 0
    avg_loss     = gross_loss   / len(losses) if losses     else 0
    expectancy   = (win_rate * avg_win) - ((1 - win_rate) * avg_loss)

    # Drawdown
    equity_curve = [CAPITAL_START] + [t["equity_after"] for t in executed]
    peak    = CAPITAL_START
    max_dd  = 0
    for e in equity_curve:
        if e > peak:
            peak = e
        dd = (peak - e) / peak
        if dd > max_dd:
            max_dd = dd

    # Sharpe / Sortino (daily returns approximation using 30m bars → 48 bars/day)
    daily_pnls = {}
    for t in executed:
        day = t["entry_ts"] // (86400 * 1000)
        daily_pnls.setdefault(day, 0)
        daily_pnls[day] += t["net_pnl"]
    dr = list(daily_pnls.values())
    if len(dr) > 1:
        mean_r = sum(dr) / len(dr)
        std_r  = math.sqrt(sum((x - mean_r)**2 for x in dr) / len(dr))
        sharpe = (mean_r / std_r * math.sqrt(365)) if std_r > 0 else 0
        down_r = [x for x in dr if x < 0]
        std_d  = math.sqrt(sum(x**2 for x in down_r) / len(down_r)) if down_r else 0
        sortino = (mean_r / std_d * math.sqrt(365)) if std_d > 0 else 0
    else:
        sharpe = sortino = 0

    # Win/loss streaks
    streak_win = streak_loss = cur_w = cur_l = 0
    for p in pnls:
        if p > 0:
            cur_w += 1; cur_l = 0
        else:
            cur_l += 1; cur_w = 0
        streak_win  = max(streak_win,  cur_w)
        streak_loss = max(streak_loss, cur_l)

    # Long/short split
    longs  = [t for t in executed if t["direction"] == "long"]
    shorts = [t for t in executed if t["direction"] == "short"]
    long_wr  = len([t for t in longs  if t["net_pnl"] > 0]) / len(longs)  if longs  else 0
    short_wr = len([t for t in shorts if t["net_pnl"] > 0]) / len(shorts) if shorts else 0

    # Avg duration (in bars, 30m each)
    durations = []
    for t in executed:
        if t.get("exit_ts") and t.get("entry_ts"):
            bars = (t["exit_ts"] - t["entry_ts"]) // (30 * 60 * 1000)
            durations.append(bars)
    avg_dur_bars = sum(durations) / len(durations) if durations else 0
    avg_dur_h    = avg_dur_bars * 0.5

    return {
        "total_trades":    total_trades,
        "skipped_max_pos": len(skipped),
        "win_rate":        round(win_rate * 100, 2),
        "profit_factor":   round(pf, 4),
        "net_pnl":         round(net_pnl, 2),
        "gross_profit":    round(gross_profit, 2),
        "gross_loss":      round(gross_loss, 2),
        "max_drawdown_pct": round(max_dd * 100, 2),
        "sharpe":          round(sharpe, 3),
        "sortino":         round(sortino, 3),
        "avg_win":         round(avg_win, 2),
        "avg_loss":        round(avg_loss, 2),
        "expectancy":      round(expectancy, 2),
        "avg_duration_h":  round(avg_dur_h, 1),
        "long_count":      len(longs),
        "short_count":     len(shorts),
        "long_wr_pct":     round(long_wr * 100, 2),
        "short_wr_pct":    round(short_wr * 100, 2),
        "max_win_streak":  streak_win,
        "max_loss_streak": streak_loss,
        "final_equity":    round(CAPITAL_START + net_pnl, 2),
    }

def per_coin_stats(all_symbol_trades):
    rows = []
    for sym, trades in all_symbol_trades.items():
        executed = [t for t in trades if t.get("exit_price") is not None]
        if not executed:
            rows.append({"symbol": sym, "trades": 0, "pf": 0, "wr": 0, "net_pnl": 0})
            continue
        pnls = [t.get("net_pnl", 0) for t in executed if "net_pnl" in t]
        if not pnls:
            rows.append({"symbol": sym, "trades": len(executed), "pf": 0, "wr": 0, "net_pnl": 0})
            continue
        wins   = [p for p in pnls if p > 0]
        losses = [p for p in pnls if p <= 0]
        gp     = sum(wins)
        gl     = abs(sum(losses))
        pf     = gp / gl if gl > 0 else float("inf")
        wr     = len(wins) / len(pnls) * 100
        rows.append({
            "symbol":   sym,
            "trades":   len(pnls),
            "pf":       round(pf, 4),
            "wr":       round(wr, 2),
            "net_pnl":  round(sum(pnls), 2),
        })
    rows.sort(key=lambda x: x["pf"], reverse=True)
    return rows

def monthly_pnl(results):
    mp = {}
    for t in results:
        if "net_pnl" not in t:
            continue
        ts  = t["entry_ts"]
        dt  = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
        key = f"{dt.year}-{dt.month:02d}"
        mp.setdefault(key, 0)
        mp[key] += t["net_pnl"]
    return {k: round(v, 2) for k, v in sorted(mp.items())}

# ── WORKER ────────────────────────────────────────────────────────────────────

def process_symbol(symbol):
    """Download data + run backtest for one symbol. Returns (symbol, trades, filter_stats, status)."""
    try:
        ltf_bars, skip_ltf = load_symbol_data(
            symbol, INTERVAL_LTF, START_YEAR, START_MONTH, END_YEAR, END_MONTH)
        htf_bars, skip_htf = load_symbol_data(
            symbol, INTERVAL_HTF, START_YEAR, START_MONTH, END_YEAR, END_MONTH)

        if len(ltf_bars) < 200:
            return symbol, [], {}, f"insufficient_data({len(ltf_bars)} bars)"
        if len(htf_bars) < 50:
            return symbol, [], {}, f"insufficient_htf_data({len(htf_bars)} bars)"

        trades, fstats = backtest_symbol(symbol, ltf_bars, htf_bars)
        return symbol, trades, fstats, "ok"
    except Exception as e:
        return symbol, [], {}, f"error:{e}"

# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    print("=" * 65)
    print("  LER v2 — Liquidity Exhaustion Reversal Backtest (tuned)")
    print(f"  Period : {START_YEAR}-{START_MONTH:02d} → {END_YEAR}-{END_MONTH:02d}")
    print(f"  Coins  : {len(SYMBOLS)}")
    print(f"  Workers: {MAX_WORKERS}")
    print("=" * 65)

    all_symbol_trades  = {}
    all_filter_stats   = {}
    symbol_statuses    = {}

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_symbol, sym): sym for sym in SYMBOLS}
        done_count = 0
        for future in as_completed(futures):
            sym = futures[future]
            symbol, trades, fstats, status = future.result()
            done_count += 1
            all_symbol_trades[symbol] = trades
            all_filter_stats[symbol]  = fstats
            symbol_statuses[symbol]   = status
            tag = f"[{len(trades):3d} trades]" if status == "ok" else f"[{status}]"
            print(f"  [{done_count:3d}/{len(SYMBOLS)}] {symbol:<22} {tag}")

    print("\nRunning portfolio simulation...")
    results, final_equity = simulate_portfolio(all_symbol_trades)

    print("Computing stats...")
    agg    = compute_stats(results)
    pc     = per_coin_stats(all_symbol_trades)
    monthly = monthly_pnl(results)

    # Aggregate filter stats
    total_fstats = {}
    for sym, fs in all_filter_stats.items():
        for k, v in fs.items():
            total_fstats[k] = total_fstats.get(k, 0) + v

    # ── PRINT SUMMARY ─────────────────────────────────────────────────────────
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
        print(f"  Max Win Streak    : {agg['max_win_streak']}")
        print(f"  Max Loss Streak   : {agg['max_loss_streak']}")
        pf_ok = agg["profit_factor"] >= PROFIT_FACTOR_TARGET
        wr_ok = agg["win_rate"] >= WIN_RATE_TARGET * 100
        verdict = "✅ MEETS TARGETS" if (pf_ok and wr_ok) else "❌ BELOW TARGETS"
        print(f"\n  VERDICT: {verdict}")
    else:
        print("  No trades executed.")

    print("\n" + "=" * 65)
    print("  PER-COIN TABLE (sorted by PF, top 30)")
    print("=" * 65)
    print(f"  {'Symbol':<22} {'Trades':>6} {'PF':>7} {'WR%':>7} {'NetPnL':>10}")
    print("  " + "-" * 57)
    for row in pc[:30]:
        pf_str = f"{row['pf']:.4f}" if row['pf'] != float("inf") else "∞"
        print(f"  {row['symbol']:<22} {row['trades']:>6} {pf_str:>7} {row['wr']:>6.2f}% ${row['net_pnl']:>9,.2f}")

    print("\n" + "=" * 65)
    print("  MONTHLY PnL BREAKDOWN")
    print("=" * 65)
    for month, pnl in monthly.items():
        bar_len = int(abs(pnl) / max(abs(v) for v in monthly.values()) * 30) if monthly else 0
        bar_str = ("█" * bar_len) if pnl >= 0 else ("░" * bar_len)
        sign    = "+" if pnl >= 0 else ""
        print(f"  {month}  {sign}${pnl:>9,.2f}  {bar_str}")

    print("\n" + "=" * 65)
    print("  FILTER REJECTION STATS (all coins combined)")
    print("=" * 65)
    total_c = total_fstats.get("total_candles", 1)
    for k, v in total_fstats.items():
        pct = v / total_c * 100 if total_c else 0
        print(f"  {k:<28} {v:>8,}  ({pct:5.1f}%)")

    # ── WRITE OUTPUTS ──────────────────────────────────────────────────────────
    with open("backtest_summary.txt", "w") as f:
        f.write(f"LER v2 Backtest Summary — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}\n")
        f.write("=" * 65 + "\n")
        if agg:
            for k, v in agg.items():
                f.write(f"  {k}: {v}\n")
        f.write("\nPer-Coin Results (all):\n")
        f.write(f"{'Symbol':<22} {'Trades':>6} {'PF':>8} {'WR%':>7} {'NetPnL':>12}\n")
        for row in pc:
            pf_str = f"{row['pf']:.4f}" if row['pf'] != float("inf") else "inf"
            f.write(f"{row['symbol']:<22} {row['trades']:>6} {pf_str:>8} {row['wr']:>6.2f}% ${row['net_pnl']:>11,.2f}\n")
        f.write("\nMonthly PnL:\n")
        for month, pnl in monthly.items():
            f.write(f"  {month}: ${pnl:,.2f}\n")
        f.write("\nFilter Stats:\n")
        for k, v in total_fstats.items():
            f.write(f"  {k}: {v}\n")

    report = {
        "meta": {
            "strategy":   "LER v2 — Liquidity Exhaustion Reversal (tuned)",
            "period":     f"{START_YEAR}-{START_MONTH:02d} to {END_YEAR}-{END_MONTH:02d}",
            "ltf":        INTERVAL_LTF,
            "htf":        INTERVAL_HTF,
            "coins":      SYMBOLS,
            "settings": {
                "capital":       CAPITAL_START,
                "risk_pct":      RISK_PCT,
                "fee_side":      FEE_SIDE,
                "slippage_side": SLIPPAGE_SIDE,
                "max_positions": MAX_POSITIONS,
                "tp_r":          TP_R,
                "max_hold_bars": MAX_HOLD_BARS,
            },
            "ler_params": {
                "wick_ratio_min":  WICK_RATIO_MIN,
                "vol_spike_mult":  VOL_SPIKE_MULT,
                "rsi_period":      RSI_PERIOD,
                "ema_period":      EMA_PERIOD,
                "atr_period":      ATR_PERIOD,
                "vol_avg_period":  VOL_AVG_PERIOD,
                "sl_atr_buffer":   SL_ATR_BUFFER,
            }
        },
        "aggregate":    agg,
        "per_coin":     pc,
        "monthly_pnl":  monthly,
        "filter_stats": total_fstats,
        "symbol_statuses": symbol_statuses,
        "trades": [
            {k: v for k, v in t.items() if k != "entry_bar_idx"}
            for t in results
        ]
    }

    with open("backtest_report.json", "w") as f:
        json.dump(report, f, indent=2, default=str)

    print("\n  Outputs written: backtest_summary.txt + backtest_report.json")
    print("=" * 65)

if __name__ == "__main__":
    main()
