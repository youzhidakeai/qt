#!/usr/bin/env python3
"""检验: dip 入场时刻的资金费率能否预测该笔交易的盈亏.
分桶: 入场时该币最近一期费率的年化 (<0 / 0~11%基线 / 11~50% / >50%热).
交易模拟与 backtest_pyramid 的 dip/3%/不加仓 完全同口径."""
import json, os
import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
KDIR = os.path.join(HERE, 'data', 'klines_1m')
FCACHE = os.path.join(HERE, 'data', 'funding_hist_cache.json')
FEE, SLIP, NOTIONAL, TRAIL, MAX_HOLD, COOLDOWN = 0.0005, 0.0010, 1500.0, 3.0, 1440, 60

funding = {s: sorted((int(t), float(r)) for t, r in v) for s, v in json.load(open(FCACHE)).items()}

def last_funding_annual(sym, ms):
    ev = funding.get(sym)
    if not ev:
        return None
    lo, hi, ans = 0, len(ev) - 1, None
    while lo <= hi:
        mid = (lo + hi) // 2
        if ev[mid][0] <= ms:
            ans = ev[mid][1]; lo = mid + 1
        else:
            hi = mid - 1
    return None if ans is None else ans * 3 * 365 * 100

def load(sym):
    df = pd.read_csv(os.path.join(KDIR, f"{sym}.csv.gz"),
                     usecols=['open_time', 'open', 'high', 'low', 'close', 'quote_volume'])
    df = df.drop_duplicates('open_time').sort_values('open_time').reset_index(drop=True)
    if len(df) < 1500:
        return None
    c, qv = df['close'], df['quote_volume']
    v5 = qv.rolling(5).sum()
    df['vol_surge'] = v5 / v5.rolling(1440, min_periods=288).median().replace(0, np.nan)
    df['vol24h'] = qv.rolling(1440, min_periods=288).sum() / 1e6
    df['ret_5m'] = (c / c.shift(5) - 1) * 100
    df['ret_15m'] = (c / c.shift(15) - 1) * 100
    df['ret_60m'] = (c / c.shift(60) - 1) * 100
    df['dist_24h_high'] = (c / df['high'].rolling(1440, min_periods=288).max() - 1) * 100
    df['sig'] = (df['ret_60m'] >= 3) & (df['vol_surge'] >= 6) & (df['vol24h'] >= 150) \
        & (df['dist_24h_high'] <= -3) & (df['ret_5m'] >= 0.5) & (df['ret_15m'] >= 1.0)
    return df

def simulate(df, sym):
    o, h, l, c, t = df['open'].values, df['high'].values, df['low'].values, df['close'].values, df['open_time'].values
    trades, blocked = [], -1
    for i in np.flatnonzero(df['sig'].values):
        j = i + 1
        if j >= len(df) or j <= blocked:
            continue
        entry = o[j] * (1 + SLIP)
        peak, exit_px, exit_k = entry, None, None
        for k in range(j, min(j + MAX_HOLD, len(df))):
            tp = peak * (1 - TRAIL / 100)
            if l[k] <= tp:
                exit_px, exit_k = min(tp, o[k]) * (1 - SLIP), k
                break
            peak = max(peak, h[k])
        if exit_px is None:
            exit_k = min(j + MAX_HOLD, len(df)) - 1
            exit_px = c[exit_k] * (1 - SLIP)
        gross = NOTIONAL * (exit_px / entry - 1)
        fees = NOTIONAL * FEE + NOTIONAL * (exit_px / entry) * FEE
        fa = last_funding_annual(sym, int(t[j]))
        trades.append({'net': gross - fees, 'fund_annual': fa})
        blocked = exit_k + COOLDOWN
    return trades

all_trades = []
syms = sorted(f[:-7] for f in os.listdir(KDIR) if f.endswith('.csv.gz'))
for n, sym in enumerate(syms):
    try:
        df = load(sym)
    except Exception:
        continue
    if df is None:
        continue
    all_trades.extend(simulate(df, sym))
    if (n + 1) % 200 == 0:
        print(f"  ... {n+1}/{len(syms)}")

d = pd.DataFrame(all_trades)
has_f = d[d['fund_annual'].notna()]
print(f"\ndip 信号交易 {len(d)} 笔, 其中 {len(has_f)} 笔有费率数据")
bins = [(-1e9, 0, "负费率 (空头拥挤)"), (0, 11, "0~11% (基线附近)"), (11, 50, "11~50% (偏热)"), (50, 1e9, ">50% (过热)")]
print(f"{'入场时费率年化':<18}{'笔数':>5}{'胜率':>7}{'盈利因子':>8}{'净U':>8}{'均U/笔':>8}")
for lo, hi, label in bins:
    b = has_f[(has_f['fund_annual'] > lo) & (has_f['fund_annual'] <= hi)]
    if len(b) == 0:
        print(f"{label:<18}{'0':>5}")
        continue
    wins = b[b['net'] > 0]
    losses = b[b['net'] <= 0]
    pf = wins['net'].sum() / abs(losses['net'].sum()) if len(losses) and losses['net'].sum() != 0 else float('inf')
    print(f"{label:<18}{len(b):>5}{100*len(wins)/len(b):>6.1f}%{pf:>8.2f}{b['net'].sum():>+8.0f}{b['net'].mean():>+8.1f}")
