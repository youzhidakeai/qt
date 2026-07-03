#!/usr/bin/env python3
"""两阶段入场回测: 突破只观察 → 等小回踩 → 收回 EMA7 确认后进场.

对应外部建议的 FOMO_BREAKOUT_WATCH → FOMO_WAIT_PULLBACK → FOMO_MICRO_RECLAIM.
本版只测价格结构核心 (无 OI/funding 过滤, K线数据里没有 OI 历史):
  1. 突破: 价格创 4h 新高 + 爆量 (与 backtest_pyramid 的 chase 同口径)
  2. 观察: 突破后 60 分钟内等待从局部高点回踩 pb%; 若已拉超 4% 直接放弃 (高位禁追)
  3. 确认: 回踩后 30 分钟内收盘价重新站上 EMA7 且为阳线 → 次根开盘进场
出场与 dip 基线完全同口径: 3% 高点回撤 + 24h 保险丝, 成本 taker+滑点 0.30% 双边.
对照基准 (同数据): dip/3%/不加仓 = 113 笔 PF 1.02 +42U; chase 全亏.
"""
import os
import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
KDIR = os.path.join(HERE, 'data', 'klines_1m')
FEE, SLIP = 0.0005, 0.0010
NOTIONAL = 1500.0
TRAIL = 3.0
MAX_HOLD = 1440
COOLDOWN = 60
MAX_POS = 2
WATCH_MIN = 60      # 突破后等回踩的窗口
RECLAIM_MIN = 30    # 回踩后等收回 EMA7 的窗口
OVEREXT_CAP = 4.0   # 突破后已拉超 4% → 高位禁追

CONFIGS = [{'pb': 0.5}, {'pb': 1.0}]   # 回踩深度%; 刻意只测 2 个, 防多重检验


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
    prev_4h_max = df['high'].shift(1).rolling(240, min_periods=240).max()
    brk = (c >= prev_4h_max) & (df['vol_surge'] >= 3) & (df['vol24h'] >= 50) & (df['ret_5m'] >= 0.3)
    df['sig_brk'] = brk & ~brk.shift(1, fill_value=False)
    df['ema7'] = c.ewm(span=7, adjust=False).mean()
    return df


def find_entries(df, pb):
    o, h, l, c = df['open'].values, df['high'].values, df['low'].values, df['close'].values
    ema7 = df['ema7'].values
    entries = []
    for i in np.flatnonzero(df['sig_brk'].values):
        run_high, entered = c[i], False
        k_end = min(i + WATCH_MIN, len(df) - 1)
        for k in range(i + 1, k_end):
            run_high = max(run_high, h[k])
            if run_high / c[i] - 1 > OVEREXT_CAP / 100:
                break                                   # 拉太远, 高位禁追
            if l[k] <= run_high * (1 - pb / 100):       # 回踩达标 → 等确认
                for m in range(k, min(k + RECLAIM_MIN, len(df) - 1)):
                    if c[m] > ema7[m] and c[m] > o[m]:  # 收回 EMA7 且阳线
                        entries.append(m + 1)           # 次根开盘进场
                        entered = True
                        break
                break
        if entered:
            continue
    return entries


def simulate(df, entry_idxs):
    o, h, l, c, t = df['open'].values, df['high'].values, df['low'].values, df['close'].values, df['open_time'].values
    trades, blocked_until = [], -1
    for j in entry_idxs:
        if j >= len(df) or j <= blocked_until:
            continue
        entry = o[j] * (1 + SLIP)
        peak = entry
        exit_px, exit_k = None, None
        for k in range(j, min(j + MAX_HOLD, len(df))):
            trail_px = peak * (1 - TRAIL / 100)
            if l[k] <= trail_px:
                exit_px, exit_k = min(trail_px, o[k]) * (1 - SLIP), k
                break
            peak = max(peak, h[k])
        if exit_px is None:
            exit_k = min(j + MAX_HOLD, len(df)) - 1
            exit_px = c[exit_k] * (1 - SLIP)
        gross = NOTIONAL * (exit_px / entry - 1)
        fees = NOTIONAL * FEE + NOTIONAL * (exit_px / entry) * FEE
        trades.append({'entry_t': int(t[j]), 'exit_t': int(t[exit_k]), 'net_usdt': gross - fees})
        blocked_until = exit_k + COOLDOWN
    return trades


def portfolio_filter(trades):
    trades.sort(key=lambda x: x['entry_t'])
    taken, open_exits = [], []
    for tr in trades:
        open_exits = [e for e in open_exits if e > tr['entry_t']]
        if len(open_exits) >= MAX_POS:
            continue
        open_exits.append(tr['exit_t'])
        taken.append(tr)
    return taken


def main():
    syms = sorted(f[:-7] for f in os.listdir(KDIR) if f.endswith('.csv.gz'))
    results = {cfg['pb']: [] for cfg in CONFIGS}
    for n, sym in enumerate(syms):
        try:
            df = load(sym)
        except Exception:
            continue
        if df is None:
            continue
        for cfg in CONFIGS:
            results[cfg['pb']].extend(simulate(df, find_entries(df, cfg['pb'])))
        if (n + 1) % 100 == 0:
            print(f"  ... {n+1}/{len(syms)}")

    print(f"\n{'回踩%':>5s} | {'笔数':>5s} {'胜率':>6s} {'盈利因子':>7s} {'净U':>8s} | {'前半净':>8s} {'后半净':>8s}")
    for cfg in CONFIGS:
        taken = portfolio_filter(results[cfg['pb']])
        if not taken:
            print(f"{cfg['pb']:>5.1f} | 无成交")
            continue
        d = pd.DataFrame(taken).sort_values('entry_t').reset_index(drop=True)
        wins, losses = d[d['net_usdt'] > 0], d[d['net_usdt'] <= 0]
        pf = abs(wins['net_usdt'].sum() / losses['net_usdt'].sum()) if len(losses) and losses['net_usdt'].sum() != 0 else float('inf')
        half = len(d) // 2
        print(f"{cfg['pb']:>5.1f} | {len(d):>5d} {100*len(wins)/len(d):>5.1f}% {pf:>7.2f} {d['net_usdt'].sum():>+8.0f} | "
              f"{d['net_usdt'][:half].sum():>+8.0f} {d['net_usdt'][half:].sum():>+8.0f}")
    print("\n对照基准 (同数据同出场): dip/3%/不加仓 = 113 笔 | 胜率 37.2% | PF 1.02 | +42U")


if __name__ == '__main__':
    main()
