#!/usr/bin/env python3
"""追高+移动止损+金字塔加仓 策略回测 (多配置单趟数据).

变体:
  chase: 突破 4 小时新高 + 爆量 + 热点币 → 买 (用户描述的"追高")
  dip:   热点币 1h 强拉升但离 24h 高点尚有距离 → 买 (画像里赢单的形态)
出场: 从持仓期最高点回撤 trail% 即全平 (无固定止盈); 24h 时间保险丝
加仓: 每上涨 add_step% 加一个单位, 最多 max_adds 次; 总名义固定 1500U 均分到各单位
成本: taker 0.05%/边 + 滑点 0.10%/边
"""
import os
import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
KDIR = os.path.join(HERE, 'data', 'klines_1m')
FEE, SLIP = 0.0005, 0.0010
TOTAL_NOTIONAL = 1500.0
MAX_HOLD = 1440   # 24h 保险丝
COOLDOWN = 60
MAX_POS = 2

CONFIGS = []
for variant in ('chase', 'dip'):
    for trail in (2.0, 3.0, 5.0):
        for max_adds in (0, 2):
            CONFIGS.append({'variant': variant, 'trail': trail, 'max_adds': max_adds, 'add_step': 2.5})


def load_features(sym):
    df = pd.read_csv(os.path.join(KDIR, f"{sym}.csv.gz"),
                     usecols=['open_time', 'open', 'high', 'low', 'close', 'quote_volume'])
    df = df.drop_duplicates('open_time').sort_values('open_time').reset_index(drop=True)
    if len(df) < 1500:
        return None
    c, qv = df['close'], df['quote_volume']
    df['ret_5m'] = (c / c.shift(5) - 1) * 100
    df['ret_15m'] = (c / c.shift(15) - 1) * 100
    df['ret_60m'] = (c / c.shift(60) - 1) * 100
    v5 = qv.rolling(5).sum()
    df['vol_surge'] = v5 / v5.rolling(1440, min_periods=288).median().replace(0, np.nan)
    df['vol24h'] = qv.rolling(1440, min_periods=288).sum() / 1e6
    df['dist_24h_high'] = (c / df['high'].rolling(1440, min_periods=288).max() - 1) * 100
    prev_4h_max = df['high'].shift(1).rolling(240, min_periods=240).max()
    brk = (c >= prev_4h_max) & (df['vol_surge'] >= 3) & (df['vol24h'] >= 50) & (df['ret_5m'] >= 0.3)
    df['sig_chase'] = brk & ~brk.shift(1, fill_value=False)   # 只取突破的第一根
    df['sig_dip'] = (df['ret_60m'] >= 3) & (df['vol_surge'] >= 6) & (df['vol24h'] >= 150) \
        & (df['dist_24h_high'] <= -3) & (df['ret_5m'] >= 0.5) & (df['ret_15m'] >= 1.0)
    return df


def simulate(df, sig_col, trail, max_adds, add_step):
    o, h, lo, c = df['open'].values, df['high'].values, df['low'].values, df['close'].values
    t = df['open_time'].values
    sig_idx = np.flatnonzero(df[sig_col].values)
    unit = TOTAL_NOTIONAL / (1 + max_adds)
    trades, blocked_until = [], -1
    for i in sig_idx:
        j = i + 1
        if j >= len(df) or j <= blocked_until:
            continue
        entry = o[j] * (1 + SLIP)
        fills = [entry]
        peak = entry
        next_trigger = entry * (1 + add_step / 100)
        exit_px, exit_k = None, None
        for k in range(j, min(j + MAX_HOLD, len(df))):
            # 悲观次序: 先用截至上一根的最高点判断回撤离场
            trail_px = peak * (1 - trail / 100)
            if lo[k] <= trail_px:
                px = min(trail_px, o[k])   # 跳空低开按开盘价成交
                exit_px, exit_k = px * (1 - SLIP), k
                break
            while len(fills) < 1 + max_adds and h[k] >= next_trigger:
                fills.append(max(next_trigger, o[k]) * (1 + SLIP))
                next_trigger = fills[-1] * (1 + add_step / 100)
            peak = max(peak, h[k])
        if exit_px is None:
            exit_k = min(j + MAX_HOLD, len(df)) - 1
            exit_px = c[exit_k] * (1 - SLIP)
        gross = sum(unit * (exit_px / f - 1) for f in fills)
        fees = sum(unit * FEE for f in fills) + sum(unit * (exit_px / f) * FEE for f in fills)
        trades.append({'entry_t': int(t[j]), 'exit_t': int(t[exit_k]),
                       'net_usdt': gross - fees, 'n_fills': len(fills)})
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
    results = {id(cfg): [] for cfg in CONFIGS}
    for n, sym in enumerate(syms):
        try:
            df = load_features(sym)
        except Exception:
            continue
        if df is None:
            continue
        for cfg in CONFIGS:
            results[id(cfg)].extend(
                simulate(df, f"sig_{cfg['variant']}", cfg['trail'], cfg['max_adds'], cfg['add_step']))
        if (n + 1) % 100 == 0:
            print(f"  ... {n+1}/{len(syms)}")

    print(f"\n{'变体':<6s} {'回撤%':>5s} {'加仓':>4s} | {'笔数':>5s} {'胜率':>6s} {'盈利因子':>7s} {'净U':>8s} | {'前半净':>8s} {'后半净':>8s}")
    rows = []
    for cfg in CONFIGS:
        taken = portfolio_filter(results[id(cfg)])
        if not taken:
            continue
        d = pd.DataFrame(taken)
        wins, losses = d[d['net_usdt'] > 0], d[d['net_usdt'] <= 0]
        pf = abs(wins['net_usdt'].sum() / losses['net_usdt'].sum()) if len(losses) and losses['net_usdt'].sum() != 0 else float('inf')
        mid = d['entry_t'].median()
        h1, h2 = d[d['entry_t'] < mid]['net_usdt'].sum(), d[d['entry_t'] >= mid]['net_usdt'].sum()
        rows.append((cfg, len(d), 100 * len(wins) / len(d), pf, d['net_usdt'].sum(), h1, h2))
    for cfg, n, wr, pf, net, h1, h2 in sorted(rows, key=lambda r: -r[4]):
        print(f"{cfg['variant']:<6s} {cfg['trail']:5.1f} {cfg['max_adds']:4d} | {n:5d} {wr:5.1f}% {pf:7.2f} {net:+8.0f} | {h1:+8.0f} {h2:+8.0f}")
    print(f"\n共测试 {len(rows)} 个配置 (多重检验警告: 配置越多, 最好那行越可能是运气)")


if __name__ == '__main__':
    main()
