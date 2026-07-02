#!/usr/bin/env python3
"""拉盘短打策略回测器: 按显性规则回放全市场 1m 数据, 含真实成本与保镖纪律.

规则 (全部可调):
  入场: ret_5m ≥ --r5 且 vol_surge ≥ --vs 且 vol24h ≥ --v24 (百万U) 且 ret_60m ≤ --r60cap
  出场: 硬止损 --hard_sl%; 追踪止盈 (浮盈达 --trail_arm% 后回撤 --trail% 离场); 时间止损 --time_stop 分钟
  成本: taker 手续费 0.05%/边 + 滑点 --slip%/边
  组合: 最多 --max_pos 个并发仓位, 单仓名义 --notional U, 同币冷却 30 分钟

用法示例: python backtest.py --r5 3 --vs 8 --v24 5
"""
import argparse, gzip, json, os
import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(HERE, 'data')
KDIR = os.path.join(DATA, 'klines_1m')
FEE = 0.0005  # taker 单边

def load_symbol(sym):
    df = pd.read_csv(os.path.join(KDIR, f"{sym}.csv.gz"),
                     usecols=['open_time', 'open', 'high', 'low', 'close', 'quote_volume'])
    return df.drop_duplicates('open_time').sort_values('open_time').reset_index(drop=True)


def add_features(df):
    c = df['close']
    qv = df['quote_volume']
    df['ret_5m'] = (c / c.shift(5) - 1) * 100
    df['ret_15m'] = (c / c.shift(15) - 1) * 100
    df['ret_60m'] = (c / c.shift(60) - 1) * 100
    v5 = qv.rolling(5).sum()
    df['vol_surge'] = v5 / v5.rolling(1440, min_periods=288).median().replace(0, np.nan)
    df['vol24h'] = qv.rolling(1440, min_periods=288).sum() / 1e6
    df['dist_24h_high'] = (c / df['high'].rolling(1440, min_periods=288).max() - 1) * 100
    return df


def simulate_symbol(sym, a):
    """返回该币种所有候选交易 (假设都能成交), 组合约束后面统一处理."""
    df = load_symbol(sym)
    if len(df) < 1500:
        return []
    df = add_features(df)
    sig = (df['ret_5m'] >= a.r5) & (df['ret_15m'] >= a.r15) & (df['vol_surge'] >= a.vs) & (df['vol24h'] >= a.v24) \
        & (df['ret_60m'] >= a.r60min) & (df['ret_60m'] <= a.r60cap) & (df['dist_24h_high'] <= a.dh_max)
    sig_idx = np.flatnonzero(sig.values)
    if len(sig_idx) == 0:
        return []

    o = df['open'].values; h = df['high'].values; lo = df['low'].values; c = df['close'].values
    t = df['open_time'].values
    trades = []
    blocked_until = -1
    for i in sig_idx:
        j = i + 1  # 信号在第 i 根收盘确认, 下一根开盘吃单入场
        if j >= len(df) or j <= blocked_until:
            continue
        entry = o[j] * (1 + a.slip / 100)
        stop = entry * (1 - a.hard_sl / 100)
        peak = entry
        exit_px, exit_k, reason = None, None, None
        for k in range(j, min(j + a.time_stop + 1, len(df))):
            # 悲观次序: 先看是否打到硬止损
            if lo[k] <= stop:
                exit_px, exit_k, reason = stop * (1 - a.slip / 100), k, 'stop'
                break
            peak = max(peak, h[k])
            if peak >= entry * (1 + a.trail_arm / 100):
                trail_px = peak * (1 - a.trail / 100)
                if lo[k] <= trail_px:
                    exit_px, exit_k, reason = max(trail_px, stop) * (1 - a.slip / 100), k, 'trail'
                    break
            if k - j >= a.time_stop:
                exit_px, exit_k, reason = c[k] * (1 - a.slip / 100), k, 'time'
                break
        if exit_px is None:
            exit_k = min(j + a.time_stop, len(df) - 1)
            exit_px, reason = c[exit_k] * (1 - a.slip / 100), 'time'
        gross_ret = exit_px / entry - 1
        net_ret = gross_ret - 2 * FEE
        trades.append({'sym': sym, 'entry_t': int(t[j]), 'exit_t': int(t[exit_k]),
                       'net_ret': net_ret, 'reason': reason,
                       'net_usdt': net_ret * a.notional})
        blocked_until = exit_k + 30  # 同币冷却 30 分钟
    return trades


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--r5', type=float, default=0.0, help='5分钟涨幅下限 %%')
    ap.add_argument('--r15', type=float, default=-999.0, help='15分钟涨幅下限 %%')
    ap.add_argument('--vs', type=float, default=8.0, help='5分钟成交额爆量倍数下限')
    ap.add_argument('--v24', type=float, default=5.0, help='24h 成交额下限 (百万U)')
    ap.add_argument('--r60min', type=float, default=-999.0, help='60分钟涨幅下限 %%')
    ap.add_argument('--r60cap', type=float, default=999.0, help='60分钟涨幅上限 %% (防追天花板)')
    ap.add_argument('--dh_max', type=float, default=999.0, help='距24h高点上限 %% (如 -2 表示至少低于高点2%%)')
    ap.add_argument('--hard_sl', type=float, default=5.0)
    ap.add_argument('--trail_arm', type=float, default=3.0)
    ap.add_argument('--trail', type=float, default=2.0)
    ap.add_argument('--time_stop', type=int, default=30, help='分钟')
    ap.add_argument('--slip', type=float, default=0.10, help='单边滑点 %%')
    ap.add_argument('--notional', type=float, default=1500.0)
    ap.add_argument('--max_pos', type=int, default=2)
    args = ap.parse_args()

    all_trades = []
    syms = sorted(f[:-7] for f in os.listdir(KDIR) if f.endswith('.csv.gz'))
    for sym in syms:
        try:
            all_trades.extend(simulate_symbol(sym, args))
        except Exception as e:
            print(f"  ⚠️ {sym}: {e}")
    all_trades.sort(key=lambda x: x['entry_t'])

    # 组合约束: 最多 max_pos 个并发, 先到先得
    taken = []
    open_pos = []  # exit_t 列表
    for tr in all_trades:
        open_pos = [e for e in open_pos if e > tr['entry_t']]
        if len(open_pos) >= args.max_pos:
            continue
        open_pos.append(tr['exit_t'])
        taken.append(tr)

    df = pd.DataFrame(taken)
    if df.empty:
        print("没有产生任何交易, 放宽参数试试。")
        return
    df['day'] = pd.to_datetime(df['entry_t'], unit='ms', utc=True).dt.tz_convert('Asia/Shanghai').dt.strftime('%m-%d')

    print(f"参数: r5≥{args.r5}% vs≥{args.vs}x v24≥{args.v24}M r60cap≤{args.r60cap}% | SL{args.hard_sl}% 追踪{args.trail_arm}/{args.trail}% 时停{args.time_stop}m | 滑点{args.slip}% 名义{args.notional}U 并发{args.max_pos}")
    print(f"信号总数 {len(all_trades)}, 组合约束后实际成交 {len(df)}\n")

    def report(d, label):
        if d.empty:
            print(f"【{label}】无交易")
            return
        wins = d[d['net_usdt'] > 0]
        losses = d[d['net_usdt'] <= 0]
        pf = abs(wins['net_usdt'].sum() / losses['net_usdt'].sum()) if len(losses) and losses['net_usdt'].sum() != 0 else float('inf')
        print(f"【{label}】{len(d)} 笔 | 胜率 {100*len(wins)/len(d):.1f}% | 盈利因子 {pf:.2f} | "
              f"净 {d['net_usdt'].sum():+.0f}U | 单笔均值 {d['net_usdt'].mean():+.2f}U | "
              f"出场分布: {dict(d['reason'].value_counts())}")

    mid = df['entry_t'].median()
    report(df, '全时段')
    report(df[df['entry_t'] < mid], '前半段 (样本内视角)')
    report(df[df['entry_t'] >= mid], '后半段 (样本外视角)')

    print("\n逐日净盈亏:")
    for day, g in df.groupby('day'):
        print(f"  {day}: {g['net_usdt'].sum():+8.0f}U  ({len(g)} 笔)")

    print("\n最赚 5 笔 / 最亏 5 笔:")
    for _, r in pd.concat([df.nlargest(5, 'net_usdt'), df.nsmallest(5, 'net_usdt')]).iterrows():
        ts = pd.to_datetime(r['entry_t'], unit='ms', utc=True).tz_convert('Asia/Shanghai').strftime('%m-%d %H:%M')
        print(f"  {r['sym']:14s} {ts} {r['net_usdt']:+8.1f}U ({r['reason']})")

    df.to_csv(os.path.join(DATA, 'backtest_trades.csv'), index=False)
    print("\n明细已存 research/data/backtest_trades.csv")


if __name__ == '__main__':
    main()
