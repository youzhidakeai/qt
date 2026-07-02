#!/usr/bin/env python3
"""入场画像: 量化"你手动买入的那一刻, 市场长什么样", 并和全市场随机时刻对比.

用法: python profile_entries.py
依赖: 先跑 download_klines.py; research/data/round_trips.json (真实成交重建的回合)
"""
import gzip, json, os, random
import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(HERE, 'data')
KDIR = os.path.join(DATA, 'klines_1m')
CUTOFF_MS = 1782748800000  # 2026-06-22 00:00 UTC+8
random.seed(42)

FEATURES = ['ret_3m', 'ret_5m', 'ret_15m', 'ret_60m', 'vol_surge', 'vol24h_musdt', 'dist_24h_high']


def load_symbol(sym):
    path = os.path.join(KDIR, f"{sym}.csv.gz")
    if not os.path.exists(path):
        return None
    df = pd.read_csv(path, usecols=['open_time', 'open', 'high', 'low', 'close', 'quote_volume'])
    df = df.drop_duplicates('open_time').sort_values('open_time').reset_index(drop=True)
    return df


def add_features(df):
    c = df['close']
    qv = df['quote_volume']
    # 全部特征只用截至上一根收盘的信息, 无未来函数
    df['ret_3m'] = (c / c.shift(3) - 1) * 100
    df['ret_5m'] = (c / c.shift(5) - 1) * 100
    df['ret_15m'] = (c / c.shift(15) - 1) * 100
    df['ret_60m'] = (c / c.shift(60) - 1) * 100
    v5 = qv.rolling(5).sum()
    base = v5.rolling(1440, min_periods=288).median()
    df['vol_surge'] = v5 / base.replace(0, np.nan)
    df['vol24h_musdt'] = qv.rolling(1440, min_periods=288).sum() / 1e6
    df['dist_24h_high'] = (c / df['high'].rolling(1440, min_periods=288).max() - 1) * 100
    return df


def main():
    rts = json.load(open(os.path.join(DATA, 'round_trips.json')))
    manual = [r for r in rts if r['period'] == 'manual']
    by_sym = {}
    for r in manual:
        by_sym.setdefault(r['sym'], []).append(r)

    entry_rows = []
    missing = 0
    for sym, rlist in by_sym.items():
        df = load_symbol(sym)
        if df is None:
            missing += len(rlist)
            continue
        df = add_features(df)
        idx = df.set_index('open_time')
        for r in rlist:
            # 入场发生在第 t 分钟内 → 特征取第 t-1 根收盘时点 (入场前一刻你看到的样子)
            t = (r['start'] // 60000) * 60000
            row = idx.reindex([t - 60000]).iloc[0]
            if pd.isna(row['ret_5m']):
                missing += 1
                continue
            entry_rows.append({**{f: row[f] for f in FEATURES}, 'net': r['net'], 'sym': sym})

    ent = pd.DataFrame(entry_rows)
    print(f"手动回合 {len(manual)} 个, 成功计算特征 {len(ent)} 个 (缺数据 {missing})\n")

    # 基线: 手动时期全市场随机时刻 (只要求 24h 成交额 > 300 万美金, 即"可交易宇宙")
    base_rows = []
    syms = [f[:-7] for f in os.listdir(KDIR) if f.endswith('.csv.gz')]
    for sym in syms:
        df = load_symbol(sym)
        if df is None or len(df) < 2000:
            continue
        df = add_features(df)
        df = df[(df['open_time'] >= CUTOFF_MS) & (df['vol24h_musdt'] > 3)].dropna(subset=['ret_5m', 'vol_surge'])
        if len(df) == 0:
            continue
        take = df.sample(n=min(60, len(df)), random_state=42)
        base_rows.append(take[FEATURES])
    base = pd.concat(base_rows, ignore_index=True)
    print(f"基线样本: {len(base)} 个随机时刻 ({len(base_rows)} 个币种)\n")

    fmt = lambda v: f"{v:8.2f}"
    print(f"{'特征':<14s} {'你的中位':>9s} {'你的P25':>9s} {'你的P75':>9s} {'市场中位':>9s} {'市场P90':>9s} {'市场P99':>9s}")
    for f in FEATURES:
        print(f"{f:<14s} {fmt(ent[f].median())} {fmt(ent[f].quantile(.25))} {fmt(ent[f].quantile(.75))} "
              f"{fmt(base[f].median())} {fmt(base[f].quantile(.90))} {fmt(base[f].quantile(.99))}")

    print("\n—— 赢单 vs 亏单的入场特征 (中位数) ——")
    w = ent[ent['net'] > 0]
    l = ent[ent['net'] <= 0]
    print(f"{'特征':<14s} {'赢单':>9s} {'亏单':>9s}   (赢 {len(w)} / 亏 {len(l)})")
    for f in FEATURES:
        print(f"{f:<14s} {fmt(w[f].median())} {fmt(l[f].median())}")

    ent.to_csv(os.path.join(DATA, 'entry_profile.csv'), index=False)
    print("\n明细已存 research/data/entry_profile.csv")


if __name__ == '__main__':
    main()
