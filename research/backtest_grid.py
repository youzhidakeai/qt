#!/usr/bin/env python3
"""现货网格回测: 在 [low, high] 区间等距摆 N 条网格线, 跌破一条线买1份,
涨回上一条线卖掉(赚一格差价), 无杠杆无强平, 最坏情况是拿着币。
区间用测试窗口开始时刻的价格 ±range_pct 划定 (不用未来高低点, 避免看穿偏差)。
两档手续费对照: taker 0.10%/边 (保守) 与 maker 0.02%/边 (网格挂限价单常见)。
"""
import os, sys
import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
KDIR = os.path.join(HERE, 'data', 'klines_1m')
NOTIONAL_PER_GRID = 100.0  # 每格买入的名义本金(USDT)


def load(sym):
    df = pd.read_csv(os.path.join(KDIR, f"{sym}.csv.gz"), usecols=['open_time', 'high', 'low', 'close'])
    return df.drop_duplicates('open_time').sort_values('open_time').reset_index(drop=True)


def simulate_grid(df, range_pct, n_grids, fee_pct):
    start_px = df['close'].iloc[0]
    low, high = start_px * (1 - range_pct / 100), start_px * (1 + range_pct / 100)
    lines = np.linspace(low, high, n_grids + 1)
    step_pct = (high / low) ** (1 / n_grids) - 1  # 近似每格间距(几何)

    holding = [False] * len(lines)   # 每条线是否持有"买在这条线"的一份
    realized, trades, busted_low, busted_high = 0.0, 0, 0, 0
    qty_per_grid = lambda px: NOTIONAL_PER_GRID / px

    hi, lo = df['high'].values, df['low'].values
    for k in range(len(df)):
        # 跌破线: 由高线向低线扫描, 价格跌破哪条线就在哪条线买入(若未持有)
        for i in range(len(lines) - 1, -1, -1):
            if lo[k] <= lines[i] and not holding[i]:
                holding[i] = True
        # 涨回线: 若持有第 i 条线买入的仓位且价格涨回第 i+1 条线, 卖出获利了结
        for i in range(len(lines) - 1):
            if holding[i] and hi[k] >= lines[i + 1]:
                qty = qty_per_grid(lines[i])
                gross = qty * (lines[i + 1] - lines[i])
                fee = qty * lines[i] * fee_pct / 100 + qty * lines[i + 1] * fee_pct / 100
                realized += gross - fee
                trades += 1
                holding[i] = False

    end_px = df['close'].iloc[-1]
    held_lines = sum(1 for h in holding if h)
    # 未平仓库存的浮动盈亏(按最终收盘价估值, 不强平)
    unrealized = sum(qty_per_grid(lines[i]) * (end_px - lines[i]) for i in range(len(lines)) if holding[i])
    if end_px < low:
        busted_low = 1   # 跌破区间下沿, 网格全仓被套在底部
    if end_px > high:
        busted_high = 1  # 涨破区间上沿, 网格早早清仓踏空后续涨幅
    buy_hold_ret_pct = (end_px / start_px - 1) * 100
    return {
        'realized': realized, 'trades': trades, 'held_lines': held_lines, 'n_grids': n_grids,
        'unrealized': unrealized, 'total': realized + unrealized,
        'start_px': start_px, 'end_px': end_px, 'buy_hold_pct': buy_hold_ret_pct,
        'busted_low': busted_low, 'busted_high': busted_high,
    }


def main():
    syms = sys.argv[1:] if len(sys.argv) > 1 else ['BTCUSDT', 'ETHUSDT', 'BNBUSDT']
    print(f"{'币种':<12}{'区间%':>6}{'格数':>5}{'手续费':>7} | {'已实现':>8}{'笔数':>5} | {'浮动库存':>9}{'总计':>9} | {'买入持有':>9} | 备注")
    for sym in syms:
        path = os.path.join(KDIR, f"{sym}.csv.gz")
        if not os.path.exists(path):
            print(f"{sym}: 无本地K线数据, 跳过")
            continue
        df = load(sym)
        for range_pct in (10, 20):
            for n_grids in (20, 40):
                for fee_pct in (0.10, 0.02):
                    r = simulate_grid(df, range_pct, n_grids, fee_pct)
                    note = []
                    if r['busted_low']: note.append("⚠️跌破区间套牢")
                    if r['busted_high']: note.append("⚠️涨破区间踏空")
                    print(f"{sym:<12}{range_pct:>6}{n_grids:>5}{fee_pct:>6.2f}% | "
                          f"{r['realized']:>+8.1f}{r['trades']:>5} | "
                          f"{r['unrealized']:>+9.1f}{r['total']:>+9.1f} | "
                          f"{r['buy_hold_pct']:>+8.1f}% | {' '.join(note)}")


if __name__ == '__main__':
    main()
