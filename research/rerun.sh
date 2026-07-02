#!/bin/bash
# 一键重跑整条研究管线: 拉账户流水 → 重建回合 → 补K线 → 入场画像 → 基准回测
# 前提: .env 有 API key 且本机 IP 在币安白名单; 首次需 python3 -m venv .venv && .venv/bin/pip install pandas numpy
set -e
cd "$(dirname "$0")"
PY=.venv/bin/python

echo "=== 1/5 拉取账户收益流水与成交记录 ==="
$PY fetch_account.py

echo "=== 2/5 重建交易回合 ==="
$PY build_round_trips.py

echo "=== 3/5 增量下载全市场 1m K线 (已有文件自动跳过) ==="
$PY download_klines.py

echo "=== 4/5 入场画像 (你买入的时刻 vs 市场随机时刻) ==="
$PY profile_entries.py

echo "=== 5/5 基准回测 (画像阈值直译, 结果重点看'后半段/样本外'行) ==="
$PY backtest.py --v24 150 --vs 6 --r60min 3 --r5 0.5 --r15 1.0 --dh_max -3 --time_stop 30

echo ""
echo "对照基准: 06/16-07/01 期间该配置为 140 笔 / 胜率 54.3% / 盈利因子 0.73 / 净 -940U (负期望)。"
echo "若新数据把盈利因子推过 1.3 且样本外同向, 才值得讨论下一步。"
