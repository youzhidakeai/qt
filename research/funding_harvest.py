#!/usr/bin/env python3
"""资金费率收割回测: 用币安真实历史费率, 量化 delta 中性收租的净年化.

策略: 每 8h 结算点, 按"上一期费率"排序, 做空费率最高的 K 个币的永续 +
持有等值现货 (delta 中性, 方向无关)。带缓冲区防频繁换仓:
持仓币掉出前 BUFFER 名才换。预测费率年化低于门槛时空仓等待。
成本: 换一个槽位 = 永续 taker 0.05%x2 + 现货 taker 0.10%x2 = 0.30%。
简化: 忽略基差损益 (现货+永续同名义对冲, 基差随每次结算收敛, 二阶小量);
现货不加杠杆, 报告的年化按"两腿合计占用资金"计算 (即真实资金效率)。
"""
import json, time, urllib.request

BASE = "https://fapi.binance.com"
VOL24_MIN = 20e6
K = 5                 # 同时持有的槽位数
ENTER_ANNUAL = 30.0   # 3日均费率年化 >30% 才进场
EXIT_ANNUAL = 10.0    # 3日均年化跌破 10% 才出场 (滞回, 与排名无关)
COST_EVENT = 0.0015   # 开或平一个槽位的单边成本 (永续taker 0.05% + 现货taker 0.10%)
DAYS = 180
CACHE = "data/funding_hist_cache.json"


def get(url):
    for _ in range(3):
        try:
            with urllib.request.urlopen(url, timeout=15) as r:
                return json.load(r)
        except Exception:
            time.sleep(1)
    return None


def main():
    import os
    since = int((time.time() - DAYS * 86400) * 1000)
    if os.path.exists(CACHE):
        hist = {s: [(int(t), float(r)) for t, r in v] for s, v in json.load(open(CACHE)).items()}
        print(f"从缓存加载 {len(hist)} 个币的费率历史")
    else:
        tickers = get(f"{BASE}/fapi/v1/ticker/24hr")
        syms = sorted(t["symbol"] for t in tickers
                      if t["symbol"].endswith("USDT") and "_" not in t["symbol"]
                      and float(t["quoteVolume"]) >= VOL24_MIN)
        print(f"流动性过滤后 {len(syms)} 个币, 拉取各自近 {DAYS} 天费率历史...")
        hist = {}  # sym -> [(fundingTime_ms, rate)]
        for i, s in enumerate(syms):
            rows = get(f"{BASE}/fapi/v1/fundingRate?symbol={s}&startTime={since}&limit=1000")
            if rows:
                hist[s] = [(int(r["fundingTime"]), float(r["fundingRate"])) for r in rows]
            if (i + 1) % 50 == 0:
                print(f"  ... {i+1}/{len(syms)}")
            time.sleep(0.05)
        if len(hist) < 50:
            raise SystemExit(f"❌ 只拉到 {len(hist)} 个币的费率历史, 疑似被限流, 不写缓存直接退出")
        os.makedirs(os.path.dirname(CACHE), exist_ok=True)
        json.dump(hist, open(CACHE, "w"))

    # 全局 8h 决策网格: 在每个网格点用"最近一期已知费率"选币, 收下一期费率
    grid_start = since + 8 * 3600 * 1000
    now_ms = int(time.time() * 1000)
    step = 8 * 3600 * 1000

    # 预处理: sym -> 按时间排序的事件
    for s in hist:
        hist[s].sort()

    held = []          # 当前持有的币
    total_ret = 0.0    # 累计收益率 (占用资金的比例)
    n_rotate = 0
    period_rets = []

    t = grid_start
    while t + step <= now_ms:
        # 决策: 用最近 3 天费率均值做年化预测 (单期噪音太大, 会导致频繁进出)
        last_rate = {}
        for s, ev in hist.items():
            recent = [r for tt, r in ev if t - 9 * step <= tt <= t]
            if len(recent) >= 6:
                last_rate[s] = sum(recent) / len(recent)
        ranked = sorted(last_rate, key=lambda s: -last_rate[s])
        # 滞回换仓: 只看币自身费率, 跌破退出线才清掉; 与排名无关
        keep = [s for s in held if last_rate.get(s, 0) * 3 * 365 * 100 >= EXIT_ANNUAL]
        n_rotate += len(held) - len(keep)   # 平仓事件
        held = keep
        for s in ranked:
            if len(held) >= K:
                break
            if s in held:
                continue
            if last_rate[s] * 3 * 365 * 100 < ENTER_ANNUAL:
                break
            held.append(s)
            n_rotate += 1                   # 开仓事件
        # 结算: 收 (t, t+8h] 内实际发生的费率 (空永续 → 正费率归我)
        period = 0.0
        for s in held:
            got = sum(r for tt, r in hist[s] if t < tt <= t + step)
            # 两腿占用: 1U 现货 + 1U 永续保证金侧敞口 → 费率按永续名义收,
            # 除以 2 折算成占用资金收益率 (保守; 永续用杠杆可提高, 但引入强平风险)
            period += got / 2 / max(len(held), 1) * (len(held) / K)  # 空槽位按 0 收益计
        total_ret += period
        period_rets.append(period)
        t += step

    n_days = len(period_rets) / 3
    rotate_cost = n_rotate * COST_EVENT / 2 / K  # 折算到两腿合计占用资金
    net = total_ret - rotate_cost
    ann = net / n_days * 365 * 100
    gross_ann = total_ret / n_days * 365 * 100
    worst = min(period_rets) * 100 if period_rets else 0
    neg_days = sum(1 for r in period_rets if r < 0) / 3
    print(f"\n=== 资金费率收割 ({n_days:.0f} 天, K={K}, 进场>{ENTER_ANNUAL}%/出场<{EXIT_ANNUAL}%) ===")
    print(f"毛年化: {gross_ann:+.1f}%  | 换仓 {n_rotate} 次, 成本拖累: -{rotate_cost/n_days*365*100:.1f}%/年")
    print(f"净年化 (占用资金口径): {ann:+.1f}%")
    print(f"最差单期: {worst:+.3f}% | 负收益天数占比: {neg_days/n_days*100:.0f}%")
    print("注: 忽略基差损益与现货挂单滑点; 永续侧 1x 无强平风险。")


if __name__ == '__main__':
    main()
