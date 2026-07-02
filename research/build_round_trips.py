#!/usr/bin/env python3
"""把成交记录 (trades.json) 重建为回合 (0→持仓→0)，BNB 手续费按小时价折算 USDT。

用法: python build_round_trips.py [--cutoff 2026-06-22]  # cutoff 前标 bot, 之后标 manual
输出: research/data/round_trips.json
"""
import argparse, json, os, sys, time, urllib.request, urllib.parse
from decimal import Decimal
from datetime import datetime, timezone, timedelta

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(HERE, 'data')
TZ8 = timezone(timedelta(hours=8))


def public_get(path, params):
    url = f"https://fapi.binance.com{path}?{urllib.parse.urlencode(params)}"
    for _ in range(3):
        try:
            with urllib.request.urlopen(url, timeout=20) as r:
                return json.loads(r.read())
        except Exception:
            time.sleep(2)
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--cutoff', default='2026-06-22', help='此日期(UTC+8 零点)之前为 bot, 之后为 manual')
    args = ap.parse_args()
    cutoff_ms = int(datetime.fromisoformat(args.cutoff).replace(tzinfo=TZ8).timestamp() * 1000)

    all_trades = json.load(open(os.path.join(DATA, 'trades.json')))
    ts_all = [t['time'] for v in all_trades.values() for t in v]
    if not ts_all:
        print("没有成交记录", file=sys.stderr)
        return

    # BNB 手续费折算表 (1h 收盘)
    bnb_px = {}
    cur, t_max = min(ts_all) - 3600_000, max(ts_all) + 3600_000
    while cur < t_max:
        kl = public_get('/fapi/v1/klines', {'symbol': 'BNBUSDT', 'interval': '1h', 'startTime': cur, 'limit': 1000}) or []
        for k in kl:
            bnb_px[k[0] // 3600_000] = float(k[4])
        if not kl or kl[-1][0] >= t_max:
            break
        cur = kl[-1][0] + 3600_000

    def to_usdt(amount, asset, ts):
        if asset == 'USDT':
            return amount
        if asset == 'BNB' and bnb_px:
            h = ts // 3600_000
            px = bnb_px.get(h) or bnb_px[min(bnb_px, key=lambda x: abs(x - h))]
            return amount * px
        return 0.0

    rts, open_pos = [], []
    for sym, trades in all_trades.items():
        pos, rt = Decimal(0), None
        for tr in trades:
            q = Decimal(tr['qty']) * (1 if tr['side'] == 'BUY' else -1)
            fee = to_usdt(float(tr['commission']), tr['commissionAsset'], tr['time'])
            if pos == 0:
                rt = {'sym': sym, 'dir': 'LONG' if q > 0 else 'SHORT', 'start': tr['time'],
                      'pnl': 0.0, 'fee': 0.0, 'fills': 0, 'maker_fills': 0,
                      'entry_px': float(tr['price']), 'max_notional': 0.0}
            if rt is None:
                continue
            pos += q
            rt['pnl'] += float(tr['realizedPnl'])
            rt['fee'] += fee
            rt['fills'] += 1
            rt['maker_fills'] += 1 if tr['maker'] else 0
            rt['max_notional'] = max(rt['max_notional'], abs(float(pos)) * float(tr['price']))
            if pos == 0:
                rt['end'] = tr['time']
                rt['exit_px'] = float(tr['price'])
                rt['net'] = rt['pnl'] - rt['fee']
                rt['dur_s'] = (rt['end'] - rt['start']) / 1000
                rt['period'] = 'bot' if rt['start'] < cutoff_ms else 'manual'
                rts.append(rt)
                rt = None
        if pos != 0 and rt is not None:
            open_pos.append((sym, float(pos)))

    json.dump(rts, open(os.path.join(DATA, 'round_trips.json'), 'w'))
    for p in ('bot', 'manual'):
        sub = [r for r in rts if r['period'] == p]
        if sub:
            print(f"{p}: {len(sub)} 回合, 净 {sum(r['net'] for r in sub):+.2f} U", file=sys.stderr)
    if open_pos:
        print(f"未平仓 (不计入): {open_pos}", file=sys.stderr)


if __name__ == '__main__':
    main()
