#!/usr/bin/env python3
"""只读拉取币安合约账户的收益流水 (income) 和成交记录 (userTrades)，缓存为 JSON。
不调用任何交易接口。需要 .env 中的 API key 且本机 IP 已加白。

用法: python fetch_account.py [--start 2026-05-01]
输出: research/data/incomes.json, research/data/trades.json
"""
import argparse, datetime, hmac, hashlib, json, os, sys, time, urllib.request, urllib.parse

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(HERE, 'data')
ENV_PATH = os.path.join(HERE, '..', '.env')

env = {}
with open(ENV_PATH) as f:
    for line in f:
        line = line.strip()
        if line and not line.startswith('#') and '=' in line:
            k, v = line.split('=', 1)
            env[k.strip()] = v.strip().strip('"').strip("'")

API_KEY = env['BINANCE_API_KEY']
SECRET = env['BINANCE_API_SECRET']
BASE = env.get('BINANCE_BASE_URL', 'https://fapi.binance.com')
DAY = 86400_000


def signed_get(path, params):
    p = dict(params)
    p['timestamp'] = int(time.time() * 1000)
    p['recvWindow'] = 60000
    qs = urllib.parse.urlencode(p)
    sig = hmac.new(SECRET.encode(), qs.encode(), hashlib.sha256).hexdigest()
    req = urllib.request.Request(f"{BASE}{path}?{qs}&signature={sig}", headers={'X-MBX-APIKEY': API_KEY})
    for _ in range(3):
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            body = e.read().decode()
            if e.code == 429:
                time.sleep(5)
                continue
            raise RuntimeError(f"HTTP {e.code}: {body}")
    raise RuntimeError("重试耗尽")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--start', default='2026-05-01')
    args = ap.parse_args()
    os.makedirs(DATA, exist_ok=True)
    start = int(datetime.datetime.fromisoformat(args.start).replace(tzinfo=datetime.timezone.utc).timestamp() * 1000)
    now = int(time.time() * 1000)

    # ---------- 收益流水: 25 天一段 + 段内分页 ----------
    incomes, seen = [], set()
    chunk_start = start
    while chunk_start < now:
        chunk_end = min(chunk_start + 25 * DAY, now)
        cursor = chunk_start
        while True:
            recs = signed_get('/fapi/v1/income', {'startTime': cursor, 'endTime': chunk_end, 'limit': 1000})
            if isinstance(recs, dict):
                raise RuntimeError(f"income 接口返回错误: {recs}")
            if not recs:
                break
            new, max_t = 0, cursor
            for r in recs:
                key = f"{r.get('tranId')}:{r.get('incomeType')}:{r.get('symbol')}:{r.get('time')}:{r.get('income')}"
                if key in seen:
                    continue
                seen.add(key)
                incomes.append(r)
                new += 1
                max_t = max(max_t, r.get('time', 0))
            if len(recs) < 1000 or new == 0:
                break
            cursor = max_t
            time.sleep(0.3)
        chunk_start = chunk_end
        time.sleep(0.3)

    incomes.sort(key=lambda r: r['time'])
    json.dump(incomes, open(os.path.join(DATA, 'incomes.json'), 'w'))
    print(f"收益流水 {len(incomes)} 条", file=sys.stderr)
    if not incomes:
        return

    first_ts = incomes[0]['time']
    symbols = sorted({r['symbol'] for r in incomes if r.get('symbol') and r['incomeType'] in ('REALIZED_PNL', 'COMMISSION')})
    print(f"涉及 {len(symbols)} 个交易对", file=sys.stderr)

    # ---------- 成交记录: 每币 7 天窗口分页 ----------
    all_trades = {}
    for sym in symbols:
        trades, seen_ids = [], set()
        win_start = first_ts - DAY
        while win_start < now:
            win_end = min(win_start + 6 * DAY, now)
            cursor = win_start
            while True:
                recs = signed_get('/fapi/v1/userTrades', {'symbol': sym, 'startTime': cursor, 'endTime': win_end, 'limit': 1000})
                if isinstance(recs, dict):
                    raise RuntimeError(f"userTrades {sym} 返回错误: {recs}")
                if not recs:
                    break
                new, max_t = 0, cursor
                for r in recs:
                    if r['id'] in seen_ids:
                        continue
                    seen_ids.add(r['id'])
                    trades.append(r)
                    new += 1
                    max_t = max(max_t, r['time'])
                if len(recs) < 1000 or new == 0:
                    break
                cursor = max_t
                time.sleep(0.3)
            win_start = win_end
            time.sleep(0.2)
        trades.sort(key=lambda r: r['time'])
        all_trades[sym] = trades

    json.dump(all_trades, open(os.path.join(DATA, 'trades.json'), 'w'))
    print(f"成交记录共 {sum(len(v) for v in all_trades.values())} 笔", file=sys.stderr)


if __name__ == '__main__':
    main()
