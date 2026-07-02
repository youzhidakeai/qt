#!/usr/bin/env python3
"""从 data.binance.vision 下载全市场 USDT 永续合约的 1m K 线 (官方历史归档, 无鉴权无限流).

用法: python download_klines.py --start 2026-06-15 --end 2026-07-01
输出: research/data/klines_1m/{SYMBOL}.csv.gz
"""
import argparse, csv, gzip, io, json, os, sys, urllib.request, zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta

HERE = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = os.path.join(HERE, 'data', 'klines_1m')
COLS = ['open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time',
        'quote_volume', 'count', 'taker_buy_volume', 'taker_buy_quote_volume', 'ignore']


def list_symbols():
    with urllib.request.urlopen('https://fapi.binance.com/fapi/v1/exchangeInfo', timeout=30) as r:
        info = json.loads(r.read())
    syms = []
    for s in info['symbols']:
        if s['quoteAsset'] == 'USDT' and s['contractType'] == 'PERPETUAL' and '_' not in s['symbol']:
            syms.append(s['symbol'])
    return sorted(syms)


def fetch_day(symbol, day):
    url = f"https://data.binance.vision/data/futures/um/daily/klines/{symbol}/1m/{symbol}-1m-{day}.zip"
    try:
        with urllib.request.urlopen(url, timeout=60) as r:
            raw = r.read()
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return []  # 该币种当天未上市/已下架
        raise
    rows = []
    with zipfile.ZipFile(io.BytesIO(raw)) as z:
        with z.open(z.namelist()[0]) as f:
            for line in io.TextIOWrapper(f):
                parts = line.strip().split(',')
                if not parts or not parts[0].isdigit():
                    continue  # 跳过表头
                rows.append(parts[:12])
    return rows


def fetch_symbol(symbol, days):
    out_path = os.path.join(OUT_DIR, f"{symbol}.csv.gz")
    if os.path.exists(out_path):
        return symbol, -1  # 已下载过, 跳过
    all_rows = []
    for day in days:
        all_rows.extend(fetch_day(symbol, day))
    if not all_rows:
        return symbol, 0
    all_rows.sort(key=lambda r: int(r[0]))
    with gzip.open(out_path, 'wt', newline='') as f:
        w = csv.writer(f)
        w.writerow(COLS)
        w.writerows(all_rows)
    return symbol, len(all_rows)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--start', default='2026-06-15')
    ap.add_argument('--end', default=(date.today() - timedelta(days=1)).isoformat(),
                    help='默认到昨天 (归档站当天数据要次日才有)')
    args = ap.parse_args()
    d0 = date.fromisoformat(args.start)
    d1 = date.fromisoformat(args.end)
    days = []
    d = d0
    while d <= d1:
        days.append(d.isoformat())
        d += timedelta(days=1)

    os.makedirs(OUT_DIR, exist_ok=True)
    symbols = list_symbols()
    print(f"共 {len(symbols)} 个 USDT 永续, 下载 {days[0]} ~ {days[-1]} ({len(days)} 天)", file=sys.stderr)

    done = empty = skipped = 0
    with ThreadPoolExecutor(max_workers=16) as ex:
        futs = {ex.submit(fetch_symbol, s, days): s for s in symbols}
        for fut in as_completed(futs):
            sym = futs[fut]
            try:
                _, n = fut.result()
                if n == -1:
                    skipped += 1
                elif n == 0:
                    empty += 1
                else:
                    done += 1
            except Exception as e:
                print(f"  ❌ {sym}: {e}", file=sys.stderr)
            total = done + empty + skipped
            if total % 50 == 0:
                print(f"  进度 {total}/{len(symbols)}", file=sys.stderr)
    print(f"完成: {done} 个有数据, {empty} 个无数据, {skipped} 个已存在", file=sys.stderr)


if __name__ == '__main__':
    main()
