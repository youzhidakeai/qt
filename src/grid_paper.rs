// ==========================================
// MODULE: 网格纸面交易 (Grid Paper Trading)
// 从 grid_scanner 筛出的候选里挑选并发运行的震荡型标的做虚拟现货网格,
// 用真实K线判断格线穿越, 零实弹验证"活的筛选结果"能不能在未来赚钱——
// 历史回测证明的是过去数据, 这里验证的是"今天筛出来的币, 明天是否真的震荡"。
// 出场纪律: 若价格决定性跌破/涨破原始区间(超出边界 BUST_MARGIN_PCT), 判定
// 网格失效, 按当前价清算全部虚拟库存(计入总账)并停止, 换下一个候选,
// 不会无限期扛单等回本。
// ==========================================
use std::collections::HashMap;
use serde::{Serialize, Deserialize};
use tokio::sync::mpsc;
use tracing::info;

const SCAN_SECS: u64 = 60;
const MAX_ACTIVE: usize = 3;          // 并发运行的虚拟网格数量上限
const RANGE_PCT: f64 = 10.0;          // 网格区间 = 起始价 ±10% (对应回测里表现稳健的一档)
const N_GRIDS: usize = 20;
const NOTIONAL_PER_GRID: f64 = 100.0; // 每格虚拟本金(USDT)
const FEE_PCT: f64 = 0.10;            // 保守假设 taker 双边, 不假设拿到 maker 返佣
const BUST_MARGIN_PCT: f64 = 5.0;     // 超出区间边界再 5% 判定网格失效, 清算止损
const COOLDOWN_SECS: u64 = 3600 * 6;  // 失效的币 6 小时内不重新开网格

#[derive(Serialize, Deserialize, Clone)]
struct GridPos {
    sym: String,
    lines: Vec<f64>,
    holding: Vec<bool>,
    entry_price: Vec<f64>, // 与 lines 等长, 对应线未持有时为 0
    opened_ms: u64,
    last_checked_ms: u64,
}

#[derive(Serialize, Deserialize, Clone, Default)]
struct GridStats {
    total_net: f64,
    gross_win: f64,
    gross_loss: f64,
    trades: u32,
    wins: u32,
    busts: u32, // 因跌破/涨破区间被强制清算的网格次数
}

fn kf(k: &serde_json::Value, i: usize) -> f64 {
    k.get(i).and_then(|x| x.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0.0)
}

async fn fetch_klines_since(http: &reqwest::Client, sym: &str, since_ms: u64) -> Option<Vec<serde_json::Value>> {
    let url = format!(
        "https://fapi.binance.com/fapi/v1/klines?symbol={}&interval=1m&startTime={}&limit=1000",
        sym, since_ms
    );
    let v: serde_json::Value = http.get(&url).send().await.ok()?.json().await.ok()?;
    let arr = v.as_array()?.clone();
    // 丢弃进行中的最后一根
    Some(arr[..arr.len().saturating_sub(1)].to_vec())
}

async fn fetch_last_price(http: &reqwest::Client, sym: &str) -> Option<f64> {
    let url = format!("https://fapi.binance.com/fapi/v1/ticker/price?symbol={}", sym);
    let v: serde_json::Value = http.get(&url).send().await.ok()?.json().await.ok()?;
    v.get("price")?.as_str()?.parse().ok()
}

async fn load_stats(con: &mut redis::aio::MultiplexedConnection) -> GridStats {
    redis::cmd("GET").arg("GRID_PAPER_STATS").query_async::<Option<String>>(con).await.ok().flatten()
        .and_then(|s| serde_json::from_str(&s).ok()).unwrap_or_default()
}

async fn save_stats(con: &mut redis::aio::MultiplexedConnection, s: &GridStats) {
    if let Ok(j) = serde_json::to_string(s) {
        let _: () = redis::cmd("SET").arg("GRID_PAPER_STATS").arg(j).query_async(con).await.unwrap_or(());
    }
}

fn new_grid(sym: &str, start_px: f64, now_ms: u64) -> GridPos {
    let low = start_px * (1.0 - RANGE_PCT / 100.0);
    let high = start_px * (1.0 + RANGE_PCT / 100.0);
    let lines: Vec<f64> = (0..=N_GRIDS).map(|i| low + (high - low) * i as f64 / N_GRIDS as f64).collect();
    let n = lines.len();
    GridPos { sym: sym.to_string(), lines, holding: vec![false; n], entry_price: vec![0.0; n], opened_ms: now_ms, last_checked_ms: now_ms }
}

pub async fn run_grid_paper(redis_client: redis::Client, tg_tx: mpsc::Sender<String>) {
    let http = reqwest::Client::new();
    let report_offset = time::UtcOffset::from_hms(8, 0, 0).unwrap();
    let mut cooldown: HashMap<String, std::time::Instant> = HashMap::new();
    info!("🔲 网格纸面交易已启动 (区间±{}%/{}格/{}U每格, 零实弹)", RANGE_PCT, N_GRIDS, NOTIONAL_PER_GRID);

    loop {
        tokio::time::sleep(std::time::Duration::from_secs(SCAN_SECS)).await;
        let mut con = match redis_client.get_multiplexed_async_connection().await {
            Ok(c) => c,
            Err(_) => continue,
        };
        let enabled = redis::cmd("GET").arg("GRID_PAPER_ENABLED").query_async::<Option<String>>(&mut con).await.ok().flatten().unwrap_or_else(|| "1".into());
        if enabled != "1" {
            continue;
        }

        // ---------- 1. 巡逻已有的虚拟网格 ----------
        let pos_keys: Vec<String> = redis::cmd("KEYS").arg("GRID_PAPER_POS_*").query_async(&mut con).await.unwrap_or_default();
        let mut active_syms: Vec<String> = Vec::new();
        for key in pos_keys {
            let Some(json) = redis::cmd("GET").arg(&key).query_async::<Option<String>>(&mut con).await.ok().flatten() else { continue };
            let Ok(mut p) = serde_json::from_str::<GridPos>(&json) else {
                let _: () = redis::cmd("DEL").arg(&key).query_async(&mut con).await.unwrap_or(());
                continue;
            };
            let now_ms = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64;
            let Some(bars) = fetch_klines_since(&http, &p.sym, p.last_checked_ms).await else {
                active_syms.push(p.sym);
                continue;
            };
            let low = p.lines[0];
            let high = *p.lines.last().unwrap();
            let bust_low = low * (1.0 - BUST_MARGIN_PCT / 100.0);
            let bust_high = high * (1.0 + BUST_MARGIN_PCT / 100.0);
            let mut busted = false;

            for bar in &bars {
                let (hi, lo) = (kf(bar, 2), kf(bar, 3));
                if lo <= bust_low || hi >= bust_high {
                    // 网格失效: 按本根收盘价清算全部持有库存, 计入总账并停止该标的
                    let close = kf(bar, 4);
                    let mut stats = load_stats(&mut con).await;
                    let mut liquidated_net = 0.0;
                    for i in 0..p.holding.len() {
                        if p.holding[i] {
                            let qty = NOTIONAL_PER_GRID / p.entry_price[i];
                            let gross = qty * (close - p.entry_price[i]);
                            let fee = qty * p.entry_price[i] * FEE_PCT / 100.0 + qty * close * FEE_PCT / 100.0;
                            liquidated_net += gross - fee;
                            p.holding[i] = false;
                        }
                    }
                    stats.total_net += liquidated_net;
                    stats.busts += 1;
                    if liquidated_net > 0.0 { stats.gross_win += liquidated_net; } else { stats.gross_loss += liquidated_net.abs(); }
                    save_stats(&mut con, &stats).await;
                    let dir = if hi >= bust_high { "涨破区间上沿(踏空)" } else { "跌破区间下沿(套牢清算)" };
                    let _ = tg_tx.send(format!(
                        "🔲 <b>【网格失效清算】</b> {} \n\n{}\n区间: {:.6} ~ {:.6}\n清算库存净盈亏: <b>{:+.2}U</b>\n\n📒 网格纸面账本累计: <b>{:+.2}U</b> (含 {} 次失效清算)",
                        p.sym, dir, low, high, liquidated_net, stats.total_net, stats.busts)).await;
                    cooldown.insert(p.sym.clone(), std::time::Instant::now());
                    busted = true;
                    break;
                }
                // 跌破线买入 (由高线向低线扫描)
                for i in (0..p.lines.len()).rev() {
                    if lo <= p.lines[i] && !p.holding[i] {
                        p.holding[i] = true;
                        p.entry_price[i] = p.lines[i];
                    }
                }
                // 涨回上一条线卖出获利
                for i in 0..p.lines.len() - 1 {
                    if p.holding[i] && hi >= p.lines[i + 1] {
                        let qty = NOTIONAL_PER_GRID / p.entry_price[i];
                        let gross = qty * (p.lines[i + 1] - p.entry_price[i]);
                        let fee = qty * p.entry_price[i] * FEE_PCT / 100.0 + qty * p.lines[i + 1] * FEE_PCT / 100.0;
                        let net = gross - fee;
                        let mut stats = load_stats(&mut con).await;
                        stats.total_net += net;
                        stats.trades += 1;
                        if net > 0.0 { stats.wins += 1; stats.gross_win += net; } else { stats.gross_loss += net.abs(); }
                        save_stats(&mut con, &stats).await;
                        p.holding[i] = false;
                        info!("🔲 [网格纸面] {} 第{}格获利了结 净{:+.3}U (账本累计 {:+.2}U)", p.sym, i, net, stats.total_net);
                    }
                }
            }

            if busted {
                let _: () = redis::cmd("DEL").arg(&key).query_async(&mut con).await.unwrap_or(());
                continue;
            }
            p.last_checked_ms = now_ms;
            if let Ok(j) = serde_json::to_string(&p) {
                let _: () = redis::cmd("SET").arg(&key).arg(j).query_async(&mut con).await.unwrap_or(());
            }
            active_syms.push(p.sym);
        }

        // ---------- 2. 从候选榜单里补齐并发数量 ----------
        if active_syms.len() >= MAX_ACTIVE {
            continue;
        }
        let Some(cand_json) = redis::cmd("GET").arg("GRID_CANDIDATES").query_async::<Option<String>>(&mut con).await.ok().flatten() else { continue };
        let Ok(candidates) = serde_json::from_str::<Vec<String>>(&cand_json) else { continue };

        for sym in candidates {
            if active_syms.len() >= MAX_ACTIVE {
                break;
            }
            if active_syms.contains(&sym) {
                continue;
            }
            if cooldown.get(&sym).map(|t| t.elapsed().as_secs() < COOLDOWN_SECS).unwrap_or(false) {
                continue;
            }
            let Some(start_px) = fetch_last_price(&http, &sym).await else { continue };
            if start_px <= 0.0 {
                continue;
            }
            let now_ms = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64;
            let pos = new_grid(&sym, start_px, now_ms);
            if let Ok(j) = serde_json::to_string(&pos) {
                let _: () = redis::cmd("SET").arg(format!("GRID_PAPER_POS_{}", sym)).arg(j).query_async(&mut con).await.unwrap_or(());
            }
            active_syms.push(sym.clone());
            info!("🔲 [网格纸面] {} 开始运行虚拟网格 @ {} (区间±{}%)", sym, start_px, RANGE_PCT);
            let _ = tg_tx.send(format!(
                "🔲 <b>【网格纸面开始】</b> {} (虚拟{}U/格 x {}格, 不是真单!)\n起始价: {:.6}\n区间: {:.6} ~ {:.6}\n失效线: 跌破/涨破区间再 {}% 判定失效并清算",
                sym, NOTIONAL_PER_GRID, N_GRIDS, start_px, pos.lines[0], pos.lines.last().unwrap(), BUST_MARGIN_PCT)).await;
        }

        // ---------- 3. 每日战报 ----------
        let now = time::OffsetDateTime::now_utc().to_offset(report_offset);
        if now.hour() >= 20 {
            let day = format!("{:04}-{:02}-{:02}", now.year(), now.month() as u8, now.day());
            let reported = redis::cmd("GET").arg("GRID_PAPER_LAST_REPORT_DAY").query_async::<Option<String>>(&mut con).await.ok().flatten().unwrap_or_default();
            if reported != day {
                let _: () = redis::cmd("SET").arg("GRID_PAPER_LAST_REPORT_DAY").arg(&day).query_async(&mut con).await.unwrap_or(());
                let stats = load_stats(&mut con).await;
                let pf = if stats.gross_loss > 0.0 { stats.gross_win / stats.gross_loss } else { 0.0 };
                let _ = tg_tx.send(format!(
                    "🔲 <b>【网格纸面日报】</b> {}\n\n运行中标的: {} 个 | 单笔成交: {} 次 | 失效清算: {} 次\n盈利因子: {:.2}\n累计净盈亏: <b>{:+.2}U</b> (虚拟)\n\n💡 验证的是筛选方法在未来是否持续有效, 不是历史回测。",
                    day, active_syms.len(), stats.trades, stats.busts, pf, stats.total_net)).await;
            }
        }
    }
}
