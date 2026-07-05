// ==========================================
// MODULE: 纸面交易引擎 (Paper Trading)
// 用回测中唯一不亏钱的配置 (dip 变体 / 3% 回撤 / 不加仓) 做前向实测:
// 真实扫描 → 虚拟开仓 → 追踪高点回撤平仓 → Telegram 战报 + Redis 记账。
// 不发送任何真实订单。实弹门槛: 纸面账本跑数周后盈利因子 > 1.3。
//
// v2 (2026-07-04): 加资金费率过滤。v1 无过滤跑了 40 笔 PF 1.01, 与回测 1.02
// 一致 = 打平判死。回测按入场时费率分桶: 负费率(死猫跳) PF 0.40 / 基线 0~11%
// PF 1.81 / 偏热 >11% 全灭 → 只在费率年化 [0, 30%] 区间入场 (research/funding_vs_dip.py)。
// ==========================================
use std::collections::HashMap;
use serde::{Serialize, Deserialize};
use tokio::sync::mpsc;
use tracing::info;

const NOTIONAL: f64 = 1500.0;      // 每仓虚拟名义 (与回测一致)
const COST_RT: f64 = 0.003;        // 双边手续费+滑点 0.30% (与回测一致)
const MAX_POS: usize = 2;
const COOLDOWN_SECS: u64 = 3600;
// 入场条件 = 回测 dip 变体 (research/backtest_pyramid.py)
const V24_MIN: f64 = 150_000_000.0;
const DIST_HIGH_MAX: f64 = -3.0;   // 距 24h 高点至少 -3%
const RET60_MIN: f64 = 3.0;
const RET15_MIN: f64 = 1.0;
const RET5_MIN: f64 = 0.5;
// 爆量 = 最近 5m 成交额 / 过去 24h 滚动 5m 成交额的**中位数**。
// 必须与 research/backtest_pyramid.py 的 vol_surge 口径一致 —— 曾误用均值分母,
// 爆发币成交量右偏使均值远大于中位数, 同样行情只有 37% 能过线, 引擎几乎不开仓。
const VSURGE_MIN: f64 = 6.0;
// v2: 入场时该币资金费率年化必须落在此区间 (research/funding_vs_dip.py):
// 负费率(死猫跳) PF 0.40 排除; 基线 0~11% PF 1.81 最佳; >11% 偏热样本小但全灭,
// 上限放宽到 30% 留出安全边际, 但绝不进入 >50% 的过热区。
const FUND_MIN_ANNUAL: f64 = 0.0;
const FUND_MAX_ANNUAL: f64 = 30.0;

#[derive(Serialize, Deserialize, Clone)]
pub struct PaperPos {
    pub sym: String,
    pub entry: f64,
    pub peak: f64,
    pub opened_ms: u64,
}

#[derive(Serialize, Deserialize, Clone, Default)]
pub struct PaperStats {
    pub total_net: f64,
    pub gross_win: f64,
    pub gross_loss: f64,
    pub trades: u32,
    pub wins: u32,
}

fn kf(k: &serde_json::Value, i: usize) -> f64 {
    k.get(i).and_then(|x| x.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0.0)
}

async fn fetch_klines(http: &reqwest::Client, sym: &str, limit: u32) -> Option<Vec<serde_json::Value>> {
    let url = format!("https://fapi.binance.com/fapi/v1/klines?symbol={}&interval=1m&limit={}", sym, limit);
    let v: serde_json::Value = http.get(&url).send().await.ok()?.json().await.ok()?;
    v.as_array().cloned()
}

// 最近一期资金费率折算年化 (单期% × 3期/天 × 365)
async fn fetch_funding_annual(http: &reqwest::Client, sym: &str) -> Option<f64> {
    let url = format!("https://fapi.binance.com/fapi/v1/premiumIndex?symbol={}", sym);
    let v: serde_json::Value = http.get(&url).send().await.ok()?.json().await.ok()?;
    let rate: f64 = v.get("lastFundingRate")?.as_str()?.parse().ok()?;
    Some(rate * 100.0 * 3.0 * 365.0)
}

async fn load_stats(con: &mut redis::aio::MultiplexedConnection) -> PaperStats {
    redis::cmd("GET").arg("PAPER_STATS").query_async::<Option<String>>(con).await.ok().flatten()
        .and_then(|s| serde_json::from_str(&s).ok()).unwrap_or_default()
}

async fn save_stats(con: &mut redis::aio::MultiplexedConnection, s: &PaperStats) {
    if let Ok(json) = serde_json::to_string(s) {
        let _: () = redis::cmd("SET").arg("PAPER_STATS").arg(json).query_async(con).await.unwrap_or(());
    }
}

pub async fn run_paper_trader(redis_client: redis::Client, tg_tx: mpsc::Sender<String>) {
    let http = reqwest::Client::new();
    let mut cooldown: HashMap<String, std::time::Instant> = HashMap::new();
    let report_offset = time::UtcOffset::from_hms(8, 0, 0).unwrap();
    let mut scan_n: u64 = 0;
    info!("📝 纸面交易引擎已启动 (dip变体/3%回撤/不加仓, 虚拟 {}U/仓, 零实弹)", NOTIONAL);

    loop {
        tokio::time::sleep(std::time::Duration::from_secs(60)).await;
        let mut con = match redis_client.get_multiplexed_async_connection().await {
            Ok(c) => c,
            Err(_) => continue,
        };
        let enabled = redis::cmd("GET").arg("PAPER_ENABLED").query_async::<Option<String>>(&mut con).await.ok().flatten().unwrap_or_else(|| "1".into());

        // 每小时一条心跳进 journal, 证明扫描循环活着 (无信号 ≠ 引擎死了)
        scan_n += 1;
        if scan_n % 60 == 1 {
            info!("📝 [纸面] 扫描心跳: 第 {} 轮, 开关={}", scan_n, enabled);
        }

        // ---------- 0. 每日战报 (20点后第一个循环推送; 停用时也报, 作为心跳) ----------
        // 去重状态放 Redis, 引擎重启不会重复推送
        let now = time::OffsetDateTime::now_utc().to_offset(report_offset);
        if now.hour() >= 20 {
            let day = format!("{:04}-{:02}-{:02}", now.year(), now.month() as u8, now.day());
            let reported = redis::cmd("GET").arg("PAPER_LAST_REPORT_DAY").query_async::<Option<String>>(&mut con).await.ok().flatten().unwrap_or_default();
            if reported != day {
                let _: () = redis::cmd("SET").arg("PAPER_LAST_REPORT_DAY").arg(&day).query_async(&mut con).await.unwrap_or(());
                let stats = load_stats(&mut con).await;
                let open_count: usize = redis::cmd("KEYS").arg("PAPER_POS_*").query_async::<Vec<String>>(&mut con).await.map(|v| v.len()).unwrap_or(0);
                let pf = if stats.gross_loss > 0.0 { stats.gross_win / stats.gross_loss } else { 0.0 };
                let verdict = if stats.trades < 30 {
                    format!("样本 {}/30 笔, 攒够前不下结论", stats.trades)
                } else if pf > 1.3 {
                    format!("盈利因子 {:.2} 已过 1.3 门槛, 可讨论小仓实弹", pf)
                } else {
                    format!("盈利因子 {:.2} 未达 1.3 门槛, 继续纸面", pf)
                };
                let _ = tg_tx.send(format!(
                    "📝 <b>【纸面日报 v2: 加资金费率过滤】</b> {}\n\n\
                    状态: {} | 虚拟持仓: {} 个\n\
                    📒 账本: {} 笔 | 胜率 {:.0}% | 盈利因子 {:.2}\n\
                    累计净盈亏: <b>{:+.2}U</b> (虚拟)\n\n\
                    ⚖️ 实弹判定: {}",
                    day,
                    if enabled == "1" { "✅ 运行中" } else { "⛔️ 已停" },
                    open_count, stats.trades,
                    if stats.trades > 0 { 100.0 * stats.wins as f64 / stats.trades as f64 } else { 0.0 },
                    pf, stats.total_net, verdict)).await;
            }
        }

        if enabled != "1" {
            continue;
        }
        let trail: f64 = redis::cmd("GET").arg("PAPER_TRAIL_PCT").query_async::<Option<String>>(&mut con).await.ok().flatten().and_then(|s| s.parse().ok()).unwrap_or(3.0);

        // ---------- 1. 管理已开的虚拟仓位 ----------
        let pos_keys: Vec<String> = redis::cmd("KEYS").arg("PAPER_POS_*").query_async(&mut con).await.unwrap_or_default();
        let mut open_syms: Vec<String> = Vec::new();
        for key in pos_keys {
            let Some(json) = redis::cmd("GET").arg(&key).query_async::<Option<String>>(&mut con).await.ok().flatten() else { continue };
            let Ok(mut p) = serde_json::from_str::<PaperPos>(&json) else {
                let _: () = redis::cmd("DEL").arg(&key).query_async(&mut con).await.unwrap_or(());
                continue;
            };
            // 用最后一根已收盘的 1m K线判断 (最后一个元素是进行中的, 丢弃)
            let Some(kl) = fetch_klines(&http, &p.sym, 3).await else { open_syms.push(p.sym); continue };
            if kl.len() >= 2 {
                let bar = &kl[kl.len() - 2];
                let (high, low) = (kf(bar, 2), kf(bar, 3));
                let held_min = (std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64 - p.opened_ms) / 60_000;
                let trail_px = p.peak * (1.0 - trail / 100.0);
                // 出场: ① 高点回撤触线 (悲观按线价成交) ② 24h 时间保险丝 (回测 MAX_HOLD 同口径)
                let exit: Option<(f64, String)> = if low <= trail_px && trail_px > 0.0 {
                    // 跳空低开砸穿线时按开盘价成交 (回测同口径), 不许按线价美化
                    Some((trail_px.min(kf(bar, 1)), format!("高点回撤 {}%", trail)))
                } else if held_min >= 1440 {
                    Some((kf(bar, 4), "24h 时间保险丝".to_string()))
                } else {
                    None
                };
                if let Some((exit_px, reason)) = exit {
                    let net = NOTIONAL * (exit_px / p.entry - 1.0) - NOTIONAL * COST_RT;
                    let mut stats = load_stats(&mut con).await;
                    stats.total_net += net;
                    stats.trades += 1;
                    if net > 0.0 { stats.wins += 1; stats.gross_win += net; } else { stats.gross_loss += net.abs(); }
                    save_stats(&mut con, &stats).await;
                    let _: () = redis::cmd("DEL").arg(&key).query_async(&mut con).await.unwrap_or(());
                    cooldown.insert(p.sym.clone(), std::time::Instant::now());
                    let emoji = if net > 0.0 { "🟢" } else { "🔴" };
                    let pf = if stats.gross_loss > 0.0 { stats.gross_win / stats.gross_loss } else { 0.0 };
                    info!("📝 [纸面] {} 平仓 净{:+.1}U (账本累计 {:+.1}U)", p.sym, net, stats.total_net);
                    let _ = tg_tx.send(format!(
                        "📝 <b>【纸面平仓】</b> {} {}\n入场 {:.6} → 出场 {:.6} ({})\n持仓 {} 分钟 | 本单净: <b>{:+.2}U</b>\n\n📒 纸面账本: {} 笔 | 胜率 {:.0}% | 盈利因子 {:.2} | 累计 <b>{:+.2}U</b>",
                        emoji, p.sym, p.entry, exit_px, reason, held_min, net,
                        stats.trades, if stats.trades > 0 { 100.0 * stats.wins as f64 / stats.trades as f64 } else { 0.0 }, pf, stats.total_net)).await;
                    continue;
                }
                if high > p.peak {
                    p.peak = high;
                    if let Ok(j) = serde_json::to_string(&p) {
                        let _: () = redis::cmd("SET").arg(&key).arg(j).query_async(&mut con).await.unwrap_or(());
                    }
                }
            }
            open_syms.push(p.sym);
        }

        // ---------- 2. 扫描新的虚拟入场 ----------
        if open_syms.len() >= MAX_POS {
            continue;
        }
        let Ok(resp) = http.get("https://fapi.binance.com/fapi/v1/ticker/24hr").send().await else { continue };
        let Ok(tickers) = resp.json::<serde_json::Value>().await else { continue };
        let Some(arr) = tickers.as_array() else { continue };

        // 预筛: 热点币 + 离 24h 高点有距离
        let mut candidates: Vec<(String, f64, f64)> = Vec::new(); // (sym, change24, dist_high)
        for t in arr {
            let sym = t["symbol"].as_str().unwrap_or("");
            if !sym.ends_with("USDT") || sym.contains('_') || sym.starts_with("XAU") || sym.starts_with("XAG") {
                continue;
            }
            let v24: f64 = t["quoteVolume"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
            let last: f64 = t["lastPrice"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
            let high24: f64 = t["highPrice"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
            let change24: f64 = t["priceChangePercent"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
            if v24 < V24_MIN || last <= 0.0 || high24 <= 0.0 {
                continue;
            }
            let dist = (last / high24 - 1.0) * 100.0;
            if dist > DIST_HIGH_MAX {
                continue;
            }
            if open_syms.contains(&sym.to_string()) {
                continue;
            }
            if cooldown.get(sym).map(|t| t.elapsed().as_secs() < COOLDOWN_SECS).unwrap_or(false) {
                continue;
            }
            candidates.push((sym.to_string(), change24, dist));
        }
        // 按 24h 涨幅排序而非成交额: 之前按成交额排, BTC/ETH 等大币在回调日
        // 永远占满名额却不可能满足 1h +3%, 真正拉升中的热点币反而查不到
        candidates.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        candidates.truncate(20); // 每轮最多细查 20 个, 控制请求量

        let mut slots = MAX_POS - open_syms.len();
        for (sym, _change24, dist) in candidates {
            if slots == 0 {
                break;
            }
            // 1500 根 1m: 61 根算动量, 其余算 24h 滚动 5m 成交额中位数 (回测同口径)
            let Some(kl) = fetch_klines(&http, &sym, 1500).await else { continue };
            let bars = &kl[..kl.len().saturating_sub(1)]; // 丢弃进行中的最后一根
            let n = bars.len();
            if n < 292 {
                continue; // 与回测 min_periods=288 对齐, 新上市数据不足不打
            }
            let c = |i: usize| kf(&bars[i], 4);
            let last = c(n - 1);
            if last <= 0.0 || c(n - 61) <= 0.0 {
                continue;
            }
            let ret5 = (last / c(n - 6) - 1.0) * 100.0;
            let ret15 = (last / c(n - 16) - 1.0) * 100.0;
            let ret60 = (last / c(n - 61) - 1.0) * 100.0;
            // 滚动 5m 成交额序列 (逐根滑动, 与 pandas rolling(5).sum() 一致)
            let qv = |i: usize| kf(&bars[i], 7);
            let mut v5s: Vec<f64> = Vec::with_capacity(n);
            let mut acc = 0.0;
            for i in 0..n {
                acc += qv(i);
                if i >= 5 {
                    acc -= qv(i - 5);
                }
                if i >= 4 {
                    v5s.push(acc);
                }
            }
            let vol5m = *v5s.last().unwrap_or(&0.0);
            let take = v5s.len().min(1440);
            let mut window: Vec<f64> = v5s[v5s.len() - take..].to_vec();
            window.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            let median = window[window.len() / 2];
            if median <= 0.0 {
                continue;
            }
            let vsurge = vol5m / median;

            if ret60 >= RET60_MIN && ret15 >= RET15_MIN && ret5 >= RET5_MIN && vsurge >= VSURGE_MIN {
                // v2 过滤: 价格结构达标后才查费率 (省 API 调用), 落在死区就放弃这个候选
                let Some(fund_annual) = fetch_funding_annual(&http, &sym).await else { continue };
                if fund_annual < FUND_MIN_ANNUAL || fund_annual > FUND_MAX_ANNUAL {
                    info!("📝 [纸面] {} 价格结构达标但费率年化 {:+.1}% 不在 [{},{}] 区间, 放弃", sym, fund_annual, FUND_MIN_ANNUAL, FUND_MAX_ANNUAL);
                    continue;
                }
                // 入场价用进行中那根 K 的最新价 (≈此刻市价单能成交的价), 不用已收盘的
                // "过期价"——拉升行情里旧收盘价系统性偏低, 会虚增账本利润
                let live_px = kf(&kl[kl.len() - 1], 4);
                let entry_px = if live_px > 0.0 { live_px } else { last };
                let now_ms = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64;
                let pos = PaperPos { sym: sym.clone(), entry: entry_px, peak: entry_px, opened_ms: now_ms };
                if let Ok(j) = serde_json::to_string(&pos) {
                    let _: () = redis::cmd("SET").arg(format!("PAPER_POS_{}", sym)).arg(j).query_async(&mut con).await.unwrap_or(());
                }
                slots -= 1;
                info!("📝 [纸面] {} 开仓 @ {} (1h {:+.1}% / 5m爆量 {:.1}x / 距高点 {:.1}% / 费率年化 {:+.1}%)", sym, entry_px, ret60, vsurge, dist, fund_annual);
                let _ = tg_tx.send(format!(
                    "📝 <b>【纸面开仓 v2】</b> {} (虚拟 {}U, 不是真单!)\n入场价: {:.6}\n信号: 1h {:+.1}% | 15m {:+.1}% | 爆量 {:.1}x | 距24h高点 {:.1}%\n资金费率年化: {:+.1}% (过滤区间 [{:.0}%,{:.0}%])\n出场规则: 高点回撤 {}% 自动了结",
                    sym, NOTIONAL, entry_px, ret60, ret15, vsurge, dist, fund_annual, FUND_MIN_ANNUAL, FUND_MAX_ANNUAL, trail)).await;
            }
            tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        }
    }
}
