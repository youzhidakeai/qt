// ==========================================
// MODULE: 网格候选扫描 (Grid Candidate Scanner)
// 每小时扫描全市场, 用 Kaufman 效率比(净涨跌/路径总长度)找震荡型标的:
// 效率比越低说明来回抵消越多、越适合现货网格; 同时要求净涨跌本身够小
// (不是"沿斜坡震荡"仍然亏库存)、日均波动够大(有肉可赚)、流动性够(能成交)。
// 每天 20 点推送候选榜单到 Telegram, 纯侦察, 不下单、不影响任何真实交易。
//
// 方法论已用 648 个币的历史数据验证 (research/backtest_grid.py):
// 未筛选的热点动量币/大盘, 网格总盈亏(含未卖库存)完全由涨跌方向主导——
// 币涨了网格暴赚(纯运气), 币跌了网格暴亏(越跌越买越套); 而通过本模块筛选
// 的候选(低ER+净涨跌小+波动够), 同期网格总盈亏全部为正, 且不依赖方向运气。
// ==========================================
use tokio::sync::mpsc;
use tracing::info;

const SCAN_SECS: u64 = 3600;
const LOOKBACK_HOURS: u32 = 168; // 7 天
const VOL24_MIN: f64 = 20_000_000.0;
const ER_MAX: f64 = 0.15;
const NET_RET_MAX_PCT: f64 = 15.0;
const DAILY_MOVE_MIN_PCT: f64 = 3.0;
const MAX_REPORT: usize = 10;
const SLEEP_BETWEEN_MS: u64 = 120; // 逐币查K线之间的节流, 避免打爆请求限速

struct GridCandidate {
    sym: String,
    er: f64,
    ret_pct: f64,
    daily_move_pct: f64,
    vol24h_m: f64,
}

async fn fetch_hourly_closes(http: &reqwest::Client, sym: &str) -> Option<Vec<f64>> {
    let url = format!("https://api.binance.com/api/v3/klines?symbol={}&interval=1h&limit={}", sym, LOOKBACK_HOURS);
    let v: serde_json::Value = http.get(&url).send().await.ok()?.json().await.ok()?;
    let arr = v.as_array()?;
    // 丢弃进行中的最后一根 (与 paper.rs/funding.rs 同口径)
    let bars = &arr[..arr.len().saturating_sub(1)];
    Some(bars.iter().filter_map(|k| k.get(4).and_then(|c| c.as_str()).and_then(|s| s.parse::<f64>().ok())).collect())
}

// 返回 (效率比, 净涨跌%, 日均波动路程%)
fn compute_metrics(closes: &[f64]) -> Option<(f64, f64, f64)> {
    if closes.len() < 24 {
        return None; // 数据不足一天, 不参与筛选
    }
    let mut total_move_pct = 0.0;
    for i in 1..closes.len() {
        if closes[i - 1] <= 0.0 {
            continue;
        }
        total_move_pct += ((closes[i] / closes[i - 1] - 1.0) * 100.0).abs();
    }
    if closes[0] <= 0.0 {
        return None;
    }
    let net_pct = (closes[closes.len() - 1] / closes[0] - 1.0) * 100.0;
    let er = if total_move_pct > 0.0 { net_pct.abs() / total_move_pct } else { 1.0 };
    let n_days = closes.len() as f64 / 24.0;
    let daily_move_pct = total_move_pct / n_days;
    Some((er, net_pct, daily_move_pct))
}

pub async fn run_grid_scanner(redis_client: redis::Client, tg_tx: mpsc::Sender<String>) {
    let http = reqwest::Client::new();
    let report_offset = time::UtcOffset::from_hms(8, 0, 0).unwrap();
    let mut latest: Vec<GridCandidate> = Vec::new();
    info!("🔲 网格候选扫描已启动 (每小时全市场扫描, 每天20点报告, 纯侦察不下单)");

    loop {
        let mut con = match redis_client.get_multiplexed_async_connection().await {
            Ok(c) => c,
            Err(_) => {
                tokio::time::sleep(std::time::Duration::from_secs(60)).await;
                continue;
            }
        };

        // ---------- 扫描 ----------
        if let Ok(resp) = http.get("https://api.binance.com/api/v3/ticker/24hr").send().await {
            if let Ok(tickers) = resp.json::<serde_json::Value>().await {
                if let Some(arr) = tickers.as_array() {
                    // 先用一次性拉回的 24hr 数据做流动性预筛, 避免对冷门币也去查K线
                    let mut eligible: Vec<(String, f64)> = Vec::new();
                    for t in arr {
                        let sym = t["symbol"].as_str().unwrap_or("");
                        if !sym.ends_with("USDT") || sym.contains('_') || sym.starts_with("XAU") || sym.starts_with("XAG") {
                            continue;
                        }
                        let v24: f64 = t["quoteVolume"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                        if v24 < VOL24_MIN {
                            continue;
                        }
                        eligible.push((sym.to_string(), v24));
                    }
                    info!("🔲 [网格扫描] 流动性预筛通过 {} 个币, 开始逐个查K线...", eligible.len());

                    let mut candidates = Vec::new();
                    for (sym, v24) in eligible {
                        let Some(closes) = fetch_hourly_closes(&http, &sym).await else { continue };
                        if let Some((er, ret_pct, daily_move_pct)) = compute_metrics(&closes) {
                            if er < ER_MAX && ret_pct.abs() < NET_RET_MAX_PCT && daily_move_pct > DAILY_MOVE_MIN_PCT {
                                candidates.push(GridCandidate { sym, er, ret_pct, daily_move_pct, vol24h_m: v24 / 1_000_000.0 });
                            }
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(SLEEP_BETWEEN_MS)).await;
                    }
                    candidates.sort_by(|a, b| b.daily_move_pct.partial_cmp(&a.daily_move_pct).unwrap_or(std::cmp::Ordering::Equal));
                    info!("🔲 [网格扫描] 本轮筛出候选 {} 个", candidates.len());

                    // 把候选榜单(符号列表)写入 Redis, 供 grid_paper.rs 挑选运行纸面网格
                    let syms: Vec<&str> = candidates.iter().map(|c| c.sym.as_str()).collect();
                    if let Ok(j) = serde_json::to_string(&syms) {
                        let _: () = redis::cmd("SET").arg("GRID_CANDIDATES").arg(j).query_async(&mut con).await.unwrap_or(());
                    }

                    latest = candidates;
                }
            }
        }

        // ---------- 每小时报告 (扫描本身就是整点一轮, 扫完即报; 按小时去重防重启重发) ----------
        let now = time::OffsetDateTime::now_utc().to_offset(report_offset);
        let hour_bucket = format!("{:04}-{:02}-{:02}-{:02}", now.year(), now.month() as u8, now.day(), now.hour());
        let reported = redis::cmd("GET").arg("GRID_LAST_REPORT_HOUR").query_async::<Option<String>>(&mut con).await.ok().flatten().unwrap_or_default();
        if reported != hour_bucket {
            let _: () = redis::cmd("SET").arg("GRID_LAST_REPORT_HOUR").arg(&hour_bucket).query_async(&mut con).await.unwrap_or(());
            let mut lines = String::new();
            if latest.is_empty() {
                lines.push_str("  (本轮未筛出合格候选, 市场普遍趋势较强)\n");
            } else {
                for c in latest.iter().take(MAX_REPORT) {
                    lines.push_str(&format!(
                        "  • {} | 效率比 {:.3} | 7日净涨跌 {:+.1}% | 日均波动路程 {:.0}% | 24h量 {:.0}百万U\n",
                        c.sym, c.er, c.ret_pct, c.daily_move_pct, c.vol24h_m));
                }
            }
            let _ = tg_tx.send(format!(
                "🔲 <b>【网格候选小时报】</b> {}:00\n(纯侦察, 不下单; 现货网格建议标的——效率比低+净涨跌小+波动够+流动性够)\n\n{}\n💡 网格只吃震荡: 效率比越低越像来回抖动, 净涨跌控制在 ±{}% 以内避免沿趋势套牢/踏空。单边趋势币不要开网格。",
                now.hour(), lines, NET_RET_MAX_PCT)).await;
        }

        tokio::time::sleep(std::time::Duration::from_secs(SCAN_SECS)).await;
    }
}
