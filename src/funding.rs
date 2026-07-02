// ==========================================
// MODULE: 资金费率监控 (仅侦察, 不下任何真实订单)
// 结构性收益的第一步: 做空正费率永续 + 持有等值现货 = 稳定收取资金费,
// 不赌方向。这里先量化"该收益到底有多少、在哪些币上", 实弹与否之后再议。
// 年化口径: 单期费率 × 3期/天 × 365。
// ==========================================
use tokio::sync::mpsc;
use tracing::info;

const SCAN_SECS: u64 = 3600;            // 费率每 8 小时结算一次, 每小时扫描足够
const VOL24_MIN: f64 = 20_000_000.0;    // 过滤流动性差的币, 防止年化好看却进不去

async fn fetch_json(http: &reqwest::Client, url: &str) -> Option<serde_json::Value> {
    http.get(url).send().await.ok()?.json().await.ok()
}

pub async fn run_funding_monitor(redis_client: redis::Client, tg_tx: mpsc::Sender<String>) {
    let http = reqwest::Client::new();
    let report_offset = time::UtcOffset::from_hms(8, 0, 0).unwrap();
    info!("💰 资金费率监控已启动 (仅侦察, 每小时扫描, 不下单)");

    loop {
        let mut con = match redis_client.get_multiplexed_async_connection().await {
            Ok(c) => c,
            Err(_) => {
                tokio::time::sleep(std::time::Duration::from_secs(60)).await;
                continue;
            }
        };
        // 年化超过该阈值的币即时提醒, Redis 可调
        let alert_annual: f64 = redis::cmd("GET").arg("FUNDING_ALERT_ANNUAL").query_async::<Option<String>>(&mut con).await
            .ok().flatten().and_then(|s| s.parse().ok()).unwrap_or(50.0);

        let Some(premium) = fetch_json(&http, "https://fapi.binance.com/fapi/v1/premiumIndex").await else {
            tokio::time::sleep(std::time::Duration::from_secs(300)).await;
            continue;
        };
        let Some(tickers) = fetch_json(&http, "https://fapi.binance.com/fapi/v1/ticker/24hr").await else {
            tokio::time::sleep(std::time::Duration::from_secs(300)).await;
            continue;
        };
        let mut vol_map = std::collections::HashMap::new();
        if let Some(arr) = tickers.as_array() {
            for t in arr {
                if let (Some(sym), Some(v)) = (t["symbol"].as_str(), t["quoteVolume"].as_str().and_then(|s| s.parse::<f64>().ok())) {
                    vol_map.insert(sym.to_string(), v);
                }
            }
        }

        // (sym, 单期费率%, 年化%)
        let mut rates: Vec<(String, f64, f64)> = Vec::new();
        if let Some(arr) = premium.as_array() {
            for p in arr {
                let sym = p["symbol"].as_str().unwrap_or("");
                if !sym.ends_with("USDT") || sym.contains('_') {
                    continue;
                }
                if vol_map.get(sym).copied().unwrap_or(0.0) < VOL24_MIN {
                    continue;
                }
                let Some(rate) = p["lastFundingRate"].as_str().and_then(|s| s.parse::<f64>().ok()) else { continue };
                let pct = rate * 100.0;
                rates.push((sym.to_string(), pct, pct * 3.0 * 365.0));
            }
        }
        rates.sort_by(|a, b| b.2.partial_cmp(&a.2).unwrap_or(std::cmp::Ordering::Equal));

        // ---------- 极端费率即时提醒 (每币每个结算周期最多一次, 用 Redis EX 去重) ----------
        for (sym, pct, annual) in &rates {
            if annual.abs() < alert_annual {
                continue;
            }
            let dedup: Option<String> = redis::cmd("SET").arg(format!("FUNDING_ALERTED_{}", sym))
                .arg("1").arg("NX").arg("EX").arg(8 * 3600)
                .query_async(&mut con).await.ok();
            if dedup.is_none() {
                continue; // 本周期已提醒过
            }
            let side = if *pct > 0.0 { "多头付费给空头 → 空永续+持现货可收" } else { "空头付费给多头 → 反向" };
            let _ = tg_tx.send(format!(
                "💰 <b>【极端资金费率】</b> {}\n单期: {:+.4}% | 年化: <b>{:+.1}%</b>\n{}\n⚠️ 仅侦察情报, 引擎不会下单。",
                sym, pct, annual, side)).await;
        }

        // ---------- 每日汇总 (20点后第一次扫描, Redis 去重防重启重发) ----------
        let now = time::OffsetDateTime::now_utc().to_offset(report_offset);
        if now.hour() >= 20 && !rates.is_empty() {
            let day = format!("{:04}-{:02}-{:02}", now.year(), now.month() as u8, now.day());
            let reported = redis::cmd("GET").arg("FUNDING_LAST_REPORT_DAY").query_async::<Option<String>>(&mut con).await.ok().flatten().unwrap_or_default();
            if reported != day {
                let _: () = redis::cmd("SET").arg("FUNDING_LAST_REPORT_DAY").arg(&day).query_async(&mut con).await.unwrap_or(());
                let mut lines = String::from("正费率榜 (空永续+持现货可收):\n");
                for (sym, pct, annual) in rates.iter().take(5) {
                    lines.push_str(&format!("  • {} {:+.4}%/期 ≈ 年化 {:+.1}%\n", sym, pct, annual));
                }
                lines.push_str("\n负费率榜:\n");
                for (sym, pct, annual) in rates.iter().rev().take(5) {
                    lines.push_str(&format!("  • {} {:+.4}%/期 ≈ 年化 {:+.1}%\n", sym, pct, annual));
                }
                let _ = tg_tx.send(format!(
                    "💰 <b>【资金费率日报】</b> {}\n(已过滤 24h 成交额 < 2000万U 的币)\n\n{}\n年化是瞬时值外推, 费率会衰减; 真实可捕获收益要扣现货腿占用与两边手续费。",
                    day, lines)).await;
            }
        }

        tokio::time::sleep(std::time::Duration::from_secs(SCAN_SECS)).await;
    }
}
