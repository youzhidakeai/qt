// ==========================================
// MODULE: 操纵嫌疑雷达 (Manipulation Radar) — 影子模式
// 对用户当前持仓的币, 每 60 秒用公开数据算四类操纵嫌疑信号:
//   1. 拉盘出货末段 (pump & dump 分布阶段): 急拉后深回撤/滞涨
//   2. 过热追高危险: 短时暴涨 + 极端爆量 + 资金费率过热同时出现
//   3. 插针密集 (stop hunt 高发区): 近 1 小时长影线比例异常
//   4. 量深比异常 (wash trading 代理指标): 成交量巨大但盘口深度很薄
// 只警告不拦截 (影子模式); 每币每小时最多提醒一次, Redis 去重。
// 诚实声明: 全部是启发式代理指标, 不是链分析级的账户证据 —— 用于提醒
// 用户"这个币此刻的微观结构不干净", 不是操纵的司法认定。
// ==========================================
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::info;

use crate::execution::BinanceExecutionClient;

const SCAN_SECS: u64 = 60;
const ALERT_COOLDOWN_SECS: u64 = 3600;

// 阈值 (影子模式先用保守值跑, 按实况误报率再调)
const PD_RISE_PCT: f64 = 8.0;        // 1小时内涨幅超过此值才谈"拉盘"
const PD_RETRACE_RATIO: f64 = 0.5;   // 从峰值回吐超过涨幅的一半 = 出货嫌疑
const HOT_RET5_PCT: f64 = 3.0;
const HOT_VSURGE: f64 = 10.0;
const HOT_FUNDING_ANNUAL: f64 = 50.0;
const WICK_PCT: f64 = 0.7;           // 单根影线超过收盘价 0.7% 算长针
const WICK_COUNT_MIN: usize = 8;     // 近 60 根里长针数量达到此值 = 扫针密集
const VOL_DEPTH_RATIO_MAX: f64 = 4000.0; // 24h成交额 / 盘口±1%深度 超过此值 = 深度虚

fn kf(k: &serde_json::Value, i: usize) -> f64 {
    k.get(i).and_then(|x| x.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0.0)
}

async fn fetch_json(http: &reqwest::Client, url: &str) -> Option<serde_json::Value> {
    http.get(url).send().await.ok()?.json().await.ok()
}

// 返回该币的嫌疑标签列表 (空 = 干净)
async fn inspect(http: &reqwest::Client, sym: &str) -> Vec<String> {
    let mut flags = Vec::new();

    // K线: 1500 根 1m (爆量中位数口径与 paper.rs 一致)
    let Some(kl) = fetch_json(http, &format!("https://fapi.binance.com/fapi/v1/klines?symbol={}&interval=1m&limit=1500", sym)).await else { return flags };
    let Some(arr) = kl.as_array() else { return flags };
    if arr.len() < 120 {
        return flags;
    }
    let bars = &arr[..arr.len() - 1];
    let n = bars.len();
    let close = |i: usize| kf(&bars[i], 4);
    let high = |i: usize| kf(&bars[i], 2);
    let low = |i: usize| kf(&bars[i], 3);
    let open = |i: usize| kf(&bars[i], 1);
    let qv = |i: usize| kf(&bars[i], 7);
    let last = close(n - 1);
    if last <= 0.0 {
        return flags;
    }

    // ---- 1. 拉盘出货末段 ----
    let hour_lo = (n - 60..n).map(low).fold(f64::MAX, f64::min);
    let hour_hi = (n - 60..n).map(high).fold(0.0f64, f64::max);
    if hour_lo > 0.0 {
        let rise = (hour_hi / hour_lo - 1.0) * 100.0;
        if rise > PD_RISE_PCT {
            let retrace_ratio = (hour_hi - last) / (hour_hi - hour_lo).max(f64::EPSILON);
            if retrace_ratio > PD_RETRACE_RATIO {
                flags.push(format!("拉盘出货嫌疑: 1h内拉 {:.1}% 后已回吐 {:.0}%", rise, retrace_ratio * 100.0));
            }
        }
    }

    // ---- 2. 过热追高危险 (三个条件同时) ----
    let ret5 = (last / close(n - 6) - 1.0) * 100.0;
    let mut v5s: Vec<f64> = Vec::with_capacity(n);
    let mut acc = 0.0;
    for i in 0..n {
        acc += qv(i);
        if i >= 5 { acc -= qv(i - 5); }
        if i >= 4 { v5s.push(acc); }
    }
    let mut window: Vec<f64> = v5s[v5s.len().saturating_sub(1440)..].to_vec();
    window.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let median = window[window.len() / 2].max(f64::EPSILON);
    let vsurge = v5s.last().unwrap_or(&0.0) / median;
    if ret5 > HOT_RET5_PCT && vsurge > HOT_VSURGE {
        let funding = fetch_json(http, &format!("https://fapi.binance.com/fapi/v1/premiumIndex?symbol={}", sym)).await
            .and_then(|v| v["lastFundingRate"].as_str().and_then(|s| s.parse::<f64>().ok()))
            .map(|r| r * 100.0 * 3.0 * 365.0)
            .unwrap_or(0.0);
        if funding > HOT_FUNDING_ANNUAL {
            flags.push(format!("过热追高危险: 5m {:+.1}% + 爆量{:.0}x + 费率年化{:+.0}%", ret5, vsurge, funding));
        }
    }

    // ---- 3. 插针密集 ----
    let mut wicks = 0usize;
    for i in n - 60..n {
        let body_hi = open(i).max(close(i));
        let body_lo = open(i).min(close(i));
        let up_wick = (high(i) - body_hi) / close(i).max(f64::EPSILON) * 100.0;
        let dn_wick = (body_lo - low(i)) / close(i).max(f64::EPSILON) * 100.0;
        if up_wick > WICK_PCT || dn_wick > WICK_PCT {
            wicks += 1;
        }
    }
    if wicks >= WICK_COUNT_MIN {
        flags.push(format!("插针密集(扫针高发): 近1小时 {} 根长影线", wicks));
    }

    // ---- 4. 量深比异常 (刷量代理) ----
    if let Some(depth) = fetch_json(http, &format!("https://fapi.binance.com/fapi/v1/depth?symbol={}&limit=100", sym)).await {
        let mut depth_notional = 0.0;
        for side in ["bids", "asks"] {
            if let Some(levels) = depth[side].as_array() {
                for l in levels {
                    let p: f64 = l[0].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                    let q: f64 = l[1].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                    if (p / last - 1.0).abs() <= 0.01 {
                        depth_notional += p * q;
                    }
                }
            }
        }
        let vol24: f64 = (0..n.min(1440)).map(|i| qv(n - 1 - i)).sum();
        if depth_notional > 0.0 {
            let ratio = vol24 / depth_notional;
            if ratio > VOL_DEPTH_RATIO_MAX {
                flags.push(format!("量深比异常(刷量嫌疑): 24h量/±1%深度 = {:.0} (深度仅 {:.0}U)", ratio, depth_notional));
            }
        }
    }

    flags
}

pub async fn run_manip_radar(exec: Arc<BinanceExecutionClient>, redis_client: redis::Client, tg_tx: mpsc::Sender<String>) {
    let http = reqwest::Client::new();
    info!("🕵️ 操纵嫌疑雷达已启动 (影子模式: 只对持仓币警告, 不拦截任何操作)");

    loop {
        tokio::time::sleep(std::time::Duration::from_secs(SCAN_SECS)).await;
        let mut con = match redis_client.get_multiplexed_async_connection().await {
            Ok(c) => c,
            Err(_) => continue,
        };
        let Ok(pos_str) = exec.check_positions().await else { continue };
        let positions: Vec<serde_json::Value> = serde_json::from_str(&pos_str).unwrap_or_default();
        for pos in &positions {
            let amt: f64 = pos.get("positionAmt").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0.0);
            if amt == 0.0 {
                continue;
            }
            let sym = pos.get("symbol").and_then(|v| v.as_str()).unwrap_or("").to_string();
            let flags = inspect(&http, &sym).await;
            if flags.is_empty() {
                continue;
            }
            info!("🕵️ [{}] 操纵嫌疑: {}", sym, flags.join(" | "));
            // 每币每小时最多打扰一次
            let dedup: Option<String> = redis::cmd("SET").arg(format!("MANIP_ALERTED_{}", sym))
                .arg("1").arg("NX").arg("EX").arg(ALERT_COOLDOWN_SECS)
                .query_async(&mut con).await.ok();
            if dedup.is_some() {
                let _ = tg_tx.send(format!(
                    "🕵️ <b>【操纵嫌疑提醒】</b> {} (你有持仓)\n\n{}\n\n⚠️ 启发式信号仅供参考, 系统不会因此自动操作。你的止损按标记价格触发, 对扫针已有基础免疫。",
                    sym, flags.iter().map(|f| format!("• {}", f)).collect::<Vec<_>>().join("\n"))).await;
            }
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
    }
}
