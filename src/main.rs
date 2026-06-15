mod models;
mod gateway;
mod orderbook;
mod execution;
mod strategy;
mod telegram;
mod portfolio;
mod ml_engine;

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::info;
use rust_decimal::Decimal;
use std::str::FromStr;

use orderbook::OrderBookManager;
use models::{DepthUpdate, AggTradeUpdate};
use execution::BinanceExecutionClient;
use strategy::{StrategyEngine, ControlMessage};
use tracing_subscriber::fmt::time::OffsetTime;
use time::macros::format_description;
use time::UtcOffset;
use teloxide::prelude::*;

pub struct SymbolContext {
    pub ob_manager: Arc<OrderBookManager>,
    pub control_tx: mpsc::Sender<ControlMessage>,
}

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok(); // 加载 .env 文件
    
    // 配置东八区时间 (UTC+8)
    let offset = UtcOffset::from_hms(8, 0, 0).unwrap();
    let timer = OffsetTime::new(offset, format_description!("[year]-[month]-[day] [hour]:[minute]:[second].[subsecond digits:3]"));

    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env().add_directive("info".parse().unwrap()))
        .with_timer(timer)          // 使用自定义的东八区时间格式器
        .with_target(false)         // 隐藏冗长的包名路径
        .with_thread_ids(true)      // 打印并发线程 ID
        .with_file(true)            // 显示触发日志的源码文件
        .with_line_number(true)     // 显示代码行号
        .compact()                  // 紧凑的终端输出风格
        .init();

    info!("Starting Quantitative Trading Engine Matrix...");

    // 从环境变量或 .env 读取 API 密钥
    let api_key = std::env::var("BINANCE_API_KEY").expect("请在 .env 中设置 BINANCE_API_KEY");
    let api_secret = std::env::var("BINANCE_API_SECRET").expect("请在 .env 中设置 BINANCE_API_SECRET");
    let tg_chat_id: i64 = std::env::var("TELEGRAM_CHAT_ID")
        .expect("请在 .env 中设置 TELEGRAM_CHAT_ID")
        .parse()
        .expect("TELEGRAM_CHAT_ID 必须是数字");
    let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1/".to_string());
    
    let exec_client = Arc::new(BinanceExecutionClient::new(&api_key, &api_secret));
    let redis_client = redis::Client::open(redis_url).expect("Failed to connect to Redis");
    
    let (signal_tx, signal_rx) = tokio::sync::mpsc::channel::<portfolio::SignalEvent>(100);
    
    // ==========================================
    // MODULE: Telegram 战报推送通道
    // ==========================================
    let (tg_tx, mut tg_rx) = mpsc::channel::<String>(100);
    let bot_notifier = Bot::from_env();
    tokio::spawn(async move {
        while let Some(msg) = tg_rx.recv().await {
            let _ = bot_notifier.send_message(ChatId(tg_chat_id), msg).parse_mode(teloxide::types::ParseMode::Html).await;
        }
    });
    // ==========================================
    // MODULE: 动态拉取币安测试网交易热度榜单
    // ==========================================
    let top_n: usize = std::env::var("TOP_SYMBOLS_COUNT").unwrap_or_else(|_| "5".to_string()).parse().unwrap_or(5);
    info!("正在请求币安测试网，获取测试网排名前 {} 的热门币种...", top_n);
    let top_symbols = fetch_top_symbols(top_n).await;
    info!("🔥 锁定当前热度榜单: {:?}", top_symbols);
    
    let symbols = top_symbols;
    let mut tg_contexts = HashMap::new();
    let mut control_senders = HashMap::new();

    for symbol in &symbols {
        let sym_str = symbol.to_string();
        
        let ob_manager = Arc::new(OrderBookManager::new(&sym_str));
        let (tx, mut rx) = mpsc::channel::<DepthUpdate>(1000);
        let (agg_tx, mut agg_rx) = mpsc::channel::<AggTradeUpdate>(1000);
        let (control_tx, mut control_rx) = mpsc::channel::<ControlMessage>(10);
        
        control_senders.insert(sym_str.clone(), control_tx.clone());
        tg_contexts.insert(sym_str.clone(), SymbolContext {
            ob_manager: ob_manager.clone(),
            control_tx: control_tx.clone(),
        });

        // 1. 深度网关
        let gw_sym = sym_str.clone();
        tokio::spawn(async move {
            gateway::run_binance_ws(&gw_sym, tx).await;
        });

        // 2. 真实吃单流网关 (Trade Flow)
        let agg_sym = sym_str.clone();
        tokio::spawn(async move {
            gateway::run_aggtrade_ws(&agg_sym, agg_tx).await;
        });

        // 3. 策略大脑任务
        let ob_clone = ob_manager.clone();
        let brain_sym = sym_str.clone();
        let r_client = redis_client.clone();
        let sig_tx = signal_tx.clone();
        let exec_clone = exec_client.clone();
        let tg_tx_clone = tg_tx.clone();
        
        tokio::spawn(async move {
            let mut brain = StrategyEngine::new(exec_clone, ob_clone.clone(), control_tx, &brain_sym, r_client, sig_tx, tg_tx_clone).await;
            loop {
                tokio::select! {
                    Some(update) = rx.recv() => {
                        ob_clone.apply_update(&update.bids, &update.asks);
                        brain.evaluate_market().await;
                    }
                    Some(trade) = agg_rx.recv() => {
                        brain.handle_trade(trade).await;
                    }
                    Some(msg) = control_rx.recv() => {
                        brain.handle_control_message(msg).await;
                    }
                }
            }
        });
        
        info!("✅ {} 并发节点启动完成 (Depth + AggTrade + Strategy Engine)", sym_str);
    }

    // ==========================================
    // MODULE: Telegram 全局中控台
    // ==========================================
    let tg_ctx_arc = Arc::new(tg_contexts);
    let tg_ctx_bot = tg_ctx_arc.clone();
    let exec_client_bot = exec_client.clone();
    tokio::spawn(async move {
        telegram::run_telegram_bot(exec_client_bot, tg_ctx_bot).await;
    });

    let portfolio_exec = exec_client.clone();
    let portfolio_tg = tg_tx.clone();
    tokio::spawn(async move {
        let pm = portfolio::PortfolioManager::new(portfolio_exec, control_senders, signal_rx, portfolio_tg);
        pm.run().await;
    });

    // ==========================================
    // MODULE: 5分钟行情定时播报
    // ==========================================
    let tg_ctx_ticker = tg_ctx_arc.clone();
    let tg_tx_ticker = tg_tx.clone();
    let redis_ticker = redis_client.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(300));
        let mut last_prices: std::collections::HashMap<String, Decimal> = std::collections::HashMap::new();
        loop {
            interval.tick().await;
            let mut report = String::from("⏱️ <b>[行情 5 分钟雷达扫瞄]</b>\n\n");
            let mut sym_keys: Vec<&String> = tg_ctx_ticker.keys().collect();
            sym_keys.sort(); // 保持固定的字母顺序
            
            for sym in sym_keys {
                if let Some(ctx) = tg_ctx_ticker.get(sym) {
                    let price = ctx.ob_manager.book.read().unwrap().bids.iter().next_back().map(|(p, _)| *p).unwrap_or(Decimal::ZERO);
                    
                    let mut extra_info = String::new();
                    if let Ok(mut con) = redis_ticker.get_multiplexed_async_connection().await {
                        let key = format!("{}_state", sym);
                        if let Ok(state_content) = redis::cmd("GET").arg(&key).query_async::<String>(&mut con).await {
                            if let Ok(state) = serde_json::from_str::<serde_json::Value>(&state_content) {
                                let amt_str = state.get("position_amt").and_then(|v| v.as_str()).unwrap_or("0");
                                if let Ok(amt) = Decimal::from_str(amt_str) {
                                if amt.abs() > Decimal::ZERO {
                                    let entry = state.get("entry_price").and_then(|v| v.as_str()).unwrap_or("0").parse::<Decimal>().unwrap_or_default();
                                    let unpnl = (price - entry) * amt; // gross pnl
                                    let sl_price = if amt > Decimal::ZERO {
                                        let high = state.get("highest_price_since_entry").and_then(|v| v.as_str()).unwrap_or("0").parse::<Decimal>().unwrap_or_default();
                                        high * (Decimal::ONE - Decimal::from_str("0.015").unwrap())
                                    } else {
                                        let low = state.get("lowest_price_since_entry").and_then(|v| v.as_str()).unwrap_or("0").parse::<Decimal>().unwrap_or_default();
                                        low * (Decimal::ONE + Decimal::from_str("0.015").unwrap())
                                    };
                                    
                                    let dir = if amt > Decimal::ZERO { "🟢 多" } else { "🔴 空" };
                                    extra_info = format!("\n    ↪ {}仓 | 均价: {} | 盈亏: {:.2}U | 预计平仓(止损)价: {:.4}", dir, entry, unpnl, sl_price);
                                }
                            }
                        }
                    }
                    }

                    let mut delta_str = String::new();
                    if let Some(last_p) = last_prices.get(sym) {
                        if *last_p > Decimal::ZERO && price > Decimal::ZERO {
                            let pct = (price - *last_p) / *last_p * rust_decimal_macros::dec!(100);
                            if pct > Decimal::ZERO {
                                delta_str = format!(" (📈 +{:.3}%)", pct);
                            } else if pct < Decimal::ZERO {
                                delta_str = format!(" (📉 {:.3}%)", pct);
                            } else {
                                delta_str = format!(" (➖ 0.000%)");
                            }
                        }
                    }
                    if price > Decimal::ZERO {
                        last_prices.insert((*sym).clone(), price);
                        report.push_str(&format!("🔹 <b>{}</b>: {}{}{}\n", sym, price, delta_str, extra_info));
                    } else {
                        report.push_str(&format!("🔹 <b>{}</b>: 数据获取中...\n", sym));
                    }
                }
            }
            let _ = tg_tx_ticker.send(report).await;
        }
    });

    // ==========================================
    // MODULE: 每日 0点 自动播报任务
    // ==========================================
    let exec_daily = exec_client.clone();
    let report_exec = exec_client.clone();
    let report_tg = tg_tx.clone();
    tokio::spawn(async move {
        run_hourly_report(report_exec, report_tg).await;
    });

    let tg_tx_daily = tg_tx.clone();
    tokio::spawn(async move {
        use time::{OffsetDateTime, UtcOffset, Duration, Time};
        let offset = UtcOffset::from_hms(8, 0, 0).unwrap();
        
        loop {
            let now = OffsetDateTime::now_utc().to_offset(offset);
            let mut next_midnight = now.replace_time(Time::MIDNIGHT);
            if next_midnight <= now {
                next_midnight = next_midnight + Duration::days(1);
            }
            
            let sleep_secs = (next_midnight - now).whole_seconds() as u64;
            tokio::time::sleep(std::time::Duration::from_secs(sleep_secs)).await;
            
            let now_midnight = OffsetDateTime::now_utc().to_offset(offset).replace_time(Time::MIDNIGHT);
            let start_of_yesterday = now_midnight - Duration::days(1);
            let end_of_yesterday = now_midnight - Duration::nanoseconds(1);
            
            let start_ts = start_of_yesterday.unix_timestamp() * 1000;
            let end_ts = end_of_yesterday.unix_timestamp() * 1000;
            
            if let Ok(res) = exec_daily.get_income_history(start_ts as u64, end_ts as u64).await {
                if let Ok(records) = serde_json::from_str::<serde_json::Value>(&res) {
                    if let Some(arr) = records.as_array() {
                        let mut total_pnl = rust_decimal::Decimal::ZERO;
                        let mut total_fee = rust_decimal::Decimal::ZERO;
                        let mut total_funding = rust_decimal::Decimal::ZERO;
                        
                        for item in arr {
                            if let (Some(income_type), Some(income)) = (item.get("incomeType").and_then(|v| v.as_str()), item.get("income").and_then(|v| v.as_str())) {
                                if let Ok(val) = rust_decimal::Decimal::from_str(income) {
                                    match income_type {
                                        "REALIZED_PNL" => total_pnl += val,
                                        "COMMISSION" => total_fee += val,
                                        "FUNDING_FEE" => total_funding += val,
                                        _ => {}
                                    }
                                }
                            }
                        }
                        
                        let net_income = total_pnl + total_fee + total_funding;
                        let report = format!(
                            "⏰ <b>零点播报</b> 📊 <b>昨日盈亏总结 (UTC+8)</b>\n\n\
                            区间: {} 00:00 ~ 23:59\n\
                            \n\
                            💰 净收益: <b>{:.2} USDT</b>\n\
                            -------------------------\n\
                            📈 实现盈亏: {:.2} USDT\n\
                            📉 交易手续费: {:.2} USDT\n\
                            ⏱ 资金费率: {:.2} USDT",
                            start_of_yesterday.date(), net_income, total_pnl, total_fee, total_funding
                        );
                        let _ = tg_tx_daily.send(report).await;
                    }
                }
            }
        }
    });

    info!("🚀 所有 {} 个交易节点就绪，进入无穷循环待命...", symbols.len());
    let _ = tg_tx.send(format!("🟢 矩阵引擎系统启动完毕！\n\n已成功挂载 {} 个交易对的 WebSocket 流动性监听网络。\n全自动微秒级突破狙击（带吃单流验证）已准备就绪，随时开火。", symbols.len())).await;
    
    tokio::signal::ctrl_c().await.unwrap();
    info!("系统正在关闭...");
}

// 从币安主网 (Mainnet) 拉取真实成交额最高的合约币种，从而避开测试网上产生的那些诸如 JELLYJELLY 的垃圾假币
async fn fetch_top_symbols(limit: usize) -> Vec<String> {
    let url = "https://fapi.binance.com/fapi/v1/ticker/24hr";
    let client = reqwest::Client::new();
    if let Ok(resp) = client.get(url).send().await {
        if let Ok(text) = resp.text().await {
            if let Ok(mut tickers) = serde_json::from_str::<Vec<serde_json::Value>>(&text) {
                tickers.retain(|t| {
                    let sym = t["symbol"].as_str().unwrap_or("");
                    sym.ends_with("USDT") && !sym.contains("_")
                });

                tickers.sort_by(|a, b| {
                    let vol_a = a["quoteVolume"].as_str().unwrap_or("0").parse::<f64>().unwrap_or(0.0);
                    let vol_b = b["quoteVolume"].as_str().unwrap_or("0").parse::<f64>().unwrap_or(0.0);
                    vol_b.partial_cmp(&vol_a).unwrap_or(std::cmp::Ordering::Equal)
                });

                return tickers.into_iter()
                    .take(limit)
                    .map(|t| t["symbol"].as_str().unwrap().to_string())
                    .collect();
            }
        }
    }
    // 如果获取失败，返回保底币种
    vec!["BTCUSDT".to_string(), "ETHUSDT".to_string(), "BNBUSDT".to_string(), "DOGEUSDT".to_string()]
}

// ==========================================
// MODULE: 资金流水与持仓报表引擎 (Hourly Telegram Report)
// ==========================================
pub async fn run_hourly_report(exec_client: Arc<BinanceExecutionClient>, tg_tx: mpsc::Sender<String>) {
    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(3600)).await;
        
        let mut report = String::from("📊 <b>机构级量化数据监控仓</b> (每小时快照)\n\n");
        
        if let Ok(account_str) = exec_client.check_account().await {
            if let Ok(account) = serde_json::from_str::<serde_json::Value>(&account_str) {
                let total = account.get("totalWalletBalance").and_then(|v| v.as_str()).unwrap_or("0");
                let upnl = account.get("totalUnrealizedProfit").and_then(|v| v.as_str()).unwrap_or("0");
                report.push_str(&format!("💰 总权益: {} USDT\n", total));
                report.push_str(&format!("📈 未实现盈亏: {} USDT\n\n", upnl));
            }
        }

        report.push_str("⚔️ <b>当前前线持仓</b>:\n");
        let mut has_positions = false;
        if let Ok(pos_str) = exec_client.check_positions().await {
            if let Ok(positions) = serde_json::from_str::<Vec<serde_json::Value>>(&pos_str) {
                for pos in positions {
                    if let Some(amt_str) = pos.get("positionAmt").and_then(|v| v.as_str()) {
                        if let Ok(amt) = amt_str.parse::<f64>() {
                            if amt != 0.0 {
                                has_positions = true;
                                let sym = pos.get("symbol").and_then(|v| v.as_str()).unwrap_or("");
                                let entry = pos.get("entryPrice").and_then(|v| v.as_str()).unwrap_or("0");
                                let unpl = pos.get("unRealizedProfit").and_then(|v| v.as_str()).unwrap_or("0");
                                let dir = if amt > 0.0 { "🟢 多" } else { "🔴 空" };
                                report.push_str(&format!("{} {} | 量: {} | 入场: {} | 盈亏: {}\n", dir, sym, amt, entry, unpl));
                            }
                        }
                    }
                }
            }
        }
        
        if !has_positions {
            report.push_str("空仓挂机中，等待绝佳猎杀时刻...\n");
        }

        let _ = tg_tx.send(report).await;
    }
}

// ==========================================
// MODULE: 机器学习模型权重热重载 (Hot Reload)
// ==========================================
pub async fn run_ml_hot_reload(redis_client: redis::Client) {
    use redis::AsyncCommands;
    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
        
        if let Ok(mut con) = redis_client.get_multiplexed_async_connection().await {
            if let Ok(weights_json) = con.get::<_, String>("ML_WEIGHTS").await {
                if let Ok(new_nn) = serde_json::from_str::<crate::ml_engine::NeuralNetwork>(&weights_json) {
                    // 原子化热重载权重矩阵！(Nano-second Lock-Free Swap)
                    crate::ml_engine::GLOBAL_NN.store(std::sync::Arc::new(new_nn));
                    tracing::info!("🧠 [热重载] 成功从 Redis 加载了最新的深度学习网络权重！");
                } else {
                    tracing::error!("🧠 [热重载] ML_WEIGHTS 解析 JSON 失败，保持使用旧权重。");
                }
            }
        }
    }
}
