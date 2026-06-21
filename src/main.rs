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
use tracing::{info, error};
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
    // MODULE: 动态加载与管理订阅币种
    // ==========================================
    let mut redis_con = redis_client.get_multiplexed_async_connection().await.expect("无法连接 Redis");
    let mut symbols: Vec<String> = redis::cmd("SMEMBERS").arg("SUBSCRIBED_SYMBOLS").query_async(&mut redis_con).await.unwrap_or_default();
    
    if symbols.is_empty() {
        let top_n: usize = std::env::var("TOP_SYMBOLS_COUNT").unwrap_or_else(|_| "5".to_string()).parse().unwrap_or(5);
        info!("Redis 订阅列表为空，正在拉取测试网排名前 {} 的热门币种作为初始订阅...", top_n);
        symbols = fetch_top_symbols(top_n).await;
        
        let mut pipe = redis::pipe();
        for sym in &symbols {
            pipe.cmd("SADD").arg("SUBSCRIBED_SYMBOLS").arg(sym);
        }
        let _: () = pipe.query_async(&mut redis_con).await.unwrap_or_default();
    }
    
    info!("🔥 锁定当前监控名单 (共 {} 个): {:?}", symbols.len(), symbols);
    let mut tg_contexts = HashMap::new();
    let mut control_senders = HashMap::new();

    let (feature_tx, mut feature_rx) = mpsc::channel::<String>(100000);
    
    let redis_logger = redis_client.clone();
    tokio::spawn(async move {
        loop {
            match redis_logger.get_multiplexed_async_connection().await {
                Ok(mut con) => {
                    tracing::info!("✅ Redis 特征流异步管道连接成功，等待数据...");
                    while let Some(json_str) = feature_rx.recv().await {
                        let res: Result<(), redis::RedisError> = redis::cmd("XADD")
                            .arg("ML_FEATURE_STREAM")
                            .arg("*")
                            .arg("data")
                            .arg(&json_str)
                            .query_async(&mut con).await;
                        
                        if let Err(e) = res {
                            tracing::error!("❌ 写入 Redis Stream 失败: {}", e);
                            break; // 退出 while，触发重新连接
                        }
                    }
                }
                Err(e) => {
                    tracing::error!("❌ Redis 管道连接失败，5秒后重试: {}", e);
                    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                }
            }
        }
    });

    for sym in symbols.iter() {
        let sym_str = sym.clone();
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
        let feat_tx_clone = feature_tx.clone();
        
        tokio::spawn(async move {
            let mut brain = StrategyEngine::new(exec_clone, ob_clone.clone(), control_tx, &brain_sym, r_client, sig_tx, tg_tx_clone, feat_tx_clone).await;
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
    // MODULE: 全网交易规范 (Exchange Info)
    // ==========================================
    info!("正在拉取全网交易规范 (Exchange Info)...");
    let exchange_info = match exec_client.fetch_exchange_info().await {
        Ok(info) => Arc::new(info),
        Err(e) => {
            error!("拉取 Exchange Info 失败: {}，将使用默认极小精度进行交易。", e);
            Arc::new(HashMap::new())
        }
    };

    // ==========================================
    // MODULE: Telegram 全局中控台
    // ==========================================
    let tg_ctx_arc = Arc::new(tg_contexts);
    let tg_ctx_bot = tg_ctx_arc.clone();
    let exec_client_bot = exec_client.clone();
    let tg_exchange = exchange_info.clone();
    tokio::spawn(async move {
        telegram::run_telegram_bot(exec_client_bot, tg_ctx_bot, tg_exchange).await;
    });

    let portfolio_exec = exec_client.clone();
    let portfolio_tg = tg_tx.clone();
    let portfolio_exchange = exchange_info.clone();
    let control_senders_clone = control_senders.clone();
    let portfolio_redis = redis_client.clone();
    tokio::spawn(async move {
        let pm = portfolio::PortfolioManager::new(portfolio_exec, control_senders_clone, signal_rx, portfolio_tg, portfolio_exchange, portfolio_redis);
        pm.run().await;
    });

    // ==========================================
    // MODULE: 10秒仓位“自愈”定时轮询 (Self-Healing Poller)
    // ==========================================
    let sync_exec = exec_client.clone();
    let sync_control = control_senders.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
            if let Ok(pos_str) = sync_exec.check_positions().await {
                if let Ok(positions) = serde_json::from_str::<Vec<serde_json::Value>>(&pos_str) {
                    for pos in positions {
                        let amt = pos.get("positionAmt").and_then(|v| v.as_str()).and_then(|s| rust_decimal::Decimal::from_str(s).ok()).unwrap_or(rust_decimal::Decimal::ZERO);
                        let sym = pos.get("symbol").and_then(|v| v.as_str()).unwrap_or("").to_string();
                        if let Some(tx) = sync_control.get(&sym) {
                            let entry = pos.get("entryPrice").and_then(|v| v.as_str()).and_then(|s| rust_decimal::Decimal::from_str(s).ok()).unwrap_or(rust_decimal::Decimal::ZERO);
                            let _ = tx.send(crate::strategy::ControlMessage::ForceUpdatePosition { amt, entry }).await;
                        }
                    }
                }
            }
        }
    });

    // ==========================================
    // MODULE: 全局大盘熔断器 (Global Circuit Breaker)
    // ==========================================
    let circuit_tg = tg_tx.clone();
    let circuit_ctx = tg_ctx_arc.clone();
    tokio::spawn(async move {
        let mut histories: std::collections::HashMap<&str, std::collections::VecDeque<rust_decimal::Decimal>> = std::collections::HashMap::new();
        histories.insert("BTCUSDT", std::collections::VecDeque::new());
        histories.insert("ETHUSDT", std::collections::VecDeque::new());
        histories.insert("BNBUSDT", std::collections::VecDeque::new());
        
        let mut panic_mode = false;
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
            
            let mut min_drop_pct = rust_decimal_macros::dec!(0.0);
            let mut trigger_symbol = "";
            let mut all_recovered = true;

            for sym in ["BTCUSDT", "ETHUSDT", "BNBUSDT"] {
                if let Some(ctx) = circuit_ctx.get(sym) {
                    let current_price = {
                        let ob = ctx.ob_manager.book.read().unwrap();
                        ob.bids.iter().next_back().map(|(p, _)| *p).unwrap_or(rust_decimal::Decimal::ZERO)
                    };
                    if current_price > rust_decimal::Decimal::ZERO {
                        let history = histories.get_mut(sym).unwrap();
                        history.push_back(current_price);
                        if history.len() > 15 {
                            history.pop_front();
                        }
                        
                        let max_price = history.iter().max_by(|a, b| a.partial_cmp(b).unwrap()).unwrap_or(&current_price);
                        let drop_pct = (current_price - *max_price) / *max_price * rust_decimal_macros::dec!(100);
                        
                        if drop_pct < min_drop_pct {
                            min_drop_pct = drop_pct;
                            trigger_symbol = sym;
                        }
                        if drop_pct < rust_decimal_macros::dec!(-0.4) {
                            all_recovered = false;
                        }
                    }
                }
            }
            
            if min_drop_pct <= rust_decimal_macros::dec!(-1.2) { // 任一巨头 15分钟内回撤超1.2%视为血洗
                if !panic_mode {
                    panic_mode = true;
                    let _ = circuit_tg.send(format!("🚨 <b>大盘熔断警报</b> 🚨\n领跌巨头 {} 在过去 15 分钟内暴跌 {}%！系统已自动启动最高级别防御：\n⛔️ <b>强制暂停所有多单开仓</b>\n(已有仓位的移动止损继续生效)", trigger_symbol.replace("USDT", ""), min_drop_pct.round_dp(2))).await;
                    for ctx in circuit_ctx.values() {
                        let _ = ctx.control_tx.send(crate::strategy::ControlMessage::PauseTrading).await;
                    }
                }
            } else if all_recovered && panic_mode {
                panic_mode = false;
                let _ = circuit_tg.send("🌤 <b>大盘企稳警报</b>\nBTC/ETH/BNB 三巨头跌幅均已收窄或横盘！大盘熔断解除：\n▶️ <b>全自动狙击引擎重新点火，恢复开仓</b>".to_string()).await;
                for ctx in circuit_ctx.values() {
                    let _ = ctx.control_tx.send(crate::strategy::ControlMessage::ResumeTrading).await;
                }
            }
        }
    });

    // ==========================================
    // MODULE: 5分钟行情定时播报
    // ==========================================
    let tg_ctx_ticker = tg_ctx_arc.clone();
    let tg_tx_ticker = tg_tx.clone();
    let redis_ticker = redis_client.clone();
    tokio::spawn(async move {
        // 【关键修复】先让系统等 15 秒，等 WebSocket 深度完全建立起来再扫瞄，防止第一根 K 线拿到 0
        tokio::time::sleep(tokio::time::Duration::from_secs(15)).await;

        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(300));
        let mut last_prices: std::collections::HashMap<String, rust_decimal::Decimal> = std::collections::HashMap::new();
        let mut global_last_prices: std::collections::HashMap<String, rust_decimal::Decimal> = std::collections::HashMap::new();
        
        loop {
            interval.tick().await;
            let mut report = String::from("⏱️ <b>[行情 5 分钟雷达扫瞄]</b>\n");
            let mut coins_report = String::new();
            
            // 1. 获取全市场真实大盘情绪
            let mut global_up_count = 0;
            let mut global_down_count = 0;
            let mut global_weighted_pct_sum = rust_decimal::Decimal::ZERO;
            let mut global_total_volume = rust_decimal::Decimal::ZERO;
            let mut global_valid = 0;
            
            if let Ok(res) = reqwest::get("https://fapi.binance.com/fapi/v1/ticker/24hr").await {
                if let Ok(json) = res.json::<serde_json::Value>().await {
                    if let Some(arr) = json.as_array() {
                        for item in arr {
                            if let (Some(sym), Some(price_str), Some(vol_str)) = (
                                item.get("symbol").and_then(|v| v.as_str()), 
                                item.get("lastPrice").and_then(|v| v.as_str()),
                                item.get("quoteVolume").and_then(|v| v.as_str())
                            ) {
                                if !sym.ends_with("USDT") { continue; }
                                if let (Ok(price), Ok(vol)) = (rust_decimal::Decimal::from_str(price_str), rust_decimal::Decimal::from_str(vol_str)) {
                                    if let Some(last_p) = global_last_prices.get(sym) {
                                        if *last_p > rust_decimal::Decimal::ZERO {
                                            let pct = (price - *last_p) / *last_p * rust_decimal_macros::dec!(100);
                                            if pct > rust_decimal::Decimal::ZERO { global_up_count += 1; }
                                            else if pct < rust_decimal::Decimal::ZERO { global_down_count += 1; }
                                            
                                            global_weighted_pct_sum += pct * vol;
                                            global_total_volume += vol;
                                            global_valid += 1;
                                        }
                                    }
                                    global_last_prices.insert(sym.to_string(), price);
                                }
                            }
                        }
                    }
                }
            }

            let mut sym_keys: Vec<&String> = tg_ctx_ticker.keys().collect();
            sym_keys.sort(); // 保持固定的字母顺序
            
            for sym in sym_keys {
                if let Some(ctx) = tg_ctx_ticker.get(sym) {
                    let (bid_price, ask_price) = {
                        let ob = ctx.ob_manager.book.read().unwrap();
                        let b = ob.bids.iter().next_back().map(|(p, _)| *p).unwrap_or(Decimal::ZERO);
                        let a = ob.asks.iter().next().map(|(p, _)| *p).unwrap_or(Decimal::ZERO);
                        (b, a)
                    };
                    let price = bid_price; // 保持原有用于涨跌幅计算的基准
                    
                    let mut extra_info = String::new();
                    if let Ok(mut con) = redis_ticker.get_multiplexed_async_connection().await {
                        let key = format!("{}_state", sym);
                        if let Ok(state_content) = redis::cmd("GET").arg(&key).query_async::<String>(&mut con).await {
                            if let Ok(state) = serde_json::from_str::<serde_json::Value>(&state_content) {
                                let amt_str = match state.get("position_amt") {
                                    Some(serde_json::Value::String(s)) => s.to_string(),
                                    Some(serde_json::Value::Number(n)) => n.to_string(),
                                    _ => "0".to_string(),
                                };
                                if let Ok(amt) = rust_decimal::Decimal::from_str(&amt_str) {
                                if amt.abs() > rust_decimal::Decimal::ZERO {
                                    let entry_str = match state.get("entry_price") {
                                        Some(serde_json::Value::String(s)) => s.to_string(),
                                        Some(serde_json::Value::Number(n)) => n.to_string(),
                                        _ => "0".to_string(),
                                    };
                                    let entry = entry_str.parse::<rust_decimal::Decimal>().unwrap_or_default();
                                    
                                    // 多单算平仓收益看买盘(bid)，空单算平仓收益看卖盘(ask)
                                    let unpnl = if amt > Decimal::ZERO {
                                        (bid_price - entry) * amt
                                    } else {
                                        (ask_price - entry) * amt
                                    };
                                    let sl_price = if amt > rust_decimal::Decimal::ZERO {
                                        let high_str = match state.get("highest_price_since_entry") {
                                            Some(serde_json::Value::String(s)) => s.to_string(),
                                            Some(serde_json::Value::Number(n)) => n.to_string(),
                                            _ => "0".to_string(),
                                        };
                                        let high = high_str.parse::<rust_decimal::Decimal>().unwrap_or_default();
                                        let current_profit_pct = (high - entry) / entry * rust_decimal::Decimal::from_str("100").unwrap();
                                        
                                        let break_even_trigger = rust_decimal::Decimal::from_str("1.0").unwrap();
                                        let break_even_target = rust_decimal::Decimal::from_str("0.15").unwrap();
                                        
                                        if current_profit_pct >= rust_decimal::Decimal::from_str("1.5").unwrap() {
                                            high * (rust_decimal::Decimal::ONE - rust_decimal::Decimal::from_str("0.008").unwrap())
                                        } else if current_profit_pct >= break_even_trigger {
                                            entry * (rust_decimal::Decimal::ONE + break_even_target / rust_decimal::Decimal::from_str("100").unwrap())
                                        } else {
                                            entry * (rust_decimal::Decimal::ONE - rust_decimal::Decimal::from_str("0.024").unwrap())
                                        }
                                    } else {
                                        let low_str = match state.get("lowest_price_since_entry") {
                                            Some(serde_json::Value::String(s)) => s.to_string(),
                                            Some(serde_json::Value::Number(n)) => n.to_string(),
                                            _ => "0".to_string(),
                                        };
                                        let low = low_str.parse::<rust_decimal::Decimal>().unwrap_or_default();
                                        let current_profit_pct = (entry - low) / entry * rust_decimal::Decimal::from_str("100").unwrap();
                                        
                                        let break_even_trigger = rust_decimal::Decimal::from_str("1.0").unwrap();
                                        let break_even_target = rust_decimal::Decimal::from_str("0.15").unwrap();
                                        
                                        if current_profit_pct >= rust_decimal::Decimal::from_str("1.5").unwrap() {
                                            low * (rust_decimal::Decimal::ONE + rust_decimal::Decimal::from_str("0.008").unwrap())
                                        } else if current_profit_pct >= break_even_trigger {
                                            entry * (rust_decimal::Decimal::ONE - break_even_target / rust_decimal::Decimal::from_str("100").unwrap())
                                        } else {
                                            entry * (rust_decimal::Decimal::ONE + rust_decimal::Decimal::from_str("0.024").unwrap())
                                        }
                                    };
                                    
                                    let dir = if amt > Decimal::ZERO { "🟢 多" } else { "🔴 空" };
                                    let notional = amt.abs() * if amt > Decimal::ZERO { bid_price } else { ask_price };
                                    extra_info = format!("\n    ↪ {}仓 | 均价: {} | 数量: {} | 价值: {:.2}U | 盈亏: {:.2}U | 预计平仓(止损)价: {:.4}", dir, entry.round_dp(6).normalize(), amt.abs().normalize(), notional, unpnl, sl_price);
                                }
                            }
                        }
                    }
                    }

                    let mut delta_str = String::new();
                    if let Some(last_p) = last_prices.get(sym) {
                        if *last_p > rust_decimal::Decimal::ZERO && price > rust_decimal::Decimal::ZERO {
                            let pct = (price - *last_p) / *last_p * rust_decimal_macros::dec!(100);
                            if pct > rust_decimal::Decimal::ZERO {
                                delta_str = format!(" (📈 +{:.3}%)", pct);
                            } else if pct < rust_decimal::Decimal::ZERO {
                                delta_str = format!(" (📉 {:.3}%)", pct);
                            } else {
                                delta_str = format!(" (➖ 0.000%)");
                            }
                        }
                    }
                    if price > rust_decimal::Decimal::ZERO {
                        last_prices.insert((*sym).clone(), price);
                        coins_report.push_str(&format!("🔹 <b>{}</b>: {}{}{}\n", sym, price, delta_str, extra_info));
                    } else {
                        coins_report.push_str(&format!("🔹 <b>{}</b>: 数据获取中...\n", sym));
                    }
                }
            }
            
            if global_valid > 0 && global_total_volume > rust_decimal::Decimal::ZERO {
                let avg_pct = global_weighted_pct_sum / global_total_volume;
                let sentiment = if avg_pct > rust_decimal_macros::dec!(0.2) {
                    "🔥 市场狂热 (全线爆发)"
                } else if avg_pct > rust_decimal_macros::dec!(0.05) && global_up_count > global_down_count {
                    "📈 偏向乐观 (多军控盘)"
                } else if avg_pct < rust_decimal_macros::dec!(-0.2) {
                    "🩸 市场恐慌 (全线血洗)"
                } else if avg_pct < rust_decimal_macros::dec!(-0.05) && global_down_count > global_up_count {
                    "📉 偏向悲观 (空军压制)"
                } else {
                    "⚖️ 震荡洗盘 (多空互博)"
                };
                
                report.push_str(&format!("📊 <b>全网真实大盘情绪 ({}只币):</b> {}\n", global_valid, sentiment));
                report.push_str(&format!("⏱️ <b>全网资金加权5分钟均幅:</b> {:.3}%\n", avg_pct));
                report.push_str(&format!("🟢 上涨家数: {} | 🔴 下跌家数: {}\n\n", global_up_count, global_down_count));
            } else {
                report.push_str("📊 <b>大盘情绪:</b> 全网数据收集中 (需等待下个5分钟)...\n\n");
            }
            report.push_str(&coins_report);
            
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
            
            // 强制休眠 60 秒，避免 0 点边界重复触发
            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
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
