use std::collections::HashMap;
use std::sync::Arc;
use teloxide::{prelude::*, utils::command::BotCommands};
use tracing::info;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::str::FromStr;
use rust_decimal::prelude::ToPrimitive;

use crate::execution::BinanceExecutionClient;
use crate::strategy::ControlMessage;
use crate::SymbolContext;

#[derive(BotCommands, Clone)]
#[command(rename_rule = "lowercase", description = "🤖 多币种狙击终端：可用指令")]
enum Command {
    #[command(description = "显示帮助信息")]
    Help,
    #[command(description = "狙击做多。用法: /buy &lt;交易对&gt; &lt;USDT本金&gt; &lt;杠杆&gt; [限价] (例: /buy ETHUSDT 500 10 1800)")]
    Buy(String),
    #[command(description = "狙击做空。用法: /sell &lt;交易对&gt; &lt;USDT本金&gt; &lt;杠杆&gt; [限价]")]
    Sell(String),
    #[command(description = "紧急清仓某个币种 (仅清空记忆不平仓)。用法: /panic DOGEUSDT")]
    Panic(String),
    #[command(description = "一键市价全平 (真实平仓并清空记忆)。用法: /close &lt;交易对&gt;")]
    Close(String),
    #[command(description = "同步仓位并开启监控。全局同步用法: /sync，单个同步: /sync &lt;交易对&gt;")]
    Sync(String),
    #[command(description = "检查交易所连接")]
    Status,
    #[command(description = "查看当前所有持仓与未实现收益")]
    Pnl,
    #[command(description = "查看特定时间段内的真实已实现收益。用法: /profit 10m, /profit 2h, /profit 1d, /profit 1w")]
    Profit(String),
    #[command(description = "获取当前所有订阅币种的实时盘口价和涨跌幅。用法: /price [时间维度]，例如: /price 15m, /price 1h, /price 24h")]
    Price(String),
    #[command(description = "生成昨日盈亏与交易手续费总结")]
    Yesterday,
    #[command(description = "订阅新币种并重启引擎。用法: /sub DOGEUSDT")]
    Sub(String),
    #[command(description = "取消订阅币种并重启引擎。用法: /unsub DOGEUSDT")]
    Unsub(String),
    #[command(description = "暂停自动开新仓。用法: /pause")]
    Pause,
    #[command(description = "恢复自动开仓。用法: /resume")]
    Resume,
    #[command(description = "做空机制开关。用法: /short on 或 /short off")]
    Short(String),
}

pub async fn run_telegram_bot(
    exec_client: Arc<BinanceExecutionClient>,
    contexts: Arc<HashMap<String, SymbolContext>>,
    exchange_info: Arc<HashMap<String, crate::execution::SymbolInfo>>,
) {
    let bot = Bot::from_env();
    info!("🚀 Telegram 遥控机器人已启动，多币种矩阵接入完毕！");

    let handler = Update::filter_message()
        .filter_command::<Command>()
        .endpoint(answer);

    Dispatcher::builder(bot, handler)
        .dependencies(dptree::deps![exec_client, contexts, exchange_info])
        .enable_ctrlc_handler()
        .build()
        .dispatch()
        .await;
}

async fn answer(
    bot: Bot,
    msg: Message,
    cmd: Command,
    exec_client: Arc<BinanceExecutionClient>,
    contexts: Arc<HashMap<String, SymbolContext>>,
    exchange_info: Arc<HashMap<String, crate::execution::SymbolInfo>>,
) -> ResponseResult<()> {
    let admin_chat_id = std::env::var("TELEGRAM_CHAT_ID").unwrap_or_default();
    let current_chat_id = msg.chat.id.to_string();

    if !admin_chat_id.is_empty() && current_chat_id != admin_chat_id {
        bot.send_message(msg.chat.id, "⛔️ <b>安全拦截</b>\n您无权调用此机构级交易终端。").parse_mode(teloxide::types::ParseMode::Html).await?;
        return Ok(());
    }

    match cmd {
        Command::Help => {
            bot.send_message(msg.chat.id, Command::descriptions().to_string()).parse_mode(teloxide::types::ParseMode::Html).await?;
        }
        Command::Price(args) => {
            let interval = args.trim().to_lowercase();
            let valid_intervals = ["1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w"];
            let interval_to_use = if valid_intervals.contains(&interval.as_str()) { interval } else { "5m".to_string() };

            bot.send_message(msg.chat.id, format!("⌛️ 正在从币安拉取全网 {} 维度的价格变化，请稍候...", interval_to_use)).await?;
            
            let mut report = format!("📊 <b>全网 {} 涨跌幅排行榜</b>\n\n", interval_to_use);
            let mut results = Vec::new();

            for (sym, ctx) in contexts.iter() {
                let current_price = ctx.ob_manager.book.read().unwrap().bids.iter().next_back().map(|(p, _)| *p).unwrap_or(rust_decimal::Decimal::ZERO);
                if current_price > rust_decimal::Decimal::ZERO {
                    if let Ok(past_price) = exec_client.fetch_kline_open_price(sym, &interval_to_use).await {
                        if past_price > rust_decimal::Decimal::ZERO {
                            let pct = (current_price - past_price) / past_price * rust_decimal_macros::dec!(100);
                            results.push((sym.clone(), current_price, pct));
                        }
                    }
                }
            }

            // Sort by percentage change descending
            results.sort_by(|a, b| b.2.partial_cmp(&a.2).unwrap_or(std::cmp::Ordering::Equal));

            for (sym, price, pct) in results {
                let emoji = if pct > rust_decimal::Decimal::ZERO { "📈" } else { "📉" };
                let plus = if pct > rust_decimal::Decimal::ZERO { "+" } else { "" };
                report.push_str(&format!("{} <b>{}</b>: {} ({}{:.2}%)\n", emoji, sym, price, plus, pct));
            }

            bot.send_message(msg.chat.id, report).parse_mode(teloxide::types::ParseMode::Html).await?;
        }
        Command::Yesterday => {
            let _ = bot.send_message(msg.chat.id, "⏳ 正在生成昨日盈亏快照数据，请稍候...").await;
            
            use time::{OffsetDateTime, UtcOffset, Duration, Time};
            let offset = UtcOffset::from_hms(8, 0, 0).unwrap();
            let now_midnight = OffsetDateTime::now_utc().to_offset(offset).replace_time(Time::MIDNIGHT);
            let start_of_yesterday = now_midnight - Duration::days(1);
            let end_of_yesterday = now_midnight - Duration::nanoseconds(1);
            
            let start_ts = start_of_yesterday.unix_timestamp() * 1000;
            let end_ts = end_of_yesterday.unix_timestamp() * 1000;
            
            if let Ok(res) = exec_client.get_income_history(start_ts as u64, end_ts as u64).await {
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
                            "📊 <b>昨日盈亏总结 (UTC+8)</b>\n\n\
                            区间: {} 00:00 ~ 23:59\n\
                            \n\
                            💰 净收益: <b>{:.2} USDT</b>\n\
                            -------------------------\n\
                            📈 实现盈亏: {:.2} USDT\n\
                            📉 交易手续费: {:.2} USDT\n\
                            ⏱ 资金费率: {:.2} USDT",
                            start_of_yesterday.date(), net_income, total_pnl, total_fee, total_funding
                        );
                        bot.send_message(msg.chat.id, report).parse_mode(teloxide::types::ParseMode::Html).await?;
                        return Ok(());
                    }
                }
            }
            bot.send_message(msg.chat.id, "❌ 获取昨日盈亏数据失败，可能是 API 调用限制或无数据。").parse_mode(teloxide::types::ParseMode::Html).await?;
        }
        Command::Buy(args) => {
            let parts: Vec<&str> = args.split_whitespace().collect();
            if parts.len() < 3 || parts.len() > 4 {
                bot.send_message(msg.chat.id, "⚠️ 参数错误。\n用法: /buy &lt;交易对&gt; &lt;USDT本金&gt; &lt;杠杆倍数&gt; [限价]\n例如: /buy ETHUSDT 500 10 1800").parse_mode(teloxide::types::ParseMode::Html).await?;
                return Ok(());
            }
            let symbol = parts[0].to_uppercase();
            let margin_usdt = Decimal::from_str(parts[1]).unwrap_or_default();
            let leverage = Decimal::from_str(parts[2]).unwrap_or(dec!(1));
            let limit_price = if parts.len() == 4 { Decimal::from_str(parts[3]).ok() } else { None };
            
            if let Some(ctx) = contexts.get(&symbol) {
                let lev_u32 = leverage.to_u32().unwrap_or(1);
                let _ = exec_client.set_leverage(&symbol, lev_u32).await;
                let _ = exec_client.set_margin_type(&symbol, "ISOLATED").await;

                let estimated_entry = limit_price.unwrap_or_else(|| ctx.ob_manager.book.read().unwrap().asks.iter().next().map(|(p, _)| *p).unwrap_or(dec!(1)));
                let notional = margin_usdt * leverage;
                let mut target_qty = notional / estimated_entry;
                
                let mut limit_price_str = String::new();
                if let Some(info) = exchange_info.get(&symbol) {
                    if info.step_size > Decimal::ZERO {
                        let steps = (target_qty / info.step_size).floor();
                        target_qty = steps * info.step_size;
                    }
                    if let Some(mut lp) = limit_price {
                        if info.tick_size > Decimal::ZERO {
                            let steps = (lp / info.tick_size).floor();
                            lp = steps * info.tick_size;
                        }
                        limit_price_str = lp.normalize().to_string();
                    }
                }
                
                let qty_str = target_qty.normalize().to_string();
                
                let res = if let Some(_) = limit_price {
                    exec_client.place_limit_order(&symbol, "BUY", &qty_str, &limit_price_str).await.map(|_| Decimal::ZERO)
                } else {
                    exec_client.place_order(&symbol, "BUY", "MARKET", &qty_str, false).await.map_err(|e| e.to_string())
                };

                match res {
                    Ok(real_avg_price) => {
                        if limit_price.is_some() {
                            bot.send_message(msg.chat.id, format!("✅ <b>[{}] 限价做多 (BUY LIMIT) 下单成功！</b>\n\n🎯 限价: <b>{}</b>\n📦 数量: {}\n\n💡 <i>当订单被币安撮合成交后，引擎的“自愈轮询器”会在 10 秒内自动发现仓位并挂载保护。</i>", symbol, limit_price_str, qty_str)).parse_mode(teloxide::types::ParseMode::Html).await?;
                        } else {
                            let fill_price = if real_avg_price > Decimal::ZERO { real_avg_price } else { estimated_entry };
                            let _ = ctx.control_tx.send(ControlMessage::TradeExecuted {
                                trade_qty: target_qty,
                                fill_price,
                            }).await;
                            bot.send_message(msg.chat.id, format!("✅ <b>[{}] 市价做多成功！</b>\n\n🎯 真实均价: <b>{}</b>\n📦 下单量: {}\n\n💡 <i>当前仓位已被接管。</i>", symbol, fill_price, qty_str)).parse_mode(teloxide::types::ParseMode::Html).await?;
                        }
                    }
                    Err(e) => { bot.send_message(msg.chat.id, format!("❌ 订单失败：\n{}", e)).parse_mode(teloxide::types::ParseMode::Html).await?; }
                }
            } else {
                bot.send_message(msg.chat.id, format!("⚠️ 系统未订阅交易对: {}", symbol)).parse_mode(teloxide::types::ParseMode::Html).await?;
            }
        }
        Command::Sell(args) => {
            let parts: Vec<&str> = args.split_whitespace().collect();
            if parts.len() < 3 || parts.len() > 4 {
                bot.send_message(msg.chat.id, "⚠️ 参数错误。\n用法: /sell &lt;交易对&gt; &lt;USDT本金&gt; &lt;杠杆倍数&gt; [限价]\n例如: /sell ETHUSDT 500 10 1800").parse_mode(teloxide::types::ParseMode::Html).await?;
                return Ok(());
            }
            let symbol = parts[0].to_uppercase();
            let margin_usdt = Decimal::from_str(parts[1]).unwrap_or_default();
            let leverage = Decimal::from_str(parts[2]).unwrap_or(dec!(1));
            let limit_price = if parts.len() == 4 { Decimal::from_str(parts[3]).ok() } else { None };
            
            if let Some(ctx) = contexts.get(&symbol) {
                let lev_u32 = leverage.to_u32().unwrap_or(1);
                let _ = exec_client.set_leverage(&symbol, lev_u32).await;
                let _ = exec_client.set_margin_type(&symbol, "ISOLATED").await;

                let estimated_entry = limit_price.unwrap_or_else(|| ctx.ob_manager.book.read().unwrap().bids.iter().next_back().map(|(p, _)| *p).unwrap_or(dec!(1)));
                let notional = margin_usdt * leverage;
                let mut target_qty = notional / estimated_entry;
                
                let mut limit_price_str = String::new();
                if let Some(info) = exchange_info.get(&symbol) {
                    if info.step_size > Decimal::ZERO {
                        let steps = (target_qty / info.step_size).floor();
                        target_qty = steps * info.step_size;
                    }
                    if let Some(mut lp) = limit_price {
                        if info.tick_size > Decimal::ZERO {
                            let steps = (lp / info.tick_size).floor();
                            lp = steps * info.tick_size;
                        }
                        limit_price_str = lp.normalize().to_string();
                    }
                }
                
                let qty_str = target_qty.normalize().to_string();
                
                let res = if let Some(_) = limit_price {
                    exec_client.place_limit_order(&symbol, "SELL", &qty_str, &limit_price_str).await.map(|_| Decimal::ZERO)
                } else {
                    exec_client.place_order(&symbol, "SELL", "MARKET", &qty_str, false).await.map_err(|e| e.to_string())
                };

                match res {
                    Ok(real_avg_price) => {
                        if limit_price.is_some() {
                            bot.send_message(msg.chat.id, format!("✅ <b>[{}] 限价做空 (SELL LIMIT) 下单成功！</b>\n\n🎯 限价: <b>{}</b>\n📦 数量: {}\n\n💡 <i>当订单被币安撮合成交后，引擎的“自愈轮询器”会在 10 秒内自动发现仓位并挂载保护。</i>", symbol, limit_price_str, qty_str)).parse_mode(teloxide::types::ParseMode::Html).await?;
                        } else {
                            let fill_price = if real_avg_price > Decimal::ZERO { real_avg_price } else { estimated_entry };
                            let _ = ctx.control_tx.send(ControlMessage::TradeExecuted {
                                trade_qty: -target_qty, // 卖出空单数量为负
                                fill_price,
                            }).await;
                            bot.send_message(msg.chat.id, format!("✅ <b>[{}] 市价做空成功！</b>\n\n🎯 真实均价: <b>{}</b>\n📦 下单量: {}\n\n💡 <i>当前仓位已被接管。</i>", symbol, fill_price, qty_str)).parse_mode(teloxide::types::ParseMode::Html).await?;
                        }
                    }
                    Err(e) => { bot.send_message(msg.chat.id, format!("❌ 订单失败：\n{}", e)).parse_mode(teloxide::types::ParseMode::Html).await?; }
                }
            } else {
                bot.send_message(msg.chat.id, format!("⚠️ 系统未订阅交易对: {}", symbol)).parse_mode(teloxide::types::ParseMode::Html).await?;
            }
        }
        Command::Panic(args) => {
            let symbol = args.trim().to_uppercase();
            if let Some(ctx) = contexts.get(&symbol) {
                let _ = ctx.control_tx.send(ControlMessage::ClearPosition).await;
                bot.send_message(msg.chat.id, format!("✅ {} 大脑仓位已被清零，终止移动止损保护 (注意: 这不会在币安平仓，只会清空记忆)。", symbol)).parse_mode(teloxide::types::ParseMode::Html).await?;
            } else {
                bot.send_message(msg.chat.id, format!("⚠️ 系统未订阅交易对: {}", symbol)).parse_mode(teloxide::types::ParseMode::Html).await?;
            }
        }
        Command::Close(args) => {
            let symbol = args.trim().to_uppercase();
            if let Some(ctx) = contexts.get(&symbol) {
                let _ = ctx.control_tx.send(ControlMessage::ClosePosition).await;
                bot.send_message(msg.chat.id, format!("⌛️ 正在向交易所发送 {} 的一键市价全平指令...", symbol)).parse_mode(teloxide::types::ParseMode::Html).await?;
            } else {
                bot.send_message(msg.chat.id, format!("⚠️ 系统未订阅交易对: {}", symbol)).parse_mode(teloxide::types::ParseMode::Html).await?;
            }
        }
        Command::Sync(args) => {
            let symbol = args.trim().to_uppercase();
            if symbol.is_empty() {
                bot.send_message(msg.chat.id, "⌛️ 正在从币安拉取全网真实仓位，请稍候...").await?;
                match exec_client.check_positions().await {
                    Ok(pos_str) => {
                        let positions: Vec<serde_json::Value> = serde_json::from_str(&pos_str).unwrap_or_default();
                        let mut synced_count = 0;
                        for pos in positions {
                            let amt = pos.get("positionAmt").and_then(|v| v.as_str()).and_then(|s| rust_decimal::Decimal::from_str(s).ok()).unwrap_or(rust_decimal::Decimal::ZERO);
                            if amt.abs() > rust_decimal::Decimal::ZERO {
                                let sym = pos.get("symbol").and_then(|v| v.as_str()).unwrap_or("").to_string();
                                if let Some(ctx) = contexts.get(&sym) {
                                    let _ = ctx.control_tx.send(ControlMessage::SyncPosition).await;
                                    synced_count += 1;
                                    // 延迟避免并发请求触发币安流控
                                    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
                                }
                            }
                        }
                        bot.send_message(msg.chat.id, format!("✅ 全网同步指令分发完毕！共唤醒了 {} 个遗留仓位的大脑开始防守。", synced_count)).await?;
                    }
                    Err(e) => {
                        bot.send_message(msg.chat.id, format!("❌ 拉取全网仓位失败: {}", e)).await?;
                    }
                }
            } else {
                if let Some(ctx) = contexts.get(&symbol) {
                    let _ = ctx.control_tx.send(ControlMessage::SyncPosition).await;
                    bot.send_message(msg.chat.id, format!("⌛️ 正在从币安同步 {} 的真实仓位数据...", symbol)).parse_mode(teloxide::types::ParseMode::Html).await?;
                } else {
                    bot.send_message(msg.chat.id, format!("⚠️ 系统未订阅交易对: {}", symbol)).parse_mode(teloxide::types::ParseMode::Html).await?;
                }
            }
        }
        Command::Status => {
            let res = exec_client.check_account().await;
            match res {
                Ok(resp) => {
                    tracing::info!("Account API Response: {}", resp);
                    let formatted = match serde_json::from_str::<serde_json::Value>(&resp) {
                        Ok(json) => {
                            if let Some(msg) = json.get("msg").and_then(|v| v.as_str()) {
                                format!("❌ API 请求失败: {}", msg)
                            } else {
                                let get_str = |key: &str| -> String {
                                    json.get(key).and_then(|v| {
                                        if let Some(s) = v.as_str() { Some(s.to_string()) }
                                        else if let Some(n) = v.as_f64() { Some(n.to_string()) }
                                        else { None }
                                    }).unwrap_or("0.00".to_string())
                                };
                                let balance = get_str("totalWalletBalance");
                                let unpnl = get_str("totalUnrealizedProfit");
                                let avail = get_str("availableBalance");
                                let can_trade = json.get("canTrade").and_then(|v| v.as_bool()).unwrap_or(true); // default to true if missing
                                let status_icon = if can_trade { "✅" } else { "❌(被禁用)" };
                                
                                format!("📊 <b>实盘资产报告</b>\n\n💰 <b>钱包总余额</b>: {} USDT\n💵 <b>可用开仓金</b>: {} USDT\n📈 <b>未实现盈亏</b>: {} USDT\n🔓 <b>API 交易权限</b>: {}", 
                                    balance, avail, unpnl, status_icon)
                            }
                        }
                        Err(e) => format!("❌ 解析数据失败: {}\n原始数据: {}", e, resp),
                    };
                    bot.send_message(msg.chat.id, formatted).parse_mode(teloxide::types::ParseMode::Html).await?;
                }
                Err(e) => { bot.send_message(msg.chat.id, format!("⚠️ API 鉴权未通过：\n{}", e)).parse_mode(teloxide::types::ParseMode::Html).await?; }
            }
        }
        Command::Pnl => {
            bot.send_message(msg.chat.id, "📡 正在扫描全网持仓与未实现收益...").parse_mode(teloxide::types::ParseMode::Html).await?;
            let res = exec_client.check_positions().await;
            match res {
                Ok(resp) => {
                    if let Ok(json_arr) = serde_json::from_str::<Vec<serde_json::Value>>(&resp) {
                        let mut pnl_text = String::from("📈 <b>全网持仓与收益雷达</b>\n\n");
                        let mut has_position = false;
                        let mut total_unpnl: f64 = 0.0;
                        
                        for pos in json_arr {
                            let amt_str = pos.get("positionAmt").and_then(|v| v.as_str()).unwrap_or("0");
                            let amt: f64 = amt_str.parse().unwrap_or(0.0);
                            
                            if amt.abs() > 0.0 {
                                has_position = true;
                                let symbol = pos.get("symbol").and_then(|v| v.as_str()).unwrap_or("UNKNOWN");
                                let unpnl_str = pos.get("unRealizedProfit").and_then(|v| v.as_str()).unwrap_or("0");
                                let unpnl: f64 = unpnl_str.parse().unwrap_or(0.0);
                                let entry = pos.get("entryPrice").and_then(|v| v.as_str()).unwrap_or("0");
                                let mark_price = pos.get("markPrice").and_then(|v| v.as_str()).unwrap_or("0");
                                
                                total_unpnl += unpnl;
                                
                                let entry_num: f64 = entry.parse().unwrap_or(0.0);
                                let notional = amt.abs() * entry_num;
                                let estimated_fee = notional * 0.001; // 估算 0.1% 的双边摩擦成本
                                let safe_tag = if unpnl > estimated_fee && unpnl > 0.0 { " 🛡️ [已覆盖手续费, 保本无忧]" } else { "" };
                                
                                let direction = if amt > 0.0 { format!("🟢 多头 (LONG){}", safe_tag) } else { format!("🔴 空头 (SHORT){}", safe_tag) };
                                pnl_text.push_str(&format!(
                                    "<b>{}</b> {}\n\
                                     🔹 持仓量: {}\n\
                                     🔹 开仓价: {}\n\
                                     🔹 当前现价: {}\n\
                                     💵 未实现盈亏: <b>{} USDT</b>\n\n",
                                     symbol, direction, amt_str, entry, mark_price, unpnl_str
                                ));
                            }
                        }
                        
                        if !has_position {
                            pnl_text.push_str("🈳 当前没有任何持仓。等待猎物出现...");
                        } else {
                            pnl_text.push_str(&format!("💰 <b>总未实现盈亏: {:.4} USDT</b>", total_unpnl));
                        }
                        
                        bot.send_message(msg.chat.id, pnl_text).parse_mode(teloxide::types::ParseMode::Html).await?;
                    } else {
                        bot.send_message(msg.chat.id, "⚠️ 解析持仓数据失败。").parse_mode(teloxide::types::ParseMode::Html).await?;
                    }
                }
                Err(e) => { bot.send_message(msg.chat.id, format!("⚠️ API 获取持仓失败：\n{}", e)).parse_mode(teloxide::types::ParseMode::Html).await?; }
            }
        }
        Command::Profit(args) => {
            let arg = args.trim().to_lowercase();
            if arg.is_empty() {
                bot.send_message(msg.chat.id, "❌ 错误: 参数为空。\n用法: /profit 10m (分钟), 2h (小时), 1d (天), 1w (周)").await?;
                return Ok(());
            }
            
            let duration_secs = if arg.ends_with('m') {
                arg.replace('m', "").parse::<u64>().unwrap_or(0) * 60
            } else if arg.ends_with('h') {
                arg.replace('h', "").parse::<u64>().unwrap_or(0) * 3600
            } else if arg.ends_with('d') {
                arg.replace('d', "").parse::<u64>().unwrap_or(0) * 86400
            } else if arg.ends_with('w') {
                arg.replace('w', "").parse::<u64>().unwrap_or(0) * 86400 * 7
            } else {
                bot.send_message(msg.chat.id, "❌ 时间格式错误。\n支持的单位: m(分钟), h(小时), d(天), w(周)").await?;
                return Ok(());
            };
            
            if duration_secs == 0 || duration_secs > 86400 * 30 {
                bot.send_message(msg.chat.id, "❌ 时间无效或跨度过大 (最大支持30天以内的小跨度查询)。").await?;
                return Ok(());
            }

            bot.send_message(msg.chat.id, format!("⏳ 正在向币安总账核算过去 {} 的真实已实现收益...", arg)).await?;

            use std::time::{SystemTime, UNIX_EPOCH};
            let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
            let start_ms = now_ms - (duration_secs * 1000);
            
            match exec_client.get_income_history(start_ms, now_ms).await {
                Ok(income_str) => {
                    if let Ok(records) = serde_json::from_str::<Vec<serde_json::Value>>(&income_str) {
                        let mut total_pnl = 0.0;
                        let mut total_fee = 0.0;
                        let mut total_funding = 0.0;
                        let mut trades_count = 0;
                        
                        for r in records {
                            let income_type = r["incomeType"].as_str().unwrap_or("");
                            let income = r["income"].as_str().unwrap_or("0").parse::<f64>().unwrap_or(0.0);
                            
                            if income_type == "REALIZED_PNL" {
                                total_pnl += income;
                                trades_count += 1;
                            } else if income_type == "COMMISSION" {
                                total_fee += income;
                            } else if income_type == "FUNDING_FEE" {
                                total_funding += income;
                            }
                        }
                        
                        let net_profit = total_pnl + total_fee + total_funding;
                        let emoji = if net_profit > 0.0 { "🤑" } else { "🩸" };
                        
                        let report = format!(
                            "{} <b>过去 {} 收益报告</b>\n\n\
                             🔹 <b>总实现盈亏</b>: {:.4} USDT\n\
                             🔹 <b>交易手续费</b>: {:.4} USDT\n\
                             🔹 <b>资金费率</b>: {:.4} USDT\n\
                             ----------------------------\n\
                             💰 <b>最终净利润</b>: <b>{:.4} USDT</b>\n\
                             (涵盖 {} 笔盈亏流水记录)",
                             emoji, arg, total_pnl, total_fee, total_funding, net_profit, trades_count
                        );
                        bot.send_message(msg.chat.id, report).parse_mode(teloxide::types::ParseMode::Html).await?;
                    } else {
                        bot.send_message(msg.chat.id, "⚠️ 解析币安收益数据失败。可能是查询跨度超出币安单次查询限制。").await?;
                    }
                }
                Err(e) => {
                    bot.send_message(msg.chat.id, format!("⚠️ 拉取收益数据失败：{}", e)).await?;
                }
            }
        }
        Command::Sub(args) => {
            let symbol = args.trim().to_uppercase();
            if symbol.is_empty() {
                let _ = bot.send_message(msg.chat.id, "❌ 错误: 参数为空。\n用法: /sub &lt;币种&gt;\n示例: /sub DOGEUSDT").parse_mode(teloxide::types::ParseMode::Html).await;
                return Ok(());
            }
            let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1/".to_string());
            if let Ok(client) = redis::Client::open(redis_url) {
                if let Ok(mut con) = client.get_multiplexed_async_connection().await {
                    let _: () = redis::cmd("SADD").arg("SUBSCRIBED_SYMBOLS").arg(&symbol).query_async(&mut con).await.unwrap_or_default();
                    let _ = bot.send_message(msg.chat.id, format!("✅ 已将 <b>{}</b> 添加到监控列表！\n\n🔄 正在触发引擎热重启以加载全新的数据流通道和量化模型，请稍候...", symbol)).parse_mode(teloxide::types::ParseMode::Html).await;
                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                    std::process::exit(0);
                }
            }
        }
        Command::Unsub(args) => {
            let symbol = args.trim().to_uppercase();
            if symbol.is_empty() {
                let _ = bot.send_message(msg.chat.id, "❌ 错误: 参数为空。\n用法: /unsub &lt;币种&gt;\n示例: /unsub DOGEUSDT").parse_mode(teloxide::types::ParseMode::Html).await;
                return Ok(());
            }
            let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1/".to_string());
            if let Ok(client) = redis::Client::open(redis_url) {
                if let Ok(mut con) = client.get_multiplexed_async_connection().await {
                    let _: () = redis::cmd("SREM").arg("SUBSCRIBED_SYMBOLS").arg(&symbol).query_async(&mut con).await.unwrap_or_default();
                    let _ = bot.send_message(msg.chat.id, format!("✅ 已将 <b>{}</b> 从监控列表移除！\n\n🔄 正在触发引擎热重启释放连接池资源，请稍候...", symbol)).parse_mode(teloxide::types::ParseMode::Html).await;
                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                    std::process::exit(0);
                }
            }
        }

        Command::Pause => {
            for ctx in contexts.values() {
                let _ = ctx.control_tx.send(ControlMessage::PauseTrading).await;
            }
            let _ = bot.send_message(msg.chat.id, "🛑 <b>已紧急暂停所有新开仓！</b>\n目前策略不会开任何新单，但已有的持仓依然会受到移动止损保护。").parse_mode(teloxide::types::ParseMode::Html).await;
        }
        Command::Resume => {
            for ctx in contexts.values() {
                let _ = ctx.control_tx.send(ControlMessage::ResumeTrading).await;
            }
            bot.send_message(msg.chat.id, "▶️ <b>自动交易已恢复！</b>\n策略引擎全功率运转，将正常捕捉所有交易信号。").parse_mode(teloxide::types::ParseMode::Html).await?;
        }
        Command::Short(args) => {
            let arg = args.trim().to_lowercase();
            if arg == "on" {
                for ctx in contexts.values() {
                    let _ = ctx.control_tx.send(ControlMessage::AllowShorting(true)).await;
                }
                bot.send_message(msg.chat.id, "📉 <b>自动做空已开启！</b>\n目前仅当触发【地狱级砸盘】条件（砸盘资金超买盘 10 倍）时，才会允许做空。").parse_mode(teloxide::types::ParseMode::Html).await?;
            } else if arg == "off" {
                for ctx in contexts.values() {
                    let _ = ctx.control_tx.send(ControlMessage::AllowShorting(false)).await;
                }
                bot.send_message(msg.chat.id, "🚫 <b>自动做空已彻底关闭！</b>\n策略引擎现已切换为纯多头（Long-Only）模式，只抓暴涨，不再做空。").parse_mode(teloxide::types::ParseMode::Html).await?;
            } else {
                bot.send_message(msg.chat.id, "❌ 参数错误。用法: /short on 或 /short off").await?;
            }
        }
    }
    Ok(())
}
