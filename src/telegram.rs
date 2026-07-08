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
    #[command(description = "查看近期历史仓位和成交记录。用法: /history [交易对] [天数]")]
    History(String),
    #[command(description = "仓位保镖。用法: /guard status | on | off | stop &lt;百分比&gt; | trail &lt;激活%&gt; &lt;回撤%&gt; | alert &lt;分钟&gt; | autoclose &lt;分钟&gt;|off")]
    Guard(String),
    #[command(description = "纸面交易引擎 (零实弹)。用法: /paper status | on | off | trail &lt;百分比&gt; | reset")]
    Paper(String),
    #[command(description = "现货网格实盘 (真钱!)。用法: /gridlive status | on [SYMBOL钉选] | unpin SYM | stop SYM | auto on|off | off | liquidate | budget &lt;U/网格&gt; | slots &lt;并发数&gt;")]
    GridLive(String),
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
            
            if let Ok(summary) = exec_client.get_income_summary(start_ts as u64, end_ts as u64).await {
                let report = format!(
                    "📊 <b>昨日盈亏总结 (UTC+8)</b>\n\n\
                    区间: {} 00:00 ~ 23:59\n\
                    \n\
                    💰 净收益: <b>{:.2} USDT</b>\n\
                    -------------------------\n\
                    📈 实现盈亏: {:.2} USDT\n\
                    📉 交易手续费: {:.2} USDT\n\
                    ⏱ 资金费率: {:.2} USDT",
                    start_of_yesterday.date(), summary.net(), summary.realized_pnl, summary.commission, summary.funding_fee
                );
                bot.send_message(msg.chat.id, report).parse_mode(teloxide::types::ParseMode::Html).await?;
                return Ok(());
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

            match exec_client.get_income_summary(start_ms, now_ms).await {
                Ok(summary) => {
                    let net_profit = summary.net();
                    let emoji = if net_profit > Decimal::ZERO { "🤑" } else { "🩸" };

                    let report = format!(
                        "{} <b>过去 {} 收益报告</b>\n\n\
                         🔹 <b>总实现盈亏</b>: {:.4} USDT\n\
                         🔹 <b>交易手续费</b>: {:.4} USDT\n\
                         🔹 <b>资金费率</b>: {:.4} USDT\n\
                         ----------------------------\n\
                         💰 <b>最终净利润</b>: <b>{:.4} USDT</b>\n\
                         (涵盖 {} 笔盈亏流水记录, 共解析 {} 条底层数据)",
                         emoji, arg, summary.realized_pnl, summary.commission, summary.funding_fee, net_profit, summary.trades_count, summary.records_processed
                    );
                    bot.send_message(msg.chat.id, report).parse_mode(teloxide::types::ParseMode::Html).await?;
                }
                Err(e) => {
                    bot.send_message(msg.chat.id, format!("⚠️ {}", e)).await?;
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
        Command::History(args) => {
            let parts: Vec<&str> = args.split_whitespace().collect();
            let mut days: u32 = 1;
            let mut specific_symbol = None;
            
            for part in parts {
                if let Ok(d) = part.parse::<u32>() {
                    days = d;
                } else {
                    specific_symbol = Some(part.to_uppercase());
                }
            }
            
            use time::{OffsetDateTime, UtcOffset, Time, Duration};
            let offset = UtcOffset::from_hms(8, 0, 0).unwrap();
            let now = OffsetDateTime::now_utc().to_offset(offset);
            let start_date = now - Duration::days((days.saturating_sub(1)) as i64);
            let start_midnight = start_date.replace_time(Time::MIDNIGHT);
            let start_time_ms = start_midnight.unix_timestamp() as u64 * 1000;
            
            let symbols_to_check: Vec<String> = if let Some(sym) = specific_symbol.clone() {
                vec![sym]
            } else {
                contexts.keys().cloned().collect()
            };
            
            let time_desc = if days == 1 { "今日" } else { &format!("近{}天", days) };
            
            if symbols_to_check.is_empty() {
                bot.send_message(msg.chat.id, "目前没有任何订阅的交易对。").await?;
                return Ok(());
            }
            
            bot.send_message(msg.chat.id, format!("⏳ 正在拉取 {} 个交易对的{}历史成交记录，请稍候...", symbols_to_check.len(), time_desc)).await?;
            
            let mut all_trades: Vec<(serde_json::Value, String)> = Vec::new();
            
            for symbol in symbols_to_check.iter() {
                if let Ok(res) = exec_client.get_user_trades(symbol, None, Some(start_time_ms)).await {
                    if let Ok(trades) = serde_json::from_str::<Vec<serde_json::Value>>(&res) {
                        for t in trades {
                            all_trades.push((t, symbol.clone()));
                        }
                    }
                }
            }
            
            if all_trades.is_empty() {
                bot.send_message(msg.chat.id, format!("🈳 {}没有任何成交记录。", time_desc)).await?;
                return Ok(());
            }
            
            all_trades.sort_by_key(|(t, _)| t.get("time").and_then(|v| v.as_u64()).unwrap_or(0));
            
            let mut total_profit = rust_decimal::Decimal::ZERO;
            let mut total_loss = rust_decimal::Decimal::ZERO;
            let mut total_fee = rust_decimal::Decimal::ZERO;
            let total_trades = all_trades.len();
            
            let mut trade_details = String::new();
            
            for (t, sym) in all_trades.iter().rev() {
                let time = t.get("time").and_then(|v| v.as_u64()).unwrap_or(0);
                let dt = time::OffsetDateTime::from_unix_timestamp((time / 1000) as i64).unwrap_or(time::OffsetDateTime::now_utc());
                let dt = dt.to_offset(offset);
                let side = t.get("side").and_then(|v| v.as_str()).unwrap_or("");
                let price = t.get("price").and_then(|v| v.as_str()).unwrap_or("0");
                let qty = t.get("qty").and_then(|v| v.as_str()).unwrap_or("0");
                let realized_pnl = t.get("realizedPnl").and_then(|v| v.as_str()).unwrap_or("0");
                let commission = t.get("commission").and_then(|v| v.as_str()).unwrap_or("0");
                
                if let Ok(fee) = rust_decimal::Decimal::from_str(commission) {
                    total_fee += fee;
                }
                
                let emoji = if side == "BUY" { "🟢买" } else { "🔴卖" };
                let pnl_str = if realized_pnl != "0" && realized_pnl != "0.00000000" && !realized_pnl.starts_with("-0.000") {
                    if let Ok(pnl) = rust_decimal::Decimal::from_str(realized_pnl) {
                        if pnl > rust_decimal::Decimal::ZERO {
                            total_profit += pnl;
                            format!(" | 盈利: {} U", pnl.normalize())
                        } else {
                            total_loss += pnl.abs();
                            format!(" | 亏损: {} U", pnl.abs().normalize())
                        }
                    } else { "".to_string() }
                } else {
                    "".to_string()
                };
                
                let detail = format!("{} [{}]\n{} {} | 价: {} | 量: {}{}\n\n", 
                    emoji, dt.format(&time::format_description::well_known::Rfc3339).unwrap_or_default(),
                    side, sym, price, qty, pnl_str
                );
                
                if trade_details.len() < 3000 {
                    trade_details.push_str(&detail);
                } else if trade_details.len() < 3100 {
                    trade_details.push_str("... (记录过多已折叠)\n");
                }
            }
            
            let net_pnl = total_profit - total_loss - total_fee;
            let net_emoji = if net_pnl > rust_decimal::Decimal::ZERO { "🤑" } else { "🩸" };
            let title = if let Some(sym) = specific_symbol {
                format!("📜 <b>{} 历史成交记录 ({})</b>", sym, time_desc)
            } else {
                format!("📜 <b>全部交易对历史记录 ({})</b>", time_desc)
            };
            
            let mut report = format!("{}\n\n{} <b>阶段汇总</b>\n🔹 <b>总交易笔数</b>: {}\n🔹 <b>总盈利</b>: {:.4} U\n🔹 <b>总亏损</b>: {:.4} U\n🔹 <b>总手续费</b>: {:.4} U\n--------------------\n💰 <b>净利润</b>: <b>{:.4} U</b>\n\n",
                title, net_emoji, total_trades, total_profit, total_loss, total_fee, net_pnl);
                
            report.push_str(&trade_details);
            
            bot.send_message(msg.chat.id, report).parse_mode(teloxide::types::ParseMode::Html).await?;
        }
        Command::Guard(args) => {
            let parts: Vec<&str> = args.split_whitespace().collect();
            let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1/".to_string());
            let mut con = match redis::Client::open(redis_url).ok() {
                Some(c) => match c.get_multiplexed_async_connection().await {
                    Ok(con) => con,
                    Err(e) => {
                        bot.send_message(msg.chat.id, format!("❌ Redis 连接失败: {}", e)).await?;
                        return Ok(());
                    }
                },
                None => {
                    bot.send_message(msg.chat.id, "❌ Redis 配置无效").await?;
                    return Ok(());
                }
            };

            let set = |con: &mut redis::aio::MultiplexedConnection, key: &'static str, val: String| {
                let mut con = con.clone();
                async move {
                    let _: () = redis::cmd("SET").arg(key).arg(val).query_async(&mut con).await.unwrap_or(());
                }
            };

            let reply = match parts.as_slice() {
                [] | ["status"] => {
                    let get = |key: &str, def: &str| {
                        let mut con = con.clone();
                        let key = key.to_string();
                        let def = def.to_string();
                        async move {
                            redis::cmd("GET").arg(&key).query_async::<Option<String>>(&mut con).await.ok().flatten().unwrap_or(def)
                        }
                    };
                    let enabled = get("GUARD_ENABLED", "1").await;
                    let stop = get("GUARD_STOP_ROE", "50").await;
                    let alert = get("GUARD_HOLD_ALERT_MIN", "30").await;
                    let ac = get("GUARD_AUTO_CLOSE_MIN", "0").await;
                    let trail_on = get("GUARD_TRAIL_ENABLED", "1").await;
                    let trail_arm = get("GUARD_TRAIL_ARM_ROE", "20").await;
                    let giveback = get("GUARD_TRAIL_GIVEBACK_PCT", "0").await;
                    let trail_pct = get("GUARD_TRAIL_ROE", "15").await;
                    let guarded: Vec<String> = redis::cmd("KEYS").arg("GUARD_OPENED_*").query_async(&mut con).await.unwrap_or_default();
                    let mut pos_lines = String::new();
                    let now_ms = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64;
                    for key in &guarded {
                        let sym = key.trim_start_matches("GUARD_OPENED_");
                        if let Ok(Some(ts)) = redis::cmd("GET").arg(key).query_async::<Option<String>>(&mut con).await {
                            if let Ok(ts) = ts.parse::<u64>() {
                                pos_lines.push_str(&format!("  • {} (已持仓 {} 分钟)\n", sym, now_ms.saturating_sub(ts) / 60_000));
                            }
                        }
                    }
                    if pos_lines.is_empty() {
                        pos_lines = "  (当前无被监护的仓位)\n".to_string();
                    }
                    format!(
                        "🛡 <b>仓位保镖状态</b>\n\n\
                        开关: {}\n\
                        硬止损线: ROE -{}% (按各仓位杠杆折算币价, 交易所侧标记价格触发)\n\
                        移动止盈: {}\n\
                        持仓超时提醒: {} 分钟\n\
                        超时自动平仓: {}\n\n\
                        <b>监护中的仓位:</b>\n{}",
                        if enabled == "1" { "✅ 开启" } else { "⛔️ 关闭" },
                        stop,
                        if trail_on == "1" {
                            if giveback != "0" && !giveback.is_empty() {
                                format!("✅ ROE +{}% 激活, 回吐峰值的 {}% 落袋 (比例模式)", trail_arm, giveback)
                            } else {
                                format!("✅ ROE +{}% 激活, 峰值回吐 ROE {}% 落袋", trail_arm, trail_pct)
                            }
                        } else { "⛔️ 关闭".to_string() },
                        alert,
                        // HTML parse mode: 裸尖括号会被 Telegram 当非法标签拒收整条消息
                        if ac == "0" { "关闭 (用 /guard autoclose &lt;分钟&gt; 开启)".to_string() } else { format!("{} 分钟", ac) },
                        pos_lines
                    )
                }
                ["stats"] => {
                    // 滚动实时统计: 只统计最近7天真实了结的仓位 —— 参数讨论的唯一合法数据源
                    let raw: Vec<String> = redis::cmd("LRANGE").arg("GUARD_TRADE_LOG").arg(0).arg(199).query_async(&mut con).await.unwrap_or_default();
                    let now_ms = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64;
                    let week_ago = now_ms.saturating_sub(7 * 24 * 3600 * 1000);
                    let mut n = 0u32;
                    let mut armed_n = 0u32;
                    let mut direct_n = 0u32;
                    let mut total_pnl = 0.0f64;
                    let mut peaks: Vec<f64> = Vec::new();
                    let mut recent_lines = String::new();
                    for (i, s) in raw.iter().enumerate() {
                        let Ok(v) = serde_json::from_str::<serde_json::Value>(s) else { continue };
                        let ts = v["ts"].as_u64().unwrap_or(0);
                        if ts < week_ago { continue; }
                        n += 1;
                        let armed = v["armed"].as_bool().unwrap_or(false);
                        let direct = v["direct_exit"].as_bool().unwrap_or(false);
                        if armed { armed_n += 1; }
                        if direct { direct_n += 1; }
                        let pnl: f64 = v["realized"].as_str().and_then(|x| x.parse().ok()).unwrap_or(0.0);
                        let peak: f64 = v["peak_roe"].as_str().and_then(|x| x.parse().ok()).unwrap_or(0.0);
                        total_pnl += pnl;
                        peaks.push(peak);
                        if i < 5 {
                            recent_lines.push_str(&format!("  • {} 峰值{:+.1}% {} 盈亏{:+.2}U\n",
                                v["sym"].as_str().unwrap_or("?"), peak,
                                if direct { "🎯直接落袋" } else if armed { "🔒激活" } else { "—" }, pnl));
                        }
                    }
                    if n == 0 {
                        "📊 最近 7 天没有已了结的受监护仓位, 统计空白。".to_string()
                    } else {
                        peaks.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                        let median_peak = peaks[peaks.len() / 2];
                        let reach10 = peaks.iter().filter(|p| **p >= 10.0).count();
                        format!(
                            "📊 <b>保镖滚动实况 (最近7天, {}单)</b>\n\n激活率: {}/{} ({:.0}%) | 峰值曾过+10%: {}/{} ({:.0}%)\n峰值ROE中位数: {:+.1}%\n移动止盈直接落袋: {} 次\n合计已实现: <b>{:+.2}U</b>\n\n最近5单:\n{}\n💡 当下市场的真实数据, 参数讨论以此为准。",
                            n, armed_n, n, 100.0 * armed_n as f64 / n as f64,
                            reach10, n, 100.0 * reach10 as f64 / n as f64,
                            median_peak, direct_n, total_pnl, recent_lines)
                    }
                }
                ["on"] => {
                    set(&mut con, "GUARD_ENABLED", "1".into()).await;
                    "🛡 保镖已开启，10 秒内开始巡逻。".to_string()
                }
                ["off"] => {
                    set(&mut con, "GUARD_ENABLED", "0".into()).await;
                    "⚠️ 保镖已下岗！新仓位不会再自动挂止损，已挂的止损单保留。".to_string()
                }
                ["stop", v] => {
                    match v.parse::<f64>() {
                        Ok(p) if (5.0..=90.0).contains(&p) => {
                            set(&mut con, "GUARD_STOP_ROE", v.to_string()).await;
                            format!("✅ 硬止损线已设为 ROE -{}% (按各仓位杠杆折算币价, 10x 时 = 币价 -{:.1}%)。已挂的止损单在均价漂移后会自动按新参数重挂，或平仓后对新仓生效。", p, p / 10.0)
                        }
                        _ => "❌ 无效参数，范围 5 ~ 90 (ROE 百分比)。例: /guard stop 50".to_string(),
                    }
                }
                ["alert", v] => {
                    match v.parse::<u64>() {
                        Ok(m) if m > 0 => {
                            set(&mut con, "GUARD_HOLD_ALERT_MIN", v.to_string()).await;
                            format!("✅ 持仓超时提醒已设为 {} 分钟 (之后每 {} 分钟重复提醒)。", m, m)
                        }
                        _ => "❌ 无效参数。例: /guard alert 30".to_string(),
                    }
                }
                ["autotune", v] if *v == "on" || *v == "off" => {
                    let on = *v == "on";
                    set(&mut con, "GUARD_AUTO_TUNE", if on { "1".into() } else { "0".into() }).await;
                    if on { "🎛 自适应调参已开: 激活线每小时按最近20单峰值中位数自动校准 (界内7~25, 单次±2, 每次调整会播报)。".to_string() }
                    else { "⏸ 自适应调参已关: 激活线固定为当前值, 手动 /guard trail 管理。".to_string() }
                }
                ["giveback", "off"] => {
                    set(&mut con, "GUARD_TRAIL_GIVEBACK_PCT", "0".into()).await;
                    "✅ 比例回吐已关闭, 回到固定回吐模式 (/guard trail 的第二个参数)。".to_string()
                }
                ["giveback", v] => {
                    match v.parse::<f64>() {
                        Ok(p) if (10.0..=60.0).contains(&p) => {
                            set(&mut con, "GUARD_TRAIL_GIVEBACK_PCT", v.to_string()).await;
                            let arm: f64 = redis::cmd("GET").arg("GUARD_TRAIL_ARM_ROE").query_async::<Option<String>>(&mut con).await.ok().flatten().and_then(|s| s.parse().ok()).unwrap_or(20.0);
                            format!("🔒 比例回吐已开启: 从峰值回吐其 {}% 落袋 (低峰值紧、高峰值宽)。\n激活后最差出场 ≈ ROE +{:.1}%; 峰值+50% 时落袋 ≈ +{:.0}%。", p, arm * (1.0 - p / 100.0), 50.0 * (1.0 - p / 100.0))
                        }
                        _ => "❌ 无效参数, 范围 10 ~ 60 (回吐峰值的百分比)。例: /guard giveback 30".to_string(),
                    }
                }
                ["trail", "off"] => {
                    set(&mut con, "GUARD_TRAIL_ENABLED", "0".into()).await;
                    "⛔️ 移动止盈已关闭, 只保留固定硬止损。".to_string()
                }
                ["trail", "on"] => {
                    set(&mut con, "GUARD_TRAIL_ENABLED", "1".into()).await;
                    "🔒 移动止盈已开启。".to_string()
                }
                ["trail", arm, pct] => {
                    match (arm.parse::<f64>(), pct.parse::<f64>()) {
                        (Ok(a), Ok(p)) if (2.0..=300.0).contains(&a) && (1.0..=100.0).contains(&p) && a > p => {
                            set(&mut con, "GUARD_TRAIL_ARM_ROE", arm.to_string()).await;
                            set(&mut con, "GUARD_TRAIL_ROE", pct.to_string()).await;
                            set(&mut con, "GUARD_TRAIL_ENABLED", "1".into()).await;
                            // 手动定参 = 用户意志优先, 自动关掉自适应防止被悄悄覆盖
                            set(&mut con, "GUARD_AUTO_TUNE", "0".into()).await;
                            format!("🔒 移动止盈已设为: ROE +{}% 激活, 峰值回吐 ROE {}% 落袋 (按各仓位杠杆自动折算币价)。收到【止损已实际上移】确认后, 该仓位最差保本出场。", a, p)
                        }
                        _ => "❌ 无效参数 (ROE 百分比)。要求: 激活 &gt; 回吐, 例: /guard trail 20 15".to_string(),
                    }
                }
                ["autoclose", "off"] | ["autoclose", "0"] => {
                    set(&mut con, "GUARD_AUTO_CLOSE_MIN", "0".into()).await;
                    "✅ 超时自动平仓已关闭，超时后只提醒不动手。".to_string()
                }
                ["autoclose", v] => {
                    match v.parse::<u64>() {
                        Ok(m) if m > 0 => {
                            set(&mut con, "GUARD_AUTO_CLOSE_MIN", v.to_string()).await;
                            format!("✂️ 超时自动平仓已开启: 任何仓位持有满 {} 分钟将被市价平掉。", m)
                        }
                        _ => "❌ 无效参数。例: /guard autoclose 60 或 /guard autoclose off".to_string(),
                    }
                }
                _ => "用法 (百分比一律按 ROE, 即 App 显示的收益率):\n/guard status - 查看状态\n/guard on|off - 开关\n/guard stop 50 - 硬止损 ROE -50%\n/guard trail 20 15 - 移动止盈: ROE +20% 激活, 峰值回吐 15% 落袋\n/guard giveback 30 - 比例回吐: 回吐峰值的30% (低峰紧高峰宽)\n/guard autotune on|off - 激活线自适应调参 (手动 trail 会自动关它)\n/guard giveback off - 回到固定回吐\n/guard trail on|off - 移动止盈开关\n/guard alert 30 - 超时提醒分钟数\n/guard autoclose 60|off - 超时自动平仓".to_string(),
            };
            bot.send_message(msg.chat.id, reply).parse_mode(teloxide::types::ParseMode::Html).await?;
        }
        Command::Paper(args) => {
            let parts: Vec<&str> = args.split_whitespace().collect();
            let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1/".to_string());
            let mut con = match redis::Client::open(redis_url).ok() {
                Some(c) => match c.get_multiplexed_async_connection().await {
                    Ok(con) => con,
                    Err(e) => {
                        bot.send_message(msg.chat.id, format!("❌ Redis 连接失败: {}", e)).await?;
                        return Ok(());
                    }
                },
                None => {
                    bot.send_message(msg.chat.id, "❌ Redis 配置无效").await?;
                    return Ok(());
                }
            };

            let reply = match parts.as_slice() {
                [] | ["status"] => {
                    let enabled = redis::cmd("GET").arg("PAPER_ENABLED").query_async::<Option<String>>(&mut con).await.ok().flatten().unwrap_or_else(|| "1".into());
                    let trail = redis::cmd("GET").arg("PAPER_TRAIL_PCT").query_async::<Option<String>>(&mut con).await.ok().flatten().unwrap_or_else(|| "3.0".into());
                    let stats: crate::paper::PaperStats = redis::cmd("GET").arg("PAPER_STATS").query_async::<Option<String>>(&mut con).await.ok().flatten()
                        .and_then(|s| serde_json::from_str(&s).ok()).unwrap_or_default();
                    let pos_keys: Vec<String> = redis::cmd("KEYS").arg("PAPER_POS_*").query_async(&mut con).await.unwrap_or_default();
                    let mut pos_lines = String::new();
                    for key in &pos_keys {
                        if let Ok(Some(json)) = redis::cmd("GET").arg(key).query_async::<Option<String>>(&mut con).await {
                            if let Ok(p) = serde_json::from_str::<crate::paper::PaperPos>(&json) {
                                pos_lines.push_str(&format!("  • {} 入场 {:.6} | 最高 {:.6}\n", p.sym, p.entry, p.peak));
                            }
                        }
                    }
                    if pos_lines.is_empty() {
                        pos_lines = "  (无)\n".to_string();
                    }
                    let pf = if stats.gross_loss > 0.0 { stats.gross_win / stats.gross_loss } else { 0.0 };
                    format!(
                        "📝 <b>纸面交易引擎</b> (零实弹前向实测)\n\n\
                        开关: {} | 高点回撤线: {}%\n\n\
                        📒 <b>账本:</b> {} 笔 | 胜率 {:.0}% | 盈利因子 {:.2}\n\
                        累计净盈亏: <b>{:+.2}U</b> (虚拟)\n\n\
                        <b>虚拟持仓:</b>\n{}\n\
                        💡 实弹门槛: 数周后盈利因子 &gt; 1.3 且样本够大",
                        if enabled == "1" { "✅ 运行中" } else { "⛔️ 已停" }, trail,
                        stats.trades,
                        if stats.trades > 0 { 100.0 * stats.wins as f64 / stats.trades as f64 } else { 0.0 },
                        pf, stats.total_net, pos_lines)
                }
                ["on"] => {
                    let _: () = redis::cmd("SET").arg("PAPER_ENABLED").arg("1").query_async(&mut con).await.unwrap_or(());
                    "📝 纸面交易引擎已开启。".to_string()
                }
                ["off"] => {
                    let _: () = redis::cmd("SET").arg("PAPER_ENABLED").arg("0").query_async(&mut con).await.unwrap_or(());
                    "⛔️ 纸面交易引擎已暂停 (虚拟持仓保留, 恢复后继续跟踪)。".to_string()
                }
                ["trail", v] => {
                    match v.parse::<f64>() {
                        Ok(p) if (0.5..=20.0).contains(&p) => {
                            let _: () = redis::cmd("SET").arg("PAPER_TRAIL_PCT").arg(v.to_string()).query_async(&mut con).await.unwrap_or(());
                            format!("✅ 纸面高点回撤线已设为 {}%。注意: 改参数会让账本和回测基准不可比。", p)
                        }
                        _ => "❌ 无效参数, 范围 0.5 ~ 20。例: /paper trail 3".to_string(),
                    }
                }
                ["reset"] => {
                    let pos_keys: Vec<String> = redis::cmd("KEYS").arg("PAPER_POS_*").query_async(&mut con).await.unwrap_or_default();
                    for key in pos_keys {
                        let _: () = redis::cmd("DEL").arg(&key).query_async(&mut con).await.unwrap_or(());
                    }
                    let _: () = redis::cmd("DEL").arg("PAPER_STATS").query_async(&mut con).await.unwrap_or(());
                    "🧹 纸面账本与虚拟持仓已清零, 重新开始记录。".to_string()
                }
                _ => "用法:\n/paper status - 账本与持仓\n/paper on|off - 开关\n/paper trail 3 - 回撤线百分比\n/paper reset - 清零账本".to_string(),
            };
            bot.send_message(msg.chat.id, reply).parse_mode(teloxide::types::ParseMode::Html).await?;
        }
        Command::GridLive(args) => {
            let parts: Vec<&str> = args.split_whitespace().collect();
            let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1/".to_string());
            let mut con = match redis::Client::open(redis_url).ok() {
                Some(c) => match c.get_multiplexed_async_connection().await {
                    Ok(con) => con,
                    Err(e) => {
                        bot.send_message(msg.chat.id, format!("❌ Redis 连接失败: {}", e)).await?;
                        return Ok(());
                    }
                },
                None => {
                    bot.send_message(msg.chat.id, "❌ Redis 配置无效").await?;
                    return Ok(());
                }
            };
            let get = |con: &mut redis::aio::MultiplexedConnection, key: &str, def: &str| {
                let mut con = con.clone();
                let key = key.to_string();
                let def = def.to_string();
                async move {
                    redis::cmd("GET").arg(&key).query_async::<Option<String>>(&mut con).await.ok().flatten().unwrap_or(def)
                }
            };
            let reply = match parts.as_slice() {
                [] | ["status"] => {
                    let enabled = get(&mut con, "GRID_LIVE_ENABLED", "0").await;
                    let auto_fill = get(&mut con, "GRID_LIVE_AUTO", "1").await;
                    let pins_raw = get(&mut con, "GRID_LIVE_PINS", "[]").await;
                    let budget = get(&mut con, "GRID_LIVE_BUDGET", "50").await;
                    let slots = get(&mut con, "GRID_LIVE_MAX_ACTIVE", "2").await;
                    let keys: Vec<String> = redis::cmd("KEYS").arg("GRID_LIVE_STATE_*").query_async(&mut con).await.unwrap_or_default();
                    let mut state_lines = String::new();
                    for k in &keys {
                        if let Ok(Some(s)) = redis::cmd("GET").arg(k).query_async::<Option<String>>(&mut con).await {
                            if let Ok(v) = serde_json::from_str::<serde_json::Value>(&s) {
                                state_lines.push_str(&format!(
                                    "  • {} | 区间 {} ~ {} | 已实现 {}U ({} 回合)\n",
                                    v["symbol"].as_str().unwrap_or("?"),
                                    v["lines"][0], v["lines"].as_array().and_then(|a| a.last()).cloned().unwrap_or_default(),
                                    v["realized_total"], v["round_trips"]));
                            }
                        }
                    }
                    if state_lines.is_empty() {
                        state_lines = "  (无运行中的网格)\n".to_string();
                    }
                    format!(
                        "🔲 <b>网格实盘执行器</b> (真钱模块)\n\n开关: {} | 自动补位: {} | 钉选: {} | 预算: {}U/网格 | 并发上限: {}\n<b>运行中的网格:</b>\n{}\n⚠️ 唯一会下真实现货订单的模块, 默认关闭。",
                        if enabled == "1" { "🔴 运行中 (真实下单!)" } else { "⚪️ 关闭" },
                        if auto_fill == "1" { "✅" } else { "⛔️" },
                        if pins_raw == "[]" { "(无)".to_string() } else { pins_raw.clone() },
                        budget, slots, state_lines)
                }
                ["on"] => {
                    let _: () = redis::cmd("SET").arg("GRID_LIVE_ENABLED").arg("1").query_async(&mut con).await.unwrap_or(());
                    "🔴 网格实盘已开启 (真实下单!)\n钉选的币优先摆盘, 剩余槽位由自动选币补齐 (/gridlive auto off 可关自动补位)。\n每个网格占一份预算, 现货 USDT 余额不够时不开新网格。".to_string()
                }
                ["on", sym] => {
                    let sym = sym.to_uppercase();
                    if !sym.ends_with("USDT") {
                        "❌ 只支持 USDT 交易对, 例: /gridlive on ACTUSDT".to_string()
                    } else {
                        // 钉选: 加入 pin 列表, 与自动选的网格共存, 不影响已在跑的
                        let mut pins: Vec<String> = redis::cmd("GET").arg("GRID_LIVE_PINS").query_async::<Option<String>>(&mut con).await
                            .ok().flatten().and_then(|s| serde_json::from_str(&s).ok()).unwrap_or_default();
                        if !pins.contains(&sym) {
                            pins.push(sym.clone());
                        }
                        if let Ok(j) = serde_json::to_string(&pins) {
                            let _: () = redis::cmd("SET").arg("GRID_LIVE_PINS").arg(j).query_async(&mut con).await.unwrap_or(());
                        }
                        let _: () = redis::cmd("SET").arg("GRID_LIVE_ENABLED").arg("1").query_async(&mut con).await.unwrap_or(());
                        format!("📌 已钉选 {} (真实下单!) —— 与在跑的其他网格共存, 有空槽位且现货 USDT 够一份预算时优先为它摆盘。\n取消钉选: /gridlive unpin {} | 停掉该网格: /gridlive stop {}", sym, sym, sym)
                    }
                }
                ["unpin", sym] => {
                    let sym = sym.to_uppercase();
                    let mut pins: Vec<String> = redis::cmd("GET").arg("GRID_LIVE_PINS").query_async::<Option<String>>(&mut con).await
                        .ok().flatten().and_then(|s| serde_json::from_str(&s).ok()).unwrap_or_default();
                    pins.retain(|p| p != &sym);
                    if let Ok(j) = serde_json::to_string(&pins) {
                        let _: () = redis::cmd("SET").arg("GRID_LIVE_PINS").arg(j).query_async(&mut con).await.unwrap_or(());
                    }
                    format!("✅ 已取消钉选 {} (在跑的网格不受影响, 跑到失效为止; 立即停掉用 /gridlive stop {})", sym, sym)
                }
                ["stop", sym] => {
                    let sym = sym.to_uppercase();
                    let _: () = redis::cmd("SET").arg(format!("GRID_LIVE_STOP_{}", sym)).arg("1").query_async(&mut con).await.unwrap_or(());
                    // 同时取消钉选, 否则空槽位下一轮又会把它开回来
                    let mut pins: Vec<String> = redis::cmd("GET").arg("GRID_LIVE_PINS").query_async::<Option<String>>(&mut con).await
                        .ok().flatten().and_then(|s| serde_json::from_str(&s).ok()).unwrap_or_default();
                    pins.retain(|p| p != &sym);
                    if let Ok(j) = serde_json::to_string(&pins) {
                        let _: () = redis::cmd("SET").arg("GRID_LIVE_PINS").arg(j).query_async(&mut con).await.unwrap_or(());
                    }
                    format!("⏸ 停止指令已发出: {} 的网格将在 20 秒内撤单 (库存保留), 且已取消钉选。", sym)
                }
                ["auto", v] if *v == "on" || *v == "off" => {
                    let on = *v == "on";
                    let _: () = redis::cmd("SET").arg("GRID_LIVE_AUTO").arg(if on { "1" } else { "0" }).query_async(&mut con).await.unwrap_or(());
                    if on { "✅ 自动补位已开: 空槽位从候选榜自动选币。".to_string() }
                    else { "⏸ 自动补位已关: 只跑钉选的币, 网格失效后不换新币。".to_string() }
                }
                ["off"] => {
                    let _: () = redis::cmd("SET").arg("GRID_LIVE_ENABLED").arg("0").query_async(&mut con).await.unwrap_or(());
                    "⏸ 网格实盘暂停指令已发出: 执行器将在 20 秒内撤销全部挂单 (已买入的库存保留)。".to_string()
                }
                ["liquidate"] => {
                    let _: () = redis::cmd("SET").arg("GRID_LIVE_LIQUIDATE").arg("1").query_async(&mut con).await.unwrap_or(());
                    "🧹 清算指令已发出: 执行器将在 20 秒内撤单+市价卖出全部库存+停机。".to_string()
                }
                ["budget", v] => {
                    match v.parse::<f64>() {
                        Ok(b) if (10.0..=100000.0).contains(&b) => {
                            let _: () = redis::cmd("SET").arg("GRID_LIVE_BUDGET").arg(v.to_string()).query_async(&mut con).await.unwrap_or(());
                            format!("✅ 每个网格的预算已设为 {}U (对运行中的网格不生效, 新网格启动时使用; 开 N 个网格需要现货钱包里有 N 份预算)。", b)
                        }
                        _ => "❌ 无效预算, 范围 10 ~ 100000 U。".to_string(),
                    }
                }
                ["slots", v] => {
                    match v.parse::<usize>() {
                        Ok(n) if (1..=6).contains(&n) => {
                            let _: () = redis::cmd("SET").arg("GRID_LIVE_MAX_ACTIVE").arg(v.to_string()).query_async(&mut con).await.unwrap_or(());
                            format!("✅ 并发网格数上限已设为 {} (每个网格独立占用一份预算, 现货 USDT 余额不够一份预算时不会开新网格)。", n)
                        }
                        _ => "❌ 无效槽位数, 范围 1 ~ 6。".to_string(),
                    }
                }
                _ => "用法:\n/gridlive status - 查看状态\n/gridlive on - 开启 (真钱! 自动选币补位)\n/gridlive on SYMBOL - 钉选某币 (与其他网格共存)\n/gridlive unpin SYMBOL - 取消钉选\n/gridlive stop SYMBOL - 停掉单个网格 (撤单保留库存)\n/gridlive auto on|off - 自动补位开关\n/gridlive off - 全部暂停\n/gridlive liquidate - 全部清算离场\n/gridlive budget 50 - 设每网格预算\n/gridlive slots 2 - 设并发上限".to_string(),
            };
            bot.send_message(msg.chat.id, reply).parse_mode(teloxide::types::ParseMode::Html).await?;
        }
    }
    Ok(())
}
