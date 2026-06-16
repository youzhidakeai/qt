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
    #[command(description = "狙击做多。用法: /buy &lt;交易对&gt; &lt;USDT本金&gt; &lt;杠杆&gt; (例: /buy ETHUSDT 500 10)")]
    Buy(String),
    #[command(description = "狙击做空。用法: /sell &lt;交易对&gt; &lt;USDT本金&gt; &lt;杠杆&gt;")]
    Sell(String),
    #[command(description = "紧急清仓某个币种 (仅清空记忆不平仓)。用法: /panic DOGEUSDT")]
    Panic(String),
    #[command(description = "一键市价全平 (真实平仓并清空记忆)。用法: /close <交易对>")]
    Close(String),
    #[command(description = "从币安同步真实仓位并开启监控。用法: /sync <交易对>")]
    Sync(String),
    #[command(description = "检查交易所连接")]
    Status,
    #[command(description = "查看当前所有持仓与未实现收益")]
    Pnl,
    #[command(description = "获取当前所有订阅币种的实时盘口价")]
    Price,
    #[command(description = "生成昨日盈亏与交易手续费总结")]
    Yesterday,
}

pub async fn run_telegram_bot(
    exec_client: Arc<BinanceExecutionClient>,
    contexts: Arc<HashMap<String, SymbolContext>>,
) {
    let bot = Bot::from_env();
    info!("🚀 Telegram 遥控机器人已启动，多币种矩阵接入完毕！");

    let handler = Update::filter_message()
        .filter_command::<Command>()
        .endpoint(answer);

    Dispatcher::builder(bot, handler)
        .dependencies(dptree::deps![exec_client, contexts])
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
) -> ResponseResult<()> {
    match cmd {
        Command::Help => {
            bot.send_message(msg.chat.id, Command::descriptions().to_string()).parse_mode(teloxide::types::ParseMode::Html).await?;
        }
        Command::Price => {
            let mut report = String::from("📈 实时盘口询价反馈\n\n");
            let mut sym_keys: Vec<String> = contexts.keys().cloned().collect();
            sym_keys.sort();
            
            for sym in sym_keys {
                if let Some(ctx) = contexts.get(&sym) {
                    let book = ctx.ob_manager.book.read().unwrap();
                    let best_bid = book.bids.iter().next_back().map(|(p, _)| *p).unwrap_or(Decimal::ZERO);
                    let best_ask = book.asks.iter().next().map(|(p, _)| *p).unwrap_or(Decimal::ZERO);
                    
                    if best_bid > Decimal::ZERO && best_ask > Decimal::ZERO {
                        let mid = (best_bid + best_ask) / dec!(2);
                        report.push_str(&format!("🔹 <b>{}</b>: {:.5}\n    买一: {} | 卖一: {}\n", sym, mid, best_bid, best_ask));
                    } else {
                        report.push_str(&format!("🔹 <b>{}</b>: 深度数据获取中...\n", sym));
                    }
                }
            }
            bot.send_message(msg.chat.id, report).parse_mode(teloxide::types::ParseMode::Html).await?;
        }
        Command::Yesterday => {
            use time::{OffsetDateTime, UtcOffset, Duration, Time};
            let offset = UtcOffset::from_hms(8, 0, 0).unwrap();
            let now = OffsetDateTime::now_utc().to_offset(offset);
            let today_midnight = now.replace_time(Time::MIDNIGHT);
            let start_of_yesterday = today_midnight - Duration::days(1);
            let end_of_yesterday = today_midnight - Duration::nanoseconds(1);
            
            let start_ts = start_of_yesterday.unix_timestamp() * 1000;
            let end_ts = end_of_yesterday.unix_timestamp() * 1000;
            
            if let Ok(res) = exec_client.get_income_history(start_ts as u64, end_ts as u64).await {
                if let Ok(records) = serde_json::from_str::<serde_json::Value>(&res) {
                    if let Some(arr) = records.as_array() {
                        let mut total_pnl = Decimal::ZERO;
                        let mut total_fee = Decimal::ZERO;
                        let mut total_funding = Decimal::ZERO;
                        
                        for item in arr {
                            if let (Some(income_type), Some(income)) = (item.get("incomeType").and_then(|v| v.as_str()), item.get("income").and_then(|v| v.as_str())) {
                                if let Ok(val) = Decimal::from_str(income) {
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
            if parts.len() != 3 {
                bot.send_message(msg.chat.id, "⚠️ 参数错误。\n用法: /buy &lt;交易对&gt; &lt;USDT本金&gt; &lt;杠杆倍数&gt;\n例如: /buy ETHUSDT 500 10").parse_mode(teloxide::types::ParseMode::Html).await?;
                return Ok(());
            }
            let symbol = parts[0].to_uppercase();
            let margin_usdt = Decimal::from_str(parts[1]).unwrap_or_default();
            let leverage = Decimal::from_str(parts[2]).unwrap_or(dec!(1));
            
            if let Some(ctx) = contexts.get(&symbol) {
                // 1. 设置真实杠杆倍数
                let lev_u32 = leverage.to_u32().unwrap_or(1);
                let _ = exec_client.set_leverage(&symbol, lev_u32).await;

                // 2. 盘口估价与数量计算
                let estimated_entry = ctx.ob_manager.book.read().unwrap().asks.iter().next().map(|(p, _)| *p).unwrap_or(dec!(1));
                let notional = margin_usdt * leverage;
                let mut target_qty = notional / estimated_entry;
                
                let precision = match symbol.as_str() {
                    "BTCUSDT" => 3,
                    "ETHUSDT" => 3,
                    "BNBUSDT" => 2,
                    _ => 0,
                };
                target_qty.rescale(precision); 

                bot.send_message(msg.chat.id, format!("⚡️ 准备市价做多 {}...\n本金: {} U | 杠杆: {}x\n计算下单量: {} 个币", 
                    symbol, margin_usdt, leverage, target_qty)).await?;
                
                // 3. 执行真实市价单
                let qty_str = target_qty.to_string();
                let res = exec_client.place_order(&symbol, "BUY", "MARKET", &qty_str, false).await;
                
                match res {
                    Ok(real_avg_price) => {
                        // 4. 解析真实的成交均价 (Fill Price)，防滑点
                        let fill_price = if real_avg_price > Decimal::ZERO { real_avg_price } else { estimated_entry };
                        
                        let _ = ctx.control_tx.send(ControlMessage::TradeExecuted {
                            trade_qty: target_qty, // 买入是正数
                            fill_price,
                        }).await;
                        
                        bot.send_message(msg.chat.id, format!("✅ {} 做多成功！\n真实成交均价 (Fill Price): {}\n已写入硬盘并挂载移动止损。", symbol, fill_price)).parse_mode(teloxide::types::ParseMode::Html).await?;
                    }
                    Err(e) => { bot.send_message(msg.chat.id, format!("❌ 订单失败：\n{}", e)).parse_mode(teloxide::types::ParseMode::Html).await?; }
                }
            } else {
                bot.send_message(msg.chat.id, format!("⚠️ 系统未订阅交易对: {}", symbol)).parse_mode(teloxide::types::ParseMode::Html).await?;
            }
        }
        Command::Sell(args) => {
            let parts: Vec<&str> = args.split_whitespace().collect();
            if parts.len() != 3 {
                bot.send_message(msg.chat.id, "⚠️ 参数错误。\n用法: /sell &lt;交易对&gt; &lt;USDT本金&gt; &lt;杠杆倍数&gt;\n例如: /sell ETHUSDT 500 10").parse_mode(teloxide::types::ParseMode::Html).await?;
                return Ok(());
            }
            let symbol = parts[0].to_uppercase();
            let margin_usdt = Decimal::from_str(parts[1]).unwrap_or_default();
            let leverage = Decimal::from_str(parts[2]).unwrap_or(dec!(1));
            
            if let Some(ctx) = contexts.get(&symbol) {
                // 1. 设置真实杠杆倍数
                let lev_u32 = leverage.to_u32().unwrap_or(1);
                let _ = exec_client.set_leverage(&symbol, lev_u32).await;

                // 2. 盘口估价与数量计算
                let estimated_entry = ctx.ob_manager.book.read().unwrap().bids.iter().next_back().map(|(p, _)| *p).unwrap_or(dec!(1));
                let notional = margin_usdt * leverage;
                let mut target_qty = notional / estimated_entry;
                let precision = match symbol.as_str() {
                    "BTCUSDT" => 3,
                    "ETHUSDT" => 3,
                    "BNBUSDT" => 2,
                    _ => 0,
                };
                target_qty.rescale(precision);

                bot.send_message(msg.chat.id, format!("⚡️ 准备市价做空 {}...\n本金: {} U | 杠杆: {}x\n计算下单量: {} 个币", 
                    symbol, margin_usdt, leverage, target_qty)).await?;
                
                // 3. 执行真实市价单
                let qty_str = target_qty.to_string();
                let res = exec_client.place_order(&symbol, "SELL", "MARKET", &qty_str, false).await;
                
                match res {
                    Ok(real_avg_price) => {
                        // 4. 解析真实的成交均价
                        let fill_price = if real_avg_price > Decimal::ZERO { real_avg_price } else { estimated_entry };
                        
                        let _ = ctx.control_tx.send(ControlMessage::TradeExecuted {
                            trade_qty: -target_qty, // 卖出是负数
                            fill_price,
                        }).await;
                        
                        bot.send_message(msg.chat.id, format!("✅ {} 做空成功！\n真实成交均价 (Fill Price): {}\n已写入硬盘并挂载移动止损。", symbol, fill_price)).parse_mode(teloxide::types::ParseMode::Html).await?;
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
            if let Some(ctx) = contexts.get(&symbol) {
                let _ = ctx.control_tx.send(ControlMessage::SyncPosition).await;
                bot.send_message(msg.chat.id, format!("⌛️ 正在从币安同步 {} 的真实仓位数据...", symbol)).parse_mode(teloxide::types::ParseMode::Html).await?;
            } else {
                bot.send_message(msg.chat.id, format!("⚠️ 系统未订阅交易对: {}", symbol)).parse_mode(teloxide::types::ParseMode::Html).await?;
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
                                
                                format!("📊 <b>实盘资产报告 (测试网)</b>\n\n💰 <b>钱包总余额</b>: {} USDT\n💵 <b>可用开仓金</b>: {} USDT\n📈 <b>未实现盈亏</b>: {} USDT\n🔓 <b>API 交易权限</b>: {}", 
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
                                
                                total_unpnl += unpnl;
                                
                                let direction = if amt > 0.0 { "🟢 多头 (LONG)" } else { "🔴 空头 (SHORT)" };
                                pnl_text.push_str(&format!(
                                    "<b>{}</b> {}\n\
                                     🔹 持仓量: {}\n\
                                     🔹 开仓价: {}\n\
                                     💵 未实现盈亏: <b>{} USDT</b>\n\n",
                                     symbol, direction, amt_str, entry, unpnl_str
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
    }
    Ok(())
}
