use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::{Serialize, Deserialize};
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{info, error};
use rust_decimal::prelude::FromPrimitive;

use crate::execution::BinanceExecutionClient;
use crate::orderbook::OrderBookManager;
use crate::models::AggTradeUpdate;

#[derive(Debug)]
pub enum ControlMessage {
    TradeExecuted {
        trade_qty: Decimal,
        fill_price: Decimal,
    },
    ClearPosition,
    ClosePosition,
    SyncPosition,
    ForceUpdatePosition {
        amt: Decimal,
        entry: Decimal,
    },
}

#[derive(Serialize, Deserialize, Clone, Default)]
#[serde(default)]
pub struct PositionManager {
    pub symbol: String,
    pub is_isolated: bool,
    pub position_amt: Decimal,
    pub entry_price: Decimal,
    pub highest_price_since_entry: Decimal,
    pub lowest_price_since_entry: Decimal,
}

impl PositionManager {
    pub async fn save_state(&self, redis: &redis::Client) {
        let key = format!("{}_state", self.symbol);
        if let Ok(json) = serde_json::to_string(self) {
            if let Ok(mut con) = redis.get_multiplexed_async_connection().await {
                let _: redis::RedisResult<()> = redis::cmd("SET").arg(&key).arg(json).query_async(&mut con).await;
            }
        }
    }

    pub async fn load_state(symbol: &str, redis: &redis::Client) -> Option<Self> {
        let key = format!("{}_state", symbol);
        if let Ok(mut con) = redis.get_multiplexed_async_connection().await {
            if let Ok(content) = redis::cmd("GET").arg(&key).query_async::<String>(&mut con).await {
                match serde_json::from_str::<Self>(&content) {
                    Ok(state) => return Some(state),
                    Err(e) => {
                        error!("❌ [严重错误] 反序列化 Redis 状态失败！符号: {} | 错误: {} | 原始数据: {}", symbol, e, content);
                    }
                }
            }
        }
        None
    }
}

pub struct StrategyEngine {
    pub position: PositionManager,
    pub exec_client: Arc<BinanceExecutionClient>,
    pub ob_manager: Arc<OrderBookManager>,
    pub redis_client: redis::Client,
    pub mid_price_history: VecDeque<Decimal>,
    pub breakout_window: usize,
    
    pub trailing_sl_pct: Decimal,
    pub round_trip_fee_pct: Decimal,

    pub taker_buy_flow: Decimal,
    pub taker_sell_flow: Decimal,

    // 全自动实盘扣款参数 (测试网可以随便填)
    #[allow(dead_code)]
    pub auto_margin_usdt: Decimal, 
    #[allow(dead_code)]
    pub auto_leverage: u32,
    
    // Telegram 推送通道
    pub tg_tx: mpsc::Sender<String>,
    
    // 日志计数器
    pub tick_counter: u64,
    
    pub signal_tx: mpsc::Sender<crate::portfolio::SignalEvent>,
    pub last_signal_time: Option<std::time::Instant>,
    pub current_funding_rate: Decimal,
    pub last_funding_fetch: Option<std::time::Instant>,
    pub last_tick_time: Option<std::time::Instant>,
}

impl StrategyEngine {
    pub async fn new(
        exec_client: Arc<crate::execution::BinanceExecutionClient>,
        ob_manager: Arc<OrderBookManager>,
        _control_tx: mpsc::Sender<ControlMessage>,
        symbol: &str,
        redis_client: redis::Client,
        signal_tx: mpsc::Sender<crate::portfolio::SignalEvent>,
        tg_tx: mpsc::Sender<String>,
    ) -> Self {
        let position = match PositionManager::load_state(symbol, &redis_client).await {
            Some(pm) => pm,
            None => PositionManager {
                symbol: symbol.to_string(),
                is_isolated: true,
                position_amt: Decimal::ZERO,
                entry_price: Decimal::ZERO,
                highest_price_since_entry: Decimal::ZERO,
                lowest_price_since_entry: dec!(999999999),
            }
        };

        Self {
            position,
            exec_client,
            ob_manager,
            redis_client,
            mid_price_history: VecDeque::with_capacity(300),
            breakout_window: 300,
            trailing_sl_pct: dec!(1.5), // 【优化】将回撤容忍度放宽到 1.5%，防止被山寨币的正常波动（白噪音）频繁止损扫地出门
            round_trip_fee_pct: dec!(0.1),
            taker_buy_flow: Decimal::ZERO,
            taker_sell_flow: Decimal::ZERO,
            auto_margin_usdt: dec!(50.0),
            auto_leverage: 10,
            tg_tx,
            tick_counter: 0,
            signal_tx,
            last_signal_time: None,
            current_funding_rate: Decimal::ZERO,
            last_funding_fetch: None,
            last_tick_time: None,
        }
    }

    pub async fn handle_control_message(&mut self, msg: ControlMessage) {
        match msg {
            ControlMessage::TradeExecuted { trade_qty, fill_price } => {
                let current_qty = self.position.position_amt;
                let new_qty = current_qty + trade_qty;
                
                if new_qty == Decimal::ZERO {
                    // 彻底平仓
                    self.position.position_amt = Decimal::ZERO;
                    info!("🎯 [{}] 仓位已被完全平掉。", self.position.symbol);
                } else if current_qty.is_sign_positive() == trade_qty.is_sign_positive() || current_qty == Decimal::ZERO {
                    // 同向加仓 (或者开新仓)
                    let total_value = current_qty.abs() * self.position.entry_price + trade_qty.abs() * fill_price;
                    self.position.entry_price = total_value / new_qty.abs();
                    self.position.position_amt = new_qty;
                    self.position.highest_price_since_entry = self.position.entry_price;
                    self.position.lowest_price_since_entry = self.position.entry_price;
                    info!("🎯 [{}] 补仓/建仓成功！总仓位: {}，新均价: {}", self.position.symbol, new_qty, self.position.entry_price);
                } else if new_qty.is_sign_positive() != current_qty.is_sign_positive() {
                    // 反手开仓
                    self.position.entry_price = fill_price;
                    self.position.position_amt = new_qty;
                    self.position.highest_price_since_entry = fill_price;
                    self.position.lowest_price_since_entry = fill_price;
                    info!("🎯 [{}] 反手开仓成功！总仓位: {}，新均价: {}", self.position.symbol, new_qty, fill_price);
                } else {
                    // 部分平仓
                    self.position.position_amt = new_qty;
                    info!("🎯 [{}] 部分平仓成功！剩余仓位: {}，均价保持不变: {}", self.position.symbol, new_qty, self.position.entry_price);
                }
                
                self.position.save_state(&self.redis_client).await;
            }
            ControlMessage::ClearPosition => {
                self.position.position_amt = Decimal::ZERO;
                self.position.save_state(&self.redis_client).await;
            }
            ControlMessage::ClosePosition => {
                let current_qty = self.position.position_amt;
                if current_qty == Decimal::ZERO {
                    let _ = self.tg_tx.send(format!("⚠️ [{}] 当前大脑记忆中没有仓位，无需平仓。如果币安 APP 上有遗留仓位，请手动在 APP 上平掉。", self.position.symbol)).await;
                    return;
                }
                let side = if current_qty > Decimal::ZERO { "SELL" } else { "BUY" };
                let qty_str = current_qty.abs().to_string();
                let res = self.exec_client.place_order(&self.position.symbol, side, "MARKET", &qty_str, true).await;
                match res {
                    Ok(_) => {
                        self.position.position_amt = Decimal::ZERO;
                        self.position.save_state(&self.redis_client).await;
                        let _ = self.tg_tx.send(format!("✅ [{}] 一键手动平仓成功！仓位已清零。", self.position.symbol)).await;
                    }
                    Err(e) => {
                        let _ = self.tg_tx.send(format!("❌ [{}] 一键手动平仓失败: {}", self.position.symbol, e)).await;
                    }
                }
            }
            ControlMessage::SyncPosition => {
                let pos_str = match self.exec_client.check_positions().await {
                    Ok(s) => s,
                    Err(e) => {
                        let _ = self.tg_tx.send(format!("❌ [{}] 同步仓位失败: 无法连接币安API ({})", self.position.symbol, e)).await;
                        return;
                    }
                };
                let positions: Vec<serde_json::Value> = serde_json::from_str(&pos_str).unwrap_or_default();
                let mut found = false;
                use std::str::FromStr;
                for pos in positions {
                    let sym = pos.get("symbol").and_then(|v| v.as_str()).unwrap_or("");
                    if sym == self.position.symbol {
                        let amt = pos.get("positionAmt").and_then(|v| v.as_str()).and_then(|s| rust_decimal::Decimal::from_str(s).ok()).unwrap_or(rust_decimal::Decimal::ZERO);
                        if amt.abs() > rust_decimal::Decimal::ZERO {
                            let entry = pos.get("entryPrice").and_then(|v| v.as_str()).and_then(|s| rust_decimal::Decimal::from_str(s).ok()).unwrap_or(rust_decimal::Decimal::ZERO);
                            self.position.position_amt = amt;
                            self.position.entry_price = entry;
                            
                            // 获取最新盘口价作为当前的最高/最低基准
                            let current_price = {
                                let ob = self.ob_manager.book.read().unwrap();
                                if amt > rust_decimal::Decimal::ZERO {
                                    ob.bids.iter().next_back().map(|(p, _)| *p).unwrap_or(entry)
                                } else {
                                    ob.asks.iter().next().map(|(p, _)| *p).unwrap_or(entry)
                                }
                            };
                            
                            self.position.highest_price_since_entry = if current_price > entry { current_price } else { entry };
                            self.position.lowest_price_since_entry = if current_price < entry && current_price > rust_decimal::Decimal::ZERO { current_price } else { entry };
                            
                            self.position.save_state(&self.redis_client).await;
                            let _ = self.tg_tx.send(format!("✅ [{}] 手动仓位接管成功！\n持仓量: {}\n开仓均价: {}\n已挂载移动止损保护，当前监测基准价: {}", self.position.symbol, amt, entry, current_price)).await;
                        } else {
                            self.position.position_amt = rust_decimal::Decimal::ZERO;
                            self.position.save_state(&self.redis_client).await;
                            let _ = self.tg_tx.send(format!("⚠️ [{}] 币安 APP 上当前没有持仓。大脑记忆已同步清零。", self.position.symbol)).await;
                        }
                        found = true;
                        break;
                    }
                }
                if !found {
                    let _ = self.tg_tx.send(format!("❌ [{}] 同步仓位失败: 未在币安账户找到该交易对数据", self.position.symbol)).await;
                }
            }
            ControlMessage::ForceUpdatePosition { amt, entry } => {
                if amt != self.position.position_amt {
                    if amt == Decimal::ZERO {
                        self.position.position_amt = Decimal::ZERO;
                        info!("🔄 [{}] 发现仓位归零 (可能被强平/手动平仓)，已自愈更新。", self.position.symbol);
                    } else {
                        self.position.position_amt = amt;
                        self.position.entry_price = entry;
                        // Reset highest/lowest to current price to restart trailing SL safely
                        let current_price = {
                            let ob = self.ob_manager.book.read().unwrap();
                            ob.bids.iter().next_back().map(|(p, _)| *p).unwrap_or(entry)
                        };
                        self.position.highest_price_since_entry = if self.position.highest_price_since_entry < current_price { current_price } else { self.position.highest_price_since_entry };
                        self.position.lowest_price_since_entry = if self.position.lowest_price_since_entry > current_price { current_price } else { self.position.lowest_price_since_entry };
                        info!("🔄 [{}] 发现仓位变化 (部分成交或手工修改)，已自愈更新。量: {}, 均价: {}", self.position.symbol, amt, entry);
                    }
                    self.position.save_state(&self.redis_client).await;
                }
            }
        }
    }

    pub async fn handle_trade(&mut self, trade: AggTradeUpdate) {
        use std::str::FromStr;
        let qty = rust_decimal::Decimal::from_str(&trade.qty).unwrap_or_default();
        if trade.is_buyer_maker {
            self.taker_sell_flow += qty;
        } else {
            self.taker_buy_flow += qty;
        }
    }

    pub async fn evaluate_market(&mut self) {
        let (best_bid, best_ask, obi) = {
            let book = self.ob_manager.book.read().unwrap();
            let bid = book.bids.iter().next_back().map(|(p, _q)| *p);
            let ask = book.asks.iter().next().map(|(p, _q)| *p);

            let mut total_bid_vol = Decimal::ZERO;
            let mut total_ask_vol = Decimal::ZERO;
            let mut depth_weight = dec!(1.0);
            let decay_factor = dec!(0.8); 

            for (_p, q) in book.bids.iter().rev().take(10) { 
                total_bid_vol += q * depth_weight; 
                depth_weight *= decay_factor;
            }
            depth_weight = dec!(1.0);
            for (_p, q) in book.asks.iter().take(10) { 
                total_ask_vol += q * depth_weight; 
                depth_weight *= decay_factor;
            }
            
            let obi = if total_bid_vol + total_ask_vol > Decimal::ZERO {
                (total_bid_vol - total_ask_vol) / (total_bid_vol + total_ask_vol)
            } else {
                Decimal::ZERO
            };

            (bid, ask, obi)
        };

        if let (Some(bid), Some(ask)) = (best_bid, best_ask) {
            let mid_price = (bid + ask) / dec!(2);
            
            // Time-based exponential decay (half-life = 1 second)
            let dt_secs = self.last_tick_time.map(|t| t.elapsed().as_secs_f64()).unwrap_or(0.0);
            self.last_tick_time = Some(std::time::Instant::now());
            if dt_secs > 0.0 {
                let decay_factor = (-dt_secs / 1.442695).exp(); // e^(-dt / (t_half / ln2))
                if let Some(decay_dec) = rust_decimal::Decimal::from_f64(decay_factor) {
                    self.taker_buy_flow *= decay_dec;
                    self.taker_sell_flow *= decay_dec;
                }
            }

            if self.mid_price_history.len() < self.breakout_window {
                self.mid_price_history.push_back(mid_price);
                return;
            }

            let mut local_high = dec!(0);
            let mut local_low = dec!(999999999);
            for &p in &self.mid_price_history {
                if p > local_high { local_high = p; }
                if p < local_low { local_low = p; }
            }

            // 更新历史窗口
            self.mid_price_history.pop_front();
            self.mid_price_history.push_back(mid_price);

            self.tick_counter += 1;
            if self.tick_counter % 100 == 0 { // 约每10秒打印一次
                info!("🔍 [切片追踪 {}] 最新价: {} | 订单簿失衡指数(OBI): {:.3} | 买方动能流: {:.2} | 卖方动能流: {:.2} | 30s局部高/低点: {} / {}",
                      self.position.symbol, bid, obi, self.taker_buy_flow, self.taker_sell_flow, local_high, local_low);
            }

            let mut state_changed = false;

            if self.position.position_amt.is_zero() {
                // ==========================================
                // 全自动开仓 (实弹发射) 与 机器学习预测过滤
                // ==========================================
                if let Some(t) = self.last_signal_time {
                    if t.elapsed() < std::time::Duration::from_secs(10) { return; }
                }

                // 异步获取资金费率 (带 5 分钟缓存)
                let should_fetch = self.last_funding_fetch.map(|t| t.elapsed() > std::time::Duration::from_secs(300)).unwrap_or(true);
                if should_fetch {
                    if let Ok(rate) = self.exec_client.fetch_funding_rate(&self.position.symbol).await {
                        self.current_funding_rate = rate;
                        self.last_funding_fetch = Some(std::time::Instant::now());
                    }
                }

                if bid > local_high {
                    let is_strong = obi > dec!(0.3) && self.taker_buy_flow > self.taker_sell_flow * dec!(3.0);
                    let strength = if is_strong { "S" } else { "A" };
                    
                    let ml_prob = crate::ml_engine::MLEngine::predict_win_rate(obi, self.taker_buy_flow, self.taker_sell_flow, self.current_funding_rate, "BUY", &self.mid_price_history);
                    if ml_prob > dec!(0.6) {
                        info!("🚀 [{}] 触发做多信号！级别: {} | AI 胜率预测: {}%", self.position.symbol, strength, (ml_prob * dec!(100)).round_dp(1));
                        
                        let _ = self.signal_tx.send(crate::portfolio::SignalEvent {
                            symbol: self.position.symbol.clone(),
                            side: "BUY".to_string(),
                            strength: strength.to_string(),
                            price: ask,
                        }).await;
                        self.last_signal_time = Some(std::time::Instant::now());
                    }
                } else if ask < local_low {
                    let is_strong = obi < dec!(-0.3) && self.taker_sell_flow > self.taker_buy_flow * dec!(3.0);
                    let strength = if is_strong { "S" } else { "A" };
                    
                    let ml_prob = crate::ml_engine::MLEngine::predict_win_rate(obi, self.taker_buy_flow, self.taker_sell_flow, self.current_funding_rate, "SELL", &self.mid_price_history);
                    if ml_prob > dec!(0.6) {
                        info!("💥 [{}] 触发做空信号！级别: {} | AI 胜率预测: {}%", self.position.symbol, strength, (ml_prob * dec!(100)).round_dp(1));

                        let _ = self.signal_tx.send(crate::portfolio::SignalEvent {
                            symbol: self.position.symbol.clone(),
                            side: "SELL".to_string(),
                            strength: strength.to_string(),
                            price: bid,
                        }).await;
                        self.last_signal_time = Some(std::time::Instant::now());
                    }
                }
            } else {
                // ==========================================
                // 全自动平仓 (追踪止损实弹平仓)
                // ==========================================
                if self.position.position_amt > Decimal::ZERO { 
                    if bid > self.position.highest_price_since_entry {
                        self.position.highest_price_since_entry = bid;
                        state_changed = true;
                    }
                    let dynamic_sl_price = self.position.highest_price_since_entry * (dec!(1) - self.trailing_sl_pct / dec!(100));

                    if bid <= dynamic_sl_price {
                        let gross_pnl_pct = (bid - self.position.entry_price) / self.position.entry_price * dec!(100);
                        let net_pnl_pct = gross_pnl_pct - self.round_trip_fee_pct;
                        
                        info!("🏁 [{}] 多单离场信号！正在向交易所发送市价卖出（平多）指令...", self.position.symbol);
                        
                        let qty_str = self.position.position_amt.abs().normalize().to_string();
                        match self.exec_client.place_order(&self.position.symbol, "SELL", "MARKET", &qty_str, true).await {
                            Ok(_) => {
                                info!("✅ [{}] 成功平多！最终净盈亏: {}%", self.position.symbol, net_pnl_pct.round_dp(3));
                                let emoji = if net_pnl_pct > Decimal::ZERO { "🏆" } else { "🔪" };
                                let _ = self.tg_tx.send(format!("{} 移动止损平多战报\n\n交易对: {}\n离场均价: {}\n📈 毛盈亏: {}%\n💸 手续费: -{}%\n💰 净盈亏: {}%", emoji, self.position.symbol, bid, gross_pnl_pct.round_dp(3), self.round_trip_fee_pct.round_dp(3), net_pnl_pct.round_dp(3))).await;
                                self.position.position_amt = Decimal::ZERO;
                                state_changed = true;
                            }
                            Err(e) => {
                                error!("❌ [{}] API平仓失败: {}", self.position.symbol, e);
                                let _ = self.tg_tx.send(format!("⚠️ <b>紧急警报：平多单 API 被拒！</b>\n交易对: {}\n请立即打开币安 APP 手动平仓！\n原因: {}", self.position.symbol, e)).await;
                            }
                        }
                    }
                } else if self.position.position_amt < Decimal::ZERO { 
                    if ask < self.position.lowest_price_since_entry {
                        self.position.lowest_price_since_entry = ask;
                        state_changed = true;
                    }
                    let dynamic_sl_price = self.position.lowest_price_since_entry * (dec!(1) + self.trailing_sl_pct / dec!(100));

                    if ask >= dynamic_sl_price {
                        let gross_pnl_pct = (self.position.entry_price - ask) / self.position.entry_price * dec!(100);
                        let net_pnl_pct = gross_pnl_pct - self.round_trip_fee_pct;

                        info!("🏁 [{}] 空单离场信号！正在向交易所发送市价买入（平空）指令...", self.position.symbol);
                        
                        let qty_str = self.position.position_amt.abs().normalize().to_string();
                        match self.exec_client.place_order(&self.position.symbol, "BUY", "MARKET", &qty_str, true).await {
                            Ok(_) => {
                                info!("✅ [{}] 成功平空！最终净盈亏: {}%", self.position.symbol, net_pnl_pct.round_dp(3));
                                let emoji = if net_pnl_pct > Decimal::ZERO { "🏆" } else { "🔪" };
                                let _ = self.tg_tx.send(format!("{} 移动止损平空战报\n\n交易对: {}\n离场均价: {}\n📈 毛盈亏: {}%\n💸 手续费: -{}%\n💰 净盈亏: {}%", emoji, self.position.symbol, ask, gross_pnl_pct.round_dp(3), self.round_trip_fee_pct.round_dp(3), net_pnl_pct.round_dp(3))).await;
                                self.position.position_amt = Decimal::ZERO;
                                state_changed = true;
                            }
                            Err(e) => {
                                error!("❌ [{}] API平仓失败: {}", self.position.symbol, e);
                                let _ = self.tg_tx.send(format!("⚠️ <b>紧急警报：平空单 API 被拒！</b>\n交易对: {}\n请立即打开币安 APP 手动平仓！\n原因: {}", self.position.symbol, e)).await;
                            }
                        }
                    }
                }
            }

            if state_changed {
                self.position.save_state(&self.redis_client).await;
            }
        }
    }
}
