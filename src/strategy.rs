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
    
    pub initial_sl_pct: Decimal,
    pub activation_pct: Decimal,
    pub trailing_sl_pct: Decimal,
    pub round_trip_fee_pct: Decimal,

    pub fast_buy_flow: Decimal,
    pub fast_sell_flow: Decimal,
    pub slow_buy_flow: Decimal,
    pub slow_sell_flow: Decimal,

    // 全自动实盘扣款参数 (测试网可以随便填)
    #[allow(dead_code)]
    pub auto_margin_usdt: Decimal, 
    #[allow(dead_code)]
    pub auto_leverage: u32,
    
    // Telegram 推送通道
    pub tg_tx: mpsc::Sender<String>,
    pub feature_tx: mpsc::Sender<String>,
    
    // 日志计数器
    pub tick_counter: u64,
    
    pub signal_tx: mpsc::Sender<crate::portfolio::SignalEvent>,
    pub last_signal_time: Option<std::time::Instant>,
    pub current_funding_rate: Decimal,
    pub last_funding_fetch: Option<std::time::Instant>,
    pub last_tick_time: Option<std::time::Instant>,
    pub last_sl_error_time: Option<std::time::Instant>,
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
        feature_tx: mpsc::Sender<String>,
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
            mid_price_history: VecDeque::with_capacity(3000),
            breakout_window: 3000,
            initial_sl_pct: dec!(2.5),
            activation_pct: dec!(3.0),
            trailing_sl_pct: dec!(1.5),
            round_trip_fee_pct: dec!(0.1),
            fast_buy_flow: Decimal::ZERO,
            fast_sell_flow: Decimal::ZERO,
            slow_buy_flow: Decimal::ZERO,
            slow_sell_flow: Decimal::ZERO,
            auto_margin_usdt: dec!(50.0),
            auto_leverage: 10,
            tg_tx,
            feature_tx,
            tick_counter: 0,
            signal_tx,
            last_signal_time: None,
            current_funding_rate: Decimal::ZERO,
            last_funding_fetch: None,
            last_tick_time: None,
            last_sl_error_time: None,
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
        let price = rust_decimal::Decimal::from_str(&trade.price).unwrap_or_default();
        let notional = qty * price;
        
        if trade.is_buyer_maker {
            self.fast_sell_flow += notional;
            self.slow_sell_flow += notional;
        } else {
            self.fast_buy_flow += notional;
            self.slow_buy_flow += notional;
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
            
            // Time-based exponential decay (half-life = 1s and 30s)
            let now = std::time::Instant::now();
            if let Some(last_time) = self.last_tick_time {
                let dt = now.duration_since(last_time).as_secs_f64();
                if dt > 0.0 {
                    let fast_decay = Decimal::from_f64((0.5_f64).powf(dt / 1.0)).unwrap();
                    let slow_decay = Decimal::from_f64((0.5_f64).powf(dt / 30.0)).unwrap();
                    self.fast_buy_flow *= fast_decay;
                    self.fast_sell_flow *= fast_decay;
                    self.slow_buy_flow *= slow_decay;
                    self.slow_sell_flow *= slow_decay;
                }
            }
            self.last_tick_time = Some(now);

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
                info!("🔍 [切片追踪 {}] 最新价: {} | 订单簿失衡指数(OBI): {:.3} | 快买/卖流: {:.2}/{:.2} | 慢买/卖流: {:.2}/{:.2} | 30s局部高/低点: {} / {}",
                      self.position.symbol, bid, obi, self.fast_buy_flow, self.fast_sell_flow, self.slow_buy_flow, self.slow_sell_flow, local_high, local_low);
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
                    let min_flow_threshold = dec!(15000.0); // 必须有绝对的资金体量爆发 (1.5万U/秒)
                    let is_strong_fast = obi > dec!(0.3) && self.fast_buy_flow > self.fast_sell_flow * dec!(3.0) && self.fast_buy_flow > min_flow_threshold;
                    let is_strong_slow = self.slow_buy_flow > self.slow_sell_flow * dec!(1.5) && self.slow_buy_flow > min_flow_threshold * dec!(2.0);
                    let is_strong = is_strong_fast && is_strong_slow;
                    
                    let strength = if is_strong { "S" } else if is_strong_fast { "A" } else { "B" };
                    let ml_prob = crate::ml_engine::MLEngine::predict_win_rate(obi, self.fast_buy_flow, self.fast_sell_flow, self.current_funding_rate, "BUY", &self.mid_price_history);
                    
                    // 异步特征采集流水线 (零延迟)
                    let timestamp = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64;
                    let feature_json = format!(r#"{{"sym":"{}","ts":{},"p":{:.4},"obi":{:.4},"buy_f":{:.4},"sell_f":{:.4},"fund":{:.4},"prob":{:.4}}}"#,
                        self.position.symbol, timestamp, bid, obi, self.fast_buy_flow, self.fast_sell_flow, self.current_funding_rate, ml_prob);
                    let _ = self.feature_tx.try_send(feature_json);

                    if is_strong || ml_prob > dec!(0.75) {
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
                    let min_flow_threshold = dec!(15000.0);
                    let is_strong_fast = obi < dec!(-0.3) && self.fast_sell_flow > self.fast_buy_flow * dec!(3.0) && self.fast_sell_flow > min_flow_threshold;
                    let is_strong_slow = self.slow_sell_flow > self.slow_buy_flow * dec!(1.5) && self.slow_sell_flow > min_flow_threshold * dec!(2.0);
                    let is_strong = is_strong_fast && is_strong_slow;
                    
                    let strength = if is_strong { "S" } else if is_strong_fast { "A" } else { "B" };
                    let ml_prob = crate::ml_engine::MLEngine::predict_win_rate(obi, self.fast_buy_flow, self.fast_sell_flow, self.current_funding_rate, "SELL", &self.mid_price_history);

                    let timestamp = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64;
                    let feature_json = format!(r#"{{"sym":"{}","ts":{},"p":{:.4},"obi":{:.4},"buy_f":{:.4},"sell_f":{:.4},"fund":{:.4},"prob":{:.4}}}"#,
                        self.position.symbol, timestamp, ask, obi, self.fast_buy_flow, self.fast_sell_flow, self.current_funding_rate, ml_prob);
                    let _ = self.feature_tx.try_send(feature_json);

                    if is_strong || ml_prob > dec!(0.75) {
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
                    let current_profit_pct = (self.position.highest_price_since_entry - self.position.entry_price) / self.position.entry_price * dec!(100);
                    let dynamic_sl_price = if current_profit_pct >= self.activation_pct {
                        self.position.highest_price_since_entry * (dec!(1) - self.trailing_sl_pct / dec!(100))
                    } else {
                        self.position.entry_price * (dec!(1) - self.initial_sl_pct / dec!(100))
                    };

                    if current_profit_pct < self.activation_pct {
                        // 已移除假墙 (Spoofing) 止损机制，避免被做市商的虚假厚盘口骗出局。
                        // 统一依赖入场均价的 initial_sl_pct 进行硬止损。
                    }

                    if bid <= dynamic_sl_price {
                        if let Some(t) = self.last_sl_error_time {
                            if t.elapsed().as_secs() < 5 { return; }
                        }
                        let gross_pnl_pct = (bid - self.position.entry_price) / self.position.entry_price * dec!(100);
                        let net_pnl_pct = gross_pnl_pct - self.round_trip_fee_pct;
                        let gross_pnl_usdt = (bid - self.position.entry_price) * self.position.position_amt;
                        let fee_usdt = (self.position.position_amt.abs() * self.position.entry_price + self.position.position_amt.abs() * bid) * dec!(0.0005);
                        let net_pnl_usdt = gross_pnl_usdt - fee_usdt;
                        self.record_trade_history("LONG", bid, gross_pnl_usdt, fee_usdt, net_pnl_usdt).await;
                        
                        info!("🏁 [{}] 多单离场信号！正在向交易所发送市价卖出（平多）指令...", self.position.symbol);
                        
                        let qty_str = self.position.position_amt.abs().normalize().to_string();
                        match self.exec_client.place_order(&self.position.symbol, "SELL", "MARKET", &qty_str, true).await {
                            Ok(_) => {
                                info!("✅ [{}] 成功平多！最终净盈亏: {}%", self.position.symbol, net_pnl_pct.round_dp(3));
                                let emoji = if net_pnl_pct > Decimal::ZERO { "🏆" } else { "🔪" };
                                let _ = self.tg_tx.send(format!("{} 移动止损平多战报\n\n交易对: {}\n离场均价: {}\n📈 毛盈亏: {}% ({} U)\n💸 手续费: -{}% ({} U)\n💰 净盈亏: {}% ({} U)", emoji, self.position.symbol, bid, gross_pnl_pct.round_dp(3), gross_pnl_usdt.round_dp(3), self.round_trip_fee_pct.round_dp(3), fee_usdt.round_dp(3), net_pnl_pct.round_dp(3), net_pnl_usdt.round_dp(3))).await;
                                self.position.position_amt = Decimal::ZERO;
                                state_changed = true;
                            }
                            Err(e) => {
                                if self.last_sl_error_time.map_or(true, |t| t.elapsed().as_secs() >= 5) {
                                    error!("❌ [{}] API平仓失败: {}", self.position.symbol, e);
                                    let _ = self.tg_tx.send(format!("⚠️ <b>紧急警报：平多单 API 被拒！</b>\n交易对: {}\n请立即打开币安 APP 手动平仓！\n原因: {}\n系统将在 5 秒后重试...", self.position.symbol, e)).await;
                                    self.last_sl_error_time = Some(std::time::Instant::now());
                                }
                            }
                        }
                    }
                } else if self.position.position_amt < Decimal::ZERO { 
                    if ask < self.position.lowest_price_since_entry {
                        self.position.lowest_price_since_entry = ask;
                        state_changed = true;
                    }
                    let current_profit_pct = (self.position.entry_price - self.position.lowest_price_since_entry) / self.position.entry_price * dec!(100);
                    let dynamic_sl_price = if current_profit_pct >= self.activation_pct {
                        self.position.lowest_price_since_entry * (dec!(1) + self.trailing_sl_pct / dec!(100))
                    } else {
                        self.position.entry_price * (dec!(1) + self.initial_sl_pct / dec!(100))
                    };

                    if current_profit_pct < self.activation_pct {
                        // 已移除假墙 (Spoofing) 止损机制，避免被做市商的虚假厚盘口骗出局。
                    }

                    if ask >= dynamic_sl_price {
                        if let Some(t) = self.last_sl_error_time {
                            if t.elapsed().as_secs() < 5 { return; }
                        }
                        let gross_pnl_pct = (self.position.entry_price - ask) / self.position.entry_price * dec!(100);
                        let net_pnl_pct = gross_pnl_pct - self.round_trip_fee_pct;

                        let gross_pnl_usdt = (self.position.entry_price - ask) * self.position.position_amt.abs();
                        let fee_usdt = (self.position.position_amt.abs() * self.position.entry_price + self.position.position_amt.abs() * ask) * dec!(0.0005);
                        let net_pnl_usdt = gross_pnl_usdt - fee_usdt;
                        self.record_trade_history("SHORT", ask, gross_pnl_usdt, fee_usdt, net_pnl_usdt).await;
                        
                        info!("🏁 [{}] 空单离场信号！正在向交易所发送市价买入（平空）指令...", self.position.symbol);
                        
                        let qty_str = self.position.position_amt.abs().normalize().to_string();
                        match self.exec_client.place_order(&self.position.symbol, "BUY", "MARKET", &qty_str, true).await {
                            Ok(_) => {
                                info!("✅ [{}] 成功平空！最终净盈亏: {}%", self.position.symbol, net_pnl_pct.round_dp(3));
                                let emoji = if net_pnl_pct > Decimal::ZERO { "🏆" } else { "🔪" };
                                let _ = self.tg_tx.send(format!("{} 移动止损平空战报\n\n交易对: {}\n离场均价: {}\n📈 毛盈亏: {}% ({} U)\n💸 手续费: -{}% ({} U)\n💰 净盈亏: {}% ({} U)", emoji, self.position.symbol, ask, gross_pnl_pct.round_dp(3), gross_pnl_usdt.round_dp(3), self.round_trip_fee_pct.round_dp(3), fee_usdt.round_dp(3), net_pnl_pct.round_dp(3), net_pnl_usdt.round_dp(3))).await;
                                self.position.position_amt = Decimal::ZERO;
                                state_changed = true;
                            }
                            Err(e) => {
                                if self.last_sl_error_time.map_or(true, |t| t.elapsed().as_secs() >= 5) {
                                    error!("❌ [{}] API平仓失败: {}", self.position.symbol, e);
                                    let _ = self.tg_tx.send(format!("⚠️ <b>紧急警报：平空单 API 被拒！</b>\n交易对: {}\n请立即打开币安 APP 手动平仓！\n原因: {}\n系统将在 5 秒后重试...", self.position.symbol, e)).await;
                                    self.last_sl_error_time = Some(std::time::Instant::now());
                                }
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

    async fn record_trade_history(&self, side: &str, exit_price: Decimal, gross_pnl: Decimal, fee: Decimal, net_pnl: Decimal) {
        let now = time::OffsetDateTime::now_utc();
        let date_str = format!("{:04}-{:02}-{:02}", now.year(), now.month() as u8, now.day());
        let key = format!("trade_history_{}", date_str);
        
        let trade = serde_json::json!({
            "symbol": self.position.symbol,
            "side": side,
            "entry_price": self.position.entry_price.to_string(),
            "exit_price": exit_price.to_string(),
            "gross_pnl_usdt": gross_pnl.to_string(),
            "fee_usdt": fee.to_string(),
            "net_pnl_usdt": net_pnl.to_string(),
            "timestamp": now.unix_timestamp(),
        });
        
        if let Ok(mut con) = self.redis_client.get_multiplexed_async_connection().await {
            let _: redis::RedisResult<()> = redis::cmd("RPUSH").arg(&key).arg(trade.to_string()).query_async(&mut con).await;
            let _: redis::RedisResult<()> = redis::cmd("EXPIRE").arg(&key).arg(604800).query_async(&mut con).await; // 7 days
        }
    }
}
