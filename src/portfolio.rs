use crate::execution::BinanceExecutionClient;
use crate::strategy::ControlMessage;
use rust_decimal::Decimal;
use rust_decimal::prelude::FromStr;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{info, error};

#[derive(Debug)]
pub struct SignalEvent {
    pub symbol: String,
    pub side: String, // "BUY" or "SELL"
    pub strength: String, // "S" or "A"
    pub price: Decimal,
}

pub struct PortfolioManager {
    exec_client: Arc<BinanceExecutionClient>,
    control_senders: HashMap<String, mpsc::Sender<ControlMessage>>,
    signal_rx: mpsc::Receiver<SignalEvent>,
    tg_tx: mpsc::Sender<String>,
    exchange_info: Arc<HashMap<String, crate::execution::SymbolInfo>>,
    active_symbols: std::collections::HashSet<String>,
    redis_client: redis::Client,
    last_new_position_time: Option<std::time::Instant>,
}

impl PortfolioManager {
    pub fn new(
        exec_client: Arc<BinanceExecutionClient>,
        control_senders: HashMap<String, mpsc::Sender<ControlMessage>>,
        signal_rx: mpsc::Receiver<SignalEvent>,
        tg_tx: mpsc::Sender<String>,
        exchange_info: Arc<HashMap<String, crate::execution::SymbolInfo>>,
        redis_client: redis::Client,
    ) -> Self {
        Self { exec_client, control_senders, signal_rx, tg_tx, exchange_info, active_symbols: std::collections::HashSet::new(), redis_client, last_new_position_time: None }
    }

    pub async fn run(mut self) {
        while let Some(signal) = self.signal_rx.recv().await {
            self.handle_signal(signal).await;
        }
    }

    async fn handle_signal(&mut self, signal: SignalEvent) {
        info!("🧠 [中央大脑] 收到信号: {} 方向: {} 强度: {}", signal.symbol, signal.side, signal.strength);
        
        let account_str = match self.exec_client.check_account().await {
            Ok(s) => s,
            Err(e) => { error!("获取账户失败: {}", e); return; }
        };
        
        let account: serde_json::Value = match serde_json::from_str(&account_str) {
            Ok(v) => v,
            Err(_) => return,
        };

        let total_wallet = account.get("totalWalletBalance").and_then(|v| v.as_str()).and_then(|s| Decimal::from_str(s).ok()).unwrap_or(Decimal::ZERO);
        let avail_balance = account.get("availableBalance").and_then(|v| v.as_str()).and_then(|s| Decimal::from_str(s).ok()).unwrap_or(Decimal::ZERO);

        if total_wallet <= Decimal::ZERO { return; }

        let pos_str = self.exec_client.check_positions().await.unwrap_or_default();
        let positions: Vec<serde_json::Value> = serde_json::from_str(&pos_str).unwrap_or_default();
        
        self.active_symbols.clear();
        for pos in &positions {
            let amt = pos.get("positionAmt").and_then(|v| v.as_str()).and_then(|s| rust_decimal::Decimal::from_str(s).ok()).unwrap_or(rust_decimal::Decimal::ZERO);
            if amt.abs() > rust_decimal::Decimal::ZERO {
                let sym = pos.get("symbol").and_then(|v| v.as_str()).unwrap_or("");
                self.active_symbols.insert(sym.to_string());
            }
        }

        if self.active_symbols.contains(&signal.symbol) {
            if signal.strength != "DCA" {
                info!("⚠️ [中央大脑] {} 已经持有仓位，拒绝重复开仓。", signal.symbol);
                return;
            } else {
                info!("🛡️ [中央大脑] 允许 {} 执行马丁格尔阶梯补仓！", signal.symbol);
            }
        } else {
            // 如果是全新开仓，检查高并发锁
            if let Some(t) = self.last_new_position_time {
                if t.elapsed() < std::time::Duration::from_secs(2) {
                    info!("⏳ [中央大脑] 正在等待上一个订单的交易所状态同步，拒绝高并发秒开新币种: {}", signal.symbol);
                    return;
                }
            }
        }

        let mut max_positions: usize = 1; // 默认上限改为 1 (最保守策略)
        match self.redis_client.get_multiplexed_async_connection().await {
            Ok(mut con) => {
                match redis::cmd("GET").arg("MAX_CONCURRENT_POSITIONS").query_async::<Option<String>>(&mut con).await {
                    Ok(Some(val)) => {
                        if let Ok(parsed) = val.parse::<usize>() {
                            max_positions = parsed;
                        } else {
                            error!("Redis 中 MAX_CONCURRENT_POSITIONS 值不是有效数字: {}", val);
                        }
                    }
                    Ok(None) => {
                        // Key 不存在
                    }
                    Err(e) => {
                        error!("查询 Redis MAX_CONCURRENT_POSITIONS 失败: {}", e);
                    }
                }
            }
            Err(e) => {
                error!("无法获取 Redis 异步连接: {}", e);
            }
        }

        if self.active_symbols.len() >= max_positions && !self.active_symbols.contains(&signal.symbol) {
            info!("⚠️ [中央大脑] 当前已持有 {} 个仓位，达到系统并发上限 ({})，拒绝新信号 {}。", self.active_symbols.len(), max_positions, signal.symbol);
            return;
        }

        let leverage = 10;
        let target_margin = total_wallet * rust_decimal_macros::dec!(0.30);
        
        let usable_margin = if avail_balance >= target_margin {
            target_margin
        } else {
            avail_balance * rust_decimal_macros::dec!(0.95)
        };
        
        if usable_margin < rust_decimal_macros::dec!(5.0) {
            info!("⚠️ [中央大脑] 可用资金不足(剩余: {})，放弃交易。", avail_balance);
            return;
        }

        let notional = usable_margin * Decimal::from(leverage);
        
        if !self.active_symbols.contains(&signal.symbol) {
            self.active_symbols.insert(signal.symbol.clone()); // 乐观锁占位，防止网络延迟造成高并发突破持仓上限
            self.last_new_position_time = Some(std::time::Instant::now());
        }
        self.execute_trade(&signal.symbol, &signal.side, notional, signal.price).await;
    }

    async fn execute_trade(&self, symbol: &str, side: &str, notional: Decimal, est_price: Decimal) {
        let mut target_qty = notional / est_price;
        if let Some(info) = self.exchange_info.get(symbol) {
            let step_size = info.step_size;
            if step_size > rust_decimal::Decimal::ZERO {
                let steps = (target_qty / step_size).floor();
                target_qty = steps * step_size;
            }
        }

        let qty_str = target_qty.normalize().to_string();
        let _ = self.exec_client.set_leverage(symbol, 10).await;
        let _ = self.exec_client.set_margin_type(symbol, "ISOLATED").await;
        match self.exec_client.place_order(symbol, side, "MARKET", &qty_str, false).await {
            Ok(fill) => {
                let actual_entry = if fill > Decimal::ZERO { fill } else { est_price };
                if let Some(tx) = self.control_senders.get(symbol) {
                    let trade_qty = if side == "BUY" { target_qty } else { -target_qty };
                    let _ = tx.send(ControlMessage::TradeExecuted { trade_qty, fill_price: actual_entry }).await;
                }
                let actual_notional = target_qty * actual_entry;
                let _ = self.tg_tx.send(format!("🤖 <b>中央大脑联合执行</b>\n✅ 交易对: {}\n🎯 方向: {}\n💰 真实均价: {}\n📦 下单量: {}\n💵 交易价值: {:.2} USDT", symbol, side, actual_entry.round_dp(6).normalize(), target_qty, actual_notional)).await;
            }
            Err(e) => {
                error!("中央大脑执行 {} 失败: {}", symbol, e);
                let _ = self.tg_tx.send(format!("❌ <b>中央大脑自动下单失败 ({})</b>\n原因: {}", symbol, e)).await;
            }
        }
    }
}
