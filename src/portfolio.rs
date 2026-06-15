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
}

impl PortfolioManager {
    pub fn new(
        exec_client: Arc<BinanceExecutionClient>,
        control_senders: HashMap<String, mpsc::Sender<ControlMessage>>,
        signal_rx: mpsc::Receiver<SignalEvent>,
        tg_tx: mpsc::Sender<String>,
    ) -> Self {
        Self { exec_client, control_senders, signal_rx, tg_tx }
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

        // 字节计算：每次调用总本金的 10%
        let target_margin = total_wallet * rust_decimal_macros::dec!(0.10);
        let leverage = 10;
        let notional = target_margin * Decimal::from(leverage);
        
        if avail_balance >= target_margin {
            // 资金充裕，直接开仓
            self.execute_trade(&signal.symbol, &signal.side, notional, signal.price).await;
        } else {
            // 资金不足
            if signal.strength == "S" {
                info!("⚠️ [中央大脑] 资金不足以执行 S 级信号！启动跨币种强平调配...");
                self.free_up_margin_and_trade(signal, notional).await;
            } else {
                info!("⚠️ [中央大脑] 资金不足且信号仅为 A 级，放弃本次交易。");
            }
        }
    }

    async fn free_up_margin_and_trade(&mut self, signal: SignalEvent, notional: Decimal) {
        let pos_str = match self.exec_client.check_positions().await {
            Ok(s) => s,
            Err(_) => return,
        };
        let positions: Vec<serde_json::Value> = serde_json::from_str(&pos_str).unwrap_or_default();
        
        let mut largest_pos_sym = String::new();
        let mut largest_pos_amt = Decimal::ZERO;
        let mut largest_pos_side = String::new();

        for pos in positions {
            let sym = pos.get("symbol").and_then(|v| v.as_str()).unwrap_or("");
            if sym == signal.symbol { continue; } // 不要平自己
            
            let amt = pos.get("positionAmt").and_then(|v| v.as_str()).and_then(|s| Decimal::from_str(s).ok()).unwrap_or(Decimal::ZERO);
            if amt.abs() > largest_pos_amt.abs() {
                largest_pos_amt = amt;
                largest_pos_sym = sym.to_string();
                largest_pos_side = if amt > Decimal::ZERO { "SELL".to_string() } else { "BUY".to_string() }; // 反向平仓
            }
        }

        if largest_pos_amt.abs() > Decimal::ZERO {
            // 强平 30%
            let reduce_amt = largest_pos_amt.abs() * rust_decimal_macros::dec!(0.30);
            
            let precision = match largest_pos_sym.as_str() {
                "BTCUSDT" => 3, "ETHUSDT" => 3, "BNBUSDT" => 2, _ => 0,
            };
            let mut qty_to_reduce = reduce_amt;
            qty_to_reduce.rescale(precision);

            if qty_to_reduce > Decimal::ZERO {
                info!("🔪 [中央大脑] 正在强平 {} 的 30% 仓位 (量: {})，为 {} 腾出子弹！", largest_pos_sym, qty_to_reduce, signal.symbol);
                if let Ok(fill_price) = self.exec_client.place_order(&largest_pos_sym, &largest_pos_side, "MARKET", &qty_to_reduce.to_string(), true).await {
                    // 通知被强平的引擎
                    if let Some(tx) = self.control_senders.get(&largest_pos_sym) {
                        let trade_qty = if largest_pos_side == "BUY" { qty_to_reduce } else { -qty_to_reduce };
                        let _ = tx.send(ControlMessage::TradeExecuted { trade_qty, fill_price }).await;
                    }
                    let _ = self.tg_tx.send(format!("🔪 **中央大脑资金调配**\n为了执行 {} 的 S 级信号，已强行平掉 {} 的 30% 仓位腾出保证金！", signal.symbol, largest_pos_sym)).await;
                }
            }
        }

        // 腾出保证金后，尝试执行
        self.execute_trade(&signal.symbol, &signal.side, notional, signal.price).await;
    }

    async fn execute_trade(&self, symbol: &str, side: &str, notional: Decimal, est_price: Decimal) {
        let precision = match symbol {
            "BTCUSDT" => 3, "ETHUSDT" => 3, "BNBUSDT" => 2, _ => 0,
        };
        let mut target_qty = notional / est_price;
        target_qty.rescale(precision);

        let _ = self.exec_client.set_leverage(symbol, 10).await;
        match self.exec_client.place_order(symbol, side, "MARKET", &target_qty.to_string(), false).await {
            Ok(fill) => {
                let actual_entry = if fill > Decimal::ZERO { fill } else { est_price };
                if let Some(tx) = self.control_senders.get(symbol) {
                    let trade_qty = if side == "BUY" { target_qty } else { -target_qty };
                    let _ = tx.send(ControlMessage::TradeExecuted { trade_qty, fill_price: actual_entry }).await;
                }
                let _ = self.tg_tx.send(format!("🤖 **中央大脑联合执行**\n✅ 交易对: {}\n🎯 方向: {}\n💰 真实均价: {}\n📦 下单量: {}", symbol, side, actual_entry, target_qty)).await;
            }
            Err(e) => {
                error!("中央大脑执行 {} 失败: {}", symbol, e);
            }
        }
    }
}
