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
}

impl PortfolioManager {
    pub fn new(
        exec_client: Arc<BinanceExecutionClient>,
        control_senders: HashMap<String, mpsc::Sender<ControlMessage>>,
        signal_rx: mpsc::Receiver<SignalEvent>,
        tg_tx: mpsc::Sender<String>,
        exchange_info: Arc<HashMap<String, crate::execution::SymbolInfo>>,
    ) -> Self {
        Self { exec_client, control_senders, signal_rx, tg_tx, exchange_info }
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
        
        let mut active_count = 0;
        let mut has_existing_pos = false;
        for pos in &positions {
            let amt = pos.get("positionAmt").and_then(|v| v.as_str()).and_then(|s| rust_decimal::Decimal::from_str(s).ok()).unwrap_or(rust_decimal::Decimal::ZERO);
            if amt.abs() > rust_decimal::Decimal::ZERO {
                active_count += 1;
                let sym = pos.get("symbol").and_then(|v| v.as_str()).unwrap_or("");
                if sym == signal.symbol {
                    has_existing_pos = true;
                }
            }
        }

        if has_existing_pos {
            info!("⚠️ [中央大脑] {} 已经持有仓位，拒绝重复开仓。", signal.symbol);
            return;
        }

        if active_count >= 3 {
            info!("⚠️ [中央大脑] 当前已持有 {} 个仓位，达到系统并发上限 (3)，拒绝新信号 {}。", active_count, signal.symbol);
            return;
        }

        let leverage = 10;
        let target_margin = total_wallet * rust_decimal_macros::dec!(0.10);
        
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
        match self.exec_client.place_order(symbol, side, "MARKET", &qty_str, false).await {
            Ok(fill) => {
                let actual_entry = if fill > Decimal::ZERO { fill } else { est_price };
                if let Some(tx) = self.control_senders.get(symbol) {
                    let trade_qty = if side == "BUY" { target_qty } else { -target_qty };
                    let _ = tx.send(ControlMessage::TradeExecuted { trade_qty, fill_price: actual_entry }).await;
                }
                let actual_notional = target_qty * actual_entry;
                let _ = self.tg_tx.send(format!("🤖 <b>中央大脑联合执行</b>\n✅ 交易对: {}\n🎯 方向: {}\n💰 真实均价: {}\n📦 下单量: {}\n💵 交易价值: {:.2} USDT", symbol, side, actual_entry, target_qty, actual_notional)).await;
            }
            Err(e) => {
                error!("中央大脑执行 {} 失败: {}", symbol, e);
                let _ = self.tg_tx.send(format!("❌ <b>中央大脑自动下单失败 ({})</b>\n原因: {}", symbol, e)).await;
            }
        }
    }
}
