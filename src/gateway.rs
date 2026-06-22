use futures_util::StreamExt;
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{error, info};
use std::time::Duration;

use crate::models::{DepthUpdate, AggTradeUpdate};

pub async fn run_binance_ws(symbol: &str, tx: mpsc::Sender<DepthUpdate>) {
    let base_ws = std::env::var("BINANCE_WS_URL").unwrap_or_else(|_| "wss://fstream.binance.com/ws".to_string());
    let ws_url = format!("{}/{}@depth@100ms", base_ws, symbol.to_lowercase());
    let mut retry_delay = 1;

    loop {
        info!("🔗 [{}] 尝试连接到币安 Depth WebSocket...", symbol);
        match connect_async(&ws_url).await {
            Ok((mut ws_stream, _)) => {
                retry_delay = 1;
                // 【修复：修改为 300 秒超时】币安通常每 3 分钟发送一次 Ping。冷门币种可能几分钟都没有盘口变化。
                while let Ok(Some(msg)) = tokio::time::timeout(Duration::from_secs(300), ws_stream.next()).await {
                    match msg {
                        Ok(Message::Text(text)) => {
                            match serde_json::from_str::<DepthUpdate>(&text) {
                                Ok(update) => {
                                    if tx.send(update).await.is_err() {
                                        return;
                                    }
                                }
                                Err(e) => {
                                    tracing::error!("Depth Parser Error: {} | Data: {}", e, text.chars().take(200).collect::<String>());
                                }
                            }
                        }
                        Ok(Message::Ping(_)) | Ok(Message::Pong(_)) => {}
                        Ok(Message::Close(_)) => break,
                        Err(_) => break,
                        _ => {}
                    }
                }
            }
            Err(e) => { error!("❌ [{}] Depth WS 连接失败: {}", symbol, e); }
        }
        tokio::time::sleep(Duration::from_secs(retry_delay)).await;
        retry_delay = std::cmp::min(retry_delay * 2, 60);
    }
}

pub async fn run_aggtrade_ws(symbol: &str, tx: mpsc::Sender<AggTradeUpdate>) {
    let base_ws = std::env::var("BINANCE_WS_URL").unwrap_or_else(|_| "wss://fstream.binance.com/ws".to_string());
    let ws_url = format!("{}/{}@aggTrade", base_ws, symbol.to_lowercase());
    let mut retry_delay = 1;

    loop {
        info!("🔗 [{}] 尝试连接到币安 AggTrade WebSocket...", symbol);
        match connect_async(&ws_url).await {
            Ok((mut ws_stream, _)) => {
                info!("✅ [{}] Trade 流接入成功，主动吃单嗅探器启动！", symbol);
                retry_delay = 1;
                while let Ok(Some(msg)) = tokio::time::timeout(Duration::from_secs(300), ws_stream.next()).await {
                    match msg {
                        Ok(Message::Text(text)) => {
                            match serde_json::from_str::<AggTradeUpdate>(&text) {
                                Ok(update) => {
                                    if tx.send(update).await.is_err() {
                                        return;
                                    }
                                }
                                Err(e) => {
                                    tracing::error!("Trade Parser Error: {} | Data: {}", e, text.chars().take(200).collect::<String>());
                                }
                            }
                        }
                        Ok(Message::Ping(_)) | Ok(Message::Pong(_)) => {}
                        Ok(Message::Close(_)) => break,
                        Err(_) => break,
                        _ => {}
                    }
                }
            }
            Err(e) => { error!("❌ [{}] Trade WS 连接失败: {}", symbol, e); }
        }
        tokio::time::sleep(Duration::from_secs(retry_delay)).await;
        retry_delay = std::cmp::min(retry_delay * 2, 60);
    }
}
