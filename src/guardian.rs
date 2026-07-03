// ==========================================
// MODULE: 仓位保镖 (Position Guardian)
// 每 10 秒扫描交易所全部真实仓位（含手动开的仓）:
//   1. 没有止损保护的仓位 → 自动挂交易所侧 STOP_MARKET 整仓止损单
//   2. 持仓超时 → Telegram 提醒 (可配置为自动市价平仓)
// 本模块只会平仓和挂平仓单，不存在任何开仓路径。
// ==========================================
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tokio::sync::mpsc;
use tracing::{info, error};

use crate::execution::{BinanceExecutionClient, SymbolInfo};

struct GuardState {
    // 我们自己挂的止损单 (用户手动挂的止损不归我们管，也绝不动它)
    own_stop_id: Option<u64>,
    entry_used: Decimal,
    last_hold_alert_min: u64,
    stop_error_notified: bool,
    auto_close_attempted: bool,
}

impl Default for GuardState {
    fn default() -> Self {
        Self { own_stop_id: None, entry_used: Decimal::ZERO, last_hold_alert_min: 0, stop_error_notified: false, auto_close_attempted: false }
    }
}

async fn read_cfg(con: &mut redis::aio::MultiplexedConnection, key: &str, default: &str) -> String {
    redis::cmd("GET").arg(key).query_async::<Option<String>>(con).await.ok().flatten().unwrap_or_else(|| default.to_string())
}

// 止损价对齐 tick_size；多单止损向下取整、空单止损向上取整，保证不会因精度被拒单
fn align_to_tick(px: Decimal, tick: Option<Decimal>, round_down: bool) -> Decimal {
    match tick {
        Some(t) if t > Decimal::ZERO => {
            let steps = px / t;
            let steps = if round_down { steps.floor() } else { steps.ceil() };
            steps * t
        }
        _ => px.round_dp(6),
    }
}

pub async fn run_guardian(
    exec: Arc<BinanceExecutionClient>,
    redis_client: redis::Client,
    tg_tx: mpsc::Sender<String>,
    exchange_info: Arc<HashMap<String, SymbolInfo>>,
) {
    info!("🛡 仓位保镖已上岗: 自动硬止损 + 持仓超时监控 (只平仓, 无开仓能力)");
    let mut states: HashMap<String, GuardState> = HashMap::new();

    loop {
        tokio::time::sleep(std::time::Duration::from_secs(10)).await;

        let mut con = match redis_client.get_multiplexed_async_connection().await {
            Ok(c) => c,
            Err(_) => continue,
        };

        if read_cfg(&mut con, "GUARD_ENABLED", "1").await != "1" {
            continue;
        }
        // 默认 5%: 这是"灾难保险线"而不是短线出场线。10x 逐仓的强平线在 -9.5% 左右,
        // 5% 卡在山寨币插针噪音区之外、强平线之内; 用 /guard stop <pct> 按需调整。
        let stop_pct = Decimal::from_str(&read_cfg(&mut con, "GUARD_STOP_PCT", "5.0").await).unwrap_or(dec!(5.0));
        let alert_min: u64 = read_cfg(&mut con, "GUARD_HOLD_ALERT_MIN", "30").await.parse().unwrap_or(30);
        let auto_close_min: u64 = read_cfg(&mut con, "GUARD_AUTO_CLOSE_MIN", "0").await.parse().unwrap_or(0);

        let pos_str = match exec.check_positions().await {
            Ok(s) => s,
            Err(e) => { error!("🛡 保镖拉取仓位失败: {}", e); continue; }
        };
        let positions: Vec<serde_json::Value> = serde_json::from_str(&pos_str).unwrap_or_default();

        // sym -> (数量, 开仓均价, 未实现盈亏)
        let mut live: HashMap<String, (Decimal, Decimal, Decimal)> = HashMap::new();
        for pos in &positions {
            let amt = pos.get("positionAmt").and_then(|v| v.as_str()).and_then(|s| Decimal::from_str(s).ok()).unwrap_or(Decimal::ZERO);
            if amt == Decimal::ZERO { continue; }
            let sym = pos.get("symbol").and_then(|v| v.as_str()).unwrap_or("").to_string();
            let entry = pos.get("entryPrice").and_then(|v| v.as_str()).and_then(|s| Decimal::from_str(s).ok()).unwrap_or(Decimal::ZERO);
            let upnl = pos.get("unRealizedProfit").and_then(|v| v.as_str()).and_then(|s| Decimal::from_str(s).ok()).unwrap_or(Decimal::ZERO);
            live.insert(sym, (amt, entry, upnl));
        }

        // ---------- 已平仓位的善后: 撤掉遗留的整仓止损单, 清除计时 ----------
        let tracked: Vec<String> = redis::cmd("KEYS").arg("GUARD_OPENED_*").query_async::<Vec<String>>(&mut con).await.unwrap_or_default();
        for key in tracked {
            let sym = key.trim_start_matches("GUARD_OPENED_").to_string();
            if live.contains_key(&sym) { continue; }
            if let Ok(orders_str) = exec.get_open_algo_orders(&sym).await {
                if let Ok(orders) = serde_json::from_str::<Vec<serde_json::Value>>(&orders_str) {
                    for o in orders {
                        let is_close_stop = o.get("closePosition").and_then(|v| v.as_bool()).unwrap_or(false)
                            && o.get("orderType").and_then(|v| v.as_str()).unwrap_or("") == "STOP_MARKET";
                        if is_close_stop {
                            if let Some(oid) = o.get("algoId").and_then(|v| v.as_u64()) {
                                let _ = exec.cancel_algo_order(oid).await;
                                info!("🛡 [{}] 仓位已平, 撤掉遗留止损挂单 #{}", sym, oid);
                            }
                        }
                    }
                }
            }
            let _: () = redis::cmd("DEL").arg(&key).query_async(&mut con).await.unwrap_or(());
            states.remove(&sym);
        }

        // ---------- 在场仓位巡逻 ----------
        for (sym, (amt, entry, upnl)) in live {
            if entry <= Decimal::ZERO { continue; }
            let now_ms = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64;
            let opened_key = format!("GUARD_OPENED_{}", sym);
            // SETNX: 只在首次发现时记录开仓时间, 引擎重启不清零
            let _: () = redis::cmd("SET").arg(&opened_key).arg(now_ms).arg("NX").query_async(&mut con).await.unwrap_or(());
            let opened_at: u64 = redis::cmd("GET").arg(&opened_key).query_async::<Option<String>>(&mut con).await.ok().flatten().and_then(|s| s.parse().ok()).unwrap_or(now_ms);
            let held_min = now_ms.saturating_sub(opened_at) / 60_000;

            let st = states.entry(sym.clone()).or_default();
            let is_long = amt > Decimal::ZERO;

            // ---------- 1. 确保交易所侧止损单存在 (条件单在 algo 接口, 不在 openOrders) ----------
            match exec.get_open_algo_orders(&sym).await {
                Ok(orders_str) => {
                    let orders: Vec<serde_json::Value> = serde_json::from_str(&orders_str).unwrap_or_default();
                    let existing_stop = orders.iter().find(|o| {
                        o.get("closePosition").and_then(|v| v.as_bool()).unwrap_or(false)
                            && o.get("orderType").and_then(|v| v.as_str()).unwrap_or("") == "STOP_MARKET"
                    });

                    let mut place_new = false;
                    match existing_stop {
                        Some(o) => {
                            let oid = o.get("algoId").and_then(|v| v.as_u64());
                            let is_ours = st.own_stop_id.is_some() && st.own_stop_id == oid;
                            // 只有我们自己挂的止损才跟随均价变化重挂 (比如加仓后均价漂移超 0.5%)
                            if is_ours && st.entry_used > Decimal::ZERO && ((entry - st.entry_used).abs() / entry) > dec!(0.005) {
                                if let Some(oid) = oid {
                                    if exec.cancel_algo_order(oid).await.is_ok() {
                                        st.own_stop_id = None;
                                        place_new = true;
                                    }
                                }
                            }
                        }
                        None => place_new = true,
                    }

                    if place_new {
                        let (side, raw_stop) = if is_long {
                            ("SELL", entry * (Decimal::ONE - stop_pct / dec!(100)))
                        } else {
                            ("BUY", entry * (Decimal::ONE + stop_pct / dec!(100)))
                        };
                        let tick = exchange_info.get(&sym).map(|i| i.tick_size);
                        let stop_px = align_to_tick(raw_stop, tick, is_long);
                        let stop_str = stop_px.normalize().to_string();
                        match exec.place_stop_market_close(&sym, side, &stop_str).await {
                            Ok(oid) => {
                                st.own_stop_id = Some(oid);
                                st.entry_used = entry;
                                st.stop_error_notified = false;
                                // 触发时的预计亏损 = 仓位数量 × |均价 - 止损价| (市价成交, 滑点另计)
                                let est_loss = (amt.abs() * (entry - stop_px).abs()).round_dp(2).normalize();
                                info!("🛡 [{}] 已挂交易所侧硬止损 @ {} (均价 {}, -{}%, 预计亏损 {}U)", sym, stop_str, entry.normalize(), stop_pct, est_loss);
                                let _ = tg_tx.send(format!("🛡 <b>【保镖已就位】</b>\n\n交易对: {}\n方向: {}\n开仓均价: {}\n硬止损已挂在交易所: <b>{}</b> (-{}%)\n触发预计亏损: <b>-{} U</b> (不含滑点)\n\n即使引擎断电, 这张止损单也会由币安执行, 强平不可能发生。", sym, if is_long { "🟢 多" } else { "🔴 空" }, entry.round_dp(6).normalize(), stop_str, stop_pct, est_loss)).await;
                            }
                            Err(e) => {
                                error!("🛡 [{}] 挂硬止损失败: {}", sym, e);
                                if !st.stop_error_notified {
                                    st.stop_error_notified = true;
                                    let _ = tg_tx.send(format!("⚠️ <b>【保镖告警】</b> {} 挂硬止损失败, 该仓位当前无保护!\n原因: {}", sym, e)).await;
                                }
                            }
                        }
                    }
                }
                Err(e) => { error!("🛡 [{}] 查询挂单失败: {}", sym, e); }
            }

            // ---------- 2. 持仓超时提醒 (首次达到 alert_min, 之后每 alert_min 重复) ----------
            if alert_min > 0 && held_min >= alert_min && (st.last_hold_alert_min == 0 || held_min >= st.last_hold_alert_min + alert_min) {
                st.last_hold_alert_min = held_min;
                let _ = tg_tx.send(format!("⏰ <b>【持仓超时提醒】</b>\n\n{} 已持仓 <b>{} 分钟</b>, 当前浮动盈亏: {} U\n\n📉 你的历史数据: 持仓超过 30 分钟的回合合计净亏 -207 U, 胜率从 80% 掉到 33%~70%。\n该走了。", sym, held_min, upnl.round_dp(2).normalize())).await;
            }

            // ---------- 3. 超时自动平仓 (默认关闭, /guard autoclose <分钟> 开启) ----------
            if auto_close_min > 0 && held_min >= auto_close_min && !st.auto_close_attempted {
                st.auto_close_attempted = true;
                let side = if is_long { "SELL" } else { "BUY" };
                let qty_str = amt.abs().normalize().to_string();
                match exec.place_order(&sym, side, "MARKET", &qty_str, true).await {
                    Ok(_) => {
                        info!("🛡 [{}] 持仓超 {} 分钟, 已自动市价平仓", sym, auto_close_min);
                        let _ = tg_tx.send(format!("✂️ <b>【超时自动平仓】</b>\n\n{} 持仓已达 {} 分钟上限, 保镖已市价平仓。\n平仓前浮动盈亏: {} U", sym, held_min, upnl.round_dp(2).normalize())).await;
                    }
                    Err(e) => {
                        st.auto_close_attempted = false; // 下一轮重试
                        error!("🛡 [{}] 超时自动平仓失败: {}", sym, e);
                        let _ = tg_tx.send(format!("❌ <b>【保镖告警】</b> {} 超时自动平仓失败: {}", sym, e)).await;
                    }
                }
            }
        }
    }
}
