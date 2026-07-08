// ==========================================
// MODULE: 仓位保镖 (Position Guardian)
// 每 10 秒扫描交易所全部真实仓位（含手动开的仓）:
//   1. 没有止损保护的仓位 → 自动挂交易所侧 STOP_MARKET 整仓止损单
//   2. 移动止盈: 浮盈过激活线后, 止损跟随最优标记价回撤向有利方向棘轮,
//      上不封顶, 高点回落自动落袋; 激活线 > 回撤保证激活后不可能盈转亏
//   3. 持仓超时 → Telegram 提醒 (可配置为自动市价平仓)
// 阈值一律按 ROE 配置 (= 币价% × 杠杆, 即币安 App 显示的收益率),
// 按各仓位实际杠杆折算币价距离 —— 固定币价阈值在高杠杆下会挂到强平线外失效。
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

// 状态持久化到 Redis (GUARD_STATE_<sym>): own_stop_id/peak/trail_armed 若只存内存,
// 引擎一重启就丢 —— 保镖会认不出交易所里自己挂的止损单, 按"用户手动单不碰"的
// 规矩放着不管, 移动止盈棘轮从此失灵 (实盘已因此发生过: 激活消息发了,
// 止损却一直钉在灾难线上, "保底出场价"成了空头支票)。
#[derive(serde::Serialize, serde::Deserialize, Clone)]
struct GuardState {
    // 我们自己挂的止损单 (用户手动挂的止损不归我们管，也绝不动它)
    own_stop_id: Option<u64>,
    entry_used: Decimal,
    // 我们自己止损单的当前触发价 (棘轮基准)
    stop_price: Decimal,
    // 持仓期最优标记价: 多单为最高价, 空单为最低价
    peak: Decimal,
    // 移动止盈是否已激活 (浮盈达到激活线后为 true, 之后止损只上移不下移)
    trail_armed: bool,
    // 已播报过的最高 ROE 里程碑 (只升不降, 防止反复提醒同一档)
    last_milestone_roe: i32,
    // 激活后首次真正搬动交易所止损单的确认是否已发 (SYN事故教训: 激活消息 ≠ 止损已搬)
    #[serde(default)]
    trail_confirm_sent: bool,
    // 棘轮卡住检测: 已激活但交易所止损明显落后于应有位置的连续轮数
    #[serde(default)]
    ratchet_stuck_cycles: u32,
    // 移动止盈主扳机是否已发出平仓单 (防止重复市价单)
    #[serde(default)]
    trail_exit_fired: bool,
    // 持仓期峰值 ROE 快照 (随巡逻更新, 仓位了结时归档进滚动统计)
    #[serde(default)]
    peak_roe: Decimal,
    last_hold_alert_min: u64,
    stop_error_notified: bool,
    auto_close_attempted: bool,
}

impl Default for GuardState {
    fn default() -> Self {
        Self { own_stop_id: None, entry_used: Decimal::ZERO, stop_price: Decimal::ZERO, peak: Decimal::ZERO, trail_armed: false, last_milestone_roe: 0, trail_confirm_sent: false, ratchet_stuck_cycles: 0, trail_exit_fired: false, peak_roe: Decimal::ZERO, last_hold_alert_min: 0, stop_error_notified: false, auto_close_attempted: false }
    }
}

// ROE 里程碑播报档位 (与旧版 strategy.rs 的暴涨通知同档, 现由保镖统一收编)
const ROE_MILESTONES: [i32; 8] = [10, 15, 20, 25, 30, 40, 50, 100];

async fn read_cfg(con: &mut redis::aio::MultiplexedConnection, key: &str, default: &str) -> String {
    redis::cmd("GET").arg(key).query_async::<Option<String>>(con).await.ok().flatten().unwrap_or_else(|| default.to_string())
}

// 止损价对齐 tick_size；多单止损向下取整、空单止损向上取整，保证不会因精度被拒单
// 追踪出场线: 比例回吐模式下 = 峰值ROE×(1-N%) 折回价格; 否则固定回吐 trail_pct(币价%)
fn trail_floor_px(is_long: bool, entry: Decimal, peak: Decimal, lev: Decimal, trail_pct: Decimal, giveback_pct: Decimal) -> Decimal {
    if giveback_pct > Decimal::ZERO && entry > Decimal::ZERO {
        let peak_roe = if is_long { (peak - entry) / entry * dec!(100) * lev } else { (entry - peak) / entry * dec!(100) * lev };
        let floor_roe = peak_roe * (Decimal::ONE - giveback_pct / dec!(100));
        if is_long {
            entry * (Decimal::ONE + floor_roe / (dec!(100) * lev))
        } else {
            entry * (Decimal::ONE - floor_roe / (dec!(100) * lev))
        }
    } else if is_long {
        peak * (Decimal::ONE - trail_pct / dec!(100))
    } else {
        peak * (Decimal::ONE + trail_pct / dec!(100))
    }
}

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
        // 3秒巡逻: 10秒采样在急拉急跌里让止损位平均滞后半个周期, 实测每单吃掉
        // 2~5 个 ROE 点 (峰值31的单只落袋15的主因之一)。API 权重预算充足。
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;

        let mut con = match redis_client.get_multiplexed_async_connection().await {
            Ok(c) => c,
            Err(_) => continue,
        };

        if read_cfg(&mut con, "GUARD_ENABLED", "1").await != "1" {
            continue;
        }
        // 所有阈值按 ROE 配置 (ROE = 币价涨跌% × 杠杆, 即币安 App 显示的收益率),
        // 每个仓位按自身实际杠杆折算成币价距离 —— 固定币价阈值在高杠杆下会失效
        // (例: 20x 强平在币价 -4.75%, 固定 -5% 币价止损挂在强平线外面, 形同虚设)。
        // 硬止损默认 ROE -50%: 10x 时 = 币价 -5%, 与旧默认等价; 20x 时 = 币价 -2.5%, 仍在强平线内。
        let stop_roe = Decimal::from_str(&read_cfg(&mut con, "GUARD_STOP_ROE", "50").await).unwrap_or(dec!(50));
        let alert_min: u64 = read_cfg(&mut con, "GUARD_HOLD_ALERT_MIN", "30").await.parse().unwrap_or(30);
        let auto_close_min: u64 = read_cfg(&mut con, "GUARD_AUTO_CLOSE_MIN", "0").await.parse().unwrap_or(0);
        // 移动止盈: 浮盈达到激活线后, 止损跟随持仓期最优价回撤 trail 并只朝有利方向棘轮。
        // 激活线 > 回撤幅度, 保证激活后的仓位数学上不可能盈转亏。
        let trail_on = read_cfg(&mut con, "GUARD_TRAIL_ENABLED", "1").await == "1";
        let arm_roe = Decimal::from_str(&read_cfg(&mut con, "GUARD_TRAIL_ARM_ROE", "20").await).unwrap_or(dec!(20));
        let trail_roe = Decimal::from_str(&read_cfg(&mut con, "GUARD_TRAIL_ROE", "15").await).unwrap_or(dec!(15));
        // 比例回吐模式 (>0 时生效, 覆盖固定回吐): 回吐"峰值ROE的N%"而非固定点数。
        // 低峰值时更紧(保本更实: 激活后最差 = 激活线×(1-N%)), 高峰值时缓冲更宽(拿得住主升浪)。
        let giveback_pct = Decimal::from_str(&read_cfg(&mut con, "GUARD_TRAIL_GIVEBACK_PCT", "0").await).unwrap_or(Decimal::ZERO);

        let pos_str = match exec.check_positions().await {
            Ok(s) => s,
            Err(e) => { error!("🛡 保镖拉取仓位失败: {}", e); continue; }
        };
        let positions: Vec<serde_json::Value> = serde_json::from_str(&pos_str).unwrap_or_default();

        // sym -> (数量, 开仓均价, 未实现盈亏, 标记价格, 杠杆)
        let mut live: HashMap<String, (Decimal, Decimal, Decimal, Decimal, Decimal)> = HashMap::new();
        for pos in &positions {
            let amt = pos.get("positionAmt").and_then(|v| v.as_str()).and_then(|s| Decimal::from_str(s).ok()).unwrap_or(Decimal::ZERO);
            if amt == Decimal::ZERO { continue; }
            let sym = pos.get("symbol").and_then(|v| v.as_str()).unwrap_or("").to_string();
            let entry = pos.get("entryPrice").and_then(|v| v.as_str()).and_then(|s| Decimal::from_str(s).ok()).unwrap_or(Decimal::ZERO);
            let upnl = pos.get("unRealizedProfit").and_then(|v| v.as_str()).and_then(|s| Decimal::from_str(s).ok()).unwrap_or(Decimal::ZERO);
            let mark = pos.get("markPrice").and_then(|v| v.as_str()).and_then(|s| Decimal::from_str(s).ok()).unwrap_or(Decimal::ZERO);
            let lev = pos.get("leverage").and_then(|v| v.as_str()).and_then(|s| Decimal::from_str(s).ok()).unwrap_or(dec!(10));
            live.insert(sym, (amt, entry, upnl, mark, lev));
        }

        // ---------- 已平仓位的善后: 撤掉遗留的整仓止损单, 清除计时 ----------
        let tracked: Vec<String> = redis::cmd("KEYS").arg("GUARD_OPENED_*").query_async::<Vec<String>>(&mut con).await.unwrap_or_default();
        for key in tracked {
            let sym = key.trim_start_matches("GUARD_OPENED_").to_string();
            if live.contains_key(&sym) { continue; }
            // 遗留止损单还挂着 = 仓位不是被它平的 (手动或直接扳机); 不在了 = 它成交的
            let mut leftover_stop_found = false;
            if let Ok(orders_str) = exec.get_open_algo_orders(&sym).await {
                if let Ok(orders) = serde_json::from_str::<Vec<serde_json::Value>>(&orders_str) {
                    for o in orders {
                        let is_close_stop = o.get("closePosition").and_then(|v| v.as_bool()).unwrap_or(false)
                            && o.get("orderType").and_then(|v| v.as_str()).unwrap_or("") == "STOP_MARKET";
                        if is_close_stop {
                            leftover_stop_found = true;
                            if let Some(oid) = o.get("algoId").and_then(|v| v.as_u64()) {
                                let _ = exec.cancel_algo_order(oid).await;
                                info!("🛡 [{}] 仓位已平, 撤掉遗留止损挂单 #{}", sym, oid);
                            }
                        }
                    }
                }
            }
            // 仓位了结战报: 无论是手动平、止损还是移动止盈成交, 都汇报已实现盈亏
            let opened_at: u64 = redis::cmd("GET").arg(&key).query_async::<Option<String>>(&mut con).await.ok().flatten().and_then(|s| s.parse().ok()).unwrap_or(0);
            let now_ms = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64;
            if opened_at > 0 {
                if let Ok(income_str) = exec.get_income_history(opened_at, now_ms).await {
                    if let Ok(items) = serde_json::from_str::<Vec<serde_json::Value>>(&income_str) {
                        let mut realized = Decimal::ZERO;
                        let mut fee_usdt = Decimal::ZERO;
                        for it in items.iter().filter(|it| it.get("symbol").and_then(|v| v.as_str()) == Some(&sym)) {
                            let val = it.get("income").and_then(|v| v.as_str()).and_then(|s| Decimal::from_str(s).ok()).unwrap_or(Decimal::ZERO);
                            match it.get("incomeType").and_then(|v| v.as_str()).unwrap_or("") {
                                "REALIZED_PNL" => realized += val,
                                "COMMISSION" if it.get("asset").and_then(|v| v.as_str()) == Some("USDT") => fee_usdt += val,
                                _ => {}
                            }
                        }
                        let held_min = now_ms.saturating_sub(opened_at) / 60_000;
                        let emoji = if realized >= Decimal::ZERO { "🟢" } else { "🔴" };
                        let (was_armed, peak_roe_rec, direct_exit) = states.get(&sym)
                            .map(|s| (s.trail_armed, s.peak_roe, s.trail_exit_fired))
                            .unwrap_or((false, Decimal::ZERO, false));
                        // 精确归因: 直接扳机开过火 > 止损单还在(说明是手动平的) > 止损单没了(它成交的)
                        let how = if direct_exit {
                            "移动止盈直接落袋"
                        } else if leftover_stop_found {
                            "手动平仓"
                        } else if was_armed {
                            "移动止盈(止损单成交)"
                        } else {
                            "硬止损成交"
                        };
                        // 滚动实时统计档案: 参数讨论只准引用这里的数据, 不准再用历史回测
                        let rec = serde_json::json!({
                            "sym": sym, "ts": now_ms, "held_min": held_min,
                            "realized": realized.to_string(), "peak_roe": peak_roe_rec.to_string(),
                            "armed": was_armed, "direct_exit": direct_exit, "exit": how,
                        });
                        let _: () = redis::cmd("LPUSH").arg("GUARD_TRADE_LOG").arg(rec.to_string()).query_async(&mut con).await.unwrap_or(());
                        let _: () = redis::cmd("LTRIM").arg("GUARD_TRADE_LOG").arg(0).arg(199).query_async(&mut con).await.unwrap_or(());
                        let _ = tg_tx.send(format!(
                            "🔓 <b>【仓位已了结】</b> {} {}\n持仓 {} 分钟 | 已实现盈亏: <b>{:+.2} U</b>{}\n出场方式: {} (交易所侧成交或手动)",
                            emoji, sym, held_min, realized.round_dp(2).normalize(),
                            if fee_usdt != Decimal::ZERO { format!(" (含USDT手续费 {:.2})", fee_usdt.round_dp(2)) } else { String::new() },
                            how)).await;
                    }
                }
            }
            let _: () = redis::cmd("DEL").arg(&key).query_async(&mut con).await.unwrap_or(());
            let _: () = redis::cmd("DEL").arg(format!("GUARD_STATE_{}", sym)).query_async(&mut con).await.unwrap_or(());
            states.remove(&sym);
        }

        // ---------- 在场仓位巡逻 ----------
        for (sym, (amt, entry, upnl, mark, lev)) in live {
            if entry <= Decimal::ZERO { continue; }
            // ROE 阈值按该仓位实际杠杆折算成币价距离。
            // 杠杆归一: ROE 阈值以 10x 为基准定义, 更高杠杆自动等比放大,
            // 保证币价距离一致 —— 否则 20x 下 ROE+10 只等于币价 0.5%,
            // 激活和回吐全部落在分钟级噪音里 (硬止损同理会被正常回踩打掉)。
            // 放大后的止损 ROE 封顶 60, 永不逼近强平线 (强平约 ROE -95)。
            let lev = lev.max(Decimal::ONE);
            let lev_scale = (lev / dec!(10)).max(Decimal::ONE);
            let stop_roe_eff = (stop_roe * lev_scale).min(dec!(60));
            let arm_roe_eff = arm_roe * lev_scale;
            let trail_roe_eff = trail_roe * lev_scale;
            let stop_pct = stop_roe_eff / lev;
            let trail_arm = arm_roe_eff / lev;
            let trail_pct = trail_roe_eff / lev;
            let now_ms = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64;
            let opened_key = format!("GUARD_OPENED_{}", sym);
            // SETNX: 只在首次发现时记录开仓时间, 引擎重启不清零
            let _: () = redis::cmd("SET").arg(&opened_key).arg(now_ms).arg("NX").query_async(&mut con).await.unwrap_or(());
            let opened_at: u64 = redis::cmd("GET").arg(&opened_key).query_async::<Option<String>>(&mut con).await.ok().flatten().and_then(|s| s.parse().ok()).unwrap_or(now_ms);
            let held_min = now_ms.saturating_sub(opened_at) / 60_000;

            // 状态恢复: 内存没有时先从 Redis 读 (引擎重启后接续之前的 peak/armed/归属记录)
            if !states.contains_key(&sym) {
                let restored: Option<GuardState> = redis::cmd("GET").arg(format!("GUARD_STATE_{}", sym))
                    .query_async::<Option<String>>(&mut con).await.ok().flatten()
                    .and_then(|s| serde_json::from_str(&s).ok());
                if restored.is_some() {
                    info!("🛡 [{}] 已从 Redis 恢复保镖状态 (重启接续)", sym);
                }
                states.insert(sym.clone(), restored.unwrap_or_default());
            }
            let st = states.get_mut(&sym).unwrap();
            let is_long = amt > Decimal::ZERO;

            // ---------- 0. 移动止盈: 跟踪最优价, 浮盈过激活线后武装 ----------
            if mark > Decimal::ZERO {
                if st.peak <= Decimal::ZERO {
                    st.peak = if is_long { mark.max(entry) } else { mark.min(entry) };
                } else {
                    st.peak = if is_long { st.peak.max(mark) } else { st.peak.min(mark) };
                }
                // ROE 里程碑播报: 用 peak (只升不降) 而非当前 mark, 语义是"曾经到过", 不随价格回落取消
                let peak_roe = if is_long { (st.peak - entry) / entry * dec!(100) * lev } else { (entry - st.peak) / entry * dec!(100) * lev };
                st.peak_roe = peak_roe; // 档案快照: 了结时写入滚动统计
                for &lvl in ROE_MILESTONES.iter() {
                    if peak_roe >= Decimal::from(lvl) && st.last_milestone_roe < lvl {
                        st.last_milestone_roe = lvl;
                        info!("🚀 [{}] 浮盈突破 ROE +{}% (峰值 {:+}%)", sym, lvl, peak_roe.round_dp(1));
                        let _ = tg_tx.send(format!(
                            "🚀 <b>【浮盈里程碑】</b> {} {} ({}x)\n\n峰值 ROE 已突破: <b>+{}%</b>\n持仓量: {} | 均价: {}",
                            sym, if is_long { "🟢 多" } else { "🔴 空" }, lev, lvl, amt.abs().normalize(), entry.round_dp(6).normalize())).await;
                    }
                }

                let profit_pct = if is_long { (mark - entry) / entry * dec!(100) } else { (entry - mark) / entry * dec!(100) };
                if trail_on && !st.trail_armed && profit_pct >= trail_arm {
                    st.trail_armed = true;
                    let floor_px = trail_floor_px(is_long, entry, st.peak, lev, trail_pct, giveback_pct);
                    let profit_roe = (profit_pct * lev).round_dp(1).normalize();
                    info!("🔒 [{}] 移动止盈已激活 (ROE {:+}%, {}x), 从最优价回撤币价 {:.2}% 落袋", sym, profit_roe, lev, trail_pct);
                    let _ = tg_tx.send(format!(
                        "🔒 <b>【移动止盈已激活】</b> {}\n\n当前浮盈: ROE <b>{:+}%</b> (已过激活线 ROE {}%, 杠杆 {}x)\n从此上不封顶: 止损将跟随最高点上移, ROE 从峰值回吐 {}% 自动落袋\n目标保底出场价: <b>{}</b>\n⚠️ 以交易所实际挂单为准: 止损单实际搬到位后会另发【止损已实际上移】确认, 没收到确认前保底不生效",
                        sym, profit_roe, arm_roe_eff.normalize(), lev, trail_roe_eff.normalize(), floor_px.round_dp(6).normalize())).await;
                }
            }

            // ---------- 0.5 移动止盈主扳机: 引擎侧直接市价平仓 ----------
            // 自动卖出不再寄生于"交易所止损单有没有被搬到位"(棘轮三次事故的教训):
            // 已激活 + 标记价跌破回吐线 → 保镖直接市价平仓落袋 (reduceOnly, 只减不开)。
            // 交易所侧止损单降级为引擎宕机时的兜底, 搬单成败不再决定卖出与否。
            if trail_on && st.trail_armed && !st.trail_exit_fired && mark > Decimal::ZERO && st.peak > Decimal::ZERO {
                let floor = trail_floor_px(is_long, entry, st.peak, lev, trail_pct, giveback_pct);
                let crossed = if is_long { mark <= floor } else { mark >= floor };
                if crossed {
                    let side = if is_long { "SELL" } else { "BUY" };
                    let qty_str = amt.abs().normalize().to_string();
                    let peak_roe_now = if is_long { (st.peak - entry) / entry * dec!(100) * lev } else { (entry - st.peak) / entry * dec!(100) * lev };
                    let mark_roe_now = if is_long { (mark - entry) / entry * dec!(100) * lev } else { (entry - mark) / entry * dec!(100) * lev };
                    match exec.place_order(&sym, side, "MARKET", &qty_str, true).await {
                        Ok(_) => {
                            st.trail_exit_fired = true;
                            info!("💰 [{}] 移动止盈触发, 引擎直接市价平仓 (峰值ROE {:+.1}% 回落至 {:+.1}%)", sym, peak_roe_now, mark_roe_now);
                            let _ = tg_tx.send(format!(
                                "💰 <b>【移动止盈触发, 已市价平仓】</b> {}\n峰值 ROE {:+.1}% → 回落至 {:+.1}%, 触及回吐线\n引擎已直接市价落袋 (不依赖止损单)。成交结果稍后以【仓位已了结】汇报。",
                                sym, peak_roe_now, mark_roe_now)).await;
                            // 平仓单已发出, 本轮不再对该仓位做其他动作
                            if let Ok(j) = serde_json::to_string(&*st) {
                                let _: () = redis::cmd("SET").arg(format!("GUARD_STATE_{}", sym)).arg(j).query_async(&mut con).await.unwrap_or(());
                            }
                            continue;
                        }
                        Err(e) => {
                            error!("💰 [{}] 移动止盈市价平仓失败 (下一轮重试, 交易所止损单仍兜底): {}", sym, e);
                            let _ = tg_tx.send(format!("⚠️ <b>【移动止盈平仓失败】</b> {} : {}\n下一轮(10秒)自动重试; 交易所侧止损单仍在兜底。", sym, e)).await;
                        }
                    }
                }
            }

            // ---------- 1. 确保交易所侧止损单存在 (条件单在 algo 接口, 不在 openOrders) ----------
            match exec.get_open_algo_orders(&sym).await {
                Ok(orders_str) => {
                    let orders: Vec<serde_json::Value> = serde_json::from_str(&orders_str).unwrap_or_default();
                    let existing_stop = orders.iter().find(|o| {
                        o.get("closePosition").and_then(|v| v.as_bool()).unwrap_or(false)
                            && o.get("orderType").and_then(|v| v.as_str()).unwrap_or("") == "STOP_MARKET"
                    });

                    // 期望止损价: 灾难线 (基于均价) 与移动止盈线 (基于最优价) 取对我们更有利者
                    let disaster = if is_long {
                        entry * (Decimal::ONE - stop_pct / dec!(100))
                    } else {
                        entry * (Decimal::ONE + stop_pct / dec!(100))
                    };
                    let raw_desired = if trail_on && st.trail_armed {
                        let trail_line = trail_floor_px(is_long, entry, st.peak, lev, trail_pct, giveback_pct);
                        if is_long { disaster.max(trail_line) } else { disaster.min(trail_line) }
                    } else {
                        disaster
                    };
                    let tick = exchange_info.get(&sym).map(|i| i.tick_size);
                    let desired_px = align_to_tick(raw_desired, tick, is_long);

                    let mut place_new = false;
                    let mut replace_old: Option<u64> = None;
                    let mut existing_trigger: Option<Decimal> = None;
                    match existing_stop {
                        Some(o) => {
                            let oid = o.get("algoId").and_then(|v| v.as_u64());
                            let tp_opt = o.get("triggerPrice").and_then(|v| v.as_str()).and_then(|s| Decimal::from_str(s).ok());
                            existing_trigger = tp_opt;
                            // 归属校准: 交易所在场的整仓止损单一律以现场为准收养 —— 记录缺失
                            // (重启丢状态) 或单号过期 (换单途中崩溃/外力动过单) 都会让旧逻辑
                            // 认为"这单不是我的"而永久袖手旁观 (BLUR 事故: 激活了却永不搬单)。
                            if let (Some(id), Some(tp)) = (oid, tp_opt) {
                                if st.own_stop_id != Some(id) {
                                    info!("🛡 [{}] 收养/校准在场止损单 #{} @ {} (原记录 {:?})", sym, id, tp.normalize(), st.own_stop_id);
                                    st.own_stop_id = Some(id);
                                    st.stop_price = tp;
                                    if st.entry_used <= Decimal::ZERO {
                                        st.entry_used = entry;
                                    }
                                }
                            }
                            let is_ours = st.own_stop_id.is_some() && st.own_stop_id == oid;
                            if is_ours {
                                // 重挂条件: ① 移动止盈棘轮上移超 0.1% (只朝有利方向)
                                //          ② 未激活时均价漂移超 0.5% (如加仓后)
                                // 比较基准必须用交易所实际触发价 —— 内部记录 (st.stop_price)
                                // 可能与现实脱节, 用它做基准会让棘轮"以为不用动" (BLUR 二次卡住的根因),
                                // 而告警用的是真实价, 于是出现"告警响了、棘轮不动"的分裂
                                let baseline = existing_trigger.unwrap_or(st.stop_price);
                                let improved = baseline > Decimal::ZERO && if is_long {
                                    desired_px > baseline * dec!(1.001)
                                } else {
                                    desired_px < baseline * dec!(0.999)
                                };
                                // 自愈: 卡住检测已计数 ≥2 轮时无条件强制换单一次, 不再信任任何判断
                                let force_retry = st.ratchet_stuck_cycles >= 5 && st.trail_armed;
                                let improved = improved || force_retry;
                                // 未激活时: 止损必须锚定当前均价。比较"交易所实际触发价"和
                                // "按当前均价应在的位置", 偏差超 0.2% 即重挂 (方向不限, 加仓摊高
                                // 摊低都跟)。旧逻辑用 0.5% 均价漂移做死区, 小幅加仓会被无视,
                                // 用户观察到"止损只认第一次买入价"就是这个死区+4130联手造成的
                                let drifted = !st.trail_armed && baseline > Decimal::ZERO
                                    && ((desired_px - baseline).abs() / baseline) > dec!(0.002);
                                if improved || drifted {
                                    // 先挂新单、成功后再撤旧单 —— 撤单在前的顺序在"撤成功+挂失败"
                                    // 时会留下无保护窗口 (仓位裸奔), 顺序在此不可反转
                                    replace_old = oid;
                                    place_new = true;
                                }
                            }
                        }
                        None => place_new = true,
                    }

                    let mut moved_ok = false;
                    if place_new {
                        let side = if is_long { "SELL" } else { "BUY" };
                        let stop_px = desired_px;
                        let stop_str = stop_px.normalize().to_string();
                        // 币安硬性限制 (-4130): 同方向只允许一张 closePosition 条件单,
                        // "先挂新后撤旧"物理上不可能 (EVAA/BLUR 卡死的实锤根因)。
                        // 必须先撤后挂; 裸奔窗口两层兜底: ①同循环毫秒级衔接+挂单失败连试3次
                        // ②移动止盈主扳机(引擎直接市价平仓)完全不依赖这张单
                        if let Some(old) = replace_old {
                            if let Err(e) = exec.cancel_algo_order(old).await {
                                info!("🛡 [{}] 旧止损单 #{} 撤销失败 (可能已触发/已撤): {}", sym, old, e);
                            }
                        }
                        let mut placed: Result<u64, String> = Err(String::new());
                        for attempt in 0..3u8 {
                            placed = exec.place_stop_market_close(&sym, side, &stop_str).await;
                            if placed.is_ok() {
                                break;
                            }
                            if attempt < 2 {
                                tokio::time::sleep(std::time::Duration::from_millis(300)).await;
                            }
                        }
                        match placed {
                            Ok(oid) => {
                                st.own_stop_id = Some(oid);
                                st.entry_used = entry;
                                st.stop_price = stop_px;
                                st.stop_error_notified = false;
                                moved_ok = true;
                                if st.trail_armed {
                                    let locked = (amt.abs() * (stop_px - entry) * if is_long { Decimal::ONE } else { dec!(-1) }).round_dp(2).normalize();
                                    info!("🔒 [{}] 止盈棘轮上移 @ {} (锁定盈亏 {:+}U)", sym, stop_str, locked);
                                    // 激活后首次真正搬动交易所止损单 → 发一次实锤确认 (SYN事故教训:
                                    // 激活消息只是意图, 这条才是交易所侧已兑现的证据); 后续上移只写日志防刷屏
                                    if !st.trail_confirm_sent {
                                        st.trail_confirm_sent = true;
                                        let _ = tg_tx.send(format!(
                                            "✅ <b>【止损已实际上移】</b> {}\n交易所挂单已确认: 触发价 <b>{}</b> (锁定盈亏 {:+}U)\n之后每次上移在服务器日志可查, 也可随时 /guard status 核对。",
                                            sym, stop_str, locked)).await;
                                    }
                                } else {
                                    // 触发时的预计亏损 = 仓位数量 × |均价 - 止损价| (市价成交, 滑点另计)
                                    let est_loss = (amt.abs() * (entry - stop_px).abs()).round_dp(2).normalize();
                                    info!("🛡 [{}] 已挂交易所侧硬止损 @ {} (均价 {}, ROE -{}% / 币价 -{:.2}%, 预计亏损 {}U)", sym, stop_str, entry.normalize(), stop_roe_eff.normalize(), stop_pct, est_loss);
                                    let _ = tg_tx.send(format!("🛡 <b>【保镖已就位】</b>\n\n交易对: {}\n方向: {} ({}x)\n开仓均价: {}\n硬止损已挂在交易所: <b>{}</b> (ROE -{}% / 币价 -{:.2}%)\n触发预计亏损: <b>-{} U</b> (不含滑点)\n浮盈过 ROE {}% 后自动切换移动止盈模式\n\n即使引擎断电, 这张止损单也会由币安执行, 强平不可能发生。", sym, if is_long { "🟢 多" } else { "🔴 空" }, lev, entry.round_dp(6).normalize(), stop_str, stop_roe_eff.normalize(), stop_pct, est_loss, arm_roe_eff.normalize())).await;
                                }
                            }
                            Err(e) => {
                                // 先撤后挂模式下, 挂新失败 = 旧单已撤、仓位暂时裸奔 (3次重试都失败才会到这)。
                                // 下一轮巡逻会按"无止损"路径重新挂; 已激活仓位另有直接市价扳机兜底。
                                st.own_stop_id = None;
                                st.stop_price = Decimal::ZERO;
                                error!("🛡 [{}] 挂止损失败 (3次重试后), 仓位暂时无止损单: {}", sym, e);
                                if !st.stop_error_notified {
                                    st.stop_error_notified = true;
                                    let _ = tg_tx.send(format!("⚠️ <b>【保镖告警】</b> {} 挂止损失败, 该仓位暂时无止损单 (每10秒自动重挂; 已激活的移动止盈不受影响)!\n原因: {}", sym, e)).await;
                                }
                            }
                        }
                    }

                    // ---------- 棘轮卡住检测 (BLUR 事故教训: 卡死必须出声, 不许沉默) ----------
                    // 已激活但交易所在场止损明显落后于应有位置且本轮没有成功搬单 → 连续 3 轮报警
                    if trail_on && st.trail_armed && !moved_ok {
                        let lagging = existing_trigger.map(|tp| if is_long {
                            desired_px > tp * dec!(1.005)
                        } else {
                            desired_px < tp * dec!(0.995)
                        }).unwrap_or(false);
                        if lagging {
                            st.ratchet_stuck_cycles += 1;
                            if st.ratchet_stuck_cycles == 10 || st.ratchet_stuck_cycles % 300 == 0 {
                                let tp = existing_trigger.unwrap_or_default();
                                error!("🛡 [{}] 棘轮疑似卡住: 交易所止损 {} 落后于应有 {} 已 {} 轮", sym, tp.normalize(), desired_px.normalize(), st.ratchet_stuck_cycles);
                                let _ = tg_tx.send(format!(
                                    "🚨 <b>【棘轮卡住告警】</b> {}\n移动止盈已激活, 但交易所止损单 ({}) 落后于应有位置 ({}) 已持续 {} 个巡逻周期。\n浮盈保护可能未生效, 建议立即手动核对/调整止损, 并查服务器日志:\nsudo journalctl -u matrix-quant | grep 棘轮 | tail",
                                    sym, tp.normalize(), desired_px.normalize(), st.ratchet_stuck_cycles)).await;
                            }
                        } else {
                            st.ratchet_stuck_cycles = 0;
                        }
                    } else if moved_ok {
                        st.ratchet_stuck_cycles = 0;
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

            // ---------- 4. 状态持久化: 每轮落盘, 引擎重启后无缝接续 ----------
            if let Ok(j) = serde_json::to_string(&*st) {
                let _: () = redis::cmd("SET").arg(format!("GUARD_STATE_{}", sym)).arg(j).query_async(&mut con).await.unwrap_or(());
            }
        }
    }
}
