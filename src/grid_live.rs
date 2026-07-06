// ==========================================
// MODULE: 现货网格实盘执行器 (Grid Live Executor) — 多币种并发版
// ⚠️ 唯一会用真钱下现货单的模块。默认关闭 (GRID_LIVE_ENABLED=0), 部署后
// 必须用 Telegram /gridlive on 显式开启才会动一分钱。
//
// 并发模型: 同时运行最多 GRID_LIVE_MAX_ACTIVE 个网格 (默认 2)。
// GRID_LIVE_BUDGET 是**每个网格**的预算 (默认 50U), 开新网格前检查现货 USDT
// 可用余额是否够一份预算, 不够就不开 (用户要开更多网格时自行充值)。
// 每个网格的格线数按"每格名义 ≥ 交易所最小限制"动态推导 (2~8 条)。
// 自动模式 (GRID_LIVE_SYMBOL=AUTO): 空槽位从候选榜单自动补币, 网格越界
// 失效后该币冷却 6 小时换下一个。手动模式: 只跑指定的那一个币。
//
// 盈利监听: 所有网格合计总盈亏(已实现+库存浮动) 亏到 (每格预算×网格数) 的 20% → 全部清算
// +彻底停机 (模型级失败要人工复盘, 不自动换币继续流血)。
// 每小时聚合报告; 独立 shell watchdog 盯引擎本身的死活 (scripts/grid_watchdog.sh)。
// ==========================================
use std::sync::Arc;
use serde::{Serialize, Deserialize};
use tokio::sync::mpsc;
use tracing::{info, error};

use crate::spot::{SpotClient, SpotSymbolRules};

const POLL_SECS: u64 = 20;
const RANGE_PCT: f64 = 10.0;
const BUST_MARGIN_PCT: f64 = 5.0;
const MAX_LOSS_PCT_OF_BUDGET: f64 = 20.0;
const AUTO_COOLDOWN_SECS: u64 = 6 * 3600;
const MAX_BUY_LINES: usize = 8;
const MIN_BUY_LINES: usize = 2;

#[derive(Serialize, Deserialize, Clone, PartialEq)]
enum LineState {
    WaitBuy { order_id: u64 },
    WaitSell { order_id: u64, qty: f64, buy_quote: f64 },
}

#[derive(Serialize, Deserialize, Clone)]
struct LiveGrid {
    symbol: String,
    base_asset: String,
    lines: Vec<f64>, // n_buy+1 条: 索引 0..n_buy-1 是买入线, i 的卖出目标是 lines[i+1]
    per_grid_quote: f64,
    line_states: Vec<Option<LineState>>,
    realized_total: f64,
    round_trips: u32,
    tick_size: f64,
    step_size: f64,
    min_notional: f64,
}

impl LiveGrid {
    fn rules(&self) -> SpotSymbolRules {
        SpotSymbolRules { tick_size: self.tick_size, step_size: self.step_size, min_notional: self.min_notional }
    }
    fn n_buy_lines(&self) -> usize {
        self.lines.len().saturating_sub(1)
    }
    fn held_qty(&self) -> f64 {
        self.line_states.iter().flatten().map(|s| if let LineState::WaitSell { qty, .. } = s { *qty } else { 0.0 }).sum()
    }
    fn inventory_cost(&self) -> f64 {
        self.line_states.iter().flatten().map(|s| if let LineState::WaitSell { buy_quote, .. } = s { *buy_quote } else { 0.0 }).sum()
    }
    fn total_pnl(&self, px: f64) -> f64 {
        self.realized_total + self.held_qty() * px - self.inventory_cost()
    }
    fn open_order_ids(&self) -> Vec<u64> {
        self.line_states.iter().flatten().filter_map(|s| match s {
            LineState::WaitBuy { order_id } if *order_id > 0 => Some(*order_id),
            LineState::WaitSell { order_id, .. } if *order_id > 0 => Some(*order_id),
            _ => None,
        }).collect()
    }
}

fn state_key(sym: &str) -> String {
    format!("GRID_LIVE_STATE_{}", sym)
}

async fn load_grids(con: &mut redis::aio::MultiplexedConnection) -> Vec<LiveGrid> {
    let keys: Vec<String> = redis::cmd("KEYS").arg("GRID_LIVE_STATE_*").query_async(con).await.unwrap_or_default();
    let mut grids = Vec::new();
    for k in keys {
        if let Some(json) = redis::cmd("GET").arg(&k).query_async::<Option<String>>(con).await.ok().flatten() {
            if let Ok(g) = serde_json::from_str::<LiveGrid>(&json) {
                grids.push(g);
            } else {
                let _: () = redis::cmd("DEL").arg(&k).query_async(con).await.unwrap_or(());
            }
        }
    }
    grids
}

async fn save_grid(con: &mut redis::aio::MultiplexedConnection, g: &LiveGrid) {
    if let Ok(j) = serde_json::to_string(g) {
        let _: () = redis::cmd("SET").arg(state_key(&g.symbol)).arg(j).query_async(con).await.unwrap_or(());
    }
}

async fn delete_grid(con: &mut redis::aio::MultiplexedConnection, sym: &str) {
    let _: () = redis::cmd("DEL").arg(state_key(sym)).query_async(con).await.unwrap_or(());
}

async fn read_flag(con: &mut redis::aio::MultiplexedConnection, key: &str, default: &str) -> String {
    redis::cmd("GET").arg(key).query_async::<Option<String>>(con).await.ok().flatten().unwrap_or_else(|| default.to_string())
}

async fn cancel_all(spot: &SpotClient, g: &LiveGrid) {
    for oid in g.open_order_ids() {
        if let Err(e) = spot.cancel_order(&g.symbol, oid).await {
            error!("🔲 [网格实盘] {} 撤单 #{} 失败 (可能已成交/已撤): {}", g.symbol, oid, e);
        }
    }
}

// 市价清算库存, 卖出量取 min(账面, 实际可用) —— 手续费从币里扣过, 账面可能略大,
// 按账面卖会整单被拒, 清算失败比少卖一点严重得多
async fn liquidate_inventory(spot: &SpotClient, g: &LiveGrid) -> f64 {
    let mut qty = g.held_qty();
    if let Ok(free) = spot.free_balance(&g.base_asset).await {
        qty = qty.min(free);
    }
    if qty <= 0.0 {
        return 0.0;
    }
    match spot.market_sell(&g.symbol, qty, &g.rules()).await {
        Ok(quote) => quote,
        Err(e) => {
            error!("🔲 [网格实盘] {} 清算库存失败: {}", g.symbol, e);
            0.0
        }
    }
}

pub async fn run_grid_live(spot: Arc<SpotClient>, redis_client: redis::Client, tg_tx: mpsc::Sender<String>) {
    let report_offset = time::UtcOffset::from_hms(8, 0, 0).unwrap();
    info!("🔲 网格实盘执行器已加载 (多币种并发, 默认关闭, /gridlive on 显式开启后才会下真实订单)");

    loop {
        tokio::time::sleep(std::time::Duration::from_secs(POLL_SECS)).await;
        let mut con = match redis_client.get_multiplexed_async_connection().await {
            Ok(c) => c,
            Err(_) => continue,
        };
        let enabled = read_flag(&mut con, "GRID_LIVE_ENABLED", "0").await == "1";
        let liquidate_req = read_flag(&mut con, "GRID_LIVE_LIQUIDATE", "0").await == "1";
        let budget: f64 = read_flag(&mut con, "GRID_LIVE_BUDGET", "50").await.parse().unwrap_or(50.0);
        let max_active: usize = read_flag(&mut con, "GRID_LIVE_MAX_ACTIVE", "2").await.parse().unwrap_or(2).clamp(1, 6);
        let mut grids = load_grids(&mut con).await;

        // ---------- 清算指令: 全部网格撤单 + 市价卖光 + 停机 ----------
        if liquidate_req {
            let _: () = redis::cmd("SET").arg("GRID_LIVE_LIQUIDATE").arg("0").query_async(&mut con).await.unwrap_or(());
            let mut total_back = 0.0;
            let mut total_realized = 0.0;
            for g in &grids {
                cancel_all(&spot, g).await;
                total_back += liquidate_inventory(&spot, g).await;
                total_realized += g.realized_total;
                delete_grid(&mut con, &g.symbol).await;
            }
            let _: () = redis::cmd("SET").arg("GRID_LIVE_ENABLED").arg("0").query_async(&mut con).await.unwrap_or(());
            let _ = tg_tx.send(format!(
                "🔲 <b>【网格实盘已全部清算】</b>\n{} 个网格: 撤销全部挂单, 库存市价卖出收回 {:.2}U\n累计已实现: {:+.2}U\n执行器已停机。",
                grids.len(), total_back, total_realized)).await;
            continue;
        }

        if !enabled {
            // 关闭状态下若还有挂单 (刚被 /gridlive off), 撤掉挂单但保留库存与账本
            for g in grids.iter_mut() {
                if g.open_order_ids().is_empty() {
                    continue;
                }
                cancel_all(&spot, g).await;
                for s in g.line_states.iter_mut() {
                    if matches!(s, Some(LineState::WaitBuy { .. })) {
                        *s = None;
                    }
                    // WaitSell 保留: 库存还在手里, 卖单已撤 (恢复后按 order_id=0 路径重挂)
                    if let Some(LineState::WaitSell { order_id, .. }) = s {
                        *order_id = 0;
                    }
                }
                save_grid(&mut con, g).await;
                let _ = tg_tx.send(format!(
                    "🔲 <b>【网格实盘已暂停】</b> {}\n挂单已撤销; 库存 {:.6} {} 保留。恢复: /gridlive on | 清仓: /gridlive liquidate",
                    g.symbol, g.held_qty(), g.base_asset)).await;
            }
            continue;
        }

        // ---------- 补齐槽位 ----------
        let cfg_symbol = read_flag(&mut con, "GRID_LIVE_SYMBOL", "AUTO").await;
        let is_auto = cfg_symbol.is_empty() || cfg_symbol == "AUTO";
        let slots = if is_auto { max_active } else { 1 };
        let per_budget = budget; // 预算是"每个网格"的, 不做均分; 钱不够就少开网格

        // 手动模式: 清理与指定标的不符的旧网格挂单 (库存保留, 不强卖)
        if !is_auto {
            for g in grids.iter().filter(|g| g.symbol != cfg_symbol) {
                info!("🔲 [网格实盘] 手动切换标的 -> {}, 清理 {} 的遗留挂单", cfg_symbol, g.symbol);
                cancel_all(&spot, g).await;
                let _ = tg_tx.send(format!(
                    "🔲 <b>【网格换标的】</b> {} 的挂单已撤销 (库存 {:.6} {} 保留, 需要清仓用 /gridlive liquidate 或手动卖出)",
                    g.symbol, g.held_qty(), g.base_asset)).await;
                delete_grid(&mut con, &g.symbol).await;
            }
            grids.retain(|g| g.symbol == cfg_symbol);
        }

        while grids.len() < slots {
            // 开新网格前先看现货 USDT 可用余额够不够一份预算 —— 不够就不开,
            // 千万不能拿"余额不足"当币的问题去冷却候选 (会把整个榜单误伤一遍)
            match spot.free_balance("USDT").await {
                Ok(free) if free >= per_budget => {}
                Ok(free) => {
                    info!("🔲 [网格实盘] USDT 可用余额 {:.2} 不足一份网格预算 {:.0}, 暂不开新网格", free, per_budget);
                    break;
                }
                Err(_) => break,
            }
            let pick = if is_auto {
                let Some(cand_json) = redis::cmd("GET").arg("GRID_CANDIDATES").query_async::<Option<String>>(&mut con).await.ok().flatten() else { break };
                let Ok(cands) = serde_json::from_str::<Vec<String>>(&cand_json) else { break };
                let mut picked = String::new();
                for s in cands {
                    if grids.iter().any(|g| g.symbol == s) {
                        continue;
                    }
                    let cooling: Option<String> = redis::cmd("GET").arg(format!("GRID_LIVE_COOLDOWN_{}", s)).query_async(&mut con).await.ok().flatten();
                    if cooling.is_none() {
                        picked = s;
                        break;
                    }
                }
                picked
            } else if grids.is_empty() {
                cfg_symbol.clone()
            } else {
                String::new()
            };
            if pick.is_empty() {
                break;
            }
            match init_grid(&spot, &pick, per_budget).await {
                Ok(g) => {
                    let _ = tg_tx.send(format!(
                        "🔲 <b>【网格实盘启动】</b> {} (真实订单!)\n该网格预算: {:.0}U | 每格: {:.2}U | 买入线: {} 条\n区间: {:.6} ~ {:.6}\n并发: {}/{} | 失效线: 越界再 {}%",
                        g.symbol, per_budget, g.per_grid_quote, g.n_buy_lines(),
                        g.lines[0], g.lines.last().unwrap(), grids.len() + 1, slots, BUST_MARGIN_PCT)).await;
                    save_grid(&mut con, &g).await;
                    grids.push(g);
                }
                Err(e) => {
                    error!("🔲 [网格实盘] {} 初始化失败: {}", pick, e);
                    if is_auto {
                        // 自动模式: 该币冷却, 换下一个候选, 不停机
                        let _: () = redis::cmd("SET").arg(format!("GRID_LIVE_COOLDOWN_{}", pick)).arg("1")
                            .arg("EX").arg(AUTO_COOLDOWN_SECS).query_async(&mut con).await.unwrap_or(());
                        let _ = tg_tx.send(format!("⚠️ <b>【网格启动失败, 已跳过】</b> {}\n{}\n(自动模式: 冷却后换下一个候选)", pick, e)).await;
                    } else {
                        let _ = tg_tx.send(format!("❌ <b>【网格实盘启动失败】</b> {}\n{}\n执行器已自动关闭, 修正后重新 /gridlive on", pick, e)).await;
                        let _: () = redis::cmd("SET").arg("GRID_LIVE_ENABLED").arg("0").query_async(&mut con).await.unwrap_or(());
                    }
                    break;
                }
            }
        }

        // ---------- 逐网格巡逻 ----------
        let mut agg_pnl = 0.0;
        let mut agg_realized = 0.0;
        let mut agg_trips = 0u32;
        let mut agg_inv_cost = 0.0;
        let mut agg_inv_val = 0.0;
        let mut survivors: Vec<String> = Vec::new();

        for g in grids.iter_mut() {
            let Ok(px) = spot.last_price(&g.symbol).await else { survivors.push(g.symbol.clone()); continue };

            // 失效检查: 越界 → 清算该网格; 自动模式冷却换下一个, 手动模式停机
            let bust_low = g.lines[0] * (1.0 - BUST_MARGIN_PCT / 100.0);
            let bust_high = g.lines.last().unwrap() * (1.0 + BUST_MARGIN_PCT / 100.0);
            if px <= bust_low || px >= bust_high {
                cancel_all(&spot, g).await;
                let quote = liquidate_inventory(&spot, g).await;
                let dir = if px >= bust_high { "涨破区间上沿(踏空离场)" } else { "跌破区间下沿(止损清算)" };
                let next = if is_auto { "该币冷却 6 小时, 自动换下一个候选。" } else { "手动模式: 执行器已停机。" };
                let _ = tg_tx.send(format!(
                    "🔲 <b>【网格实盘失效清算】</b> {}\n{} @ {:.6}\n库存市价卖出收回 {:.2}U | 该网格已实现: {:+.2}U ({} 回合)\n{}",
                    g.symbol, dir, px, quote, g.realized_total, g.round_trips, next)).await;
                delete_grid(&mut con, &g.symbol).await;
                if is_auto {
                    let _: () = redis::cmd("SET").arg(format!("GRID_LIVE_COOLDOWN_{}", g.symbol)).arg("1")
                        .arg("EX").arg(AUTO_COOLDOWN_SECS).query_async(&mut con).await.unwrap_or(());
                } else {
                    let _: () = redis::cmd("SET").arg("GRID_LIVE_ENABLED").arg("0").query_async(&mut con).await.unwrap_or(());
                }
                continue; // 不进 survivors
            }

            patrol_grid(&spot, &tg_tx, g, px).await;
            save_grid(&mut con, g).await;

            agg_pnl += g.total_pnl(px);
            agg_realized += g.realized_total;
            agg_trips += g.round_trips;
            agg_inv_cost += g.inventory_cost();
            agg_inv_val += g.held_qty() * px;
            survivors.push(g.symbol.clone());
        }

        // ---------- 盈利监听熔断: 全部网格合计, 上限随在跑网格数量伸缩 ----------
        let deployed = budget * survivors.len().max(1) as f64;
        let loss_limit = -deployed * MAX_LOSS_PCT_OF_BUDGET / 100.0;
        if !survivors.is_empty() && agg_pnl <= loss_limit {
            for g in grids.iter().filter(|g| survivors.contains(&g.symbol)) {
                cancel_all(&spot, g).await;
                let _ = liquidate_inventory(&spot, g).await;
                delete_grid(&mut con, &g.symbol).await;
            }
            let _: () = redis::cmd("SET").arg("GRID_LIVE_ENABLED").arg("0").query_async(&mut con).await.unwrap_or(());
            let _ = tg_tx.send(format!(
                "🛑 <b>【网格盈利监听熔断】</b>\n全部网格合计总盈亏 {:+.2}U 触及亏损上限 {:.0}U (在场资金的 {}%)\n已清算全部挂单与库存, <b>执行器彻底停机</b>。\n模型判定为不盈利——需要人工复盘后再决定是否重启。",
                agg_pnl, loss_limit, MAX_LOSS_PCT_OF_BUDGET)).await;
            continue;
        }

        // ---------- 每小时聚合报告 ----------
        let now = time::OffsetDateTime::now_utc().to_offset(report_offset);
        let hour_bucket = format!("{:04}-{:02}-{:02}-{:02}", now.year(), now.month() as u8, now.day(), now.hour());
        let reported = read_flag(&mut con, "GRID_LIVE_LAST_REPORT_HOUR", "").await;
        if reported != hour_bucket && !survivors.is_empty() {
            let _: () = redis::cmd("SET").arg("GRID_LIVE_LAST_REPORT_HOUR").arg(&hour_bucket).query_async(&mut con).await.unwrap_or(());
            let _ = tg_tx.send(format!(
                "🔲 <b>【网格实盘小时报】</b> {}:00\n\n运行中: {} 个网格 ({})\n已实现: <b>{:+.2}U</b> ({} 回合) | 库存: 成本 {:.2}U → 现值 {:.2}U ({:+.2}U)\n总盈亏: <b>{:+.2}U</b> / 在场资金 {:.0}U | 熔断线: {:.0}U",
                now.hour(), survivors.len(), survivors.join(", "),
                agg_realized, agg_trips, agg_inv_cost, agg_inv_val, agg_inv_val - agg_inv_cost,
                agg_pnl, deployed, loss_limit)).await;
        }
    }
}

// 单个网格的一轮巡逻: 检查每条线的订单状态并做状态转移
async fn patrol_grid(
    spot: &SpotClient,
    tg_tx: &mpsc::Sender<String>,
    g: &mut LiveGrid,
    px: f64,
) {
    let Ok(open_ids) = spot.open_order_ids(&g.symbol).await else { return };
    let n_buy = g.n_buy_lines();
    for i in 0..g.line_states.len() {
        let Some(ls) = g.line_states[i].clone() else {
            // 空闲买入线: 补挂买单
            if i < n_buy && g.lines[i] < px {
                let qty = g.per_grid_quote / g.lines[i];
                match spot.place_limit(&g.symbol, "BUY", qty, g.lines[i], &g.rules()).await {
                    Ok(oid) => g.line_states[i] = Some(LineState::WaitBuy { order_id: oid }),
                    Err(e) => error!("🔲 [网格实盘] {} 第{}格补挂买单失败: {}", g.symbol, i, e),
                }
            }
            continue;
        };
        // 挂卖单失败的重试路径: order_id=0 = 有库存无卖单, 必须在 get_order 之前处理
        // (拿 0 去查单永远报错, 否则该格库存永久卡死)
        if let LineState::WaitSell { order_id: 0, qty, buy_quote } = &ls {
            let sell_px = g.lines[i + 1];
            if let Ok(sell_oid) = spot.place_limit(&g.symbol, "SELL", *qty, sell_px, &g.rules()).await {
                info!("🔲 [网格实盘] {} 第{}格补挂卖单成功 @ {:.6}", g.symbol, i, sell_px);
                g.line_states[i] = Some(LineState::WaitSell { order_id: sell_oid, qty: *qty, buy_quote: *buy_quote });
            }
            continue;
        }
        let oid = match &ls { LineState::WaitBuy { order_id } => *order_id, LineState::WaitSell { order_id, .. } => *order_id };
        if open_ids.contains(&oid) {
            continue;
        }
        let Ok((status, exec_qty, quote)) = spot.get_order(&g.symbol, oid).await else { continue };
        match (&ls, status.as_str()) {
            (LineState::WaitBuy { .. }, "FILLED") => {
                // 现货买入手续费从收到的币里扣, 按 99.9% 挂卖防余额不足被拒
                let sell_px = g.lines[i + 1];
                let sell_qty = exec_qty * 0.999;
                match spot.place_limit(&g.symbol, "SELL", sell_qty, sell_px, &g.rules()).await {
                    Ok(sell_oid) => {
                        info!("🔲 [网格实盘] {} 第{}格买入成交 {:.6} @ {:.6}, 已挂卖单 @ {:.6}", g.symbol, i, exec_qty, g.lines[i], sell_px);
                        g.line_states[i] = Some(LineState::WaitSell { order_id: sell_oid, qty: sell_qty, buy_quote: quote });
                    }
                    Err(e) => {
                        error!("🔲 [网格实盘] {} 第{}格挂卖单失败: {} (下一轮重试)", g.symbol, i, e);
                        g.line_states[i] = Some(LineState::WaitSell { order_id: 0, qty: sell_qty, buy_quote: quote });
                    }
                }
            }
            (LineState::WaitSell { buy_quote, .. }, "FILLED") => {
                let net = quote - buy_quote;
                g.realized_total += net;
                g.round_trips += 1;
                let _ = tg_tx.send(format!(
                    "🔲 <b>【网格实盘回合完成】</b> {} 🟢\n第{}格: 买 {:.2}U → 卖 {:.2}U | 本格净: <b>{:+.3}U</b>\n📒 该网格累计: {:+.2}U ({} 回合)",
                    g.symbol, i, buy_quote, quote, net, g.realized_total, g.round_trips)).await;
                let buy_qty = g.per_grid_quote / g.lines[i];
                match spot.place_limit(&g.symbol, "BUY", buy_qty, g.lines[i], &g.rules()).await {
                    Ok(buy_oid) => g.line_states[i] = Some(LineState::WaitBuy { order_id: buy_oid }),
                    Err(e) => {
                        error!("🔲 [网格实盘] {} 第{}格重挂买单失败: {} (下一轮补挂)", g.symbol, i, e);
                        g.line_states[i] = None;
                    }
                }
            }
            (LineState::WaitBuy { .. }, "CANCELED") | (LineState::WaitBuy { .. }, "EXPIRED") | (LineState::WaitBuy { .. }, "REJECTED") => {
                // 买单被外力撤销; 部分成交的币转成待挂卖, 不能丢下变成无主库存
                info!("🔲 [网格实盘] {} 第{}格买单 #{} 状态 {}, 已成交 {:.6}", g.symbol, i, oid, status, exec_qty);
                if exec_qty > 0.0 {
                    g.line_states[i] = Some(LineState::WaitSell { order_id: 0, qty: exec_qty * 0.999, buy_quote: quote });
                } else {
                    g.line_states[i] = None;
                }
            }
            (LineState::WaitSell { qty, buy_quote, .. }, "CANCELED") | (LineState::WaitSell { qty, buy_quote, .. }, "EXPIRED") | (LineState::WaitSell { qty, buy_quote, .. }, "REJECTED") => {
                // 卖单被外力撤销 → 库存还在, 标记待重挂, 否则该格拿着死订单号无限空转
                info!("🔲 [网格实盘] {} 第{}格卖单 #{} 状态 {}, 下一轮重挂", g.symbol, i, oid, status);
                g.line_states[i] = Some(LineState::WaitSell { order_id: 0, qty: *qty, buy_quote: *buy_quote });
            }
            _ => {} // PARTIALLY_FILLED 等: 等下一轮
        }
    }
}

async fn init_grid(spot: &SpotClient, symbol: &str, budget: f64) -> Result<LiveGrid, String> {
    let rules = spot.symbol_rules(symbol).await?;
    let px = spot.last_price(symbol).await?;
    let usdt = spot.free_balance("USDT").await?;
    if usdt < budget {
        return Err(format!("现货 USDT 可用余额 {:.2} 不足该槽位预算 {:.0} (注意: 现货和合约钱包是分开的, 需要先划转)", usdt, budget));
    }
    // 格线数按预算动态推导: 每格名义必须 ≥ 交易所最小限制的 1.15 倍 (留价格波动余量)
    let n_buy = ((budget / (rules.min_notional * 1.15)) as usize).clamp(MIN_BUY_LINES, MAX_BUY_LINES);
    let per_grid = budget / n_buy as f64;
    if per_grid < rules.min_notional {
        return Err(format!("每格 {:.2}U 低于交易所最小名义 {:.2}U, 请加大预算或减少并发槽位 (/gridlive slots)", per_grid, rules.min_notional));
    }
    let low = px * (1.0 - RANGE_PCT / 100.0);
    let n = n_buy + 1;
    let lines: Vec<f64> = (0..n).map(|i| low + (px - low) * i as f64 / (n - 1) as f64).collect();

    let base_asset = symbol.trim_end_matches("USDT").to_string();
    let mut g = LiveGrid {
        symbol: symbol.to_string(),
        base_asset,
        lines,
        per_grid_quote: per_grid,
        line_states: vec![None; n],
        realized_total: 0.0,
        round_trips: 0,
        tick_size: rules.tick_size,
        step_size: rules.step_size,
        min_notional: rules.min_notional,
    };
    for i in 0..n_buy {
        let qty = g.per_grid_quote / g.lines[i];
        match spot.place_limit(symbol, "BUY", qty, g.lines[i], &rules).await {
            Ok(oid) => g.line_states[i] = Some(LineState::WaitBuy { order_id: oid }),
            Err(e) => {
                error!("🔲 [网格实盘] {} 初始第{}格买单失败: {}", symbol, i, e);
                // 回滚已成功的挂单, 快速失败并明确报告 (不留半开状态)
                for ls in &g.line_states {
                    if let Some(LineState::WaitBuy { order_id }) = ls {
                        let _ = spot.cancel_order(symbol, *order_id).await;
                    }
                }
                return Err(format!("第{}格挂单被币安拒绝: {}。已撤销部分成功的挂单。", i + 1, e));
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(120)).await;
    }
    Ok(g)
}
