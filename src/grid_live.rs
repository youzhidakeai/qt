// ==========================================
// MODULE: 现货网格实盘执行器 (Grid Live Executor)
// ⚠️ 唯一会用真钱下现货单的模块。默认关闭 (GRID_LIVE_ENABLED=0), 部署后
// 必须用 Telegram /gridlive on <SYMBOL> 显式开启才会动一分钱。
//
// 机制: 以启动时价格为中心 ±RANGE_PCT 摆 N_LINES 条等距格线, 在低于现价的
// 格线挂限价买单; 某买单成交后, 在其上一条格线挂等量限价卖单; 卖单成交 =
// 完成一个回合(赚一格差价), 回到原格线重新挂买单。
// 出场纪律: 价格越过区间边界再 BUST_MARGIN_PCT% → 撤全部挂单+市价清算库存
// +自动停机, 不无限期扛单 (跟纸面版同一条规矩)。
//
// 资金安全: ① 预算上限 GRID_LIVE_BUDGET (默认 50U), 所有买单总名义不超过它
// ② 现货无杠杆, 最坏情况 = 预算全部变成币且币价下跌, 不存在强平/负债
// ③ 启动前检查现货 USDT 余额够不够、单格名义是否满足交易所最小限制
// ==========================================
use std::sync::Arc;
use serde::{Serialize, Deserialize};
use tokio::sync::mpsc;
use tracing::{info, error};

use crate::spot::{SpotClient, SpotSymbolRules};

const POLL_SECS: u64 = 20;
const RANGE_PCT: f64 = 10.0;   // 与纸面版同参数
const N_BUY_LINES: usize = 8;  // 现价下方的买入格线数; 50U 预算 → 每格 6.25U (> 币安最小 5U)
const BUST_MARGIN_PCT: f64 = 5.0;
// 盈利监听熔断: 总盈亏(已实现+库存浮动) 亏到预算的这个比例 → 清算+停机, 判定模型不盈利。
// 熔断后不自动轮换下一个币——模型级失败要人来复盘, 不是换个标的继续流血。
const MAX_LOSS_PCT_OF_BUDGET: f64 = 20.0;
const AUTO_COOLDOWN_SECS: u64 = 6 * 3600; // 自动模式下, 失效(越界)的币冷却 6 小时不再选

#[derive(Serialize, Deserialize, Clone, PartialEq)]
enum LineState {
    WaitBuy { order_id: u64 },
    WaitSell { order_id: u64, qty: f64, buy_quote: f64 },
}

#[derive(Serialize, Deserialize, Clone)]
struct LiveGrid {
    symbol: String,
    base_asset: String,
    lines: Vec<f64>,          // N_BUY_LINES+1 条: 索引 0..N_BUY_LINES-1 是买入线, i 的卖出目标是 lines[i+1]
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
    fn held_qty(&self) -> f64 {
        self.line_states.iter().flatten().map(|s| if let LineState::WaitSell { qty, .. } = s { *qty } else { 0.0 }).sum()
    }
    // 库存的买入成本合计 (报价资产)
    fn inventory_cost(&self) -> f64 {
        self.line_states.iter().flatten().map(|s| if let LineState::WaitSell { buy_quote, .. } = s { *buy_quote } else { 0.0 }).sum()
    }
    // 总盈亏 = 已实现 + 库存按现价的浮动盈亏 —— 盈利监听的核心读数
    fn total_pnl(&self, px: f64) -> f64 {
        self.realized_total + self.held_qty() * px - self.inventory_cost()
    }
    fn open_order_ids(&self) -> Vec<u64> {
        self.line_states.iter().flatten().map(|s| match s {
            LineState::WaitBuy { order_id } => *order_id,
            LineState::WaitSell { order_id, .. } => *order_id,
        }).collect()
    }
}

async fn load_state(con: &mut redis::aio::MultiplexedConnection) -> Option<LiveGrid> {
    redis::cmd("GET").arg("GRID_LIVE_STATE").query_async::<Option<String>>(con).await.ok().flatten()
        .and_then(|s| serde_json::from_str(&s).ok())
}

async fn save_state(con: &mut redis::aio::MultiplexedConnection, g: &LiveGrid) {
    if let Ok(j) = serde_json::to_string(g) {
        let _: () = redis::cmd("SET").arg("GRID_LIVE_STATE").arg(j).query_async(con).await.unwrap_or(());
    }
}

async fn read_flag(con: &mut redis::aio::MultiplexedConnection, key: &str, default: &str) -> String {
    redis::cmd("GET").arg(key).query_async::<Option<String>>(con).await.ok().flatten().unwrap_or_else(|| default.to_string())
}

// 撤掉全部在场挂单 (逐个撤, 失败的记日志但继续)
async fn cancel_all(spot: &SpotClient, g: &LiveGrid) {
    for oid in g.open_order_ids() {
        if let Err(e) = spot.cancel_order(&g.symbol, oid).await {
            error!("🔲 [网格实盘] 撤单 #{} 失败 (可能已成交/已撤): {}", oid, e);
        }
    }
}

// 市价清算全部已持有库存, 返回收回的 USDT。
// 卖出量取 min(账面库存, 实际可用余额): 手续费从币里扣过, 账面量可能略大于
// 真实余额, 按账面卖会整单被拒 —— 清算失败比少卖一点严重得多。
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
            error!("🔲 [网格实盘] 清算库存失败: {}", e);
            0.0
        }
    }
}

pub async fn run_grid_live(spot: Arc<SpotClient>, redis_client: redis::Client, tg_tx: mpsc::Sender<String>) {
    let report_offset = time::UtcOffset::from_hms(8, 0, 0).unwrap();
    info!("🔲 网格实盘执行器已加载 (默认关闭, /gridlive on 显式开启后才会下真实订单)");

    loop {
        tokio::time::sleep(std::time::Duration::from_secs(POLL_SECS)).await;
        let mut con = match redis_client.get_multiplexed_async_connection().await {
            Ok(c) => c,
            Err(_) => continue,
        };
        let enabled = read_flag(&mut con, "GRID_LIVE_ENABLED", "0").await == "1";
        let liquidate_req = read_flag(&mut con, "GRID_LIVE_LIQUIDATE", "0").await == "1";
        let mut state = load_state(&mut con).await;

        // ---------- 清算指令: 撤单 + 市价卖光库存 + 停机 ----------
        if liquidate_req {
            let _: () = redis::cmd("SET").arg("GRID_LIVE_LIQUIDATE").arg("0").query_async(&mut con).await.unwrap_or(());
            if let Some(g) = &state {
                cancel_all(&spot, g).await;
                let quote = liquidate_inventory(&spot, g).await;
                let _ = tg_tx.send(format!(
                    "🔲 <b>【网格实盘已清算】</b> {}\n撤销全部挂单, 库存市价卖出收回 {:.2}U\n累计已实现: {:+.2}U ({} 个回合)\n执行器已停机。",
                    g.symbol, quote, g.realized_total, g.round_trips)).await;
                let _: () = redis::cmd("DEL").arg("GRID_LIVE_STATE").query_async(&mut con).await.unwrap_or(());
                let _: () = redis::cmd("SET").arg("GRID_LIVE_ENABLED").arg("0").query_async(&mut con).await.unwrap_or(());
            }
            continue;
        }

        if !enabled {
            // 关闭状态下若还有挂单 (例如刚被 /gridlive off), 撤掉挂单但保留库存与账本
            if let Some(g) = &state {
                if !g.open_order_ids().is_empty() {
                    cancel_all(&spot, g).await;
                    let mut g2 = g.clone();
                    for s in g2.line_states.iter_mut() {
                        if matches!(s, Some(LineState::WaitBuy { .. })) {
                            *s = None; // 买单已撤, 该线空闲
                        }
                        // WaitSell 保留记录: 库存还在手里, 但卖单已撤
                    }
                    save_state(&mut con, &g2).await;
                    let _ = tg_tx.send(format!(
                        "🔲 <b>【网格实盘已暂停】</b> {}\n全部挂单已撤销; 已持有库存 {:.6} {} 保留在账户里。\n恢复: /gridlive on {} | 卖光库存: /gridlive liquidate",
                        g2.symbol, g2.held_qty(), g2.base_asset, g2.symbol)).await;
                }
            }
            continue;
        }

        // ---------- 已开启 ----------
        let cfg_symbol = read_flag(&mut con, "GRID_LIVE_SYMBOL", "AUTO").await;
        let is_auto = cfg_symbol.is_empty() || cfg_symbol == "AUTO";
        let budget: f64 = read_flag(&mut con, "GRID_LIVE_BUDGET", "50").await.parse().unwrap_or(50.0);

        // 目标标的: 手动模式用指定币; 自动模式在无运行网格时从候选榜首取 (跳过冷却中的)
        let need_init = if is_auto {
            state.is_none()
        } else {
            state.as_ref().map(|g| g.symbol != cfg_symbol).unwrap_or(true)
        };
        let symbol = if !need_init {
            state.as_ref().map(|g| g.symbol.clone()).unwrap_or_default()
        } else if is_auto {
            let Some(cand_json) = redis::cmd("GET").arg("GRID_CANDIDATES").query_async::<Option<String>>(&mut con).await.ok().flatten() else { continue };
            let Ok(cands) = serde_json::from_str::<Vec<String>>(&cand_json) else { continue };
            let mut picked = String::new();
            for s in cands {
                let cooling: Option<String> = redis::cmd("GET").arg(format!("GRID_LIVE_COOLDOWN_{}", s)).query_async(&mut con).await.ok().flatten();
                if cooling.is_none() {
                    picked = s;
                    break;
                }
            }
            if picked.is_empty() {
                continue; // 候选全在冷却或榜单为空, 等下一轮
            }
            picked
        } else {
            cfg_symbol.clone()
        };

        // ---------- 初始化: 摆格线 + 挂初始买单 ----------
        if need_init {
            // 如果存在旧网格且标的不同，先清理旧网格遗留挂单
            if let Some(old_g) = &state {
                if old_g.symbol != symbol {
                    info!("🔲 [网格实盘] 切换标的 {} -> {}, 清理旧挂单", old_g.symbol, symbol);
                    cancel_all(&spot, old_g).await;
                    // Note: 不做市价清仓，只撤销挂单，旧标的现货作为投资保留，或手动卖出
                }
            }
            
            match init_grid(&spot, &symbol, budget).await {
                Ok(g) => {
                    let _ = tg_tx.send(format!(
                        "🔲 <b>【网格实盘启动】</b> {} (真实订单!)\n预算: {:.0}U | 每格: {:.2}U | 买入线: {} 条\n区间: {:.6} ~ {:.6}\n失效线: 越界再 {}% 自动清算停机\n随时可用 /gridlive off 暂停, /gridlive liquidate 清算离场",
                        g.symbol, budget, g.per_grid_quote, N_BUY_LINES,
                        g.lines[0], g.lines.last().unwrap(), BUST_MARGIN_PCT)).await;
                    save_state(&mut con, &g).await;
                    state = Some(g);
                }
                Err(e) => {
                    error!("🔲 [网格实盘] 初始化失败: {}", e);
                    let _ = tg_tx.send(format!("❌ <b>【网格实盘启动失败】</b> {}\n{}\n执行器已自动关闭, 修正后重新 /gridlive on", symbol, e)).await;
                    let _: () = redis::cmd("SET").arg("GRID_LIVE_ENABLED").arg("0").query_async(&mut con).await.unwrap_or(());
                    continue;
                }
            }
        }
        let Some(mut g) = state else { continue };

        // ---------- 盈利监听熔断: 总盈亏(已实现+库存浮动) 触及亏损上限 → 清算+彻底停机 ----------
        let Ok(px) = spot.last_price(&g.symbol).await else { continue };
        let total_pnl = g.total_pnl(px);
        let loss_limit = -budget * MAX_LOSS_PCT_OF_BUDGET / 100.0;
        if total_pnl <= loss_limit {
            cancel_all(&spot, &g).await;
            let quote = liquidate_inventory(&spot, &g).await;
            let _ = tg_tx.send(format!(
                "🛑 <b>【网格盈利监听熔断】</b> {}\n总盈亏 {:+.2}U 触及亏损上限 {:.0}U (预算的 {}%)\n已清算全部挂单与库存 (收回 {:.2}U), <b>执行器彻底停机</b>。\n模型判定为不盈利——需要人工复盘后再决定是否重启, 不会自动换币继续。",
                g.symbol, total_pnl, loss_limit, MAX_LOSS_PCT_OF_BUDGET, quote)).await;
            let _: () = redis::cmd("DEL").arg("GRID_LIVE_STATE").query_async(&mut con).await.unwrap_or(());
            let _: () = redis::cmd("SET").arg("GRID_LIVE_ENABLED").arg("0").query_async(&mut con).await.unwrap_or(());
            continue;
        }

        // ---------- 失效检查: 价格越界 → 清算; 自动模式冷却该币并轮换下一个, 手动模式停机 ----------
        let bust_low = g.lines[0] * (1.0 - BUST_MARGIN_PCT / 100.0);
        let bust_high = g.lines.last().unwrap() * (1.0 + BUST_MARGIN_PCT / 100.0);
        if px <= bust_low || px >= bust_high {
            cancel_all(&spot, &g).await;
            let quote = liquidate_inventory(&spot, &g).await;
            let dir = if px >= bust_high { "涨破区间上沿(踏空离场)" } else { "跌破区间下沿(止损清算)" };
            let next = if is_auto { "自动模式: 该币冷却 6 小时, 将从候选榜自动换下一个标的。" } else { "手动模式: 执行器已停机。" };
            let _ = tg_tx.send(format!(
                "🔲 <b>【网格实盘失效清算】</b> {} \n{} @ {:.6}\n库存市价卖出收回 {:.2}U | 累计已实现: {:+.2}U ({} 回合)\n{}",
                g.symbol, dir, px, quote, g.realized_total, g.round_trips, next)).await;
            let _: () = redis::cmd("DEL").arg("GRID_LIVE_STATE").query_async(&mut con).await.unwrap_or(());
            if is_auto {
                let _: () = redis::cmd("SET").arg(format!("GRID_LIVE_COOLDOWN_{}", g.symbol)).arg("1")
                    .arg("EX").arg(AUTO_COOLDOWN_SECS).query_async(&mut con).await.unwrap_or(());
            } else {
                let _: () = redis::cmd("SET").arg("GRID_LIVE_ENABLED").arg("0").query_async(&mut con).await.unwrap_or(());
            }
            continue;
        }

        // ---------- 巡逻: 检查每条线的订单状态并做状态转移 ----------
        let Ok(open_ids) = spot.open_order_ids(&g.symbol).await else { continue };
        let mut dirty = false;
        for i in 0..g.line_states.len() {
            let Some(ls) = g.line_states[i].clone() else {
                // 空闲线 (暂停恢复后的买入线): 补挂买单
                if i < N_BUY_LINES && g.lines[i] < px {
                    let qty = g.per_grid_quote / g.lines[i];
                    match spot.place_limit(&g.symbol, "BUY", qty, g.lines[i], &g.rules()).await {
                        Ok(oid) => { g.line_states[i] = Some(LineState::WaitBuy { order_id: oid }); dirty = true; }
                        Err(e) => error!("🔲 [网格实盘] 第{}格补挂买单失败: {}", i, e),
                    }
                }
                continue;
            };
            // 挂卖单失败的重试路径: order_id=0 是"有库存但没有卖单在场"的标记,
            // 必须在 get_order 之前处理 —— 拿 0 去查单永远报错, 否则该格库存会永久卡死
            if let LineState::WaitSell { order_id: 0, qty, buy_quote } = &ls {
                let sell_px = g.lines[i + 1];
                if let Ok(sell_oid) = spot.place_limit(&g.symbol, "SELL", *qty, sell_px, &g.rules()).await {
                    info!("🔲 [网格实盘] {} 第{}格补挂卖单成功 @ {:.6}", g.symbol, i, sell_px);
                    g.line_states[i] = Some(LineState::WaitSell { order_id: sell_oid, qty: *qty, buy_quote: *buy_quote });
                    dirty = true;
                }
                continue;
            }
            let oid = match &ls { LineState::WaitBuy { order_id } => *order_id, LineState::WaitSell { order_id, .. } => *order_id };
            if open_ids.contains(&oid) {
                continue; // 还挂着, 无事发生
            }
            // 订单不在挂单列表里了: 查明去向
            let Ok((status, exec_qty, quote)) = spot.get_order(&g.symbol, oid).await else { continue };
            match (&ls, status.as_str()) {
                (LineState::WaitBuy { .. }, "FILLED") => {
                    // 买入成交 → 上一格挂卖单。
                    // 现货买入手续费从收到的币里扣 (非BNB抵扣时), 按 99.9% 挂卖防余额不足被拒
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
                    dirty = true;
                }
                (LineState::WaitSell { qty, buy_quote, .. }, "FILLED") => {
                    // 卖出成交 → 回合完成, 记账, 回原格线重新挂买单
                    let net = quote - buy_quote; // 双边报价差 = 毛利 (手续费已从成交额中体现/以币收取, 保守近似)
                    g.realized_total += net;
                    g.round_trips += 1;
                    let _ = tg_tx.send(format!(
                        "🔲 <b>【网格实盘回合完成】</b> {} 🟢\n第{}格: 买 {:.2}U → 卖 {:.2}U | 本格净: <b>{:+.3}U</b>\n📒 累计: {:+.2}U ({} 回合)",
                        g.symbol, i, buy_quote, quote, net, g.realized_total, g.round_trips)).await;
                    let buy_qty = g.per_grid_quote / g.lines[i];
                    match spot.place_limit(&g.symbol, "BUY", buy_qty, g.lines[i], &g.rules()).await {
                        Ok(buy_oid) => g.line_states[i] = Some(LineState::WaitBuy { order_id: buy_oid }),
                        Err(e) => {
                            error!("🔲 [网格实盘] {} 第{}格重挂买单失败: {} (下一轮补挂)", g.symbol, i, e);
                            g.line_states[i] = None;
                        }
                    }
                    let _ = qty;
                    dirty = true;
                }
                (LineState::WaitBuy { .. }, "CANCELED") | (LineState::WaitBuy { .. }, "EXPIRED") | (LineState::WaitBuy { .. }, "REJECTED") => {
                    // 买单被外力撤销 (例如用户在App手动撤)。若已有部分成交, 那部分币
                    // 不能丢下不管 —— 转成"待挂卖"状态把它卖掉, 否则成为无主库存
                    info!("🔲 [网格实盘] {} 第{}格买单 #{} 状态 {}, 已成交 {:.6}", g.symbol, i, oid, status, exec_qty);
                    if exec_qty > 0.0 {
                        g.line_states[i] = Some(LineState::WaitSell { order_id: 0, qty: exec_qty * 0.999, buy_quote: quote });
                    } else {
                        g.line_states[i] = None; // 下一轮按空闲线重新挂买单
                    }
                    dirty = true;
                }
                (LineState::WaitSell { qty, buy_quote, .. }, "CANCELED") | (LineState::WaitSell { qty, buy_quote, .. }, "EXPIRED") | (LineState::WaitSell { qty, buy_quote, .. }, "REJECTED") => {
                    // 卖单被外力撤销 → 库存还在手里, 标记为"待挂卖"下一轮重挂,
                    // 否则该格会拿着死掉的订单号无限空转, 库存永远卖不出去
                    info!("🔲 [网格实盘] {} 第{}格卖单 #{} 状态 {}, 下一轮重挂", g.symbol, i, oid, status);
                    g.line_states[i] = Some(LineState::WaitSell { order_id: 0, qty: *qty, buy_quote: *buy_quote });
                    dirty = true;
                }
                _ => {} // PARTIALLY_FILLED 等: 等下一轮
            }
        }
        if dirty {
            save_state(&mut con, &g).await;
        }

        // ---------- 每小时盈利报告 (监听器的常规输出; 按小时去重防重启重发) ----------
        let now = time::OffsetDateTime::now_utc().to_offset(report_offset);
        let hour_bucket = format!("{:04}-{:02}-{:02}-{:02}", now.year(), now.month() as u8, now.day(), now.hour());
        let reported = read_flag(&mut con, "GRID_LIVE_LAST_REPORT_HOUR", "").await;
        if reported != hour_bucket {
            let _: () = redis::cmd("SET").arg("GRID_LIVE_LAST_REPORT_HOUR").arg(&hour_bucket).query_async(&mut con).await.unwrap_or(());
            let inv_cost = g.inventory_cost();
            let inv_val = g.held_qty() * px;
            let _ = tg_tx.send(format!(
                "🔲 <b>【网格实盘小时报】</b> {} {}:00\n\n已实现: <b>{:+.2}U</b> ({} 回合) | 库存: 成本 {:.2}U → 现值 {:.2}U ({:+.2}U)\n总盈亏: <b>{:+.2}U</b> / 预算 {:.0}U\n熔断线: {:.0}U (触及即自动清算停机)",
                g.symbol, now.hour(), g.realized_total, g.round_trips,
                inv_cost, inv_val, inv_val - inv_cost, total_pnl, budget, loss_limit)).await;
        }
    }
}

async fn init_grid(spot: &SpotClient, symbol: &str, budget: f64) -> Result<LiveGrid, String> {
    let rules = spot.symbol_rules(symbol).await?;
    let px = spot.last_price(symbol).await?;
    let usdt = spot.free_balance("USDT").await?;
    if usdt < budget {
        return Err(format!("现货 USDT 余额 {:.2} 不足预算 {:.0} (注意: 现货和合约钱包是分开的, 需要先划转)", usdt, budget));
    }
    let per_grid = budget / N_BUY_LINES as f64;
    if per_grid < rules.min_notional {
        return Err(format!("每格 {:.2}U 低于交易所最小名义 {:.2}U, 请加大预算或减少格数", per_grid, rules.min_notional));
    }
    // 格线: 现价下方 N_BUY_LINES 条买入线 + 每条的上方卖出目标线, 等距铺满 ±RANGE_PCT 的下半区
    // lines[i] (i<N_BUY_LINES) = 买入线, lines[i+1] = 对应卖出线
    let low = px * (1.0 - RANGE_PCT / 100.0);
    let n = N_BUY_LINES + 1;
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
    // 挂初始买单 (只挂现价下方的线)
    for i in 0..N_BUY_LINES {
        let qty = g.per_grid_quote / g.lines[i];
        match spot.place_limit(symbol, "BUY", qty, g.lines[i], &rules).await {
            Ok(oid) => g.line_states[i] = Some(LineState::WaitBuy { order_id: oid }),
            Err(e) => {
                error!("🔲 [网格实盘] 初始第{}格买单失败: {} (巡逻时补挂)", i, e);
                // Rollback successfully placed orders
                for ls in &g.line_states {
                    if let Some(LineState::WaitBuy { order_id }) = ls {
                        let _ = spot.cancel_order(symbol, *order_id).await;
                    }
                }
                return Err(format!("第{}格挂单被币安拒绝: {}。已撤销部分成功的挂单。", i+1, e));
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(120)).await;
    }
    Ok(g)
}
