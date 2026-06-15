# 🦀 Matrix Quant Engine (Rust)

**Matrix Quant Engine** 是一个基于 Rust 开发的机构级高频量化交易矩阵系统。它利用 Rust 极佳的内存安全与并发性能（Tokio 模型），能够无缝接入币安 (Binance) 数据源，并在毫秒级完成行情捕捉、策略运算与挂单执行。

本系统的核心投资哲学是：**“截断亏损，让利润奔跑 (Cut losses short, let profits run)”。**

---

## 🔥 核心特性 (Features)

- **⚡️ 极速并发矩阵 (Concurrency Matrix)**
  采用 `tokio::select!` 与无锁异步架构，单进程并发启动 N 个币种的微服务。每个币种（如 BTC, ETH, SOL, DOGE）拥有自己独立的行情网关与策略大脑，互不阻塞，独享高频事件循环。
  
- **📈 动量突破策略 (Breakout Momentum)**
  拒绝左侧接飞刀！内置极值突破追踪器，自动计算过去 N 个跳动的局部高低点。在多头力量撕裂前高时无情追涨，在空头砸穿支撑时顺势追空。

- **🛡 无止盈移动止损 (Trailing Stop-Loss)**
  彻底抛弃死板的“预期止盈价”。策略大脑跟踪每一次高低点，动态调整生命线（回撤百分比）。只要利润在奔跑，机器绝不下车；一旦市场反转跌破水位，毫秒级无情斩仓，锁定胜局。

- **🤖 Telegram 全局远程指挥中心**
  内嵌异步 Telegram Bot 进程，支持手机端实时人工干预。采用“本金+杠杆”机构级思考模式输入指令，大脑自动接管执行并挂载移动止损保护。

---

## 🏗 架构模块 (Architecture)

1. `gateway.rs`: **数据摄取层**。通过 `tokio-tungstenite` 挂载 Binance `@depth@100ms` 级长连接 WebSocket。
2. `orderbook.rs`: **高频内存账本**。基于 `BTreeMap` 实现的无锁线程安全本地盘口，计算实时微观中间价（Mid-price）。
3. `strategy.rs`: **策略大脑**。持仓状态机、极值监控与移动止损运算中心。
4. `execution.rs`: **交易执行层**。基于 `reqwest` 的高并发 HTTP 连接池，内嵌 HMAC SHA256 签名鉴权，负责秒级发单。
5. `telegram.rs`: **指挥控制层**。基于 `teloxide` 框架的交互面板，指令参数解析引擎。

---

## 🚀 部署与运行 (Quick Start)

### 1. 环境准备
确保你已经安装了 Rust 工具链 (建议 1.70+ 版本)：
```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

### 2. 配置环境变量
在运行前，请向你的运行环境（或 `.env` 文件）注入以下 Token 秘钥：

```bash
# 币安交易所 API 凭证
export BINANCE_API_KEY="your_binance_api_key_here"
export BINANCE_API_SECRET="your_binance_api_secret_here"

# Telegram 机器人 Token (向 @BotFather 申请)
export TELOXIDE_TOKEN="your_telegram_bot_token_here"
```

### 3. 调整订阅矩阵
在 `src/main.rs` 中，找到以下行，修改为你希望系统并发盯盘的币种集合：
```rust
let symbols = vec!["BTCUSDT", "BNBUSDT", "ETHUSDT", "DOGEUSDT"];
```

### 4. 启动系统
```bash
cargo run --release
```
> *注：强烈推荐使用 `--release` 模式运行，这将为你开启 Rust 的 LLVM 极致编译器优化，使决策延迟逼近微秒级。*

---

## 📱 Telegram 指挥手册

当你的系统启动后，在 Telegram 中打开你的机器人私聊界面，你可以发送以下最高权限指令：

| 指令语法 | 功能描述 | 示例 |
| :--- | :--- | :--- |
| `/status` | 检查交易所 API 连接状态与账户延迟。 | `/status` |
| `/buy <交易对> <USDT本金> <杠杆>` | 狙击做多：系统将按名义价值计算市价下单量，大脑随后无缝接管并挂载移动止损。 | `/buy ETHUSDT 500 10` |
| `/sell <交易对> <USDT本金> <杠杆>` | 狙击做空：逻辑同上，方向向下。 | `/sell DOGEUSDT 200 20` |
| `/panic <交易对>` | 紧急避险：无视当前移动止损水位，一键抹除大脑所有多空记忆和仓位。 | `/panic BTCUSDT` |

---

## ⚠️ 风险免责声明 (Disclaimer)

**本系统由原生代码生成，具备真实消耗资金与直接向交易所发送订单的能力。**
高频量化与杠杆衍生品交易风险极高，程序本身存在的任何 Bug、网络异常、交易所熔断等不可抗力皆可能导致您的本金全部损失。在将本套件接入主网实盘资金前，**请务必在 Binance Testnet（测试网）中运行数周以验证策略盈利能力与风控安全性**。

使用本代码即代表您同意自行承担所有交易结果，作者不对您的投资盈亏负责。
