#!/bin/bash
# ==============================================================================
# 网格实盘独立监听 (Watchdog) —— 独立于交易引擎进程运行
# 职责: 引擎内置的熔断/报告在引擎卡死时全部失效, 而真实挂单还留在交易所。
# 本脚本由 systemd timer 每 10 分钟拉起一次, 只在异常时报警:
#   1. 网格实盘开着, 但引擎服务不在运行 → 报警
#   2. 网格实盘开着, 但引擎超过 2 小时没发过小时报 (卡死/断连) → 报警
# 平时不发任何消息, 不制造噪音。
# ==============================================================================
set -u

ENV_FILE="/opt/matrix-quant/.env"
SERVICE="matrix-quant"

# 读取 TG 配置
TOKEN=$(grep -E '^TELOXIDE_TOKEN=' "$ENV_FILE" | cut -d= -f2- | tr -d '"' | tr -d "'")
CHAT_ID=$(grep -E '^TELEGRAM_CHAT_ID=' "$ENV_FILE" | cut -d= -f2- | tr -d '"' | tr -d "'")

alert() {
    curl -s -X POST "https://api.telegram.org/bot${TOKEN}/sendMessage" \
        -d chat_id="${CHAT_ID}" -d parse_mode=HTML \
        --data-urlencode text="$1" > /dev/null
}

ENABLED=$(redis-cli GET GRID_LIVE_ENABLED 2>/dev/null)
if [ "$ENABLED" != "1" ]; then
    exit 0  # 网格实盘没开, 无需监听
fi

# 检查 1: 引擎服务是否在运行
if ! systemctl is-active --quiet "$SERVICE"; then
    alert "🚨 <b>【独立监听报警】</b>
网格实盘处于开启状态, 但交易引擎服务 ($SERVICE) <b>不在运行</b>!
交易所里可能还留着真实挂单无人看管。
请立即: sudo systemctl start $SERVICE 或手动上币安撤单。"
    exit 0
fi

# 检查 2: 引擎是否卡死 (超过 2 小时没更新小时报标记)
LAST_HOUR=$(redis-cli GET GRID_LIVE_LAST_REPORT_HOUR 2>/dev/null)
if [ -n "$LAST_HOUR" ]; then
    # 格式 YYYY-MM-DD-HH (北京时间), 折算成epoch比对
    LAST_EPOCH=$(date -d "$(echo "$LAST_HOUR" | sed 's/-\([0-9][0-9]\)$/ \1:00/') CST" +%s 2>/dev/null || echo 0)
    NOW_EPOCH=$(date +%s)
    if [ "$LAST_EPOCH" -gt 0 ] && [ $((NOW_EPOCH - LAST_EPOCH)) -gt 7200 ]; then
        alert "🚨 <b>【独立监听报警】</b>
网格实盘开启中, 引擎服务在运行, 但已超过 2 小时没有输出小时报。
引擎可能卡死或与交易所断连, 请检查:
sudo journalctl -u $SERVICE -n 50"
    fi
fi
exit 0
