#!/bin/bash

# ==============================================================================
# Matrix Quant Engine 一键编译与部署脚本
# ==============================================================================

set -e # 遇到错误立即退出

# 配置项
APP_NAME="QuantitativeTrading"
SERVICE_NAME="matrix-quant.service"
DEPLOY_DIR="/opt/matrix-quant"
SYSTEMD_DIR="/etc/systemd/system"

# 部署目录所有权必须给 systemd 里声明的运行用户 (而不是执行部署的登录用户),
# 否则服务运行时写 feature_logs 等目录会 Permission denied
SERVICE_USER=$(sed -n 's/^User=//p' "$SERVICE_NAME" 2>/dev/null)
SERVICE_USER=${SERVICE_USER:-$USER}

echo "🚀 开始部署 Matrix Quant Engine..."

# 1. 检查环境
if ! command -v cargo &> /dev/null; then
    echo "❌ 错误: 未找到 Rust 编译环境 (cargo)。请先执行: curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh"
    exit 1
fi

# 2. 编译项目
echo "⚙️  正在执行极致优化编译 (Release 模式), 请耐心等待..."
cargo build --release

# 3. 部署二进制文件
echo "📂 正在配置部署目录: $DEPLOY_DIR"
sudo mkdir -p "$DEPLOY_DIR"

# ⚠️ 关键修复：先停止正在运行的服务，释放文件句柄，避免 Text file busy 错误
if systemctl is-active --quiet ${SERVICE_NAME%.*}; then
    echo "🛑 正在停止旧版本服务..."
    sudo systemctl stop ${SERVICE_NAME%.*}
fi

sudo cp "target/release/$APP_NAME" "$DEPLOY_DIR/"

# 将 .env 文件也拷贝过去，供程序读取
if [ -f ".env" ]; then
    echo "📄 发现 .env 配置文件, 正在同步到部署目录..."
    sudo cp .env "$DEPLOY_DIR/"
fi

# 特征落盘目录随部署建好, 避免服务运行时因目录缺失/无权限而丢数据
sudo mkdir -p "$DEPLOY_DIR/feature_logs"
sudo chown -R "$SERVICE_USER:$SERVICE_USER" "$DEPLOY_DIR"
sudo chmod +x "$DEPLOY_DIR/$APP_NAME"

# 3.5 部署 Python 研究管线 (research/) 并注册每日定时任务
RESEARCH_SERVICE="matrix-quant-research.service"
RESEARCH_TIMER="matrix-quant-research.timer"
if [ -d "research" ]; then
    echo "🐍 正在同步 Python 研究管线到 $DEPLOY_DIR/research ..."
    # 排除 venv 和已下载的 K 线数据; 不带 --delete, 服务器上已有的 data/ 增量数据不会被清掉
    sudo rsync -a --exclude='.venv' --exclude='data' --exclude='__pycache__' research/ "$DEPLOY_DIR/research/"
    sudo mkdir -p "$DEPLOY_DIR/research/data"
    sudo chmod +x "$DEPLOY_DIR/research/rerun.sh"

    # 确保 venv 存在且依赖装全 (仅 pandas/numpy, 其余为标准库)
    # 健康标准是 bin/pip 而非 bin/python: 系统缺 python3-venv 包时建出的
    # 残缺 venv 有 python 没 pip, 这种直接删掉重建
    if [ ! -x "$DEPLOY_DIR/research/.venv/bin/pip" ]; then
        echo "📦 venv 缺失或残缺 (无 pip), 正在重建..."
        sudo rm -rf "$DEPLOY_DIR/research/.venv"
        if ! sudo python3 -m venv "$DEPLOY_DIR/research/.venv"; then
            echo "❌ venv 创建失败, 请先执行: sudo apt install python3-venv"
            exit 1
        fi
    fi
    # pip install 幂等: 已装全时秒过, 不能只在建 venv 时装 (venv 可能存在但依赖不全)
    echo "📦 正在确保 Python 依赖 (pandas numpy)..."
    sudo "$DEPLOY_DIR/research/.venv/bin/pip" install -q pandas numpy
    # 研究管线同样以 $SERVICE_USER 运行 (见 matrix-quant-research.service)
    sudo chown -R "$SERVICE_USER:$SERVICE_USER" "$DEPLOY_DIR/research"

    if [ -f "$RESEARCH_SERVICE" ] && [ -f "$RESEARCH_TIMER" ]; then
        echo "⏰ 正在注册研究管线每日定时任务 (05:30)..."
        sudo cp "$RESEARCH_SERVICE" "$RESEARCH_TIMER" "$SYSTEMD_DIR/"
        sudo systemctl daemon-reload
        sudo systemctl enable --now "$RESEARCH_TIMER"
    fi
fi

# 4. 部署 Systemd 服务文件
if [ -f "$SERVICE_NAME" ]; then
    echo "📝 发现服务配置文件 $SERVICE_NAME, 正在注册到 Systemd..."
    
    # 提醒用户配置密钥
    if grep -q "your_binance_api_key" "$SERVICE_NAME"; then
        echo "⚠️  警告: 你似乎还没有在 $SERVICE_NAME 中填入真实的 API Key。"
        echo "   建议使用 Ctrl+C 中断部署，修改文件后再运行此脚本。"
        sleep 3
    fi

    sudo cp "$SERVICE_NAME" "$SYSTEMD_DIR/"
    sudo systemctl daemon-reload
    sudo systemctl enable ${SERVICE_NAME%.*} # 设置开机自启
else
    echo "❌ 错误: 未在当前目录找到 $SERVICE_NAME"
    exit 1
fi

# 5. 启动系统
echo "🔄 正在重启量化引擎服务..."
sudo systemctl restart ${SERVICE_NAME%.*}

# 6. 检查状态
sleep 2 # 等待两秒让程序跑起来
if sudo systemctl is-active --quiet ${SERVICE_NAME%.*}; then
    echo "✅ 部署成功！量化引擎已在后台极限运行。"
    echo "--------------------------------------------------------"
    echo "你可以使用以下命令查看实时引擎日志："
    echo "👉 sudo journalctl -fu ${SERVICE_NAME%.*}"
    echo "研究管线每日 05:30 自动跑, 查看排期/手动触发/看日志："
    echo "👉 systemctl list-timers ${RESEARCH_TIMER%.*}"
    echo "👉 sudo systemctl start ${RESEARCH_SERVICE%.*}"
    echo "👉 sudo journalctl -u ${RESEARCH_SERVICE%.*} -n 50"
    echo "--------------------------------------------------------"
    echo "请打开 Telegram，向你的机器人发送 /status 检查网络连通性。"
else
    echo "❌ 警告: 服务似乎启动失败。请查看最后几行错误日志："
    sudo journalctl -u ${SERVICE_NAME%.*} -n 15 --no-pager
fi
