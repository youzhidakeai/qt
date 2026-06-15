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
sudo cp "target/release/$APP_NAME" "$DEPLOY_DIR/"

# 将 .env 文件也拷贝过去，供程序读取
if [ -f ".env" ]; then
    echo "📄 发现 .env 配置文件, 正在同步到部署目录..."
    sudo cp .env "$DEPLOY_DIR/"
fi

sudo chown -R $USER:$USER "$DEPLOY_DIR" # 根据需要更改权限
sudo chmod +x "$DEPLOY_DIR/$APP_NAME"

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
    echo "--------------------------------------------------------"
    echo "请打开 Telegram，向你的机器人发送 /status 检查网络连通性。"
else
    echo "❌ 警告: 服务似乎启动失败。请查看最后几行错误日志："
    sudo journalctl -u ${SERVICE_NAME%.*} -n 15 --no-pager
fi
