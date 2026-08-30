#!/bin/bash
set -eo pipefail

###############################################################################
# bootstrap.sh
# 交互式脚本：安装 maxc-cli 并完成 OAuth-first 认证（公共云版本）
# 适用于阿里云公共 MaxCompute 服务（非弹内环境）
#
# 使用方法：
#   curl -fsSL <oss-url>/bootstrap.sh | bash
###############################################################################

# 颜色定义
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# 兼容 curl | bash 管道执行：将交互式 read 从终端 tty 读取，而非从管道 stdin
tty_read() {
    IFS= read -r "$@" < /dev/tty
}

echo -e "${CYAN}=============================================${NC}"
echo -e "${CYAN}  MaxC-CLI 安装与配置向导（公共云版）${NC}"
echo -e "${CYAN}=============================================${NC}"
echo ""

###############################################################################
# 步骤 1: 安装 maxc-cli
###############################################################################
echo -e "${GREEN}步骤 1/3: 安装 maxc-cli${NC}"
echo ""

# 选定一个 Python；后续所有 pip 和 user-site 查询都必须使用同一个解释器。
if command -v python3 &> /dev/null; then
    PYTHON_BIN="$(command -v python3)"
    PYTHON_VERSION=$("$PYTHON_BIN" --version | awk '{print $2}')
    echo -e "  检测到 Python 版本: ${YELLOW}${PYTHON_VERSION}${NC}"

    # 检查 Python 版本是否满足要求 (>= 3.9)
    PYTHON_MAJOR=$(echo "$PYTHON_VERSION" | cut -d. -f1)
    PYTHON_MINOR=$(echo "$PYTHON_VERSION" | cut -d. -f2)

    if [ "$PYTHON_MAJOR" -gt 3 ] || { [ "$PYTHON_MAJOR" -eq 3 ] && [ "$PYTHON_MINOR" -ge 9 ]; }; then
        echo -e "  ${GREEN}Python 版本满足要求 (>= 3.9)${NC}"
    else
        echo -e "  ${RED}Python 版本不满足要求，需要 >= 3.9${NC}"
        exit 1
    fi
else
    echo -e "  ${RED}未检测到 Python3，请先安装 Python >= 3.9${NC}"
    exit 1
fi

# 使用选定 Python 所属的 pip，避免裸 pip 指向另一个环境。
if ! "$PYTHON_BIN" -m pip --version &> /dev/null; then
    echo -e "  ${RED}未找到 pip，无法安装 maxc-cli${NC}"
    echo -e "  ${RED}请先安装 pip: ${PYTHON_BIN} -m ensurepip --upgrade${NC}"
    exit 1
fi
PIP_CMD=("$PYTHON_BIN" -m pip)

echo -e "  使用 pip 命令: ${CYAN}${PYTHON_BIN} -m pip${NC}"

# 检查 maxc-cli 是否已安装
if command -v maxc &> /dev/null; then
    # 提取纯净的版本号（去掉 "maxc " 前缀）
    CURRENT_VERSION=$(maxc --version 2>/dev/null | awk '{print $NF}' || echo "未知")
    echo -e "  ${YELLOW}maxc-cli 已安装 (版本: ${CURRENT_VERSION})${NC}"

    # 检查 PyPI 上的最新版本
    echo -e "  正在检查最新版本..."
    LATEST_VERSION=$("${PIP_CMD[@]}" index versions maxc-cli 2>/dev/null \
        | awk '/LATEST:/ {print $NF; exit}' || true)

    if [ -z "$LATEST_VERSION" ]; then
        # 备选方案：尝试用其他方式获取最新版本号
        LATEST_VERSION=$("${PIP_CMD[@]}" install maxc-cli --dry-run 2>&1 \
            | grep -o "maxc-cli-[0-9.]*" \
            | head -1 \
            | grep -o "[0-9.]*" || true)
    fi

    if [ -n "$LATEST_VERSION" ] && [ "$LATEST_VERSION" != "$CURRENT_VERSION" ]; then
        # 使用 sort -V 判断 LATEST_VERSION 是否真的比 CURRENT_VERSION 更高
        HIGHER_VERSION=$(printf '%s\n%s' "$LATEST_VERSION" "$CURRENT_VERSION" | sort -V | tail -1)
        if [ "$HIGHER_VERSION" = "$LATEST_VERSION" ]; then
            echo -e "  ${YELLOW}发现新版本: ${LATEST_VERSION} (当前: ${CURRENT_VERSION})${NC}"
            tty_read -p "  是否升级到最新版本？(y/N): " UPGRADE_MAXC
            if [[ "$UPGRADE_MAXC" =~ ^[Yy]$ ]]; then
                echo -e "  正在升级 maxc-cli..."
                "${PIP_CMD[@]}" install --upgrade maxc-cli
                echo -e "  ${GREEN}maxc-cli 升级成功！${NC}"
            else
                echo -e "  ${GREEN}保持当前版本${NC}"
            fi
        else
            echo -e "  ${GREEN}当前版本 (${CURRENT_VERSION}) 已高于 PyPI 最新稳定版 (${LATEST_VERSION})${NC}"
        fi
    elif [ -n "$LATEST_VERSION" ]; then
        echo -e "  ${GREEN}已是最新版本 (v${LATEST_VERSION})${NC}"
    else
        echo -e "  ${YELLOW}无法获取最新版本信息${NC}"
        tty_read -p "  是否尝试升级？(y/N): " UPGRADE_MAXC
        if [[ "$UPGRADE_MAXC" =~ ^[Yy]$ ]]; then
            echo -e "  正在升级 maxc-cli..."
            "${PIP_CMD[@]}" install --upgrade maxc-cli
            echo -e "  ${GREEN}maxc-cli 升级完成！${NC}"
        fi
    fi
else
    echo -e "  正在安装 maxc-cli..."
    "${PIP_CMD[@]}" install maxc-cli
    echo -e "  ${GREEN}maxc-cli 安装成功！${NC}"
fi

# 确保 pip user-site 的 bin 目录在 PATH 中
# 当 pip 落到 user-site (PEP 668 / --user / 自动 fallback) 时，
# maxc 入口脚本会被装到一个默认不在 PATH 的目录
# Linux: ~/.local/bin    macOS: ~/Library/Python/3.x/bin
USER_BIN="$("$PYTHON_BIN" -m site --user-base 2>/dev/null)/bin"
if [ -d "$USER_BIN" ]; then
    case ":$PATH:" in
        *":$USER_BIN:"*) ;;
        *)
            export PATH="$USER_BIN:$PATH"
            echo -e "  ${CYAN}已临时将 ${USER_BIN} 加入 PATH${NC}"
            echo -e "  ${YELLOW}提示: 永久生效请将以下行加入 ~/.bashrc 或 ~/.zshrc:${NC}"
            echo -e "    ${YELLOW}export PATH=\"$USER_BIN:\$PATH\"${NC}"
            ;;
    esac
fi

echo ""
echo -e "  maxc-cli 版本:"
maxc --version 2>/dev/null || echo "  (无法获取版本信息)"
echo ""

###############################################################################
# 步骤 2: 配置并在线验证认证
###############################################################################
echo -e "${GREEN}步骤 2/3: 配置认证（OAuth 优先）${NC}"
echo ""

echo -e "  正在执行在线就绪检查..."
set +e
DOCTOR_OUTPUT=$(maxc agent doctor --online --json 2>&1)
DOCTOR_EXIT_CODE=$?
set -e

SKIP_AUTH=false
if [ "$DOCTOR_EXIT_CODE" -eq 0 ] && \
   echo "$DOCTOR_OUTPUT" | grep -q '"online_ready"[[:space:]]*:[[:space:]]*true'; then
    echo -e "  ${GREEN}当前认证和后端连接已在线验证${NC}"
    tty_read -p "  是否要重新配置认证？(y/N): " RECONFIGURE_AUTH
    if [[ ! "$RECONFIGURE_AUTH" =~ ^[Yy]$ ]]; then
        SKIP_AUTH=true
    fi
else
    echo -e "  ${YELLOW}当前身份尚未通过在线验证${NC}"
fi

if [[ "$SKIP_AUTH" != "true" ]]; then
    echo ""
    echo -e "  ${CYAN}请选择认证方式:${NC}"
    echo -e "  [1] OAuth 浏览器登录（推荐）"
    echo -e "  [2] 使用当前进程已注入的环境变量/STS"
    echo ""
    echo -e "  ${YELLOW}本脚本不会收集密钥，也不会把凭据放入命令参数。${NC}"
    tty_read -p "  请选择 (1-2): " AUTH_CHOICE

    case $AUTH_CHOICE in
        1)
            maxc auth login --oauth --json
            ;;
        2)
            REQUIRED_ENV_VARS=(
                "ALIBABA_CLOUD_ACCESS_KEY_ID"
                "ALIBABA_CLOUD_ACCESS_KEY_SECRET"
                "MAXCOMPUTE_PROJECT"
                "MAXCOMPUTE_ENDPOINT"
            )
            MISSING_ENV_VARS=()
            for var in "${REQUIRED_ENV_VARS[@]}"; do
                if [ -z "${!var}" ]; then
                    MISSING_ENV_VARS+=("$var")
                fi
            done
            if [ ${#MISSING_ENV_VARS[@]} -gt 0 ]; then
                echo -e "  ${RED}缺少环境变量: ${MISSING_ENV_VARS[*]}${NC}"
                echo -e "  ${YELLOW}请在调用脚本前由可信运行环境注入这些变量。${NC}"
                exit 1
            fi
            echo -e "  ${GREEN}已检测到所需环境变量（仅报告变量名，不显示值）${NC}"
            maxc auth login --from-env --json
            ;;
        *)
            echo -e "  ${RED}无效选择${NC}"
            exit 1
            ;;
    esac
fi

echo ""
echo -e "  正在通过 ${CYAN}agent doctor --online${NC} 验证身份和后端..."
set +e
DOCTOR_OUTPUT=$(maxc agent doctor --online --json 2>&1)
DOCTOR_EXIT_CODE=$?
set -e

if [ "$DOCTOR_EXIT_CODE" -ne 0 ] || \
   ! echo "$DOCTOR_OUTPUT" | grep -q '"online_ready"[[:space:]]*:[[:space:]]*true'; then
    echo -e "  ${RED}✗ 在线就绪检查未通过；不会把仅配置或验证失败视为成功。${NC}"
    echo "$DOCTOR_OUTPUT" | head -30 | while IFS= read -r line; do
        echo -e "    ${YELLOW}${line}${NC}"
    done
    echo -e "  ${YELLOW}请按 error.recovery_steps 或 agent_hints.next_actions 修复后重试。${NC}"
    exit 1
fi

echo -e "  ${GREEN}✓ 身份与 MaxCompute 后端已在线验证${NC}"
echo ""

###############################################################################
# 步骤 3: 安装 skill
###############################################################################
echo -e "${GREEN}步骤 3/3: 安装 Skill${NC}"
echo ""

echo -e "  ${CYAN}请选择要安装 skill 的平台:${NC}"
echo -e "  [1] Claude Code"
echo -e "  [2] Cursor"
echo -e "  [3] Windsurf"
echo -e "  [4] Codex"
echo -e "  [5] Qwen"
echo -e "  [6] Qoder"
echo -e "  [7] QoderWork"
echo -e "  [8] 跳过 skill 安装"
echo ""

tty_read -p "  请选择 (1-8): " SKILL_CHOICE

case $SKILL_CHOICE in
    1) PLATFORM="claude-code" ;;
    2) PLATFORM="cursor" ;;
    3) PLATFORM="windsurf" ;;
    4) PLATFORM="codex" ;;
    5) PLATFORM="qwen" ;;
    6) PLATFORM="qoder" ;;
    7) PLATFORM="qoderwork" ;;
    8)
        echo -e "  ${YELLOW}跳过 skill 安装${NC}"
        PLATFORM=""
        ;;
    *)
        echo -e "  ${RED}无效选择，跳过 skill 安装${NC}"
        PLATFORM=""
        ;;
esac

if [ -n "$PLATFORM" ]; then
    echo ""
    echo -e "  正在为 ${CYAN}${PLATFORM}${NC} 安装 skill..."
    maxc agent skill install "$PLATFORM" --invocation maxc --json

    echo ""
    echo -e "  ${GREEN}Skill 安装成功！${NC}"
    echo -e "  现在可以在 ${PLATFORM} 中使用 maxc-cli 相关的技能了"
fi

###############################################################################
# 完成
###############################################################################
echo ""
echo -e "${CYAN}=============================================${NC}"
echo -e "${GREEN}  安装与配置完成！${NC}"
echo -e "${CYAN}=============================================${NC}"
echo ""
echo -e "  常用命令:"
echo -e "    ${CYAN}maxc agent doctor --online --json${NC}  - 验证身份与后端"
echo -e "    ${CYAN}maxc auth whoami --json${NC}           - 查看当前身份"
echo -e "    ${CYAN}maxc meta list-tables --json${NC}      - 列出可用表"
echo -e "    ${CYAN}maxc query \"SELECT ...\" --json${NC}    - 执行查询"
echo -e "    ${CYAN}maxc cache build --json${NC}           - 构建元数据缓存"
echo ""
echo -e "  公共云常用 endpoint:"
echo -e "    ${CYAN}华东1(杭州): https://service.cn-hangzhou.maxcompute.aliyun.com/api${NC}"
echo -e "    ${CYAN}华东2(上海): https://service.cn-shanghai.maxcompute.aliyun.com/api${NC}"
echo -e "    ${CYAN}华北2(北京): https://service.cn-beijing.maxcompute.aliyun.com/api${NC}"
echo -e "    ${CYAN}华南1(深圳): https://service.cn-shenzhen.maxcompute.aliyun.com/api${NC}"
echo ""
echo -e "  更多信息请参阅:"
echo -e "    ${CYAN}maxc --help${NC}"
echo -e "    ${CYAN}maxc agent skill --json${NC}            - 查看 skill 信息"
echo ""
echo -e "${GREEN}祝使用愉快！${NC}"
