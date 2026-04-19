#!/bin/bash
set -eo pipefail

###############################################################################
# bootstrap.sh
# 交互式脚本：安装 maxc-cli 并完成 AK/SK 认证（公共云版本）
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
    read "$@" < /dev/tty
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

# 检查 Python 版本
if command -v python3 &> /dev/null; then
    PYTHON_VERSION=$(python3 --version | awk '{print $2}')
    echo -e "  检测到 Python 版本: ${YELLOW}${PYTHON_VERSION}${NC}"

    # 检查 Python 版本是否在支持范围内 (3.8-3.12)
    PYTHON_MAJOR=$(echo $PYTHON_VERSION | cut -d. -f1)
    PYTHON_MINOR=$(echo $PYTHON_VERSION | cut -d. -f2)

    if [ "$PYTHON_MAJOR" -eq 3 ] && [ "$PYTHON_MINOR" -ge 8 ]; then
        echo -e "  ${GREEN}Python 版本满足要求 (>= 3.8)${NC}"
    else
        echo -e "  ${RED}Python 版本不满足要求，需要 >= 3.8${NC}"
        tty_read -p "  是否继续安装？(y/N): " CONTINUE_INSTALL
        if [[ ! "$CONTINUE_INSTALL" =~ ^[Yy]$ ]]; then
            echo -e "  ${YELLOW}安装已取消${NC}"
            exit 1
        fi
    fi
else
    echo -e "  ${RED}未检测到 Python3，请先安装 Python >= 3.8${NC}"
    exit 1
fi

# 尝试获取 pip 命令（在需要时用于安装/升级 maxc-cli）
if command -v pip3 &> /dev/null; then
    PIP_CMD="pip3"
elif command -v pip &> /dev/null; then
    PIP_CMD="pip"
elif python3 -m pip --version &> /dev/null; then
    PIP_CMD="python3 -m pip"
else
    echo -e "  ${RED}未找到 pip，无法安装 maxc-cli${NC}"
    echo -e "  ${RED}请先安装 pip: python3 -m ensurepip --upgrade${NC}"
    exit 1
fi

echo -e "  使用 pip 命令: ${CYAN}${PIP_CMD}${NC}"

# 检查 maxc-cli 是否已安装
if command -v maxc &> /dev/null; then
    # 提取纯净的版本号（去掉 "maxc " 前缀）
    CURRENT_VERSION=$(maxc --version 2>/dev/null | awk '{print $NF}' || echo "未知")
    echo -e "  ${YELLOW}maxc-cli 已安装 (版本: ${CURRENT_VERSION})${NC}"

    # 检查 PyPI 上的最新版本
    echo -e "  正在检查最新版本..."
    LATEST_VERSION=$($PIP_CMD index versions maxc-cli 2>/dev/null | grep "LATEST:" | awk '{print $NF}')

    if [ -z "$LATEST_VERSION" ]; then
        # 备选方案：尝试用其他方式获取最新版本号
        LATEST_VERSION=$($PIP_CMD install maxc-cli --dry-run 2>&1 | grep -o "maxc-cli-[0-9.]*" | head -1 | grep -o "[0-9.]*" || echo "")
    fi

    if [ -n "$LATEST_VERSION" ] && [ "$LATEST_VERSION" != "$CURRENT_VERSION" ]; then
        # 使用 sort -V 判断 LATEST_VERSION 是否真的比 CURRENT_VERSION 更高
        HIGHER_VERSION=$(printf '%s\n%s' "$LATEST_VERSION" "$CURRENT_VERSION" | sort -V | tail -1)
        if [ "$HIGHER_VERSION" = "$LATEST_VERSION" ]; then
            echo -e "  ${YELLOW}发现新版本: ${LATEST_VERSION} (当前: ${CURRENT_VERSION})${NC}"
            tty_read -p "  是否升级到最新版本？(y/N): " UPGRADE_MAXC
            if [[ "$UPGRADE_MAXC" =~ ^[Yy]$ ]]; then
                echo -e "  正在升级 maxc-cli..."
                $PIP_CMD install --upgrade maxc-cli
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
            $PIP_CMD install --upgrade maxc-cli
            echo -e "  ${GREEN}maxc-cli 升级完成！${NC}"
        fi
    fi
else
    echo -e "  正在安装 maxc-cli..."
    $PIP_CMD install maxc-cli
    echo -e "  ${GREEN}maxc-cli 安装成功！${NC}"
fi

echo ""
echo -e "  maxc-cli 版本:"
maxc --version 2>/dev/null || echo "  (无法获取版本信息)"
echo ""

###############################################################################
# 步骤 2: 配置认证
###############################################################################
echo -e "${GREEN}步骤 2/3: 配置认证（AK/SK）${NC}"
echo ""

# 检查当前认证状态
echo -e "  正在检查当前认证状态..."
AUTH_STATUS=$(maxc auth whoami --json 2>/dev/null || echo '{"data":{"identity":{"authenticated":false}}}')

# 使用简单的检查（不依赖 jq）
if echo "$AUTH_STATUS" | grep -q '"authenticated"[[:space:]]*:[[:space:]]*true'; then
    echo -e "  ${GREEN}当前已认证${NC}"
    echo ""

    # 显示当前身份
    PRINCIPAL=$(echo "$AUTH_STATUS" | grep -o '"principal_display"[[:space:]]*:[[:space:]]*"[^"]*"' | cut -d'"' -f4)
    PROJECT=$(echo "$AUTH_STATUS" | grep -o '"project"[[:space:]]*:[[:space:]]*"[^"]*"' | cut -d'"' -f4)
    AUTH_TYPE=$(echo "$AUTH_STATUS" | grep -o '"auth_type"[[:space:]]*:[[:space:]]*"[^"]*"' | cut -d'"' -f4)

    if [ -n "$PRINCIPAL" ]; then
        echo -e "  当前身份: ${CYAN}${PRINCIPAL}${NC}"
    fi
    if [ -n "$PROJECT" ]; then
        echo -e "  项目: ${CYAN}${PROJECT}${NC}"
    fi
    if [ -n "$AUTH_TYPE" ]; then
        echo -e "  认证方式: ${CYAN}${AUTH_TYPE}${NC}"
    fi
    echo ""

    tty_read -p "  是否要重新配置认证？(y/N): " RECONFIGURE_AUTH
    if [[ ! "$RECONFIGURE_AUTH" =~ ^[Yy]$ ]]; then
        echo -e "  ${GREEN}继续使用当前认证${NC}"
        SKIP_AUTH=true
    fi
else
    echo -e "  ${YELLOW}当前未认证或认证状态无效${NC}"
    SKIP_AUTH=false
fi

if [[ "$SKIP_AUTH" != "true" ]]; then
    echo ""

    # ===== 在认证前检查并清理环境变量 =====
    echo -e "  ${CYAN}检查可能覆盖配置的环境变量...${NC}"

    ENV_VARS_TO_CHECK=("ALIBABA_CLOUD_ACCESS_KEY_ID" "ALIBABA_CLOUD_ACCESS_KEY_SECRET"
                       "MAXCOMPUTE_PROJECT" "MAXCOMPUTE_ENDPOINT" "MAXCOMPUTE_REGION"
                       "ODPS_PROJECT" "ODPS_ENDPOINT" "ODPS_ACCESS_ID" "ODPS_ACCESS_KEY"
                       "ALIBABA_CLOUD_SECURITY_TOKEN" "ALIBABA_CLOUD_STS_TOKEN" "SECURITY_TOKEN")

    FOUND_CONFLICTING_VARS=()
    for var in "${ENV_VARS_TO_CHECK[@]}"; do
        if [ -n "${!var}" ]; then
            FOUND_CONFLICTING_VARS+=("$var")
        fi
    done

    if [ ${#FOUND_CONFLICTING_VARS[@]} -gt 0 ]; then
        echo -e "  ${YELLOW}发现以下环境变量已设置:${NC}"
        for var in "${FOUND_CONFLICTING_VARS[@]}"; do
            value="${!var}"
            # 隐藏敏感信息
            if [[ "$var" == *"SECRET"* ]] || [[ "$var" == *"KEY"* ]] || [[ "$var" == *"TOKEN"* ]]; then
                masked_value="${value:0:5}..."
            else
                masked_value="$value"
            fi
            echo -e "    ${YELLOW}${var}=${masked_value}${NC}"
        done
        echo ""
        echo -e "  ${RED}⚠ 这些环境变量将在运行时覆盖配置文件中的值${NC}"
        echo ""

        tty_read -p "  是否在脚本中忽略这些环境变量并继续？(Y/n): " UNSET_DONE
        if [[ "$UNSET_DONE" =~ ^[Nn]$ ]]; then
            echo -e "  ${YELLOW}请在当前 shell 中手动 unset 这些变量后重新运行本脚本${NC}"
            echo -e "  ${YELLOW}unset ${FOUND_CONFLICTING_VARS[*]}${NC}"
            exit 1
        fi
        echo -e "  ${GREEN}已在脚本内忽略冲突的环境变量，继续配置认证${NC}"
        for var in "${FOUND_CONFLICTING_VARS[@]}"; do
            unset "$var"
        done
    else
        echo -e "  ${GREEN}未发现冲突的环境变量${NC}"
    fi

    echo ""
    echo -e "  ${CYAN}请选择认证方式:${NC}"
    echo -e "  [1] 输入 Access Key / Secret Key（推荐）"
    echo -e "  [2] 使用环境变量（ALIBABA_CLOUD_ACCESS_KEY_ID 等）"
    echo ""

    tty_read -p "  请选择 (1-2): " AUTH_CHOICE

    case $AUTH_CHOICE in
        1)
            echo ""
            echo -e "  ${CYAN}请输入阿里云 Access Key 信息...${NC}"
            echo ""

            echo -e "  ${CYAN}Access Key ID:${NC}"
            tty_read ACCESS_KEY_ID

            echo -e "  ${CYAN}Access Key Secret:${NC}"
            tty_read -s ACCESS_KEY_SECRET
            echo ""

            echo -e "  ${CYAN}项目空间 (project):${NC}"
            tty_read PROJECT_NAME

            echo -e "  ${CYAN}Endpoint (可选，直接回车使用公共云默认值):${NC}"
            echo -e "  ${CYAN}常用 endpoint:${NC}"
            echo -e "    华东1(杭州): https://service.cn-hangzhou.maxcompute.aliyun.com/api"
            echo -e "    华东2(上海): https://service.cn-shanghai.maxcompute.aliyun.com/api"
            echo -e "    华北2(北京): https://service.cn-beijing.maxcompute.aliyun.com/api"
            echo -e "    华南1(深圳): https://service.cn-shenzhen.maxcompute.aliyun.com/api"
            tty_read ENDPOINT

            # 默认使用公共云杭州 endpoint
            if [ -z "$ENDPOINT" ]; then
                ENDPOINT="https://service.cn-hangzhou.maxcompute.aliyun.com/api"
                echo -e "  ${CYAN}使用默认 endpoint: ${ENDPOINT}${NC}"
            fi

            if [ -z "$ACCESS_KEY_ID" ] || [ -z "$ACCESS_KEY_SECRET" ] || [ -z "$PROJECT_NAME" ]; then
                echo -e "  ${RED}必填字段不能为空${NC}"
                exit 1
            fi

            # 使用 maxc auth login 配置
            echo ""
            echo -e "  正在配置认证信息..."
            maxc auth login --access-key-id "$ACCESS_KEY_ID" \
                --access-key-secret "$ACCESS_KEY_SECRET" \
                --project "$PROJECT_NAME" \
                --endpoint "$ENDPOINT"

            echo -e "  ${GREEN}AK/SK 认证配置完成！${NC}"
            ;;

        2)
            echo ""
            echo -e "  ${CYAN}使用环境变量进行认证...${NC}"
            echo ""

            # 检查是否已设置环境变量
            if [ -n "$ALIBABA_CLOUD_ACCESS_KEY_ID" ] && [ -n "$ALIBABA_CLOUD_ACCESS_KEY_SECRET" ]; then
                echo -e "  ${GREEN}检测到环境变量已设置:${NC}"
                echo -e "    ALIBABA_CLOUD_ACCESS_KEY_ID: ${CYAN}${ALIBABA_CLOUD_ACCESS_KEY_ID:0:5}...${NC}"
                echo ""

                tty_read -p "  是否使用当前环境变量进行认证？(Y/n): " USE_ENV
                if [[ "$USE_ENV" =~ ^[Nn]$ ]]; then
                    echo -e "  ${YELLOW}请手动设置环境变量后重新运行脚本${NC}"
                    exit 1
                fi
            else
                echo -e "  ${YELLOW}请先设置以下环境变量:${NC}"
                echo ""
                echo -e "    ${CYAN}export ALIBABA_CLOUD_ACCESS_KEY_ID=<your-access-key-id>${NC}"
                echo -e "    ${CYAN}export ALIBABA_CLOUD_ACCESS_KEY_SECRET=<your-access-key-secret>${NC}"
                echo -e "    ${CYAN}export MAXC_PROJECT=<your-project-name>${NC}"
                echo ""
                tty_read -p "  环境变量已设置？(y/N): " ENV_READY
                if [[ ! "$ENV_READY" =~ ^[Yy]$ ]]; then
                    echo -e "  ${YELLOW}请设置环境变量后重新运行脚本${NC}"
                    exit 1
                fi
            fi

            # 使用环境变量进行认证
            echo ""
            echo -e "  正在从环境变量读取认证信息..."
            maxc auth login --from-env

            echo -e "  ${GREEN}环境变量认证配置完成！${NC}"
            ;;

        *)
            echo -e "  ${RED}无效选择${NC}"
            exit 1
            ;;
    esac
fi

echo ""
echo -e "  ${GREEN}正在验证认证状态...${NC}"

# ===== 检查 session override 文件 =====
SESSION_OVERRIDE_FILE="$HOME/.maxc/session_override.yaml"

if [ -f "$SESSION_OVERRIDE_FILE" ]; then
    echo ""
    echo -e "  ${CYAN}发现 session override 文件: ${SESSION_OVERRIDE_FILE}${NC}"
    echo -e "  ${CYAN}内容:${NC}"
    while read -r line; do
        echo -e "    ${YELLOW}${line}${NC}"
    done < "$SESSION_OVERRIDE_FILE"
    echo ""
    echo -e "  ${RED}⚠ Session override 是最高优先级，将覆盖 config.yaml 中的配置${NC}"
    echo ""

    tty_read -p "  是否删除 session override 以使用 config.yaml 中的配置？(Y/n): " REMOVE_SESSION_OVERRIDE
    if [[ "$REMOVE_SESSION_OVERRIDE" =~ ^[Yy]$ ]] || [ -z "$REMOVE_SESSION_OVERRIDE" ]; then
        rm -f "$SESSION_OVERRIDE_FILE"
        echo -e "  ${GREEN}Session override 已删除${NC}"
    else
        echo -e "  ${YELLOW}保留 session override，它将覆盖 config.yaml 中的 project/endpoint 等配置${NC}"
    fi
fi

# ===== 检查环境变量覆盖 =====
echo ""
echo -e "  ${CYAN}检查环境变量覆盖情况...${NC}"

ENV_VARS=("ALIBABA_CLOUD_ACCESS_KEY_ID" "ALIBABA_CLOUD_ACCESS_KEY_SECRET" "MAXCOMPUTE_PROJECT"
          "MAXCOMPUTE_ENDPOINT" "MAXCOMPUTE_REGION" "ODPS_PROJECT" "ODPS_ENDPOINT"
          "ODPS_ACCESS_ID" "ODPS_ACCESS_KEY")

FOUND_ENV_VARS=()
for var in "${ENV_VARS[@]}"; do
    if [ -n "${!var}" ]; then
        FOUND_ENV_VARS+=("$var")
    fi
done

if [ ${#FOUND_ENV_VARS[@]} -gt 0 ]; then
    echo -e "  ${YELLOW}发现以下环境变量已设置，将覆盖配置文件中的值:${NC}"
    for var in "${FOUND_ENV_VARS[@]}"; do
        value="${!var}"
        if [[ "$var" == *"SECRET"* ]] || [[ "$var" == *"KEY"* ]]; then
            masked_value="${value:0:5}..."
        else
            masked_value="$value"
        fi
        echo -e "    ${YELLOW}${var}=${masked_value}${NC}"
    done
    echo ""
    echo -e "  ${RED}⚠ 这些环境变量将覆盖你刚才配置的 project/endpoint 等${NC}"
    echo ""

    tty_read -p "  是否取消这些环境变量以使用配置文件？(Y/n): " UNSET_ENVS
    if [[ "$UNSET_ENVS" =~ ^[Yy]$ ]] || [ -z "$UNSET_ENVS" ]; then
        for var in "${FOUND_ENV_VARS[@]}"; do
            echo -e "  取消设置: ${CYAN}${var}${NC}"
            unset "$var"
        done
        echo -e "  ${GREEN}环境变量已取消${NC}"
    else
        echo -e "  ${YELLOW}保留环境变量。请注意：当前 shell 中的环境变量将覆盖配置文件${NC}"
    fi
fi

# 获取详细的认证信息
WHOAMI_OUTPUT=$(maxc auth whoami --json 2>&1)
WHOAMI_EXIT_CODE=$?

echo ""
if [ $WHOAMI_EXIT_CODE -eq 0 ]; then
    echo -e "  ${CYAN}认证状态详情:${NC}"
    echo "$WHOAMI_OUTPUT" | head -20 | while IFS= read -r line; do
        echo -e "    ${YELLOW}${line}${NC}"
    done
    echo ""

    # 检查关键字段
    AUTHENTICATED=$(echo "$WHOAMI_OUTPUT" | grep -o '"authenticated"[[:space:]]*:[[:space:]]*[a-z]*' | head -1 | awk '{print $NF}')
    CONFIGURED=$(echo "$WHOAMI_OUTPUT" | grep -o '"configured"[[:space:]]*:[[:space:]]*[a-z]*' | head -1 | awk '{print $NF}')
    VALIDATION_STATUS=$(echo "$WHOAMI_OUTPUT" | grep -o '"validation_status"[[:space:]]*:[[:space:]]*"[^"]*"' | head -1 | cut -d'"' -f4)
    AUTH_TYPE=$(echo "$WHOAMI_OUTPUT" | grep -o '"auth_type"[[:space:]]*:[[:space:]]*"[^"]*"' | head -1 | cut -d'"' -f4)
    PROJECT=$(echo "$WHOAMI_OUTPUT" | grep -o '"project"[[:space:]]*:[[:space:]]*"[^"]*"' | head -1 | cut -d'"' -f4)
    ENDPOINT_WHOAMI=$(echo "$WHOAMI_OUTPUT" | grep -o '"endpoint"[[:space:]]*:[[:space:]]*"[^"]*"' | head -1 | cut -d'"' -f4)

    echo -e "  认证状态: ${CYAN}authenticated=${AUTHENTICATED}${NC}"
    echo -e "  配置状态: ${CYAN}configured=${CONFIGURED}${NC}"
    echo -e "  验证结果: ${CYAN}validation_status=${VALIDATION_STATUS}${NC}"
    echo -e "  认证类型: ${CYAN}auth_type=${AUTH_TYPE}${NC}"
    echo -e "  项目名称: ${CYAN}project=${PROJECT}${NC}"
    echo -e "  Endpoint: ${CYAN}endpoint=${ENDPOINT_WHOAMI}${NC}"
    echo ""

    if [[ "$AUTHENTICATED" == "true" ]] && [[ "$VALIDATION_STATUS" == "verified" ]]; then
        echo -e "  ${GREEN}✓ 认证成功！${NC}"
    elif [[ "$AUTHENTICATED" == "true" ]] && [[ "$VALIDATION_STATUS" == "configuration_only" ]]; then
        echo -e "  ${YELLOW}! 认证已配置但未进行远程验证${NC}"
        echo -e "  ${YELLOW}  这是正常的，可以继续使用${NC}"
        echo -e "  ${GREEN}✓ 视为认证成功${NC}"
    else
        echo -e "  ${RED}✗ 认证验证失败${NC}"
        echo ""
        echo -e "  ${RED}可能的原因和解决方案:${NC}"
        echo ""
        echo -e "  ${CYAN}1. 检查 AK/SK 是否正确${NC}"
        echo -e "  ${CYAN}2. 检查 endpoint 是否正确（公共云使用 https 格式）:${NC}"
        echo -e "     ${YELLOW}cat ~/.maxc/config.yaml${NC}"
        echo ""
        echo -e "  ${CYAN}3. 检查 session override 是否覆盖了配置:${NC}"
        echo -e "     ${YELLOW}cat ~/.maxc/session_override.yaml${NC}"
        echo ""
        echo -e "  ${CYAN}4. 重新配置认证:${NC}"
        echo -e "     ${YELLOW}maxc auth login --access-key-id <AK> --access-key-secret <SK> --project <project> --endpoint <endpoint>${NC}"
        echo ""

        tty_read -p "  是否继续后续步骤？(y/N): " CONTINUE_ANYWAY
        if [[ ! "$CONTINUE_ANYWAY" =~ ^[Yy]$ ]]; then
            exit 1
        fi
    fi
else
    echo -e "  ${RED}✗ 无法获取认证状态 (退出码: $WHOAMI_EXIT_CODE)${NC}"
    echo ""
    echo -e "  ${RED}错误输出:${NC}"
    echo "$WHOAMI_OUTPUT" | head -10 | while IFS= read -r line; do
        echo -e "    ${YELLOW}${line}${NC}"
    done
    echo ""

    tty_read -p "  是否继续后续步骤？(y/N): " CONTINUE_ANYWAY
    if [[ ! "$CONTINUE_ANYWAY" =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

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
    maxc agent install-skill "$PLATFORM" --json

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
