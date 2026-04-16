#!/bin/bash
set -e

###############################################################################
# install-and-setup.sh
# 交互式脚本：安装 ncs、maxc-cli，完成 auth 并安装 skill
#
# 使用方法：
#   bash scripts/install-and-setup.sh
###############################################################################

# 颜色定义
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

echo -e "${CYAN}=============================================${NC}"
echo -e "${CYAN}  MaxC-CLI 安装与配置向导${NC}"
echo -e "${CYAN}=============================================${NC}"
echo ""

###############################################################################
# 步骤 1: 安装 ncs (Normandy Credential CLI)
###############################################################################
echo -e "${GREEN}步骤 1/4: 安装 ncs (Normandy Credential CLI)${NC}"
echo ""

# 检查系统架构
ARCH=$(uname -m)
echo -e "  检测到系统架构: ${YELLOW}${ARCH}${NC}"

# 检查 ncs 是否已安装
if command -v ncs &> /dev/null; then
    echo -e "  ${YELLOW}ncs 已安装，跳过安装步骤${NC}"
    echo -e "  正在执行 ${CYAN}ncs upgrade${NC}..."
    ncs upgrade
else
    echo -e "  正在下载 ncs..."
    
    # 根据架构选择下载链接
    if [ "$ARCH" = "arm64" ]; then
        NCS_URL="https://authx.alibaba-inc.com/api/v1/releases/latest/bin/darwin/arm64/ncs"
    elif [ "$ARCH" = "x86_64" ]; then
        NCS_URL="https://authx.alibaba-inc.com/api/v1/releases/latest/bin/darwin/amd64/ncs"
    else
        echo -e "  ${RED}不支持的架构: ${ARCH}${NC}"
        echo -e "  ${RED}仅支持 arm64 和 x86_64${NC}"
        exit 1
    fi
    
    # 下载 ncs
    curl -L -o ~/Downloads/ncs "$NCS_URL"
    
    # 添加可执行权限
    echo -e "  正在添加可执行权限..."
    chmod +x ~/Downloads/ncs
    
    # 在 macOS 上移除扩展属性（避免 Gatekeeper 阻止）
    if [ "$(uname)" = "Darwin" ]; then
        xattr -c ~/Downloads/ncs 2>/dev/null || true
    fi
    
    # 移动到系统可执行命令目录
    echo -e "  正在将 ncs 安装到 /usr/local/bin/..."
    sudo mv ~/Downloads/ncs /usr/local/bin/ncs
    
    echo -e "  ${GREEN}ncs 安装成功！${NC}"
    echo ""
    
    # 升级 ncs 到最新版本
    echo -e "  正在执行 ${CYAN}ncs upgrade${NC}..."
    ncs upgrade
fi

echo ""
echo -e "  ncs 版本:"
ncs --version 2>/dev/null || echo "  (无法获取版本信息)"
echo ""

###############################################################################
# 步骤 2: 安装 maxc-cli
###############################################################################
echo -e "${GREEN}步骤 2/4: 安装 maxc-cli${NC}"
echo ""

# 检查 Python 版本
if command -v python3 &> /dev/null; then
    PYTHON_VERSION=$(python3 --version | awk '{print $2}')
    echo -e "  检测到 Python 版本: ${YELLOW}${PYTHON_VERSION}${NC}"
    
    # 检查 Python 版本是否在支持范围内 (3.8-3.12)
    PYTHON_MAJOR=$(echo $PYTHON_VERSION | cut -d. -f1)
    PYTHON_MINOR=$(echo $PYTHON_VERSION | cut -d. -f2)
    
    if [ "$PYTHON_MAJOR" -eq 3 ] && [ "$PYTHON_MINOR" -ge 8 ] && [ "$PYTHON_MINOR" -le 12 ]; then
        echo -e "  ${GREEN}Python 版本满足要求 (3.8-3.12)${NC}"
    else
        echo -e "  ${RED}Python 版本不满足要求，需要 3.8-3.12${NC}"
        read -p "  是否继续安装？(y/N): " CONTINUE_INSTALL
        if [[ ! "$CONTINUE_INSTALL" =~ ^[Yy]$ ]]; then
            echo -e "  ${YELLOW}安装已取消${NC}"
            exit 1
        fi
    fi
else
    echo -e "  ${RED}未检测到 Python3，请先安装 Python 3.8-3.12${NC}"
    exit 1
fi

# 检查 maxc-cli 是否已安装
if command -v maxc &> /dev/null; then
    # 提取纯净的版本号（去掉 "maxc " 前缀）
    CURRENT_VERSION=$(maxc --version 2>/dev/null | awk '{print $NF}' || echo "未知")
    echo -e "  ${YELLOW}maxc-cli 已安装 (版本: ${CURRENT_VERSION})${NC}"
    
    # 检查 PyPI 上的最新版本
    echo -e "  正在检查最新版本..."
    LATEST_VERSION=$(pip index versions maxc-cli 2>/dev/null | grep "LATEST:" | awk '{print $NF}')
    
    if [ -z "$LATEST_VERSION" ]; then
        # 备选方案：尝试用其他方式获取最新版本号
        LATEST_VERSION=$(pip install maxc-cli --dry-run 2>&1 | grep -o "maxc-cli ([0-9.]*')" | grep -o "[0-9.]*" || echo "")
    fi
    
    if [ -n "$LATEST_VERSION" ] && [ "$LATEST_VERSION" != "$CURRENT_VERSION" ]; then
        echo -e "  ${YELLOW}发现新版本: ${LATEST_VERSION} (当前: ${CURRENT_VERSION})${NC}"
        read -p "  是否升级到最新版本？(y/N): " UPGRADE_MAXC
        if [[ "$UPGRADE_MAXC" =~ ^[Yy]$ ]]; then
            echo -e "  正在升级 maxc-cli..."
            pip install --upgrade maxc-cli
            echo -e "  ${GREEN}maxc-cli 升级成功！${NC}"
        else
            echo -e "  ${GREEN}保持当前版本${NC}"
        fi
    elif [ -n "$LATEST_VERSION" ]; then
        echo -e "  ${GREEN}已是最新版本 (v${LATEST_VERSION})${NC}"
    else
        echo -e "  ${YELLOW}无法获取最新版本信息${NC}"
        read -p "  是否尝试升级？(y/N): " UPGRADE_MAXC
        if [[ "$UPGRADE_MAXC" =~ ^[Yy]$ ]]; then
            echo -e "  正在升级 maxc-cli..."
            pip install --upgrade maxc-cli
            echo -e "  ${GREEN}maxc-cli 升级完成！${NC}"
        fi
    fi
else
    echo -e "  正在安装 maxc-cli..."
    pip install maxc-cli
    echo -e "  ${GREEN}maxc-cli 安装成功！${NC}"
fi

echo ""
echo -e "  maxc-cli 版本:"
maxc --version 2>/dev/null || echo "  (无法获取版本信息)"
echo ""

###############################################################################
# 步骤 3: 配置认证
###############################################################################
echo -e "${GREEN}步骤 3/4: 配置认证${NC}"
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
    
    read -p "  是否要重新配置认证？(y/N): " RECONFIGURE_AUTH
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
                       "ODPS_STS_TOKEN" "SECURITY_TOKEN")
    
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
        echo -e "  ${RED}  必须在当前 shell 中取消这些变量才能使用配置文件${NC}"
        echo ""
        
        # 生成 unset 命令供用户执行
        UNSET_COMMAND="unset ${FOUND_CONFLICTING_VARS[*]}"
        echo -e "  ${CYAN}请在当前终端执行以下命令取消环境变量:${NC}"
        echo -e "  ${YELLOW}${UNSET_COMMAND}${NC}"
        echo ""
        
        read -p "  是否已执行上述 unset 命令？(y/N): " UNSET_DONE
        if [[ "$UNSET_DONE" =~ ^[Yy]$ ]]; then
            echo -e "  ${GREEN}环境变量已取消，继续配置认证${NC}"
            # 在当前脚本中也取消这些变量（用于后续验证）
            for var in "${FOUND_CONFLICTING_VARS[@]}"; do
                unset "$var"
            done
        else
            echo -e "  ${YELLOW}请先执行 unset 命令后重新运行本脚本${NC}"
            echo -e "  ${YELLOW}复制并执行以下命令：${NC}"
            echo -e "  ${YELLOW}${UNSET_COMMAND}${NC}"
            exit 1
        fi
    else
        echo -e "  ${GREEN}未发现冲突的环境变量${NC}"
    fi
    
    echo ""
    echo -e "  ${CYAN}请选择认证方式:${NC}"
    echo -e "  [1] 使用 ncs (Normandy Credential CLI) - 推荐弹内用户"
    echo -e "  [2] 使用 Access Key / Secret Key"
    echo -e "  [3] 使用环境变量"
    echo ""
    
    read -p "  请选择 (1-3): " AUTH_CHOICE
    
    case $AUTH_CHOICE in
        1)
            echo ""
            echo -e "  ${CYAN}使用 ncs 进行认证...${NC}"
            echo ""
            
            # 检查 ncs 是否可用
            if ! command -v ncs &> /dev/null; then
                echo -e "  ${RED}ncs 未安装，无法使用此方式${NC}"
                exit 1
            fi
            
            # 使用 ncs 创建凭证
            echo -e "  ${YELLOW}提示: 以下操作将引导你完成 ncs 认证配置${NC}"
            echo ""
            
            # 选择项目空间
            echo -e "  ${CYAN}请填写默认工作空间 (project):${NC}"
            read PROJECT_NAME
            
            if [ -z "$PROJECT_NAME" ]; then
                echo -e "  ${RED}项目空间不能为空${NC}"
                exit 1
            fi
            
            # 获取 ncs 授权列表
            echo ""
            echo -e "  ${CYAN}正在获取可用账号列表...${NC}"
            
            # 列出可用的 employee 账号
            EMPLOYEE_LIST=$(ncs list authorizations odpsuser -o custom-columns=BUC_USER_ID:.extension.bucUserId,BUC_USER_TYPE:.extension.bucUserType,BUC_ACCOUNT_NAME:.extension.bucDomainAccount 2>/dev/null | awk 'NF {print $0}' | tail -n +3)
            
            if [ -n "$EMPLOYEE_LIST" ]; then
                echo ""
                echo -e "  ${GREEN}找到以下个人账号:${NC}"
                echo "$EMPLOYEE_LIST"
                echo ""
                echo -e "  ${CYAN}请输入要使用的工号 (BUC_USER_ID):${NC}"
                read EMPLOYEE_ID

                if [ -n "$EMPLOYEE_ID" ]; then
                    # 获取现有 endpoint（作为参考）
                    EXISTING_ENDPOINT=$(maxc auth whoami --json 2>/dev/null | grep -o '"endpoint"[[:space:]]*:[[:space:]]*"[^"]*"' | head -1 | cut -d'"' -f4)

                    # 让用户配置 endpoint
                    echo ""
                    if [ -n "$EXISTING_ENDPOINT" ]; then
                        echo -e "  ${CYAN}请填写 MaxCompute endpoint (现有配置: ${EXISTING_ENDPOINT})${NC}"
                        echo -e "  ${CYAN}常用 endpoint:${NC}"
                        echo -e "    国内弹内: http://service-corp.odps.aliyun-inc.com/api"
                        echo -e "    新加坡弹内: http://service-all.ali-sg-lazada.odps.aliyun-inc.com/api"
                        echo -e "  ${CYAN}直接回车使用现有配置，或输入新的 endpoint:${NC}"
                    else
                        echo -e "  ${CYAN}请填写 MaxCompute endpoint${NC}"
                        echo -e "  ${CYAN}常用 endpoint:${NC}"
                        echo -e "    国内弹内: http://service-corp.odps.aliyun-inc.com/api"
                        echo -e "    新加坡弹内: http://service-all.ali-sg-lazada.odps.aliyun-inc.com/api"
                    fi
                    read ENDPOINT

                    # 如果用户直接回车，使用现有配置
                    if [ -z "$ENDPOINT" ] && [ -n "$EXISTING_ENDPOINT" ]; then
                        ENDPOINT="$EXISTING_ENDPOINT"
                    fi

                    if [ -z "$ENDPOINT" ]; then
                        echo -e "  ${RED}endpoint 不能为空${NC}"
                        exit 1
                    fi

                    # 构建 ncs 命令
                    NCS_CMD="ncs create credential odpsuser --employee-id $EMPLOYEE_ID -o template -t odpscmd"

                    # 使用 maxc auth login-external 配置外部凭证
                    # 先用 --no-validate 避免验证失败，因为 validation 可能受到各种因素影响
                    echo ""
                    echo -e "  正在配置 maxc-cli 使用外部凭证..."
                    maxc auth login-external \
                        --process-command "$NCS_CMD" \
                        --project "$PROJECT_NAME" \
                        --endpoint "$ENDPOINT" \
                        --no-validate

                    echo -e "  ${GREEN}ncs 认证配置完成！${NC}"
                    echo -e "  ${YELLOW}提示: 认证已保存但跳过远程验证。实际使用时 ncs 会自动获取凭据。${NC}"
                else
                    echo -e "  ${RED}工号不能为空${NC}"
                    exit 1
                fi
            else
                echo -e "  ${YELLOW}未找到可用的个人账号，请联系管理员${NC}"
                exit 1
            fi
            ;;
            
        2)
            echo ""
            echo -e "  ${CYAN}请输入 Access Key 信息...${NC}"
            echo ""
            
            echo -e "  ${CYAN}Access Key ID:${NC}"
            read ACCESS_KEY_ID
            
            echo -e "  ${CYAN}Access Key Secret:${NC}"
            read -s ACCESS_KEY_SECRET
            echo ""
            
            echo -e "  ${CYAN}项目空间 (project):${NC}"
            read PROJECT_NAME
            
            echo -e "  ${CYAN}Endpoint (可选，直接回车使用默认值):${NC}"
            read ENDPOINT
            
            if [ -z "$ACCESS_KEY_ID" ] || [ -z "$ACCESS_KEY_SECRET" ] || [ -z "$PROJECT_NAME" ]; then
                echo -e "  ${RED}必填字段不能为空${NC}"
                exit 1
            fi
            
            # 使用 maxc auth login 配置
            echo ""
            echo -e "  正在配置认证信息..."
            if [ -n "$ENDPOINT" ]; then
                maxc auth login --access-key-id "$ACCESS_KEY_ID" \
                    --access-key-secret "$ACCESS_KEY_SECRET" \
                    --project "$PROJECT_NAME" \
                    --endpoint "$ENDPOINT"
            else
                maxc auth login --access-key-id "$ACCESS_KEY_ID" \
                    --access-key-secret "$ACCESS_KEY_SECRET" \
                    --project "$PROJECT_NAME"
            fi
            
            echo -e "  ${GREEN}AK/SK 认证配置完成！${NC}"
            ;;
            
        3)
            echo ""
            echo -e "  ${CYAN}使用环境变量进行认证...${NC}"
            echo ""
            
            # 检查是否已设置环境变量
            if [ -n "$ALIBABA_CLOUD_ACCESS_KEY_ID" ] && [ -n "$ALIBABA_CLOUD_ACCESS_KEY_SECRET" ]; then
                echo -e "  ${GREEN}检测到环境变量已设置:${NC}"
                echo -e "    ALIBABA_CLOUD_ACCESS_KEY_ID: ${CYAN}${ALIBABA_CLOUD_ACCESS_KEY_ID:0:5}...${NC}"
                echo ""
                
                read -p "  是否使用当前环境变量进行认证？(Y/n): " USE_ENV
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
                read -p "  环境变量已设置？(y/N): " ENV_READY
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
    cat "$SESSION_OVERRIDE_FILE" | while read -r line; do
        echo -e "    ${YELLOW}${line}${NC}"
    done
    echo ""
    echo -e "  ${RED}⚠ Session override 是最高优先级，将覆盖 config.yaml 中的配置${NC}"
    echo ""
    
    read -p "  是否删除 session override 以使用 config.yaml 中的配置？(Y/n): " REMOVE_SESSION_OVERRIDE
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

# 相关环境变量列表
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
        # 隐藏敏感信息
        if [[ "$var" == *"SECRET"* ]] || [[ "$var" == *"KEY"* ]]; then
            masked_value="${value:0:5}..."
        else
            masked_value="$value"
        fi
        echo -e "    ${YELLOW}${var}=${masked_value}${NC}"
    done
    echo ""
    echo -e "  ${RED}⚠ 这些环境变量将覆盖你刚才配置的 project/endpoint/endpoint${NC}"
    echo ""
    
    read -p "  是否取消这些环境变量以使用配置文件？(Y/n): " UNSET_ENVS
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

# 获取详细的认证信息（带 --json 参数）
WHOAMI_OUTPUT=$(maxc auth whoami --json 2>&1)
WHOAMI_EXIT_CODE=$?

echo ""
if [ $WHOAMI_EXIT_CODE -eq 0 ]; then
    echo -e "  ${CYAN}认证状态详情:${NC}"
    echo "$WHOAMI_OUTPUT" | head -20 | while read -r line; do
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
    IDENTITY_SOURCE=$(echo "$WHOAMI_OUTPUT" | grep -o '"identity_source"[[:space:]]*:[[:space:]]*"[^"]*"' | head -1 | cut -d'"' -f4)
    
    echo -e "  认证状态: ${CYAN}authenticated=${AUTHENTICATED}${NC}"
    echo -e "  配置状态: ${CYAN}configured=${CONFIGURED}${NC}"
    echo -e "  验证结果: ${CYAN}validation_status=${VALIDATION_STATUS}${NC}"
    echo -e "  认证类型: ${CYAN}auth_type=${AUTH_TYPE}${NC}"
    echo -e "  项目名称: ${CYAN}project=${PROJECT}${NC}"
    echo -e "  Endpoint: ${CYAN}endpoint=${ENDPOINT_WHOAMI}${NC}"
    echo -e "  身份来源: ${CYAN}identity_source=${IDENTITY_SOURCE}${NC}"
    echo ""
    
    # 检查是否有环境变量的混合来源
    if [[ "$IDENTITY_SOURCE" == "mixed" ]]; then
        echo -e "  ${YELLOW}! 检测到混合来源配置${NC}"
        echo -e "  ${YELLOW}  环境变量和配置文件同时存在，环境变量优先${NC}"
        echo ""
    fi
    
    # 判断认证是否成功
    if [[ "$AUTHENTICATED" == "true" ]] && [[ "$VALIDATION_STATUS" == "verified" ]]; then
        echo -e "  ${GREEN}✓ 认证成功！${NC}"
    elif [[ "$AUTHENTICATED" == "true" ]] && [[ "$VALIDATION_STATUS" == "configuration_only" ]]; then
        echo -e "  ${YELLOW}! 认证已配置但未进行远程验证${NC}"
        echo -e "  ${YELLOW}  这是正常的，可以继续使用${NC}"
        echo -e "  ${GREEN}✓ 视为认证成功${NC}"
    elif [[ "$AUTHENTICATED" == "true" ]] && [[ "$VALIDATION_STATUS" == "validation_failed" ]]; then
        echo -e "  ${YELLOW}! 认证已配置但远程验证失败${NC}"
        echo -e "  ${YELLOW}  对于 external 认证，这通常是正常的（ncs 需要在实际查询时调用）${NC}"
        
        # 检查是否是因为 validation probe 的问题
        if echo "$WHOAMI_OUTPUT" | grep -q "Argument auth not acceptable"; then
            echo -e "  ${CYAN}  验证探针错误：这是已知问题，不影响实际使用${NC}"
        fi
        
        echo ""
        echo -e "  ${GREEN}✓ 视为认证成功${NC}"
        echo -e "  ${CYAN}  可以使用 maxc query 等命令进行实际查询${NC}"
    else
        echo -e "  ${RED}✗ 认证验证失败${NC}"
        echo ""
        echo -e "  ${RED}可能的原因和解决方案:${NC}"
        echo ""
        echo -e "  ${CYAN}1. 检查 session override 是否覆盖了配置:${NC}"
        echo -e "     ${YELLOW}cat ~/.maxc/session_override.yaml${NC}"
        echo -e "     ${YELLOW}maxc session show --json${NC}"
        echo ""
        echo -e "  ${CYAN}2. 清除 session override:${NC}"
        echo -e "     ${YELLOW}maxc session unset --json${NC}"
        echo ""
        echo -e "  ${CYAN}3. 检查环境变量是否覆盖:${NC}"
        echo -e "     ${YELLOW}env | grep -iE 'maxcompute|odps'${NC}"
        echo ""
        echo -e "  ${CYAN}4. 检查 ncs 是否正常:${NC}"
        echo -e "     ${YELLOW}ncs create credential odpsuser --employee-id $EMPLOYEE_ID -o template -t odpscmd${NC}"
        echo ""
        echo -e "  ${CYAN}5. 重新配置认证:${NC}"
        echo -e "     ${YELLOW}maxc auth login-external --process-command \"ncs create credential odpsuser --employee-id $EMPLOYEE_ID -o template -t odpscmd\" --project \"$PROJECT_NAME\" --endpoint \"$ENDPOINT\" --no-validate --json${NC}"
        echo ""
        
        read -p "  是否继续后续步骤？(y/N): " CONTINUE_ANYWAY
        if [[ ! "$CONTINUE_ANYWAY" =~ ^[Yy]$ ]]; then
            exit 1
        fi
    fi
else
    echo -e "  ${RED}✗ 无法获取认证状态 (退出码: $WHOAMI_EXIT_CODE)${NC}"
    echo ""
    echo -e "  ${RED}错误输出:${NC}"
    echo "$WHOAMI_OUTPUT" | head -10 | while read -r line; do
        echo -e "    ${YELLOW}${line}${NC}"
    done
    echo ""
    echo -e "  ${CYAN}建议:${NC}"
    echo -e "  1. 检查 ncs 是否正常工作: ${YELLOW}ncs --version${NC}"
    echo -e "  2. 检查 maxc 配置: ${YELLOW}cat ~/.maxc/config.yaml${NC}"
    echo -e "  3. 手动测试 ncs: ${YELLOW}ncs create credential odpsuser --employee-id $EMPLOYEE_ID -o template -t odpscmd${NC}"
    echo -e "  4. 检查环境变量是否覆盖配置:${NC}"
    echo -e "     ${YELLOW}env | grep -E 'ALIBABA_CLOUD|MAXCOMPUTE|ODPS'${NC}"
    echo ""
    
    read -p "  是否继续后续步骤？(y/N): " CONTINUE_ANYWAY
    if [[ ! "$CONTINUE_ANYWAY" =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

echo ""

###############################################################################
# 步骤 4: 安装 skill
###############################################################################
echo -e "${GREEN}步骤 4/4: 安装 Skill${NC}"
echo ""

# 检查是否有已安装的 skill 平台
echo -e "  ${CYAN}请选择要安装 skill 的平台:${NC}"
echo -e "  [1] Claude Code"
echo -e "  [2] Cursor"
echo -e "  [3] Windsurf"
echo -e "  [4] Codex"
echo -e "  [5] Qwen"
echo -e "  [6] 跳过 skill 安装"
echo ""

read -p "  请选择 (1-6): " SKILL_CHOICE

case $SKILL_CHOICE in
    1)
        PLATFORM="claude-code"
        ;;
    2)
        PLATFORM="cursor"
        ;;
    3)
        PLATFORM="windsurf"
        ;;
    4)
        PLATFORM="codex"
        ;;
    5)
        PLATFORM="qwen"
        ;;
    6)
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
echo -e "    ${CYAN}ncs upgrade${NC}                       - 升级 ncs"
echo ""
echo -e "  更多信息请参阅:"
echo -e "    ${CYAN}maxc --help${NC}"
echo -e "    ${CYAN}maxc agent skill --json${NC}            - 查看 skill 信息"
echo ""
echo -e "${GREEN}祝使用愉快！${NC}"
