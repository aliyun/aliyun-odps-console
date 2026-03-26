#!/bin/bash
#
# Author: zhiwei
# NCS 安装脚本
# 下载并安装 NCS 命令行工具
#
# 可被 entry.sh source 调用，也可独立运行
#

set -e

# ============================================================
# 公共函数（当未被 entry.sh source 时独立定义）
# ============================================================
if [[ -z "${_ODPSCMD_COMMON_LOADED:-}" ]]; then
    RED='\033[0;31m'
    GREEN='\033[0;32m'
    YELLOW='\033[1;33m'
    BLUE='\033[0;34m'
    CYAN='\033[0;36m'
    NC='\033[0m'

    log_info() {
        echo -e "${BLUE}[INFO]${NC} $1"
    }

    log_success() {
        echo -e "${GREEN}[SUCCESS]${NC} $1"
    }

    log_warning() {
        echo -e "${YELLOW}[WARNING]${NC} $1"
    }

    log_error() {
        echo -e "${RED}[ERROR]${NC} $1"
    }

    log_step() {
        echo -e "${CYAN}----------------------------------------${NC}"
        echo -e "${CYAN}  $1${NC}"
        echo -e "${CYAN}----------------------------------------${NC}"
    }

    check_macos() {
        if [[ "$OSTYPE" != "darwin"* ]]; then
            log_error "此脚本仅适用于 macOS 系统"
            exit 1
        fi
        log_info "检测到 macOS 系统"
    }

    get_script_dir() {
        local DIR
        DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
        echo "$DIR"
    }
fi

# 获取 shell 配置文件路径（独立运行时使用，被 source 时从 entry.sh 获得）
get_shell_rc_file() {
    if [[ -n "${ZSH_VERSION:-}" ]] || [[ "$SHELL" == *"/zsh" ]]; then
        echo "$HOME/.zshrc"
    else
        echo "$HOME/.bash_profile"
    fi
}

# 检查并确保 /usr/sbin 在 PATH 中（当前会话 + 持久化到 shell 配置文件）
ensure_usr_sbin_in_path() {
    local SHELL_RC
    SHELL_RC=$(get_shell_rc_file)

    # 检查当前会话
    if [[ ":$PATH:" != *":/usr/sbin:"* ]]; then
        export PATH="/usr/sbin:$PATH"
        log_info "已将 /usr/sbin 添加到当前会话 PATH"

        if ! grep -q '"/usr/sbin:\$PATH"' "$SHELL_RC" 2>/dev/null; then
            echo "" >> "$SHELL_RC"
            echo "# 系统工具路径" >> "$SHELL_RC"
            echo 'export PATH="/usr/sbin:$PATH"' >> "$SHELL_RC"
            log_info "已将 /usr/sbin 持久化到 $SHELL_RC"
        fi
    fi
}

# ============================================================
# NCS 安装专有函数
# ============================================================

# 检测架构（NCS 版本：x86_64 映射为 amd64）
# 使用 NCS_ARCH 避免与其他脚本的 ARCH 变量冲突
detect_ncs_arch() {
    NCS_ARCH=$(uname -m)
    case $NCS_ARCH in
        arm64)
            log_info "架构：Apple Silicon (arm64)"
            ;;
        x86_64)
            log_info "架构：Intel (x86_64)"
            NCS_ARCH="amd64"
            ;;
        *)
            log_error "不支持的架构：$NCS_ARCH"
            exit 1
            ;;
    esac
}

# 下载并安装 NCS 到 HOME 目录
install_ncs() {
    log_info "下载并安装 NCS..."

    local INSTALL_DIR
    INSTALL_DIR="$HOME/.ncs"
    mkdir -p "$INSTALL_DIR"
    local DOWNLOAD_URL="https://authx.alibaba-inc.com/api/v1/releases/latest/bin/darwin/${NCS_ARCH}/ncs"
    local NCS_BIN="$INSTALL_DIR/ncs"

    log_info "下载链接：$DOWNLOAD_URL"
    log_info "安装目录: $INSTALL_DIR"

    # 下载 NCS
    if command -v curl &> /dev/null; then
        local HTTP_CODE
        HTTP_CODE=$(curl -L -o "$NCS_BIN" -w "%{http_code}" "$DOWNLOAD_URL" --progress-bar)
        if [[ "$HTTP_CODE" -lt 200 || "$HTTP_CODE" -ge 300 ]]; then
            log_error "下载失败，HTTP 状态码: ${HTTP_CODE}"
            exit 1
        fi
    elif command -v wget &> /dev/null; then
        wget -O "$NCS_BIN" "$DOWNLOAD_URL" --show-progress
    else
        log_error "未找到 curl 或 wget，无法下载"
        exit 1
    fi

    if [[ ! -f "$NCS_BIN" ]]; then
        log_error "下载失败"
        exit 1
    fi

    local CHMOD_CMD="chmod"
    command -v chmod &> /dev/null || CHMOD_CMD="/bin/chmod"
    $CHMOD_CMD +x "$NCS_BIN"

    log_success "NCS 下载完成: $NCS_BIN"
}

# 配置 NCS 环境变量
setup_ncs_env() {
    log_info "配置 NCS 环境变量..."

    local INSTALL_DIR
    INSTALL_DIR="$HOME/.ncs"
    local SHELL_RC
    SHELL_RC=$(get_shell_rc_file)

    # 更新或添加 NCS_HOME 到 shell 配置文件
    if [[ -f "$SHELL_RC" ]] && grep -q "^export NCS_HOME=" "$SHELL_RC" 2>/dev/null; then
        sed -i '' "s|^export NCS_HOME=.*|export NCS_HOME=$INSTALL_DIR|" "$SHELL_RC"
        log_info "已更新 NCS_HOME: $INSTALL_DIR"
        # 补检 PATH 行
        if ! grep -q 'NCS_HOME:' "$SHELL_RC" 2>/dev/null; then
            echo 'export PATH=$NCS_HOME:$PATH' >> "$SHELL_RC"
            log_info "已补充 PATH 配置"
        fi
    else
        echo "" >> "$SHELL_RC"
        echo "# NCS" >> "$SHELL_RC"
        echo "export NCS_HOME=$INSTALL_DIR" >> "$SHELL_RC"
        log_info "已添加 NCS_HOME: $INSTALL_DIR"

        # 更新或添加 PATH
        if grep -q 'NCS_HOME:' "$SHELL_RC" 2>/dev/null; then
            log_info "PATH 配置已存在"
        else
            echo 'export PATH=$NCS_HOME:$PATH' >> "$SHELL_RC"
            log_info "已添加 PATH 配置"
        fi
    fi

    # 导出到当前会话
    export NCS_HOME="$INSTALL_DIR"
    export PATH="$NCS_HOME:$PATH"

    log_success "NCS 环境变量已配置到 $SHELL_RC"
}

# 验证安装
verify_ncs() {
    log_info "验证 NCS 安装..."

    if command -v ncs &> /dev/null; then
        local NCS_PATH
        NCS_PATH=$(which ncs)
        # 检测到 aone-kit 中的 ncs 时，继续安装（不复用）
        if [[ "$NCS_PATH" == *".real/third_party/cli/aone-kit/bin"* ]]; then
            log_warning "检测到 aone-kit 中的 NCS，将安装独立版本"
            return 1
        fi
        log_success "✓ NCS 可用"
        log_info "版本：$(ncs --version 2>&1 || echo '无法获取版本信息')"
        log_info "路径：$NCS_PATH"
        return 0
    else
        log_warning "✗ NCS 不可用"
        return 1
    fi
}

# 主函数
ncs_main() {
    # 首先检查 /usr/sbin 在 PATH 中
    ensure_usr_sbin_in_path

    echo "========================================"
    echo "    STEP 2/4: NCS 安装脚本 START"
    echo "========================================"
    echo ""

    log_warning "安装过程中需要管理员权限，可能会提示输入解锁屏幕的密码"
    echo ""

    check_macos
    detect_ncs_arch
    echo ""

    # 检查是否已安装
    if verify_ncs; then
        echo ""
    else
        install_ncs
        echo ""

        setup_ncs_env
        echo ""

        verify_ncs
        echo ""
    fi

    echo "========================================"
    echo "    STEP 2/4: NCS 安装脚本 END"
    echo "========================================"
}

# 独立运行时执行主函数；被 source 时仅加载函数
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    ncs_main "$@"
fi
