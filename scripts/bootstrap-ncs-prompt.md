# MaxC-CLI 安装与配置引导

请帮我在当前机器上完成 MaxC-CLI 的安装和配置。按以下 4 个步骤顺序执行，每一步都先检测再操作，失败时停下来告诉我原因。

---

## 步骤 1: 确保 Python 环境可用 (>= 3.9)

检测当前机器是否有可用的 Python:

```bash
python3 --version 2>/dev/null || python --version 2>/dev/null
```

Windows 上用:

```powershell
python --version
```

**判断逻辑:**

- 如果 Python 存在且版本 >= 3.9 → 直接进入步骤 2
- 如果 Python 不存在或版本过低 → **先告诉我，征求我的同意后**，按如下方式安装:
  - **macOS**: `brew install python@3.12`（如果没有 brew，提示我去 python.org 下载安装包）
  - **Linux (Debian/Ubuntu)**: `sudo apt-get update && sudo apt-get install -y python3 python3-pip`
  - **Linux (CentOS/RHEL)**: `sudo yum install -y python3 python3-pip`
  - **Windows**: 提示我去 https://www.python.org/downloads/ 下载安装，安装时勾选 "Add Python to PATH"

安装后再验证一次 `python3 --version` 或 `python --version`。

同时确认 pip 可用:

```bash
python3 -m pip --version
```

---

## 步骤 2: 安装 ncs (Normandy Credential CLI)

检测 ncs 是否已安装:

```bash
ncs --version
```

**如果已安装** → 跳到步骤 3。

**如果未安装** → 根据操作系统和架构下载安装:

### 确定下载 URL

先检测 OS 和架构:

```bash
uname -s   # Darwin / Linux
uname -m   # x86_64 / arm64 / aarch64
```

Windows 上:

```powershell
$env:OS                      # Windows_NT
$env:PROCESSOR_ARCHITECTURE  # AMD64 / ARM64
```

下载地址规则: `https://authx.alibaba-inc.com/api/v1/releases/latest/bin/{os}/{arch}/ncs{ext}`

| 操作系统 | 架构 | os | arch | ext |
|---------|------|-----|------|-----|
| macOS | x86_64 | darwin | amd64 | (无) |
| macOS | arm64 | darwin | arm64 | (无) |
| Linux | x86_64 | linux | amd64 | (无) |
| Linux | aarch64 | linux | arm64 | (无) |
| Windows | AMD64 | windows | amd64 | .exe |
| Windows | ARM64 | windows | arm64 | .exe |

### macOS / Linux 安装

```bash
# 下载（替换实际 URL）
curl -fsSL -o /tmp/ncs "<download_url>"
chmod +x /tmp/ncs

# macOS 去除隔离属性
xattr -c /tmp/ncs 2>/dev/null || true

# 安装到系统目录（需要 sudo）
sudo mv /tmp/ncs /usr/local/bin/ncs
```

### Windows 安装

```powershell
# 安装目录
$installDir = Join-Path $env:LOCALAPPDATA "Programs\ncs"
New-Item -ItemType Directory -Path $installDir -Force | Out-Null

# 下载
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
Invoke-WebRequest -Uri "<download_url>" -OutFile (Join-Path $installDir "ncs.exe") -UseBasicParsing

# 加入当前会话 PATH
$env:PATH = "$installDir;$env:PATH"

# 持久化写入用户 PATH
$userPath = [Environment]::GetEnvironmentVariable("PATH", "User")
if ($userPath -notlike "*$installDir*") {
    [Environment]::SetEnvironmentVariable("PATH", "$installDir;$userPath", "User")
}
```

### 验证

```bash
ncs --version
```

---

## 步骤 3: 安装 maxc-cli 并配置认证

### 3a. 安装 maxc-cli

```bash
python3 -m pip install --upgrade maxc-cli
```

Windows 上如果 `python3` 不可用，用 `python -m pip install --upgrade maxc-cli`。

验证:

```bash
maxc --version
```

如果 `maxc` 不在 PATH 中，尝试 `python3 -m maxc_cli --version`。后续命令也用 `python3 -m maxc_cli` 替代 `maxc`。

### 3b. 检查认证状态

```bash
maxc auth whoami --json
```

查看返回 JSON 中的 `data.identity`:

- `authenticated=true, validation_status=verified` → **已认证**，告诉我当前身份（principal_display、project、auth_type），问我是否继续使用还是重新配置
- 其他情况 → 进入下面的认证配置流程

### 3c. 检查环境变量来源

检查以下环境变量是否存在:

```bash
env | grep -E 'ALIBABA_CLOUD_ACCESS_KEY|MAXCOMPUTE_|ODPS_|SECURITY_TOKEN'
```

Windows:

```powershell
Get-ChildItem Env: | Where-Object { $_.Name -match 'ALIBABA_CLOUD_ACCESS_KEY|MAXCOMPUTE_|ODPS_|SECURITY_TOKEN' }
```

只报告检测到的变量名，不要输出变量值。显式配置 `external` 认证 provider 后，
CLI 会抑制认证环境变量并给出警告，避免静默切换身份；不要自动 `unset` 用户
环境中的变量。

### 3d. 配置 ncs 认证

**问我以下信息:**

1. **默认工作空间 (project)**: 我要使用的 MaxCompute project 名称
2. **工号**: 通过以下命令获取可选账号列表:

```bash
ncs list authorizations odpsuser -o custom-columns=BUC_USER_ID:.extension.bucUserId,BUC_USER_TYPE:.extension.bucUserType,BUC_ACCOUNT_NAME:.extension.bucDomainAccount
```

如果只有一个账号，自动选中并告诉我；如果有多个，列出来让我选。

3. **Endpoint**: 让我选择:
   - [1] 国内弹内: `http://service-corp.odps.aliyun-inc.com/api`
   - [2] 新加坡弹内: `http://service-all.ali-sg-lazada.odps.aliyun-inc.com/api`
   - [3] 手动输入

### 3e. 执行认证

用收集到的信息执行:

```bash
maxc auth login-external \
  --process-command "ncs create credential odpsuser --employee-id <工号> -o template -t odpscmd" \
  --project "<project>" \
  --endpoint "<endpoint>" \
  --no-validate \
  --json
```

### 3f. 验证认证

```bash
maxc agent context --json
maxc agent manifest --json
maxc agent doctor --online --json
```

`agent context` 只检查本地配置，不访问网络。`--no-validate` 保存 external provider
后，必须通过 `agent doctor --online` 做实时身份检查；仅当返回
`data.ready=true` 时继续数据操作。`configuration_only` 和 `validation_failed` 都
不能证明远端已就绪。

如果存在历史遗留的 `~/.maxc/session_override.yaml`，CLI 会在首次运行时自动迁移其内容到 `~/.maxc/config.yaml` 并删除该文件，无需手动处理。

---

## 步骤 4: 安装 Agent Skill

**问我使用的 Agent 平台:**

- claude-code
- cursor
- windsurf
- codex
- qwen
- qoder
- qoderwork
- openclaw
- hermes
- others

然后执行:

```bash
maxc agent skill install <平台名> --invocation maxc --json
```

安装目录中的 Skill 名为 `alibabacloud-maxcompute-cli`。平台列表以
`maxc agent skill install --help` 的实时输出为准。

---

## 完成

全部完成后，告诉我以下常用命令:

```
maxc auth whoami --json        # 查看当前身份
maxc agent manifest --json     # 查看实时命令契约
maxc agent doctor --online --json # 验证远端就绪状态
maxc meta list-tables --json   # 列出可用表
maxc query "SELECT ..." --json # 执行查询
maxc cache build --json        # 构建元数据缓存
ncs upgrade                    # 升级 ncs
```
