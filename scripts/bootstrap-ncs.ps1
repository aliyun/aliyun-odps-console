<#
.SYNOPSIS
    MaxC-CLI 安装与配置向导 (Windows 版本)
.DESCRIPTION
    交互式脚本：安装 ncs、maxc-cli，完成 auth 并安装 skill
    适用于集团弹内环境（使用 ncs 免 AK 方案）
.EXAMPLE
    # 方式 1: 一行命令远程执行（推荐）
    $u='https://maxcompute-repo.oss-cn-hangzhou.aliyuncs.com/maxc-cli/bootstrap-ncs.ps1';iex([Text.Encoding]::UTF8.GetString((New-Object Net.WebClient).DownloadData($u)))

    # 方式 2: 下载后本地执行
    powershell -ExecutionPolicy Bypass -File bootstrap-ncs.ps1
#>

& {

$ErrorActionPreference = "Stop"

# 颜色输出辅助函数
function Write-Step   { param([string]$Msg) Write-Host $Msg -ForegroundColor Green }
function Write-Info   { param([string]$Msg) Write-Host $Msg -ForegroundColor Cyan }
function Write-Warn   { param([string]$Msg) Write-Host $Msg -ForegroundColor Yellow }
function Write-Err    { param([string]$Msg) Write-Host $Msg -ForegroundColor Red }

function Confirm-Prompt {
    param(
        [string]$Message,
        [bool]$DefaultYes = $false
    )
    $suffix = if ($DefaultYes) { "(Y/n)" } else { "(y/N)" }
    $answer = Read-Host "  $Message $suffix"
    if ($DefaultYes) {
        return $answer -notmatch '^[Nn]$'
    } else {
        return $answer -match '^[Yy]$'
    }
}

Write-Host ""
Write-Info "============================================="
Write-Info "  MaxC-CLI 安装与配置向导 (Windows)"
Write-Info "============================================="
Write-Host ""

###############################################################################
# 步骤 1: 安装 ncs (Normandy Credential CLI)
###############################################################################
Write-Step "步骤 1/4: 安装 ncs (Normandy Credential CLI)"
Write-Host ""

# 检测系统架构（兼容 PowerShell 5.1 / .NET Framework）
$arch = $env:PROCESSOR_ARCHITECTURE  # AMD64, x86, ARM64
Write-Host "  检测到系统架构: " -NoNewline
Write-Warn "$arch"

# 检查 ncs 是否已安装
$ncsCmd = Get-Command ncs -ErrorAction SilentlyContinue
if ($ncsCmd) {
    Write-Warn "  ncs 已安装，跳过安装步骤"
} else {
    Write-Host "  正在下载 ncs..."

    # 根据架构选择下载链接
    switch ($arch) {
        "AMD64" {
            $ncsUrl = "https://authx.alibaba-inc.com/api/v1/releases/latest/bin/windows/amd64/ncs.exe"
        }
        "ARM64" {
            $ncsUrl = "https://authx.alibaba-inc.com/api/v1/releases/latest/bin/windows/arm64/ncs.exe"
        }
        default {
            Write-Err "  不支持的架构: $arch"
            Write-Err "  Windows 仅支持 X64 和 Arm64"
            return
        }
    }

    # 确定安装目录
    $installDir = Join-Path $env:LOCALAPPDATA "Programs\ncs"
    if (-not (Test-Path $installDir)) {
        New-Item -ItemType Directory -Path $installDir -Force | Out-Null
    }

    $ncsPath = Join-Path $installDir "ncs.exe"

    # 下载 ncs
    try {
        [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
        Invoke-WebRequest -Uri $ncsUrl -OutFile $ncsPath -UseBasicParsing
    } catch {
        Write-Err "  下载 ncs 失败: $_"
        return
    }

    # 将安装目录加入当前会话 PATH
    if ($env:PATH -notlike "*$installDir*") {
        $env:PATH = "$installDir;$env:PATH"
    }

    # 持久化写入用户 PATH（注册表）
    $userPath = [Environment]::GetEnvironmentVariable("PATH", "User")
    if ($userPath -notlike "*$installDir*") {
        [Environment]::SetEnvironmentVariable("PATH", "$installDir;$userPath", "User")
        Write-Info "  已将 $installDir 加入用户 PATH（重新打开终端生效）"
    }

    Write-Step "  ncs 安装成功！"
}

Write-Host ""
Write-Host "  ncs 版本:"
try { ncs --version } catch { Write-Host "  (无法获取版本信息)" }
Write-Host ""

###############################################################################
# 步骤 2: 安装 maxc-cli
###############################################################################
Write-Step "步骤 2/4: 安装 maxc-cli"
Write-Host ""

# 检查 Python
$pythonCmd = Get-Command python -ErrorAction SilentlyContinue
if (-not $pythonCmd) {
    $pythonCmd = Get-Command python3 -ErrorAction SilentlyContinue
}
if (-not $pythonCmd) {
    Write-Err "  未检测到 Python，请先安装 Python >= 3.9"
    Write-Err "  下载地址: https://www.python.org/downloads/"
    return
}

$pythonExe = $pythonCmd.Source
$pythonVersion = & $pythonExe --version 2>&1 | ForEach-Object { ($_ -split ' ')[1] }
Write-Host "  检测到 Python 版本: " -NoNewline
Write-Warn "$pythonVersion"

$versionParts = $pythonVersion -split '\.'
$major = [int]$versionParts[0]
$minor = [int]$versionParts[1]

if ($major -gt 3 -or ($major -eq 3 -and $minor -ge 9)) {
    Write-Step "  Python 版本满足要求 (>= 3.9)"
} else {
    Write-Err "  Python 版本不满足要求，需要 >= 3.9"
    return
}

# 确定 pip 命令
$hasPip = $false
try {
    & $pythonExe -m pip --version 2>&1 | Out-Null
    $hasPip = $true
} catch {}

if (-not $hasPip) {
    Write-Err "  未找到 pip，无法安装 maxc-cli"
    Write-Err "  请先安装 pip: & '$pythonExe' -m ensurepip --upgrade"
    return
}

Write-Host "  使用 pip 命令: " -NoNewline
Write-Info "& '$pythonExe' -m pip"

# 检查 maxc-cli 是否已安装
$maxcCmd = Get-Command maxc -ErrorAction SilentlyContinue
if ($maxcCmd) {
    $currentVersion = (maxc --version 2>$null) -replace '.*\s', ''
    Write-Warn "  maxc-cli 已安装 (版本: $currentVersion)"

    Write-Host "  正在检查最新版本..."
    $latestVersion = ""
    try {
        $indexOutput = & $pythonExe -m pip index versions maxc-cli 2>&1
        if ($indexOutput -match 'LATEST:\s*(\S+)') {
            $latestVersion = $Matches[1]
        }
    } catch {}

    if ($latestVersion -and $latestVersion -ne $currentVersion) {
        # 简单版本比较
        $isNewer = $false
        $currentParts = $currentVersion -split '\.' | ForEach-Object { [int]$_ }
        $latestParts  = $latestVersion  -split '\.' | ForEach-Object { [int]$_ }
        for ($i = 0; $i -lt [Math]::Max($currentParts.Count, $latestParts.Count); $i++) {
            $c = if ($i -lt $currentParts.Count) { $currentParts[$i] } else { 0 }
            $l = if ($i -lt $latestParts.Count)  { $latestParts[$i]  } else { 0 }
            if ($l -gt $c) { $isNewer = $true; break }
            if ($c -gt $l) { break }
        }

        if ($isNewer) {
            Write-Warn "  发现新版本: $latestVersion (当前: $currentVersion)"
            if (Confirm-Prompt "是否升级到最新版本？") {
                Write-Host "  正在升级 maxc-cli..."
                & $pythonExe -m pip install --upgrade maxc-cli
                if ($LASTEXITCODE -ne 0) {
                    Write-Err "  maxc-cli 升级失败 (exit code: $LASTEXITCODE)"
                    return
                }
                Write-Step "  maxc-cli 升级成功！"
            } else {
                Write-Step "  保持当前版本"
            }
        } else {
            Write-Step "  当前版本 ($currentVersion) 已高于 PyPI 最新稳定版 ($latestVersion)"
        }
    } elseif ($latestVersion) {
        Write-Step "  已是最新版本 (v$latestVersion)"
    } else {
        Write-Warn "  无法获取最新版本信息"
        if (Confirm-Prompt "是否尝试升级？") {
            Write-Host "  正在升级 maxc-cli..."
            & $pythonExe -m pip install --upgrade maxc-cli
            if ($LASTEXITCODE -ne 0) {
                Write-Err "  maxc-cli 升级失败 (exit code: $LASTEXITCODE)"
                return
            }
            Write-Step "  maxc-cli 升级完成！"
        }
    }
} else {
    Write-Host "  正在安装 maxc-cli..."
    & $pythonExe -m pip install maxc-cli
    if ($LASTEXITCODE -ne 0) {
        Write-Err "  maxc-cli 安装失败 (exit code: $LASTEXITCODE)"
        return
    }
    Write-Step "  maxc-cli 安装成功！"
}

Write-Host ""
Write-Host "  maxc-cli 版本:"
try { maxc --version } catch { Write-Host "  (无法获取版本信息)" }
Write-Host ""

###############################################################################
# 步骤 3: 配置认证
###############################################################################
Write-Step "步骤 3/4: 配置认证"
Write-Host ""

$skipAuth = $false

# 只把在线 doctor 通过的身份视为可用；本地配置存在并不等于认证成功。
Write-Host "  正在执行在线就绪检查..."
$doctorOutput = maxc agent doctor --online --json 2>&1 | Out-String
$doctorExitCode = $LASTEXITCODE
if ($doctorExitCode -eq 0 -and $doctorOutput -match '"online_ready"\s*:\s*true') {
    Write-Step "  当前认证和后端连接已在线验证"
    if (-not (Confirm-Prompt "是否要重新配置认证？")) {
        Write-Step "  继续使用当前已验证身份"
        $skipAuth = $true
    }
} else {
    Write-Warn "  当前身份尚未通过在线验证"
}

if (-not $skipAuth) {
    Write-Host ""

    # 显式 external provider 优先于认证环境变量；仅报告变量名，不显示值。
    Write-Info "  检查当前进程中的认证环境变量..."

    $envVarsToCheck = @(
        "ALIBABA_CLOUD_ACCESS_KEY_ID", "ALIBABA_CLOUD_ACCESS_KEY_SECRET",
        "MAXCOMPUTE_PROJECT", "MAXCOMPUTE_ENDPOINT", "MAXCOMPUTE_REGION",
        "ODPS_PROJECT", "ODPS_ENDPOINT", "ODPS_ACCESS_ID", "ODPS_ACCESS_KEY",
        "ODPS_STS_TOKEN", "SECURITY_TOKEN"
    )

    $foundConflicting = @()
    foreach ($var in $envVarsToCheck) {
        $val = [Environment]::GetEnvironmentVariable($var)
        if ($val) { $foundConflicting += $var }
    }

    if ($foundConflicting.Count -gt 0) {
        Write-Warn "  发现以下环境变量名已设置:"
        foreach ($var in $foundConflicting) {
            Write-Warn "    $var"
        }
        Write-Host ""
        Write-Warn "  选择 ncs external provider 后，认证变量不会静默覆盖该 provider。"
        Write-Warn "  最终生效身份以 agent doctor --online 的结果为准。"
    } else {
        Write-Step "  未发现冲突的环境变量"
    }

    Write-Host ""
    Write-Info "  使用 ncs 进行认证..."
    Write-Host ""

    # 检查 ncs
    if (-not (Get-Command ncs -ErrorAction SilentlyContinue)) {
        Write-Err "  ncs 未安装，请检查步骤 1 是否成功"
        return
    }

    Write-Warn "  提示: 以下操作将引导你完成 ncs 认证配置"
    Write-Host ""

    # 输入项目空间
    Write-Info "  请填写默认工作空间 (project):"
    $projectName = Read-Host "  "
    if (-not $projectName) {
        Write-Err "  项目空间不能为空"
        return
    }

    # 获取 ncs 授权列表
    Write-Host ""
    Write-Info "  正在获取可用账号列表..."

    $employeeRaw = ""
    try {
        $employeeRaw = (ncs list authorizations odpsuser -o custom-columns=BUC_USER_ID:.extension.bucUserId,BUC_USER_TYPE:.extension.bucUserType,BUC_ACCOUNT_NAME:.extension.bucDomainAccount 2>$null) | Out-String
    } catch {}

    # 解析列表（跳过前两行表头）; @() 强制为数组，防止单元素时被解包为 string
    $employeeLines = @(($employeeRaw -split "`n" | Where-Object { "$_".Trim() }) | Select-Object -Skip 2)
    $employeeId = ""

    if ($employeeLines.Count -gt 0) {
        if ($employeeLines.Count -eq 1) {
            $parts = "$($employeeLines[0])".Trim() -split '\s+'
            $employeeId   = $parts[0]
            $employeeName = if ($parts.Count -ge 3) { $parts[2] } else { "" }
            Write-Host ""
            Write-Host "  找到账号: " -NoNewline
            Write-Info "$employeeId ($employeeName), 自动选中"
        } else {
            Write-Host ""
            Write-Step "  找到以下个人账号:"
            foreach ($line in $employeeLines) { Write-Host "  $line" }
            Write-Host ""
            Write-Info "  请输入要使用的工号 (BUC_USER_ID):"
            $employeeId = Read-Host "  "
        }

        if (-not $employeeId) {
            Write-Err "  工号不能为空"
            return
        }

        # 获取现有 endpoint
        $existingEndpoint = ""
        try {
            $whoamiJson = maxc auth whoami --json 2>$null | Out-String
            if ($whoamiJson -match '"endpoint"\s*:\s*"([^"]*)"') {
                $existingEndpoint = $Matches[1]
            }
        } catch {}

        # 选择 endpoint
        Write-Host ""
        Write-Info "  请选择 MaxCompute endpoint:"
        Write-Host "  [1] 国内弹内:   http://service-corp.odps.aliyun-inc.com/api"
        Write-Host "  [2] 新加坡弹内: http://service-all.ali-sg-lazada.odps.aliyun-inc.com/api"
        if ($existingEndpoint) {
            Write-Host "  [3] 使用现有配置: $existingEndpoint"
            Write-Host "  [4] 手动输入"
            $epChoice = Read-Host "  请选择 (1-4)"
        } else {
            Write-Host "  [3] 手动输入"
            $epChoice = Read-Host "  请选择 (1-3)"
        }

        $endpoint = switch ($epChoice) {
            "1" { "http://service-corp.odps.aliyun-inc.com/api" }
            "2" { "http://service-all.ali-sg-lazada.odps.aliyun-inc.com/api" }
            "3" {
                if ($existingEndpoint) { $existingEndpoint }
                else {
                    Write-Info "  请输入 endpoint:"
                    Read-Host "  "
                }
            }
            "4" {
                if ($existingEndpoint) {
                    Write-Info "  请输入 endpoint:"
                    Read-Host "  "
                } else {
                    Write-Err "  无效选择"; return
                }
            }
            default { Write-Err "  无效选择"; return }
        }

        if (-not $endpoint) {
            Write-Err "  endpoint 不能为空"
            return
        }

        # 构建 ncs 命令
        $ncsCommand = "ncs create credential odpsuser --employee-id $employeeId -o template -t odpscmd"

        # 配置 maxc-cli
        Write-Host ""
        Write-Host "  正在配置 maxc-cli 使用外部凭证..."
        try {
            maxc auth login-external `
                --process-command $ncsCommand `
                --project $projectName `
                --endpoint $endpoint `
                --no-validate
        } catch {
            Write-Err "  maxc auth login-external 配置失败，请检查参数后重试"
            return
        }

        Write-Step "  ncs 认证配置完成！"
        Write-Warn "  提示: 认证已保存但跳过远程验证。实际使用时 ncs 会自动获取凭据。"
    } else {
        Write-Warn "  未找到可用的个人账号，请联系管理员"
        return
    }
}

Write-Host ""
Write-Step "  正在通过 agent doctor --online 验证身份和后端..."
$doctorOutput = maxc agent doctor --online --json 2>&1 | Out-String
$doctorExitCode = $LASTEXITCODE
if ($doctorExitCode -ne 0 -or $doctorOutput -notmatch '"online_ready"\s*:\s*true') {
    Write-Err "  [FAIL] 在线就绪检查未通过；不会把 configuration_only 或 validation_failed 当作成功。"
    ($doctorOutput -split "`n" | Select-Object -First 30) | ForEach-Object { Write-Warn "    $_" }
    Write-Warn "  检查 ncs、精确 project/endpoint，并按 recovery_steps 修复后重试。"
    exit 1
}

Write-Step "  [OK] ncs 身份与 MaxCompute 后端已在线验证"

Write-Host ""

###############################################################################
# 步骤 4: 安装 skill
###############################################################################
Write-Step "步骤 4/4: 安装 Skill"
Write-Host ""

Write-Info "  请选择要安装 skill 的平台:"
Write-Host "  [1] Claude Code"
Write-Host "  [2] Cursor"
Write-Host "  [3] Windsurf"
Write-Host "  [4] Codex"
Write-Host "  [5] Qwen"
Write-Host "  [6] Qoder"
Write-Host "  [7] Qoder Work"
Write-Host "  [8] 跳过 skill 安装"
Write-Host ""

$skillChoice = Read-Host "  请选择 (1-8)"

$platform = switch ($skillChoice) {
    "1" { "claude-code" }
    "2" { "cursor" }
    "3" { "windsurf" }
    "4" { "codex" }
    "5" { "qwen" }
    "6" { "qoder" }
    "7" { "qoderwork" }
    "8" { Write-Warn "  跳过 skill 安装"; "" }
    default { Write-Err "  无效选择，跳过 skill 安装"; "" }
}

if ($platform) {
    Write-Host ""
    Write-Host "  正在为 " -NoNewline; Write-Info "$platform" -NoNewline; Write-Host " 安装 skill..."
    maxc agent skill install $platform --invocation maxc --json
    if ($LASTEXITCODE -ne 0) {
        Write-Err "  Skill 安装失败 (exit code: $LASTEXITCODE)"
    } else {
        Write-Host ""
        Write-Step "  Skill 安装成功！"
        Write-Host "  现在可以在 $platform 中使用 maxc-cli 相关的技能了"
    }
}

###############################################################################
# 完成
###############################################################################
Write-Host ""
Write-Info "============================================="
Write-Step "  安装与配置完成！"
Write-Info "============================================="
Write-Host ""
Write-Host "  常用命令:"
Write-Info "    maxc agent doctor --online --json  "; Write-Host "- 验证身份与后端"
Write-Info "    maxc auth whoami --json           "; Write-Host "- 查看当前身份"
Write-Info "    maxc meta list-tables --json      "; Write-Host "- 列出可用表"
Write-Info '    maxc query "SELECT ..." --json    '; Write-Host "- 执行查询"
Write-Info "    maxc cache build --json           "; Write-Host "- 构建元数据缓存"
Write-Info "    ncs upgrade                       "; Write-Host "- 升级 ncs"
Write-Host ""
Write-Host "  更多信息请参阅:"
Write-Info "    maxc --help"
Write-Info "    maxc agent skill --json            "; Write-Host "- 查看 skill 信息"
Write-Host ""
Write-Step "祝使用愉快！"

} # end of & { }
