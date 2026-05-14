#!/usr/bin/env bash
# ──────────────────────────────────────────────────────
# maxc-cli 发布脚本
# 用法:
#   bash scripts/release.sh              # 打包 + 验证
#   bash scripts/release.sh --upload     # 打包 + 验证 + 上传 PyPI
#   bash scripts/release.sh --upload --repository testpypi   # 上传到 TestPyPI
# ──────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

UPLOAD=0
REPOSITORY=""

# ── 解析参数 ──────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        --upload)      UPLOAD=1; shift ;;
        --repository)  REPOSITORY="$2"; shift 2 ;;
        -h|--help)
            echo "用法: bash scripts/release.sh [--upload] [--repository <name>]"
            echo ""
            echo "选项:"
            echo "  --upload        打包并上传到 PyPI"
            echo "  --repository    指定上传目标 (默认 pypi，可设为 testpypi)"
            exit 0
            ;;
        *) echo "未知参数: $1"; exit 1 ;;
    esac
done

cd "$PROJECT_DIR"

VERSION="$(PYTHONPATH=src python -c 'import maxc_cli; print(maxc_cli.__version__)')"
echo "=========================================="
echo " maxc-cli v${VERSION} 发布"
echo "=========================================="

# ── Step 1: 清理 ──────────────────────────────────────
echo ""
echo "[1/5] 清理旧构建产物..."
rm -rf dist/ build/ src/*.egg-info

# ── Step 2: 运行测试 ──────────────────────────────────
echo ""
echo "[2/5] 运行单元测试..."
python -m pytest -m unit -q --tb=short

# ── Step 3: 构建 ──────────────────────────────────────
echo ""
echo "[3/5] 构建 sdist + wheel..."
python -m build

echo ""
echo "产物:"
ls -lh dist/

# ── Step 4: 验证安装 ──────────────────────────────────
echo ""
echo "[4/5] 验证 wheel 安装..."
python -m pip install "dist/maxc_cli-${VERSION}-py3-none-any.whl" --force-reinstall --quiet
maxc --version
maxc --help >/dev/null 2>&1 && echo "✓ maxc --help 正常"
maxc agent skill --help >/dev/null 2>&1 && echo "✓ maxc agent skill 正常"

# ── Step 5: 上传（可选） ──────────────────────────────
if [[ "$UPLOAD" -eq 1 ]]; then
    echo ""
    echo "[5/5] 上传到 $( [[ -n "$REPOSITORY" ]] && echo "$REPOSITORY" || echo "PyPI" )..."

    if ! command -v twine &>/dev/null; then
        echo "安装 twine..."
        pip install twine --quiet
    fi

    if [[ -n "$REPOSITORY" ]]; then
        twine upload --repository "$REPOSITORY" dist/*
    else
        twine upload dist/*
    fi

    echo ""
    echo "✅ 发布完成: maxc-cli ${VERSION}"
    echo "   PyPI: https://pypi.org/project/maxc-cli/${VERSION}/"
else
    echo ""
    echo "[5/5] 跳过上传 (加 --upload 可上传 PyPI)"
fi

echo ""
echo "=========================================="
echo " 下一步:"
echo "   git tag v${VERSION} && git push origin v${VERSION}"
echo "   bash scripts/release.sh --upload              # 上传 PyPI"
echo "   bash scripts/release.sh --upload --repository testpypi  # 上传 TestPyPI"
echo "=========================================="
