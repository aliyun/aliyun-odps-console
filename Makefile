.PHONY: test unit build clean smoke build-bin bin-smoke release-build release-upload release-publish-latest release-local

test:
	pytest -q --tb=short

unit:
	pytest -m unit -q --tb=short

build:
	python -m build

clean:
	rm -rf dist/ build/ *.egg-info src/*.egg-info

smoke: build
	pip install dist/maxc_cli-*.whl --force-reinstall
	python -c "import maxc_cli; print(maxc_cli.__version__)"
	maxc --help
	maxc agent skill --help

build-bin:
	pyinstaller --noconfirm maxc.spec

bin-smoke: build-bin
	./dist/maxc/maxc --version
	./dist/maxc/maxc --help >/dev/null
	./dist/maxc/maxc --format json agent skill | python -c "import json,sys; d=json.load(sys.stdin); assert d['data']['skill_exists'], 'SKILL.md not bundled'; print('skill_exists OK')"

# ─────────────────────────────────────────────
# Release pipeline (spec 2026-05-19-maxc-cliext-integration-design)
# ─────────────────────────────────────────────

# Detect current platform — overridable for cross-build CI (where the actual
# build still happens on the target runner, this just labels the artifact).
_PLATFORM_OS  := $(shell uname -s | tr '[:upper:]' '[:lower:]')
_PLATFORM_ARCH_RAW := $(shell uname -m)
_PLATFORM_ARCH := $(if $(filter x86_64,$(_PLATFORM_ARCH_RAW)),amd64,$(if $(filter aarch64 arm64,$(_PLATFORM_ARCH_RAW)),arm64,$(_PLATFORM_ARCH_RAW)))
PLATFORM ?= $(_PLATFORM_OS)-$(_PLATFORM_ARCH)

VERSION ?= $(shell grep -E '^__version__' src/maxc_cli/__init__.py | sed -E 's/.*"([^"]+)".*/\1/')

# OSS publishing defaults — match the public bucket layout
#   https://maxcompute-repo.oss-cn-hangzhou.aliyuncs.com/maxc-cli/
# Override any of these to redirect uploads to a mirror or staging bucket.
OSS_BUCKET ?= maxcompute-repo
OSS_REGION ?= cn-hangzhou
OSS_PREFIX ?= maxc-cli

release-build:
	OUTPUT_DIR=dist/release bash scripts/build_release.sh

release-upload: release-build
	INPUT_DIR=dist/release VERSION=$(VERSION) PLATFORM=$(PLATFORM) \
	  OSS_BUCKET=$(OSS_BUCKET) OSS_REGION=$(OSS_REGION) OSS_PREFIX=$(OSS_PREFIX) \
	  bash scripts/upload_to_oss.sh

release-publish-latest:
	VERSION=$(VERSION) \
	  OSS_BUCKET=$(OSS_BUCKET) OSS_REGION=$(OSS_REGION) OSS_PREFIX=$(OSS_PREFIX) \
	  OSS_ACCESS_KEY_ID=$(OSS_ACCESS_KEY_ID) OSS_ACCESS_KEY_SECRET=$(OSS_ACCESS_KEY_SECRET) \
	  bash scripts/publish_latest.sh

# Convenience target for a one-platform local rehearsal of the build path
release-local: release-build
	@echo "==> built $(VERSION) for $(PLATFORM), output in dist/release/"
	@ls -la dist/release/
