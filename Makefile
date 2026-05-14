.PHONY: test unit build clean smoke build-bin bin-smoke

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
