.PHONY: test unit build clean smoke

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
