from pathlib import Path

from setuptools import find_packages, setup


ROOT = Path(__file__).resolve().parent
README = ROOT / "README.md"


setup(
    name="maxc-cli",
    version="0.4.1",
    description="Agent-native MaxCompute CLI for external coding agents",
    long_description=README.read_text(encoding="utf-8"),
    long_description_content_type="text/markdown",
    python_requires=">=3.9",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    include_package_data=True,
    package_data={
        "maxc_cli": [
            "skills/SKILL.md",
            "skills/references/*.md",
            "skills/agents/*.yaml",
        ],
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
        "Programming Language :: Python :: 3.14",
    ],
    install_requires=[
        "PyYAML>=5.4",
        "pyodps>=0.12.0",
    ],
    entry_points={
        "console_scripts": [
            "maxc=maxc_cli.__main__:main",
        ],
    },
)
