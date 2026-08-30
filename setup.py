from pathlib import Path
from runpy import run_path

from setuptools import find_packages, setup

ROOT = Path(__file__).resolve().parent
README = ROOT / "README.md"
VERSION = run_path(str(ROOT / "src" / "maxc_cli" / "__init__.py"))["__version__"]


setup(
    name="maxc-cli",
    version=VERSION,
    description="Agent-native MaxCompute CLI for external coding agents",
    long_description=README.read_text(encoding="utf-8"),
    long_description_content_type="text/markdown",
    python_requires=">=3.9",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    # Skill files are runtime resources, not part of the public Python package
    # API. Keep the install surface explicit so setuptools does not auto-discover
    # the resource-only directories as namespace packages.
    include_package_data=False,
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
