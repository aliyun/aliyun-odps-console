# -*- mode: python ; coding: utf-8 -*-
from PyInstaller.utils.hooks import collect_data_files

datas = collect_data_files('maxc_cli')
binaries = []
hiddenimports = []

# PyODPS exposes integrations for many optional ecosystems. PyInstaller sees
# those guarded imports when the build machine happens to have the packages
# installed and would otherwise ship a machine-dependent 100+ MB bundle. MaxC
# uses the core ODPS client/Tunnel paths and deliberately has no pandas/NumPy,
# notebook, SQLAlchemy, async-web, telemetry, or test-runner runtime surface.
optional_runtime_excludes = [
    'IPython',
    'PIL',
    '_pytest',
    'aiohttp',
    'apscheduler',
    'grpc',
    'matplotlib',
    'notebook',
    'numpy',
    'opentelemetry',
    'pandas',
    'pyarrow',
    'pytest',
    'redis',
    'sqlalchemy',
    'tkinter',
    'uvloop',
]


a = Analysis(
    ['scripts/pyinstaller_entry.py'],
    pathex=['src'],
    binaries=binaries,
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=['libcst', *optional_runtime_excludes],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='maxc',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='maxc',
)
