# -*- mode: python ; coding: utf-8 -*-
"""
JudiQ AI — PyInstaller Spec File
Compiles the unified backend and embedded frontend into a standalone Windows executable.
"""

import sys
import os
from pathlib import Path

block_cipher = None

# In PyInstaller spec files, SPECPATH is pre-defined as the spec directory
try:
    ROOT_DIR = os.path.abspath(SPECPATH)
except NameError:
    ROOT_DIR = os.path.abspath(os.getcwd())

BACKEND_DIR = os.path.join(ROOT_DIR, 'backend')
FRONTEND_DIR = os.path.join(ROOT_DIR, 'frontend')

datas = [
    (FRONTEND_DIR, 'frontend'),
]

hidden_imports = [
    # Uvicorn ASGI internals
    'uvicorn',
    'uvicorn.logging',
    'uvicorn.loops',
    'uvicorn.loops.auto',
    'uvicorn.protocols',
    'uvicorn.protocols.http',
    'uvicorn.protocols.http.auto',
    'uvicorn.protocols.http.httptools_impl',
    'uvicorn.protocols.websockets',
    'uvicorn.protocols.websockets.auto',
    'uvicorn.lifespans',
    'uvicorn.lifespans.on',
    # FastAPI and Starlette
    'fastapi',
    'fastapi.middleware.cors',
    'fastapi.staticfiles',
    'starlette.staticfiles',
    'starlette.middleware.cors',
    'starlette.middleware.base',
    'starlette.middleware.errors',
    'starlette.applications',
    'starlette.responses',
    'starlette.routing',
    'pydantic',
    'pydantic_settings',
    # Core Domain & Statutory Engines
    'engine_core',
    'banking.recovery_engine',
    'banking.sarfaesi_engine',
    'criminal.criminal_adversarial_engine',
    'criminal.criminal_timeline_engine',
    'criminal.criminal_utils',
    'civil.civil_engine',
    'caseroom_logic',
    'documents',
    'metrics',
    'api_v1',
    'config',
    'session',
    'limiter',
    # Utilities & Dependencies
    'sqlite3',
    'jinja2',
    'psutil',
    'prometheus_client',
    'slowapi',
    'passlib',
    'passlib.handlers.bcrypt',
    'bcrypt',
    'cryptography',
]

a = Analysis(
    ['desktop_launcher.py'],
    pathex=[ROOT_DIR, BACKEND_DIR],
    binaries=[],
    datas=datas,
    hiddenimports=hidden_imports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=['tkinter', 'matplotlib', 'scipy', 'IPython', 'pytest'],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='JudiQ_AI',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=False,
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
    a.zipfiles,
    a.datas,
    strip=False,
    upx=False,
    upx_exclude=[],
    name='JudiQ_AI',
)
