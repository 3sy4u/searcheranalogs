# -*- mode: python ; coding: utf-8 -*-
import os
import sys

block_cipher = None

a = Analysis(
    ['main.py'],
    pathex=['.'],
    binaries=[],
    datas=[
        ('service_account.json', '.'),
        ('Sansation-Bold.ttf', '.'),
        ('app_icon.ico', '.'),
    ],
    hiddenimports=[
        'pandas',
        'openpyxl',
        'openpyxl.cell._writer',
        'gspread',
        'google.oauth2.service_account',
        'google.auth.transport.requests',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        'matplotlib',
        'scipy',
        'tkinter',
        'IPython',
        'PyQt6.QtMultimedia',
        'PyQt6.QtMultimediaWidgets',
        'PyQt6.QtOpenGL',
        'PyQt6.QtOpenGLWidgets',
        'PyQt6.QtWebEngineCore',
        'PyQt6.QtWebEngineWidgets',
        'PyQt6.QtDesigner',
    ],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='YNICDLSsearcher',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,
    disable_windowed_traceback=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon='app_icon.ico',
)