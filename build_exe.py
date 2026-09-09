"""
JudiQ AI — Windows Executable Compiler Automation
------------------------------------------------
Compiles the application into dist/JudiQ_AI/ using PyInstaller.
"""

import os
import sys
import subprocess
import shutil
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent

def run_build():
    print("=" * 70)
    print("  JUDIQ AI — BUILDING WINDOWS STANDALONE EXECUTABLE (.EXE)")
    print("=" * 70)

    # 1. Regenerate frontend production asset manifest
    print("[1/3] Updating frontend asset SRI manifest...")
    manifest_script = ROOT_DIR / "frontend" / "build_prod.py"
    if manifest_script.exists():
        subprocess.run([sys.executable, str(manifest_script)], check=True)

    # 2. Invoke PyInstaller with judiq_ai.spec
    spec_file = ROOT_DIR / "judiq_ai.spec"
    if not spec_file.exists():
        print(f"[-] Error: Spec file not found at {spec_file}")
        sys.exit(1)

    print("\n[2/3] Executing PyInstaller compilation...")
    cmd = [
        sys.executable,
        "-m",
        "PyInstaller",
        "--clean",
        "--noconfirm",
        str(spec_file)
    ]
    result = subprocess.run(cmd)
    if result.returncode != 0:
        print(f"[-] PyInstaller compilation failed with exit code {result.returncode}")
        sys.exit(result.returncode)

    # 3. Verify output
    dist_dir = ROOT_DIR / "dist" / "JudiQ_AI"
    exe_file = dist_dir / "JudiQ_AI.exe"

    print("\n[3/3] Verifying output artifacts...")
    if exe_file.exists():
        size_mb = exe_file.stat().st_size / (1024 * 1024)
        print("=" * 70)
        print(f"  [+] SUCCESS! Executable built successfully:")
        print(f"      Location: {exe_file}")
        print(f"      Size:     {size_mb:.2f} MB")
        print(f"      Folder:   {dist_dir}")
        print("=" * 70)
    else:
        print(f"[-] Error: Executable not found at expected path: {exe_file}")
        sys.exit(1)

if __name__ == "__main__":
    run_build()
