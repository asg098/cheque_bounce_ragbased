"""
JudiQ AI — Desktop Standalone Executable Launcher
-------------------------------------------------
Initializes the unified JudiQ AI litigation platform backend, serves the frontend,
and launches the application in the system's default browser.
"""

import sys
import os
import time
import threading
import webbrowser
import argparse
from pathlib import Path

# Setup paths for frozen and regular execution
if getattr(sys, "frozen", False):
    ROOT_DIR = Path(getattr(sys, "_MEIPASS", Path(sys.executable).parent))
else:
    ROOT_DIR = Path(__file__).resolve().parent

BACKEND_DIR = ROOT_DIR / "backend"

if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(1, str(ROOT_DIR))

# Ensure current working directory is writable (for SQLite analytics.db)
if getattr(sys, "frozen", False):
    # Use the folder containing the exe for user data and SQLite persistence
    exe_dir = Path(sys.executable).parent
    os.chdir(str(exe_dir))
    if not os.environ.get("SQLITE_DB_PATH"):
        os.environ["SQLITE_DB_PATH"] = str(exe_dir / "analytics.db")

def print_banner(host: str, port: int):
    print("=" * 70)
    print("  JUDIQ AI — INSTITUTIONAL LITIGATION INTELLIGENCE PLATFORM")
    print("=" * 70)
    print("  * Dual Engine: Deterministic Statutory Audit & Adversarial Strategy")
    print("  * Section 138 NI Act, SARFAESI, Commercial Suits & Criminal Defense")
    print("  * 1,307+ Benchmark Test Suites Verified")
    print("-" * 70)
    print(f"  [+] Local Web App:   http://{host}:{port}")
    print(f"  [+] API Swagger:     http://{host}:{port}/docs")
    print(f"  [+] Health Probe:    http://{host}:{port}/health")
    print("-" * 70)
    print("  [*] Launching browser interface... Press Ctrl+C to stop.")
    print("=" * 70 + "\n")

def open_browser_delayed(url: str, delay: float = 1.2):
    def _open():
        time.sleep(delay)
        try:
            webbrowser.open(url)
        except Exception as e:
            print(f"  [!] Notice: Could not open browser automatically: {e}")
            print(f"  [*] Please open {url} in your web browser manually.")
    thread = threading.Thread(target=_open, daemon=True)
    thread.start()

def main():
    parser = argparse.ArgumentParser(description="JudiQ AI Litigation Platform Launcher")
    parser.add_argument("--host", default="127.0.0.1", help="Host address to bind (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8000, help="Port to bind (default: 8000)")
    parser.add_argument("--no-browser", action="store_true", help="Do not open browser automatically")
    args = parser.parse_args()

    host = args.host
    port = args.port

    print_banner(host, port)

    if not args.no_browser:
        open_browser_delayed(f"http://{host}:{port}")

    import uvicorn
    import backend.main as backend_main
    uvicorn.run(backend_main.app, host=host, port=port, log_level="info")

if __name__ == "__main__":
    main()
