# -*- coding: utf-8 -*-
import os
import subprocess
import sys
import threading
import time

from flask import Blueprint, render_template

shipping_bp = Blueprint("shipping", __name__, url_prefix="/shipping")

_streamlit_process: subprocess.Popen | None = None
_streamlit_lock = threading.Lock()

SHIPPING_PORT = int(os.environ.get("SHIPPING_APP_PORT", 8502))
SHIPPING_APP_URL = os.environ.get("SHIPPING_APP_URL", "/shipping/app")


def start_shipping_streamlit() -> None:
    global _streamlit_process
    with _streamlit_lock:
        if _streamlit_process is not None and _streamlit_process.poll() is None:
            return
        cmd = [
            sys.executable, "-m", "streamlit", "run",
            "shipping_main.py",
            "--server.port", str(SHIPPING_PORT),
            "--server.address", "localhost",
            "--server.headless", "true",
            "--server.enableCORS", "false",
            "--server.enableXsrfProtection", "false",
            "--browser.gatherUsageStats", "false",
            "--server.baseUrlPath", "/shipping/app",
        ]
        _streamlit_process = subprocess.Popen(
            cmd,
            cwd=os.path.dirname(os.path.abspath(__file__)),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )


def _wait_for_streamlit(timeout: int = 30) -> bool:
    import urllib.request
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            urllib.request.urlopen(f"http://localhost:{SHIPPING_PORT}/_stcore/health", timeout=2)
            return True
        except Exception:
            time.sleep(1)
    return False


@shipping_bp.route("/")
@shipping_bp.route("")
def shipping_hub():
    return render_template(
        "shipping_hub.html",
        streamlit_url=SHIPPING_APP_URL,
    )
