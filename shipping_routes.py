# -*- coding: utf-8 -*-
import os
import subprocess
import sys
import threading
import time

from flask import Blueprint, render_template, jsonify

shipping_bp = Blueprint("shipping", __name__, url_prefix="/shipping")

_streamlit_process: subprocess.Popen | None = None
_streamlit_stderr_log: list[str] = []
_streamlit_lock = threading.Lock()

SHIPPING_PORT = int(os.environ.get("SHIPPING_APP_PORT", 8502))
SHIPPING_APP_URL = os.environ.get("SHIPPING_APP_URL", "/shipping/app/")


def _tail_stderr(proc: subprocess.Popen) -> None:
    for line in proc.stderr:
        decoded = line.decode("utf-8", errors="replace").rstrip()
        _streamlit_stderr_log.append(decoded)
        if len(_streamlit_stderr_log) > 200:
            _streamlit_stderr_log.pop(0)


def _streamlit_responding(timeout: float = 1.0) -> bool:
    import urllib.request
    try:
        urllib.request.urlopen(
            f"http://localhost:{SHIPPING_PORT}/shipping/app/_stcore/health",
            timeout=timeout,
        )
        return True
    except Exception:
        return False


def start_shipping_streamlit() -> None:
    global _streamlit_process
    with _streamlit_lock:
        if _streamlit_process is not None and _streamlit_process.poll() is None:
            return
        # Another gunicorn worker (or an orphan from a restarted worker) may
        # already serve the port; spawning again would just loop on
        # "Port 8502 is not available".
        if _streamlit_responding():
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
            stderr=subprocess.PIPE,
        )
        threading.Thread(
            target=_tail_stderr, args=(_streamlit_process,), daemon=True
        ).start()


@shipping_bp.route("/")
@shipping_bp.route("")
def shipping_hub():
    return render_template(
        "shipping_hub.html",
        streamlit_url=SHIPPING_APP_URL,
    )


@shipping_bp.route("/status")
def shipping_status():
    import urllib.request
    running = False
    try:
        urllib.request.urlopen(
            f"http://localhost:{SHIPPING_PORT}/shipping/app/_stcore/health", timeout=2
        )
        running = True
    except Exception:
        pass

    proc_alive = _streamlit_process is not None and _streamlit_process.poll() is None
    return jsonify({
        "streamlit_http_ok": running,
        "process_alive": proc_alive,
        "returncode": _streamlit_process.poll() if _streamlit_process else None,
        "recent_log": _streamlit_stderr_log[-40:],
    })
