"""Calendar navigation in real Chrome: arrows skip empty months, month picker jumps.
Skipped when Playwright or Chrome is unavailable."""
import json
import secrets
import shutil
import socket
import sqlite3
import threading
import time
from datetime import datetime

import pytest
from PIL import Image

playwright_api = pytest.importorskip('playwright.sync_api')
uvicorn = pytest.importorskip('uvicorn')
CHROME = shutil.which('google-chrome') or shutil.which('chromium')
pytestmark = pytest.mark.skipif(not CHROME, reason='Chrome er ikke installert')

from familie.server import create_app, code_hash


def test_calendar_skips_empty_months_and_jumps(tmp_path):
    salt = secrets.token_hex(16)
    (tmp_path / 'access.json').write_text(json.dumps({'salt': salt, 'code_hash': code_hash('kode-for-test-123', salt), 'session_key': secrets.token_hex(32)}))
    app = create_app(tmp_path, secure_cookie=False)
    with sqlite3.connect(tmp_path / 'posts.sqlite3') as conn:
        for year, month in [(2024, 3), (2025, 7), (2026, 11)]:
            ident = secrets.token_hex(16)
            Image.new('RGB', (40, 40), 'red').save(tmp_path / 'media' / (ident + '.png'))
            conn.execute('INSERT INTO posts(id,mime,suffix,name,caption,created,expires,bytes) VALUES(?,?,?,?,?,?,?,?)',
                         (ident, 'image/png', '.png', 'Test', '', datetime(year, month, 10, 12).timestamp(), 0, 100))
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        port = s.getsockname()[1]
    server = uvicorn.Server(uvicorn.Config(app, host='127.0.0.1', port=port, log_level='warning'))
    threading.Thread(target=server.run, daemon=True).start()
    while not server.started:
        time.sleep(.05)
    try:
        with playwright_api.sync_playwright() as p:
            browser = p.chromium.launch(executable_path=CHROME, args=['--no-sandbox'])
            page = browser.new_page(base_url=f'http://localhost:{port}', viewport={'width': 1200, 'height': 900})
            page.goto('/familie/')
            page.fill('#name', 'Test')
            page.fill('#code', 'kode-for-test-123')
            page.click('#loginButton')
            page.wait_for_selector('#room:not([hidden])')
            page.click('#calendar summary')
            page.wait_for_function('document.querySelectorAll("#calendarJump option").length>=3')
            month = lambda: page.text_content('#calendarMonth')
            page.select_option('#calendarJump', '2026-11')
            assert month() == 'november 2026' and page.is_disabled('#nextMonth')
            page.click('#previousMonth')
            assert month() == 'juli 2025'
            page.click('#previousMonth')
            assert month() == 'mars 2024' and page.is_disabled('#previousMonth')
            page.click('#nextMonth')
            assert month() == 'juli 2025'
            options = page.eval_on_selector_all('#calendarJump option', 'e=>e.map(x=>x.value)')
            assert options[-3:] == ['2026-11', '2025-07', '2024-03']
            browser.close()
    finally:
        server.should_exit = True
