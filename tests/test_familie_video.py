"""Family video calls: Render signs invitations only for rooms the Pi created for a
logged-in family member. Includes an end-to-end test in Chrome (skipped without it)."""
import json
import secrets
import shutil
import socket
import threading
import time

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import familie_proxy
import meet_routes


def test_video_view_signs_only_family_rooms(monkeypatch):
    monkeypatch.setattr(meet_routes, 'KODE', 'test-kode-123')
    assert familie_proxy._video_view({'room': 'mote-abc', 'name': 'X', 'created': time.time()}) is None
    assert familie_proxy._video_view({'room': 'familie-' + 'g' * 16, 'created': time.time()}) is None
    room = 'familie-' + secrets.token_hex(8)
    view = familie_proxy._video_view({'room': room, 'name': 'Robert', 'created': time.time()})
    assert view['active'] and view['participants'] == 0
    inv = view['url'].split('?inv=')[1]
    assert view['url'].startswith('/meet#' + room + '?inv=') and meet_routes._gyldig_invitasjon(inv, room)
    assert not meet_routes._gyldig_invitasjon(inv, 'familie-' + secrets.token_hex(8))
    old = familie_proxy._video_view({'room': room, 'name': 'Robert', 'created': time.time() - 600})
    assert not old['active'] and 'url' not in old  # nobody joined: no longer offered


def test_video_routes_require_family_session(monkeypatch):
    calls = []
    async def fake(method, path, session):
        calls.append((method, path, session))
        if session != 'ok':
            return 401, None
        if method == 'GET':
            return 200, {'call': None}
        return 200, {'call': {'room': 'familie-' + 'a' * 16, 'name': 'Robert', 'created': time.time()}, 'started': True}
    monkeypatch.setattr(familie_proxy, '_family_api', fake)
    app = FastAPI(); app.include_router(familie_proxy.router)
    with TestClient(app) as c:
        assert c.get('/familie/api/video').status_code == 401
        assert c.post('/familie/api/video/start', headers={'X-Family-Request': '1'}).status_code == 401
        c.cookies.set('motes_family_session', 'ok')
        assert c.post('/familie/api/video/start').status_code == 403
        r = c.post('/familie/api/video/start', headers={'X-Family-Request': '1'}).json()
        assert r['started'] is True and r['call']['url'].startswith('/meet#familie-')
        assert c.get('/familie/api/video').json() == {'call': None}


playwright_api = pytest.importorskip('playwright.sync_api')
uvicorn = pytest.importorskip('uvicorn')
CHROME = shutil.which('google-chrome') or shutil.which('chromium')


def _serve(app):
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        port = s.getsockname()[1]
    server = uvicorn.Server(uvicorn.Config(app, host='127.0.0.1', port=port, log_level='warning'))
    threading.Thread(target=server.run, daemon=True).start()
    while not server.started:
        time.sleep(.05)
    return server, port


@pytest.mark.skipif(not CHROME, reason='Chrome er ikke installert')
def test_family_members_start_and_join_the_same_call(tmp_path, monkeypatch):
    from familie.server import create_app, code_hash
    salt = secrets.token_hex(16)
    (tmp_path / 'access.json').write_text(json.dumps({'salt': salt, 'code_hash': code_hash('kode-for-test-123', salt), 'session_key': secrets.token_hex(32)}))
    pi, pi_port = _serve(create_app(tmp_path, secure_cookie=False))
    monkeypatch.setattr(familie_proxy, '_family_origin', lambda: f'http://127.0.0.1:{pi_port}')
    monkeypatch.setattr(meet_routes, 'KODE', 'test-kode-123')  # meetings require a code or invitation
    site = FastAPI(); site.include_router(meet_routes.router); site.include_router(familie_proxy.router)
    render, port = _serve(site)
    try:
        with playwright_api.sync_playwright() as p:
            browser = p.chromium.launch(executable_path=CHROME, args=['--no-sandbox', '--use-fake-device-for-media-stream', '--use-fake-ui-for-media-stream'])
            def member(name):
                ctx = browser.new_context(base_url=f'http://localhost:{port}', permissions=['camera', 'microphone'])
                page = ctx.new_page()
                page.goto('/familie/')
                page.fill('#name', name); page.fill('#code', 'kode-for-test-123'); page.click('#loginButton')
                page.wait_for_selector('#room:not([hidden])')
                return ctx, page
            def join(page, name):
                page.wait_for_url('**/meet#familie-*', timeout=15000)
                assert page.input_value('#nameIn') == name
                page.click('#joinBtn')
                page.wait_for_selector('#leaveBtn', state='visible', timeout=15000)
                return page.url.split('#')[1].split('?')[0]
            stranger = browser.new_context(base_url=f'http://localhost:{port}')
            assert stranger.request.get('/familie/api/video').status == 401
            robert_ctx, robert = member('Robert')
            assert robert.is_hidden('#videoBanner')
            robert.click('#startVideo')
            room = join(robert, 'Robert')
            gert_ctx, gert = member('Gert')
            gert.wait_for_selector('#videoBanner:not([hidden])', timeout=10000)
            assert 'startet av Robert' in gert.text_content('#videoBannerText')
            gert.click('#joinVideo')
            assert join(gert, 'Gert') == room
            ole_ctx, ole = member('Ole-Andreas')
            ole.click('#startVideo')  # a call is going on: joins it instead of starting another
            assert join(ole, 'Ole-Andreas') == room
            for _ in range(50):
                if len(meet_routes._rom.get(room, {})) == 3:
                    break
                time.sleep(.2)
            assert len(meet_routes._rom.get(room, {})) == 3
            browser.close()
    finally:
        render.should_exit = True
        pi.should_exit = True
