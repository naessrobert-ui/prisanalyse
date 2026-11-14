import io
import json
import secrets
import sqlite3
import time

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from PIL import Image

from familie.server import COOKIE, code_hash, create_app


@pytest.fixture
def family(tmp_path):
    salt = secrets.token_hex(16)
    (tmp_path / 'access.json').write_text(json.dumps({'salt': salt, 'code_hash': code_hash('familie-test-kode', salt), 'session_key': secrets.token_hex(32)}))
    app = create_app(tmp_path, secure_cookie=False)
    with TestClient(app, base_url='http://localhost') as client:
        yield app, client, tmp_path


def login(client, name='Robert'):
    return client.post('/familie/api/login', json={'name': name, 'code': 'familie-test-kode'}, headers={'X-Family-Request': '1'})


def photo():
    data = io.BytesIO()
    Image.new('RGB', (10, 10), '#216052').save(data, format='PNG')
    return data.getvalue()


def upload(client, days=1, caption='En hilsen'):
    return client.post('/familie/api/items', params={'days': days, 'caption': caption}, content=photo(), headers={'X-Family-Request': '1', 'Content-Type': 'image/png'})


def test_private_login_cookie_and_no_unauthed_files(family):
    app, client, root = family
    assert client.get('/familie').status_code == 200
    assert client.get('/familie/api/items').status_code == 401
    assert client.get('/familie/media/' + 'a' * 32).status_code == 401
    assert client.post('/familie/api/login', json={'name': 'X', 'code': 'familie-test-kode'}).status_code == 403
    assert client.post('/familie/api/login', json={'name': 'X', 'code': 'feil'}, headers={'X-Family-Request': '1'}).status_code == 401
    response = login(client)
    assert response.status_code == 200
    cookie = response.headers['set-cookie']
    assert 'HttpOnly' in cookie and 'SameSite=strict' in cookie and 'Path=/familie' in cookie
    assert 'familie-test-kode' not in cookie
    assert client.get('/familie/api/items').json() == {'name': 'Robert', 'items': []}
    # Modification without the request header is rejected even with a session.
    assert client.post('/familie/api/items', content=photo(), headers={'Content-Type': 'image/png'}).status_code == 403


def test_media_roundtrip_range_logout_and_deletion(family):
    app, client, root = family
    login(client)
    response = upload(client)
    assert response.status_code == 200
    ident = response.json()['id']
    listing = client.get('/familie/api/items').json()['items']
    assert listing[0]['name'] == 'Robert' and listing[0]['caption'] == 'En hilsen'
    path = '/familie/media/' + ident
    media = client.get(path)
    assert media.content == photo() and media.headers['content-type'] == 'image/png'
    assert media.headers['cache-control'] == 'private, no-store'
    assert 'noindex' in media.headers['x-robots-tag']
    partial = client.get(path, headers={'range': 'bytes=0-9'})
    assert partial.status_code == 206 and partial.content == photo()[:10]
    assert client.head(path).status_code == 200
    client.post('/familie/api/logout', headers={'X-Family-Request': '1'})
    assert client.get(path).status_code == 401
    login(client)
    assert client.delete('/familie/api/items/' + ident, headers={'X-Family-Request': '1'}).status_code == 200
    assert client.get(path).status_code == 404
    assert not list((root / 'media').iterdir())


def test_expired_posts_hidden_before_cleanup_and_bytes_deleted(family):
    app, client, root = family
    login(client)
    ident = upload(client, days=3).json()['id']
    with sqlite3.connect(root / 'posts.sqlite3') as conn:
        row = conn.execute('SELECT created,expires FROM posts WHERE id=?', (ident,)).fetchone()
        assert row[1] - row[0] == 3 * 86400
        conn.execute('UPDATE posts SET expires=? WHERE id=?', (time.time() - 1, ident))
    assert client.get('/familie/api/items').json()['items'] == []
    assert client.get('/familie/media/' + ident).status_code == 404
    assert list((root / 'media').iterdir())
    app.state.cleanup()
    assert not list((root / 'media').iterdir())
    with sqlite3.connect(root / 'posts.sqlite3') as conn:
        assert conn.execute('SELECT COUNT(*) FROM posts').fetchone()[0] == 0


def test_invalid_retention_files_and_upload_limits(family, monkeypatch):
    import familie.server as server
    app, client, root = family
    login(client)
    for days in (0, 2, 8, 'forever'):
        assert upload(client, days).status_code == 400
    headers = {'X-Family-Request': '1', 'Content-Type': 'image/png'}
    assert client.post('/familie/api/items', content=b'<script>bad</script>', headers=headers).status_code == 415
    assert client.post('/familie/api/items', content=b'', headers=headers).status_code == 400
    assert client.post('/familie/api/items', content=b'<svg/>', headers={**headers, 'Content-Type': 'image/svg+xml'}).status_code == 415
    assert client.post('/familie/api/items', content=b'fake-video', headers={**headers, 'Content-Type': 'video/mp4'}).status_code == 415
    monkeypatch.setattr(server, 'MAX_FILE', 10)
    assert upload(client).status_code == 413
    assert not list((root / 'media').iterdir())
    monkeypatch.setattr(server, 'MAX_TOTAL', 1)
    assert upload(client).status_code == 507


def test_bad_session_rate_limit_and_private_headers(family):
    app, client, root = family
    client.cookies.set(COOKIE, 'forged', domain='localhost.local', path='/familie')
    assert client.get('/familie/api/items').status_code == 401
    for _ in range(10):
        assert client.post('/familie/api/login', json={'code': 'bad', 'name': 'X'}, headers={'X-Family-Request': '1'}).status_code == 401
    assert login(client).status_code == 429
    page = client.get('/familie')
    assert "frame-ancestors 'none'" in page.headers['content-security-policy']
    assert page.headers['referrer-policy'] == 'no-referrer'


def test_restart_retains_media_and_requires_config(family):
    app, client, root = family
    login(client)
    ident = upload(client, days=7).json()['id']
    with TestClient(create_app(root, secure_cookie=False), base_url='http://localhost') as restarted:
        assert restarted.get('/familie/api/items').status_code == 401
        login(restarted)
        assert restarted.get('/familie/media/' + ident).content == photo()
    (root / 'access.json').unlink()
    with pytest.raises(FileNotFoundError):
        create_app(root)


def test_proxy_streams_range_and_only_family_cookie(monkeypatch):
    import familie_proxy
    monkeypatch.setenv('FAMILY_ORIGIN', 'https://raspberrypi.example.ts.net:8443')
    seen = []

    class Body(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b'abcd'

    async def upstream(request):
        seen.append(request)
        assert await request.aread() == b''
        return httpx.Response(206, headers={'Content-Type': 'video/mp4', 'Content-Range': 'bytes 0-3/20', 'Accept-Ranges': 'bytes', 'Cache-Control': 'private, no-store'}, stream=Body())

    real_client = httpx.AsyncClient
    monkeypatch.setattr(familie_proxy.httpx, 'AsyncClient', lambda **kwargs: real_client(transport=httpx.MockTransport(upstream), **kwargs))
    app = FastAPI()
    app.include_router(familie_proxy.router)
    with TestClient(app) as client:
        response = client.get('/familie/media/abc', headers={'Range': 'bytes=0-3', 'Cookie': 'session=prisanalyse-secret; motes_family_session=family-token', 'Authorization': 'Bearer main-site-secret'})
    assert response.status_code == 206 and response.content == b'abcd'
    assert response.headers['content-range'] == 'bytes 0-3/20'
    assert str(seen[0].url) == 'https://raspberrypi.example.ts.net:8443/familie/media/abc'
    assert seen[0].headers['cookie'] == 'motes_family_session=family-token'
    assert 'authorization' not in seen[0].headers


@pytest.mark.parametrize('origin', ['', 'http://100.106.5.19:8025', 'https://example.com', 'https://pi.test.ts.net/other', 'https://pi.test.ts.net:broken', 'https://user:secret@pi.test.ts.net'])
def test_proxy_rejects_unconfigured_or_unsafe_origin(monkeypatch, origin):
    import familie_proxy
    monkeypatch.setenv('FAMILY_ORIGIN', origin)
    app = FastAPI()
    app.include_router(familie_proxy.router)
    assert TestClient(app).get('/familie').status_code == 503


def test_proxy_upload_and_login_cookie_forwarded_without_redirects(monkeypatch):
    import familie_proxy
    monkeypatch.setenv('FAMILY_ORIGIN', 'https://pi.example.ts.net:8443')
    closed = []

    class Body(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b'{"ok":true}'
        async def aclose(self):
            closed.append(True)

    async def upstream(request):
        assert await request.aread() == b'upload-bytes'
        assert request.headers['x-family-request'] == '1'
        assert request.headers['accept-encoding'] == 'identity'
        assert request.url.query == b'days=3'
        return httpx.Response(200, headers={'Set-Cookie': 'motes_family_session=signed; Path=/familie; Secure; HttpOnly; SameSite=strict', 'Content-Type': 'application/json'}, stream=Body())

    real_client = httpx.AsyncClient
    monkeypatch.setattr(familie_proxy.httpx, 'AsyncClient', lambda **kwargs: real_client(transport=httpx.MockTransport(upstream), **kwargs))
    app = FastAPI()
    app.include_router(familie_proxy.router)
    with TestClient(app) as client:
        response = client.post('/familie/api/items?days=3', content=b'upload-bytes', headers={'X-Family-Request': '1'})
    assert response.json() == {'ok': True}
    assert 'HttpOnly' in response.headers['set-cookie']
    assert closed


def test_unreachable_pi_has_controlled_response(monkeypatch):
    import familie_proxy
    monkeypatch.setenv('FAMILY_ORIGIN', 'https://pi.example.ts.net:8443')

    async def upstream(request):
        raise httpx.ConnectError('offline')

    real_client = httpx.AsyncClient
    monkeypatch.setattr(familie_proxy.httpx, 'AsyncClient', lambda **kwargs: real_client(transport=httpx.MockTransport(upstream), **kwargs))
    app = FastAPI()
    app.include_router(familie_proxy.router)
    response = TestClient(app).get('/familie')
    assert response.status_code == 503
    assert response.headers['cache-control'] == 'no-store'
