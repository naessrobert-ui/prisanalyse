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
    assert media.headers['cache-control'] == 'private, no-cache, must-revalidate'
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
    for days in (-1, 2, 8, 'forever'):
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


def test_chat_private_ordered_idempotent_and_retained(family):
    app, client, root = family
    headers = {'X-Family-Request': '1'}
    ident = secrets.token_hex(16)
    message = {'client_id': ident, 'text': 'Hei familien <script>test</script>'}
    assert client.get('/familie/api/chat').status_code == 401
    assert client.post('/familie/api/chat', json=message, headers=headers).status_code == 401
    login(client)
    assert client.post('/familie/api/chat', json=message).status_code == 403
    assert client.post('/familie/api/chat', json=message, headers=headers).status_code == 200
    assert client.post('/familie/api/chat', json=message, headers=headers).status_code == 200
    listing = client.get('/familie/api/chat').json()
    assert listing['name'] == 'Robert' and len(listing['messages']) == 1
    item = listing['messages'][0]
    assert item['text'] == message['text'] and item['name'] == 'Robert'
    assert item['expires'] - item['created'] == 7 * 86400
    assert client.post('/familie/api/chat', json={**message, 'text': 'Endret'}, headers=headers).status_code == 409
    assert client.delete('/familie/api/chat/' + ident).status_code == 403
    assert client.delete('/familie/api/chat/' + ident, headers=headers).status_code == 200
    assert client.get('/familie/api/chat').json()['messages'] == []


def test_chat_expiry_validation_and_restart(family):
    app, client, root = family
    login(client)
    headers = {'X-Family-Request': '1'}
    for text in ('', ' ' * 4, 'x' * 2001, 4, None):
        assert client.post('/familie/api/chat', json={'client_id': secrets.token_hex(16), 'text': text}, headers=headers).status_code == 400
    assert client.post('/familie/api/chat', content=b'x' * 17000, headers=headers).status_code == 413
    ident = secrets.token_hex(16)
    client.post('/familie/api/chat', json={'client_id': ident, 'text': 'En melding'}, headers=headers)
    with TestClient(create_app(root, secure_cookie=False), base_url='http://localhost') as restarted:
        login(restarted)
        assert restarted.get('/familie/api/chat').json()['messages'][0]['id'] == ident
    with sqlite3.connect(root / 'posts.sqlite3') as conn:
        conn.execute('UPDATE messages SET expires=?', (time.time() - 1,))
    assert client.get('/familie/api/chat').json()['messages'] == []
    app.state.cleanup()
    with sqlite3.connect(root / 'posts.sqlite3') as conn:
        assert conn.execute('SELECT COUNT(*) FROM messages').fetchone()[0] == 0


def test_chat_rate_limit(family):
    app, client, root = family
    login(client)
    now = time.time()
    with sqlite3.connect(root / 'posts.sqlite3') as conn:
        conn.executemany('INSERT INTO messages (id,name,text,created,expires) VALUES (?,?,?,?,?)', [(secrets.token_hex(16), 'Familien', 'Hei', now, now + 86400) for _ in range(60)])
    assert client.post('/familie/api/chat', json={'client_id': secrets.token_hex(16), 'text': 'For raskt'}, headers={'X-Family-Request': '1'}).status_code == 429


def test_post_likes_comments_are_private_and_idempotent(family):
    app, client, root = family
    headers = {'X-Family-Request': '1'}
    login(client)
    post_id = upload(client).json()['id']
    like_url = '/familie/api/items/' + post_id + '/like'
    comments_url = '/familie/api/items/' + post_id + '/comments'
    assert client.post(like_url, json={'liked': True}).status_code == 403
    assert client.post(like_url, json={'liked': 'yes'}, headers=headers).status_code == 400
    for _ in range(2):
        assert client.post(like_url, json={'liked': True}, headers=headers).json() == {'liked': True, 'likes': ['Robert']}
    ident = secrets.token_hex(16)
    comment = {'client_id': ident, 'text': 'Så fint! <img src=x onerror=alert(1)>'}
    for _ in range(2):
        assert client.post(comments_url, json=comment, headers=headers).status_code == 200
    assert len(client.get(comments_url).json()['comments']) == 1
    post = client.get('/familie/api/items').json()['items'][0]
    assert post['liked'] is True and post['likes'] == ['Robert'] and post['comments'] == 1
    login(client, 'Anna')
    post = client.get('/familie/api/items').json()['items'][0]
    assert post['liked'] is False
    client.post(like_url, json={'liked': True}, headers=headers)
    assert client.post(like_url, json={'liked': False}, headers=headers).json()['likes'] == ['Robert']
    assert client.delete(comments_url + '/' + ident, headers=headers).status_code == 200
    assert client.get(comments_url).json()['comments'] == []
    client.post('/familie/api/logout', headers=headers)
    assert client.get(comments_url).status_code == 401
    assert client.post(like_url, json={'liked': True}, headers=headers).status_code == 401


def test_expired_or_deleted_posts_reject_and_remove_social_content(family):
    app, client, root = family
    headers = {'X-Family-Request': '1'}
    login(client)
    for deleted in (False, True):
        post_id = upload(client).json()['id']
        base = '/familie/api/items/' + post_id
        client.post(base + '/like', json={'liked': True}, headers=headers)
        client.post(base + '/comments', json={'client_id': secrets.token_hex(16), 'text': 'En kommentar'}, headers=headers)
        if deleted:
            assert client.delete(base, headers=headers).status_code == 200
        else:
            with sqlite3.connect(root / 'posts.sqlite3') as conn:
                conn.execute('UPDATE posts SET expires=? WHERE id=?', (time.time() - 1, post_id))
        assert client.get(base + '/comments').status_code == 404
        assert client.post(base + '/like', json={'liked': True}, headers=headers).status_code == 404
        app.state.cleanup()
        with sqlite3.connect(root / 'posts.sqlite3') as conn:
            assert conn.execute('SELECT COUNT(*) FROM comments WHERE post_id=?', (post_id,)).fetchone()[0] == 0
            assert conn.execute('SELECT COUNT(*) FROM reactions WHERE post_id=?', (post_id,)).fetchone()[0] == 0


def test_mobile_mime_detection_and_heic_conversion(family):
    from pillow_heif import from_pillow
    app, client, root = family
    login(client)
    # Some phones supply a MIME hint that differs from the encoded image.
    response = client.post('/familie/api/items', content=photo(), headers={'X-Family-Request': '1', 'Content-Type': 'image/jpeg'})
    assert response.status_code == 200
    ident = response.json()['id']
    assert client.get('/familie/media/' + ident).headers['content-type'] == 'image/png'
    data = io.BytesIO()
    from_pillow(Image.new('RGB', (24, 16), '#216052')).save(data)
    for mime in ('image/heic', 'image/jpeg'):
        response = client.post('/familie/api/items', content=data.getvalue(), headers={'X-Family-Request': '1', 'Content-Type': mime})
        assert response.status_code == 200, response.text
        media = client.get('/familie/media/' + response.json()['id'])
        assert media.headers['content-type'] == 'image/jpeg'
        with Image.open(io.BytesIO(media.content)) as image:
            assert image.format == 'JPEG' and image.size == (24, 16)


def test_capture_metadata_calendar_and_retention_are_independent(family):
    app, client, root = family
    assert client.get('/familie/api/calendar').status_code == 401
    login(client)
    data = io.BytesIO()
    exif = Image.Exif()
    exif[34665] = {36867: '2024:07:02 23:45:00', 36881: '+02:00'}
    Image.new('RGB', (10, 10)).save(data, format='JPEG', exif=exif)
    response = client.post('/familie/api/items', params={'days': 3}, content=data.getvalue(), headers={'X-Family-Request': '1', 'Content-Type': 'image/jpeg'})
    assert response.status_code == 200
    item = client.get('/familie/api/items').json()['items'][0]
    assert item['recorded_at'] == '2024-07-02T23:45:00+02:00'
    assert item['calendar_day'] == '2024-07-02'
    assert item['expires'] - item['created'] == 3 * 86400
    assert client.get('/familie/api/calendar').json()['days'] == {'2024-07-02': 1}
    assert len(client.get('/familie/api/items?day=2024-07-02').json()['items']) == 1
    assert not client.get('/familie/api/items?day=2024-07-03').json()['items']
    client.delete('/familie/api/items/' + item['id'], headers={'X-Family-Request': '1'})
    assert not client.get('/familie/api/calendar').json()['days']


def test_calendar_reaches_posts_beyond_latest_200(family):
    app, client, root = family
    login(client)
    now = time.time()
    with sqlite3.connect(root / 'posts.sqlite3') as conn:
        for number in range(205):
            conn.execute('INSERT INTO posts (id,mime,suffix,name,caption,created,expires,bytes,recorded_at) VALUES (?,?,?,?,?,?,?,?,?)',
                         (f'{number:032x}', 'image/png', '.png', 'Robert', '', now + number, now + 86400, 1,
                          '2020-01-01T12:00:00' if number == 0 else '2026-11-14T12:00:00'))
    assert len(client.get('/familie/api/items').json()['items']) == 200
    assert client.get('/familie/api/calendar').json()['days']['2020-01-01'] == 1
    assert len(client.get('/familie/api/items?day=2020-01-01').json()['items']) == 1


def test_mp4_capture_time_and_bad_metadata(tmp_path):
    from datetime import datetime, timezone
    from familie.server import recording_time
    def atom(kind, data):
        return (len(data) + 8).to_bytes(4, 'big') + kind + data
    seconds = int((datetime(2024, 8, 9, 12, tzinfo=timezone.utc) - datetime(1904, 1, 1, tzinfo=timezone.utc)).total_seconds())
    path = tmp_path / 'clip.mp4'
    for version in (0, 1):
        payload = bytes([version, 0, 0, 0]) + seconds.to_bytes(8 if version else 4, 'big') + b'\0' * 8
        path.write_bytes(atom(b'ftyp', b'isom\0\0\0\0') + atom(b'moov', atom(b'mvhd', payload)))
        assert recording_time(path, 'video/mp4') == ('2024-08-09T12:00:00+00:00', 'video_metadata')
    path.write_bytes(atom(b'moov', b'\0\0\0\x04mvhd'))
    assert recording_time(path, 'video/mp4') == (None, None)


def test_keep_until_deleted_survives_cleanup_and_restart(family):
    app, client, root = family
    login(client)
    ident = upload(client, days=0).json()['id']
    item = client.get('/familie/api/items').json()['items'][0]
    assert item['expires'] == 0
    app.state.cleanup()
    assert client.get('/familie/media/' + ident).status_code == 200
    assert client.get('/familie/api/calendar').json()['days'][item['calendar_day']] == 1
    assert client.post('/familie/api/items/' + ident + '/like', json={'liked': True}, headers={'X-Family-Request': '1'}).status_code == 200
    with TestClient(create_app(root, secure_cookie=False), base_url='http://localhost') as restarted:
        login(restarted)
        assert restarted.get('/familie/media/' + ident).status_code == 200
        assert restarted.delete('/familie/api/items/' + ident, headers={'X-Family-Request': '1'}).status_code == 200
        assert restarted.get('/familie/media/' + ident).status_code == 404
    assert not list((root / 'media').iterdir())


def test_legacy_database_capture_backfill_preserves_posts(tmp_path):
    salt = secrets.token_hex(16)
    (tmp_path / 'access.json').write_text(json.dumps({'salt': salt, 'code_hash': code_hash('familie-test-kode', salt), 'session_key': secrets.token_hex(32)}))
    media = tmp_path / 'media'
    media.mkdir()
    ident = 'a' * 32
    exif = Image.Exif()
    exif[34665] = {36867: '2021:08:09 10:11:12'}
    Image.new('RGB', (10, 10)).save(media / (ident + '.jpg'), exif=exif)
    now = time.time()
    with sqlite3.connect(tmp_path / 'posts.sqlite3') as conn:
        conn.execute('CREATE TABLE posts (id TEXT PRIMARY KEY,mime TEXT NOT NULL,suffix TEXT NOT NULL,name TEXT NOT NULL,caption TEXT NOT NULL,created REAL NOT NULL,expires REAL NOT NULL,bytes INTEGER NOT NULL)')
        conn.execute('INSERT INTO posts VALUES (?,?,?,?,?,?,?,?)', (ident, 'image/jpeg', '.jpg', 'Robert', 'Gammelt bilde', now, now + 86400, 100))
    app = create_app(tmp_path, secure_cookie=False)
    with TestClient(app, base_url='http://localhost') as client:
        login(client)
        item = client.get('/familie/api/items').json()['items'][0]
        assert item['recorded_at'] == '2021-08-09T10:11:12'
        assert item['caption'] == 'Gammelt bilde'
        assert item['expires'] == now + 86400
        assert client.get('/familie/media/' + ident).status_code == 200


def test_camera_mpo_jpeg_is_accepted(family):
    app, client, root = family
    login(client)
    data = io.BytesIO()
    Image.new('RGB', (20, 20), 'blue').save(data, format='MPO', save_all=True, append_images=[Image.new('RGB', (20, 20), 'red')])
    response = client.post('/familie/api/items', content=data.getvalue(), headers={'X-Family-Request': '1', 'Content-Type': 'image/jpeg'})
    assert response.status_code == 200, response.text
    media = client.get('/familie/media/' + response.json()['id'])
    with Image.open(io.BytesIO(media.content)) as image:
        assert image.format in {'JPEG', 'MPO'}


def test_genealogy_private_upload_and_sandbox(family):
    app, client, root = family
    assert client.get('/familie/api/slekt').status_code == 401
    assert client.get('/familie/slekt/vis').status_code == 401
    assert client.get('/familie/slekt', follow_redirects=False).status_code == 303
    login(client)
    headers = {'X-Family-Request': '1', 'Content-Type': 'text/html'}
    assert client.get('/familie/api/slekt').json() == {'exists': False}
    assert client.post('/familie/api/slekt', content=b'<html></html>', headers={'Content-Type': 'text/html'}).status_code == 403
    assert client.post('/familie/api/slekt', content=b'not html', headers=headers).status_code == 415
    html = b'<!doctype html><html><body><script>document.body.textContent="Tree"</script></body></html>'
    assert client.post('/familie/api/slekt', content=html, headers=headers).status_code == 200
    assert client.get('/familie/api/slekt').json()['exists'] is True
    response = client.get('/familie/slekt/vis')
    assert response.content == html
    policy = response.headers['content-security-policy']
    assert 'sandbox allow-scripts allow-downloads' in policy and 'allow-same-origin' not in policy
    assert "frame-ancestors 'self'" in policy and "default-src 'none'" in policy
    assert response.headers['cache-control'] == 'private, no-store'
    app.state.cleanup()
    assert (root / 'slekt.html').read_bytes() == html
    with TestClient(create_app(root, secure_cookie=False), base_url='http://localhost') as restarted:
        login(restarted)
        assert restarted.get('/familie/slekt/vis').content == html


def push_subscription():
    import base64
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.hazmat.primitives import serialization
    public = ec.generate_private_key(ec.SECP256R1()).public_key().public_bytes(serialization.Encoding.X962, serialization.PublicFormat.UncompressedPoint)
    enc = lambda value: base64.urlsafe_b64encode(value).decode().rstrip('=')
    return {'endpoint': 'https://fcm.googleapis.com/fcm/send/test-only', 'keys': {'p256dh': enc(public), 'auth': enc(b'1234567890123456')}}


def test_push_names_counts_chat_badge_and_bulk_uploads(family, monkeypatch):
    from familie import notifications
    app, client, root = family
    sent = []
    monkeypatch.setattr(notifications, 'webpush', lambda **kwargs: sent.append(json.loads(kwargs['data'])))
    assert client.get('/familie/api/push').status_code == 401
    login(client)
    assert client.get('/familie/api/push').json()['public_key']
    device = 'a' * 32
    headers = {'X-Family-Request': '1'}
    def change(action, **extra):
        return client.post('/familie/api/push', json={'id': device, 'action': action, **extra}, headers=headers)
    deliver = lambda later=0: app.state.push.deliver(time.time() + later)
    gert = TestClient(app, base_url='http://localhost'); login(gert, 'Gert')
    assert change('subscribe', subscription=push_subscription()).status_code == 200
    upload(gert)  # suppressed while Robert's page is active (90 s)
    deliver(60)
    assert not sent
    assert change('away').status_code == 200
    upload(client)  # Robert's own upload never notifies Robert
    upload(gert); upload(gert)
    deliver()  # bulk upload still settling: nothing yet
    assert not sent
    deliver(120)
    assert sent == [{'title': 'Gert delte 3 nye bilder', 'body': 'Trykk for å se det i familierommet.', 'badge': 3, 'url': '/familie/'}]
    deliver(240)
    assert len(sent) == 1  # nothing new since the notification
    with sqlite3.connect(root / 'posts.sqlite3') as conn:  # the fake clock above ran ahead; notified "10 s ago"
        conn.execute('UPDATE push_devices SET notified_at=?', (time.time() - 10,))
    terje = TestClient(app, base_url='http://localhost'); login(terje, 'Terje')
    upload(terje)
    assert terje.post('/familie/api/chat', json={'text': 'Hei mamma!', 'client_id': 'c' * 32}, headers=headers).status_code == 200
    deliver(600)
    assert len(sent) == 1  # at most one further notification per 30 minutes
    deliver(2000)
    assert sent[1]['title'] == 'Gert og Terje delte 4 nye bilder'
    assert sent[1]['body'] == 'Terje: en ny melding i familiechatten' and sent[1]['badge'] == 5
    assert change('visit').status_code == 200  # opening the room resets everything
    change('away')
    deliver(4000)
    assert len(sent) == 2
    gert.post('/familie/api/chat', json={'text': 'Hei!', 'client_id': 'd' * 32}, headers=headers)
    deliver(4100)
    assert sent[2] == {'title': 'Gert: en ny melding i familiechatten', 'body': 'Trykk for å se det i familierommet.', 'badge': 1, 'url': '/familie/?chat'}
    change('unsubscribe')
    upload(gert)
    deliver(9000)
    assert len(sent) == 3
    gert.close(); terje.close()


def test_push_rejects_arbitrary_endpoints_and_removes_expired(family, monkeypatch):
    from familie import notifications
    app, client, root = family
    login(client)
    subscription = push_subscription()
    headers = {'X-Family-Request': '1'}
    for endpoint in ['http://127.0.0.1/x', 'https://example.com/x', 'https://fcm.googleapis.com.evil.test/x', 'https://user:pass@fcm.googleapis.com/x']:
        assert client.post('/familie/api/push', json={'id': 'b' * 32, 'action': 'subscribe', 'subscription': {**subscription, 'endpoint': endpoint}}, headers=headers).status_code == 400
    assert client.post('/familie/api/push', json={'id': 'b' * 32, 'action': 'subscribe', 'subscription': subscription}, headers=headers).status_code == 200
    client.post('/familie/api/push', json={'id': 'b' * 32, 'action': 'away'}, headers=headers)
    def gone(**kwargs):
        raise notifications.WebPushException('gone', response=httpx.Response(410))
    monkeypatch.setattr(notifications, 'webpush', gone)
    gert = TestClient(app, base_url='http://localhost'); login(gert, 'Gert')
    upload(gert)
    app.state.push.deliver(time.time() + 120)
    gert.close()
    with sqlite3.connect(root / 'posts.sqlite3') as conn:
        assert conn.execute('SELECT COUNT(*) FROM push_devices').fetchone()[0] == 0


def test_manifest_service_worker_and_icon(family):
    app, client, root = family
    manifest = client.get('/familie/manifest.json').json()
    assert manifest['scope'] == '/familie/' and manifest['display'] == 'standalone'
    assert client.get('/familie/sw.js').headers['content-type'].startswith('application/javascript')
    icon = client.get('/familie/icon.png?size=512')
    with Image.open(io.BytesIO(icon.content)) as image:
        assert image.size == (512, 512)
def test_threads_separate_messages_and_validate_requests(family):
    app, client, root = family
    headers = {'X-Family-Request': '1'}
    assert client.get('/familie/api/threads').status_code == 401
    login(client)
    payload = {'text': 'Sommerferien', 'client_id': secrets.token_hex(16)}
    assert client.post('/familie/api/threads', json=payload).status_code == 403
    topic = client.post('/familie/api/threads', json=payload, headers=headers).json()
    assert client.post('/familie/api/threads', json=payload, headers=headers).json() == topic
    assert client.get('/familie/api/threads').json()['threads'] == [topic]
    url = '/familie/api/chat?thread=' + topic['id']
    message = {'text': 'Hvor reiser vi?', 'client_id': secrets.token_hex(16)}
    assert client.post(url, json=message, headers=headers).status_code == 200
    assert client.get(url).json()['messages'][0]['text'] == message['text']
    assert client.get('/familie/api/chat').json()['messages'] == []
    assert client.post('/familie/api/chat', json=message, headers=headers).status_code == 409
    assert client.get('/familie/api/chat?thread=missing').status_code == 404
    assert client.post('/familie/api/threads', json={'text': 'x' * 101, 'client_id': secrets.token_hex(16)}, headers=headers).status_code == 400


def test_upload_defaults_to_indefinite_and_survives_cleanup(family):
    app, client, root = family
    login(client)
    result = client.post('/familie/api/items', content=photo(), headers={'X-Family-Request': '1', 'Content-Type': 'image/png'})
    assert result.status_code == 200
    ident = result.json()['id']
    app.state.cleanup()
    assert client.get('/familie/api/items').json()['items'][0]['expires'] == 0
    assert client.get('/familie/media/' + ident).status_code == 200


@pytest.mark.parametrize('filename,content_type,data', [
    ('ferie.html', 'text/html', b'<html><script>alert(1)</script></html>'),
    ('plan.pdf', 'application/pdf', b'%PDF-example'),
    ('arkiv.zip', 'application/zip', b'PK-example'),
])
def test_shared_files_are_authenticated_downloads(family, filename, content_type, data):
    app, client, root = family
    login(client)
    response = client.post('/familie/api/items', params={'filename': filename}, content=data,
                           headers={'X-Family-Request': '1', 'Content-Type': content_type})
    assert response.status_code == 200
    ident = response.json()['id']
    item = client.get('/familie/api/items').json()['items'][0]
    assert item['filename'] == filename and item['expires'] == 0
    download = client.get('/familie/media/' + ident)
    assert download.content == data
    assert download.headers['content-type'] == 'application/octet-stream'
    assert download.headers['content-disposition'].startswith('attachment;')
    assert download.headers['x-content-type-options'] == 'nosniff'
    client.post('/familie/api/logout', headers={'X-Family-Request': '1'})
    assert client.get('/familie/media/' + ident).status_code == 401


def test_any_family_member_can_delete_thread_and_its_messages(family):
    app, client, root = family
    headers = {'X-Family-Request': '1'}
    ident = secrets.token_hex(16)
    assert client.delete('/familie/api/threads/' + ident, headers=headers).status_code == 401
    login(client, 'Robert')
    topic = client.post('/familie/api/threads', json={'text': 'Tur', 'client_id': ident}, headers=headers)
    assert topic.status_code == 200
    message = {'text': 'Hei', 'client_id': secrets.token_hex(16)}
    client.post('/familie/api/chat?thread=' + ident, json=message, headers=headers)
    client.post('/familie/api/chat', json={'text': 'Felles', 'client_id': secrets.token_hex(16)}, headers=headers)
    login(client, 'En annen')
    assert client.delete('/familie/api/threads/' + ident).status_code == 403
    assert client.delete('/familie/api/threads/' + ident, headers=headers).status_code == 200
    assert client.get('/familie/api/threads').json()['threads'] == []
    assert client.get('/familie/api/chat?thread=' + ident).status_code == 404
    assert len(client.get('/familie/api/chat').json()['messages']) == 1
    with sqlite3.connect(root / 'posts.sqlite3') as conn:
        assert conn.execute('SELECT COUNT(*) FROM messages WHERE thread_id=?', (ident,)).fetchone()[0] == 0
    assert client.delete('/familie/api/threads/' + ident, headers=headers).status_code == 200


@pytest.mark.parametrize('progressive', [False, True])
def test_gopro_sized_jpeg_upload(family, progressive):
    app, client, root = family
    login(client)
    data = io.BytesIO()
    image = Image.new('RGB', (5568, 4872), '#216052')
    exif = Image.Exif()
    exif[271] = 'GoPro'
    exif[272] = 'HERO12 Black'
    image.save(data, format='JPEG', progressive=progressive, exif=exif)
    response = client.post('/familie/api/items', content=data.getvalue(), headers={'X-Family-Request': '1', 'Content-Type': 'image/jpeg'})
    assert response.status_code == 200
    assert client.get('/familie/media/' + response.json()['id']).content == data.getvalue()


def test_gopro_jpg_with_mpf_metadata_detected_as_mpo(family):
    app, client, root = family
    login(client)
    data = io.BytesIO()
    Image.new('RGB', (5568, 4872), '#216052').save(
        data, format='MPO', save_all=True,
        append_images=[Image.new('RGB', (320, 240), '#216052')])
    raw = data.getvalue()
    with Image.open(io.BytesIO(raw)) as image:
        assert image.format == 'MPO'
    response = client.post('/familie/api/items', params={'filename': 'G0012096.JPG'},
                           content=raw, headers={'X-Family-Request': '1', 'Content-Type': 'image/jpeg'})
    assert response.status_code == 200
    item = client.get('/familie/api/items').json()['items'][0]
    assert item['mime'] == 'image/jpeg'
    media = client.get('/familie/media/' + response.json()['id'])
    assert media.headers['content-type'] == 'image/jpeg'
    assert media.content == raw


def test_private_media_cache_revalidates_auth_deletion_and_expiry(family):
    app, client, root = family
    login(client)
    ident = upload(client).json()['id']
    path = '/familie/media/' + ident
    original = client.get(path)
    etag = original.headers['etag']
    modified = original.headers['last-modified']
    assert original.headers['vary'] == 'Cookie'
    for headers in ({'If-None-Match': etag}, {'If-None-Match': 'W/' + etag}, {'If-Modified-Since': modified}):
        response = client.get(path, headers=headers)
        assert response.status_code == 304 and response.content == b''
        assert response.headers['cache-control'] == 'private, no-cache, must-revalidate'
    assert client.head(path, headers={'If-None-Match': etag}).status_code == 304
    assert client.get(path, headers={'If-None-Match': '"different"', 'If-Modified-Since': modified}).status_code == 200
    partial = client.get(path, headers={'Range': 'bytes=0-9', 'If-Range': etag})
    assert partial.status_code == 206 and partial.content == photo()[:10]
    client.post('/familie/api/logout', headers={'X-Family-Request': '1'})
    blocked = client.get(path, headers={'If-None-Match': etag})
    assert blocked.status_code == 401 and blocked.headers['cache-control'] == 'private, no-store'
    login(client)
    with sqlite3.connect(root / 'posts.sqlite3') as conn:
        conn.execute('UPDATE posts SET expires=? WHERE id=?', (time.time() - 1, ident))
    assert client.get(path, headers={'If-None-Match': etag}).status_code == 404
    with sqlite3.connect(root / 'posts.sqlite3') as conn:
        conn.execute('UPDATE posts SET expires=0 WHERE id=?', (ident,))
    assert client.get(path, headers={'If-None-Match': etag}).status_code == 304
    client.delete('/familie/api/items/' + ident, headers={'X-Family-Request': '1'})
    assert client.get(path, headers={'If-Modified-Since': modified}).status_code == 404


def test_proxy_preserves_conditional_media_requests_and_304(monkeypatch):
    import familie_proxy
    monkeypatch.setenv('FAMILY_ORIGIN', 'https://pi.example.ts.net:8443')
    async def upstream(request):
        assert request.headers['if-none-match'] == '"family-test"'
        assert request.headers['if-modified-since'] == 'Sat, 14 Nov 2026 12:00:00 GMT'
        assert request.headers['cookie'] == 'motes_family_session=family-token'
        assert 'authorization' not in request.headers
        return httpx.Response(304, headers={'ETag': '"family-test"', 'Last-Modified': 'Sat, 14 Nov 2026 12:00:00 GMT', 'Vary': 'Cookie', 'Cache-Control': 'private, no-cache, must-revalidate'})
    real_client = httpx.AsyncClient
    monkeypatch.setattr(familie_proxy.httpx, 'AsyncClient', lambda **kwargs: real_client(transport=httpx.MockTransport(upstream), **kwargs))
    app = FastAPI()
    app.include_router(familie_proxy.router)
    with TestClient(app) as client:
        response = client.get('/familie/media/abc', headers={'If-None-Match': '"family-test"', 'If-Modified-Since': 'Sat, 14 Nov 2026 12:00:00 GMT', 'Cookie': 'session=secret; motes_family_session=family-token'})
        assert response.status_code == 304 and response.content == b''
        assert response.headers['etag'] == '"family-test"' and response.headers['vary'] == 'Cookie'
        assert response.headers['cache-control'] == 'private, no-cache, must-revalidate'


def test_video_start_notifies_others_and_reuses_recent_call(family, monkeypatch):
    from familie import notifications
    app, client, root = family
    sent = []
    monkeypatch.setattr(notifications, 'webpush', lambda **kwargs: sent.append(json.loads(kwargs['data'])))
    headers = {'X-Family-Request': '1'}
    assert client.get('/familie/api/video').status_code == 401
    assert client.post('/familie/api/video/start', headers=headers).status_code == 401
    login(client)
    gert = TestClient(app, base_url='http://localhost'); login(gert, 'Gert')
    for c, device in ((client, 'a' * 32), (gert, 'b' * 32)):
        assert c.post('/familie/api/push', json={'id': device, 'action': 'subscribe', 'subscription': push_subscription()}, headers=headers).status_code == 200
    assert client.get('/familie/api/video').json() == {'call': None}
    assert client.post('/familie/api/video/start').status_code == 403
    started = client.post('/familie/api/video/start', headers=headers).json()
    assert started['started'] is True and started['call']['name'] == 'Robert'
    assert started['call']['room'].startswith('familie-') and len(started['call']['room']) == 24
    assert sent == [{'title': 'Robert har startet en videosamtale', 'body': 'Trykk for å bli med.', 'url': '/familie/?video', 'tag': 'family-video'}]
    again = gert.post('/familie/api/video/start', headers=headers).json()
    assert again == {'call': started['call'], 'started': False} and len(sent) == 1
    assert gert.get('/familie/api/video').json()['call']['room'] == started['call']['room']
    gert.close()


def test_push_really_signs_with_vapid_and_test_button(family, monkeypatch):
    # Only the network send is faked: the real VAPID signing runs (a bad 'sub' broke it).
    import pywebpush
    sent = []
    class Accepted:
        status_code = 201
        text = ''
    def fake_send(self, *args, **kwargs):
        sent.append(args[1] if len(args) > 1 else kwargs.get('headers'))
        return Accepted()
    monkeypatch.setattr(pywebpush.WebPusher, 'send', fake_send)
    app, client, root = family
    login(client)
    headers = {'X-Family-Request': '1'}
    device = 'e' * 32
    body = lambda action, **extra: {'id': device, 'action': action, **extra}
    assert client.post('/familie/api/push', json=body('test'), headers=headers).status_code == 502  # not subscribed
    assert client.post('/familie/api/push', json=body('subscribe', subscription=push_subscription()), headers=headers).status_code == 200
    assert client.post('/familie/api/push', json=body('test'), headers=headers).json() == {'ok': True}
    assert len(sent) == 1 and 'vapid' in sent[0]['Authorization'].lower()
    def broken(self, *args, **kwargs):
        raise RuntimeError('push service down')
    monkeypatch.setattr(pywebpush.WebPusher, 'send', broken)
    assert client.post('/familie/api/push', json=body('test'), headers=headers).status_code == 502
    assert client.post('/familie/api/video/start', headers=headers).json()['started'] is True  # starts anyway
