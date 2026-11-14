"""Stream /familie to the Pi over HTTPS. No media is persisted on Render."""
import os
from urllib.parse import urlsplit

import httpx
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, StreamingResponse

router = APIRouter()
# Credentials and hop-by-hop headers from the main site must not reach the Pi.
REQUEST_HEADERS = {"content-type", "content-length", "range", "if-range", "x-family-request"}
RESPONSE_HEADERS = {"content-type", "content-length", "content-encoding", "content-range", "accept-ranges", "set-cookie", "cache-control", "x-content-type-options", "referrer-policy", "x-robots-tag", "content-security-policy"}


@router.api_route('/familie', methods=['GET', 'HEAD', 'POST', 'DELETE'])
@router.api_route('/familie/{path:path}', methods=['GET', 'HEAD', 'POST', 'DELETE'])
async def family_proxy(request: Request, path: str = ''):
    origin = (os.environ.get('FAMILY_ORIGIN') or '').rstrip('/')
    try:
        parsed = urlsplit(origin)
        port = parsed.port
    except ValueError:
        parsed = urlsplit('')
        port = None
    if (parsed.scheme != 'https' or not (parsed.hostname or '').endswith('.ts.net')
            or parsed.username or parsed.password or parsed.query or parsed.fragment
            or parsed.path or port not in {None, 443, 8443, 10000}):
        return HTMLResponse('<!doctype html><html lang="no"><meta charset="utf-8"><meta name="viewport" content="width=device-width"><title>Familierom · møtes.no</title><body style="font:17px/1.6 system-ui;max-width:500px;margin:15vh auto;padding:24px;color:#183236"><h1>Familierommet er ikke klart ennå.</h1><p>Prøv igjen senere.</p><a href="/">Tilbake til møtes.no</a></body></html>', status_code=503, headers={'Cache-Control': 'no-store', 'X-Robots-Tag': 'noindex'})
    headers = {k: v for k, v in request.headers.items() if k.lower() in REQUEST_HEADERS}
    headers['accept-encoding'] = 'identity'
    session = request.cookies.get('motes_family_session')
    if session:
        headers['cookie'] = 'motes_family_session=' + session
    client = httpx.AsyncClient(timeout=httpx.Timeout(60, connect=10), follow_redirects=False, trust_env=False)
    # Path and query stay on one fixed, configured origin. Never follow redirects.
    target = origin + request.url.path + ('?' + request.url.query if request.url.query else '')
    upstream_request = client.build_request(request.method, target, headers=headers, content=request.stream())
    try:
        upstream = await client.send(upstream_request, stream=True)
    except httpx.HTTPError:
        await client.aclose()
        return HTMLResponse('Familierommet er midlertidig utilgjengelig. Prøv igjen om litt.', status_code=503,
                            headers={'Cache-Control': 'no-store', 'X-Robots-Tag': 'noindex'})

    except BaseException:
        await client.aclose()
        raise

    async def stream():
        try:
            async for chunk in upstream.aiter_raw():
                yield chunk
        finally:
            await upstream.aclose()
            await client.aclose()

    response_headers = {k: v for k, v in upstream.headers.items() if k.lower() in RESPONSE_HEADERS}
    return StreamingResponse(stream(), status_code=upstream.status_code, headers=response_headers)
