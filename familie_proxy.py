"""Stream /familie to the Pi over HTTPS. No media is persisted on Render."""
import os
import re
from urllib.parse import urlsplit

import httpx
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response, StreamingResponse

router = APIRouter()
# Credentials and hop-by-hop headers from the main site must not reach the Pi.
REQUEST_HEADERS = {"content-type", "content-length", "range", "if-range", "if-none-match", "if-modified-since", "x-family-request", "x-fruen-request"}
RESPONSE_HEADERS = {"content-type", "content-length", "content-encoding", "content-range", "accept-ranges", "set-cookie", "cache-control", "x-content-type-options", "referrer-policy", "x-robots-tag", "content-security-policy", "etag", "last-modified", "vary", "permissions-policy"}


@router.get('/fruen/api/ice', include_in_schema=False)
async def fruen_ice(request: Request):
    # Only a currently invited device gets relay credentials. Family login alone is insufficient.
    origin=(os.environ.get('FAMILY_ORIGIN') or '').rstrip('/')
    try:
        parsed=urlsplit(origin)
        valid=(parsed.scheme=='https' and (parsed.hostname or '').endswith('.ts.net') and parsed.port in {None,443,8443,10000}
               and not (parsed.username or parsed.password or parsed.path or parsed.query or parsed.fragment))
    except ValueError:valid=False
    token=request.cookies.get('motes_fruen_session','')
    if not valid or not re.fullmatch('[0-9a-f]{64}',token):
        return JSONResponse({'detail':'Denne enheten må inviteres først.'},status_code=401)
    try:
        async with httpx.AsyncClient(timeout=8,trust_env=False,follow_redirects=False) as client:
            checked=await client.get(origin+'/fruen/api/me',headers={'cookie':'motes_fruen_session='+token})
    except httpx.HTTPError:
        return JSONResponse({'detail':'Besøkstjenesten er midlertidig utilgjengelig.'},status_code=503)
    if checked.status_code!=200:
        return JSONResponse({'detail':'Tilgangen er ikke aktiv.'},status_code=401)
    import meet_routes
    return await meet_routes.meet_ice(kode=meet_routes.KODE,rom='fruen',inv='')


@router.api_route('/fruen', methods=['GET', 'HEAD', 'POST', 'DELETE'])
@router.api_route('/fruen/{path:path}', methods=['GET', 'HEAD', 'POST', 'DELETE'])
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
    cookie_names=('motes_fruen_session','motes_fruen_admin') if request.url.path.startswith('/fruen') else ('motes_family_session',)
    cookie='; '.join(name+'='+request.cookies[name] for name in cookie_names if request.cookies.get(name) and (name=='motes_family_session' or re.fullmatch('[0-9a-f]{64}',request.cookies[name])))
    if cookie:headers['cookie']=cookie
    client = httpx.AsyncClient(timeout=httpx.Timeout(60, connect=10), follow_redirects=False, trust_env=False)
    # Path and query stay on one fixed, configured origin. Never follow redirects.
    target = origin + request.url.path + ('?' + request.url.query if request.url.query else '')
    upstream_request = client.build_request(request.method, target, headers=headers, content=request.stream())
    try:
        upstream = await client.send(upstream_request, stream=True)
    except httpx.HTTPError:
        await client.aclose()
        return HTMLResponse('Familietjenesten er midlertidig utilgjengelig. Prøv igjen om litt.', status_code=503,
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
    if request.method == 'HEAD' or upstream.status_code in {204, 304}:
        await upstream.aclose()
        await client.aclose()
        return Response(status_code=upstream.status_code, headers=response_headers)
    return StreamingResponse(stream(), status_code=upstream.status_code, headers=response_headers)
