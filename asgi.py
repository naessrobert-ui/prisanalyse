"""ASGI entrypoint that serves Flask + mounted FastAPI APIs."""

import asyncio
import contextlib
import logging

import httpx
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import RedirectResponse, Response, StreamingResponse
from starlette.requests import Request
from a2wsgi import WSGIMiddleware

from app import app as flask_app
from shipping_routes import start_shipping_streamlit

logger = logging.getLogger(__name__)

logger = logging.getLogger(__name__)


def _build_api_fallback(*, title: str, service: str, reason: str, hint: str, route_prefix: str = "") -> FastAPI:
    fallback = FastAPI(title=title)

    @fallback.get(f"{route_prefix}/")
    def root():
        return {
            "ok": False,
            "error": f"{service} unavailable",
            "reason": reason,
            "hint": hint,
        }

    @fallback.get(f"{route_prefix}/health")
    def health():
        return {
            "ok": False,
            "service": service,
            "error": f"{service} disabled",
            "reason": reason,
        }

    return fallback


try:
    from Fastapi_Backend import app as regnskap_api
except Exception as exc:
    regnskap_api = _build_api_fallback(
        title="Regnskap API unavailable",
        service="prisanalyse-api",
        reason=repr(exc),
        hint="Sjekk at AWS IAM-konfig er riktig (RDS_HOST, RDS_USER, AWS_REGION).",
    )

try:
    from analysis_platform import app as analysis_api
    analysis_router = analysis_api.router
except Exception:
    try:
        from analysis_api_compat import router as analysis_router
    except Exception as exc:
        analysis_api = _build_api_fallback(
            title="Analysis API unavailable",
            service="analysis-api",
            reason=repr(exc),
            hint="Sjekk at DATABASE_URL eller AWS/RDS-konfig er satt for analyse-API-et.",
            route_prefix="/analysis-api",
        )
        analysis_router = analysis_api.router

app = FastAPI(title="Prisanalyse Combined ASGI", version="1.0.5")
app.include_router(analysis_router)
app.mount("/regnskap-api", regnskap_api)

# ---------------------------------------------------------------------------
# Shipping Streamlit reverse proxy
# Forwards /shipping/app/* → http://localhost:8502/shipping/app/*
# and WebSocket connections at /shipping/app/_stcore/stream
# ---------------------------------------------------------------------------

_STREAMLIT_HTTP = "http://localhost:8502"
_STREAMLIT_WS = "ws://localhost:8502"

_SKIP_HEADERS = {"transfer-encoding", "content-encoding", "content-length", "connection"}


@contextlib.asynccontextmanager
async def _connect_streamlit_websocket(target: str):
    """Connect to Streamlit across supported websockets versions."""
    import websockets as _ws

    try:
        async with _ws.connect(target, additional_headers={"origin": _STREAMLIT_HTTP}) as upstream:
            yield upstream
    except TypeError:
        # websockets < 14 used ``extra_headers`` instead of ``additional_headers``.
        async with _ws.connect(target, extra_headers={"origin": _STREAMLIT_HTTP}) as upstream:
            yield upstream


@app.websocket("/shipping/app/{path:path}")
async def _shipping_ws_proxy(websocket: WebSocket, path: str):
    start_shipping_streamlit()
    query = websocket.scope.get("query_string", b"")
    qs = f"?{query.decode()}" if query else ""
    target = f"{_STREAMLIT_WS}/shipping/app/{path}{qs}"

    try:
        async with _connect_streamlit_websocket(target) as upstream:
            await websocket.accept()

            async def _client_to_streamlit():
                while True:
                    message = await websocket.receive()
                    if message.get("type") == "websocket.disconnect":
                        break
                    if message.get("bytes") is not None:
                        await upstream.send(message["bytes"])
                    elif message.get("text") is not None:
                        await upstream.send(message["text"])

            async def _streamlit_to_client():
                async for message in upstream:
                    if isinstance(message, bytes):
                        await websocket.send_bytes(message)
                    else:
                        await websocket.send_text(message)

            tasks = [
                asyncio.create_task(_client_to_streamlit()),
                asyncio.create_task(_streamlit_to_client()),
            ]
            try:
                done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    with contextlib.suppress(WebSocketDisconnect):
                        task.result()
                for task in pending:
                    task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await task
            finally:
                for task in tasks:
                    if not task.done():
                        task.cancel()
    except WebSocketDisconnect:
        pass
    except Exception:
        logger.exception("Shipping websocket proxy failed: %s", target)
    finally:
        with contextlib.suppress(Exception):
            await websocket.close()


@app.api_route(
    "/shipping/app",
    methods=["GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS", "PATCH"],
)
@app.api_route(
    "/shipping/app/{path:path}",
    methods=["GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS", "PATCH"],
)
async def _shipping_http_proxy(request: Request, path: str = ""):
    query = request.scope.get("query_string", b"")
    qs = f"?{query.decode()}" if query else ""

    if path == "" and not request.url.path.endswith("/"):
        return RedirectResponse(url=f"/shipping/app/{qs}", status_code=307)

    start_shipping_streamlit()
    target = f"{_STREAMLIT_HTTP}/shipping/app/{path}{qs}"

    fwd_headers = {
        k: v for k, v in request.headers.items()
        if k.lower() not in {"host", "connection", "upgrade"}
    }

    async with httpx.AsyncClient(timeout=60) as client:
        try:
            resp = await client.request(
                method=request.method,
                url=target,
                headers=fwd_headers,
                content=await request.body(),
                follow_redirects=False,
            )
        except httpx.ConnectError:
            return Response(
                content=b"Shipping-appen er ikke klar enn\xc3\xa5. Vent litt og last inn siden p\xc3\xa5 nytt.",
                status_code=503,
                media_type="text/plain; charset=utf-8",
            )

    resp_headers = {
        k: v for k, v in resp.headers.items()
        if k.lower() not in _SKIP_HEADERS
    }

    if resp.status_code in (301, 302, 307, 308):
        loc = resp.headers.get("location", "")
        if loc.startswith("/") and not loc.startswith("/shipping/app"):
            resp_headers["location"] = "/shipping/app" + loc
        elif loc.startswith(f"http://localhost:{8502}"):
            resp_headers["location"] = loc.replace(f"http://localhost:{8502}", "")

    return Response(
        content=resp.content,
        status_code=resp.status_code,
        headers=resp_headers,
        media_type=resp.headers.get("content-type"),
    )


# Flask app keeps existing routes on '/'.
app.mount("/", WSGIMiddleware(flask_app))
