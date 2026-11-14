"""Standalone Pi service. Run one worker; never serve the data directory statically."""
from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import logging
import os
import re
import secrets
import shutil
import sqlite3
import time
from contextlib import asynccontextmanager, contextmanager, suppress
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from PIL import Image, UnidentifiedImageError

COOKIE = "motes_family_session"
MAX_FILE = 100 * 1024 * 1024
MAX_TOTAL = 10 * 1024**3
SESSION_SECONDS = 7 * 86400
Image.MAX_IMAGE_PIXELS = 25_000_000
ALLOWED_IMAGES = {"JPEG": ("image/jpeg", ".jpg"), "PNG": ("image/png", ".png"), "WEBP": ("image/webp", ".webp")}


def code_hash(code: str, salt: str) -> str:
    return hashlib.pbkdf2_hmac("sha256", code.encode(), bytes.fromhex(salt), 600_000).hex()


def create_app(data_dir: Path | str | None = None, *, secure_cookie: bool = True) -> FastAPI:
    root = Path(data_dir or os.environ.get("FAMILY_DATA_DIR", "/var/lib/motes-familie"))
    # Config is created by the installer, never by an unauthenticated HTTP request.
    config = json.loads((root / "access.json").read_text())
    signing_key = bytes.fromhex(config["session_key"])
    media = root / "media"
    media.mkdir(mode=0o700, exist_ok=True)
    database = root / "posts.sqlite3"
    failed_logins: list[float] = []
    upload_lock = asyncio.Lock()

    @contextmanager
    def db():
        conn = sqlite3.connect(database)
        conn.row_factory = sqlite3.Row
        try:
            with conn:
                yield conn
        finally:
            conn.close()

    with db() as conn:
        conn.execute("""CREATE TABLE IF NOT EXISTS posts (
          id TEXT PRIMARY KEY, mime TEXT NOT NULL, suffix TEXT NOT NULL,
          name TEXT NOT NULL, caption TEXT NOT NULL, created REAL NOT NULL,
          expires REAL NOT NULL, bytes INTEGER NOT NULL)""")
        conn.execute("CREATE INDEX IF NOT EXISTS posts_expiry ON posts(expires)")

    def cleanup():
        now = time.time()
        with db() as conn:
            expired = conn.execute("SELECT id,suffix FROM posts WHERE expires<=?", (now,)).fetchall()
            for row in expired:
                (media / (row["id"] + row["suffix"])).unlink(missing_ok=True)
                conn.execute("DELETE FROM posts WHERE id=?", (row["id"],))
            active = {row["id"] + row["suffix"] for row in conn.execute("SELECT id,suffix FROM posts")}
        # Clean interrupted uploads and files left by a crash between rename and DB commit.
        for path in media.iterdir():
            if path.name not in active and path.stat().st_mtime < now - 300:
                path.unlink(missing_ok=True)

    async def sweeper():
        while True:
            await asyncio.sleep(60)
            async with upload_lock:
                try:
                    await asyncio.to_thread(cleanup)
                except Exception:
                    logging.getLogger(__name__).exception("Kunne ikke rydde familielager; prøver igjen om ett minutt")

    @asynccontextmanager
    async def lifespan(app):
        await asyncio.to_thread(cleanup)
        task = asyncio.create_task(sweeper())
        try:
            yield
        finally:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

    app = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)
    app.state.cleanup = cleanup

    def sign_session(name):
        raw = json.dumps({"name": name, "exp": int(time.time()) + SESSION_SECONDS}, separators=(",", ":")).encode()
        payload = base64.urlsafe_b64encode(raw).decode().rstrip("=")
        return payload + "." + hmac.new(signing_key, payload.encode(), hashlib.sha256).hexdigest()

    def user(request):
        try:
            token = request.cookies.get(COOKIE, "")
            if len(token) > 1024:
                raise ValueError()
            payload, signature = token.split(".")
            expected = hmac.new(signing_key, payload.encode(), hashlib.sha256).hexdigest()
            if not hmac.compare_digest(signature, expected):
                raise ValueError()
            value = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
            if value["exp"] <= time.time() or not isinstance(value["name"], str):
                raise ValueError()
            return value["name"]
        except (ValueError, KeyError, TypeError):
            raise HTTPException(401, "Logg inn med familiekoden.")

    def mutation(request):
        # Browser cross-origin requests cannot set this header without a CORS preflight.
        if request.headers.get("x-family-request") != "1":
            raise HTTPException(403, "Åpne familierommet og prøv igjen.")

    @app.middleware("http")
    async def privacy_headers(request, call_next):
        response = await call_next(request)
        response.headers.update({
            "Cache-Control": "private, no-store",
            "X-Content-Type-Options": "nosniff",
            "Referrer-Policy": "no-referrer",
            "X-Robots-Tag": "noindex, nofollow, noarchive",
            "Content-Security-Policy": "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; img-src 'self' blob:; media-src 'self' blob:; frame-ancestors 'none'; base-uri 'none'; form-action 'self'",
        })
        return response

    @app.get("/familie", response_class=HTMLResponse)
    @app.get("/familie/", response_class=HTMLResponse)
    async def page():
        return HTMLResponse(Path(__file__).with_name("index.html").read_text())

    @app.get("/familie/health")
    async def health():
        return {"ok": True}

    @app.post("/familie/api/login")
    async def login(request: Request):
        mutation(request)
        now = time.time()
        failed_logins[:] = [t for t in failed_logins if t > now - 900]
        if len(failed_logins) >= 10:
            raise HTTPException(429, "For mange feil forsøk. Vent 15 minutter og prøv igjen.")
        body = bytearray()
        async for chunk in request.stream():
            body.extend(chunk)
            if len(body) > 2048:
                raise HTTPException(413, "Forespørselen er for stor.")
        try:
            data = json.loads(body)
            code = str(data.get("code", ""))
            name = str(data.get("name", "")).strip()[:40]
        except (ValueError, AttributeError):
            raise HTTPException(400, "Skriv navn og familiekode.")
        failed_logins.append(now)  # Count before awaiting: concurrent guesses are bounded too.
        valid = await asyncio.to_thread(code_hash, code, config["salt"])
        if not hmac.compare_digest(valid, config["code_hash"]):
            raise HTTPException(401, "Familiekoden stemmer ikke.")
        if not name:
            raise HTTPException(400, "Skriv inn navnet ditt.")
        failed_logins.clear()
        response = JSONResponse({"name": name})
        response.set_cookie(COOKIE, sign_session(name), max_age=SESSION_SECONDS, secure=secure_cookie,
                            httponly=True, samesite="strict", path="/familie")
        return response

    @app.post("/familie/api/logout")
    async def logout(request: Request):
        mutation(request)
        response = JSONResponse({"ok": True})
        response.delete_cookie(COOKIE, path="/familie")
        return response

    @app.get("/familie/api/items")
    async def items(request: Request):
        name = user(request)
        with db() as conn:
            rows = conn.execute("SELECT * FROM posts WHERE expires>? ORDER BY created DESC LIMIT 200", (time.time(),)).fetchall()
        return {"name": name, "items": [dict(row) for row in rows]}

    @app.post("/familie/api/items")
    async def upload(request: Request):
        name = user(request)
        mutation(request)
        try:
            days = int(request.query_params.get("days", "1"))
        except ValueError:
            days = 0
        if days not in {1, 3, 7}:
            raise HTTPException(400, "Velg 1, 3 eller 7 døgn.")
        caption = request.query_params.get("caption", "").strip()[:500]
        mime = request.headers.get("content-type", "").split(";", 1)[0].lower()
        if mime not in {"image/jpeg", "image/png", "image/webp", "video/mp4", "video/webm", "video/quicktime"}:
            raise HTTPException(415, "Velg JPG, PNG, WebP, MP4, WebM eller MOV. HEIC må først lagres som JPG.")
        # An upload is capped at 100 MiB; no client-controlled filesystem paths are used.
        ident = secrets.token_hex(16)
        temp = media / (ident + ".part")
        async with upload_lock:
            await asyncio.to_thread(cleanup)
            with db() as conn:
                used = conn.execute("SELECT COALESCE(SUM(bytes),0) FROM posts").fetchone()[0]
            if used + MAX_FILE > MAX_TOTAL or shutil.disk_usage(root).free < MAX_FILE + 2 * 1024**3:
                raise HTTPException(507, "Familielageret er fullt. Slett noen innlegg og prøv igjen.")
            size = 0
            final = None
            try:
                with temp.open("xb") as out:
                    async for chunk in request.stream():
                        size += len(chunk)
                        if size > MAX_FILE:
                            raise HTTPException(413, "Filen er for stor. Maks 100 MB per bilde eller klipp.")
                        out.write(chunk)
                if not size:
                    raise HTTPException(400, "Filen er tom.")
                header = temp.read_bytes()[:32] if size < 32 else None
                if header is None:
                    with temp.open("rb") as f:
                        header = f.read(32)
                if mime.startswith("image/"):
                    try:
                        with Image.open(temp) as image:
                            actual = ALLOWED_IMAGES.get(image.format)
                            image.verify()
                        if not actual or actual[0] != mime:
                            raise ValueError()
                    except (UnidentifiedImageError, OSError, ValueError, Image.DecompressionBombError):
                        raise HTTPException(415, "Bildet kunne ikke leses. Prøv et annet bilde.")
                    suffix = actual[1]
                else:
                    # Validate the media container; decoding is left to the browser.
                    valid_mp4 = len(header) >= 12 and header[4:8] == b"ftyp"
                    valid_webm = header.startswith(b"\x1aE\xdf\xa3")
                    if (mime == "video/webm" and not valid_webm) or (mime != "video/webm" and not valid_mp4):
                        raise HTTPException(415, "Klippet er ikke i et støttet videoformat.")
                    suffix = {"video/mp4": ".mp4", "video/webm": ".webm", "video/quicktime": ".mov"}[mime]
                final = media / (ident + suffix)
                temp.replace(final)
                now = time.time()
                with db() as conn:
                    conn.execute("INSERT INTO posts VALUES (?,?,?,?,?,?,?,?)", (ident, mime, suffix, name, caption, now, now + days * 86400, size))
                return {"id": ident}
            except BaseException:
                if final:
                    final.unlink(missing_ok=True)
                raise
            finally:
                temp.unlink(missing_ok=True)

    @app.api_route("/familie/media/{ident}", methods=["GET", "HEAD"])
    async def get_media(ident: str, request: Request):
        user(request)
        if not re.fullmatch(r"[0-9a-f]{32}", ident):
            raise HTTPException(404)
        with db() as conn:
            row = conn.execute("SELECT * FROM posts WHERE id=? AND expires>?", (ident, time.time())).fetchone()
        if not row:
            raise HTTPException(404, "Innlegget er slettet eller har gått ut.")
        path = media / (ident + row["suffix"])
        if not path.is_file():
            raise HTTPException(404)
        return FileResponse(path, media_type=row["mime"])

    @app.delete("/familie/api/items/{ident}")
    async def delete_item(ident: str, request: Request):
        user(request)
        mutation(request)
        async with upload_lock:
            with db() as conn:
                row = conn.execute("SELECT * FROM posts WHERE id=?", (ident,)).fetchone()
                if row:
                    (media / (row["id"] + row["suffix"])).unlink(missing_ok=True)
                    conn.execute("DELETE FROM posts WHERE id=?", (ident,))
        return {"ok": True}

    return app
