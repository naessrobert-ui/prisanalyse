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
import struct
import time
from email.utils import formatdate, parsedate_to_datetime
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from contextlib import asynccontextmanager, contextmanager, suppress
from pathlib import Path

from fastapi import BackgroundTasks, FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse, Response
from PIL import Image, ImageOps, UnidentifiedImageError
from pillow_heif import register_heif_opener
from familie.notifications import FamilyPush, valid_subscription

register_heif_opener()

COOKIE = "motes_family_session"
MAX_FILE = 100 * 1024 * 1024
MAX_TOTAL = 10 * 1024**3
SESSION_SECONDS = 7 * 86400
Image.MAX_IMAGE_PIXELS = 60_000_000
# GoPro JPG files may contain MPF metadata and be identified as MPO by Pillow.
# MPO is JPEG-compatible; browsers display the primary image.
ALLOWED_IMAGES = {"JPEG": ("image/jpeg", ".jpg"), "MPO": ("image/jpeg", ".jpg"), "PNG": ("image/png", ".png"), "WEBP": ("image/webp", ".webp"), "HEIF": ("image/jpeg", ".jpg")}


def recording_time(path: Path, mime: str) -> tuple[str | None, str | None]:
    """Read capture metadata, never mistake file modification time for capture time."""
    try:
        if mime.startswith("image/"):
            with Image.open(path) as image:
                exif = image.getexif()
                details = exif.get_ifd(34665) if 34665 in exif else exif
                raw = details.get(36867) or details.get(36868)
                if not raw:
                    return None, None
                value = datetime.strptime(str(raw).strip(), "%Y:%m:%d %H:%M:%S")
                offset = details.get(36881) or details.get(36882)
                if offset and re.fullmatch(r"[+-]\d{2}:\d{2}", str(offset)):
                    sign = 1 if offset[0] == "+" else -1
                    value = value.replace(tzinfo=timezone(sign * timedelta(hours=int(offset[1:3]), minutes=int(offset[4:6]))))
                return value.isoformat(), "exif"
        if mime in {"video/mp4", "video/quicktime"}:
            # Seek over video payloads; bound malformed atom traversal.
            with path.open("rb") as stream:
                size = path.stat().st_size
                def atoms(start, end, depth=0):
                    if depth > 4:
                        return None
                    position = start
                    for _ in range(4096):
                        if position + 8 > end:
                            break
                        stream.seek(position)
                        header = stream.read(8)
                        length, kind = int.from_bytes(header[:4], "big"), header[4:]
                        width = 8
                        if length == 1:
                            length = int.from_bytes(stream.read(8), "big")
                            width = 16
                        elif length == 0:
                            length = end - position
                        if length < width or position + length > end:
                            break
                        if kind == b"moov":
                            found = atoms(position + width, position + length, depth + 1)
                            if found:
                                return found
                        elif kind == b"mvhd":
                            payload = stream.read(min(16, length - width))
                            version = payload[0] if payload else -1
                            count = 8 if version == 1 else 4
                            if version in {0, 1} and len(payload) >= 4 + count:
                                seconds = int.from_bytes(payload[4:4 + count], "big")
                                if seconds:
                                    value = datetime(1904, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=seconds)
                                    if 1970 <= value.year <= datetime.now(timezone.utc).year + 1:
                                        return value.isoformat()
                        position += length
                    return None
                found = atoms(0, size)
                if found:
                    return found, "video_metadata"
    except (OSError, ValueError, TypeError, OverflowError, SyntaxError, KeyError, IndexError, RuntimeError, struct.error, Image.DecompressionBombError):
        pass
    return None, None


def calendar_day(item: dict) -> str:
    value = item.get("recorded_at")
    if value:
        date = datetime.fromisoformat(value)
        # Camera timestamps without an offset preserve their original local day.
        return (date.astimezone(ZoneInfo("Europe/Oslo")) if date.tzinfo else date).date().isoformat()
    return datetime.fromtimestamp(item["created"], ZoneInfo("Europe/Oslo")).date().isoformat()


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
        conn.execute("PRAGMA foreign_keys=ON")
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
        columns = {row[1] for row in conn.execute("PRAGMA table_info(posts)")}
        needs_capture_backfill = "recorded_at" not in columns
        for column in ("recorded_at", "recorded_source"):
            if column not in columns:
                conn.execute(f"ALTER TABLE posts ADD COLUMN {column} TEXT")

        if "filename" not in columns:
            conn.execute("ALTER TABLE posts ADD COLUMN filename TEXT")

        conn.execute("""CREATE TABLE IF NOT EXISTS messages (
          id TEXT PRIMARY KEY, name TEXT NOT NULL, text TEXT NOT NULL,
          created REAL NOT NULL, expires REAL NOT NULL)""")
        conn.execute("CREATE TABLE IF NOT EXISTS threads (id TEXT PRIMARY KEY, title TEXT NOT NULL, created REAL NOT NULL)")
        if "thread_id" not in {row[1] for row in conn.execute("PRAGMA table_info(messages)")}:
            conn.execute("ALTER TABLE messages ADD COLUMN thread_id TEXT NOT NULL DEFAULT ''")
        conn.execute("CREATE INDEX IF NOT EXISTS messages_thread ON messages(thread_id,created)")
        conn.execute("CREATE INDEX IF NOT EXISTS messages_created ON messages(created)")
        conn.execute("CREATE INDEX IF NOT EXISTS messages_expiry ON messages(expires)")
        conn.execute("""CREATE TABLE IF NOT EXISTS reactions (
          post_id TEXT NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
          author_key TEXT NOT NULL, name TEXT NOT NULL,
          PRIMARY KEY(post_id,author_key))""")
        conn.execute("""CREATE TABLE IF NOT EXISTS comments (
          id TEXT PRIMARY KEY, post_id TEXT NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
          name TEXT NOT NULL, text TEXT NOT NULL, created REAL NOT NULL)""")
        conn.execute("CREATE INDEX IF NOT EXISTS comments_post ON comments(post_id,created)")

    if needs_capture_backfill:
        with db() as conn:
            for row in conn.execute("SELECT id,suffix,mime FROM posts WHERE (expires=0 OR expires>?)", (time.time(),)).fetchall():
                captured, source = recording_time(media / (row["id"] + row["suffix"]), row["mime"])
                if captured:
                    conn.execute("UPDATE posts SET recorded_at=?,recorded_source=? WHERE id=?", (captured, source, row["id"]))

    def cleanup():
        now = time.time()
        with db() as conn:
            conn.execute("DELETE FROM messages WHERE expires<=?", (now,))
            conn.execute("DELETE FROM comments WHERE post_id NOT IN (SELECT id FROM posts)")
            conn.execute("DELETE FROM reactions WHERE post_id NOT IN (SELECT id FROM posts)")
            expired = conn.execute("SELECT id,suffix FROM posts WHERE expires!=0 AND expires<=?", (now,)).fetchall()
            for row in expired:
                (media / (row["id"] + row["suffix"])).unlink(missing_ok=True)
                conn.execute("DELETE FROM posts WHERE id=?", (row["id"],))
            active = {row["id"] + row["suffix"] for row in conn.execute("SELECT id,suffix FROM posts")}
        # Clean interrupted uploads and files left by a crash between rename and DB commit.
        for path in media.iterdir():
            if path.name not in active and path.stat().st_mtime < now - 300:
                path.unlink(missing_ok=True)

    push = FamilyPush(root, db)

    async def sweeper():
        while True:
            await asyncio.sleep(60)
            async with upload_lock:
                try:
                    await asyncio.to_thread(cleanup)
                except Exception:
                    logging.getLogger(__name__).exception("Kunne ikke rydde familielager; prøver igjen om ett minutt")
            try:
                await asyncio.to_thread(app.state.fruen_cleanup)
                await asyncio.to_thread(push.deliver)
            except Exception:
                logging.getLogger(__name__).exception("Kunne ikke sende familievarsler; prøver igjen om ett minutt")

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
    app.state.push = push

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
            "Cache-Control": response.headers.get("Cache-Control") or "private, no-store",
            "X-Content-Type-Options": "nosniff",
            "Referrer-Policy": "no-referrer",
            "X-Robots-Tag": "noindex, nofollow, noarchive",
            "Content-Security-Policy": response.headers.get("Content-Security-Policy") or "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; img-src 'self' blob:; media-src 'self' blob:; frame-ancestors 'none'; base-uri 'none'; form-action 'self'",
        })
        if request.url.path.startswith('/fruen'):
            response.headers['Permissions-Policy']='camera=(self), microphone=(self), autoplay=(self), display-capture=()'
        return response

    @app.get("/familie", response_class=HTMLResponse)
    @app.get("/familie/", response_class=HTMLResponse)
    async def page():
        return HTMLResponse(Path(__file__).with_name("index.html").read_text())

    @app.get("/familie/health")
    async def health():
        return {"ok": True, "version": "family-2026-11-share-progress"}

    @app.get("/familie/slekt", response_class=HTMLResponse)
    async def genealogy_page(request: Request):
        try:
            user(request)
        except HTTPException:
            return RedirectResponse("/familie", status_code=303)
        return HTMLResponse(Path(__file__).with_name("slekt.html").read_text())

    @app.get("/familie/api/slekt")
    async def genealogy_status(request: Request):
        user(request)
        path = root / "slekt.html"
        if not path.exists():
            return {"exists": False}
        info = path.stat()
        return {"exists": True, "bytes": info.st_size, "updated": info.st_mtime}

    @app.post("/familie/api/slekt")
    async def genealogy_upload(request: Request):
        user(request)
        mutation(request)
        if request.headers.get("content-type", "").split(";", 1)[0].lower() != "text/html":
            raise HTTPException(415, "Velg HTML-filen fra slektsdatabasen.")
        body = bytearray()
        async for chunk in request.stream():
            body.extend(chunk)
            if len(body) > 10 * 1024 * 1024:
                raise HTTPException(413, "Slektsfilen kan være opptil 10 MB.")
        try:
            text = body.decode("utf-8-sig")
            if not re.search(r"<html(?:\s|>)", text, re.I) or not re.search(r"</html\s*>", text, re.I):
                raise ValueError()
        except (ValueError, UnicodeError):
            raise HTTPException(415, "Filen må være en HTML-side lagret som UTF-8.")
        async with upload_lock:
            temporary = root / "slekt.upload"
            try:
                with temporary.open("wb") as stream:
                    os.chmod(temporary, 0o600)
                    stream.write(body)
                temporary.replace(root / "slekt.html")
            finally:
                temporary.unlink(missing_ok=True)
        return {"ok": True}

    @app.get("/familie/slekt/vis")
    async def genealogy_view(request: Request):
        user(request)
        path = root / "slekt.html"
        if not path.exists():
            raise HTTPException(404, "Ingen slektsdatabase er lagt inn ennå.")
        # A sandboxed opaque origin prevents uploaded scripts reading family cookies,
        # storage or APIs, even when this URL is opened outside the parent iframe.
        policy = ("default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; "
                  "img-src data: blob: https://cache.kartverket.no https://server.arcgisonline.com https://tile.openstreetmap.org; "
                  "connect-src https://ws.geonorge.no; base-uri 'none'; form-action 'none'; "
                  "frame-ancestors 'self'; sandbox allow-scripts allow-downloads")
        return FileResponse(path, media_type="text/html", headers={"Content-Security-Policy": policy})

    @app.get("/familie/sw.js")
    async def service_worker():
        return FileResponse(Path(__file__).with_name("sw.js"), media_type="application/javascript")

    @app.get("/familie/manifest.json")
    async def manifest():
        return JSONResponse({"name": "Familierommet", "short_name": "Familien", "id": "/familie/", "start_url": "/familie/", "scope": "/familie/", "display": "standalone", "background_color": "#fafbf8", "theme_color": "#216052", "icons": [{"src": "/familie/icon.png?size=" + str(size), "sizes": f"{size}x{size}", "type": "image/png"} for size in (192, 512)]})

    @app.get("/familie/icon.png")
    async def app_icon(request: Request):
        from io import BytesIO
        from PIL import ImageDraw
        from fastapi.responses import Response
        size = 512 if request.query_params.get("size") == "512" else 192
        image = Image.new("RGB", (size, size), "#216052")
        draw = ImageDraw.Draw(image)
        draw.ellipse((size//4, size//5, size*3//4, size*7//10), fill="#e5f0e9")
        draw.rounded_rectangle((size//5, size//2, size*4//5, size*4//5), radius=size//8, fill="#e5f0e9")
        out = BytesIO()
        image.save(out, "PNG")
        return Response(out.getvalue(), media_type="image/png")

    @app.get("/familie/api/push")
    async def push_info(request: Request):
        user(request)
        return {"public_key": push.public_key}

    @app.post("/familie/api/push")
    async def push_update(request: Request):
        name = user(request)
        mutation(request)
        body = bytearray()
        async for chunk in request.stream():
            body.extend(chunk)
            if len(body) > 8192:
                raise HTTPException(413)
        try:
            data = json.loads(body)
            ident = data["id"]
            if not isinstance(ident, str) or not re.fullmatch(r"[0-9a-f]{32}", ident):
                raise ValueError()
            action = data["action"]
            if action not in {"subscribe", "visit", "away", "unsubscribe"}:
                raise ValueError()
        except (ValueError, KeyError, TypeError):
            raise HTTPException(400, "Ugyldig varselvalg.")
        now = time.time()
        with db() as conn:
            if action == "subscribe":
                subscription = data.get("subscription")
                if not valid_subscription(subscription):
                    raise HTTPException(400, "Nettleserens varseladresse er ikke gyldig.")
                if not conn.execute("SELECT id FROM push_devices WHERE id=?", (ident,)).fetchone() and conn.execute("SELECT COUNT(*) FROM push_devices").fetchone()[0] >= 100:
                    raise HTTPException(429, "For mange varselenheter. Slå av varsler på gamle enheter først.")
                conn.execute("INSERT OR REPLACE INTO push_devices VALUES (?,?,?,?,?,0)", (ident, json.dumps(subscription), name, now, now + 90))
            elif action == "unsubscribe":
                conn.execute("DELETE FROM push_devices WHERE id=?", (ident,))
            elif action == "visit":
                conn.execute("UPDATE push_devices SET seen=?,active_until=?,notified=0 WHERE id=?", (now, now + 90, ident))
            else:
                conn.execute("UPDATE push_devices SET active_until=0 WHERE id=?", (ident,))
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
            rows = conn.execute("SELECT * FROM posts WHERE (expires=0 OR expires>?) ORDER BY created DESC", (time.time(),)).fetchall()
            selected_day = request.query_params.get("day", "")
            if selected_day and not re.fullmatch(r"\d{4}-\d{2}-\d{2}", selected_day):
                raise HTTPException(400, "Velg en gyldig kalenderdag.")
            result = []
            for row in rows:
                item = dict(row)
                item["calendar_day"] = calendar_day(item)
                if selected_day and item["calendar_day"] != selected_day:
                    continue
                if len(result) >= 200:
                    break
                likes = conn.execute("SELECT author_key,name FROM reactions WHERE post_id=? ORDER BY name", (row["id"],)).fetchall()
                item.update(likes=[like["name"] for like in likes], liked=any(like["author_key"] == name.casefold() for like in likes),
                            comments=conn.execute("SELECT COUNT(*) FROM comments WHERE post_id=?", (row["id"],)).fetchone()[0])
                result.append(item)
        return {"name": name, "items": result}

    @app.get("/familie/api/calendar")
    async def calendar(request: Request):
        user(request)
        counts = {}
        with db() as conn:
            for row in conn.execute("SELECT created,recorded_at FROM posts WHERE (expires=0 OR expires>?)", (time.time(),)):
                day = calendar_day(dict(row))
                counts[day] = counts.get(day, 0) + 1
        return {"days": counts}

    async def message_payload(request):
        body = bytearray()
        async for chunk in request.stream():
            body.extend(chunk)
            if len(body) > 16384:
                raise HTTPException(413, "Meldingen er for stor.")
        try:
            payload = json.loads(body)
            text, ident = payload["text"], payload["client_id"]
            if not isinstance(text, str) or not isinstance(ident, str):
                raise ValueError()
            text = text.strip()
            if not 1 <= len(text) <= 2000 or not re.fullmatch(r"[0-9a-f]{32}", ident):
                raise ValueError()
        except (ValueError, KeyError, TypeError):
            raise HTTPException(400, "Skriv en melding på mellom 1 og 2000 tegn.")
        return text, ident

    def require_post(conn, ident):
        if not conn.execute("SELECT id FROM posts WHERE id=? AND (expires=0 OR expires>?)", (ident, time.time())).fetchone():
            raise HTTPException(404, "Innlegget er slettet eller har gått ut.")

    @app.post("/familie/api/items/{post_id}/like")
    async def like_post(post_id: str, request: Request):
        name = user(request)
        mutation(request)
        body = bytearray()
        async for chunk in request.stream():
            body.extend(chunk)
            if len(body) > 512:
                raise HTTPException(413)
        try:
            liked = json.loads(body)["liked"]
            if type(liked) is not bool:
                raise ValueError()
        except (ValueError, KeyError, TypeError):
            raise HTTPException(400, "Ugyldig liker-markering.")
        with db() as conn:
            require_post(conn, post_id)
            if liked:
                conn.execute("INSERT OR REPLACE INTO reactions VALUES (?,?,?)", (post_id, name.casefold(), name))
            else:
                conn.execute("DELETE FROM reactions WHERE post_id=? AND author_key=?", (post_id, name.casefold()))
            likes = [r["name"] for r in conn.execute("SELECT name FROM reactions WHERE post_id=? ORDER BY name", (post_id,))]
        return {"liked": liked, "likes": likes}

    @app.get("/familie/api/items/{post_id}/comments")
    async def comments_list(post_id: str, request: Request):
        user(request)
        with db() as conn:
            require_post(conn, post_id)
            rows = conn.execute("SELECT * FROM comments WHERE post_id=? ORDER BY created,id", (post_id,)).fetchall()
        return {"comments": [dict(row) for row in rows]}

    @app.post("/familie/api/items/{post_id}/comments")
    async def comment_send(post_id: str, request: Request):
        name = user(request)
        mutation(request)
        text, ident = await message_payload(request)
        now = time.time()
        with db() as conn:
            require_post(conn, post_id)
            previous = conn.execute("SELECT * FROM comments WHERE id=?", (ident,)).fetchone()
            if previous:
                if previous["name"] != name or previous["text"] != text or previous["post_id"] != post_id:
                    raise HTTPException(409, "Kommentaren er endret. Prøv igjen.")
                return {"id": ident}
            if conn.execute("SELECT COUNT(*) FROM comments WHERE created>?", (now - 60,)).fetchone()[0] >= 60:
                raise HTTPException(429, "Mange kommentarer akkurat nå. Prøv igjen om litt.")
            if conn.execute("SELECT COUNT(*) FROM comments WHERE post_id=?", (post_id,)).fetchone()[0] >= 200:
                raise HTTPException(429, "Innlegget har nådd grensen på 200 kommentarer.")
            conn.execute("INSERT INTO comments VALUES (?,?,?,?,?)", (ident, post_id, name, text, now))
        return {"id": ident}

    @app.delete("/familie/api/items/{post_id}/comments/{ident}")
    async def comment_delete(post_id: str, ident: str, request: Request):
        user(request)
        mutation(request)
        with db() as conn:
            require_post(conn, post_id)
            conn.execute("DELETE FROM comments WHERE id=? AND post_id=?", (ident, post_id))
        return {"ok": True}

    def require_thread(conn, thread_id):
        if thread_id and not conn.execute("SELECT id FROM threads WHERE id=?", (thread_id,)).fetchone():
            raise HTTPException(404, "Tråden finnes ikke.")

    @app.get("/familie/api/threads")
    async def thread_list(request: Request):
        user(request)
        with db() as conn:
            rows = conn.execute("SELECT * FROM threads ORDER BY created DESC,id DESC").fetchall()
        return {"threads": [dict(row) for row in rows]}

    @app.post("/familie/api/threads")
    async def thread_create(request: Request):
        user(request)
        mutation(request)
        title, ident = await message_payload(request)
        if len(title) > 100:
            raise HTTPException(400, "Overskriften kan ha opptil 100 tegn.")
        with db() as conn:
            previous = conn.execute("SELECT * FROM threads WHERE id=?", (ident,)).fetchone()
            if previous:
                if previous["title"] != title:
                    raise HTTPException(409, "Overskriften er endret.")
                return dict(previous)
            if conn.execute("SELECT COUNT(*) FROM threads").fetchone()[0] >= 1000:
                raise HTTPException(429, "Familierommet har nådd grensen på 1000 tråder.")
            now = time.time()
            conn.execute("INSERT INTO threads VALUES (?,?,?)", (ident, title, now))
        return {"id": ident, "title": title, "created": now}

    @app.delete("/familie/api/threads/{ident}")
    async def thread_delete(ident: str, request: Request):
        user(request)
        mutation(request)
        if not re.fullmatch(r"[0-9a-f]{32}", ident):
            raise HTTPException(404, "Tråden finnes ikke.")
        with db() as conn:
            conn.execute("DELETE FROM messages WHERE thread_id=?", (ident,))
            conn.execute("DELETE FROM threads WHERE id=?", (ident,))
        return {"ok": True}

    @app.get("/familie/api/chat")
    async def chat_list(request: Request):
        name = user(request)
        thread_id = request.query_params.get("thread", "")
        with db() as conn:
            require_thread(conn, thread_id)
            rows = conn.execute(
                "SELECT * FROM messages WHERE expires>? AND thread_id=? ORDER BY created DESC,id DESC LIMIT 200",
                (time.time(), thread_id),
            ).fetchall()
        return {"name": name, "messages": [dict(row) for row in reversed(rows)]}

    @app.post("/familie/api/chat")
    async def chat_send(request: Request):
        name = user(request)
        mutation(request)
        text, ident = await message_payload(request)
        now = time.time()
        thread_id = request.query_params.get("thread", "")
        with db() as conn:
            require_thread(conn, thread_id)
            previous = conn.execute("SELECT * FROM messages WHERE id=?", (ident,)).fetchone()
            if previous:
                if previous["name"] != name or previous["text"] != text or previous["thread_id"] != thread_id:
                    raise HTTPException(409, "Meldingen er endret. Prøv igjen.")
                return {"id": ident}
            if conn.execute("SELECT COUNT(*) FROM messages WHERE created>?", (now - 60,)).fetchone()[0] >= 60:
                raise HTTPException(429, "Mange meldinger akkurat nå. Vent litt og prøv igjen.")
            if conn.execute("SELECT COUNT(*) FROM messages WHERE expires>?", (now,)).fetchone()[0] >= 5000:
                raise HTTPException(429, "Chatten er full. Slett eldre meldinger eller vent til de går ut.")
            conn.execute("INSERT INTO messages (id,name,text,created,expires,thread_id) VALUES (?,?,?,?,?,?)", (ident, name, text, now, now + 7 * 86400, thread_id))
        return {"id": ident}

    @app.delete("/familie/api/chat/{ident}")
    async def chat_delete(ident: str, request: Request):
        user(request)
        mutation(request)
        with db() as conn:
            conn.execute("DELETE FROM messages WHERE id=?", (ident,))
        return {"ok": True}

    @app.post("/familie/api/items")
    async def upload(request: Request, background_tasks: BackgroundTasks):
        name = user(request)
        mutation(request)
        try:
            days = int(request.query_params.get("days", "0"))
        except ValueError:
            days = -1
        if days not in {0, 1, 3, 7}:
            raise HTTPException(400, "Velg 1 døgn, 1 uke eller til innlegget slettes.")
        caption = request.query_params.get("caption", "").strip()[:500]
        mime = request.headers.get("content-type", "").split(";", 1)[0].lower()
        mime = {"image/jpg": "image/jpeg", "image/pjpeg": "image/jpeg"}.get(mime, mime)
        filename = request.query_params.get("filename", "")
        filename = re.sub(r"[\x00-\x1f\x7f/\\]", "_", filename).strip()[:180]
        supported_media = mime in {"image/jpeg", "image/png", "image/webp", "image/heic", "image/heif", "video/mp4", "video/webm", "video/quicktime"}
        if not supported_media:
            if not filename:
                raise HTTPException(415, "Velg en fil med filnavn.")
            mime = "application/octet-stream"
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
                recorded_at, recorded_source = await asyncio.to_thread(recording_time, temp, mime)
                if mime.startswith("image/"):
                    source_format = "unidentified"
                    try:
                        with Image.open(temp) as image:
                            actual = ALLOWED_IMAGES.get(image.format)
                            source_format = image.format
                            if image.width * image.height > Image.MAX_IMAGE_PIXELS:
                                raise HTTPException(413, "Bildet har for høy oppløsning. Maks 60 megapiksler.")
                            image.verify()
                        if not actual:
                            raise HTTPException(415, "Bildeformatet støttes ikke. Eksporter bildet som vanlig JPEG og prøv igjen.")
                        if source_format == "HEIF":
                            converted = temp.with_suffix(".converted")
                            try:
                                with Image.open(temp) as image:
                                    image = ImageOps.exif_transpose(image)
                                    image.thumbnail((4096, 4096))
                                    image.convert("RGB").save(converted, "JPEG", quality=90)
                                converted.replace(temp)
                                size = temp.stat().st_size
                            finally:
                                converted.unlink(missing_ok=True)
                        mime = actual[0]
                    except Image.DecompressionBombError:
                        raise HTTPException(413, "Bildet har for høy oppløsning. Maks 60 megapiksler.")
                    except (UnidentifiedImageError, OSError, ValueError, RuntimeError) as exc:
                        # Log only exception type, never names, image content or EXIF.
                        logging.getLogger(__name__).warning("Familiebilde avvist: %s; format=%s; bytes=%d", type(exc).__name__, source_format, size)
                        raise HTTPException(415, "Bildet kunne ikke leses. Prøv å lagre en ny kopi som JPG og last opp igjen.")
                    suffix = actual[1]
                elif mime.startswith("video/"):
                    # Validate the media container; decoding is left to the browser.
                    valid_mp4 = len(header) >= 12 and header[4:8] == b"ftyp"
                    valid_webm = header.startswith(b"\x1aE\xdf\xa3")
                    if (mime == "video/webm" and not valid_webm) or (mime != "video/webm" and not valid_mp4):
                        raise HTTPException(415, "Klippet er ikke i et støttet videoformat.")
                    suffix = {"video/mp4": ".mp4", "video/webm": ".webm", "video/quicktime": ".mov"}[mime]
                else:
                    suffix = ".file"
                final = media / (ident + suffix)
                temp.replace(final)
                now = time.time()
                with db() as conn:
                    conn.execute("INSERT INTO posts (id,mime,suffix,name,caption,created,expires,bytes,recorded_at,recorded_source,filename) VALUES (?,?,?,?,?,?,?,?,?,?,?)", (ident, mime, suffix, name, caption, now, now + days * 86400 if days else 0, size, recorded_at, recorded_source, filename or None))
                background_tasks.add_task(push.deliver)
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
        return await media_response(ident, request)

    async def media_response(ident: str, request: Request):
        if not re.fullmatch(r"[0-9a-f]{32}", ident):
            raise HTTPException(404)
        with db() as conn:
            row = conn.execute("SELECT * FROM posts WHERE id=? AND (expires=0 OR expires>?)", (ident, time.time())).fetchone()
        if not row:
            raise HTTPException(404, "Innlegget er slettet eller har gått ut.")
        path = media / (ident + row["suffix"])
        if not path.is_file():
            raise HTTPException(404)
        # Authenticate and check expiry/existence BEFORE any conditional response.
        # IDs are immutable: a new upload always receives a different URL.
        modified = int(path.stat().st_mtime)
        etag = f'"family-{ident}-{row["bytes"]}"'
        headers = {"Cache-Control": "private, no-cache, must-revalidate", "ETag": etag,
                   "Last-Modified": formatdate(modified, usegmt=True), "Vary": "Cookie"}
        supplied = request.headers.get("if-none-match")
        unchanged = False
        if supplied is not None:
            unchanged = any(tag.strip().removeprefix("W/") in {etag, "*"} for tag in supplied.split(","))
        elif request.headers.get("if-modified-since"):
            try:
                unchanged = parsedate_to_datetime(request.headers["if-modified-since"]).timestamp() >= modified
            except (ValueError, TypeError, OverflowError):
                pass
        if unchanged:
            return Response(status_code=304, headers=headers)
        return FileResponse(path, media_type=row["mime"], headers=headers,
                            filename=(row["filename"] or "nedlasting") if row["mime"] == "application/octet-stream" else None)

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

    from .fruen import register as register_fruen
    register_fruen(app, root, db, media_response, secure_cookie)
    return app
