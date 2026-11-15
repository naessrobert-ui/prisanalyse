"""Opt-in Web Push, at most once per device between visits. All state stays on Pi."""
from __future__ import annotations

import base64
import json
import logging
import re
import time
from pathlib import Path
from urllib.parse import urlsplit

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec
from pywebpush import WebPushException, webpush


def valid_subscription(value):
    if not isinstance(value, dict):
        return False
    endpoint = value.get("endpoint", "")
    if not isinstance(endpoint, str) or len(endpoint) > 4096:
        return False
    try:
        url = urlsplit(endpoint)
        host = url.hostname or ""
        trusted = (host in {"fcm.googleapis.com", "android.googleapis.com", "web.push.apple.com"}
                   or host.endswith(".push.services.mozilla.com") or host.endswith(".notify.windows.com"))
        if url.scheme != "https" or not trusted or url.port not in {None, 443} or url.username or url.password or url.fragment:
            return False
        keys = value["keys"]
        for key, expected in (("p256dh", 65), ("auth", 16)):
            raw = keys[key]
            if not isinstance(raw, str) or not re.fullmatch(r"[A-Za-z0-9_-]+={0,2}", raw):
                return False
            decoded = base64.urlsafe_b64decode(raw + "=" * (-len(raw) % 4))
            if len(decoded) != expected:
                return False
        if base64.urlsafe_b64decode(keys["p256dh"] + "=" * (-len(keys["p256dh"]) % 4))[0] != 4:
            return False
        return True
    except (ValueError, KeyError, TypeError):
        return False


class FamilyPush:
    QUIET = 45  # seconds without new content before notifying (bulk uploads)
    REPEAT = 1800  # at most one further notification per 30 minutes until a visit

    def __init__(self, root: Path, db):
        self.db = db
        self.key_path = root / "push-vapid.pem"
        if not self.key_path.exists():
            key = ec.generate_private_key(ec.SECP256R1())
            with self.key_path.open("xb") as out:
                self.key_path.chmod(0o600)
                out.write(key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()))
        private = serialization.load_pem_private_key(self.key_path.read_bytes(), password=None)
        raw = private.public_key().public_bytes(serialization.Encoding.X962, serialization.PublicFormat.UncompressedPoint)
        self.public_key = base64.urlsafe_b64encode(raw).decode().rstrip("=")
        with db() as conn:
            conn.execute("""CREATE TABLE IF NOT EXISTS push_devices (
              id TEXT PRIMARY KEY, subscription TEXT NOT NULL, name TEXT NOT NULL,
              seen REAL NOT NULL, active_until REAL NOT NULL, notified INTEGER NOT NULL DEFAULT 0)""")
            if "notified_at" not in {row[1] for row in conn.execute("PRAGMA table_info(push_devices)")}:
                conn.execute("ALTER TABLE push_devices ADD COLUMN notified_at REAL NOT NULL DEFAULT 0")

    def summary(self, conn, device, now):
        """What is new for this device since its last visit, excluding its own posts."""
        since, me = device["seen"], device["name"]
        posts = conn.execute("""SELECT name,mime,created FROM posts WHERE created>? AND name!=? AND (expires=0 OR expires>?)""",
                             (since, me, now)).fetchall()
        messages = conn.execute("""SELECT name,created FROM messages WHERE created>? AND name!=? AND expires>?""",
                                (since, me, now)).fetchall()
        return posts, messages

    @staticmethod
    def text(posts, messages):
        def who(rows):
            names = list(dict.fromkeys(r["name"] for r in rows))
            return names[0] if len(names) == 1 else names[0] + " og " + names[1] if len(names) == 2 else "Familien"
        parts = []
        if posts:
            images = sum(r["mime"].startswith("image/") for r in posts)
            videos = sum(r["mime"].startswith("video/") for r in posts)
            n = len(posts)
            if images == n:
                what = "et nytt bilde" if n == 1 else f"{n} nye bilder"
            elif videos == n:
                what = "en ny video" if n == 1 else f"{n} nye videoer"
            elif images + videos == n:
                what = f"{n} nye bilder og videoer"
            else:
                what = "noe nytt" if n == 1 else f"{n} nye innlegg"
            parts.append(f"{who(posts)} delte {what}")
        if messages:
            n = len(messages)
            parts.append(f"{who(messages)}: " + ("en ny melding" if n == 1 else f"{n} nye meldinger") + " i familiechatten")
        title = parts[0]
        body = parts[1] if len(parts) > 1 else "Trykk for å se det i familierommet."
        return title, body

    def deliver(self, now=None):
        """Notify a device that is away, once new content has settled (an upload of many
        files gives one notification with the total). Further notifications only when
        more has arrived and at least REPEAT seconds have passed, until the next visit."""
        now = now or time.time()
        with self.db() as conn:
            rows = conn.execute("SELECT * FROM push_devices WHERE active_until<?", (now,)).fetchall()
        for device in rows:
            last = device["notified_at"]
            if last and now - last < self.REPEAT:
                continue
            with self.db() as conn:
                posts, messages = self.summary(conn, device, now)
            newest = max([r["created"] for r in posts] + [r["created"] for r in messages], default=0)
            if not newest or newest <= last or now - newest < self.QUIET:
                continue
            with self.db() as conn:
                claimed = conn.execute("UPDATE push_devices SET notified=1,notified_at=? WHERE id=? AND notified_at=? AND seen=? AND active_until<?",
                                       (now, device["id"], last, device["seen"], now)).rowcount
            if not claimed:
                continue
            title, body = self.text(posts, messages)
            try:
                webpush(subscription_info=json.loads(device["subscription"]),
                        data=json.dumps({"title": title, "body": body, "badge": len(posts) + len(messages),
                                         "url": "/familie/?chat" if messages and not posts else "/familie/"}),
                        vapid_private_key=str(self.key_path),
                        vapid_claims={"sub": "https://xn--mtes-gra.no/familie"}, ttl=3600, timeout=10)
            except WebPushException as exc:
                status = exc.response.status_code if exc.response is not None else None
                with self.db() as conn:
                    if status in {404, 410}:
                        conn.execute("DELETE FROM push_devices WHERE id=?", (device["id"],))
                    else:
                        conn.execute("UPDATE push_devices SET notified=0,notified_at=? WHERE id=? AND seen=?", (last, device["id"], device["seen"]))
                logging.getLogger(__name__).warning("Familievarsel feilet (HTTP %s)", status)
            except (OSError, ValueError):
                with self.db() as conn:
                    conn.execute("UPDATE push_devices SET notified=0,notified_at=? WHERE id=? AND seen=?", (last, device["id"], device["seen"]))
                logging.getLogger(__name__).warning("Familievarsel kunne ikke sendes")
