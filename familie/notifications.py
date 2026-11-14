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

    def deliver(self):
        now = time.time()
        with self.db() as conn:
            rows = conn.execute("""SELECT * FROM push_devices WHERE notified=0 AND active_until<?
              AND EXISTS (SELECT 1 FROM posts WHERE created>push_devices.seen AND (expires=0 OR expires>?))""", (now, now)).fetchall()
        for device in rows:
            with self.db() as conn:
                claimed = conn.execute("UPDATE push_devices SET notified=1 WHERE id=? AND notified=0 AND active_until<? AND seen=?",
                                       (device["id"], now, device["seen"])).rowcount
            if not claimed:
                continue
            try:
                webpush(subscription_info=json.loads(device["subscription"]),
                        data=json.dumps({"title": "Nytt i familierommet", "body": "Familien har delt et nytt øyeblikk. Åpne familierommet for å se det.", "url": "/familie/"}),
                        vapid_private_key=str(self.key_path),
                        vapid_claims={"sub": "https://xn--mtes-gra.no/familie"}, ttl=3600, timeout=10)
            except WebPushException as exc:
                status = exc.response.status_code if exc.response is not None else None
                with self.db() as conn:
                    if status in {404, 410}:
                        conn.execute("DELETE FROM push_devices WHERE id=?", (device["id"],))
                    else:
                        conn.execute("UPDATE push_devices SET notified=0 WHERE id=? AND seen=?", (device["id"], device["seen"]))
                logging.getLogger(__name__).warning("Familievarsel feilet (HTTP %s)", status)
            except (OSError, ValueError):
                with self.db() as conn:
                    conn.execute("UPDATE push_devices SET notified=0 WHERE id=? AND seen=?", (device["id"], device["seen"]))
                logging.getLogger(__name__).warning("Familievarsel kunne ikke sendes")
