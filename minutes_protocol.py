"""Signed, replay-bounded requests from the meeting server to the Pi."""
import base64
import hashlib
import secrets
import time

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey, Ed25519PublicKey


def message(method, path, timestamp, nonce, body):
    return '\n'.join([method.upper(), path, timestamp, nonce, hashlib.sha256(body).hexdigest()]).encode()


def public_key(private):
    return base64.b64encode(Ed25519PrivateKey.from_private_bytes(base64.b64decode(private, validate=True)).public_key().public_bytes_raw()).decode()


def sign(private, method, path, body):
    stamp, nonce = str(int(time.time())), secrets.token_hex(16)
    key = Ed25519PrivateKey.from_private_bytes(base64.b64decode(private, validate=True))
    signature = base64.b64encode(key.sign(message(method, path, stamp, nonce, body))).decode()
    return {'X-Minutes-Time': stamp, 'X-Minutes-Nonce': nonce, 'X-Minutes-Signature': signature}


def verify(public, method, path, headers, body, nonces):
    stamp, nonce = headers.get('x-minutes-time', ''), headers.get('x-minutes-nonce', '')
    if not stamp.isdigit() or abs(time.time() - int(stamp)) > 60 or len(nonce) != 32:
        raise ValueError('Expired or malformed signature')
    for old, expiry in list(nonces.items()):
        if expiry < time.time():
            del nonces[old]
    if nonce in nonces or len(nonces) > 10000:
        raise ValueError('Repeated request')
    key = Ed25519PublicKey.from_public_bytes(base64.b64decode(public, validate=True))
    key.verify(base64.b64decode(headers.get('x-minutes-signature', ''), validate=True), message(method, path, stamp, nonce, body))
    nonces[nonce] = time.time() + 120
