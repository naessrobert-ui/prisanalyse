"""ASGI entrypoint that serves Flask + (optionally) FastAPI regnskapsmodul."""

from fastapi import FastAPI
from a2wsgi import WSGIMiddleware

from app import app as flask_app

app = FastAPI(title="Prisanalyse Combined ASGI", version="1.0.1")


def _build_regnskap_fallback(reason: str) -> FastAPI:
    fallback = FastAPI(title="Regnskap API unavailable")

    @fallback.get("/")
    def root():
        return {
            "ok": False,
            "error": "Regnskap API is unavailable",
            "reason": reason,
            "hint": "Set DATABASE_URL environment variable for Fastapi_Backend.py",
        }

    @fallback.get("/health")
    def health():
        return {
            "ok": False,
            "service": "prisanalyse-api",
            "error": "Regnskap API disabled",
            "reason": reason,
        }

    return fallback


# Mount FastAPI regnskap first so '/regnskap-api/*' resolves before '/'.
try:
    from Fastapi_Backend import app as regnskap_api
except Exception as exc:  # pragma: no cover - startup fallback
    regnskap_api = _build_regnskap_fallback(repr(exc))

app.mount("/regnskap-api", regnskap_api)

# Flask app keeps existing routes on '/'.
app.mount("/", WSGIMiddleware(flask_app))
