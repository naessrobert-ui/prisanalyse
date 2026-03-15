"""ASGI entrypoint that serves Flask + (optionally) FastAPI regnskapsmodul."""

from contextlib import asynccontextmanager

from fastapi import FastAPI
from a2wsgi import WSGIMiddleware

from app import app as flask_app


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
    from Fastapi_Backend import app as regnskap_api, pool
    _regnskap_ok = True
except Exception as exc:  # pragma: no cover - startup fallback
    regnskap_api = _build_regnskap_fallback(repr(exc))
    pool = None
    _regnskap_ok = False


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Open/close the psycopg connection pool for the regnskap API."""
    if _regnskap_ok and pool is not None:
        print("[asgi] Opening DB connection pool...")
        pool.open()
        print("[asgi] DB pool open.")
    try:
        yield
    finally:
        if _regnskap_ok and pool is not None:
            print("[asgi] Closing DB connection pool...")
            pool.close()
            print("[asgi] DB pool closed.")


app = FastAPI(
    title="Prisanalyse Combined ASGI",
    version="1.0.2",
    lifespan=lifespan,
)

app.mount("/regnskap-api", regnskap_api)

# Flask app keeps existing routes on '/'.
app.mount("/", WSGIMiddleware(flask_app))
