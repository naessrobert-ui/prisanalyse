"""ASGI entrypoint that serves Flask + mounted FastAPI APIs."""

from fastapi import FastAPI
from a2wsgi import WSGIMiddleware

from app import app as flask_app


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
except Exception as exc:
    analysis_api = _build_api_fallback(
        title="Analysis API unavailable",
        service="analysis-api",
        reason=repr(exc),
        hint="Sjekk at DATABASE_URL er satt i miljøet for analyseplattformen.",
        route_prefix="/analysis-api",
    )

app = FastAPI(title="Prisanalyse Combined ASGI", version="1.0.4")
app.include_router(analysis_api.router)
app.mount("/regnskap-api", regnskap_api)

# Flask app keeps existing routes on '/'.
app.mount("/", WSGIMiddleware(flask_app))
