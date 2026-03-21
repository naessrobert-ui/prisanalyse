from __future__ import annotations

import csv
import io
import os
import re
import subprocess
import psycopg
from decimal import Decimal
from typing import Any, Literal

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from psycopg.rows import dict_row


# ------------------------------------------------------------
# Configuration – AWS RDS IAM
# ------------------------------------------------------------
RDS_HOST = os.getenv("RDS_HOST", "brreg-mini-proff.cvos4q86gzmu.eu-north-1.rds.amazonaws.com")
RDS_PORT = int(os.getenv("RDS_PORT", "5432"))
RDS_DB   = os.getenv("RDS_DB",   "postgres")
RDS_USER = os.getenv("RDS_USER", "postgres")
AWS_REGION = os.getenv("AWS_REGION", "eu-north-1")
AWS_CLI    = os.getenv("AWS_CLI",    "aws")

ALLOWED_ORIGINS = [
    origin.strip()
    for origin in os.getenv(
        "ALLOWED_ORIGINS",
        "https://prisanalyse.no,http://localhost:3000,http://localhost:5173",
    ).split(",")
    if origin.strip()
]

DEFAULT_LIMIT = int(os.getenv("DEFAULT_LIMIT", "100"))
MAX_LIMIT     = int(os.getenv("MAX_LIMIT",     "500"))
SEARCH_EXPORT_BATCH_SIZE = int(os.getenv("SEARCH_EXPORT_BATCH_SIZE", "1000"))


# ------------------------------------------------------------
# Næringskode: tekstnavn → kode-prefix
# Legg til etter behov. Søk på ?sektor=eiendom gir naeringskode LIKE '68%'
# ------------------------------------------------------------
NAERINGSKODE_MAP: dict[str, str] = {
    # Primærnæringer
    "landbruk": "01",
    "skogbruk": "02",
    "fiske": "03",
    "akvakultur": "03",
    "oppdrett": "03",
    # Industri / olje
    "olje": "06",
    "petroleum": "06",
    "bergverk": "08",
    "industri": "10",
    "næringsmidler": "10",
    "kjemisk": "20",
    "bygg": "41",
    "anlegg": "42",
    "installasjon": "43",
    # Handel / service
    "handel": "45",
    "detaljhandel": "47",
    "engros": "46",
    "transport": "49",
    "shipping": "50",
    "luftfart": "51",
    "lagring": "52",
    "hotell": "55",
    "restaurant": "56",
    "servering": "56",
    # IKT / media
    "ikt": "62",
    "it": "62",
    "programvare": "62",
    "konsulent": "70",
    "telekom": "61",
    "media": "59",
    "film": "59",
    "reklame": "73",
    # Finans / eiendom
    "finans": "64",
    "bank": "64",
    "forsikring": "65",
    "eiendom": "68",
    # Helse / utdanning
    "helse": "86",
    "lege": "86",
    "sykehus": "86",
    "tannlege": "86",
    "barnehage": "88",
    "utdanning": "85",
    "skole": "85",
    # Organisasjoner
    "organisasjon": "94",
    "idrett": "93",
}


def resolve_naeringskode_prefix(value: str) -> str:
    """
    Returnerer kode-prefix for oppslag.
    Aksepterer enten tekstnavn ('eiendom') eller direkte kode ('68', '68.1').
    """
    normalized = value.strip().lower()
    if normalized in NAERINGSKODE_MAP:
        return NAERINGSKODE_MAP[normalized]
    # Sannsynligvis allerede en kode – returner som-er (strip whitespace)
    return value.strip()


# ------------------------------------------------------------
# Subquery som alltid henter siste regnskapsår per orgnr
# Bruk som JOIN i alle SELECT-spørringer
# ------------------------------------------------------------
LATEST_REGNSKAP_JOIN = """
    LEFT JOIN LATERAL (
        SELECT *
        FROM regnskap_metrics rm
        WHERE rm.orgnr = e.orgnr
        ORDER BY rm.accounting_year DESC NULLS LAST
        LIMIT 1
    ) r ON true
"""

# Samme, men brukt der vi ønsker et spesifikt år
def latest_regnskap_join_for_year(year: int) -> str:
    return f"""
    LEFT JOIN LATERAL (
        SELECT *
        FROM regnskap_metrics rm
        WHERE rm.orgnr = e.orgnr
          AND rm.accounting_year = {int(year)}
        LIMIT 1
    ) r ON true
"""


# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------
def clean_limit(limit: int) -> int:
    return max(1, min(limit, MAX_LIMIT))


def normalize_search_tokens(value: str) -> list[str]:
    cleaned = (value or "").lower()
    cleaned = re.sub(r"\b(as|asa|ans|nuf|sa|d[aå])\b", " ", cleaned)
    cleaned = re.sub(r"[^0-9a-zæøå]+", " ", cleaned)
    tokens = [token for token in cleaned.split() if token]
    return list(dict.fromkeys(tokens))[:8]


def build_search_base_sql(
    *,
    orgnr: str | None = None,
    q: str | None = None,
    orgform: str | None = None,
    kommune: str | None = None,
    naeringskode_prefix: str | None = None,
    sektor: str | None = None,
    accounting_year: int | None = None,
    min_revenue: float | None = None,
    max_revenue: float | None = None,
    min_profit: float | None = None,
    max_profit: float | None = None,
    min_operating_profit: float | None = None,
    max_operating_profit: float | None = None,
    min_equity_ratio: float | None = None,
    max_equity_ratio: float | None = None,
    min_total_assets: float | None = None,
    max_total_assets: float | None = None,
    min_equity: float | None = None,
    max_equity: float | None = None,
    has_regnskap: bool = False,
) -> tuple[str, list[Any], str]:
    regnskap_join = (
        latest_regnskap_join_for_year(accounting_year)
        if accounting_year
        else LATEST_REGNSKAP_JOIN
    )
    base_sql = f"""
        FROM entity e
        {regnskap_join}
        WHERE 1=1
    """
    params: list[Any] = []
    base_sql = append_search_filters(
        base_sql,
        params,
        orgnr=orgnr,
        q=q,
        orgform=orgform,
        kommune=kommune,
        naeringskode_prefix=naeringskode_prefix,
        sektor=sektor,
        min_revenue=min_revenue,
        max_revenue=max_revenue,
        min_profit=min_profit,
        max_profit=max_profit,
        min_operating_profit=min_operating_profit,
        max_operating_profit=max_operating_profit,
        min_equity_ratio=min_equity_ratio,
        max_equity_ratio=max_equity_ratio,
        min_total_assets=min_total_assets,
        max_total_assets=max_total_assets,
        min_equity=min_equity,
        max_equity=max_equity,
        has_regnskap=has_regnskap,
    )
    return base_sql, params, regnskap_join


def build_csv_response(columns: list[str], batch_iter, filename: str) -> StreamingResponse:
    def generate():
        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(columns)
        yield buffer.getvalue()
        buffer.seek(0)
        buffer.truncate(0)

        for rows in batch_iter:
            for row in rows:
                writer.writerow([row.get(col, "") if row.get(col) is not None else "" for col in columns])
            yield buffer.getvalue()
            buffer.seek(0)
            buffer.truncate(0)

    return StreamingResponse(
        generate(),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


def normalize_decimal(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, dict):
        return {k: normalize_decimal(v) for k, v in value.items()}
    if isinstance(value, list):
        return [normalize_decimal(v) for v in value]
    return value


# IAM-token cache – gyldige i 15 min, vi fornyer etter 12 min
import time
_token_cache: dict = {"token": None, "expires_at": 0.0}
_TOKEN_TTL = 12 * 60  # 12 minutter


def get_iam_token() -> str:
    """Generer midlertidig RDS IAM-token via AWS CLI. Caches i 12 min."""
    now = time.monotonic()
    if _token_cache["token"] and now < _token_cache["expires_at"]:
        return _token_cache["token"]
    try:
        token = subprocess.check_output(
            [AWS_CLI, "rds", "generate-db-auth-token",
             "--hostname", RDS_HOST,
             "--port", str(RDS_PORT),
             "--username", RDS_USER,
             "--region", AWS_REGION],
            text=True,
        ).strip()
        _token_cache["token"] = token
        _token_cache["expires_at"] = now + _TOKEN_TTL
        print(f"[iam] Nytt token generert, gyldig til {_TOKEN_TTL}s fra nå")
        return token
    except FileNotFoundError as e:
        raise HTTPException(status_code=500, detail="AWS CLI ikke funnet på serveren.") from e
    except subprocess.CalledProcessError as e:
        raise HTTPException(status_code=500, detail=f"Klarte ikke generere RDS IAM token: {e}") from e


def get_conn() -> psycopg.Connection:
    token = get_iam_token()
    return psycopg.connect(
        host=RDS_HOST,
        port=RDS_PORT,
        dbname=RDS_DB,
        user=RDS_USER,
        password=token,
        sslmode="require",
        row_factory=dict_row,
        connect_timeout=10,
    )


def fetch_all(sql: str, params: list[Any] | tuple[Any, ...]) -> list[dict[str, Any]]:
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                print("SQL:", sql)
                print("PARAMS:", params)
                cur.execute(sql, params)
                rows = cur.fetchall()
                print("ROW COUNT:", len(rows))
                return [normalize_decimal(dict(row)) for row in rows]
    except HTTPException:
        raise
    except Exception as e:
        print("FETCH_ALL ERROR:", repr(e))
        raise HTTPException(status_code=500, detail=f"DB error: {repr(e)}")


def fetch_one(sql: str, params: list[Any] | tuple[Any, ...]) -> dict[str, Any] | None:
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                print("SQL:", sql)
                print("PARAMS:", params)
                cur.execute(sql, params)
                row = cur.fetchone()
                print("ROW:", row)
                return normalize_decimal(dict(row)) if row else None
    except HTTPException:
        raise
    except Exception as e:
        print("FETCH_ONE ERROR:", repr(e))
        raise HTTPException(status_code=500, detail=f"DB error: {repr(e)}")


# ------------------------------------------------------------
# Response models
# ------------------------------------------------------------
class SearchResult(BaseModel):
    orgnr: str
    navn: str | None = None
    orgform: str | None = None
    naeringskode: str | None = None
    kommunenummer: str | None = None
    postnummer: str | None = None
    adresse: str | None = None
    accounting_year: int | None = None
    revenue: float | None = None
    operating_profit: float | None = None
    net_profit: float | None = None
    total_assets: float | None = None
    equity: float | None = None
    equity_ratio: float | None = None
    lat: float | None = None
    lon: float | None = None
    distance_m: float | None = None


class SearchResponse(BaseModel):
    total_count: int        # totalt antall treff (uavhengig av paginering)
    total_returned: int     # antall returnert i denne siden
    limit: int
    offset: int
    results: list[SearchResult]


class InsightBucket(BaseModel):
    key: str
    count: int


class SearchInsightsResponse(BaseModel):
    total_matches: int
    by_kommune: list[InsightBucket]
    by_naeringskode_prefix: list[InsightBucket]
    by_revenue_band: list[InsightBucket]


class SearchSummaryResponse(BaseModel):
    total_matches: int
    companies_with_regnskap: int
    sum_revenue: float = 0.0
    sum_operating_profit: float = 0.0
    sum_net_profit: float = 0.0
    sum_equity: float = 0.0
    avg_equity_ratio: float | None = None


class DatabaseDiagnosticsResponse(BaseModel):
    total_entities: int
    entities_with_nonempty_name: int
    entities_missing_name: int
    entities_with_blank_name: int
    distinct_names: int
    orgnr_like_names: int
    names_equal_address: int
    total_regnskap_rows: int
    entities_with_regnskap: int


class CompanyDetail(BaseModel):
    orgnr: str
    navn: str | None = None
    orgform: str | None = None
    naeringskode: str | None = None
    ansatte: int | None = None
    mva: bool | None = None
    status: str | None = None
    stiftet: str | None = None
    registrert: str | None = None
    oppdatert: str | None = None
    kommunenummer: str | None = None
    postnummer: str | None = None
    adresse: str | None = None
    lat: float | None = None
    lon: float | None = None
    accounting_year: int | None = None
    revenue: float | None = None
    operating_profit: float | None = None
    net_profit: float | None = None
    total_assets: float | None = None
    equity: float | None = None
    total_liabilities: float | None = None
    current_assets: float | None = None
    short_term_liabilities: float | None = None
    equity_ratio: float | None = None
    ebit_margin: float | None = None
    net_margin: float | None = None


class HealthResponse(BaseModel):
    ok: bool = True
    service: str = "prisanalyse-api"


# ------------------------------------------------------------
# App (ingen pool – ny IAM-tilkobling per request)
# ------------------------------------------------------------
app = FastAPI(
    title="Prisanalyse API",
    version="0.2.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def root():
    return {"message": "Prisanalyse API kjører"}


@app.get("/health")
def health():
    return {"ok": True, "service": "prisanalyse-api"}


@app.get("/debug/direct")
def debug_direct():
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT current_database() AS db, current_user AS usr")
                return dict(cur.fetchone())
    except Exception as e:
        return {"error": repr(e)}


# ------------------------------------------------------------
# Filter-builder (alle søkeendepunkter bruker denne)
# ------------------------------------------------------------
def append_search_filters(
    sql: str,
    params: list[Any],
    *,
    orgnr: str | None = None,
    q: str | None = None,
    orgform: str | None = None,
    kommune: str | None = None,
    naeringskode_prefix: str | None = None,   # rå kode eller tekstnavn
    sektor: str | None = None,                 # alias for naeringskode via NAERINGSKODE_MAP
    min_revenue: float | None = None,
    max_revenue: float | None = None,
    min_profit: float | None = None,
    max_profit: float | None = None,
    min_operating_profit: float | None = None,
    max_operating_profit: float | None = None,
    min_equity_ratio: float | None = None,
    max_equity_ratio: float | None = None,
    min_total_assets: float | None = None,
    max_total_assets: float | None = None,
    min_equity: float | None = None,
    max_equity: float | None = None,
    has_regnskap: bool = False,
) -> str:
    if orgnr:
        sql += " AND e.orgnr::text = %s"
        params.append(orgnr)

    if q:
        tokens = normalize_search_tokens(q)
        if tokens:
            sql += " AND " + " AND ".join("e.navn ILIKE %s" for _ in tokens)
            params.extend(f"%{token}%" for token in tokens)
        else:
            sql += " AND e.navn ILIKE %s"
            params.append(f"%{q}%")

    if orgform:
        sql += " AND e.orgform = %s"
        params.append(orgform.upper())

    if kommune:
        sql += " AND e.kommunenummer = %s"
        params.append(kommune)

    # sektor (tekst) overstyres av naeringskode_prefix (rå kode) hvis begge er satt
    effective_prefix: str | None = None
    if naeringskode_prefix:
        effective_prefix = resolve_naeringskode_prefix(naeringskode_prefix)
    elif sektor:
        effective_prefix = resolve_naeringskode_prefix(sektor)

    if effective_prefix:
        sql += " AND e.naeringskode LIKE %s"
        params.append(f"{effective_prefix}%")

    if min_revenue is not None:
        sql += " AND r.revenue >= %s"
        params.append(min_revenue)
    if max_revenue is not None:
        sql += " AND r.revenue <= %s"
        params.append(max_revenue)

    if min_profit is not None:
        sql += " AND r.net_profit >= %s"
        params.append(min_profit)
    if max_profit is not None:
        sql += " AND r.net_profit <= %s"
        params.append(max_profit)

    if min_operating_profit is not None:
        sql += " AND r.operating_profit >= %s"
        params.append(min_operating_profit)
    if max_operating_profit is not None:
        sql += " AND r.operating_profit <= %s"
        params.append(max_operating_profit)

    if min_equity_ratio is not None:
        sql += " AND r.equity_ratio >= %s"
        params.append(min_equity_ratio)
    if max_equity_ratio is not None:
        sql += " AND r.equity_ratio <= %s"
        params.append(max_equity_ratio)

    if min_total_assets is not None:
        sql += " AND r.total_assets >= %s"
        params.append(min_total_assets)
    if max_total_assets is not None:
        sql += " AND r.total_assets <= %s"
        params.append(max_total_assets)

    if min_equity is not None:
        sql += " AND r.equity >= %s"
        params.append(min_equity)
    if max_equity is not None:
        sql += " AND r.equity <= %s"
        params.append(max_equity)

    if has_regnskap:
        sql += " AND r.accounting_year IS NOT NULL"

    return sql


# SELECT-kolonner som brukes av search og nearby
_SEARCH_COLS = """
    e.orgnr::text AS orgnr,
    e.navn,
    e.orgform,
    e.naeringskode,
    e.kommunenummer,
    e.postnummer,
    e.adresse,
    e.lat,
    e.lon,
    r.accounting_year,
    r.revenue,
    r.operating_profit,
    r.net_profit,
    r.total_assets,
    r.equity,
    r.equity_ratio
"""

_SORT_MAP = {
    "revenue":           "r.revenue DESC NULLS LAST, e.navn ASC",
    "profit":            "r.net_profit DESC NULLS LAST, e.navn ASC",
    "operating_profit":  "r.operating_profit DESC NULLS LAST, e.navn ASC",
    "equity":            "r.equity DESC NULLS LAST, e.navn ASC",
    "total_assets":      "r.total_assets DESC NULLS LAST, e.navn ASC",
    "equity_ratio":      "r.equity_ratio DESC NULLS LAST, e.navn ASC",
    "name":              "e.navn ASC",
}


# ------------------------------------------------------------
# Endpoints
# ------------------------------------------------------------
@app.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    return HealthResponse()


@app.get("/debug/direct")
def debug_direct():
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT current_database() AS db, current_user AS usr")
                return dict(cur.fetchone())
    except Exception as e:
        return {
            "error": repr(e),
            "db_host": RDS_HOST,
            "db_port": RDS_PORT,
            "db_name": RDS_DB,
            "db_user": RDS_USER,
        }


@app.get("/api/naeringskoder")
def list_naeringskoder() -> dict:
    """Returnerer alle kjente tekstnavn → kode-mappinger."""
    return {"mappings": NAERINGSKODE_MAP}


@app.get("/api/db/diagnostics", response_model=DatabaseDiagnosticsResponse)
def db_diagnostics() -> DatabaseDiagnosticsResponse:
    row = fetch_one(
        """
        SELECT
            COUNT(*)::int AS total_entities,
            COUNT(*) FILTER (
                WHERE e.navn IS NOT NULL AND NULLIF(BTRIM(e.navn), '') IS NOT NULL
            )::int AS entities_with_nonempty_name,
            COUNT(*) FILTER (WHERE e.navn IS NULL)::int AS entities_missing_name,
            COUNT(*) FILTER (WHERE e.navn IS NOT NULL AND NULLIF(BTRIM(e.navn), '') IS NULL)::int AS entities_with_blank_name,
            COUNT(DISTINCT NULLIF(BTRIM(e.navn), ''))::int AS distinct_names,
            COUNT(*) FILTER (WHERE COALESCE(BTRIM(e.navn), '') ~ '^[0-9]{9}$')::int AS orgnr_like_names,
            COUNT(*) FILTER (
                WHERE NULLIF(BTRIM(e.navn), '') IS NOT NULL
                  AND NULLIF(BTRIM(e.adresse), '') IS NOT NULL
                  AND LOWER(BTRIM(e.navn)) = LOWER(BTRIM(e.adresse))
            )::int AS names_equal_address,
            COALESCE((SELECT COUNT(*)::int FROM regnskap_metrics), 0)::int AS total_regnskap_rows,
            COUNT(*) FILTER (WHERE r.accounting_year IS NOT NULL)::int AS entities_with_regnskap
        FROM entity e
        LEFT JOIN LATERAL (
            SELECT rm.accounting_year
            FROM regnskap_metrics rm
            WHERE rm.orgnr = e.orgnr
            LIMIT 1
        ) r ON true
        """,
        [],
    ) or {
        "total_entities": 0,
        "entities_with_nonempty_name": 0,
        "entities_missing_name": 0,
        "entities_with_blank_name": 0,
        "distinct_names": 0,
        "orgnr_like_names": 0,
        "names_equal_address": 0,
        "total_regnskap_rows": 0,
        "entities_with_regnskap": 0,
    }
    return DatabaseDiagnosticsResponse(**row)


@app.get("/api/search", response_model=SearchResponse)
def search(
    q: str | None = Query(default=None, description="Fritekst mot navn"),
    orgnr: str | None = None,
    orgform: str | None = Query(default=None, description="F.eks. AS, ENK, ANS"),
    kommune: str | None = Query(default=None, description="Kommunenummer, f.eks. 4601"),
    naeringskode_prefix: str | None = Query(
        default=None, alias="naeringskode",
        description="Kode-prefix (f.eks. '68') eller tekstnavn (f.eks. 'eiendom')",
    ),
    sektor: str | None = Query(
        default=None,
        description="Tekstnavn for sektor, f.eks. 'eiendom', 'it', 'shipping'",
    ),
    accounting_year: int | None = Query(default=None, description="Hent tall for spesifikt år"),
    min_revenue: float | None = Query(default=None, description="Min omsetning (NOK)"),
    max_revenue: float | None = Query(default=None, description="Maks omsetning (NOK)"),
    min_profit: float | None = Query(default=None, description="Min nettoresultat"),
    max_profit: float | None = Query(default=None, description="Maks nettoresultat"),
    min_operating_profit: float | None = Query(default=None, description="Min driftsresultat"),
    max_operating_profit: float | None = Query(default=None, description="Maks driftsresultat"),
    min_equity_ratio: float | None = Query(default=None, description="Min egenkapitalandel (0–1)"),
    max_equity_ratio: float | None = Query(default=None, description="Maks egenkapitalandel (0–1)"),
    min_total_assets: float | None = Query(default=None, description="Min sum eiendeler"),
    max_total_assets: float | None = Query(default=None, description="Maks sum eiendeler"),
    min_equity: float | None = Query(default=None, description="Min egenkapital"),
    max_equity: float | None = Query(default=None, description="Maks egenkapital"),
    has_regnskap: bool = False,
    sort: Literal[
        "revenue", "profit", "operating_profit", "equity", "total_assets", "equity_ratio", "name"
    ] = "revenue",
    limit: int = DEFAULT_LIMIT,
    offset: int = 0,
) -> SearchResponse:
    limit = clean_limit(limit)

    base_sql, params, _regnskap_join = build_search_base_sql(
        orgnr=orgnr, q=q, orgform=orgform, kommune=kommune,
        accounting_year=accounting_year,
        naeringskode_prefix=naeringskode_prefix, sektor=sektor,
        min_revenue=min_revenue, max_revenue=max_revenue,
        min_profit=min_profit, max_profit=max_profit,
        min_operating_profit=min_operating_profit, max_operating_profit=max_operating_profit,
        min_equity_ratio=min_equity_ratio, max_equity_ratio=max_equity_ratio,
        min_total_assets=min_total_assets, max_total_assets=max_total_assets,
        min_equity=min_equity, max_equity=max_equity,
        has_regnskap=has_regnskap,
    )

    # Total count (same WHERE, no LIMIT)
    count_row = fetch_one(f"SELECT COUNT(*)::int AS n {base_sql}", params) or {"n": 0}
    total_count: int = count_row["n"]

    order_by = _SORT_MAP[sort]
    data_sql = (
        f"SELECT {_SEARCH_COLS}, NULL::double precision AS distance_m {base_sql}"
        f" ORDER BY {order_by} LIMIT %s OFFSET %s"
    )
    rows = fetch_all(data_sql, [*params, limit, offset])

    return SearchResponse(
        total_count=total_count,
        total_returned=len(rows),
        limit=limit,
        offset=offset,
        results=[SearchResult(**row) for row in rows],
    )


@app.get("/api/search/insights", response_model=SearchInsightsResponse)
def search_insights(
    q: str | None = Query(default=None, description="Fritekst mot navn"),
    orgnr: str | None = None,
    orgform: str | None = None,
    kommune: str | None = None,
    naeringskode_prefix: str | None = Query(default=None, alias="naeringskode"),
    sektor: str | None = None,
    accounting_year: int | None = None,
    min_revenue: float | None = None,
    max_revenue: float | None = None,
    min_profit: float | None = None,
    max_profit: float | None = None,
    min_operating_profit: float | None = None,
    max_operating_profit: float | None = None,
    min_equity_ratio: float | None = None,
    max_equity_ratio: float | None = None,
    min_total_assets: float | None = None,
    max_total_assets: float | None = None,
    min_equity: float | None = None,
    max_equity: float | None = None,
    has_regnskap: bool = False,
    group_limit: int = Query(default=8, ge=1, le=20),
) -> SearchInsightsResponse:
    base_sql, base_params, _regnskap_join = build_search_base_sql(
        orgnr=orgnr, q=q, orgform=orgform, kommune=kommune,
        accounting_year=accounting_year,
        naeringskode_prefix=naeringskode_prefix, sektor=sektor,
        min_revenue=min_revenue, max_revenue=max_revenue,
        min_profit=min_profit, max_profit=max_profit,
        min_operating_profit=min_operating_profit, max_operating_profit=max_operating_profit,
        min_equity_ratio=min_equity_ratio, max_equity_ratio=max_equity_ratio,
        min_total_assets=min_total_assets, max_total_assets=max_total_assets,
        min_equity=min_equity, max_equity=max_equity,
        has_regnskap=has_regnskap,
    )

    total_row = fetch_one(f"SELECT COUNT(*)::int AS total_matches {base_sql}", base_params) or {"total_matches": 0}

    kommune_rows = fetch_all(
        f"""
        SELECT
            COALESCE(NULLIF(e.kommunenummer, ''), 'Ukjent') AS key,
            COUNT(*)::int AS count
        {base_sql}
        GROUP BY 1
        ORDER BY count DESC, key ASC
        LIMIT %s
        """,
        [*base_params, group_limit],
    )

    naering_rows = fetch_all(
        f"""
        SELECT
            COALESCE(NULLIF(SUBSTRING(e.naeringskode FROM 1 FOR 2), ''), 'Ukjent') AS key,
            COUNT(*)::int AS count
        {base_sql}
        GROUP BY 1
        ORDER BY count DESC, key ASC
        LIMIT %s
        """,
        [*base_params, group_limit],
    )

    revenue_rows = fetch_all(
        f"""
        SELECT
            CASE
                WHEN r.revenue IS NULL      THEN 'Mangler omsetning'
                WHEN r.revenue < 1000000    THEN '< 1 mill'
                WHEN r.revenue < 10000000   THEN '1–10 mill'
                WHEN r.revenue < 100000000  THEN '10–100 mill'
                WHEN r.revenue < 1000000000 THEN '100 mill–1 mrd'
                ELSE                             '> 1 mrd'
            END AS key,
            COUNT(*)::int AS count
        {base_sql}
        GROUP BY 1
        ORDER BY count DESC, key ASC
        """,
        base_params,
    )

    return SearchInsightsResponse(
        total_matches=total_row.get("total_matches", 0),
        by_kommune=[InsightBucket(**row) for row in kommune_rows],
        by_naeringskode_prefix=[InsightBucket(**row) for row in naering_rows],
        by_revenue_band=[InsightBucket(**row) for row in revenue_rows],
    )


@app.get("/api/search/summary", response_model=SearchSummaryResponse)
def search_summary(
    q: str | None = Query(default=None, description="Fritekst mot navn"),
    orgnr: str | None = None,
    orgform: str | None = None,
    kommune: str | None = None,
    naeringskode_prefix: str | None = Query(default=None, alias="naeringskode"),
    sektor: str | None = None,
    accounting_year: int | None = None,
    min_revenue: float | None = None,
    max_revenue: float | None = None,
    min_profit: float | None = None,
    max_profit: float | None = None,
    min_operating_profit: float | None = None,
    max_operating_profit: float | None = None,
    min_equity_ratio: float | None = None,
    max_equity_ratio: float | None = None,
    min_total_assets: float | None = None,
    max_total_assets: float | None = None,
    min_equity: float | None = None,
    max_equity: float | None = None,
    has_regnskap: bool = False,
) -> SearchSummaryResponse:
    base_sql, base_params, _regnskap_join = build_search_base_sql(
        orgnr=orgnr,
        q=q,
        orgform=orgform,
        kommune=kommune,
        accounting_year=accounting_year,
        naeringskode_prefix=naeringskode_prefix,
        sektor=sektor,
        min_revenue=min_revenue,
        max_revenue=max_revenue,
        min_profit=min_profit,
        max_profit=max_profit,
        min_operating_profit=min_operating_profit,
        max_operating_profit=max_operating_profit,
        min_equity_ratio=min_equity_ratio,
        max_equity_ratio=max_equity_ratio,
        min_total_assets=min_total_assets,
        max_total_assets=max_total_assets,
        min_equity=min_equity,
        max_equity=max_equity,
        has_regnskap=has_regnskap,
    )
    summary_row = fetch_one(
        f"""
        SELECT
            COUNT(*)::int AS total_matches,
            COUNT(*) FILTER (WHERE r.accounting_year IS NOT NULL)::int AS companies_with_regnskap,
            COALESCE(SUM(r.revenue), 0)::double precision AS sum_revenue,
            COALESCE(SUM(r.operating_profit), 0)::double precision AS sum_operating_profit,
            COALESCE(SUM(r.net_profit), 0)::double precision AS sum_net_profit,
            COALESCE(SUM(r.equity), 0)::double precision AS sum_equity,
            AVG(r.equity_ratio)::double precision AS avg_equity_ratio
        {base_sql}
        """,
        base_params,
    ) or {
        "total_matches": 0,
        "companies_with_regnskap": 0,
        "sum_revenue": 0.0,
        "sum_operating_profit": 0.0,
        "sum_net_profit": 0.0,
        "sum_equity": 0.0,
        "avg_equity_ratio": None,
    }
    return SearchSummaryResponse(**summary_row)


@app.get("/api/search/export.csv")
def export_search_csv(
    q: str | None = Query(default=None, description="Fritekst mot navn"),
    orgnr: str | None = None,
    orgform: str | None = None,
    kommune: str | None = None,
    naeringskode_prefix: str | None = Query(default=None, alias="naeringskode"),
    sektor: str | None = None,
    accounting_year: int | None = None,
    min_revenue: float | None = None,
    max_revenue: float | None = None,
    min_profit: float | None = None,
    max_profit: float | None = None,
    min_operating_profit: float | None = None,
    max_operating_profit: float | None = None,
    min_equity_ratio: float | None = None,
    max_equity_ratio: float | None = None,
    min_total_assets: float | None = None,
    max_total_assets: float | None = None,
    min_equity: float | None = None,
    max_equity: float | None = None,
    has_regnskap: bool = False,
    sort: Literal[
        "revenue", "profit", "operating_profit", "equity", "total_assets", "equity_ratio", "name"
    ] = "revenue",
) -> StreamingResponse:
    base_sql, base_params, _regnskap_join = build_search_base_sql(
        orgnr=orgnr,
        q=q,
        orgform=orgform,
        kommune=kommune,
        accounting_year=accounting_year,
        naeringskode_prefix=naeringskode_prefix,
        sektor=sektor,
        min_revenue=min_revenue,
        max_revenue=max_revenue,
        min_profit=min_profit,
        max_profit=max_profit,
        min_operating_profit=min_operating_profit,
        max_operating_profit=max_operating_profit,
        min_equity_ratio=min_equity_ratio,
        max_equity_ratio=max_equity_ratio,
        min_total_assets=min_total_assets,
        max_total_assets=max_total_assets,
        min_equity=min_equity,
        max_equity=max_equity,
        has_regnskap=has_regnskap,
    )
    order_by = _SORT_MAP[sort]
    columns = [
        "orgnr",
        "navn",
        "orgform",
        "naeringskode",
        "kommunenummer",
        "postnummer",
        "adresse",
        "accounting_year",
        "revenue",
        "operating_profit",
        "net_profit",
        "total_assets",
        "equity",
        "equity_ratio",
        "lat",
        "lon",
    ]

    def batch_iter():
        offset = 0
        while True:
            rows = fetch_all(
                f"""
                SELECT {_SEARCH_COLS}
                {base_sql}
                ORDER BY {order_by}
                LIMIT %s OFFSET %s
                """,
                [*base_params, SEARCH_EXPORT_BATCH_SIZE, offset],
            )
            if not rows:
                break
            yield rows
            offset += len(rows)

    return build_csv_response(columns, batch_iter(), "regnskap_search_results.csv")


@app.get("/api/company/{orgnr}", response_model=CompanyDetail)
def company_detail(orgnr: str) -> CompanyDetail:
    """
    Henter siste regnskap via LATERAL JOIN – ikke LIMIT 1 på tvers av år.
    """
    sql = f"""
        SELECT
            e.orgnr::text AS orgnr,
            e.navn,
            e.orgform,
            e.naeringskode,
            e.ansatte,
            e.mva,
            e.status,
            e.stiftet::text,
            e.registrert::text,
            e.oppdatert::text,
            e.kommunenummer,
            e.postnummer,
            e.adresse,
            e.lat,
            e.lon,
            r.accounting_year,
            r.revenue,
            r.operating_profit,
            r.net_profit,
            r.total_assets,
            r.equity,
            r.total_liabilities,
            r.current_assets,
            r.short_term_liabilities,
            r.equity_ratio,
            r.ebit_margin,
            r.net_margin
        FROM entity e
        {LATEST_REGNSKAP_JOIN}
        WHERE e.orgnr::text = %s
    """
    row = fetch_one(sql, [orgnr])
    if not row:
        raise HTTPException(status_code=404, detail="Company not found")
    return CompanyDetail(**row)


@app.get("/api/nearby", response_model=SearchResponse)
def nearby(
    lat: float,
    lon: float,
    radius_km: float = Query(default=5.0, gt=0),
    q: str | None = None,
    orgform: str | None = None,
    naeringskode_prefix: str | None = Query(default=None, alias="naeringskode"),
    sektor: str | None = None,
    accounting_year: int | None = None,
    min_revenue: float | None = None,
    max_revenue: float | None = None,
    min_profit: float | None = None,
    max_profit: float | None = None,
    min_operating_profit: float | None = None,
    max_operating_profit: float | None = None,
    min_equity_ratio: float | None = None,
    max_equity_ratio: float | None = None,
    min_total_assets: float | None = None,
    max_total_assets: float | None = None,
    has_regnskap: bool = False,
    limit: int = DEFAULT_LIMIT,
    offset: int = 0,
) -> SearchResponse:
    limit = clean_limit(limit)
    radius_m = radius_km * 1000

    regnskap_join = (
        latest_regnskap_join_for_year(accounting_year)
        if accounting_year
        else LATEST_REGNSKAP_JOIN
    )

    sql = f"""
        SELECT
            {_SEARCH_COLS},
            ST_Distance(
                e.geog,
                ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography
            ) AS distance_m
        FROM entity e
        {regnskap_join}
        WHERE e.geog IS NOT NULL
          AND ST_DWithin(
                e.geog,
                ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography,
                %s
          )
    """
    params: list[Any] = [lon, lat, lon, lat, radius_m]

    params_filters: list[Any] = []
    # Bruk append_search_filters på en tom streng og hent bare AND-delen
    filter_sql = append_search_filters(
        "",
        params_filters,
        q=q, orgform=orgform,
        naeringskode_prefix=naeringskode_prefix, sektor=sektor,
        min_revenue=min_revenue, max_revenue=max_revenue,
        min_profit=min_profit, max_profit=max_profit,
        min_operating_profit=min_operating_profit, max_operating_profit=max_operating_profit,
        min_equity_ratio=min_equity_ratio, max_equity_ratio=max_equity_ratio,
        min_total_assets=min_total_assets, max_total_assets=max_total_assets,
        has_regnskap=has_regnskap,
    )
    sql += filter_sql
    params.extend(params_filters)

    # Count
    count_sql = f"SELECT COUNT(*)::int AS n FROM entity e {regnskap_join} WHERE e.geog IS NOT NULL AND ST_DWithin(e.geog, ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography, %s){filter_sql}"
    count_params = [lon, lat, radius_m, *params_filters]
    count_row = fetch_one(count_sql, count_params) or {"n": 0}

    sql += " ORDER BY distance_m ASC LIMIT %s OFFSET %s"
    params.extend([limit, offset])

    rows = fetch_all(sql, params)
    return SearchResponse(
        total_count=count_row["n"],
        total_returned=len(rows),
        limit=limit,
        offset=offset,
        results=[SearchResult(**row) for row in rows],
    )


@app.get("/api/nearby/summary", response_model=SearchSummaryResponse)
def nearby_summary(
    lat: float,
    lon: float,
    radius_km: float = Query(default=5.0, gt=0),
    q: str | None = None,
    orgform: str | None = None,
    naeringskode_prefix: str | None = Query(default=None, alias="naeringskode"),
    sektor: str | None = None,
    accounting_year: int | None = None,
    min_revenue: float | None = None,
    max_revenue: float | None = None,
    min_profit: float | None = None,
    max_profit: float | None = None,
    min_operating_profit: float | None = None,
    max_operating_profit: float | None = None,
    min_equity_ratio: float | None = None,
    max_equity_ratio: float | None = None,
    min_total_assets: float | None = None,
    max_total_assets: float | None = None,
    has_regnskap: bool = False,
) -> SearchSummaryResponse:
    radius_m = radius_km * 1000
    regnskap_join = (
        latest_regnskap_join_for_year(accounting_year)
        if accounting_year
        else LATEST_REGNSKAP_JOIN
    )
    params_filters: list[Any] = []
    filter_sql = append_search_filters(
        "",
        params_filters,
        q=q,
        orgform=orgform,
        naeringskode_prefix=naeringskode_prefix,
        sektor=sektor,
        min_revenue=min_revenue,
        max_revenue=max_revenue,
        min_profit=min_profit,
        max_profit=max_profit,
        min_operating_profit=min_operating_profit,
        max_operating_profit=max_operating_profit,
        min_equity_ratio=min_equity_ratio,
        max_equity_ratio=max_equity_ratio,
        min_total_assets=min_total_assets,
        max_total_assets=max_total_assets,
        has_regnskap=has_regnskap,
    )
    row = fetch_one(
        f"""
        SELECT
            COUNT(*)::int AS total_matches,
            COUNT(*) FILTER (WHERE r.accounting_year IS NOT NULL)::int AS companies_with_regnskap,
            COALESCE(SUM(r.revenue), 0)::double precision AS sum_revenue,
            COALESCE(SUM(r.operating_profit), 0)::double precision AS sum_operating_profit,
            COALESCE(SUM(r.net_profit), 0)::double precision AS sum_net_profit,
            COALESCE(SUM(r.equity), 0)::double precision AS sum_equity,
            AVG(r.equity_ratio)::double precision AS avg_equity_ratio
        FROM entity e
        {regnskap_join}
        WHERE e.geog IS NOT NULL
          AND ST_DWithin(
                e.geog,
                ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography,
                %s
          )
          {filter_sql}
        """,
        [lon, lat, radius_m, *params_filters],
    ) or {
        "total_matches": 0,
        "companies_with_regnskap": 0,
        "sum_revenue": 0.0,
        "sum_operating_profit": 0.0,
        "sum_net_profit": 0.0,
        "sum_equity": 0.0,
        "avg_equity_ratio": None,
    }
    return SearchSummaryResponse(**row)


@app.get("/api/nearby/export.csv")
def export_nearby_csv(
    lat: float,
    lon: float,
    radius_km: float = Query(default=5.0, gt=0),
    q: str | None = None,
    orgform: str | None = None,
    naeringskode_prefix: str | None = Query(default=None, alias="naeringskode"),
    sektor: str | None = None,
    accounting_year: int | None = None,
    min_revenue: float | None = None,
    max_revenue: float | None = None,
    min_profit: float | None = None,
    max_profit: float | None = None,
    min_operating_profit: float | None = None,
    max_operating_profit: float | None = None,
    min_equity_ratio: float | None = None,
    max_equity_ratio: float | None = None,
    min_total_assets: float | None = None,
    max_total_assets: float | None = None,
    has_regnskap: bool = False,
) -> StreamingResponse:
    radius_m = radius_km * 1000
    regnskap_join = (
        latest_regnskap_join_for_year(accounting_year)
        if accounting_year
        else LATEST_REGNSKAP_JOIN
    )
    params_filters: list[Any] = []
    filter_sql = append_search_filters(
        "",
        params_filters,
        q=q,
        orgform=orgform,
        naeringskode_prefix=naeringskode_prefix,
        sektor=sektor,
        min_revenue=min_revenue,
        max_revenue=max_revenue,
        min_profit=min_profit,
        max_profit=max_profit,
        min_operating_profit=min_operating_profit,
        max_operating_profit=max_operating_profit,
        min_equity_ratio=min_equity_ratio,
        max_equity_ratio=max_equity_ratio,
        min_total_assets=min_total_assets,
        max_total_assets=max_total_assets,
        has_regnskap=has_regnskap,
    )
    columns = [
        "orgnr",
        "navn",
        "orgform",
        "naeringskode",
        "kommunenummer",
        "postnummer",
        "adresse",
        "accounting_year",
        "revenue",
        "operating_profit",
        "net_profit",
        "total_assets",
        "equity",
        "equity_ratio",
        "lat",
        "lon",
        "distance_m",
    ]

    def batch_iter():
        offset = 0
        while True:
            rows = fetch_all(
                f"""
                SELECT
                    {_SEARCH_COLS},
                    ST_Distance(
                        e.geog,
                        ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography
                    ) AS distance_m
                FROM entity e
                {regnskap_join}
                WHERE e.geog IS NOT NULL
                  AND ST_DWithin(
                        e.geog,
                        ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography,
                        %s
                  )
                  {filter_sql}
                ORDER BY distance_m ASC
                LIMIT %s OFFSET %s
                """,
                [lon, lat, lon, lat, radius_m, *params_filters, SEARCH_EXPORT_BATCH_SIZE, offset],
            )
            if not rows:
                break
            yield rows
            offset += len(rows)

    return build_csv_response(columns, batch_iter(), "regnskap_nearby_results.csv")


@app.get("/api/toplist", response_model=SearchResponse)
def toplist(
    by: Literal["revenue", "profit", "operating_profit", "equity", "total_assets"] = "revenue",
    kommune: str | None = None,
    naeringskode_prefix: str | None = Query(default=None, alias="naeringskode"),
    sektor: str | None = None,
    orgform: str | None = None,
    accounting_year: int | None = None,
    min_revenue: float | None = None,
    max_revenue: float | None = None,
    limit: int = DEFAULT_LIMIT,
    offset: int = 0,
) -> SearchResponse:
    limit = clean_limit(limit)

    metric_col = {
        "revenue":          "r.revenue",
        "profit":           "r.net_profit",
        "operating_profit": "r.operating_profit",
        "equity":           "r.equity",
        "total_assets":     "r.total_assets",
    }[by]

    regnskap_join = (
        latest_regnskap_join_for_year(accounting_year)
        if accounting_year
        else LATEST_REGNSKAP_JOIN
    )

    base_sql = f"""
        FROM entity e
        {regnskap_join}
        WHERE {metric_col} IS NOT NULL
    """
    params: list[Any] = []
    base_sql = append_search_filters(
        base_sql, params,
        kommune=kommune,
        naeringskode_prefix=naeringskode_prefix, sektor=sektor,
        orgform=orgform,
        min_revenue=min_revenue, max_revenue=max_revenue,
    )

    count_row = fetch_one(f"SELECT COUNT(*)::int AS n {base_sql}", params) or {"n": 0}

    data_sql = (
        f"SELECT {_SEARCH_COLS}, NULL::double precision AS distance_m {base_sql}"
        f" ORDER BY {metric_col} DESC NULLS LAST, e.navn ASC LIMIT %s OFFSET %s"
    )
    rows = fetch_all(data_sql, [*params, limit, offset])

    return SearchResponse(
        total_count=count_row["n"],
        total_returned=len(rows),
        limit=limit,
        offset=offset,
        results=[SearchResult(**row) for row in rows],
    )
