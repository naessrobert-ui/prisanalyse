from __future__ import annotations

import os
from contextlib import asynccontextmanager
from decimal import Decimal
from typing import Any, Literal

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool


# ------------------------------------------------------------
# Configuration
# ------------------------------------------------------------
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("Missing DATABASE_URL environment variable")

ALLOWED_ORIGINS = [
    origin.strip()
    for origin in os.getenv("ALLOWED_ORIGINS", "https://prisanalyse.no,http://localhost:3000,http://localhost:5173").split(",")
    if origin.strip()
]

DEFAULT_LIMIT = int(os.getenv("DEFAULT_LIMIT", "20"))
MAX_LIMIT = int(os.getenv("MAX_LIMIT", "100"))


# ------------------------------------------------------------
# Database pool
# ------------------------------------------------------------
pool = ConnectionPool(
    conninfo=DATABASE_URL,
    min_size=1,
    max_size=10,
    kwargs={"row_factory": dict_row},
    open=False,
)


# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------
def clean_limit(limit: int) -> int:
    return max(1, min(limit, MAX_LIMIT))


def normalize_decimal(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, dict):
        return {k: normalize_decimal(v) for k, v in value.items()}
    if isinstance(value, list):
        return [normalize_decimal(v) for v in value]
    return value


def fetch_all(sql: str, params: list[Any] | tuple[Any, ...]) -> list[dict[str, Any]]:
    try:
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()
                return [normalize_decimal(dict(row)) for row in rows]
    except Exception as e:
        print("FETCH_ALL ERROR:", repr(e))
        raise HTTPException(status_code=500, detail=f"DB error: {repr(e)}")


def fetch_one(sql: str, params: list[Any] | tuple[Any, ...]) -> dict[str, Any] | None:
    try:
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                row = cur.fetchone()
                return normalize_decimal(dict(row)) if row else None
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
    total_returned: int
    limit: int
    offset: int
    results: list[SearchResult]


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


LATEST_REGNSKAP_JOIN = """
    LEFT JOIN LATERAL (
        SELECT
            rm.accounting_year,
            rm.revenue,
            rm.operating_profit,
            rm.net_profit,
            rm.total_assets,
            rm.equity,
            rm.total_liabilities,
            rm.current_assets,
            rm.short_term_liabilities,
            rm.equity_ratio,
            rm.ebit_margin,
            rm.net_margin
        FROM regnskap_metrics rm
        WHERE rm.orgnr = e.orgnr
        ORDER BY rm.accounting_year DESC NULLS LAST
        LIMIT 1
    ) r ON TRUE
"""

from contextlib import asynccontextmanager


@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Starting lifespan...")
    pool.open()
    print("Pool opened")
    try:
        yield
    finally:
        print("Closing pool...")
        pool.close()


app = FastAPI(
    title="Prisanalyse API",
    version="0.1.0",
    lifespan=lifespan,
)

import psycopg

@app.get("/debug/direct")
def debug_direct():
    try:
        with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT current_database() AS db, current_user AS usr")
                return dict(cur.fetchone())
    except Exception as e:
        return {"error": repr(e), "database_url": DATABASE_URL}

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ------------------------------------------------------------
# Endpoints
# ------------------------------------------------------------
@app.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    return HealthResponse()


@app.get("/api/search", response_model=SearchResponse)
def search(
    q: str | None = Query(default=None, description="Fritekst mot navn"),
    orgnr: str | None = None,
    orgform: str | None = None,
    kommune: str | None = None,
    naeringskode_prefix: str | None = Query(default=None, alias="naeringskode"),
    min_revenue: float | None = None,
    max_revenue: float | None = None,
    min_profit: float | None = None,
    min_equity_ratio: float | None = None,
    has_regnskap: bool = False,
    sort: Literal["revenue", "profit", "equity", "name"] = "revenue",
    limit: int = DEFAULT_LIMIT,
    offset: int = 0,
) -> SearchResponse:
    limit = clean_limit(limit)

    sql = f"""
        SELECT
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
            r.equity_ratio,
            NULL::double precision AS distance_m
        FROM entity e
        {LATEST_REGNSKAP_JOIN}
        WHERE 1=1
    """
    params: list[Any] = []

    if orgnr:
        sql += " AND e.orgnr::text = %s"
        params.append(orgnr)

    if q:
        sql += " AND e.navn ILIKE %s"
        params.append(f"%{q}%")

    if orgform:
        sql += " AND e.orgform = %s"
        params.append(orgform)

    if kommune:
        sql += " AND e.kommunenummer = %s"
        params.append(kommune)

    if naeringskode_prefix:
        sql += " AND e.naeringskode LIKE %s"
        params.append(f"{naeringskode_prefix}%")

    if min_revenue is not None:
        sql += " AND r.revenue >= %s"
        params.append(min_revenue)

    if max_revenue is not None:
        sql += " AND r.revenue <= %s"
        params.append(max_revenue)

    if min_profit is not None:
        sql += " AND r.net_profit >= %s"
        params.append(min_profit)

    if min_equity_ratio is not None:
        sql += " AND r.equity_ratio >= %s"
        params.append(min_equity_ratio)

    if has_regnskap:
        sql += " AND r.accounting_year IS NOT NULL"

    order_by = {
        "revenue": "r.revenue DESC NULLS LAST, e.navn ASC",
        "profit": "r.net_profit DESC NULLS LAST, e.navn ASC",
        "equity": "r.equity DESC NULLS LAST, e.navn ASC",
        "name": "e.navn ASC",
    }[sort]

    sql += f" ORDER BY {order_by} LIMIT %s OFFSET %s"
    params.extend([limit, offset])

    rows = fetch_all(sql, params)
    return SearchResponse(
        total_returned=len(rows),
        limit=limit,
        offset=offset,
        results=[SearchResult(**row) for row in rows],
    )

@app.get("/api/company/{orgnr}", response_model=CompanyDetail)
def company_detail(orgnr: str) -> CompanyDetail:
    sql = f"""
        SELECT
            e.orgnr,
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
        WHERE e.orgnr = %s
        LIMIT 1
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
    min_revenue: float | None = None,
    has_regnskap: bool = False,
    limit: int = DEFAULT_LIMIT,
    offset: int = 0,
) -> SearchResponse:
    limit = clean_limit(limit)
    radius_m = radius_km * 1000

    sql = f"""
        SELECT
            e.orgnr,
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
            r.equity_ratio,
            ST_Distance(
                e.geog,
                ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography
            ) AS distance_m
        FROM entity e
        {LATEST_REGNSKAP_JOIN}
        WHERE e.geog IS NOT NULL
          AND ST_DWithin(
                e.geog,
                ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography,
                %s
          )
    """
    params: list[Any] = [lon, lat, lon, lat, radius_m]

    if q:
        sql += " AND e.navn ILIKE %s"
        params.append(f"%{q}%")

    if min_revenue is not None:
        sql += " AND r.revenue >= %s"
        params.append(min_revenue)

    if has_regnskap:
        sql += " AND r.accounting_year IS NOT NULL"

    sql += " ORDER BY distance_m ASC LIMIT %s OFFSET %s"
    params.extend([limit, offset])

    rows = fetch_all(sql, params)
    return SearchResponse(
        total_returned=len(rows),
        limit=limit,
        offset=offset,
        results=[SearchResult(**row) for row in rows],
    )


@app.get("/api/toplist", response_model=SearchResponse)
def toplist(
    by: Literal["revenue", "profit", "equity"] = "revenue",
    kommune: str | None = None,
    naeringskode_prefix: str | None = Query(default=None, alias="naeringskode"),
    limit: int = DEFAULT_LIMIT,
    offset: int = 0,
) -> SearchResponse:
    limit = clean_limit(limit)

    metric_col = {
        "revenue": "r.revenue",
        "profit": "r.net_profit",
        "equity": "r.equity",
    }[by]

    sql = f"""
        SELECT
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
            r.equity_ratio,
            NULL::double precision AS distance_m
        FROM entity e
        {LATEST_REGNSKAP_JOIN}
        WHERE {metric_col} IS NOT NULL
    """
    params: list[Any] = []

    if kommune:
        sql += " AND e.kommunenummer = %s"
        params.append(kommune)

    if naeringskode_prefix:
        sql += " AND e.naeringskode LIKE %s"
        params.append(f"{naeringskode_prefix}%")

    sql += f" ORDER BY {metric_col} DESC NULLS LAST, e.navn ASC LIMIT %s OFFSET %s"
    params.extend([limit, offset])

    rows = fetch_all(sql, params)
    return SearchResponse(
        total_returned=len(rows),
        limit=limit,
        offset=offset,
        results=[SearchResult(**row) for row in rows],
    )
