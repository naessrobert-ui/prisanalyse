from __future__ import annotations

import os
import re
from contextlib import contextmanager
from functools import lru_cache
from typing import Any, Generator

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - optional dev dependency
    load_dotenv = None

import psycopg
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field


# ----------------------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------------------
if load_dotenv is not None:
    load_dotenv(override=True)

DATABASE_URL = os.environ.get("DATABASE_URL", "")
API_TITLE = "Prisanalyse Regnskap API"
API_VERSION = "0.1.0"
DEFAULT_LIMIT = 50
MAX_LIMIT = 500

if not DATABASE_URL:
    raise RuntimeError("Mangler DATABASE_URL")


# ----------------------------------------------------------------------------
# App
# ----------------------------------------------------------------------------
app = FastAPI(title=API_TITLE, version=API_VERSION)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ----------------------------------------------------------------------------
# Database helpers
# ----------------------------------------------------------------------------
@contextmanager
def get_conn() -> Generator[psycopg.Connection, None, None]:
    conn = psycopg.connect(DATABASE_URL)
    try:
        yield conn
    finally:
        conn.close()


def fetch_all_dict(sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    with get_conn() as conn:
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(sql, params)
            return list(cur.fetchall())


def fetch_one_dict(sql: str, params: tuple[Any, ...] = ()) -> dict[str, Any] | None:
    with get_conn() as conn:
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
            return dict(row) if row else None


# ----------------------------------------------------------------------------
# Response models
# ----------------------------------------------------------------------------
class HealthResponse(BaseModel):
    ok: bool
    entity_count: int
    regnskap_count: int
    queue_pending: int
    queue_done: int
    queue_no_data: int
    queue_error: int


class KPIResponse(BaseModel):
    selskaper_total: int
    selskaper_med_regnskap: int
    andel_med_regnskap: float
    med_omsetning: int
    over_10m: int
    over_100m: int
    over_1mrd: int
    median_omsetning: float | None
    total_omsetning: float | None
    total_resultat: float | None


class CompanyRow(BaseModel):
    orgnr: str
    navn: str | None = None
    orgform: str | None = None
    kommune: str | None = Field(default=None, alias="kommunenummer")
    ansatte: int | None = None
    regnskapsaar: int | None = None
    omsetning: float | None = Field(default=None, alias="sum_driftsinntekter")
    driftsresultat: float | None = None
    aarsresultat: float | None = None
    sum_eiendeler: float | None = None
    sum_egenkapital: float | None = None
    netto_margin: float | None = None
    egenkapitalandel: float | None = None
    omsetning_per_ansatt: float | None = None
    resultat_per_ansatt: float | None = None


class QualityResponse(BaseModel):
    total_regnskap: int
    med_omsetning: int
    med_aarsresultat: int
    med_eiendeler: int
    med_egenkapital: int
    ugyldig_omsetning_negativ: int
    ugyldig_eiendeler_negativ: int
    mangler_orgnr_join: int
    aar_fordeling: list[dict[str, Any]]


class CompanySearchResponse(BaseModel):
    total_returned: int
    limit: int
    offset: int
    sort_by: str
    sort_dir: str
    results: list[CompanyRow]


class CompanyFilterMetaResponse(BaseModel):
    address_columns: list[str]
    industry_code_columns: list[str]
    industry_text_columns: list[str]
    max_limit: int


# ----------------------------------------------------------------------------
# SQL fragments
# ----------------------------------------------------------------------------
COMPANY_SELECT = """
select
    e.orgnr,
    e.navn,
    e.orgform,
    e.kommunenummer,
    e.ansatte,
    r.regnskapsaar,
    r.sum_driftsinntekter,
    r.driftsresultat,
    r.aarsresultat,
    r.sum_eiendeler,
    r.sum_egenkapital,
    case
        when r.sum_driftsinntekter is not null and r.sum_driftsinntekter <> 0 and r.aarsresultat is not null
        then round((r.aarsresultat / r.sum_driftsinntekter) * 100.0, 2)
        else null
    end as netto_margin,
    case
        when r.sum_eiendeler is not null and r.sum_eiendeler <> 0 and r.sum_egenkapital is not null
        then round((r.sum_egenkapital / r.sum_eiendeler) * 100.0, 2)
        else null
    end as egenkapitalandel,
    case
        when e.ansatte is not null and e.ansatte > 0 and r.sum_driftsinntekter is not null
        then round(r.sum_driftsinntekter / e.ansatte::numeric, 2)
        else null
    end as omsetning_per_ansatt,
    case
        when e.ansatte is not null and e.ansatte > 0 and r.aarsresultat is not null
        then round(r.aarsresultat / e.ansatte::numeric, 2)
        else null
    end as resultat_per_ansatt
from regnskap_siste r
join entity e on e.orgnr = r.orgnr
"""


# ----------------------------------------------------------------------------
# Query helpers
# ----------------------------------------------------------------------------


@lru_cache(maxsize=8)
def get_table_columns(table_name: str) -> set[str]:
    rows = fetch_all_dict(
        """
        select column_name
        from information_schema.columns
        where table_name = %s
          and table_schema not in ('information_schema', 'pg_catalog')
        """,
        (table_name,),
    )
    return {str(row["column_name"]) for row in rows}


def existing_columns(table_name: str, *candidates: str) -> list[str]:
    available = get_table_columns(table_name)
    return [candidate for candidate in candidates if candidate in available]


def _text_match_clause(column_name: str) -> str:
    return f"coalesce(cast(e.{column_name} as text), '') ilike %s"


def _prefix_match_clause(column_name: str) -> str:
    return f"coalesce(cast(e.{column_name} as text), '') like %s"


def available_address_columns() -> list[str]:
    return existing_columns(
        'entity',
        'beliggenhetsadresse',
        'beliggenhetsadresse_adresselinje1',
        'beliggenhetsadresse_adresselinje2',
        'beliggenhetsadresse_adresselinje3',
        'forretningsadresse',
        'forretningsadresse_adresselinje1',
        'forretningsadresse_adresselinje2',
        'forretningsadresse_adresselinje3',
        'postadresse',
        'postadresse_adresselinje1',
        'postadresse_adresselinje2',
        'postadresse_adresselinje3',
        'adresse',
        'gateadresse',
    )


def available_industry_code_columns() -> list[str]:
    return existing_columns(
        'entity',
        'naeringskode',
        'naeringskode1',
        'naeringskode2',
        'naeringskode3',
        'naeringskode_1',
        'naeringskode_2',
        'naeringskode_3',
        'naringskode',
        'naringskode1',
        'naringskode2',
        'naringskode3',
    )


def available_industry_text_columns() -> list[str]:
    return existing_columns(
        'entity',
        'naeringskode_beskrivelse',
        'naering',
        'naering_beskrivelse',
        'bransje',
        'bransjebeskrivelse',
        'nace_beskrivelse',
    )


def build_company_filter(
    *,
    q: str | None,
    kommune: str | None,
    naeringskode: str | None,
    adresse: str | None,
    min_omsetning: float | None,
    max_omsetning: float | None,
    min_resultat: float | None,
    max_resultat: float | None,
    min_egenkapitalandel: float | None,
    min_netto_margin: float | None,
    min_ansatte: int | None,
    max_ansatte: int | None,
    min_omsetning_per_ansatt: float | None,
    max_omsetning_per_ansatt: float | None,
    min_resultat_per_ansatt: float | None,
    max_resultat_per_ansatt: float | None,
    orgform: str | None,
) -> tuple[list[str], list[Any]]:
    filters: list[str] = []
    params: list[Any] = []

    if q and q.strip():
        query = q.strip()
        like_query = f"%{query}%"
        filters.append("(e.orgnr = %s or e.orgnr ilike %s or e.navn ilike %s)")
        params.extend([query, like_query, like_query])

    if kommune and kommune.strip():
        filters.append("e.kommunenummer = %s")
        params.append(kommune.strip())

    if orgform and orgform.strip():
        filters.append("e.orgform = %s")
        params.append(orgform.strip())

    if min_omsetning is not None:
        filters.append("r.sum_driftsinntekter >= %s")
        params.append(min_omsetning)

    if max_omsetning is not None:
        filters.append("r.sum_driftsinntekter <= %s")
        params.append(max_omsetning)

    if min_resultat is not None:
        filters.append("r.aarsresultat >= %s")
        params.append(min_resultat)

    if max_resultat is not None:
        filters.append("r.aarsresultat <= %s")
        params.append(max_resultat)

    if min_egenkapitalandel is not None:
        filters.append(
            "r.sum_eiendeler is not null and r.sum_eiendeler <> 0 and r.sum_egenkapital is not null and (r.sum_egenkapital / r.sum_eiendeler) * 100.0 >= %s"
        )
        params.append(min_egenkapitalandel)

    if min_netto_margin is not None:
        filters.append(
            "r.sum_driftsinntekter is not null and r.sum_driftsinntekter <> 0 and r.aarsresultat is not null and (r.aarsresultat / r.sum_driftsinntekter) * 100.0 >= %s"
        )
        params.append(min_netto_margin)

    if min_ansatte is not None:
        filters.append("e.ansatte >= %s")
        params.append(min_ansatte)

    if max_ansatte is not None:
        filters.append("e.ansatte <= %s")
        params.append(max_ansatte)

    if min_omsetning_per_ansatt is not None:
        filters.append(
            "e.ansatte is not null and e.ansatte > 0 and r.sum_driftsinntekter is not null and (r.sum_driftsinntekter / e.ansatte::numeric) >= %s"
        )
        params.append(min_omsetning_per_ansatt)

    if max_omsetning_per_ansatt is not None:
        filters.append(
            "e.ansatte is not null and e.ansatte > 0 and r.sum_driftsinntekter is not null and (r.sum_driftsinntekter / e.ansatte::numeric) <= %s"
        )
        params.append(max_omsetning_per_ansatt)

    if min_resultat_per_ansatt is not None:
        filters.append(
            "e.ansatte is not null and e.ansatte > 0 and r.aarsresultat is not null and (r.aarsresultat / e.ansatte::numeric) >= %s"
        )
        params.append(min_resultat_per_ansatt)

    if max_resultat_per_ansatt is not None:
        filters.append(
            "e.ansatte is not null and e.ansatte > 0 and r.aarsresultat is not null and (r.aarsresultat / e.ansatte::numeric) <= %s"
        )
        params.append(max_resultat_per_ansatt)

    if naeringskode and naeringskode.strip():
        value = naeringskode.strip()
        digit_search = re.sub(r"\D", "", value)
        code_columns = available_industry_code_columns()
        text_columns = available_industry_text_columns()
        sub_filters: list[str] = []

        if digit_search and code_columns:
            sub_filters.extend(_prefix_match_clause(col) for col in code_columns)
            params.extend([f"{digit_search}%"] * len(code_columns))

        if text_columns:
            sub_filters.extend(_text_match_clause(col) for col in text_columns)
            params.extend([f"%{value}%"] * len(text_columns))

        if not sub_filters:
            raise HTTPException(status_code=400, detail="Fant ingen næringskode-felter i entity-tabellen")

        filters.append("(" + " or ".join(sub_filters) + ")")

    if adresse and adresse.strip():
        address_columns = available_address_columns()
        if not address_columns:
            raise HTTPException(status_code=400, detail="Fant ingen adressefelter i entity-tabellen")
        filters.append("(" + " or ".join(_text_match_clause(col) for col in address_columns) + ")")
        params.extend([f"%{adresse.strip()}%"] * len(address_columns))

    return filters, params


def company_order_by(sort_by: str, sort_dir: str) -> str:
    direction = 'asc' if str(sort_dir).lower() == 'asc' else 'desc'
    sort_map = {
        'omsetning': 'r.sum_driftsinntekter',
        'aarsresultat': 'r.aarsresultat',
        'egenkapitalandel': 'egenkapitalandel',
        'netto_margin': 'netto_margin',
        'navn': 'e.navn',
        'ansatte': 'e.ansatte',
        'omsetning_per_ansatt': 'omsetning_per_ansatt',
        'resultat_per_ansatt': 'resultat_per_ansatt',
        'regnskapsaar': 'r.regnskapsaar',
    }
    if sort_by not in sort_map:
        raise HTTPException(status_code=400, detail='Ugyldig sortering')
    return f"{sort_map[sort_by]} {direction} nulls last, e.navn asc"


# ----------------------------------------------------------------------------
# Endpoints
# ----------------------------------------------------------------------------
@app.get("/analysis-api/health", response_model=HealthResponse)
def health() -> HealthResponse:
    row = fetch_one_dict(
        """
        select
            (select count(*) from entity) as entity_count,
            (select count(*) from regnskap_siste) as regnskap_count,
            (select count(*) from regnskap_queue where status = 'pending') as queue_pending,
            (select count(*) from regnskap_queue where status = 'done') as queue_done,
            (select count(*) from regnskap_queue where status = 'no_data') as queue_no_data,
            (select count(*) from regnskap_queue where status = 'error') as queue_error
        """
    )
    assert row is not None
    return HealthResponse(ok=True, **row)


@app.get("/analysis-api/kpis", response_model=KPIResponse)
def kpis(orgform: str | None = Query(default="AS")) -> KPIResponse:
    filters = []
    params: list[Any] = []
    if orgform:
        filters.append("e.orgform = %s")
        params.append(orgform)

    where_clause = ""
    if filters:
        where_clause = "where " + " and ".join(filters)

    row = fetch_one_dict(
        f"""
        with base as (
            select e.orgnr, r.*
            from entity e
            left join regnskap_siste r on r.orgnr = e.orgnr
            {where_clause}
        )
        select
            count(*)::int as selskaper_total,
            count(*) filter (where regnskapsaar is not null)::int as selskaper_med_regnskap,
            round(
                (count(*) filter (where regnskapsaar is not null)::numeric / nullif(count(*), 0))::numeric,
                4
            )::float as andel_med_regnskap,
            count(*) filter (where sum_driftsinntekter is not null)::int as med_omsetning,
            count(*) filter (where sum_driftsinntekter > 10000000)::int as over_10m,
            count(*) filter (where sum_driftsinntekter > 100000000)::int as over_100m,
            count(*) filter (where sum_driftsinntekter > 1000000000)::int as over_1mrd,
            percentile_cont(0.5) within group (order by sum_driftsinntekter)::float as median_omsetning,
            sum(sum_driftsinntekter)::float as total_omsetning,
            sum(aarsresultat)::float as total_resultat
        from base
        """,
        tuple(params),
    )
    assert row is not None
    return KPIResponse(**row)

@app.get("/analysis-api/quality", response_model=QualityResponse)
def quality() -> QualityResponse:
    totals = fetch_one_dict(
        """
        select
            count(*)::int as total_regnskap,
            count(*) filter (where sum_driftsinntekter is not null)::int as med_omsetning,
            count(*) filter (where aarsresultat is not null)::int as med_aarsresultat,
            count(*) filter (where sum_eiendeler is not null)::int as med_eiendeler,
            count(*) filter (where sum_egenkapital is not null)::int as med_egenkapital,
            count(*) filter (where sum_driftsinntekter < 0)::int as ugyldig_omsetning_negativ,
            count(*) filter (where sum_eiendeler < 0)::int as ugyldig_eiendeler_negativ,
            count(*) filter (
                where not exists (
                    select 1
                    from entity e
                    where e.orgnr = r.orgnr
                )
            )::int as mangler_orgnr_join
        from regnskap_siste r
        """
    )

    year_distribution = fetch_all_dict(
        """
        select r.regnskapsaar, count(*)::int as antall
        from regnskap_siste r
        group by r.regnskapsaar
        order by r.regnskapsaar desc nulls last
        """
    )

    return QualityResponse(**totals, aar_fordeling=year_distribution)

@app.get("/analysis-api/companies/top-omsetning", response_model=list[CompanyRow])
def top_omsetning(
    limit: int = Query(default=50, ge=1, le=MAX_LIMIT),
    min_omsetning: float = Query(default=0),
    orgform: str | None = Query(default="AS"),
) -> list[CompanyRow]:
    filters = ["r.sum_driftsinntekter is not null", "r.sum_driftsinntekter >= %s"]
    params: list[Any] = [min_omsetning]

    if orgform:
        filters.append("e.orgform = %s")
        params.append(orgform)

    sql = (
        COMPANY_SELECT
        + "\nwhere "
        + " and ".join(filters)
        + "\norder by r.sum_driftsinntekter desc\nlimit %s"
    )
    params.append(limit)
    rows = fetch_all_dict(sql, tuple(params))
    return [CompanyRow(**row) for row in rows]


@app.get("/analysis-api/companies/search", response_model=list[CompanyRow])
def search_companies(
    q: str = Query(..., min_length=2),
    limit: int = Query(default=50, ge=1, le=MAX_LIMIT),
) -> list[CompanyRow]:
    query = f"%{q.strip()}%"
    rows = fetch_all_dict(
        COMPANY_SELECT
        + """
        where e.orgnr = %s
           or e.navn ilike %s
           or e.orgnr ilike %s
        order by r.sum_driftsinntekter desc nulls last, e.navn asc
        limit %s
        """,
        (q.strip(), query, query, limit),
    )
    return [CompanyRow(**row) for row in rows]


@app.get("/analysis-api/companies/high-quality", response_model=list[CompanyRow])
def high_quality_companies(
    limit: int = Query(default=50, ge=1, le=MAX_LIMIT),
    min_omsetning: float = Query(default=10_000_000),
    min_margin_pct: float = Query(default=5.0),
    min_equity_ratio_pct: float = Query(default=20.0),
    orgform: str | None = Query(default="AS"),
) -> list[CompanyRow]:
    filters = [
        "r.sum_driftsinntekter >= %s",
        "r.aarsresultat is not null",
        "r.sum_eiendeler is not null",
        "r.sum_egenkapital is not null",
        "r.sum_driftsinntekter > 0",
        "r.sum_eiendeler > 0",
        "(r.aarsresultat / r.sum_driftsinntekter) * 100.0 >= %s",
        "(r.sum_egenkapital / r.sum_eiendeler) * 100.0 >= %s",
    ]
    params: list[Any] = [min_omsetning, min_margin_pct, min_equity_ratio_pct]

    if orgform:
        filters.append("e.orgform = %s")
        params.append(orgform)

    sql = (
        COMPANY_SELECT
        + "\nwhere "
        + " and ".join(filters)
        + "\norder by r.sum_driftsinntekter desc\nlimit %s"
    )
    params.append(limit)
    rows = fetch_all_dict(sql, tuple(params))
    return [CompanyRow(**row) for row in rows]


@app.get("/analysis-api/companies/by-kommune", response_model=list[dict[str, Any]])
def companies_by_kommune(
    limit: int = Query(default=100, ge=1, le=500),
    orgform: str | None = Query(default="AS"),
) -> list[dict[str, Any]]:
    filters = ["r.sum_driftsinntekter is not null"]
    params: list[Any] = []
    if orgform:
        filters.append("e.orgform = %s")
        params.append(orgform)

    sql = f"""
    select
        e.kommunenummer,
        count(*)::int as antall_selskaper,
        sum(r.sum_driftsinntekter)::float as total_omsetning,
        avg(r.sum_driftsinntekter)::float as snitt_omsetning,
        sum(r.aarsresultat)::float as total_resultat
    from regnskap_siste r
    join entity e on e.orgnr = r.orgnr
    where {' and '.join(filters)}
    group by e.kommunenummer
    order by total_omsetning desc nulls last
    limit %s
    """
    params.append(limit)
    return fetch_all_dict(sql, tuple(params))


@app.get("/analysis-api/companies/filter/meta", response_model=CompanyFilterMetaResponse)
def company_filter_meta() -> CompanyFilterMetaResponse:
    return CompanyFilterMetaResponse(
        address_columns=available_address_columns(),
        industry_code_columns=available_industry_code_columns(),
        industry_text_columns=available_industry_text_columns(),
        max_limit=MAX_LIMIT,
    )


@app.get("/analysis-api/companies/filter", response_model=CompanySearchResponse)
def filter_companies(
    q: str | None = Query(default=None),
    kommune: str | None = Query(default=None),
    naeringskode: str | None = Query(default=None),
    adresse: str | None = Query(default=None),
    min_omsetning: float | None = Query(default=None),
    max_omsetning: float | None = Query(default=None),
    min_resultat: float | None = Query(default=None),
    max_resultat: float | None = Query(default=None),
    min_egenkapitalandel: float | None = Query(default=None),
    min_netto_margin: float | None = Query(default=None),
    min_ansatte: int | None = Query(default=None, ge=0),
    max_ansatte: int | None = Query(default=None, ge=0),
    min_omsetning_per_ansatt: float | None = Query(default=None),
    max_omsetning_per_ansatt: float | None = Query(default=None),
    min_resultat_per_ansatt: float | None = Query(default=None),
    max_resultat_per_ansatt: float | None = Query(default=None),
    orgform: str | None = Query(default='AS'),
    limit: int = Query(default=50, ge=1, le=MAX_LIMIT),
    offset: int = Query(default=0, ge=0),
    sort_by: str = Query(default='omsetning'),
    sort_dir: str = Query(default='desc'),
) -> CompanySearchResponse:
    filters, params = build_company_filter(
        q=q,
        kommune=kommune,
        naeringskode=naeringskode,
        adresse=adresse,
        min_omsetning=min_omsetning,
        max_omsetning=max_omsetning,
        min_resultat=min_resultat,
        max_resultat=max_resultat,
        min_egenkapitalandel=min_egenkapitalandel,
        min_netto_margin=min_netto_margin,
        min_ansatte=min_ansatte,
        max_ansatte=max_ansatte,
        min_omsetning_per_ansatt=min_omsetning_per_ansatt,
        max_omsetning_per_ansatt=max_omsetning_per_ansatt,
        min_resultat_per_ansatt=min_resultat_per_ansatt,
        max_resultat_per_ansatt=max_resultat_per_ansatt,
        orgform=orgform,
    )

    where_clause = ''
    if filters:
        where_clause = '\nwhere ' + ' and '.join(filters)

    order_clause = company_order_by(sort_by, sort_dir)
    base_sql = COMPANY_SELECT + where_clause
    count_sql = f"select count(*)::int as total from ({base_sql}) as filtered"
    total_row = fetch_one_dict(count_sql, tuple(params))
    total = int(total_row['total']) if total_row else 0

    sql = base_sql + f"\norder by {order_clause}\nlimit %s offset %s"
    rows = fetch_all_dict(sql, tuple(params + [limit, offset]))
    return CompanySearchResponse(
        total_returned=total,
        limit=limit,
        offset=offset,
        sort_by=sort_by,
        sort_dir='asc' if str(sort_dir).lower() == 'asc' else 'desc',
        results=[CompanyRow(**row) for row in rows],
    )


@app.get("/analysis-api/company/{orgnr}")
def company_detail(orgnr: str) -> dict[str, Any]:
    row = fetch_one_dict(
        """
        select
            e.*,
            r.regnskapsaar,
            r.journalnr,
            r.sum_driftsinntekter,
            r.sum_driftskostnad,
            r.driftsresultat,
            r.aarsresultat,
            r.sum_eiendeler,
            r.sum_egenkapital,
            r.sum_gjeld,
            r.regnskapsperiode_fra,
            r.regnskapsperiode_til,
            r.raw_payload,
            case
                when r.sum_driftsinntekter is not null and r.sum_driftsinntekter <> 0 and r.aarsresultat is not null
                then round((r.aarsresultat / r.sum_driftsinntekter) * 100.0, 2)
                else null
            end as netto_margin,
            case
                when r.sum_eiendeler is not null and r.sum_eiendeler <> 0 and r.sum_egenkapital is not null
                then round((r.sum_egenkapital / r.sum_eiendeler) * 100.0, 2)
                else null
            end as egenkapitalandel
        from entity e
        left join regnskap_siste r on r.orgnr = e.orgnr
        where e.orgnr = %s
        """,
        (orgnr,),
    )
    if not row:
        raise HTTPException(status_code=404, detail="Selskap ikke funnet")
    return row


@app.get("/analysis-api/debug/sql-samples")
def debug_sql_samples() -> dict[str, str]:
    return {
        "over_100m": "select count(*) from regnskap_siste where sum_driftsinntekter > 100000000;",
        "top_20": "select orgnr, sum_driftsinntekter from regnskap_siste where sum_driftsinntekter is not null order by sum_driftsinntekter desc limit 20;",
        "coverage": "select count(*) from regnskap_siste;",
    }


# ----------------------------------------------------------------------------
# Local dev entrypoint
# ----------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "analysis_platform:app",
        host="127.0.0.1",
        port=8010,
        reload=True,
    )
