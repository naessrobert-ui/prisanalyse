from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Query

from Fastapi_Backend import MAX_LIMIT, build_search_base_sql, clean_limit, fetch_all, fetch_one


router = APIRouter()


_COMPAT_SORT_MAP = {
    "omsetning": "r.revenue",
    "aarsresultat": "r.net_profit",
    "driftsresultat": "r.operating_profit",
    "egenkapitalandel": "r.equity_ratio",
    "netto_margin": "CASE WHEN r.revenue IS NOT NULL AND r.revenue <> 0 AND r.net_profit IS NOT NULL THEN (r.net_profit / r.revenue) END",
    "ansatte": "e.ansatte",
    "navn": "e.navn",
    "regnskapsaar": "r.accounting_year",
}


def _sort_sql(sort_by: str, sort_dir: str) -> str:
    direction = "ASC" if str(sort_dir).lower() == "asc" else "DESC"
    column = _COMPAT_SORT_MAP.get(sort_by, _COMPAT_SORT_MAP["omsetning"])
    if column == "e.navn":
        return f"{column} {direction} NULLS LAST, e.orgnr ASC"
    return f"{column} {direction} NULLS LAST, e.navn ASC, e.orgnr ASC"


def get_analysis_root_payload() -> dict[str, Any]:
    return {"ok": True, "service": "analysis-api-compat"}


def get_analysis_health_payload() -> dict[str, Any]:
    entity_row = fetch_one("SELECT COUNT(*)::int AS n FROM entity", []) or {"n": 0}
    regnskap_row = fetch_one("SELECT COUNT(*)::int AS n FROM regnskap_metrics", []) or {"n": 0}
    return {
        "ok": True,
        "entity_count": int(entity_row.get("n") or 0),
        "regnskap_count": int(regnskap_row.get("n") or 0),
        "queue_pending": 0,
        "queue_done": 0,
        "queue_no_data": 0,
        "queue_error": 0,
    }


def get_companies_filter_meta_payload() -> dict[str, Any]:
    return {
        "address_columns": ["adresse"],
        "industry_code_columns": ["naeringskode"],
        "industry_text_columns": [],
        "max_limit": MAX_LIMIT,
    }


def get_companies_filter_payload(
    *,
    q: str | None = None,
    kommune: str | None = None,
    naeringskode: str | None = None,
    adresse: str | None = None,
    min_omsetning: float | None = None,
    max_omsetning: float | None = None,
    min_resultat: float | None = None,
    max_resultat: float | None = None,
    min_egenkapitalandel: float | None = None,
    min_netto_margin: float | None = None,
    min_ansatte: int | None = None,
    max_ansatte: int | None = None,
    orgform: str | None = None,
    has_regnskap: bool = False,
    limit: int = 100,
    offset: int = 0,
    sort_by: str = "omsetning",
    sort_dir: str = "desc",
) -> dict[str, Any]:
    limit = clean_limit(limit)
    base_sql, params, _regnskap_join = build_search_base_sql(
        q=q,
        orgform=orgform,
        kommune=kommune,
        naeringskode_prefix=naeringskode,
        min_revenue=min_omsetning,
        max_revenue=max_omsetning,
        min_profit=min_resultat,
        max_profit=max_resultat,
        min_equity_ratio=(min_egenkapitalandel / 100.0) if min_egenkapitalandel is not None else None,
        has_regnskap=has_regnskap,
    )

    if adresse:
        base_sql += " AND COALESCE(e.adresse, '') ILIKE %s"
        params.append(f"%{adresse}%")

    if min_netto_margin is not None:
        base_sql += " AND r.revenue IS NOT NULL AND r.revenue <> 0 AND r.net_profit IS NOT NULL AND ((r.net_profit / r.revenue) * 100.0) >= %s"
        params.append(min_netto_margin)

    if min_ansatte is not None:
        base_sql += " AND COALESCE(e.ansatte, 0) >= %s"
        params.append(min_ansatte)

    if max_ansatte is not None:
        base_sql += " AND COALESCE(e.ansatte, 0) <= %s"
        params.append(max_ansatte)

    count_row = fetch_one(f"SELECT COUNT(*)::int AS n {base_sql}", params) or {"n": 0}
    total_count = int(count_row.get("n") or 0)

    data_sql = f"""
        SELECT
            e.orgnr::text AS orgnr,
            e.navn,
            e.orgform,
            e.kommunenummer,
            e.ansatte,
            r.accounting_year AS regnskapsaar,
            r.revenue AS omsetning,
            r.operating_profit AS driftsresultat,
            r.net_profit AS aarsresultat,
            r.total_assets AS sum_eiendeler,
            r.equity AS sum_egenkapital,
            CASE
                WHEN r.revenue IS NOT NULL AND r.revenue <> 0 AND r.net_profit IS NOT NULL
                THEN ROUND((r.net_profit / r.revenue) * 100.0, 2)
                ELSE NULL
            END AS netto_margin,
            CASE
                WHEN r.equity_ratio IS NOT NULL THEN ROUND(r.equity_ratio * 100.0, 2)
                ELSE NULL
            END AS egenkapitalandel
        {base_sql}
        ORDER BY {_sort_sql(sort_by, sort_dir)}
        LIMIT %s OFFSET %s
    """
    rows = fetch_all(data_sql, [*params, limit, offset])

    return {
        "total_returned": total_count,
        "limit": limit,
        "offset": offset,
        "sort_by": sort_by,
        "sort_dir": str(sort_dir).lower(),
        "results": rows,
    }


def get_companies_top_omsetning_payload(
    *,
    limit: int = 100,
    min_omsetning: float | None = None,
    orgform: str | None = None,
    has_regnskap: bool = False,
) -> list[dict[str, Any]]:
    payload = get_companies_filter_payload(
        min_omsetning=min_omsetning,
        orgform=orgform,
        has_regnskap=has_regnskap,
        limit=limit,
        offset=0,
        sort_by="omsetning",
        sort_dir="desc",
    )
    return payload["results"]


@router.get("/analysis-api/")
def analysis_root() -> dict[str, Any]:
    return get_analysis_root_payload()


@router.get("/analysis-api/health")
def analysis_health() -> dict[str, Any]:
    return get_analysis_health_payload()


@router.get("/analysis-api/companies/filter/meta")
def companies_filter_meta() -> dict[str, Any]:
    return get_companies_filter_meta_payload()


@router.get("/analysis-api/companies/filter")
def companies_filter(
    q: str | None = None,
    kommune: str | None = None,
    naeringskode: str | None = None,
    adresse: str | None = None,
    min_omsetning: float | None = None,
    max_omsetning: float | None = None,
    min_resultat: float | None = None,
    max_resultat: float | None = None,
    min_egenkapitalandel: float | None = None,
    min_netto_margin: float | None = None,
    min_ansatte: int | None = None,
    max_ansatte: int | None = None,
    orgform: str | None = None,
    has_regnskap: bool = False,
    limit: int = Query(default=100, ge=1, le=MAX_LIMIT),
    offset: int = Query(default=0, ge=0),
    sort_by: str = "omsetning",
    sort_dir: str = "desc",
) -> dict[str, Any]:
    return get_companies_filter_payload(
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
        orgform=orgform,
        limit=limit,
        offset=offset,
        sort_by=sort_by,
        sort_dir=sort_dir,
    )


@router.get("/analysis-api/companies/top-omsetning")
def companies_top_omsetning(
    limit: int = Query(default=100, ge=1, le=MAX_LIMIT),
    min_omsetning: float | None = None,
    orgform: str | None = None,
    has_regnskap: bool = False,
) -> list[dict[str, Any]]:
    return get_companies_top_omsetning_payload(
        limit=limit,
        min_omsetning=min_omsetning,
        orgform=orgform,
        has_regnskap=has_regnskap,
    )
