from __future__ import annotations

import re
from typing import Any

from fastapi import APIRouter, Query

from Fastapi_Backend import MAX_LIMIT, build_search_base_sql, clean_limit, fetch_all, fetch_one, resolve_naeringskode_prefix


router = APIRouter()
_MUNICIPALITY_NAME_TO_NUMBER = {
    "oslo": "0301",
    "bergen": "4601",
    "trondheim": "5001",
    "stavanger": "1103",
    "sandnes": "1108",
    "kristiansand": "4204",
    "tromso": "5501",
    "tromsø": "5501",
    "bodo": "1804",
    "bodø": "1804",
    "drammen": "3301",
}


_COMPAT_SORT_MAP = {
    "omsetning": "r.revenue",
    "aarsresultat": "r.net_profit",
    "driftsresultat": "r.operating_profit",
    "egenkapitalandel": "r.equity_ratio",
    "netto_margin": "CASE WHEN r.revenue IS NOT NULL AND r.revenue <> 0 AND r.net_profit IS NOT NULL THEN (r.net_profit / r.revenue) END",
    "ansatte": "e.ansatte",
    "omsetning_per_ansatt": "CASE WHEN e.ansatte IS NOT NULL AND e.ansatte > 0 AND r.revenue IS NOT NULL THEN (r.revenue / e.ansatte) END",
    "resultat_per_ansatt": "CASE WHEN e.ansatte IS NOT NULL AND e.ansatte > 0 AND r.net_profit IS NOT NULL THEN (r.net_profit / e.ansatte) END",
    "navn": "e.navn",
    "regnskapsaar": "r.accounting_year",
    "oppdatert_dato": "r.oppdatert_dato",
}

_INDUSTRY_HINTS = [
    {"code": "45", "description": "Handel og reparasjon av motorvogner"},
    {"code": "45.11", "description": "Handel med biler og lette motorvogner"},
    {"code": "45.20", "description": "Vedlikehold og reparasjon av motorvogner"},
    {"code": "45.31", "description": "Engroshandel med deler og utstyr til motorvogner"},
    {"code": "49.41", "description": "Godstransport på vei"},
]


def _sort_sql(sort_by: str, sort_dir: str) -> str:
    direction = "ASC" if str(sort_dir).lower() == "asc" else "DESC"
    column = _COMPAT_SORT_MAP.get(sort_by, _COMPAT_SORT_MAP["omsetning"])
    if column == "e.navn":
        return f"{column} {direction} NULLS LAST, e.orgnr ASC"
    return f"{column} {direction} NULLS LAST, e.navn ASC, e.orgnr ASC"


def _normalize_orgnr(value: str | None) -> str:
    return re.sub(r"\D", "", str(value or "").strip())


def _normalize_naeringskode_query(value: str | None) -> tuple[str, str]:
    raw = str(value or "").strip()
    digits = re.sub(r"\D", "", raw)
    if not raw:
        return "", ""
    leading_code = re.match(r"^(\d[\d\.\-]*)", raw)
    if leading_code:
        raw = leading_code.group(1)
    return raw, digits


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
        "municipality_name_columns": [],
        "max_limit": MAX_LIMIT,
    }


def get_industry_suggest_payload(q: str, limit: int = 8) -> dict[str, Any]:
    query = (q or "").strip()
    if not query:
        return {"query": "", "limit": limit, "suggestions": []}

    safe_limit = max(1, min(int(limit), 25))
    q_lower = query.lower()
    digits = re.sub(r"\D", "", query)

    suggestions: list[dict[str, Any]] = []
    for hint in _INDUSTRY_HINTS:
        haystack = f'{hint["code"]} {hint["description"]}'.lower()
        if q_lower in haystack or (digits and hint["code"].replace(".", "").startswith(digits)):
            suggestions.append({**hint, "company_count": 0})

    if digits:
        rows = fetch_all(
            """
            SELECT
                TRIM(COALESCE(e.naeringskode::text, '')) AS code,
                NULL::text AS description,
                COUNT(*)::int AS company_count
            FROM entity e
            WHERE regexp_replace(COALESCE(e.naeringskode::text, ''), '\D', '', 'g') LIKE %s
            GROUP BY 1
            HAVING TRIM(COALESCE(e.naeringskode::text, '')) <> ''
            ORDER BY company_count DESC, code ASC
            LIMIT %s
            """,
            [f"{digits}%", safe_limit],
        )
    else:
        rows = fetch_all(
            """
            SELECT
                TRIM(COALESCE(e.naeringskode::text, '')) AS code,
                NULL::text AS description,
                COUNT(*)::int AS company_count
            FROM entity e
            WHERE COALESCE(e.naeringskode::text, '') ILIKE %s
            GROUP BY 1
            HAVING TRIM(COALESCE(e.naeringskode::text, '')) <> ''
            ORDER BY company_count DESC, code ASC
            LIMIT %s
            """,
            [f"%{query}%", safe_limit],
        )

    for row in rows:
        code = str(row.get("code") or "").strip()
        if not code:
            continue
        if any(item["code"] == code for item in suggestions):
            continue
        suggestions.append(
            {
                "code": code,
                "description": row.get("description"),
                "company_count": int(row.get("company_count") or 0),
            }
        )

    return {
        "query": query,
        "limit": safe_limit,
        "suggestions": suggestions[:safe_limit],
    }


def get_company_history_payload(orgnr: str) -> dict[str, Any]:
    normalized_orgnr = _normalize_orgnr(orgnr)
    if len(normalized_orgnr) != 9:
        return {"orgnr": normalized_orgnr, "company": None, "total_returned": 0, "results": []}

    entity_row = fetch_one(
        "SELECT * FROM entity WHERE orgnr::text = %s LIMIT 1",
        [normalized_orgnr],
    ) or {}

    rows = fetch_all(
        """
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
            END AS egenkapitalandel,
            CASE
                WHEN e.ansatte IS NOT NULL AND e.ansatte > 0 AND r.revenue IS NOT NULL
                THEN ROUND((r.revenue / e.ansatte), 2)
                ELSE NULL
            END AS omsetning_per_ansatt,
            CASE
                WHEN e.ansatte IS NOT NULL AND e.ansatte > 0 AND r.net_profit IS NOT NULL
                THEN ROUND((r.net_profit / e.ansatte), 2)
                ELSE NULL
            END AS resultat_per_ansatt,
            r.oppdatert_dato::text AS oppdatert_dato
        FROM entity e
        LEFT JOIN regnskap_metrics r ON r.orgnr = e.orgnr
        WHERE e.orgnr::text = %s
        ORDER BY r.accounting_year DESC NULLS LAST, r.oppdatert_dato DESC NULLS LAST
        """,
        [normalized_orgnr],
    )
    company_name = rows[0].get("navn") if rows else None
    return {
        "orgnr": normalized_orgnr,
        "company": company_name,
        "entity": entity_row,
        "total_returned": len(rows),
        "results": rows,
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
    min_omsetning_per_ansatt: float | None = None,
    max_omsetning_per_ansatt: float | None = None,
    min_resultat_per_ansatt: float | None = None,
    max_resultat_per_ansatt: float | None = None,
    orgform: str | None = None,
    has_regnskap: bool = False,
    regnskapsaar: int | None = None,
    innlevert_etter: str | None = None,
    limit: int = 100,
    offset: int = 0,
    sort_by: str = "omsetning",
    sort_dir: str = "desc",
) -> dict[str, Any]:
    limit = clean_limit(limit)
    normalized_orgnr_query = _normalize_orgnr(q)
    orgnr_query = normalized_orgnr_query if len(normalized_orgnr_query) == 9 else None
    text_query = None if orgnr_query else q
    normalized_kommune = (kommune or "").strip()
    resolved_kommune = _MUNICIPALITY_NAME_TO_NUMBER.get(normalized_kommune.lower(), normalized_kommune) if normalized_kommune else None
    naeringskode_raw, naeringskode_digits = _normalize_naeringskode_query(naeringskode)

    base_sql, params, _regnskap_join = build_search_base_sql(
        orgnr=orgnr_query,
        q=text_query,
        orgform=orgform,
        kommune=resolved_kommune,
        naeringskode_prefix=None,
        min_revenue=min_omsetning,
        max_revenue=max_omsetning,
        min_profit=min_resultat,
        max_profit=max_resultat,
        min_equity_ratio=(min_egenkapitalandel / 100.0) if min_egenkapitalandel is not None else None,
        has_regnskap=has_regnskap,
    )

    if naeringskode_raw:
        if naeringskode_digits:
            base_sql += " AND regexp_replace(COALESCE(e.naeringskode::text, ''), '\\D', '', 'g') LIKE %s"
            params.append(f"{naeringskode_digits}%")
        else:
            resolved_prefix = resolve_naeringskode_prefix(naeringskode_raw)
            base_sql += " AND COALESCE(e.naeringskode::text, '') ILIKE %s"
            params.append(f"{resolved_prefix}%")

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

    if min_omsetning_per_ansatt is not None:
        base_sql += " AND e.ansatte IS NOT NULL AND e.ansatte > 0 AND r.revenue IS NOT NULL AND (r.revenue / e.ansatte) >= %s"
        params.append(min_omsetning_per_ansatt)

    if max_omsetning_per_ansatt is not None:
        base_sql += " AND e.ansatte IS NOT NULL AND e.ansatte > 0 AND r.revenue IS NOT NULL AND (r.revenue / e.ansatte) <= %s"
        params.append(max_omsetning_per_ansatt)

    if min_resultat_per_ansatt is not None:
        base_sql += " AND e.ansatte IS NOT NULL AND e.ansatte > 0 AND r.net_profit IS NOT NULL AND (r.net_profit / e.ansatte) >= %s"
        params.append(min_resultat_per_ansatt)

    if max_resultat_per_ansatt is not None:
        base_sql += " AND e.ansatte IS NOT NULL AND e.ansatte > 0 AND r.net_profit IS NOT NULL AND (r.net_profit / e.ansatte) <= %s"
        params.append(max_resultat_per_ansatt)

    if regnskapsaar is not None:
        base_sql += " AND r.accounting_year = %s"
        params.append(regnskapsaar)

    if innlevert_etter is not None:
        base_sql += " AND r.updated_at >= %s"
        params.append(innlevert_etter)

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
            END AS egenkapitalandel,
            CASE
                WHEN e.ansatte IS NOT NULL AND e.ansatte > 0 AND r.revenue IS NOT NULL
                THEN ROUND((r.revenue / e.ansatte), 2)
                ELSE NULL
            END AS omsetning_per_ansatt,
            CASE
                WHEN e.ansatte IS NOT NULL AND e.ansatte > 0 AND r.net_profit IS NOT NULL
                THEN ROUND((r.net_profit / e.ansatte), 2)
                ELSE NULL
            END AS resultat_per_ansatt,
            r.oppdatert_dato::text AS oppdatert_dato
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


@router.get("/analysis-api/industry/suggest")
def industry_suggest(
    q: str = Query(min_length=1),
    limit: int = Query(default=8, ge=1, le=25),
) -> dict[str, Any]:
    return get_industry_suggest_payload(q=q, limit=limit)


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
    min_omsetning_per_ansatt: float | None = None,
    max_omsetning_per_ansatt: float | None = None,
    min_resultat_per_ansatt: float | None = None,
    max_resultat_per_ansatt: float | None = None,
    orgform: str | None = None,
    has_regnskap: bool = False,
    regnskapsaar: int | None = None,
    innlevert_etter: str | None = None,
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
        min_omsetning_per_ansatt=min_omsetning_per_ansatt,
        max_omsetning_per_ansatt=max_omsetning_per_ansatt,
        min_resultat_per_ansatt=min_resultat_per_ansatt,
        max_resultat_per_ansatt=max_resultat_per_ansatt,
        orgform=orgform,
        regnskapsaar=regnskapsaar,
        innlevert_etter=innlevert_etter,
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
