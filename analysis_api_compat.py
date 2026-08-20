from __future__ import annotations

import copy
import os
import re
import threading
import time
from functools import lru_cache
from typing import Any

from fastapi import APIRouter, Query

from Fastapi_Backend import MAX_LIMIT, build_search_base_sql, clean_limit, fetch_all, fetch_one, resolve_naeringskode_prefix
from fylke_registry import (
    har_innbyggertall,
    kommune_innbyggere,
    kommune_navn,
    list_fylker,
    nace_seksjon,
    nace_seksjon_intervall,
    resolve_fylke,
)


router = APIRouter()
_SECTOR_BREAKDOWN_CACHE_TTL_SECONDS = max(
    60 * 60,
    int(os.getenv("SECTOR_BREAKDOWN_CACHE_TTL_SECONDS", str(30 * 24 * 60 * 60))),
)
_SECTOR_BREAKDOWN_CACHE_STALE_SECONDS = max(
    _SECTOR_BREAKDOWN_CACHE_TTL_SECONDS,
    int(os.getenv("SECTOR_BREAKDOWN_CACHE_STALE_SECONDS", str(37 * 24 * 60 * 60))),
)
_SECTOR_BREAKDOWN_CACHE_MAX_SIZE = 256
_sector_breakdown_cache: dict[tuple[Any, ...], tuple[float, float, dict[str, Any]]] = {}
_sector_breakdown_refreshing: set[tuple[Any, ...]] = set()
_sector_breakdown_cache_lock = threading.Lock()

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

_MUNICIPALITY_NAME_COLUMNS_CANDIDATES = [
    "kommunenavn",
    "kommune",
    "kommune_navn",
    "forretningsadresse_kommune",
    "postadresse_kommune",
    "beliggenhetsadresse_kommune",
    "forretningsadresse_poststed",
    "postadresse_poststed",
    "beliggenhetsadresse_poststed",
]


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
    {"code": "69", "description": "Juridisk og regnskapsmessig tjenesteyting"},
    {"code": "69.10", "description": "Juridisk tjenesteyting"},
    {"code": "69.100", "description": "Juridisk tjenesteyting"},
    {"code": "68.200", "description": "Utleie av egen eller leid fast eiendom"},
    {"code": "47.810", "description": "Butikkhandel med motorvogner, unntatt motorsykler"},
]

_INDUSTRY_CODE_DESCRIPTIONS = {
    "45": "Handel og reparasjon av motorvogner",
    "45.11": "Handel med biler og lette motorvogner",
    "45.20": "Vedlikehold og reparasjon av motorvogner",
    "47.810": "Butikkhandel med motorvogner, unntatt motorsykler",
    "49.41": "Godstransport på vei",
    "69": "Juridisk og regnskapsmessig tjenesteyting",
    "69.10": "Juridisk tjenesteyting",
    "69.100": "Juridisk tjenesteyting",
    "68.200": "Utleie av egen eller leid fast eiendom",
}

_TWO_DIGIT_SECTOR_DESCRIPTIONS = {
    "00": "Uspesifisert næringskode",
    "01": "Jordbruk og tjenester tilknyttet jordbruk, jakt og viltstell",
    "02": "Skogbruk og tjenester tilknyttet skogbruk",
    "03": "Fiske, fangst og akvakultur",
    "05": "Bryting av steinkull og brunkull",
    "06": "Utvinning av råolje og naturgass",
    "07": "Bryting av metallholdig malm",
    "08": "Annen bergverksdrift",
    "09": "Tjenester tilknyttet bergverksdrift og utvinning",
    "10": "Produksjon av nærings- og nytelsesmidler",
    "11": "Produksjon av drikkevarer",
    "12": "Produksjon av tobakksvarer",
    "13": "Produksjon av tekstiler",
    "14": "Produksjon av klær",
    "15": "Produksjon av lær og lærvarer",
    "16": "Produksjon av trelast og varer av tre",
    "17": "Produksjon av papir og papirvarer",
    "18": "Trykking og reproduksjon av innspilte opptak",
    "19": "Produksjon av raffinerte petroleumsprodukter",
    "20": "Produksjon av kjemikalier og kjemiske produkter",
    "21": "Produksjon av farmasøytiske råvarer og preparater",
    "22": "Produksjon av gummi- og plastprodukter",
    "23": "Produksjon av andre ikke-metallholdige mineralprodukter",
    "24": "Produksjon av metaller",
    "25": "Produksjon av metallvarer",
    "26": "Produksjon av data- og elektroniske produkter",
    "27": "Produksjon av elektrisk utstyr",
    "28": "Produksjon av maskiner og utstyr",
    "29": "Produksjon av motorvogner og tilhengere",
    "30": "Produksjon av andre transportmidler",
    "31": "Produksjon av møbler",
    "32": "Annen industriproduksjon",
    "33": "Reparasjon og installasjon av maskiner og utstyr",
    "35": "Elektrisitets-, gass- og varmtvannsforsyning",
    "36": "Uttak fra kilde, rensing og distribusjon av vann",
    "37": "Oppsamling og behandling av avløpsvann",
    "38": "Innsamling, behandling, disponering og gjenvinning av avfall",
    "39": "Miljørydding, miljørensing og lignende virksomhet",
    "41": "Oppføring av bygninger",
    "42": "Anleggsvirksomhet",
    "43": "Spesialisert bygge- og anleggsvirksomhet",
    "45": "Handel og reparasjon av motorvogner",
    "46": "Agentur- og engroshandel, unntatt med motorvogner",
    "47": "Detaljhandel, unntatt med motorvogner",
    "49": "Landtransport og transport via rørledninger",
    "50": "Sjøfart",
    "51": "Lufttransport",
    "52": "Lagring og andre tjenester tilknyttet transport",
    "53": "Post og distribusjonsvirksomhet",
    "55": "Overnattingsvirksomhet",
    "56": "Serveringsvirksomhet",
    "58": "Forlagsvirksomhet",
    "59": "Film-, video- og musikkproduksjon",
    "60": "Programskapings- og kringkastingsvirksomhet",
    "61": "Telekommunikasjon",
    "62": "Tjenester tilknyttet informasjonsteknologi",
    "63": "Informasjonstjenester",
    "64": "Finansieringsvirksomhet",
    "65": "Forsikring og pensjonskasser",
    "66": "Tjenester tilknyttet finansierings- og forsikringsvirksomhet",
    "68": "Omsetning og drift av fast eiendom",
    "69": "Juridisk og regnskapsmessig tjenesteyting",
    "70": "Hovedkontortjenester og administrativ rådgivning",
    "71": "Arkitektvirksomhet og teknisk konsulentvirksomhet",
    "72": "Forskning og utviklingsarbeid",
    "73": "Reklame og markedsundersøkelser",
    "74": "Annen faglig, vitenskapelig og teknisk virksomhet",
    "75": "Veterinærtjenester",
    "77": "Utleie- og leasingvirksomhet",
    "78": "Arbeidskrafttjenester",
    "79": "Reisebyrå- og reisearrangørvirksomhet",
    "80": "Vakttjeneste og etterforskning",
    "81": "Tjenester tilknyttet eiendomsdrift",
    "82": "Annen forretningsmessig tjenesteyting",
    "84": "Offentlig administrasjon og forsvar, og trygdeordninger",
    "85": "Undervisning",
    "86": "Helsetjenester",
    "87": "Pleie- og omsorgstjenester i institusjon",
    "88": "Sosiale omsorgstjenester uten botilbud",
    "90": "Kunstnerisk virksomhet og underholdningsvirksomhet",
    "91": "Bibliotek, arkiv, museum og annen kulturvirksomhet",
    "92": "Lotteri og totalisatorspill",
    "93": "Sports- og fritidsaktiviteter",
    "94": "Aktiviteter i medlemsorganisasjoner",
    "95": "Reparasjon av datamaskiner, husholdningsvarer og varer til personlig bruk",
    "96": "Annen personlig tjenesteyting",
    "97": "Lønnet arbeid i private husholdninger",
    "98": "Private husholdninger som produserer varer/tjenester til eget bruk",
    "99": "Internasjonale organisasjoner og organer",
}

_INDUSTRY_SYNONYMS = {
    "sport": ["idrett", "trening", "fritid", "aktivitet"],
    "idrett": ["sport", "trening", "fritid"],
    "eiendom": ["utleie", "bygg", "bolig", "næringseiendom"],
    "bil": ["motor", "verksted", "kjøretøy", "transport"],
    "transport": ["logistikk", "gods", "frakt", "sjøfart"],
    "advokat": ["juridisk", "jus", "law", "legal", "advokater"],
    "advokater": ["juridisk", "jus", "law", "legal", "advokat"],
    "juridisk": ["advokat", "advokater", "jus", "legal"],
    "jus": ["juridisk", "advokat", "advokater", "legal"],
}


@lru_cache(maxsize=1)
def _entity_columns() -> set[str]:
    rows = fetch_all(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'entity'
        """,
        [],
    )
    return {str(row.get("column_name")) for row in rows}


@lru_cache(maxsize=1)
def _municipality_name_columns() -> list[str]:
    columns = _entity_columns()
    return [col for col in _MUNICIPALITY_NAME_COLUMNS_CANDIDATES if col in columns]


def _normalize_municipality_name(value: str | None) -> str:
    return re.sub(r"[^0-9a-zæøå]+", "", str(value or "").strip().lower())


def _append_kommune_filter(base_sql: str, params: list[Any], kommune: str | None) -> tuple[str, list[Any]]:
    normalized_kommune = (kommune or "").strip()
    if not normalized_kommune:
        return base_sql, params

    municipality_number = _MUNICIPALITY_NAME_TO_NUMBER.get(_normalize_municipality_name(normalized_kommune))
    if re.fullmatch(r"\d+", normalized_kommune):
        municipality_number = normalized_kommune

    sub_filters: list[str] = []
    if municipality_number:
        sub_filters.append("e.kommunenummer = %s")
        params.append(municipality_number)

    name_columns = _municipality_name_columns()
    if name_columns:
        like_value = f"%{normalized_kommune}%"
        sub_filters.extend(f"COALESCE(e.{column_name}::text, '') ILIKE %s" for column_name in name_columns)
        params.extend([like_value] * len(name_columns))

    # Robust fallback: many datasets only have gate/adresse fields populated with poststed,
    # while kommunenavn columns may be absent or incomplete.
    if not re.fullmatch(r"\d+", normalized_kommune):
        sub_filters.append("COALESCE(e.adresse::text, '') ILIKE %s")
        params.append(f"%{normalized_kommune}%")

    if sub_filters:
        base_sql += " AND (" + " OR ".join(sub_filters) + ")"
    else:
        # fallback: preserve previous behaviour where kommune matched kommunenummer directly
        base_sql += " AND e.kommunenummer = %s"
        params.append(normalized_kommune)
    return base_sql, params


def _industry_description_expr(alias: str = "e") -> str:
    columns = _entity_columns()
    candidates = [
        "naeringskode_beskrivelse",
        "naering_beskrivelse",
        "naering",
        "bransje",
        "bransjebeskrivelse",
        "nace_beskrivelse",
    ]
    existing = [col for col in candidates if col in columns]
    if not existing:
        return "NULL::text"
    return "COALESCE(" + ", ".join(f"NULLIF(TRIM({alias}.{col}::text), '')" for col in existing) + ")"


def _industry_description_columns() -> list[str]:
    columns = _entity_columns()
    candidates = [
        "naeringskode_beskrivelse",
        "naering_beskrivelse",
        "naering",
        "bransje",
        "bransjebeskrivelse",
        "nace_beskrivelse",
    ]
    return [col for col in candidates if col in columns]


def _expand_industry_terms(query: str) -> list[str]:
    base_terms = [term for term in re.split(r"\s+", str(query or "").strip().lower()) if term]
    expanded: list[str] = []
    for term in base_terms:
        expanded.append(term)
        expanded.extend(_INDUSTRY_SYNONYMS.get(term, []))
    # unique, keep order
    unique: list[str] = []
    for term in expanded:
        if term not in unique:
            unique.append(term)
    return unique[:10]


def _taxonomy_suggestions(terms: list[str], limit: int) -> list[dict[str, Any]]:
    if not terms:
        return []
    catalog = {**_TWO_DIGIT_SECTOR_DESCRIPTIONS, **_INDUSTRY_CODE_DESCRIPTIONS}
    rows: list[dict[str, Any]] = []
    for code, description in catalog.items():
        haystack = f"{code} {description}".lower()
        if any(term in haystack for term in terms):
            rows.append({"code": code, "description": description, "company_count": 0})
    rows.sort(key=lambda item: (len(str(item["code"])), str(item["code"])))
    return rows[:limit]


def _fallback_description_for_code(code: str, hints: list[dict[str, Any]]) -> str | None:
    normalized_code = str(code or "").strip()
    if not normalized_code:
        return None
    if normalized_code in _INDUSTRY_CODE_DESCRIPTIONS:
        return _INDUSTRY_CODE_DESCRIPTIONS[normalized_code]
    for known_code, known_desc in sorted(_INDUSTRY_CODE_DESCRIPTIONS.items(), key=lambda kv: len(kv[0]), reverse=True):
        if normalized_code.startswith(known_code):
            return known_desc
    two_digit = re.sub(r"\D", "", normalized_code)[:2]
    if two_digit in _TWO_DIGIT_SECTOR_DESCRIPTIONS:
        return _TWO_DIGIT_SECTOR_DESCRIPTIONS[two_digit]
    for hint in hints:
        if hint["code"] == normalized_code:
            return str(hint.get("description") or "").strip() or None
    for hint in hints:
        if normalized_code.startswith(str(hint["code"])):
            return str(hint.get("description") or "").strip() or None
    return None


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
    regnskap_row = fetch_one("SELECT COUNT(*)::int AS n FROM regnskap_siste", []) or {"n": 0}
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
        "municipality_name_columns": _municipality_name_columns(),
        "max_limit": MAX_LIMIT,
    }


def get_industry_suggest_payload(q: str, limit: int = 8) -> dict[str, Any]:
    query = (q or "").strip()
    if not query:
        return {"query": "", "limit": limit, "suggestions": []}

    safe_limit = max(1, min(int(limit), 25))
    digits = re.sub(r"\D", "", query)

    if digits:
        # Søk på kode-prefix fra naeringskode_stats
        rows = fetch_all(
            """
            SELECT
                naeringskode AS code,
                antall_selskaper AS company_count,
                antall_med_regnskap
            FROM naeringskode_stats
            WHERE regexp_replace(naeringskode, '\\D', '', 'g') LIKE %s
            ORDER BY antall_selskaper DESC, naeringskode ASC
            LIMIT %s
            """,
            [f"{digits}%", safe_limit],
        )
    else:
        # Tekstsøk — bruk _expand_industry_terms og hints
        terms = _expand_industry_terms(query) or [query.lower()]
        hint_matches = []
        for hint in _INDUSTRY_HINTS:
            haystack = f'{hint["code"]} {hint["description"]}'.lower()
            if any(t in haystack for t in terms):
                hint_matches.append(hint)

        # Hent stats for matchende koder
        if hint_matches:
            codes = [h["code"] for h in hint_matches[:safe_limit]]
            placeholders = ",".join(["%s"] * len(codes))
            rows = fetch_all(
                f"""
                SELECT naeringskode AS code, antall_selskaper AS company_count, antall_med_regnskap
                FROM naeringskode_stats
                WHERE naeringskode IN ({placeholders})
                ORDER BY antall_selskaper DESC
                LIMIT %s
                """,
                [*codes, safe_limit],
            )
        else:
            rows = []

    hints_by_code = {h["code"]: h for h in _INDUSTRY_HINTS}
    suggestions = [
        {
            "code": str(row.get("code") or "").strip(),
            "description": hints_by_code.get(str(row.get("code") or "").strip(), {}).get("description")
                           or _fallback_description_for_code(str(row.get("code") or "").strip(), _INDUSTRY_HINTS),
            "company_count": int(row.get("company_count") or 0),
            "antall_med_regnskap": int(row.get("antall_med_regnskap") or 0),
        }
        for row in rows
        if str(row.get("code") or "").strip()
    ]

    if not suggestions:
        suggestions = _taxonomy_suggestions(_expand_industry_terms(query), safe_limit) or []

    return {
        "query": query,
        "limit": safe_limit,
        "suggestions": suggestions[:safe_limit],
    }


def get_industry_overview_payload(limit: int = 25) -> dict[str, Any]:
    safe_limit = max(1, min(int(limit), 100))
    rows = fetch_all(
        """
        SELECT
            prefix AS code,
            SUM(antall_selskaper)::int AS company_count,
            SUM(antall_med_regnskap)::int AS antall_med_regnskap
        FROM naeringskode_stats
        WHERE prefix IS NOT NULL AND prefix != ''
        GROUP BY prefix
        ORDER BY company_count DESC, prefix ASC
        LIMIT %s
        """,
        [safe_limit],
    )
    suggestions = [
        {
            "code": str(row.get("code") or ""),
            "description": _fallback_description_for_code(str(row.get("code") or ""), _INDUSTRY_HINTS),
            "company_count": int(row.get("company_count") or 0),
            "antall_med_regnskap": int(row.get("antall_med_regnskap") or 0),
        }
        for row in rows
    ]
    return {"limit": safe_limit, "groups": suggestions}


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
            r.regnskapsaar AS regnskapsaar,
            r.sum_driftsinntekter AS omsetning,
            r.driftsresultat AS driftsresultat,
            r.aarsresultat AS aarsresultat,
            r.sum_eiendeler AS sum_eiendeler,
            r.sum_egenkapital AS sum_egenkapital,
            CASE
                WHEN r.sum_driftsinntekter IS NOT NULL AND r.sum_driftsinntekter <> 0 AND r.aarsresultat IS NOT NULL
                THEN ROUND((r.aarsresultat / r.sum_driftsinntekter) * 100.0, 2)
                ELSE NULL
            END AS netto_margin,
            CASE
                WHEN r.sum_eiendeler > 0 AND r.sum_egenkapital IS NOT NULL
                THEN ROUND((r.sum_egenkapital / r.sum_eiendeler) * 100.0, 2)
                ELSE NULL
            END AS egenkapitalandel,
            CASE
                WHEN e.ansatte IS NOT NULL AND e.ansatte > 0 AND r.sum_driftsinntekter IS NOT NULL
                THEN ROUND((r.sum_driftsinntekter / e.ansatte), 2)
                ELSE NULL
            END AS omsetning_per_ansatt,
            CASE
                WHEN e.ansatte IS NOT NULL AND e.ansatte > 0 AND r.aarsresultat IS NOT NULL
                THEN ROUND((r.aarsresultat / e.ansatte), 2)
                ELSE NULL
            END AS resultat_per_ansatt,
            r.fetched_at::date::text AS oppdatert_dato
        FROM entity e
        LEFT JOIN regnskap_siste r ON r.orgnr = e.orgnr
        WHERE e.orgnr::text = %s
        ORDER BY r.regnskapsaar DESC NULLS LAST
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
    naeringskode_raw, naeringskode_digits = _normalize_naeringskode_query(naeringskode)

    base_sql, params, _regnskap_join = build_search_base_sql(
        orgnr=orgnr_query,
        q=text_query,
        orgform=orgform,
        kommune=None,
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
    base_sql, params = _append_kommune_filter(base_sql, params, kommune)

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


def get_companies_filter_summary_payload(
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
    top_n: int = 5,
    scatter_limit: int = 800,
) -> dict[str, Any]:
    normalized_orgnr_query = _normalize_orgnr(q)
    orgnr_query = normalized_orgnr_query if len(normalized_orgnr_query) == 9 else None
    text_query = None if orgnr_query else q
    naeringskode_raw, naeringskode_digits = _normalize_naeringskode_query(naeringskode)

    base_sql, params, _regnskap_join = build_search_base_sql(
        orgnr=orgnr_query,
        q=text_query,
        orgform=orgform,
        kommune=None,
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
    base_sql, params = _append_kommune_filter(base_sql, params, kommune)

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

    safe_top_n = max(1, min(int(top_n), 20))
    safe_scatter_limit = max(100, min(int(scatter_limit), 2000))

    summary_row = fetch_one(
        f"""
        SELECT
            COUNT(*)::int AS total_companies,
            COUNT(r.net_profit)::int AS companies_with_result,
            COUNT(*) FILTER (WHERE r.net_profit > 0)::int AS profitable_count,
            COUNT(*) FILTER (
                WHERE r.net_profit IS NOT NULL
                  AND r.revenue IS NOT NULL
                  AND r.revenue <> 0
                  AND ((r.net_profit / r.revenue) * 100.0) >= 5
                  AND r.equity_ratio IS NOT NULL
                  AND (r.equity_ratio * 100.0) >= 25
            )::int AS robust_count,
            SUM(r.revenue) AS sum_omsetning,
            SUM(r.net_profit) AS sum_aarsresultat,
            AVG(
                CASE
                    WHEN r.net_profit IS NOT NULL AND r.revenue IS NOT NULL AND r.revenue <> 0
                    THEN (r.net_profit / r.revenue) * 100.0
                    ELSE NULL
                END
            ) AS avg_netto_margin,
            AVG(
                CASE
                    WHEN r.equity_ratio IS NOT NULL
                    THEN r.equity_ratio * 100.0
                    ELSE NULL
                END
            ) AS avg_egenkapitalandel
        {base_sql}
        """,
        params,
    ) or {}

    top_rows = fetch_all(
        f"""
        SELECT e.orgnr::text AS orgnr, e.navn, r.net_profit AS aarsresultat
        {base_sql}
        ORDER BY r.net_profit DESC NULLS LAST, e.navn ASC
        LIMIT %s
        """,
        [*params, safe_top_n],
    )
    bottom_rows = fetch_all(
        f"""
        SELECT e.orgnr::text AS orgnr, e.navn, r.net_profit AS aarsresultat
        {base_sql}
        ORDER BY r.net_profit ASC NULLS LAST, e.navn ASC
        LIMIT %s
        """,
        [*params, safe_top_n],
    )
    margin_rows = fetch_all(
        f"""
        SELECT
            e.orgnr::text AS orgnr,
            e.navn,
            ROUND((r.net_profit / r.revenue) * 100.0, 2) AS netto_margin
        {base_sql}
        AND r.net_profit IS NOT NULL
        AND r.revenue IS NOT NULL
        AND r.revenue <> 0
        ORDER BY (r.net_profit / r.revenue) DESC NULLS LAST, e.navn ASC
        LIMIT %s
        """,
        [*params, safe_top_n],
    )
    scatter_rows = fetch_all(
        f"""
        SELECT
            e.orgnr::text AS orgnr,
            e.navn,
            r.revenue AS omsetning,
            r.net_profit AS aarsresultat
        {base_sql}
        AND r.revenue IS NOT NULL
        AND r.net_profit IS NOT NULL
        ORDER BY ABS(r.net_profit) DESC NULLS LAST, r.revenue DESC NULLS LAST
        LIMIT %s
        """,
        [*params, safe_scatter_limit],
    )

    total_companies = int(summary_row.get("total_companies") or 0)
    companies_with_result = int(summary_row.get("companies_with_result") or 0)
    profitable_count = int(summary_row.get("profitable_count") or 0)
    robust_count = int(summary_row.get("robust_count") or 0)

    return {
        "summary_scope": "full_selection",
        "total_companies": total_companies,
        "companies_with_result": companies_with_result,
        "profitable_count": profitable_count,
        "robust_count": robust_count,
        "sum_omsetning": summary_row.get("sum_omsetning"),
        "sum_aarsresultat": summary_row.get("sum_aarsresultat"),
        "avg_netto_margin": summary_row.get("avg_netto_margin"),
        "avg_egenkapitalandel": summary_row.get("avg_egenkapitalandel"),
        "share_profitable_pct": ((profitable_count / companies_with_result) * 100.0) if companies_with_result else None,
        "share_robust_pct": ((robust_count / total_companies) * 100.0) if total_companies else None,
        "top_result": top_rows,
        "bottom_result": bottom_rows,
        "top_margin": margin_rows,
        "scatter_points": scatter_rows,
    }


def _coalesce_municipality_name_expr(alias: str = "e") -> str:
    name_columns = _municipality_name_columns()
    if not name_columns:
        return "NULL::text"
    return "COALESCE(" + ", ".join(f"NULLIF(TRIM({alias}.{col}::text), '')" for col in name_columns) + ")"


def _build_sector_breakdown_cache_key(
    *,
    q: str | None,
    orgnr_query: str | None,
    kommune: str | None,
    naeringskode_raw: str | None,
    naeringskode_digits: str,
    min_omsetning: float | None,
    max_omsetning: float | None,
    min_resultat: float | None,
    max_resultat: float | None,
    orgform: str | None,
    regnskapsaar: int | None,
    parent_digits: str,
    level: int,
    limit: int,
    kommune_limit: int,
) -> tuple[Any, ...]:
    return (
        (q or "").strip().lower(),
        orgnr_query,
        (kommune or "").strip().lower(),
        naeringskode_raw,
        naeringskode_digits,
        min_omsetning,
        max_omsetning,
        min_resultat,
        max_resultat,
        (orgform or "").strip().upper(),
        regnskapsaar,
        parent_digits,
        max(2, min(int(level), 5)),
        max(1, min(int(limit), 200)),
        max(1, min(int(kommune_limit), 200)),
    )


def _store_sector_breakdown_cache_entry(cache_key: tuple[Any, ...], payload: dict[str, Any]) -> None:
    with _sector_breakdown_cache_lock:
        now = time.time()
        _sector_breakdown_cache[cache_key] = (
            now + _SECTOR_BREAKDOWN_CACHE_TTL_SECONDS,
            now + _SECTOR_BREAKDOWN_CACHE_STALE_SECONDS,
            payload,
        )
        if len(_sector_breakdown_cache) > _SECTOR_BREAKDOWN_CACHE_MAX_SIZE:
            oldest_key = min(_sector_breakdown_cache, key=lambda key: _sector_breakdown_cache[key][0])
            _sector_breakdown_cache.pop(oldest_key, None)


def _compute_sector_breakdown_payload(
    *,
    q: str | None = None,
    kommune: str | None = None,
    naeringskode: str | None = None,
    min_omsetning: float | None = None,
    max_omsetning: float | None = None,
    min_resultat: float | None = None,
    max_resultat: float | None = None,
    orgform: str | None = None,
    regnskapsaar: int | None = None,
    parent_code: str | None = None,
    level: int = 2,
    limit: int = 50,
    kommune_limit: int = 50,
) -> dict[str, Any]:
    normalized_orgnr_query = _normalize_orgnr(q)
    orgnr_query = normalized_orgnr_query if len(normalized_orgnr_query) == 9 else None
    text_query = None if orgnr_query else q
    naeringskode_raw, naeringskode_digits = _normalize_naeringskode_query(naeringskode)
    parent_digits = re.sub(r"\D", "", str(parent_code or ""))
    cache_key = (
        (text_query or "").strip().lower(),
        orgnr_query,
        (kommune or "").strip().lower(),
        naeringskode_raw,
        naeringskode_digits,
        min_omsetning,
        max_omsetning,
        min_resultat,
        max_resultat,
        (orgform or "").strip().upper(),
        regnskapsaar,
        parent_digits,
        max(2, min(int(level), 5)),
        max(1, min(int(limit), 200)),
        max(1, min(int(kommune_limit), 200)),
    )

    now = time.time()
    with _sector_breakdown_cache_lock:
        cached_entry = _sector_breakdown_cache.get(cache_key)
        if cached_entry and cached_entry[0] > now:
            return copy.deepcopy(cached_entry[1])

    base_sql, params, _regnskap_join = build_search_base_sql(
        orgnr=orgnr_query,
        q=text_query,
        orgform=orgform,
        kommune=None,
        naeringskode_prefix=None,
        min_revenue=min_omsetning,
        max_revenue=max_omsetning,
        min_profit=min_resultat,
        max_profit=max_resultat,
        min_equity_ratio=None,
        has_regnskap=False,
    )

    if naeringskode_raw:
        if naeringskode_digits:
            base_sql += " AND regexp_replace(COALESCE(e.naeringskode::text, ''), '\\D', '', 'g') LIKE %s"
            params.append(f"{naeringskode_digits}%")
        else:
            resolved_prefix = resolve_naeringskode_prefix(naeringskode_raw)
            base_sql += " AND COALESCE(e.naeringskode::text, '') ILIKE %s"
            params.append(f"{resolved_prefix}%")

    if parent_digits:
        base_sql += " AND regexp_replace(COALESCE(e.naeringskode::text, ''), '\\D', '', 'g') LIKE %s"
        params.append(f"{parent_digits}%")

    if regnskapsaar is not None:
        base_sql += " AND r.accounting_year = %s"
        params.append(regnskapsaar)

    base_sql, params = _append_kommune_filter(base_sql, params, kommune)

    safe_level = max(2, min(int(level), 5))
    safe_limit = max(1, min(int(limit), 200))
    safe_kommune_limit = max(1, min(int(kommune_limit), 200))

    code_expr = (
        "CASE "
        "WHEN COALESCE(regexp_replace(COALESCE(e.naeringskode::text, ''), '\\D', '', 'g'), '') = '' "
        "THEN '00' "
        f"ELSE LPAD(SUBSTRING(regexp_replace(COALESCE(e.naeringskode::text, ''), '\\D', '', 'g') FROM 1 FOR {safe_level}), {safe_level}, '0') "
        "END"
    )

    sector_rows = fetch_all(
        f"""
        SELECT
            {code_expr} AS code,
            COUNT(*)::int AS company_count,
            COUNT(*) FILTER (WHERE r.accounting_year IS NOT NULL)::int AS company_count_with_regnskap,
            COUNT(*) FILTER (WHERE r.net_profit > 0)::int AS profitable_count,
            SUM(r.revenue) AS sum_omsetning,
            SUM(r.net_profit) AS sum_aarsresultat,
            CASE
                WHEN SUM(r.revenue) IS NOT NULL AND SUM(r.revenue) <> 0
                THEN (SUM(r.net_profit) / SUM(r.revenue)) * 100.0
                ELSE NULL
            END AS avg_margin,
            CASE
                WHEN COUNT(*) FILTER (WHERE r.accounting_year IS NOT NULL) > 0
                THEN (
                    COUNT(*) FILTER (WHERE r.net_profit > 0)::numeric
                    / COUNT(*) FILTER (WHERE r.accounting_year IS NOT NULL)::numeric
                ) * 100.0
                ELSE NULL
            END AS profitable_share_pct,
            CASE
                WHEN SUM(CASE WHEN e.ansatte IS NOT NULL AND e.ansatte > 0 AND r.revenue IS NOT NULL THEN e.ansatte ELSE 0 END) > 0
                THEN (
                    SUM(r.revenue)
                    / SUM(CASE WHEN e.ansatte IS NOT NULL AND e.ansatte > 0 AND r.revenue IS NOT NULL THEN e.ansatte ELSE 0 END)
                )
                ELSE NULL
            END AS omsetning_per_ansatt,
            CASE
                WHEN SUM(CASE WHEN e.ansatte IS NOT NULL AND e.ansatte > 0 AND r.net_profit IS NOT NULL THEN e.ansatte ELSE 0 END) > 0
                THEN (
                    SUM(r.net_profit)
                    / SUM(CASE WHEN e.ansatte IS NOT NULL AND e.ansatte > 0 AND r.net_profit IS NOT NULL THEN e.ansatte ELSE 0 END)
                )
                ELSE NULL
            END AS resultat_per_ansatt
        {base_sql}
        GROUP BY 1
        ORDER BY sum_omsetning DESC NULLS LAST, code ASC
        LIMIT %s
        """,
        [*params, safe_limit],
    )

    municipality_name_expr = _coalesce_municipality_name_expr("e")
    municipality_rows = fetch_all(
        f"""
        SELECT
            COALESCE(NULLIF(TRIM(e.kommunenummer::text), ''), 'Ukjent') AS kommunenummer,
            COALESCE({municipality_name_expr}, 'Ukjent') AS kommunenavn,
            COUNT(*)::int AS company_count,
            COUNT(*) FILTER (WHERE r.accounting_year IS NOT NULL)::int AS company_count_with_regnskap,
            SUM(r.revenue) AS sum_omsetning,
            SUM(r.net_profit) AS sum_aarsresultat
        {base_sql}
        GROUP BY 1, 2
        ORDER BY sum_omsetning DESC NULLS LAST, company_count DESC, kommunenummer ASC
        LIMIT %s
        """,
        [*params, safe_kommune_limit],
    )

    sectors = []
    for row in sector_rows:
        code = str(row.get("code") or "")
        sectors.append(
            {
                "code": code,
                "description": _fallback_description_for_code(code, _INDUSTRY_HINTS),
                "company_count": int(row.get("company_count") or 0),
                "company_count_with_regnskap": int(row.get("company_count_with_regnskap") or 0),
                "profitable_count": int(row.get("profitable_count") or 0),
                "sum_omsetning": row.get("sum_omsetning"),
                "sum_aarsresultat": row.get("sum_aarsresultat"),
                "avg_margin": row.get("avg_margin"),
                "profitable_share_pct": row.get("profitable_share_pct"),
                "omsetning_per_ansatt": row.get("omsetning_per_ansatt"),
                "resultat_per_ansatt": row.get("resultat_per_ansatt"),
            }
        )

    payload = {
        "level": safe_level,
        "parent_code": parent_digits or None,
        "filters": {
            "q": q,
            "kommune": kommune,
            "naeringskode": naeringskode,
            "regnskapsaar": regnskapsaar,
        },
        "sectors": sectors,
        "municipalities": municipality_rows,
    }
    return payload


def _refresh_sector_breakdown_cache_async(cache_key: tuple[Any, ...], kwargs: dict[str, Any]) -> None:
    def _worker() -> None:
        try:
            payload = _compute_sector_breakdown_payload(**kwargs)
            _store_sector_breakdown_cache_entry(cache_key, payload)
        finally:
            with _sector_breakdown_cache_lock:
                _sector_breakdown_refreshing.discard(cache_key)

    with _sector_breakdown_cache_lock:
        if cache_key in _sector_breakdown_refreshing:
            return
        _sector_breakdown_refreshing.add(cache_key)

    thread = threading.Thread(target=_worker, daemon=True, name="sector-breakdown-cache-refresh")
    thread.start()


def get_sector_breakdown_payload(
    *,
    q: str | None = None,
    kommune: str | None = None,
    naeringskode: str | None = None,
    min_omsetning: float | None = None,
    max_omsetning: float | None = None,
    min_resultat: float | None = None,
    max_resultat: float | None = None,
    orgform: str | None = None,
    regnskapsaar: int | None = None,
    parent_code: str | None = None,
    level: int = 2,
    limit: int = 50,
    kommune_limit: int = 50,
) -> dict[str, Any]:
    normalized_orgnr_query = _normalize_orgnr(q)
    orgnr_query = normalized_orgnr_query if len(normalized_orgnr_query) == 9 else None
    text_query = None if orgnr_query else q
    naeringskode_raw, naeringskode_digits = _normalize_naeringskode_query(naeringskode)
    parent_digits = re.sub(r"\D", "", str(parent_code or ""))
    cache_key = _build_sector_breakdown_cache_key(
        q=text_query,
        orgnr_query=orgnr_query,
        kommune=kommune,
        naeringskode_raw=naeringskode_raw,
        naeringskode_digits=naeringskode_digits,
        min_omsetning=min_omsetning,
        max_omsetning=max_omsetning,
        min_resultat=min_resultat,
        max_resultat=max_resultat,
        orgform=orgform,
        regnskapsaar=regnskapsaar,
        parent_digits=parent_digits,
        level=level,
        limit=limit,
        kommune_limit=kommune_limit,
    )
    kwargs = {
        "q": q,
        "kommune": kommune,
        "naeringskode": naeringskode,
        "min_omsetning": min_omsetning,
        "max_omsetning": max_omsetning,
        "min_resultat": min_resultat,
        "max_resultat": max_resultat,
        "orgform": orgform,
        "regnskapsaar": regnskapsaar,
        "parent_code": parent_code,
        "level": level,
        "limit": limit,
        "kommune_limit": kommune_limit,
    }

    now = time.time()
    with _sector_breakdown_cache_lock:
        cached_entry = _sector_breakdown_cache.get(cache_key)

    if cached_entry:
        fresh_until, stale_until, payload = cached_entry
        if fresh_until > now:
            return copy.deepcopy(payload)
        if stale_until > now:
            _refresh_sector_breakdown_cache_async(cache_key, kwargs)
            return copy.deepcopy(payload)

    payload = _compute_sector_breakdown_payload(**kwargs)
    _store_sector_breakdown_cache_entry(cache_key, payload)
    return copy.deepcopy(payload)


def _prewarm_default_sector_cache() -> None:
    _refresh_sector_breakdown_cache_async(
        _build_sector_breakdown_cache_key(
            q=None,
            orgnr_query=None,
            kommune=None,
            naeringskode_raw=None,
            naeringskode_digits="",
            min_omsetning=None,
            max_omsetning=None,
            min_resultat=None,
            max_resultat=None,
            orgform=None,
            regnskapsaar=None,
            parent_digits="",
            level=2,
            limit=100,
            kommune_limit=75,
        ),
        {
            "q": None,
            "kommune": None,
            "naeringskode": None,
            "min_omsetning": None,
            "max_omsetning": None,
            "min_resultat": None,
            "max_resultat": None,
            "orgform": None,
            "regnskapsaar": None,
            "parent_code": None,
            "level": 2,
            "limit": 100,
            "kommune_limit": 75,
        },
    )


_prewarm_default_sector_cache()


# ---------------------------------------------------------------------------
# Fylkesaggregat
#   Geografi lagres som kommunenummer; fylke utledes fra de to første sifrene.
#   Se fylke_registry for reform-håndtering (f.eks. Vestland = 46/12/14).
# ---------------------------------------------------------------------------

# NACE-divisjon (to siffer) trukket ut av e.naeringskode. Tom/manglende kode → ''.
_FYLKE_DIVISJON_EXPR = (
    "NULLIF("
    "LPAD(SUBSTRING(regexp_replace(COALESCE(e.naeringskode::text, ''), '\\D', '', 'g') FROM 1 FOR 2), 2, '0'),"
    " '00')"
)


def _fylke_where_clause(prefikser: list[str]) -> tuple[str, list[Any]]:
    """
    Bygger `LEFT(kommunenummer, 2) IN (...)`-filter fra fylkesprefikser.

    Merk: uttrykket er indeks-vennlig KUN med en funksjonell indeks som
    matcher det, se scripts/sql/idx_entity_kommune_prefix.sql. Uten den
    fører filteret til full scan av entity, som er hovedgrunnen til at
    fylkes-uttrekk kan være tregt.
    """
    placeholders = ", ".join(["%s"] * len(prefikser))
    clause = f"LEFT(COALESCE(e.kommunenummer::text, ''), 2) IN ({placeholders})"
    return clause, list(prefikser)


def get_fylke_list_payload() -> dict[str, Any]:
    """Alle fylker – nyttig for nedtrekksmeny/validering i frontend."""
    return {"fylker": list_fylker()}


# Enkel TTL-cache for fylkesaggregat. Etter første (potensielt trege) uttrekk
# svarer gjentatte kall momentant, og DB-lasten faller kraftig.
_FYLKE_AGGREGAT_CACHE_TTL = max(60, int(os.getenv("FYLKE_AGGREGAT_CACHE_TTL_SECONDS", str(15 * 60))))
_fylke_aggregat_cache: dict[tuple[Any, ...], tuple[float, dict[str, Any]]] = {}
_fylke_aggregat_cache_lock = threading.Lock()


def get_fylke_aggregat_payload(
    *,
    fylke: str | None,
    orgform: str | None = "AS",
    regnskapsaar: int | None = None,
    kommune_limit: int = 60,
) -> dict[str, Any]:
    """
    Aggregerte regnskapstall for ett fylke:
      - antall bedrifter (og hvor mange som har levert regnskap)
      - samlet omsetning, drifts- og årsresultat
      - fordeling på næringshovedområder (NACE-seksjoner A–U)
      - fordeling per kommune i fylket

    Returnerer {"error": ...} ved ukjent fylke i stedet for å kaste, slik at
    kallere (Flask-proxy og FastAPI) kan gjøre om til en pen 400.
    """
    info = resolve_fylke(fylke)
    if not info:
        return {
            "error": "ukjent_fylke",
            "detail": (
                f"Ukjent fylke: {fylke!r}. Oppgi fylkesnummer (f.eks. 46) "
                "eller navn (f.eks. Vestland)."
            ),
            "fylker": list_fylker(),
        }

    safe_kommune_limit = max(1, min(int(kommune_limit), 200))
    cache_key = (info["nummer"], (orgform or "").strip().upper(), regnskapsaar, safe_kommune_limit)

    now = time.time()
    with _fylke_aggregat_cache_lock:
        cached = _fylke_aggregat_cache.get(cache_key)
        if cached and cached[0] > now:
            return copy.deepcopy(cached[1])

    prefikser = info["kommune_prefikser"]
    fylke_clause, fylke_params = _fylke_where_clause(prefikser)

    def _base() -> tuple[str, list[Any]]:
        base_sql, params, _ = build_search_base_sql(orgform=orgform, has_regnskap=False)
        if regnskapsaar is not None:
            base_sql += " AND r.accounting_year = %s"
            params.append(regnskapsaar)
        base_sql += f" AND {fylke_clause}"
        params.extend(fylke_params)
        return base_sql, params

    # ── Sektorer + totaler i ÉN scan ──────────────────────────────────────────
    # Grupperer på to-sifret NACE-divisjon. Totalene utledes i Python ved å
    # summere divisjonsradene, så vi slipper en egen full scan for totaler.
    sektor_sql_from, sektor_params = _base()
    divisjon_rows = fetch_all(
        f"""
        SELECT
            COALESCE({_FYLKE_DIVISJON_EXPR}, '') AS divisjon,
            COUNT(*)::int AS antall_bedrifter,
            COUNT(*) FILTER (WHERE r.accounting_year IS NOT NULL)::int AS antall_med_regnskap,
            COUNT(*) FILTER (WHERE r.revenue IS NOT NULL)::int AS antall_med_omsetning,
            COUNT(*) FILTER (WHERE r.net_profit > 0)::int AS antall_med_overskudd,
            SUM(r.revenue) AS total_omsetning,
            SUM(r.operating_profit) AS total_driftsresultat,
            SUM(r.net_profit) AS total_aarsresultat
        {sektor_sql_from}
        GROUP BY 1
        """,
        sektor_params,
    )
    totals = _summer_fylke_totaler(divisjon_rows)
    sektorer = _rull_opp_sektorer(divisjon_rows)

    # ── Kommuner i fylket ─────────────────────────────────────────────────────
    kommune_sql_from, kommune_params = _base()
    municipality_name_expr = _coalesce_municipality_name_expr("e")
    kommune_rows = fetch_all(
        f"""
        SELECT
            COALESCE(NULLIF(TRIM(e.kommunenummer::text), ''), 'Ukjent') AS kommunenummer,
            COALESCE({municipality_name_expr}, 'Ukjent') AS kommunenavn,
            COUNT(*)::int AS antall_bedrifter,
            COUNT(*) FILTER (WHERE r.revenue IS NOT NULL)::int AS antall_med_omsetning,
            SUM(r.revenue) AS total_omsetning,
            SUM(r.net_profit) AS total_aarsresultat
        {kommune_sql_from}
        GROUP BY 1, 2
        ORDER BY total_omsetning DESC NULLS LAST, antall_bedrifter DESC, kommunenummer ASC
        LIMIT %s
        """,
        [*kommune_params, safe_kommune_limit],
    )
    _berik_kommuner(kommune_rows)

    payload = {
        "fylke": info,
        "filters": {"orgform": orgform, "regnskapsaar": regnskapsaar},
        "har_innbyggertall": har_innbyggertall(),
        "totaler": totals,
        "sektorer": sektorer,
        "kommuner": kommune_rows,
    }

    with _fylke_aggregat_cache_lock:
        _fylke_aggregat_cache[cache_key] = (now + _FYLKE_AGGREGAT_CACHE_TTL, payload)
    return copy.deepcopy(payload)


def _summer_fylke_totaler(divisjon_rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Utleder fylkestotaler ved å summere divisjonsradene (sparer en scan)."""
    totaler = {
        "antall_bedrifter": 0,
        "antall_med_regnskap": 0,
        "antall_med_omsetning": 0,
        "antall_med_overskudd": 0,
        "antall_med_naeringskode": 0,
        "total_omsetning": None,
        "total_driftsresultat": None,
        "total_aarsresultat": None,
    }
    for row in divisjon_rows:
        antall = int(row.get("antall_bedrifter") or 0)
        totaler["antall_bedrifter"] += antall
        totaler["antall_med_regnskap"] += int(row.get("antall_med_regnskap") or 0)
        totaler["antall_med_omsetning"] += int(row.get("antall_med_omsetning") or 0)
        totaler["antall_med_overskudd"] += int(row.get("antall_med_overskudd") or 0)
        # Divisjon '' = manglende næringskode; alt annet teller som "med næringskode".
        if str(row.get("divisjon") or "") != "":
            totaler["antall_med_naeringskode"] += antall
        for felt in ("total_omsetning", "total_driftsresultat", "total_aarsresultat"):
            verdi = row.get(felt)
            if verdi is not None:
                totaler[felt] = (totaler[felt] or 0.0) + float(verdi)
    return totaler


def _rull_opp_sektorer(divisjon_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Ruller to-sifrede NACE-divisjoner opp til næringshovedområder (A–U)."""
    buckets: dict[str, dict[str, Any]] = {}
    for row in divisjon_rows:
        seksjon, navn = nace_seksjon(row.get("divisjon"))
        bucket = buckets.setdefault(
            seksjon,
            {
                "seksjon": seksjon,
                "navn": navn,
                "antall_bedrifter": 0,
                "antall_med_omsetning": 0,
                "total_omsetning": None,
                "total_aarsresultat": None,
            },
        )
        bucket["antall_bedrifter"] += int(row.get("antall_bedrifter") or 0)
        bucket["antall_med_omsetning"] += int(row.get("antall_med_omsetning") or 0)
        for felt, kilde in (("total_omsetning", "total_omsetning"), ("total_aarsresultat", "total_aarsresultat")):
            verdi = row.get(kilde)
            if verdi is not None:
                bucket[felt] = (bucket[felt] or 0.0) + float(verdi)

    return sorted(
        buckets.values(),
        key=lambda b: (b["total_omsetning"] is not None, b["total_omsetning"] or 0.0),
        reverse=True,
    )


def _berik_kommuner(rader: list[dict[str, Any]]) -> None:
    """Fyller inn kommunenavn (fra registeret) og folketall der vi har dem."""
    for rad in rader:
        nr = str(rad.get("kommunenummer") or "").strip()
        navn = kommune_navn(nr)
        if navn:
            # Overstyr bare når DB ikke ga et reelt navn.
            if not rad.get("kommunenavn") or str(rad.get("kommunenavn")).strip().lower() in {"", "ukjent"}:
                rad["kommunenavn"] = navn
        rad["innbyggere"] = kommune_innbyggere(nr)


def _fylke_sektor_filter(sektor: str | None) -> tuple[str, list[Any], dict[str, Any] | None]:
    """
    Bygger et SQL-filter for en sektor. Godtar:
      - NACE-hovedområde (bokstav A–U) -> intervall på to-sifret divisjon
      - siffer (f.eks. 68 eller 6820) -> prefiks-match på næringskode
    Returnerer (sql_bit, params, meta) der meta beskriver tolkningen.
    """
    verdi = str(sektor or "").strip()
    if not verdi:
        return "", [], None

    digits = re.sub(r"\D", "", verdi)
    if not digits:
        intervall = nace_seksjon_intervall(verdi)
        if intervall:
            navn, lav, hoy = intervall
            sql_bit = (
                " AND NULLIF(regexp_replace(COALESCE(e.naeringskode::text, ''), '\\D', '', 'g'), '') IS NOT NULL"
                " AND SUBSTRING(regexp_replace(COALESCE(e.naeringskode::text, ''), '\\D', '', 'g') FROM 1 FOR 2)::int"
                " BETWEEN %s AND %s"
            )
            return sql_bit, [lav, hoy], {"type": "seksjon", "navn": navn, "intervall": [lav, hoy]}
        # ukjent bokstav/navn -> ingen match, men ikke krasj
        return " AND FALSE", [], {"type": "ukjent", "verdi": verdi}

    sql_bit = " AND regexp_replace(COALESCE(e.naeringskode::text, ''), '\\D', '', 'g') LIKE %s"
    return sql_bit, [f"{digits}%"], {"type": "naeringskode", "prefiks": digits}


def get_fylke_toppselskaper_payload(
    *,
    fylke: str | None,
    sektor: str | None = None,
    kommune: str | None = None,
    orgform: str | None = "AS",
    regnskapsaar: int | None = None,
    limit: int = 25,
) -> dict[str, Any]:
    """
    De største selskapene (på omsetning) i et fylke, valgfritt avgrenset til en
    sektor og/eller én kommune. Tar med antall ansatte der vi har tallet.
    """
    info = resolve_fylke(fylke)
    if not info:
        return {
            "error": "ukjent_fylke",
            "detail": f"Ukjent fylke: {fylke!r}.",
            "fylker": list_fylker(),
        }

    safe_limit = max(1, min(int(limit), 200))
    fylke_clause, fylke_params = _fylke_where_clause(info["kommune_prefikser"])

    base_sql, params, _ = build_search_base_sql(orgform=orgform, has_regnskap=False)
    base_sql += " AND r.revenue IS NOT NULL"
    if regnskapsaar is not None:
        base_sql += " AND r.accounting_year = %s"
        params.append(regnskapsaar)
    base_sql += f" AND {fylke_clause}"
    params.extend(fylke_params)

    kommune_nr = re.sub(r"\D", "", str(kommune or ""))
    if kommune_nr:
        base_sql += " AND regexp_replace(COALESCE(e.kommunenummer::text, ''), '\\D', '', 'g') = %s"
        params.append(kommune_nr)

    sektor_sql, sektor_params, sektor_meta = _fylke_sektor_filter(sektor)
    base_sql += sektor_sql
    params.extend(sektor_params)

    rows = fetch_all(
        f"""
        SELECT
            e.orgnr,
            e.navn,
            e.orgform,
            e.kommunenummer,
            e.ansatte,
            e.naeringskode,
            r.revenue AS omsetning,
            r.operating_profit AS driftsresultat,
            r.net_profit AS aarsresultat,
            r.accounting_year AS regnskapsaar,
            CASE
                WHEN e.ansatte IS NOT NULL AND e.ansatte > 0 AND r.revenue IS NOT NULL
                THEN r.revenue / e.ansatte
                ELSE NULL
            END AS omsetning_per_ansatt
        {base_sql}
        ORDER BY r.revenue DESC NULLS LAST, e.navn ASC
        LIMIT %s
        """,
        [*params, safe_limit],
    )

    for rad in rows:
        rad["kommunenavn"] = kommune_navn(rad.get("kommunenummer"))
        rad["innbyggere"] = kommune_innbyggere(rad.get("kommunenummer"))

    return {
        "fylke": info,
        "filters": {
            "sektor": sektor,
            "sektor_tolkning": sektor_meta,
            "kommune": kommune or None,
            "orgform": orgform,
            "regnskapsaar": regnskapsaar,
            "limit": safe_limit,
        },
        "selskaper": rows,
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


@router.get("/analysis-api/industry/overview")
def industry_overview(
    limit: int = Query(default=25, ge=1, le=100),
) -> dict[str, Any]:
    return get_industry_overview_payload(limit=limit)


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
        has_regnskap=has_regnskap,
        regnskapsaar=regnskapsaar,
        innlevert_etter=innlevert_etter,
        limit=limit,
        offset=offset,
        sort_by=sort_by,
        sort_dir=sort_dir,
    )


@router.get("/analysis-api/companies/sector-breakdown")
def companies_sector_breakdown(
    q: str | None = None,
    kommune: str | None = None,
    naeringskode: str | None = None,
    min_omsetning: float | None = None,
    max_omsetning: float | None = None,
    min_resultat: float | None = None,
    max_resultat: float | None = None,
    orgform: str | None = None,
    regnskapsaar: int | None = None,
    parent_code: str | None = None,
    level: int = Query(default=2, ge=2, le=5),
    limit: int = Query(default=50, ge=1, le=200),
    kommune_limit: int = Query(default=50, ge=1, le=200),
) -> dict[str, Any]:
    return get_sector_breakdown_payload(
        q=q,
        kommune=kommune,
        naeringskode=naeringskode,
        min_omsetning=min_omsetning,
        max_omsetning=max_omsetning,
        min_resultat=min_resultat,
        max_resultat=max_resultat,
        orgform=orgform,
        regnskapsaar=regnskapsaar,
        parent_code=parent_code,
        level=level,
        limit=limit,
        kommune_limit=kommune_limit,
    )


@router.get("/analysis-api/companies/filter/summary")
def companies_filter_summary(
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
    top_n: int = Query(default=5, ge=1, le=20),
    scatter_limit: int = Query(default=800, ge=100, le=2000),
) -> dict[str, Any]:
    return get_companies_filter_summary_payload(
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
        has_regnskap=has_regnskap,
        regnskapsaar=regnskapsaar,
        innlevert_etter=innlevert_etter,
        top_n=top_n,
        scatter_limit=scatter_limit,
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


def get_kart_payload(
    *,
    north: float = 90.0,
    south: float = -90.0,
    east: float = 180.0,
    west: float = -180.0,
    q: str | None = None,
    naeringskode: str | None = None,
    orgform: str | None = None,
    min_omsetning: float | None = None,
    max_omsetning: float | None = None,
    has_regnskap: bool = False,
    regnskapsaar: int | None = None,
    limit: int = 500,
) -> dict[str, Any]:
    """Henter selskaper innenfor bbox med GeoJSON-format for kartvisning."""
    from Fastapi_Backend import LATEST_REGNSKAP_JOIN, latest_regnskap_join_for_year, normalize_decimal

    limit = min(limit, 2000)

    regnskap_join = (
        latest_regnskap_join_for_year(regnskapsaar)
        if regnskapsaar
        else LATEST_REGNSKAP_JOIN
    )

    params: list[Any] = [south, north, west, east]
    where = [
        "e.lat IS NOT NULL",
        "e.lat BETWEEN %s AND %s",
        "e.lon BETWEEN %s AND %s",
    ]

    # Tekstsøk — bruker UNION for indeksbruk
    if q:
        tokens = [t.strip() for t in q.lower().split() if t.strip()]
        if tokens:
            name_conditions = " AND ".join("navn ILIKE %s" for _ in tokens)
            safe_tokens = [t.replace("'", "''") for t in tokens]
            bedr_conditions = " AND ".join(f"bn.bedr_navn ILIKE '%%{t}%%'" for t in safe_tokens)
            where.append(f"""e.orgnr IN (
                SELECT orgnr FROM entity WHERE {name_conditions}
                UNION
                SELECT DISTINCT bn.parent_orgnr FROM bedr_navn bn WHERE {bedr_conditions}
            )""")
            params.extend(f"%{t}%" for t in tokens)
        else:
            safe_q = q.replace("'", "''")
            where.append(f"""e.orgnr IN (
                SELECT orgnr FROM entity WHERE navn ILIKE '%%{safe_q}%%'
                UNION
                SELECT DISTINCT bn.parent_orgnr FROM bedr_navn bn WHERE bn.bedr_navn ILIKE '%%{safe_q}%%'
            )""")

    if naeringskode:
        where.append("e.naeringskode LIKE %s")
        params.append(naeringskode + "%")

    if orgform:
        where.append("e.orgform = %s")
        params.append(orgform)

    if min_omsetning is not None:
        where.append("r.revenue >= %s")
        params.append(min_omsetning)

    if max_omsetning is not None:
        where.append("r.revenue <= %s")
        params.append(max_omsetning)

    if has_regnskap:
        where.append("r.accounting_year IS NOT NULL")

    params.append(limit)

    sql = f"""
        SELECT
            e.orgnr::text AS orgnr,
            e.orgnr::text AS parent_orgnr,
            e.navn,
            e.adresse,
            e.postnummer,
            e.naeringskode,
            e.orgform,
            e.ansatte,
            e.lat,
            e.lon,
            r.revenue          AS driftsinntekter,
            r.operating_profit AS driftsresultat,
            r.net_profit       AS aarsresultat,
            r.equity           AS sum_egenkapital,
            r.accounting_year  AS regnskapsaar
        FROM entity e
        {regnskap_join}
        WHERE {" AND ".join(where)}
        ORDER BY r.revenue DESC NULLS LAST
        LIMIT %s
    """

    rows = fetch_all(sql, params)

    def farge(row):
        rev = row.get("driftsinntekter")
        dri = row.get("driftsresultat")
        if dri is None or rev is None:
            return "gray"
        pct = dri / rev if rev else 0
        if pct >= 0.20:
            return "green"
        if pct >= 0.0:
            return "yellow"
        return "red"

    features = [
        {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [row["lon"], row["lat"]]
            },
            "properties": {**row, "farge": farge(row)}
        }
        for row in rows
        if row.get("lat") and row.get("lon")
    ]

    return {
        "type": "FeatureCollection",
        "features": features,
        "count": len(features),
    }
