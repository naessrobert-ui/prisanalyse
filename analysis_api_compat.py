from __future__ import annotations

import re
from functools import lru_cache
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
    {"code": "68.200", "description": "Utleie av egen eller leid fast eiendom"},
    {"code": "47.810", "description": "Butikkhandel med motorvogner, unntatt motorsykler"},
]

_INDUSTRY_CODE_DESCRIPTIONS = {
    "45": "Handel og reparasjon av motorvogner",
    "45.11": "Handel med biler og lette motorvogner",
    "45.20": "Vedlikehold og reparasjon av motorvogner",
    "47.810": "Butikkhandel med motorvogner, unntatt motorsykler",
    "49.41": "Godstransport på vei",
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
        "municipality_name_columns": _municipality_name_columns(),
        "max_limit": MAX_LIMIT,
    }


def get_industry_suggest_payload(q: str, limit: int = 8) -> dict[str, Any]:
    query = (q or "").strip()
    if not query:
        return {"query": "", "limit": limit, "suggestions": []}

    safe_limit = max(1, min(int(limit), 25))
    q_lower = query.lower()
    digits = re.sub(r"\D", "", query)

    hint_matches: list[dict[str, Any]] = []
    for hint in _INDUSTRY_HINTS:
        haystack = f'{hint["code"]} {hint["description"]}'.lower()
        if q_lower in haystack or (digits and hint["code"].replace(".", "").startswith(digits)):
            hint_matches.append({**hint, "company_count": 0})

    desc_expr = _industry_description_expr("e")
    if digits:
        rows = fetch_all(
            f"""
            SELECT
                TRIM(COALESCE(e.naeringskode::text, '')) AS code,
                {desc_expr} AS description,
                COUNT(*)::int AS company_count
            FROM entity e
            WHERE regexp_replace(COALESCE(e.naeringskode::text, ''), '\D', '', 'g') LIKE %s
            GROUP BY 1, 2
            HAVING TRIM(COALESCE(e.naeringskode::text, '')) <> ''
            ORDER BY company_count DESC, code ASC
            LIMIT %s
            """,
            [f"{digits}%", safe_limit],
        )
    else:
        description_columns = _industry_description_columns()
        terms = _expand_industry_terms(query)
        clauses: list[str] = []
        params: list[Any] = []
        for term in terms or [query.lower()]:
            term_like = f"%{term}%"
            clauses.append("COALESCE(e.naeringskode::text, '') ILIKE %s")
            params.append(term_like)
            for col in description_columns:
                clauses.append(f"COALESCE(e.{col}::text, '') ILIKE %s")
                params.append(term_like)
        where_clause = " OR ".join(clauses) if clauses else "COALESCE(e.naeringskode::text, '') ILIKE %s"
        if not clauses:
            params = [f"%{query}%"]
        params.append(safe_limit)
        rows = fetch_all(
            f"""
            SELECT
                TRIM(COALESCE(e.naeringskode::text, '')) AS code,
                {desc_expr} AS description,
                COUNT(*)::int AS company_count
            FROM entity e
            WHERE {where_clause}
            GROUP BY 1, 2
            HAVING TRIM(COALESCE(e.naeringskode::text, '')) <> ''
            ORDER BY company_count DESC, code ASC
            LIMIT %s
            """,
            params,
        )

    suggestions: list[dict[str, Any]] = []
    for row in rows:
        code = str(row.get("code") or "").strip()
        if not code:
            continue
        hint = next((item for item in hint_matches if item["code"] == code), None)
        suggestions.append(
            {
                "code": code,
                "description": row.get("description") or (hint["description"] if hint else _fallback_description_for_code(code, hint_matches)),
                "company_count": int(row.get("company_count") or 0),
            }
        )
    if not suggestions:
        terms = _expand_industry_terms(query)
        suggestions = _taxonomy_suggestions(terms, safe_limit) or hint_matches

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
            SUBSTRING(regexp_replace(COALESCE(e.naeringskode::text, ''), '\D', '', 'g') FROM 1 FOR 2) AS code,
            COUNT(*)::int AS company_count
        FROM entity e
        WHERE NULLIF(TRIM(COALESCE(e.naeringskode::text, '')), '') IS NOT NULL
        GROUP BY 1
        HAVING NULLIF(TRIM(COALESCE(SUBSTRING(regexp_replace(COALESCE(e.naeringskode::text, ''), '\D', '', 'g') FROM 1 FOR 2), '')), '') IS NOT NULL
        ORDER BY company_count DESC, code ASC
        LIMIT %s
        """,
        [safe_limit],
    )
    suggestions = [
        {
            "code": str(row.get("code") or ""),
            "description": _fallback_description_for_code(str(row.get("code") or ""), _INDUSTRY_HINTS),
            "company_count": int(row.get("company_count") or 0),
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
