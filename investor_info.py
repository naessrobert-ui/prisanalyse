# investor_info.py
"""Informasjon om en investor (selskap) til Handler Oslo Børs.

Hentes først når en investor velges, og caches. Alle kall går parallelt:

  * Enhetsregisteret (Brreg): navn, organisasjonsform, bransje, kommune, ansatte
  * Roller (Brreg): styreleder og daglig leder
  * Regnskapsregisteret (Brreg): siste år med omsetning, resultat og egenkapital
  * Aksjonærregisteret (egen database, hvis tilgjengelig): største eiere

Orgnr finnes fra investorens raw_id hvis det er et orgnr, ellers ved navnesøk i
Enhetsregisteret (bare eksakt navnetreff). Privatpersoner og utenlandske
investorer slås ikke opp.
"""
from __future__ import annotations

import logging
import re
import time
import unicodedata
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
from typing import Any

import requests

_LOG = logging.getLogger(__name__)

ENHET_URL = "https://data.brreg.no/enhetsregisteret/api/enheter/{orgnr}"
ROLLER_URL = "https://data.brreg.no/enhetsregisteret/api/enheter/{orgnr}/roller"
SOK_URL = "https://data.brreg.no/enhetsregisteret/api/enheter"
REGNSKAP_URL = "https://data.brreg.no/regnskapsregisteret/regnskap/{orgnr}"
TIMEOUT = 6

_CACHE: dict[str, tuple[float, dict]] = {}
_CACHE_TTL = 12 * 3600
_CACHE_TTL_DELVIS = 10 * 60          # kortere når noe manglet, så det prøves igjen

# Hele oppslaget skal aldri holde en web-tråd lenger enn dette
FRIST_SEKUNDER = 8

# Aksjonærdatabasen: hopp over i 10 min etter feil eller tidsavbrudd, så
# klikk ikke hoper seg opp mens databasen er nede
_EIERE_NEDE_TIL = 0.0
_EIERE_PAUSE = 10 * 60

# Felles trådpool for oppslag. Tråder som henger (f.eks. DB-tilkobling uten
# svar) blir liggende her, ikke i web-serverens tråder.
_POOL = ThreadPoolExecutor(max_workers=8, thread_name_prefix="investor-info")

_PRIVAT = {"p", "priv", "privat", "private", "person", "individual"}


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"Accept": "application/json", "User-Agent": "prisanalyse.no/handler"})
    return s


def _get_json(sess: requests.Session, url: str, **params) -> Any:
    try:
        r = sess.get(url, params=params or None, timeout=TIMEOUT)
        if r.status_code != 200:
            return None
        return r.json()
    except (requests.RequestException, ValueError):
        return None


def normaliser_navn(navn: str) -> str:
    """Store bokstaver, uten tegnsetting og doble mellomrom, for å sammenligne navn."""
    s = unicodedata.normalize("NFKC", navn or "").upper()
    s = re.sub(r"[^\wÆØÅ ]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def er_orgnr(v: str | None) -> bool:
    return bool(v) and bool(re.fullmatch(r"\d{9}", str(v).strip()))


# =========================================================
# Orgnr
# =========================================================
def finn_orgnr(sess: requests.Session, navn: str, raw_id: str | None) -> tuple[str | None, str]:
    """Returner (orgnr, kilde). kilde er 'vps', 'navnesøk' eller forklaring på hvorfor ikke funnet."""
    rid = re.sub(r"\D", "", str(raw_id or ""))
    if er_orgnr(rid):
        return rid, "vps"
    data = _get_json(sess, SOK_URL, navn=navn, size=20)
    enheter = ((data or {}).get("_embedded") or {}).get("enheter") or []
    maal = normaliser_navn(navn)
    treff = [e for e in enheter if normaliser_navn(e.get("navn", "")) == maal]
    if len(treff) == 1:
        return str(treff[0].get("organisasjonsnummer")), "navnesøk"
    if len(treff) > 1:
        aktive = [e for e in treff if not e.get("slettedato") and not e.get("konkurs")]
        if len(aktive) == 1:
            return str(aktive[0].get("organisasjonsnummer")), "navnesøk"
        return None, "flere selskaper med samme navn"
    return None, "fant ikke selskapet i Enhetsregisteret"


# =========================================================
# Oppslag
# =========================================================
def hent_enhet(sess: requests.Session, orgnr: str) -> dict | None:
    d = _get_json(sess, ENHET_URL.format(orgnr=orgnr))
    if not isinstance(d, dict):
        return None
    adr = d.get("forretningsadresse") or d.get("postadresse") or {}
    return {
        "navn": d.get("navn"),
        "organisasjonsform": (d.get("organisasjonsform") or {}).get("beskrivelse"),
        "bransje": (d.get("naeringskode1") or {}).get("beskrivelse"),
        "kommune": adr.get("kommune"),
        "stiftet": d.get("stiftelsesdato"),
        "ansatte": d.get("antallAnsatte"),
        "hjemmeside": d.get("hjemmeside"),
        "konkurs": bool(d.get("konkurs")),
        "slettet": d.get("slettedato"),
    }


def _rolle_navn(rolle: dict) -> str | None:
    p = rolle.get("person") or {}
    n = p.get("navn") or {}
    if n:
        return " ".join(x for x in (n.get("fornavn"), n.get("mellomnavn"), n.get("etternavn")) if x).strip() or None
    e = rolle.get("enhet") or {}
    navn = e.get("navn")
    if isinstance(navn, list):
        navn = " ".join(navn)
    return navn or None


def hent_roller(sess: requests.Session, orgnr: str) -> dict:
    d = _get_json(sess, ROLLER_URL.format(orgnr=orgnr)) or {}
    out: dict[str, str] = {}
    for gruppe in d.get("rollegrupper") or []:
        for r in gruppe.get("roller") or []:
            if r.get("fratraadt") or r.get("avregistrert"):
                continue
            kode = (r.get("type") or {}).get("kode")
            felt = {"LEDE": "styreleder", "DAGL": "daglig_leder"}.get(kode)
            if felt and felt not in out:
                navn = _rolle_navn(r)
                if navn:
                    out[felt] = navn
    return out


def _belop(obj: Any, *keys: str) -> float | None:
    for k in keys:
        if not isinstance(obj, dict):
            return None
        obj = obj.get(k)
    if isinstance(obj, (int, float)):
        return float(obj)
    if isinstance(obj, dict) and isinstance(obj.get("beloep"), (int, float)):
        return float(obj["beloep"])
    return None


def hent_regnskap(sess: requests.Session, orgnr: str) -> dict | None:
    d = _get_json(sess, REGNSKAP_URL.format(orgnr=orgnr))
    if isinstance(d, list):
        d = max(d, key=lambda x: str((x.get("regnskapsperiode") or {}).get("tilDato", "")), default=None)
    if not isinstance(d, dict):
        return None
    til = str((d.get("regnskapsperiode") or {}).get("tilDato", ""))
    return {
        "aar": int(til[:4]) if re.match(r"^\d{4}", til) else None,
        "valuta": d.get("valuta") or "NOK",
        "omsetning": _belop(d, "resultatregnskapResultat", "driftsresultat", "driftsinntekter", "sumDriftsinntekter"),
        "driftsresultat": _belop(d, "resultatregnskapResultat", "driftsresultat", "driftsresultat"),
        "resultat_etter_skatt": _belop(d, "resultatregnskapResultat", "aarsresultat"),
        "egenkapital": _belop(d, "egenkapitalGjeld", "egenkapital", "sumEgenkapital"),
        "sum_eiendeler": _belop(d, "eiendeler", "sumEiendeler"),
    }


def hent_eiere(orgnr: str, antall: int = 5) -> list[dict] | None:
    """Største eiere fra aksjonærregisteret i egen database. None hvis databasen ikke er tilgjengelig."""
    global _EIERE_NEDE_TIL
    if time.time() < _EIERE_NEDE_TIL:
        return None
    try:
        import aksjonaer_routes as ar
        from psycopg import sql
    except Exception:
        return None
    try:
        with ar.connect_db() as conn:
            schema, cols = ar._load_table_columns(conn, ar.SHAREHOLDER_TABLE)
            if not cols:
                return None
            tabell = ar._qualified_table(schema, ar.SHAREHOLDER_TABLE)
            navn_col = ar._find_first_column(cols, ["shareholder_name"])
            org_col = ar._find_first_column(cols, ["orgnr"])
            aksjer_col = ar._find_first_column(cols, ["shares_count", "shares_owned"])
            total_col = ar._find_first_column(cols, ["company_shares_count", "company_total_shares"])
            snap_col = ar._find_first_column(cols, ["snapshot_date"])
            if not (navn_col and org_col and aksjer_col):
                return None
            snap_filter = sql.SQL("")
            if snap_col:
                snap_filter = sql.SQL("AND {snap} = (SELECT MAX({snap}) FROM {t} WHERE {org}::text = %s)").format(
                    snap=sql.Identifier(snap_col), t=tabell, org=sql.Identifier(org_col))
            q = sql.SQL(
                """
                SELECT {navn} AS navn, SUM(COALESCE({aksjer},0)) AS aksjer, MAX({total}) AS total
                FROM {t}
                WHERE {org}::text = %s {snap_filter}
                GROUP BY 1 ORDER BY 2 DESC NULLS LAST LIMIT %s
                """
            ).format(
                navn=sql.Identifier(navn_col), aksjer=sql.Identifier(aksjer_col),
                total=ar._qualified_column(total_col), t=tabell, org=sql.Identifier(org_col),
                snap_filter=snap_filter,
            )
            params = [orgnr] + ([orgnr] if snap_col else []) + [antall]
            with conn.cursor() as cur:
                cur.execute(q, params)
                rows = cur.fetchall()
    except Exception:
        _LOG.info("Aksjonærdatabase ikke tilgjengelig for eieroppslag", exc_info=True)
        _EIERE_NEDE_TIL = time.time() + _EIERE_PAUSE
        return None
    out = []
    for navn, aksjer, total in rows:
        andel = float(aksjer) / float(total) * 100 if total else None
        out.append({"navn": navn, "aksjer": float(aksjer or 0), "andel_pct": andel})
    return out


# =========================================================
# Samlet
# =========================================================
def lenker(orgnr: str | None, navn: str) -> dict:
    if orgnr:
        return {
            "proff": f"https://www.proff.no/bransjes%C3%B8k?q={orgnr}",
            "brreg": f"https://virksomhet.brreg.no/nb/oppslag/enheter/{orgnr}",
            "aksjonaerer": f"/aksjonaer?q={orgnr}&mode=company",
        }
    return {"proff": "https://www.proff.no/bransjes%C3%B8k?q=" + requests.utils.quote(navn)}


def investor_info(investor_id: str, navn: str, investor_type: str | None, country_code: str | None,
                  raw_id: str | None, sess: requests.Session | None = None) -> dict:
    """Samlet informasjon om en investor. Caches i 12 timer per investor."""
    global _EIERE_NEDE_TIL
    nå = time.time()
    hit = _CACHE.get(investor_id)
    if hit and nå - hit[0] < _CACHE_TTL:
        return hit[1]

    t0 = time.perf_counter()
    typ = (investor_type or "").strip().lower()
    land = (country_code or "").strip().upper()
    res: dict[str, Any] = {"investor_id": investor_id, "navn": navn, "land": land or None}

    if typ in _PRIVAT:
        res.update({"type": "person", "melding": "Privatperson, slås ikke opp.",
                    "lenker": {"aksjonaerer": f"/aksjonaer?q={requests.utils.quote(navn)}&mode=person"}})
    elif land and land not in ("NO", "NOR"):
        res.update({"type": "utenlandsk", "melding": f"Utenlandsk investor ({land}), finnes ikke i Brreg.",
                    "lenker": lenker(None, navn)})
    else:
        sess = sess or _session()
        orgnr, kilde = finn_orgnr(sess, navn, raw_id)
        res.update({"type": "selskap", "orgnr": orgnr, "orgnr_kilde": kilde, "lenker": lenker(orgnr, navn)})
        if orgnr:
            frist = time.monotonic() + FRIST_SEKUNDER
            futures = {
                "enhet": _POOL.submit(hent_enhet, sess, orgnr),
                "roller": _POOL.submit(hent_roller, sess, orgnr),
                "regnskap": _POOL.submit(hent_regnskap, sess, orgnr),
                "eiere": _POOL.submit(hent_eiere, orgnr),
            }
            for navn_felt, fut in futures.items():
                try:
                    res[navn_felt] = fut.result(timeout=max(0.1, frist - time.monotonic()))
                except FuturesTimeout:
                    res[navn_felt] = {} if navn_felt == "roller" else None
                    res.setdefault("tidsavbrudd", []).append(navn_felt)
                    if navn_felt == "eiere":
                        _EIERE_NEDE_TIL = time.time() + _EIERE_PAUSE
                except Exception:
                    _LOG.warning("Oppslag %s feilet for %s", navn_felt, orgnr, exc_info=True)
                    res[navn_felt] = {} if navn_felt == "roller" else None
        else:
            res["melding"] = f"Fant ikke orgnr: {kilde}."

    res["tid_ms"] = round((time.perf_counter() - t0) * 1000)
    delvis = bool(res.get("tidsavbrudd")) or (res.get("orgnr") and (res.get("enhet") is None or res.get("eiere") is None))
    # Delvise svar caches kort, så de prøves igjen senere
    _CACHE[investor_id] = (nå - (_CACHE_TTL - _CACHE_TTL_DELVIS) if delvis else nå, res)
    return res
