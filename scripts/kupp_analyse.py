"""
scripts/kupp_analyse.py – Etteranalyse av kupp-vakten
=====================================================
Svarer på spørsmålet: *hva kjennetegner bilene kupp-vakten varslet om som nå er
solgt på FINN?* Bruk det til å tune tersklene (KUPP_RABATT_TRAPP m.fl.) og
filtrene (fylke/selger/drivstoff) – dimensjonene med høy solgt-andel er der
vakten treffer best.

Grunnlag
--------
Leser den persistente loggen `calc/bil/kupp_vakt_logg.json` som kupp_vakt.py
skriver (nøklet på FinnKode -> egenskaper ved varslingstidspunktet). Loggen
bygges fra og med logge-oppdateringen, så de første dagene er den tynn; kjør
igjen når den har vokst. Har du ikke loggen ennå, sier skriptet fra og stopper –
det bruker med vilje IKKE state-fila som fallback, for den inneholder alle
annonser vakten har *sett*, ikke bare kuppene. Vil du ha med gamle varsler,
importer dem fra Pushover med scripts/kupp_import_pushover.py.

For hver bil hentes FINN-annonsen og status avgjøres: solgt / aktiv / borte.
`dager` er tiden fra varsel til vi *oppdaget* at den var solgt (øvre grense –
selve salgstidspunktet vises ikke av FINN).

Kjøring
-------
    python -m scripts.kupp_analyse              # full rapport til stdout
    python -m scripts.kupp_analyse --csv        # + skriv per-bil CSV til S3
    python -m scripts.kupp_analyse --limit 30   # test på et utvalg
    python -m scripts.kupp_analyse --verbose    # vis rå status-signal per bil

Solgt-deteksjon kan justeres uten kodeendring via env `KUPP_SOLGT_MARKORER`
(markører adskilt med `|`). Kjør med --verbose på noen kjente solgte/aktive
annonser først for å bekrefte at deteksjonen treffer.
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import os
import re
import sys
import statistics
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Gjenbruk konfig og lettvekts FINN-/S3-hjelpere fra selve vakten, så scraping
# (User-Agent, retry, sleep) og bøtte/nøkler holder seg synkronisert.
from scripts.kupp_vakt import (  # noqa: E402
    FINN_ITEM_URL,
    LOGG_KEY,
    S3_BUCKET,
    _fetch,
    _make_session,
    _s3,
)

# ---- Solgt-deteksjon ---------------------------------------------------------
# FINN merker solgte/utløpte annonser både i innbakt JSON og med en synlig
# «Solgt»-markør. Vi sjekker et sett markører mot både rå og whitespace-strippet
# HTML (JSON-markørene overlever strippingen, tag-markørene i rå tekst).
_DEFAULT_MARKORER = [
    '"disposed":true',
    '"isdisposed":true',
    '"status":"disposed"',
    '"status":"expired"',
    '"expired":true',
    '"sold":true',
    '>solgt<',
    'content="oos"',           # product/og:availability = out of stock
    'availability":"soldout"',
]
_MARKORER = [
    m.strip().lower()
    for m in os.getenv("KUPP_SOLGT_MARKORER", "").split("|")
    if m.strip()
] or _DEFAULT_MARKORER

_BORTE_MARKORER = [
    "ikke lenger tilgjengelig",
    "annonsen er ikke aktiv",
    "finner ikke siden",
    "siden finnes ikke",
]


def _hent_status(session, fk: str):
    """Returner (status, signal) der status er 'solgt' | 'aktiv' | 'borte' |
    'ukjent', og signal er markøren/koden som avgjorde det (for --verbose)."""
    resp = _fetch(session, FINN_ITEM_URL.format(fk))
    if resp is None:
        return "ukjent", "ingen respons"
    if resp.status_code in (404, 410):
        return "borte", f"http {resp.status_code}"
    if resp.status_code != 200:
        return "ukjent", f"http {resp.status_code}"

    low = resp.text.lower()
    compact = re.sub(r"\s+", "", low)
    for m in _MARKORER:
        if m in low or m in compact:
            return "solgt", m
    for m in _BORTE_MARKORER:
        if m in low:
            return "borte", m
    if re.search(r"\d[\d\s ]*\s*kr\b", low):
        return "aktiv", "pris synlig"
    return "ukjent", "ingen markør"


# ---- Grunnlag: kupp-loggen (calc/bil/kupp_vakt_logg.json) --------------------
def _last_logg(s3) -> dict:
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=LOGG_KEY)
        data = json.loads(obj["Body"].read().decode("utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _dager_siden(iso: str | None) -> float | None:
    if not iso:
        return None
    try:
        t = datetime.fromisoformat(iso)
        if t.tzinfo is None:
            t = t.replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - t).total_seconds() / 86400.0
    except Exception:
        return None


# ---- Banding for nedbrytning -------------------------------------------------
def _band(v, kanter, etikett):
    """Plasser tallverdi v i et intervall gitt av `kanter` (stigende)."""
    if v is None:
        return "ukjent"
    try:
        v = float(v)
    except (TypeError, ValueError):
        return "ukjent"
    forrige = None
    for k in kanter:
        if v < k:
            return etikett(forrige, k)
        forrige = k
    return etikett(forrige, None)


def _rabatt_band(v):
    return _band(v, [6, 8, 12, 18, 25], lambda lo, hi:
                 f"<{hi:g}%" if lo is None else (f"{lo:g}%+" if hi is None else f"{lo:g}–{hi:g}%"))


def _pris_band(v):
    def e(lo, hi):
        k = lambda x: f"{int(x/1000)}k"
        return f"<{k(hi)}" if lo is None else (f"{k(lo)}+" if hi is None else f"{k(lo)}–{k(hi)}")
    return _band(v, [100_000, 200_000, 300_000, 500_000], e)


def _km_band(v):
    def e(lo, hi):
        k = lambda x: f"{int(x/1000)}k"
        return f"<{k(hi)}" if lo is None else (f"{k(lo)}+" if hi is None else f"{k(lo)}–{k(hi)}")
    return _band(v, [30_000, 60_000, 100_000, 150_000], e)


def _aar_band(v):
    if not v:
        return "ukjent"
    try:
        aar = int(v)
    except (TypeError, ValueError):
        return "ukjent"
    n = datetime.now().year
    alder = n - aar
    if alder <= 2:
        return "0–2 år"
    if alder <= 5:
        return "3–5 år"
    if alder <= 8:
        return "6–8 år"
    if alder <= 12:
        return "9–12 år"
    return "13 år+"


# ---- Rapport -----------------------------------------------------------------
def _skriv_gruppe(tittel, rader, nokkel_fn, min_n=1, topp=None):
    """Skriv én nedbrytningstabell: bøtte -> antall, solgt, andel, median dager."""
    grupper: dict = {}
    for r in rader:
        b = nokkel_fn(r)
        if b is None:
            continue
        grupper.setdefault(b, []).append(r)

    linjer = []
    for b, rs in grupper.items():
        n = len(rs)
        if n < min_n:
            continue
        solgte = [r for r in rs if r["status"] == "solgt"]
        andel = 100.0 * len(solgte) / n if n else 0.0
        dager = [r["dager"] for r in solgte if r["dager"] is not None]
        med = statistics.median(dager) if dager else None
        linjer.append((b, n, len(solgte), andel, med))

    # Sorter etter solgt-andel (høyest først), så antall
    linjer.sort(key=lambda x: (-x[3], -x[1]))
    if topp:
        linjer = linjer[:topp]
    if not linjer:
        return

    print(f"\n{tittel}")
    print(f"  {'gruppe':<16}{'n':>5}{'solgt':>8}{'andel':>9}{'md.dager':>10}")
    for b, n, s, andel, med in linjer:
        med_s = f"{med:.0f}" if med is not None else "–"
        print(f"  {str(b):<16}{n:>5}{s:>8}{andel:>8.0f}%{med_s:>10}")


def rapport(rader: list[dict], grunnlag: str, dager_span):
    n = len(rader)
    solgte = [r for r in rader if r["status"] == "solgt"]
    aktive = [r for r in rader if r["status"] == "aktiv"]
    borte = [r for r in rader if r["status"] == "borte"]
    ukjent = [r for r in rader if r["status"] == "ukjent"]
    andel = 100.0 * len(solgte) / n if n else 0.0
    dager = [r["dager"] for r in solgte if r["dager"] is not None]
    med = statistics.median(dager) if dager else None

    print("=" * 66)
    print("KUPP-ANALYSE – kjennetegn ved biler som er blitt solgt")
    print("=" * 66)
    print(f"Grunnlag : {grunnlag}")
    if dager_span:
        print(f"Periode  : varslet mellom {dager_span[0]} og {dager_span[1]}")
    print(f"Biler    : {n}  (solgt {len(solgte)}, aktiv {len(aktive)}, "
          f"borte {len(borte)}, ukjent {len(ukjent)})")
    print(f"Solgt-andel: {andel:.0f} %"
          + (f"   |   median dager til solgt: ≤{med:.0f}" if med is not None else ""))
    print("(Høy solgt-andel i en gruppe = tersklene/filteret treffer godt der.)")

    _skriv_gruppe("Etter RABATT (viktigst):", rader, lambda r: _rabatt_band(r.get("rabatt_pct")))
    _skriv_gruppe("Etter PRIS:", rader, lambda r: _pris_band(r.get("pris")))
    _skriv_gruppe("Etter DRIVSTOFF:", rader, lambda r: (r.get("Drivstoff") or "ukjent"))
    _skriv_gruppe("Etter HJEMFYLKE:", rader, lambda r: (
        "ukjent" if r.get("i_hjemfylke") is None
        else ("hjemfylke" if r.get("i_hjemfylke") else "utenfor")))
    _skriv_gruppe("Etter SELGER-filter:", rader, lambda r: (r.get("selger_filter") or "ukjent"))
    _skriv_gruppe("Etter FYLKE-filter:", rader, lambda r: (r.get("fylke_filter") or "ukjent"))
    _skriv_gruppe("Etter STED (topp 12):", rader, lambda r: (r.get("sted") or "ukjent"),
                  min_n=2, topp=12)
    _skriv_gruppe("Etter MERKE (topp 12):", rader, lambda r: (r.get("Merke") or "ukjent"),
                  min_n=2, topp=12)
    _skriv_gruppe("Etter ÅRSMODELL:", rader, lambda r: _aar_band(r.get("Årstall")))
    _skriv_gruppe("Etter KJØRELENGDE:", rader, lambda r: _km_band(r.get("Kjørelengde")))
    print("\n" + "=" * 66)


def _skriv_csv(s3, rader, key="calc/bil/kupp_vakt_analyse.csv"):
    kol = ["FinnKode", "status", "dager", "flagget", "Merke", "Modell", "Årstall",
           "Kjørelengde", "Drivstoff", "sted", "pris", "forventet_pris",
           "rabatt_pct", "rabatt_kr", "selger_filter", "fylke_filter",
           "i_hjemfylke", "url"]
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=kol, extrasaction="ignore")
    w.writeheader()
    for r in rader:
        w.writerow(r)
    s3.put_object(Bucket=S3_BUCKET, Key=key,
                  Body=buf.getvalue().encode("utf-8"), ContentType="text/csv")
    print(f"[kupp_analyse] Skrev per-bil CSV til s3://{S3_BUCKET}/{key} ({len(rader)} rader)")


def main():
    ap = argparse.ArgumentParser(description="Etteranalyse av kupp-vakten")
    ap.add_argument("--limit", type=int, default=0, help="Analyser bare de N nyeste (test)")
    ap.add_argument("--csv", action="store_true", help="Skriv per-bil CSV til S3")
    ap.add_argument("--verbose", action="store_true", help="Vis status-signal per bil")
    args = ap.parse_args()

    s3 = _s3()
    logg = _last_logg(s3)
    if not logg:
        # Merk: state-fila er IKKE et gyldig grunnlag – den inneholder alle
        # annonser vakten har *sett* (hundrevis–tusenvis), ikke bare kuppene.
        print(f"[kupp_analyse] Ingen kupp-logg ennå i s3://{S3_BUCKET}/{LOGG_KEY}.")
        print("  Loggen fylles etter hvert som vakten varsler om kupp – prøv igjen")
        print("  om en dag eller to. Vil du ha med gamle varsler nå, importer dem")
        print("  fra Pushover: python -m scripts.kupp_import_pushover meldinger.txt")
        return

    grunnlag = f"logg ({len(logg)} biler) fra s3://{S3_BUCKET}/{LOGG_KEY}"
    poster = {fk: dict(rec, FinnKode=fk) for fk, rec in logg.items()}

    # Nyeste først (etter flagget-tidspunkt)
    ordnet = sorted(poster.values(), key=lambda r: r.get("flagget") or "", reverse=True)
    if args.limit > 0:
        ordnet = ordnet[:args.limit]

    tstamps = [r.get("flagget") for r in ordnet if r.get("flagget")]
    dager_span = None
    if tstamps:
        dager_span = (min(tstamps)[:10], max(tstamps)[:10])

    session = _make_session()
    rader = []
    print(f"[kupp_analyse] Sjekker status på {len(ordnet)} biler mot FINN ...")
    for i, rec in enumerate(ordnet, 1):
        fk = rec["FinnKode"]
        status, signal = _hent_status(session, fk)
        rec["status"] = status
        rec["dager"] = _dager_siden(rec.get("flagget")) if status == "solgt" else None
        rader.append(rec)
        if args.verbose:
            print(f"  [{i}/{len(ordnet)}] {fk}: {status} ({signal})")
        if i % 40 == 0:
            session = _make_session()  # rotér UA/økt for å unngå blokkering

    rapport(rader, grunnlag, dager_span)
    if args.csv:
        _skriv_csv(s3, rader)


if __name__ == "__main__":
    main()
