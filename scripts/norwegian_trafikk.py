# -*- coding: utf-8 -*-
"""Tracker for Norwegians offisielle månedlige trafikktall (OSE: NAS).

Dette er den *autoritative* booking-indikatoren – i motsetning til prisdataene
(som er en proxy). Norwegian publiserer hver måned en «trafikkrapport» til Oslo
Børs med passasjerer, ASK, RPK, kabinfaktor og vekst.

Vi lagrer tallene i data/norwegian_trafikk.csv. Legg inn én rad per måned fra
den offisielle rapporten (se docs/flypriser_norwegian.md for hvor du finner
den). Modulen beregner endring måned-over-måned og år-over-år.

VIKTIG: Fyll kun inn *verifiserte* tall fra den offisielle rapporten. Modulen
finner ikke opp data – tom fil gir tomt panel i dashbordet.
"""

from __future__ import annotations

import csv
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
TRAFIKK_CSV = DATA_DIR / "norwegian_trafikk.csv"

FELT = [
    "maaned",           # YYYY-MM
    "passasjerer",      # antall reisende
    "ask_mill",         # Available Seat Kilometres (millioner)
    "rpk_mill",         # Revenue Passenger Kilometres (millioner)
    "kabinfaktor_pst",  # load factor, prosent
    "punktlighet_pst",  # regularitet/punktlighet, prosent (valgfritt)
    "kilde",            # URL eller referanse til den offisielle rapporten
]

NUM = ["passasjerer", "ask_mill", "rpk_mill", "kabinfaktor_pst", "punktlighet_pst"]


def _num(v):
    if v is None or str(v).strip() == "":
        return None
    try:
        return float(str(v).replace(" ", "").replace(" ", "").replace(",", "."))
    except ValueError:
        return None


def sikre_fil(sti: Path = TRAFIKK_CSV) -> None:
    """Opprett CSV med header hvis den ikke finnes (ingen oppdiktede tall)."""
    if sti.exists():
        return
    sti.parent.mkdir(parents=True, exist_ok=True)
    with sti.open("w", newline="", encoding="utf-8") as f:
        csv.DictWriter(f, fieldnames=FELT).writeheader()


def les_trafikk(sti: Path = TRAFIKK_CSV) -> list[dict]:
    if not sti.exists():
        return []
    with sti.open(encoding="utf-8") as f:
        rader = []
        for r in csv.DictReader(f):
            if not (r.get("maaned") or "").strip():
                continue
            rad = {"maaned": r["maaned"].strip(), "kilde": r.get("kilde", "")}
            for k in NUM:
                rad[k] = _num(r.get(k))
            rader.append(rad)
        return sorted(rader, key=lambda x: x["maaned"])


def _pst_endring(ny, gammel):
    if ny is None or gammel in (None, 0):
        return None
    return round(100 * (ny / gammel - 1), 1)


def analyse(sti: Path = TRAFIKK_CSV) -> dict:
    """Siste måned + endringer MoM og YoY for nøkkelmetrikker."""
    rader = les_trafikk(sti)
    if not rader:
        return {"ok": False, "rader": [], "error": "Ingen trafikktall lagt inn ennå."}

    per_maaned = {r["maaned"]: r for r in rader}
    siste = rader[-1]
    m = siste["maaned"]
    aar, mnd = m.split("-")
    forrige_m = rader[-2]["maaned"] if len(rader) >= 2 else None
    yoy_m = f"{int(aar) - 1}-{mnd}"

    def blokk(felt):
        ny = siste.get(felt)
        return {
            "verdi": ny,
            "mom_pst": _pst_endring(ny, per_maaned.get(forrige_m, {}).get(felt)) if forrige_m else None,
            "yoy_pst": _pst_endring(ny, per_maaned.get(yoy_m, {}).get(felt)) if yoy_m in per_maaned else None,
        }

    return {
        "ok": True,
        "siste_maaned": m,
        "kilde": siste.get("kilde", ""),
        "antall_maaneder": len(rader),
        "metrikker": {felt: blokk(felt) for felt in NUM},
        "serie": [
            {"maaned": r["maaned"], **{k: r.get(k) for k in NUM}}
            for r in rader
        ],
    }
