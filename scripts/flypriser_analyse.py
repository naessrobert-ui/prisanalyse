# -*- coding: utf-8 -*-
"""Analyse av flyprishistorikk – investor-vinklede signaler for Norwegian (DY).

Rene funksjoner (ingen web/IO utover å lese CSV) som bygger:
  * Norwegian-prisindeks vs. markedet over tid
  * Avviksdeteksjon – ruter der prisen plutselig er unormalt høy/lav
  * Konkurrent-gap – Norwegians pris vs. billigste konkurrent per rute
  * Booking-kurve – hvordan billigste pris utvikler seg mot avreise

Alt tåler sparsom historikk (få snapshots) og degraderer pent.
"""

from __future__ import annotations

import csv
import statistics
from collections import defaultdict
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
HISTORIKK_CSV = DATA_DIR / "flypriser_historikk.csv"

NORWEGIAN_KODER = {"DY", "D8"}
# Andel avvik fra basislinjen som flagges
AVVIK_TERSKEL = 0.12
# Minst antall tidligere observasjoner for å beregne en basislinje
MIN_BASIS_PUNKTER = 3


def _f(verdi) -> float | None:
    try:
        return float(verdi)
    except (TypeError, ValueError):
        return None


def les_historikk(sti: Path = HISTORIKK_CSV) -> list[dict]:
    if not sti.exists():
        return []
    with sti.open(encoding="utf-8") as f:
        rader = []
        for r in csv.DictReader(f):
            pris = _f(r.get("pris"))
            if pris is None:
                continue
            r["pris_f"] = pris
            rader.append(r)
        return rader


def _rute(r: dict) -> str:
    return f"{r['origin']}-{r['destinasjon']}"


def _rute_navn(r: dict) -> str:
    return f"{r.get('origin_navn', r['origin'])} → {r.get('destinasjon_navn', r['destinasjon'])}"


def _samle_per(rader: list[dict], bare_norwegian: bool | None = None):
    """Samle ALLE priser per (dato, rute) som lister – filtrert på selskap.

    bare_norwegian=True  -> kun DY/D8
    bare_norwegian=False -> kun konkurrenter
    bare_norwegian=None  -> alle selskap (markedet)
    """
    ut: dict[str, dict[str, list]] = defaultdict(lambda: defaultdict(list))
    navn: dict[str, str] = {}
    for r in rader:
        er_dy = r.get("selskap") in NORWEGIAN_KODER
        if bare_norwegian is True and not er_dy:
            continue
        if bare_norwegian is False and er_dy:
            continue
        rute = _rute(r)
        navn[rute] = _rute_navn(r)
        ut[r["hentet_dato"]][rute].append(r["pris_f"])
    return ut, navn


def _reduser(samlet: dict[str, dict[str, list]], aggfun) -> dict[str, dict[str, float]]:
    """Reduser lister per (dato, rute) til én verdi med aggfun (min/median/…)."""
    ut: dict[str, dict[str, float]] = defaultdict(dict)
    for dato, ruter in samlet.items():
        for rute, priser in ruter.items():
            if priser:
                ut[dato][rute] = aggfun(priser)
    return ut


def _billigst_per(rader: list[dict], bare_norwegian: bool | None = None):
    """Billigste pris per (dato, rute)."""
    samlet, navn = _samle_per(rader, bare_norwegian)
    return _reduser(samlet, min), navn


def _median_per(rader: list[dict], bare_norwegian: bool | None = None):
    """Medianpris per (dato, rute) – 'typisk' prisnivå (yield-proxy)."""
    samlet, navn = _samle_per(rader, bare_norwegian)
    return _reduser(samlet, statistics.median), navn


def prisindeks(rader: list[dict]) -> dict:
    """Prisindeks (median av pris-relativer, basis=100 ved første observasjon).

    Egen indeks for Norwegian og for markedet. Robust mot at rute-utvalget
    endrer seg mellom datoer, siden vi bruker per-rute-relativer.
    """
    def bygg(per_dato: dict[str, dict[str, float]]) -> list[dict]:
        datoer = sorted(per_dato)
        basis: dict[str, float] = {}
        for dato in datoer:
            for rute, pris in per_dato[dato].items():
                basis.setdefault(rute, pris)  # første observerte pris = basis
        serie = []
        for dato in datoer:
            relativer = [
                per_dato[dato][rute] / basis[rute]
                for rute in per_dato[dato]
                if basis.get(rute)
            ]
            if not relativer:
                continue
            serie.append({
                "dato": dato,
                "indeks": round(100 * statistics.median(relativer), 1),
                "antall_ruter": len(relativer),
            })
        return serie

    dy_min, _ = _billigst_per(rader, bare_norwegian=True)
    mk_min, _ = _billigst_per(rader, bare_norwegian=None)
    dy_med, _ = _median_per(rader, bare_norwegian=True)
    mk_med, _ = _median_per(rader, bare_norwegian=None)
    return {
        "norwegian": bygg(dy_min),
        "norwegian_median": bygg(dy_med),
        "marked": bygg(mk_min),
        "marked_median": bygg(mk_med),
    }


def dispersjon(rader: list[dict]) -> dict:
    """Prisspredning for Norwegian: gap mellom median og laveste pris.

    Lite gap = de billigste billettene er 'spist opp' (prispress/høy etterspørsel).
    Stort gap = mye billig kapasitet igjen. Rapporterer aggregat for siste dato
    samt en tidsserie av median-gap i prosent.
    """
    dy_min, navn = _billigst_per(rader, bare_norwegian=True)
    dy_med, _ = _median_per(rader, bare_norwegian=True)
    datoer = sorted(dy_min)
    if not datoer:
        return {"serie": [], "siste": None, "ruter": []}

    serie = []
    for dato in datoer:
        gaps = [
            dy_med[dato][rute] / dy_min[dato][rute] - 1
            for rute in dy_min[dato]
            if rute in dy_med[dato] and dy_min[dato][rute] > 0
        ]
        if gaps:
            serie.append({"dato": dato, "gap_pst": round(100 * statistics.median(gaps), 1),
                          "antall_ruter": len(gaps)})

    siste = datoer[-1]
    ruter = []
    for rute, mn in dy_min[siste].items():
        md = dy_med[siste].get(rute)
        if md is None or mn <= 0:
            continue
        ruter.append({"rute": rute, "navn": navn.get(rute, rute), "laveste": round(mn),
                      "median": round(md), "gap_pst": round(100 * (md / mn - 1), 1)})
    ruter.sort(key=lambda x: x["gap_pst"])
    return {"serie": serie, "siste": serie[-1] if serie else None, "ruter": ruter}


def avvik(rader: list[dict], terskel: float = AVVIK_TERSKEL) -> list[dict]:
    """Ruter der Norwegians billigste pris på siste dato avviker unormalt fra
    basislinjen (median av tidligere observasjoner for samme rute)."""
    per_dato, navn = _billigst_per(rader, bare_norwegian=True)
    datoer = sorted(per_dato)
    if len(datoer) < MIN_BASIS_PUNKTER + 1:
        return []
    siste = datoer[-1]
    tidligere = datoer[:-1]
    treff = []
    for rute, pris in per_dato[siste].items():
        historikk = [per_dato[d][rute] for d in tidligere if rute in per_dato[d]]
        if len(historikk) < MIN_BASIS_PUNKTER:
            continue
        basis = statistics.median(historikk)
        if basis <= 0:
            continue
        endring = pris / basis - 1
        if abs(endring) >= terskel:
            treff.append({
                "rute": rute,
                "navn": navn.get(rute, rute),
                "pris": round(pris),
                "basis": round(basis),
                "endring_pst": round(100 * endring, 1),
                "retning": "opp" if endring > 0 else "ned",
            })
    return sorted(treff, key=lambda x: abs(x["endring_pst"]), reverse=True)


def konkurrent_gap(rader: list[dict]) -> list[dict]:
    """Per rute på siste dato: Norwegians billigste vs. billigste konkurrent."""
    dy, navn = _billigst_per(rader, bare_norwegian=True)
    komp, _ = _billigst_per(rader, bare_norwegian=False)
    if not dy:
        return []
    siste = max(dy)
    # finn hvilket konkurrentselskap som er billigst (for etikett)
    beste_komp_selskap: dict[str, str] = {}
    for r in rader:
        if r["hentet_dato"] != siste or r.get("selskap") in NORWEGIAN_KODER:
            continue
        rute = _rute(r)
        if rute not in komp.get(siste, {}):
            continue
        if r["pris_f"] == komp[siste][rute]:
            beste_komp_selskap[rute] = r.get("selskap_navn") or r.get("selskap") or "?"
    ut = []
    for rute, dy_pris in dy.get(siste, {}).items():
        k = komp.get(siste, {}).get(rute)
        if k is None:
            continue
        ut.append({
            "rute": rute,
            "navn": navn.get(rute, rute),
            "norwegian": round(dy_pris),
            "konkurrent": round(k),
            "konkurrent_navn": beste_komp_selskap.get(rute, "konkurrent"),
            "gap_pst": round(100 * (dy_pris / k - 1), 1),
        })
    return sorted(ut, key=lambda x: x["gap_pst"])


def booking_kurve(rader: list[dict], ruter: list[str] | None = None, maks_ruter: int = 8) -> list[dict]:
    """For hver rute: billigste Norwegian-pris per hentedato (utvikling mot avreise)."""
    per_dato, navn = _billigst_per(rader, bare_norwegian=True)
    datoer = sorted(per_dato)
    # ruter med flest observasjoner (mest informative kurver)
    antall: dict[str, int] = defaultdict(int)
    for dato in datoer:
        for rute in per_dato[dato]:
            antall[rute] += 1
    valgte = ruter or [r for r, _ in sorted(antall.items(), key=lambda x: x[1], reverse=True)[:maks_ruter]]
    ut = []
    for rute in valgte:
        punkter = [{"dato": d, "pris": round(per_dato[d][rute])} for d in datoer if rute in per_dato[d]]
        if punkter:
            ut.append({"rute": rute, "navn": navn.get(rute, rute), "punkter": punkter})
    return ut


def sammendrag(rader: list[dict]) -> dict:
    """Nøkkeltall for toppen av dashbordet."""
    idx = prisindeks(rader)
    dy_serie = idx["norwegian"]
    mk_serie = idx["marked"]
    gap = konkurrent_gap(rader)
    dy_per_dato, _ = _billigst_per(rader, bare_norwegian=True)
    siste = max(dy_per_dato) if dy_per_dato else None

    def endring(serie):
        if len(serie) < 2:
            return None
        return round(serie[-1]["indeks"] - serie[-2]["indeks"], 1)

    dy_med_serie = idx["norwegian_median"]
    disp = dispersjon(rader)
    dy_billigere = sum(1 for g in gap if g["gap_pst"] <= 0)
    median_dy = None
    if siste and dy_per_dato[siste]:
        median_dy = round(statistics.median(dy_per_dato[siste].values()))

    disp_gap = disp["siste"]["gap_pst"] if disp["siste"] else None
    disp_endring = None
    if len(disp["serie"]) >= 2:
        disp_endring = round(disp["serie"][-1]["gap_pst"] - disp["serie"][-2]["gap_pst"], 1)

    return {
        "sist_hentet": siste,
        "antall_datoer": len(dy_serie),
        "norwegian_indeks": dy_serie[-1]["indeks"] if dy_serie else None,
        "norwegian_indeks_endring": endring(dy_serie),
        "norwegian_median_indeks": dy_med_serie[-1]["indeks"] if dy_med_serie else None,
        "norwegian_median_indeks_endring": endring(dy_med_serie),
        "marked_indeks": mk_serie[-1]["indeks"] if mk_serie else None,
        "marked_indeks_endring": endring(mk_serie),
        "median_norwegian_pris": median_dy,
        "dispersjon_gap_pst": disp_gap,
        "dispersjon_endring": disp_endring,
        "ruter_dy_billigst": dy_billigere,
        "ruter_med_gap": len(gap),
    }


def full_analyse(sti: Path = HISTORIKK_CSV) -> dict:
    rader = les_historikk(sti)
    if not rader:
        return {"ok": False, "error": "Ingen prisdata ennå. Kjør «python -m scripts.flypriser_bergen»."}
    return {
        "ok": True,
        "sammendrag": sammendrag(rader),
        "indeks": prisindeks(rader),
        "avvik": avvik(rader),
        "konkurrent_gap": konkurrent_gap(rader),
        "booking_kurve": booking_kurve(rader),
        "dispersjon": dispersjon(rader),
    }
