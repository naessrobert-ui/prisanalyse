# -*- coding: utf-8 -*-
"""
fylke_registry.py
=================
Delt oppslag for norske fylker og NACE-næringshovedområder.

Bakgrunn
--------
Regnskaps- og entity-dataene lagrer geografi som *kommunenummer* (fire siffer),
ikke fylke. De to første sifrene i kommunenummeret er fylkesnummeret, så fylke
utledes ved å matche på prefiks.

På grunn av fylkesreformene (2020 og 2024) kan eldre data fortsatt bruke gamle
kommunenummer. Derfor kan hvert fylke ha flere gyldige prefikser. Vestland (46)
ble f.eks. slått sammen fra Hordaland (12) og Sogn og Fjordane (14) i 2020, så
alle tre prefiksene skal telle som Vestland.

Kun *entydige* eldre prefikser tas med. De kortlivede sammenslåtte fylkene
Viken (30), Vestfold og Telemark (38) og Troms og Finnmark (54) er utelatt fordi
prefikset deres ikke peker entydig til ett av dagens fylker.
"""

from __future__ import annotations

import re
from typing import Any


# ---------------------------------------------------------------------------
# Fylkesregister
#   nummer           – dagens fylkesnummer (to siffer)
#   navn             – offisielt fylkesnavn
#   kommune_prefikser – to-sifrede prefikser på kommunenummer som hører til
#                       fylket (dagens + entydige historiske)
#   aliaser          – ekstra søkeord (normaliseres på lik linje med navnet)
# ---------------------------------------------------------------------------
FYLKER: list[dict[str, Any]] = [
    {"nummer": "03", "navn": "Oslo", "kommune_prefikser": ["03"], "aliaser": []},
    {"nummer": "11", "navn": "Rogaland", "kommune_prefikser": ["11"], "aliaser": []},
    {"nummer": "15", "navn": "Møre og Romsdal", "kommune_prefikser": ["15"], "aliaser": []},
    {"nummer": "18", "navn": "Nordland", "kommune_prefikser": ["18"], "aliaser": []},
    {"nummer": "31", "navn": "Østfold", "kommune_prefikser": ["31", "01"], "aliaser": []},
    {"nummer": "32", "navn": "Akershus", "kommune_prefikser": ["32", "02"], "aliaser": []},
    {"nummer": "33", "navn": "Buskerud", "kommune_prefikser": ["33", "06"], "aliaser": []},
    {"nummer": "34", "navn": "Innlandet", "kommune_prefikser": ["34", "04", "05"], "aliaser": ["hedmark", "oppland"]},
    {"nummer": "39", "navn": "Vestfold", "kommune_prefikser": ["39", "07"], "aliaser": []},
    {"nummer": "40", "navn": "Telemark", "kommune_prefikser": ["40", "08"], "aliaser": []},
    {"nummer": "42", "navn": "Agder", "kommune_prefikser": ["42", "09", "10"], "aliaser": ["austagder", "vestagder"]},
    {"nummer": "46", "navn": "Vestland", "kommune_prefikser": ["46", "12", "14"], "aliaser": ["hordaland", "sognogfjordane"]},
    {"nummer": "50", "navn": "Trøndelag", "kommune_prefikser": ["50", "16", "17"], "aliaser": ["sortrondelag", "nordtrondelag"]},
    {"nummer": "55", "navn": "Troms", "kommune_prefikser": ["55", "19"], "aliaser": []},
    {"nummer": "56", "navn": "Finnmark", "kommune_prefikser": ["56", "20"], "aliaser": []},
]


def _normaliser(value: Any) -> str:
    """Små bokstaver, behold bokstaver/tall (inkl. æøå), fjern resten."""
    return re.sub(r"[^0-9a-zæøå]+", "", str(value or "").strip().lower())


# Forhåndsberegnede alias-sett (normalisert navn + eksplisitte aliaser).
for _f in FYLKER:
    _f["_aliaser_norm"] = {_normaliser(_f["navn"]), *[_normaliser(a) for a in _f["aliaser"]]}


def resolve_fylke(value: Any) -> dict[str, Any] | None:
    """
    Slår opp et fylke ut fra fritekst.

    Godtar:
      - fylkesnummer ("46", "12", "14" → Vestland)
      - kommunenummer ("4601" → Vestland, via prefiks)
      - navn/alias ("Vestland", "Hordaland")

    Returnerer et fylke-dict (uten interne felt) eller None.
    """
    raw = str(value or "").strip()
    if not raw:
        return None

    normalisert = _normaliser(raw)
    for fylke in FYLKER:
        if normalisert and normalisert in fylke["_aliaser_norm"]:
            return _offentlig(fylke)

    digits = re.sub(r"\D", "", raw)
    if len(digits) in (1, 2):
        to_siffer = digits.zfill(2)
        for fylke in FYLKER:
            if fylke["nummer"] == to_siffer or to_siffer in fylke["kommune_prefikser"]:
                return _offentlig(fylke)
    elif len(digits) >= 4:
        to_siffer = digits[:2]
        for fylke in FYLKER:
            if to_siffer in fylke["kommune_prefikser"]:
                return _offentlig(fylke)

    return None


def _offentlig(fylke: dict[str, Any]) -> dict[str, Any]:
    return {
        "nummer": fylke["nummer"],
        "navn": fylke["navn"],
        "kommune_prefikser": list(fylke["kommune_prefikser"]),
    }


def list_fylker() -> list[dict[str, Any]]:
    """Alle fylker sortert på fylkesnummer – egnet for nedtrekksmenyer."""
    return [_offentlig(f) for f in sorted(FYLKER, key=lambda x: x["nummer"])]


# ---------------------------------------------------------------------------
# NACE-næringshovedområder (SSB-seksjoner A–U)
#   Hver seksjon dekker et intervall av to-sifrede næringsdivisjoner.
# ---------------------------------------------------------------------------
_NACE_SEKSJONER: list[tuple[str, str, int, int]] = [
    ("A", "Jordbruk, skogbruk og fiske", 1, 3),
    ("B", "Bergverksdrift og utvinning", 5, 9),
    ("C", "Industri", 10, 33),
    ("D", "Elektrisitets-, gass-, damp- og varmtvannsforsyning", 35, 35),
    ("E", "Vannforsyning, avløps- og renovasjonsvirksomhet", 36, 39),
    ("F", "Bygge- og anleggsvirksomhet", 41, 43),
    ("G", "Varehandel; reparasjon av motorvogner", 45, 47),
    ("H", "Transport og lagring", 49, 53),
    ("I", "Overnattings- og serveringsvirksomhet", 55, 56),
    ("J", "Informasjon og kommunikasjon", 58, 63),
    ("K", "Finansierings- og forsikringsvirksomhet", 64, 66),
    ("L", "Omsetning og drift av fast eiendom", 68, 68),
    ("M", "Faglig, vitenskapelig og teknisk tjenesteyting", 69, 75),
    ("N", "Forretningsmessig tjenesteyting", 77, 82),
    ("O", "Offentlig administrasjon og forsvar; trygdeordninger", 84, 84),
    ("P", "Undervisning", 85, 85),
    ("Q", "Helse- og sosialtjenester", 86, 88),
    ("R", "Kulturell virksomhet, underholdning og fritid", 90, 93),
    ("S", "Annen tjenesteyting", 94, 96),
    ("T", "Lønnet arbeid i private husholdninger", 97, 98),
    ("U", "Internasjonale organisasjoner og organer", 99, 99),
]

_UOPPGITT = ("?", "Uoppgitt / ukjent næring")


def nace_seksjon(divisjon: Any) -> tuple[str, str]:
    """
    Mapper en to-sifret NACE-divisjon (f.eks. "47") til (seksjonsbokstav, navn).

    Tomme eller ukjente verdier gir ("?", "Uoppgitt / ukjent næring").
    """
    digits = re.sub(r"\D", "", str(divisjon or ""))
    if not digits:
        return _UOPPGITT
    try:
        nr = int(digits[:2])
    except ValueError:
        return _UOPPGITT
    for bokstav, navn, lav, hoy in _NACE_SEKSJONER:
        if lav <= nr <= hoy:
            return bokstav, navn
    return _UOPPGITT
