# -*- coding: utf-8 -*-
"""Fint-score: hvor fint været er en gitt time, 0-100, for ulike aktiviteter.

Brukes av værsidene på blirdetfint.no. Én felles regel, slik at svarlinjen
("Fint kl. 11-15"), kortene og fargestripene aldri motsier hverandre.

Skalaen (sol-skala i designet):
    80+  Strålende   65-79 Fint   50-64 Greit   30-49 Grått   under 30 Unngå

Poengene trekkes fra 100 for nedbør, temperatur utenfor et behagelig spenn,
vind og vindkast, skydekke (eller mørke) og torden. Profilene vekter dette ulikt.
Profilen "generelt" er årstidsjustert: en tørr og solrik novemberdag kan være
strålende selv om det bare er fem grader.
"""

from __future__ import annotations

import math
from typing import Callable, Optional

#: Nedre behagelige (følte) temperatur per måned for profilen "generelt".
SESONG_LO = [0, 0, 3, 7, 11, 14, 16, 16, 12, 8, 3, 0]

PROFILER: dict[str, dict] = {
    "generelt": {"navn": "Generelt", "temp": lambda m: (SESONG_LO[m], SESONG_LO[m] + 12),
                 "w_temp": 1.6, "w_regn": 1.0, "v0": 6, "w_vind": 3.0, "k0": 14, "w_sol": 38},
    "lop": {"navn": "Løping", "temp": lambda m: (4, 16),
            "w_temp": 2.0, "w_regn": 0.7, "v0": 7, "w_vind": 2.5, "k0": 15, "w_sol": 10},
    "tur": {"navn": "Tur og hytte", "temp": lambda m: (6, 20),
            "w_temp": 1.5, "w_regn": 1.1, "v0": 7, "w_vind": 3.0, "k0": 15, "w_sol": 30},
    "sykkel": {"navn": "Sykkel", "temp": lambda m: (12, 24),
               "w_temp": 2.0, "w_regn": 1.2, "v0": 5, "w_vind": 4.0, "k0": 12, "w_sol": 14},
    "strand": {"navn": "Strand og sol", "temp": lambda m: (24, 31),
               "w_temp": 3.0, "w_regn": 1.2, "v0": 6, "w_vind": 3.0, "k0": 12, "w_sol": 40},
}

#: Skydekke (prosent) anslått fra Yr-symbolet når vi ikke har tallet.
_SKY = {"clearsky": 0, "fair": 20, "partlycloudy": 55, "cloudy": 100, "fog": 100}

NIVAAER = [(80, "Strålende"), (65, "Fint"), (50, "Greit"), (30, "Grått"), (0, "Unngå")]


def skydekke(symbol: Optional[str]) -> float:
    """Skydekke i prosent ut fra Yr-symbolkoden (nedbør betyr overskyet)."""
    if not symbol:
        return 60.0
    return float(_SKY.get(symbol.split("_")[0], 100))


def torden(symbol: Optional[str]) -> bool:
    return bool(symbol) and "thunder" in symbol


def nivaa(score: int) -> str:
    return next(navn for grense, navn in NIVAAER if score >= grense)


def score(*, temp: float, foles: Optional[float], regn: float, vind: float, kast: Optional[float],
          sky: float, lyst: bool, tordenvaer: bool, maaned: int,
          profil: str = "generelt") -> tuple[int, Optional[str]]:
    """Fint-score 0-100 og hovedgrunnen til trekket (eller None om lite trekk).

    maaned er 1-12. Grunnene er "regn", "vind", "kaldt", "varmt", "sky" og "torden".
    """
    p = PROFILER[profil]
    trekk: dict[str, float] = {}
    mm = max(0.0, regn or 0.0)
    trekk["regn"] = min(70.0, (10 + 28 * math.sqrt(mm)) * p["w_regn"]) if mm >= 0.1 else 0.0
    lo, hi = p["temp"](maaned - 1)
    t = foles if foles is not None else temp
    if t < lo:
        trekk["kaldt"] = min(45.0, (lo - t) * p["w_temp"])
    elif t > hi:
        trekk["varmt"] = min(45.0, (t - hi) * p["w_temp"])
    # Vinden trekker mer jo sterkere den er (kuling skal aldri bli "fint").
    over = max(0.0, (vind or 0) - p["v0"])
    trekk["vind"] = min(60.0, over * p["w_vind"] * (1 + over / 10) + max(0.0, (kast or 0) - p["k0"]) * 1.5)
    trekk["sky"] = (sky / 100.0) * p["w_sol"] if lyst else p["w_sol"] * 0.35
    if mm >= 0.1:
        # Når det regner, er skyene en del av regnet: da er grunnen "regn".
        trekk["regn"] += trekk.pop("sky")
    if tordenvaer:
        trekk["torden"] = 45.0
    grunn, stoerst = max(trekk.items(), key=lambda kv: kv[1])
    verdi = int(round(max(0.0, min(100.0, 100 - sum(trekk.values())))))
    return verdi, (grunn if stoerst > 8 else None)


def merk_timer(rows: list[dict], lyst_fn: Callable[[dict], bool], foles_fn: Callable[[dict], Optional[float]]) -> None:
    """Legger fint-score for alle profiler på hver time (rader med temp og symbol).

    Feltene som legges til: lyst (bool), foles, fint {profil: score},
    fint_grunn {profil: grunn}. Rader uten temperatur røres ikke.
    """
    from datetime import datetime

    for h in rows:
        if h.get("temp") is None or not h.get("time"):
            continue
        try:
            maaned = datetime.fromisoformat(h["time"]).month
        except ValueError:
            continue
        lyst = bool(lyst_fn(h))
        foles = foles_fn(h)
        felles = dict(temp=h["temp"], foles=foles, regn=h.get("rain") or 0.0, vind=h.get("wind") or 0.0,
                      kast=h.get("gust"), sky=skydekke(h.get("symbol")), lyst=lyst,
                      tordenvaer=torden(h.get("symbol")), maaned=maaned)
        fint, grunn = {}, {}
        for navn in PROFILER:
            fint[navn], grunn[navn] = score(profil=navn, **felles)
        h["lyst"] = lyst
        h["foles"] = None if foles is None else round(foles, 1)
        h["fint"] = fint
        h["fint_grunn"] = grunn
