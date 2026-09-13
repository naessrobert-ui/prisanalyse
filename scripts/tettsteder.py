# -*- coding: utf-8 -*-
"""Norske tettsteder brukt til «kun byer»-filteret i værnormalkartet.

Listen dekker de største tettstedene (SSB sin tettstedsstatistikk, rundet
innbyggertall) pluss et utvalg mindre steder som folk typisk slår opp.
Koordinatene peker på sentrum; kartet kobler hvert tettsted til nærmeste
værstasjon innenfor en gitt radius.

``rang`` er plassering etter innbyggertall og brukes til å sortere bylista.
"""
from __future__ import annotations

from typing import TypedDict


class Tettsted(TypedDict):
    navn: str
    lat: float
    lon: float
    innbyggere: int
    rang: int


_RAA: list[tuple[str, float, float, int]] = [
    # navn, lat, lon, innbyggere
    ("Oslo", 59.9139, 10.7522, 1064235),
    ("Bergen", 60.3913, 5.3221, 268000),
    ("Stavanger/Sandnes", 58.9700, 5.7331, 232000),
    ("Trondheim", 63.4305, 10.3951, 194000),
    ("Drammen", 59.7440, 10.2045, 122000),
    ("Fredrikstad", 59.2181, 10.9298, 84000),
    ("Porsgrunn/Skien", 59.2096, 9.6090, 94000),
    ("Kristiansand", 58.1467, 7.9956, 67000),
    ("Ålesund", 62.4722, 6.1549, 55000),
    ("Tønsberg", 59.2674, 10.4076, 55000),
    ("Moss", 59.4340, 10.6577, 49000),
    ("Haugesund", 59.4136, 5.2680, 46000),
    ("Sandefjord", 59.1313, 10.2166, 46000),
    ("Arendal", 58.4616, 8.7724, 45000),
    ("Bodø", 67.2804, 14.4049, 43000),
    ("Tromsø", 69.6492, 18.9553, 42000),
    ("Hamar", 60.7945, 11.0680, 29000),
    ("Halden", 59.1230, 11.3875, 26000),
    ("Larvik", 59.0533, 10.0292, 25000),
    ("Askøy", 60.4667, 5.1833, 25000),
    ("Molde", 62.7375, 7.1591, 22000),
    ("Horten", 59.4172, 10.4832, 21000),
    ("Lillehammer", 61.1153, 10.4662, 21000),
    ("Harstad", 68.7986, 16.5415, 21000),
    ("Gjøvik", 60.7957, 10.6915, 21000),
    ("Kongsberg", 59.6686, 9.6503, 20000),
    ("Ski", 59.7195, 10.8355, 20000),
    ("Kristiansund", 63.1105, 7.7280, 19000),
    ("Jessheim", 60.1417, 11.1750, 19000),
    ("Narvik", 68.4385, 17.4272, 18000),
    ("Elverum", 60.8819, 11.5626, 17000),
    ("Alta", 69.9689, 23.2717, 16000),
    ("Steinkjer", 64.0148, 11.4954, 13000),
    ("Førde", 61.4523, 5.8570, 13000),
    ("Vennesla", 58.2739, 7.9750, 13000),
    ("Lillestrøm", 59.9556, 11.0493, 13000),
    ("Mysen", 59.5537, 11.3272, 13000),
    ("Nesodden", 59.8333, 10.6500, 12000),
    ("Bryne", 58.7353, 5.6473, 12000),
    ("Leirvik (Stord)", 59.7800, 5.5000, 12000),
    ("Egersund", 58.4515, 5.9993, 11000),
    ("Kongsvinger", 60.1903, 11.9962, 11000),
    ("Levanger", 63.7462, 11.2988, 10000),
    ("Grimstad", 58.3405, 8.5934, 10000),
    ("Mandal", 58.0294, 7.4609, 10000),
    ("Notodden", 59.5590, 9.2586, 9000),
    ("Florø", 61.5994, 5.0328, 9000),
    ("Brumunddal", 60.8811, 10.9410, 9000),
    ("Verdal", 63.7911, 11.4820, 9000),
    ("Sortland", 68.6961, 15.4130, 9000),
    ("Namsos", 64.4664, 11.4956, 8000),
    ("Voss", 60.6286, 6.4157, 8000),
    ("Ålgård", 58.7700, 5.8500, 8000),
    ("Kvinesdal/Flekkefjord", 58.2969, 6.6610, 8000),
    ("Stjørdal", 63.4700, 10.9192, 8000),
    ("Hønefoss", 60.1682, 10.2578, 15000),
    ("Sandnessjøen", 66.0217, 12.6314, 6000),
    ("Ulsteinvik", 62.3433, 5.8489, 6000),
    ("Åndalsnes", 62.5673, 7.6874, 5000),
    ("Odda", 60.0700, 6.5450, 5000),
    ("Rjukan", 59.8786, 8.5936, 3000),
    ("Lærdalsøyri", 61.0994, 7.4790, 2000),
    ("Geilo", 60.5340, 8.2058, 2500),
    ("Gol", 60.7000, 8.9500, 2500),
    ("Nesbyen", 60.5686, 9.1006, 2000),
    ("Beitostølen", 61.2472, 8.9083, 500),
    ("Røros", 62.5747, 11.3846, 4000),
    ("Oppdal", 62.5946, 9.6890, 4000),
    ("Dombås", 62.0750, 9.1250, 1200),
    ("Otta", 61.7722, 9.5389, 2000),
    ("Fagernes", 60.9861, 9.2306, 2000),
    ("Tynset", 62.2767, 10.7828, 3000),
    ("Finnsnes", 69.2306, 17.9781, 5000),
    ("Svolvær", 68.2342, 14.5681, 4000),
    ("Leknes", 68.1472, 13.6117, 3500),
    ("Mosjøen", 65.8371, 13.1922, 10000),
    ("Fauske", 67.2597, 15.3928, 6000),
    ("Hammerfest", 70.6634, 23.6821, 8000),
    ("Vadsø", 70.0744, 29.7487, 5000),
    ("Kirkenes", 69.7270, 30.0450, 3500),
    ("Karasjok", 69.4720, 25.5100, 1800),
    ("Kautokeino", 69.0117, 23.0417, 1500),
    ("Honningsvåg", 70.9821, 25.9704, 2400),
    ("Andenes", 69.3167, 16.1167, 2600),
    ("Brønnøysund", 65.4747, 12.2113, 5000),
    ("Ørsta", 62.1997, 6.1339, 7000),
    ("Sunndalsøra", 62.6747, 8.5626, 4000),
    ("Surnadal", 62.9739, 8.6486, 3000),
    ("Orkanger", 63.3000, 9.8500, 8000),
    ("Røyken/Slemmestad", 59.7833, 10.4667, 8000),
    ("Drøbak", 59.6633, 10.6300, 14000),
    ("Askim", 59.5833, 11.1667, 16000),
    ("Sarpsborg", 59.2839, 11.1097, 58000),
    ("Holmestrand", 59.4889, 10.3139, 8000),
    ("Stathelle/Langesund", 59.0000, 9.6833, 12000),
    ("Kragerø", 58.8686, 9.4114, 5000),
    ("Risør", 58.7200, 9.2333, 4500),
    ("Lyngdal", 58.1400, 7.0700, 6000),
    ("Farsund", 58.0950, 6.8000, 4000),
    ("Sauda", 59.6500, 6.3500, 4500),
    ("Sandeid/Ølen", 59.6000, 5.8000, 2000),
    ("Norheimsund", 60.3717, 6.1436, 2500),
    ("Osøyro", 60.1833, 5.4667, 9000),
    ("Knarvik", 60.5450, 5.2900, 6000),
    ("Sogndal", 61.2297, 7.1000, 5000),
    ("Årdalstangen", 61.2361, 7.7050, 4000),
    ("Stryn", 61.9111, 6.7167, 3000),
    ("Nordfjordeid", 61.9083, 5.9944, 3000),
    ("Måløy", 61.9367, 5.1133, 3000),
    ("Volda", 62.1478, 6.0703, 6000),
    ("Vestnes", 62.6297, 7.0942, 2500),
    ("Batnfjordsøra", 62.8667, 7.6667, 1000),
    ("Frøya/Sistranda", 63.7256, 8.7658, 1500),
    ("Rørvik", 64.8622, 11.2372, 3000),
    ("Grong", 64.4667, 12.3167, 1200),
    ("Mo i Rana", 66.3128, 14.1428, 20000),
    ("Ballangen", 68.3439, 16.8172, 1000),
    ("Bardufoss", 69.0667, 18.5333, 2500),
    ("Setermoen", 68.8608, 18.3419, 2500),
    ("Skjervøy", 70.0333, 20.9667, 2200),
    ("Lakselv", 70.0500, 24.9667, 2300),
    ("Båtsfjord", 70.6333, 29.7167, 2000),
    ("Vardø", 70.3706, 31.1107, 2000),
    ("Berlevåg", 70.8578, 29.0861, 1000),
    ("Longyearbyen", 78.2232, 15.6267, 2500),
]


def _bygg() -> list[Tettsted]:
    # Fjern duplikater (noen steder står oppført to ganger under ulike navn).
    sett: set[tuple[float, float]] = set()
    unike: list[tuple[str, float, float, int]] = []
    for navn, lat, lon, folk in _RAA:
        nokkel = (round(lat, 3), round(lon, 3))
        if nokkel in sett:
            continue
        sett.add(nokkel)
        unike.append((navn, lat, lon, folk))

    unike.sort(key=lambda r: -r[3])
    return [
        Tettsted(navn=navn, lat=lat, lon=lon, innbyggere=folk, rang=i)
        for i, (navn, lat, lon, folk) in enumerate(unike, start=1)
    ]


TETTSTEDER: list[Tettsted] = _bygg()
