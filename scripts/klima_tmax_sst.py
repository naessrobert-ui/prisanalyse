"""Klimahjelpedata: gjennomsnittlig daglig makstemp og sjøtemperatur.

NASA POWER T2M er døgnmiddelet (alle 24 timer). For feriepanlegning er
gjennomsnittlig daglig makstemp (typisk dagstemp) langt mer informativ.
Fordi NASA POWER T2M_MAX er *absolutt* ekstremverdi over 30 år (ikke
gjennomsnittet av daglige makser), bruker vi et regionbasert offset:
    tmax_avg ≈ T2M_mean + offset[region][month-1]

Offsettene er kalibrert mot WMO/Wikipedia klimanormmaler for representative
stasjoner i hvert klimaregion.

Sjøtemperaturer (SST) er månedlige normaler fra NOAA World Ocean Atlas /
Copernicus Marine Service, tilordnet det nærmeste havbasseng per destinasjon.
"""
from __future__ import annotations

# ─── REGIONBASERTE OFFSETS (Tmax_avg - T2M_mean) ────────────────────────────
# 12 verdier = jan–des
# Kilde: kalibrering mot WMO-stasjonsdata for representative byer per region.

_OFFSETS: dict[str, list[float]] = {
    # Middelhav, kyst (Nice, Barcelona, Valencia, Napoli …)
    "med_coast":   [5.0, 5.0, 5.5, 6.0, 6.0, 6.0, 6.5, 6.5, 6.0, 5.5, 5.0, 4.5],
    # Middelhav, øyer (Mallorca, Sardinia, Sicilia, Malta, Korfu …)
    "med_island":  [5.0, 5.5, 6.0, 7.0, 7.0, 7.0, 7.0, 7.0, 7.0, 6.0, 5.5, 5.0],
    # Egeisk hav (Rhodos, Kos, Santorini, Bodrum …)
    "aegean":      [6.0, 6.0, 6.5, 7.0, 7.0, 7.0, 8.0, 8.0, 7.5, 7.0, 6.5, 6.0],
    # Øst-Middelhav (Kypros, Antalya, Alanya, Kreta, Tel Aviv …)
    "med_east":    [7.0, 7.0, 7.5, 8.0, 8.0, 8.0, 8.5, 8.5, 8.0, 7.5, 7.0, 6.5],
    # Adriatisk hav (Dubrovnik, Split, Zadar, Pula, Tivat …)
    "adriatic":    [5.0, 5.5, 6.0, 7.0, 8.0, 8.5, 9.0, 8.5, 7.5, 6.5, 5.5, 5.0],
    # Atlanterhavs-Iberia sør (Málaga, Faro, Algarve)
    "atlantic_s":  [4.5, 4.5, 5.0, 5.5, 5.5, 5.5, 5.5, 5.5, 5.0, 5.0, 4.5, 4.0],
    # Atlanterhavs-Iberia nord (Lisboa)
    "atlantic_n_ib": [4.0, 4.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 4.0, 4.0],
    # Kanariøyene + Madeira
    "canaries":    [3.5, 3.5, 4.0, 4.0, 4.0, 4.0, 4.0, 4.0, 4.0, 4.0, 3.5, 3.5],
    # Nord-Atlanteren (UK, Irland, Biskaya, Frankrike)
    "atlantic_n":  [3.0, 3.5, 4.0, 5.0, 5.5, 6.0, 6.0, 6.0, 5.5, 4.5, 3.5, 3.0],
    # Skandinavia / Nordsøen kyst
    "nordic":      [2.5, 3.0, 4.0, 5.5, 7.0, 8.0, 8.0, 7.5, 6.0, 4.5, 3.0, 2.5],
    # Baltikum
    "baltic_reg":  [2.0, 3.0, 5.0, 8.0, 10.0, 10.5, 10.5, 10.0, 7.0, 5.0, 2.5, 2.0],
    # Kontinent (Sentral-/Øst-Europa)
    "continental": [3.5, 5.0, 7.0, 9.5, 11.0, 11.5, 11.5, 11.0, 8.5, 6.5, 4.0, 3.5],
    # Alper
    "alpine":      [4.0, 5.0, 6.5, 8.0, 8.5, 8.5, 9.0, 9.0, 7.5, 6.0, 4.5, 4.0],
    # Vest-Balkan + Svartehavet innland
    "balkan":      [4.0, 5.0, 7.0, 9.0, 10.5, 11.0, 12.0, 11.5, 9.0, 7.0, 4.5, 4.0],
    # Svartehavet kyst (Varna, Burgas)
    "black_sea":   [4.0, 5.0, 6.5, 8.0, 9.5, 10.0, 11.0, 10.5, 8.0, 6.5, 4.5, 4.0],
    # Tørt/varmt (Egypt, Rødehavet, Gulfen)
    "hot_dry":     [8.0, 8.5, 9.0, 9.0, 9.0, 8.5, 8.0, 7.5, 7.5, 8.0, 8.0, 7.5],
}

# Destinasjon → klimaregion for tmax-offset
DEST_KLIMAREGION: dict[str, str] = {
    # Norge
    "oslo": "nordic", "bergen": "nordic", "stavanger": "nordic",
    "kristiansand": "nordic", "trondheim": "nordic", "alesund": "nordic",
    "molde": "nordic", "haugesund": "nordic", "sandefjord": "nordic",
    "bodo": "nordic", "tromso": "nordic", "harstad": "nordic",
    "andenes": "nordic", "alta": "nordic", "kirkenes": "nordic",
    "longyearbyen": "nordic", "geiranger": "nordic", "flam": "nordic",
    "kvamskogen": "nordic", "hovden": "nordic",
    # Norden
    "stockholm": "nordic", "goteborg": "nordic", "malmo": "nordic",
    "visby": "baltic_reg", "umea": "nordic", "lulea": "nordic",
    "kiruna": "nordic", "vaxjo": "nordic",
    "kobenhavn": "nordic", "aalborg": "nordic", "aarhus": "nordic",
    "billund": "nordic",
    "helsinki": "nordic", "oulu": "nordic", "rovaniemi": "nordic",
    "reykjavik": "nordic",
    # UK & Irland
    "london": "atlantic_n", "edinburgh": "atlantic_n",
    "manchester": "atlantic_n", "dublin": "atlantic_n",
    # Iberia
    "madrid": "continental", "barcelona": "med_coast",
    "valencia": "med_coast", "bilbao": "atlantic_n",
    "alicante": "med_coast", "malaga": "atlantic_s",
    "palma": "med_island", "gran_canaria": "canaries",
    "tenerife": "canaries", "lisboa": "atlantic_n_ib",
    "porto": "atlantic_n_ib", "faro": "atlantic_s",
    "funchal": "canaries", "azorene": "atlantic_n",
    # Italia
    "roma": "med_coast", "milano": "continental", "venezia": "continental",
    "verona": "continental", "bologna": "continental", "pisa": "med_coast",
    "napoli": "med_coast", "bari": "med_coast",
    "olbia": "med_island", "palermo": "med_island", "catania": "med_island",
    # Frankrike
    "paris": "atlantic_n", "nice": "med_coast", "bordeaux": "atlantic_n",
    "toulouse": "med_coast", "montpellier": "med_coast",
    "ajaccio": "med_island",
    # Hellas
    "athen": "med_coast", "thessaloniki": "med_coast",
    "heraklion": "med_island", "chania": "med_island",
    "rhodos": "aegean", "santorini": "aegean",
    "korfu": "med_island", "kos": "aegean",
    # Balkan & Øst-Middelhav
    "dubrovnik": "adriatic", "split": "adriatic", "zadar": "adriatic",
    "pula": "adriatic", "zagreb": "balkan", "tivat": "adriatic",
    "valletta": "med_island", "larnaca": "med_east",
    "istanbul": "balkan", "antalya": "med_east",
    "bodrum": "aegean", "alanya": "med_east",
    # Vest-Europa
    "berlin": "continental", "munchen": "continental",
    "hamburg": "nordic", "dusseldorf": "atlantic_n",
    "amsterdam": "atlantic_n", "brussel": "atlantic_n",
    "geneve": "continental", "basel": "continental", "wien": "continental",
    # Sentral-/Øst-Europa
    "praha": "continental", "budapest": "continental",
    "warszawa": "continental", "krakow": "continental",
    "gdansk": "baltic_reg", "wroclaw": "continental",
    "poznan": "continental", "szczecin": "continental",
    "bratislava": "continental", "bucuresti": "continental",
    "sofia": "balkan", "varna": "black_sea", "burgas": "black_sea",
    # Vest-Balkan
    "tirana": "balkan", "sarajevo": "balkan", "beograd": "balkan",
    "pristina": "balkan", "skopje": "balkan",
    # Baltikum
    "tallinn": "nordic", "riga": "nordic", "vilnius": "continental",
    # Lengre vekk
    "hurghada": "hot_dry", "sharm": "hot_dry", "kairo": "hot_dry",
    "telaviv": "med_east", "dubai": "hot_dry",
    # Alpene
    "chamonix": "alpine", "zermatt": "alpine",
    "innsbruck": "alpine", "cortina": "alpine",
}

_DEFAULT_OFFSET = _OFFSETS["continental"]


def tmax_avg(dest_id: str, temp_mean: float, month: int) -> float:
    """Returnerer estimert gjennomsnittlig daglig makstemp for destinasjonen."""
    region = DEST_KLIMAREGION.get(dest_id)
    offset_list = _OFFSETS.get(region, _DEFAULT_OFFSET) if region else _DEFAULT_OFFSET
    return round(temp_mean + offset_list[month - 1], 1)


# ─── SJØTEMPERATURER (SST) ────────────────────────────────────────────────────
# Kilde: NOAA World Ocean Atlas 2023 / Copernicus Marine Service klimanormaler.
# Verdier i °C, månedlige normaler jan–des.

_SST_REGIONER: dict[str, list[int]] = {
    "skagerrak":    [5, 4, 5,  8, 12, 16, 19, 19, 16, 13,  9,  6],
    "nordsjoen":    [7, 7, 7,  8, 10, 13, 15, 16, 14, 12,  9,  7],
    "baltikum":     [3, 3, 4,  6, 10, 15, 18, 18, 14, 11,  7,  5],
    "svartehavet":  [8, 7, 8, 12, 17, 21, 24, 25, 22, 18, 14, 10],
    "med_vest":     [13,13,13, 14, 17, 21, 24, 25, 22, 20, 17, 14],  # Nice, Ajaccio
    "med_iberia":   [13,13,14, 15, 18, 22, 25, 26, 23, 20, 17, 14],  # Barcelona, Valencia, Alicante, Palma
    "med_central":  [14,14,14, 16, 19, 23, 26, 27, 25, 22, 18, 15],  # Napoli, Sardinia, Sicily, Malta
    "adriatisk":    [12,11,12, 15, 19, 23, 26, 27, 24, 20, 16, 13],
    "ionisk":       [15,14,14, 16, 20, 24, 26, 27, 25, 22, 18, 16],  # Korfu
    "egeisk":       [15,14,15, 17, 21, 25, 27, 27, 25, 22, 18, 16],  # Rhodos, Kos, Santorini
    "med_ost":      [17,16,17, 19, 22, 26, 28, 29, 28, 25, 22, 19],  # Kypros, Antalya, Kreta, Tel Aviv
    "atl_iberia_s": [15,15,15, 16, 17, 19, 21, 21, 20, 19, 17, 16],  # Málaga, Faro
    "atl_iberia_n": [13,13,13, 13, 15, 17, 18, 18, 17, 16, 15, 13],  # Lisboa, Porto
    "kanarioyene":  [19,18,18, 19, 20, 21, 23, 24, 24, 23, 21, 20],
    "madeira":      [18,18,18, 19, 20, 22, 23, 24, 24, 23, 21, 19],
    "rodehavet":    [22,21,22, 24, 26, 28, 29, 30, 29, 28, 25, 23],
    "persiagulfen": [21,20,22, 26, 29, 31, 33, 34, 33, 30, 26, 23],
    "nordatl":      [11,10,10, 11, 12, 15, 16, 16, 15, 14, 13, 12],  # Azorene
}

# Destinasjon → SST-region (kun for destinasjoner med 'strand'-tag)
DEST_SST_REGION: dict[str, str] = {
    "kristiansand": "skagerrak",
    "visby":        "baltikum",
    "gdansk":       "baltikum",
    "varna":        "svartehavet",
    "burgas":       "svartehavet",
    "nice":         "med_vest",
    "ajaccio":      "med_vest",
    "barcelona":    "med_iberia",
    "valencia":     "med_iberia",
    "alicante":     "med_iberia",
    "palma":        "med_iberia",
    "malaga":       "atl_iberia_s",
    "faro":         "atl_iberia_s",
    "lisboa":       "atl_iberia_n",
    "gran_canaria": "kanarioyene",
    "tenerife":     "kanarioyene",
    "funchal":      "madeira",
    "azorene":      "nordatl",
    "napoli":       "med_central",
    "bari":         "med_central",
    "olbia":        "med_central",
    "palermo":      "med_central",
    "catania":      "med_central",
    "valletta":     "med_central",
    "dubrovnik":    "adriatisk",
    "split":        "adriatisk",
    "zadar":        "adriatisk",
    "pula":         "adriatisk",
    "tivat":        "adriatisk",
    "korfu":        "ionisk",
    "rhodos":       "egeisk",
    "santorini":    "egeisk",
    "kos":          "egeisk",
    "bodrum":       "egeisk",
    "heraklion":    "med_ost",
    "chania":       "med_ost",
    "larnaca":      "med_ost",
    "antalya":      "med_ost",
    "alanya":       "med_ost",
    "telaviv":      "med_ost",
    "hurghada":     "rodehavet",
    "sharm":        "rodehavet",
    "dubai":        "persiagulfen",
}


def sst(dest_id: str, month: int) -> int | None:
    """Returnerer sjøtemperatur i °C for destinasjonen, eller None hvis ikke relevant."""
    region = DEST_SST_REGION.get(dest_id)
    if region is None:
        return None
    return _SST_REGIONER[region][month - 1]
