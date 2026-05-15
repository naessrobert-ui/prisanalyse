"""Destinasjonsliste for ferieplanlegger.

Listen er bygget rundt Norwegian Air Shuttle sitt rutenett, supplert med
noen klassiske fjellresorter (Chamonix, Zermatt, Innsbruck, Cortina) og
norske fjord-/fjellsteder Norwegian ikke flyr til (Geiranger, Flåm,
Kvamskogen, Hovden).

Tags:
    strand     – typisk badedestinasjon
    by         – storby / byferie
    fjell      – fjellresort / alpin
    øy         – øy (Mallorca, Kreta, Madeira, ...)
    nord       – nordlys / Arktis
    middelhav  – Middelhavet
    varmt      – jevnt varmt klima året rundt

Hver destinasjon har én "primær"-tag pluss valgfrie tilleggstagger,
slik at filtrering på flere tagger er en OR-operasjon.
"""
from __future__ import annotations

from typing import TypedDict


class Destinasjon(TypedDict):
    id: str
    navn: str
    land: str          # ISO-3166-1 alpha-2
    region: str        # fri tekst, brukes til gruppering i UI
    lat: float
    lon: float
    tags: list[str]


DESTINASJONER: list[Destinasjon] = [
    # ---------- NORGE ----------
    {"id": "oslo", "navn": "Oslo", "land": "NO", "region": "Norge", "lat": 59.9139, "lon": 10.7522, "tags": ["by"]},
    {"id": "bergen", "navn": "Bergen", "land": "NO", "region": "Norge", "lat": 60.3913, "lon": 5.3221, "tags": ["by"]},
    {"id": "stavanger", "navn": "Stavanger", "land": "NO", "region": "Norge", "lat": 58.9700, "lon": 5.7331, "tags": ["by"]},
    {"id": "kristiansand", "navn": "Kristiansand", "land": "NO", "region": "Norge", "lat": 58.1467, "lon": 7.9956, "tags": ["by", "strand"]},
    {"id": "trondheim", "navn": "Trondheim", "land": "NO", "region": "Norge", "lat": 63.4305, "lon": 10.3951, "tags": ["by"]},
    {"id": "alesund", "navn": "Ålesund", "land": "NO", "region": "Norge", "lat": 62.4722, "lon": 6.1495, "tags": ["by"]},
    {"id": "molde", "navn": "Molde", "land": "NO", "region": "Norge", "lat": 62.7378, "lon": 7.1607, "tags": ["by"]},
    {"id": "haugesund", "navn": "Haugesund", "land": "NO", "region": "Norge", "lat": 59.4138, "lon": 5.2680, "tags": ["by"]},
    {"id": "sandefjord", "navn": "Sandefjord", "land": "NO", "region": "Norge", "lat": 59.1313, "lon": 10.2166, "tags": ["by"]},
    {"id": "bodo", "navn": "Bodø", "land": "NO", "region": "Norge", "lat": 67.2804, "lon": 14.4049, "tags": ["by", "nord"]},
    {"id": "tromso", "navn": "Tromsø", "land": "NO", "region": "Norge", "lat": 69.6492, "lon": 18.9553, "tags": ["by", "nord"]},
    {"id": "harstad", "navn": "Harstad/Narvik", "land": "NO", "region": "Norge", "lat": 68.4953, "lon": 16.6781, "tags": ["nord"]},
    {"id": "andenes", "navn": "Andenes (Lofoten)", "land": "NO", "region": "Norge", "lat": 69.3267, "lon": 16.1340, "tags": ["nord", "øy"]},
    {"id": "alta", "navn": "Alta", "land": "NO", "region": "Norge", "lat": 69.9689, "lon": 23.2716, "tags": ["nord"]},
    {"id": "kirkenes", "navn": "Kirkenes", "land": "NO", "region": "Norge", "lat": 69.7273, "lon": 30.0444, "tags": ["nord"]},
    {"id": "longyearbyen", "navn": "Longyearbyen (Svalbard)", "land": "NO", "region": "Norge", "lat": 78.2232, "lon": 15.6267, "tags": ["nord"]},
    {"id": "geiranger", "navn": "Geiranger", "land": "NO", "region": "Norge", "lat": 62.1010, "lon": 7.2050, "tags": ["fjell"]},
    {"id": "flam", "navn": "Flåm", "land": "NO", "region": "Norge", "lat": 60.8628, "lon": 7.1133, "tags": ["fjell"]},
    {"id": "kvamskogen", "navn": "Kvamskogen", "land": "NO", "region": "Norge", "lat": 60.3783, "lon": 5.9796, "tags": ["fjell"]},
    {"id": "hovden", "navn": "Hovden", "land": "NO", "region": "Norge", "lat": 59.5570, "lon": 7.3540, "tags": ["fjell"]},

    # ---------- NORDEN ELLERS ----------
    {"id": "stockholm", "navn": "Stockholm", "land": "SE", "region": "Norden", "lat": 59.3293, "lon": 18.0686, "tags": ["by"]},
    {"id": "goteborg", "navn": "Göteborg", "land": "SE", "region": "Norden", "lat": 57.7089, "lon": 11.9746, "tags": ["by"]},
    {"id": "malmo", "navn": "Malmö", "land": "SE", "region": "Norden", "lat": 55.6050, "lon": 13.0038, "tags": ["by"]},
    {"id": "visby", "navn": "Visby (Gotland)", "land": "SE", "region": "Norden", "lat": 57.6348, "lon": 18.2948, "tags": ["øy", "strand"]},
    {"id": "umea", "navn": "Umeå", "land": "SE", "region": "Norden", "lat": 63.8258, "lon": 20.2630, "tags": ["by", "nord"]},
    {"id": "lulea", "navn": "Luleå", "land": "SE", "region": "Norden", "lat": 65.5848, "lon": 22.1547, "tags": ["nord"]},
    {"id": "kiruna", "navn": "Kiruna", "land": "SE", "region": "Norden", "lat": 67.8558, "lon": 20.2253, "tags": ["nord"]},
    {"id": "vaxjo", "navn": "Växjö", "land": "SE", "region": "Norden", "lat": 56.8777, "lon": 14.8094, "tags": ["by"]},
    {"id": "kobenhavn", "navn": "København", "land": "DK", "region": "Norden", "lat": 55.6761, "lon": 12.5683, "tags": ["by"]},
    {"id": "aalborg", "navn": "Aalborg", "land": "DK", "region": "Norden", "lat": 57.0488, "lon": 9.9217, "tags": ["by"]},
    {"id": "aarhus", "navn": "Aarhus", "land": "DK", "region": "Norden", "lat": 56.1629, "lon": 10.2039, "tags": ["by"]},
    {"id": "billund", "navn": "Billund", "land": "DK", "region": "Norden", "lat": 55.7333, "lon": 9.1167, "tags": ["by"]},
    {"id": "helsinki", "navn": "Helsinki", "land": "FI", "region": "Norden", "lat": 60.1699, "lon": 24.9384, "tags": ["by"]},
    {"id": "oulu", "navn": "Oulu", "land": "FI", "region": "Norden", "lat": 65.0121, "lon": 25.4651, "tags": ["nord"]},
    {"id": "rovaniemi", "navn": "Rovaniemi", "land": "FI", "region": "Norden", "lat": 66.5039, "lon": 25.7294, "tags": ["nord"]},
    {"id": "reykjavik", "navn": "Reykjavik", "land": "IS", "region": "Norden", "lat": 64.1466, "lon": -21.9426, "tags": ["by", "nord", "øy"]},

    # ---------- UK & IRLAND ----------
    {"id": "london", "navn": "London", "land": "GB", "region": "UK & Irland", "lat": 51.5074, "lon": -0.1278, "tags": ["by"]},
    {"id": "edinburgh", "navn": "Edinburgh", "land": "GB", "region": "UK & Irland", "lat": 55.9533, "lon": -3.1883, "tags": ["by"]},
    {"id": "manchester", "navn": "Manchester", "land": "GB", "region": "UK & Irland", "lat": 53.4808, "lon": -2.2426, "tags": ["by"]},
    {"id": "dublin", "navn": "Dublin", "land": "IE", "region": "UK & Irland", "lat": 53.3498, "lon": -6.2603, "tags": ["by"]},

    # ---------- IBERIA + ATLANTEREN ----------
    {"id": "madrid", "navn": "Madrid", "land": "ES", "region": "Iberia & Atlanteren", "lat": 40.4168, "lon": -3.7038, "tags": ["by"]},
    {"id": "barcelona", "navn": "Barcelona", "land": "ES", "region": "Iberia & Atlanteren", "lat": 41.3851, "lon": 2.1734, "tags": ["by", "strand", "middelhav"]},
    {"id": "valencia", "navn": "Valencia", "land": "ES", "region": "Iberia & Atlanteren", "lat": 39.4699, "lon": -0.3763, "tags": ["by", "strand", "middelhav"]},
    {"id": "bilbao", "navn": "Bilbao", "land": "ES", "region": "Iberia & Atlanteren", "lat": 43.2630, "lon": -2.9350, "tags": ["by"]},
    {"id": "alicante", "navn": "Alicante", "land": "ES", "region": "Iberia & Atlanteren", "lat": 38.3452, "lon": -0.4810, "tags": ["strand", "by", "middelhav", "varmt"]},
    {"id": "malaga", "navn": "Málaga", "land": "ES", "region": "Iberia & Atlanteren", "lat": 36.7213, "lon": -4.4214, "tags": ["strand", "by", "middelhav", "varmt"]},
    {"id": "palma", "navn": "Palma de Mallorca", "land": "ES", "region": "Iberia & Atlanteren", "lat": 39.5696, "lon": 2.6502, "tags": ["strand", "øy", "middelhav", "varmt"]},
    {"id": "gran_canaria", "navn": "Gran Canaria (Las Palmas)", "land": "ES", "region": "Iberia & Atlanteren", "lat": 28.1235, "lon": -15.4363, "tags": ["strand", "øy", "varmt"]},
    {"id": "tenerife", "navn": "Tenerife", "land": "ES", "region": "Iberia & Atlanteren", "lat": 28.2916, "lon": -16.6291, "tags": ["strand", "øy", "varmt"]},
    {"id": "lisboa", "navn": "Lisboa", "land": "PT", "region": "Iberia & Atlanteren", "lat": 38.7223, "lon": -9.1393, "tags": ["by", "strand"]},
    {"id": "porto", "navn": "Porto", "land": "PT", "region": "Iberia & Atlanteren", "lat": 41.1579, "lon": -8.6291, "tags": ["by"]},
    {"id": "faro", "navn": "Faro (Algarve)", "land": "PT", "region": "Iberia & Atlanteren", "lat": 37.0194, "lon": -7.9304, "tags": ["strand", "by", "varmt"]},
    {"id": "funchal", "navn": "Funchal (Madeira)", "land": "PT", "region": "Iberia & Atlanteren", "lat": 32.6669, "lon": -16.9241, "tags": ["øy", "varmt"]},
    {"id": "azorene", "navn": "Ponta Delgada (Azorene)", "land": "PT", "region": "Iberia & Atlanteren", "lat": 37.7412, "lon": -25.6756, "tags": ["øy"]},

    # ---------- ITALIA ----------
    {"id": "roma", "navn": "Roma", "land": "IT", "region": "Italia", "lat": 41.9028, "lon": 12.4964, "tags": ["by", "middelhav"]},
    {"id": "milano", "navn": "Milano", "land": "IT", "region": "Italia", "lat": 45.4642, "lon": 9.1900, "tags": ["by"]},
    {"id": "venezia", "navn": "Venezia", "land": "IT", "region": "Italia", "lat": 45.4408, "lon": 12.3155, "tags": ["by"]},
    {"id": "verona", "navn": "Verona", "land": "IT", "region": "Italia", "lat": 45.4384, "lon": 10.9916, "tags": ["by"]},
    {"id": "bologna", "navn": "Bologna", "land": "IT", "region": "Italia", "lat": 44.4949, "lon": 11.3426, "tags": ["by"]},
    {"id": "pisa", "navn": "Pisa", "land": "IT", "region": "Italia", "lat": 43.7228, "lon": 10.4017, "tags": ["by"]},
    {"id": "napoli", "navn": "Napoli", "land": "IT", "region": "Italia", "lat": 40.8518, "lon": 14.2681, "tags": ["by", "strand", "middelhav"]},
    {"id": "bari", "navn": "Bari", "land": "IT", "region": "Italia", "lat": 41.1171, "lon": 16.8719, "tags": ["by", "middelhav"]},
    {"id": "olbia", "navn": "Olbia (Sardinia)", "land": "IT", "region": "Italia", "lat": 40.9230, "lon": 9.4982, "tags": ["strand", "øy", "middelhav", "varmt"]},
    {"id": "palermo", "navn": "Palermo (Sicilia)", "land": "IT", "region": "Italia", "lat": 38.1157, "lon": 13.3615, "tags": ["strand", "øy", "middelhav", "varmt"]},
    {"id": "catania", "navn": "Catania (Sicilia)", "land": "IT", "region": "Italia", "lat": 37.5079, "lon": 15.0830, "tags": ["strand", "øy", "middelhav", "varmt"]},

    # ---------- FRANKRIKE ----------
    {"id": "paris", "navn": "Paris", "land": "FR", "region": "Frankrike", "lat": 48.8566, "lon": 2.3522, "tags": ["by"]},
    {"id": "nice", "navn": "Nice", "land": "FR", "region": "Frankrike", "lat": 43.7102, "lon": 7.2620, "tags": ["strand", "by", "middelhav"]},
    {"id": "bordeaux", "navn": "Bordeaux", "land": "FR", "region": "Frankrike", "lat": 44.8378, "lon": -0.5792, "tags": ["by"]},
    {"id": "toulouse", "navn": "Toulouse", "land": "FR", "region": "Frankrike", "lat": 43.6047, "lon": 1.4442, "tags": ["by"]},
    {"id": "montpellier", "navn": "Montpellier", "land": "FR", "region": "Frankrike", "lat": 43.6109, "lon": 3.8772, "tags": ["by", "middelhav"]},
    {"id": "ajaccio", "navn": "Ajaccio (Korsika)", "land": "FR", "region": "Frankrike", "lat": 41.9192, "lon": 8.7386, "tags": ["strand", "øy", "middelhav"]},

    # ---------- HELLAS ----------
    {"id": "athen", "navn": "Athen", "land": "GR", "region": "Hellas", "lat": 37.9838, "lon": 23.7275, "tags": ["by", "middelhav", "varmt"]},
    {"id": "thessaloniki", "navn": "Thessaloniki", "land": "GR", "region": "Hellas", "lat": 40.6401, "lon": 22.9444, "tags": ["by", "middelhav"]},
    {"id": "heraklion", "navn": "Heraklion (Kreta)", "land": "GR", "region": "Hellas", "lat": 35.3387, "lon": 25.1442, "tags": ["strand", "øy", "middelhav", "varmt"]},
    {"id": "chania", "navn": "Chania (Kreta)", "land": "GR", "region": "Hellas", "lat": 35.5138, "lon": 24.0180, "tags": ["strand", "øy", "middelhav", "varmt"]},
    {"id": "rhodos", "navn": "Rhodos", "land": "GR", "region": "Hellas", "lat": 36.4341, "lon": 28.2176, "tags": ["strand", "øy", "middelhav", "varmt"]},
    {"id": "santorini", "navn": "Santorini", "land": "GR", "region": "Hellas", "lat": 36.3932, "lon": 25.4615, "tags": ["strand", "øy", "middelhav", "varmt"]},
    {"id": "korfu", "navn": "Korfu", "land": "GR", "region": "Hellas", "lat": 39.6243, "lon": 19.9217, "tags": ["strand", "øy", "middelhav"]},
    {"id": "kos", "navn": "Kos", "land": "GR", "region": "Hellas", "lat": 36.8929, "lon": 27.2887, "tags": ["strand", "øy", "middelhav", "varmt"]},

    # ---------- BALKAN, MALTA, KYPROS, TYRKIA ----------
    {"id": "dubrovnik", "navn": "Dubrovnik", "land": "HR", "region": "Balkan & østre Middelhav", "lat": 42.6507, "lon": 18.0944, "tags": ["strand", "by", "middelhav"]},
    {"id": "split", "navn": "Split", "land": "HR", "region": "Balkan & østre Middelhav", "lat": 43.5081, "lon": 16.4402, "tags": ["strand", "by", "middelhav"]},
    {"id": "zadar", "navn": "Zadar", "land": "HR", "region": "Balkan & østre Middelhav", "lat": 44.1194, "lon": 15.2314, "tags": ["strand", "by", "middelhav"]},
    {"id": "pula", "navn": "Pula", "land": "HR", "region": "Balkan & østre Middelhav", "lat": 44.8666, "lon": 13.8496, "tags": ["strand", "by", "middelhav"]},
    {"id": "zagreb", "navn": "Zagreb", "land": "HR", "region": "Balkan & østre Middelhav", "lat": 45.8150, "lon": 15.9819, "tags": ["by"]},
    {"id": "tivat", "navn": "Tivat", "land": "ME", "region": "Balkan & østre Middelhav", "lat": 42.4347, "lon": 18.7081, "tags": ["strand", "middelhav"]},
    {"id": "valletta", "navn": "Valletta (Malta)", "land": "MT", "region": "Balkan & østre Middelhav", "lat": 35.8989, "lon": 14.5146, "tags": ["strand", "by", "øy", "middelhav", "varmt"]},
    {"id": "larnaca", "navn": "Larnaca (Kypros)", "land": "CY", "region": "Balkan & østre Middelhav", "lat": 34.9229, "lon": 33.6233, "tags": ["strand", "øy", "middelhav", "varmt"]},
    {"id": "istanbul", "navn": "Istanbul", "land": "TR", "region": "Balkan & østre Middelhav", "lat": 41.0082, "lon": 28.9784, "tags": ["by"]},
    {"id": "antalya", "navn": "Antalya", "land": "TR", "region": "Balkan & østre Middelhav", "lat": 36.8969, "lon": 30.7133, "tags": ["strand", "middelhav", "varmt"]},
    {"id": "bodrum", "navn": "Bodrum", "land": "TR", "region": "Balkan & østre Middelhav", "lat": 37.0344, "lon": 27.4305, "tags": ["strand", "middelhav", "varmt"]},
    {"id": "alanya", "navn": "Alanya", "land": "TR", "region": "Balkan & østre Middelhav", "lat": 36.5444, "lon": 31.9997, "tags": ["strand", "middelhav", "varmt"]},

    # ---------- VEST-/SENTRAL-EUROPA ----------
    {"id": "berlin", "navn": "Berlin", "land": "DE", "region": "Vest-Europa", "lat": 52.5200, "lon": 13.4050, "tags": ["by"]},
    {"id": "munchen", "navn": "München", "land": "DE", "region": "Vest-Europa", "lat": 48.1351, "lon": 11.5820, "tags": ["by"]},
    {"id": "hamburg", "navn": "Hamburg", "land": "DE", "region": "Vest-Europa", "lat": 53.5511, "lon": 9.9937, "tags": ["by"]},
    {"id": "dusseldorf", "navn": "Düsseldorf", "land": "DE", "region": "Vest-Europa", "lat": 51.2277, "lon": 6.7735, "tags": ["by"]},
    {"id": "amsterdam", "navn": "Amsterdam", "land": "NL", "region": "Vest-Europa", "lat": 52.3676, "lon": 4.9041, "tags": ["by"]},
    {"id": "brussel", "navn": "Brussel", "land": "BE", "region": "Vest-Europa", "lat": 50.8503, "lon": 4.3517, "tags": ["by"]},
    {"id": "geneve", "navn": "Genève", "land": "CH", "region": "Vest-Europa", "lat": 46.2044, "lon": 6.1432, "tags": ["by"]},
    {"id": "basel", "navn": "Basel", "land": "CH", "region": "Vest-Europa", "lat": 47.5596, "lon": 7.5886, "tags": ["by"]},
    {"id": "wien", "navn": "Wien", "land": "AT", "region": "Vest-Europa", "lat": 48.2082, "lon": 16.3738, "tags": ["by"]},

    # ---------- SENTRAL-/ØST-EUROPA ----------
    {"id": "praha", "navn": "Praha", "land": "CZ", "region": "Sentral-/Øst-Europa", "lat": 50.0755, "lon": 14.4378, "tags": ["by"]},
    {"id": "budapest", "navn": "Budapest", "land": "HU", "region": "Sentral-/Øst-Europa", "lat": 47.4979, "lon": 19.0402, "tags": ["by"]},
    {"id": "warszawa", "navn": "Warszawa", "land": "PL", "region": "Sentral-/Øst-Europa", "lat": 52.2297, "lon": 21.0122, "tags": ["by"]},
    {"id": "krakow", "navn": "Kraków", "land": "PL", "region": "Sentral-/Øst-Europa", "lat": 50.0647, "lon": 19.9450, "tags": ["by"]},
    {"id": "gdansk", "navn": "Gdańsk", "land": "PL", "region": "Sentral-/Øst-Europa", "lat": 54.3520, "lon": 18.6466, "tags": ["by", "strand"]},
    {"id": "wroclaw", "navn": "Wrocław", "land": "PL", "region": "Sentral-/Øst-Europa", "lat": 51.1079, "lon": 17.0385, "tags": ["by"]},
    {"id": "poznan", "navn": "Poznań", "land": "PL", "region": "Sentral-/Øst-Europa", "lat": 52.4064, "lon": 16.9252, "tags": ["by"]},
    {"id": "szczecin", "navn": "Szczecin", "land": "PL", "region": "Sentral-/Øst-Europa", "lat": 53.4285, "lon": 14.5528, "tags": ["by"]},
    {"id": "bratislava", "navn": "Bratislava", "land": "SK", "region": "Sentral-/Øst-Europa", "lat": 48.1486, "lon": 17.1077, "tags": ["by"]},
    {"id": "bucuresti", "navn": "București", "land": "RO", "region": "Sentral-/Øst-Europa", "lat": 44.4268, "lon": 26.1025, "tags": ["by"]},
    {"id": "sofia", "navn": "Sofia", "land": "BG", "region": "Sentral-/Øst-Europa", "lat": 42.6977, "lon": 23.3219, "tags": ["by"]},
    {"id": "varna", "navn": "Varna", "land": "BG", "region": "Sentral-/Øst-Europa", "lat": 43.2141, "lon": 27.9147, "tags": ["strand", "by"]},
    {"id": "burgas", "navn": "Burgas", "land": "BG", "region": "Sentral-/Øst-Europa", "lat": 42.5048, "lon": 27.4626, "tags": ["strand", "by"]},

    # ---------- VEST-BALKAN ----------
    {"id": "tirana", "navn": "Tirana", "land": "AL", "region": "Vest-Balkan", "lat": 41.3275, "lon": 19.8189, "tags": ["by"]},
    {"id": "sarajevo", "navn": "Sarajevo", "land": "BA", "region": "Vest-Balkan", "lat": 43.8563, "lon": 18.4131, "tags": ["by"]},
    {"id": "beograd", "navn": "Beograd", "land": "RS", "region": "Vest-Balkan", "lat": 44.7866, "lon": 20.4489, "tags": ["by"]},
    {"id": "pristina", "navn": "Pristina", "land": "XK", "region": "Vest-Balkan", "lat": 42.6629, "lon": 21.1655, "tags": ["by"]},
    {"id": "skopje", "navn": "Skopje", "land": "MK", "region": "Vest-Balkan", "lat": 41.9981, "lon": 21.4254, "tags": ["by"]},

    # ---------- BALTIKUM ----------
    {"id": "tallinn", "navn": "Tallinn", "land": "EE", "region": "Baltikum", "lat": 59.4370, "lon": 24.7536, "tags": ["by"]},
    {"id": "riga", "navn": "Riga", "land": "LV", "region": "Baltikum", "lat": 56.9496, "lon": 24.1052, "tags": ["by"]},
    {"id": "vilnius", "navn": "Vilnius", "land": "LT", "region": "Baltikum", "lat": 54.6872, "lon": 25.2797, "tags": ["by"]},

    # ---------- LENGRE VEKK ----------
    {"id": "hurghada", "navn": "Hurghada", "land": "EG", "region": "Lengre vekk", "lat": 27.2579, "lon": 33.8116, "tags": ["strand", "varmt"]},
    {"id": "sharm", "navn": "Sharm El Sheikh", "land": "EG", "region": "Lengre vekk", "lat": 27.9158, "lon": 34.3300, "tags": ["strand", "varmt"]},
    {"id": "kairo", "navn": "Kairo", "land": "EG", "region": "Lengre vekk", "lat": 30.0444, "lon": 31.2357, "tags": ["by", "varmt"]},
    {"id": "telaviv", "navn": "Tel Aviv", "land": "IL", "region": "Lengre vekk", "lat": 32.0853, "lon": 34.7818, "tags": ["strand", "by", "middelhav", "varmt"]},
    {"id": "dubai", "navn": "Dubai", "land": "AE", "region": "Lengre vekk", "lat": 25.2048, "lon": 55.2708, "tags": ["strand", "by", "varmt"]},

    # ---------- ALPENE-PERLER (ikke Norwegian-rutenett) ----------
    {"id": "chamonix", "navn": "Chamonix", "land": "FR", "region": "Alpene", "lat": 45.9237, "lon": 6.8694, "tags": ["fjell"]},
    {"id": "zermatt", "navn": "Zermatt", "land": "CH", "region": "Alpene", "lat": 46.0207, "lon": 7.7491, "tags": ["fjell"]},
    {"id": "innsbruck", "navn": "Innsbruck", "land": "AT", "region": "Alpene", "lat": 47.2692, "lon": 11.4041, "tags": ["fjell", "by"]},
    {"id": "cortina", "navn": "Cortina d'Ampezzo", "land": "IT", "region": "Alpene", "lat": 46.5405, "lon": 12.1357, "tags": ["fjell"]},
]


# Lookup-tabeller for raskt oppslag.
DESTINASJONER_BY_ID: dict[str, Destinasjon] = {d["id"]: d for d in DESTINASJONER}


def alle_tags() -> list[str]:
    """Returnerer alle unike tags, sortert i en stabil rekkefølge."""
    rekkefolge = ["strand", "by", "fjell", "øy", "nord", "middelhav", "varmt"]
    funnet = {tag for d in DESTINASJONER for tag in d["tags"]}
    return [t for t in rekkefolge if t in funnet]


def alle_land() -> list[tuple[str, int]]:
    """Returnerer (landkode, antall destinasjoner) sortert etter antall."""
    fra_land: dict[str, int] = {}
    for d in DESTINASJONER:
        fra_land[d["land"]] = fra_land.get(d["land"], 0) + 1
    return sorted(fra_land.items(), key=lambda x: (-x[1], x[0]))
