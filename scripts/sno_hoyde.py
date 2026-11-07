"""Høydetilpasning og lang horisont for snøvarselet (/ver/varsel-kvamskogen).

Tre ting skal til for å regne snø i fjellet (f.eks. Såta) i stedet for ved
målestasjonen:

1. Temperatur i riktig høyde. Yr (MET Locationforecast) korrigerer selv
   temperaturen når vi oppgir høyde. WeatherNext har et rutenett på ca. 11 km med
   eget, glattet terreng, så det kalibreres mot Yr i timene der begge finnes
   (gjennomsnittlig avvik), og avviket legges på resten av WeatherNext-perioden.
2. Startdybde i riktig høyde. Frost-stasjonen måler bare der den står. NVEs
   seNorge-grid (1 km, `gts.nve.no`) gir modellert snødybde og cellens snitthøyde.
   Vi henter et lite rutenett rundt punktet og tilpasser snødybde mot høyde, slik
   at vi kan lese av dybden i ønsket høyde.
3. Lang horisont. Yr gir ca. 9-10 døgn. WeatherNext 3 (lagret hver time for
   Kvamskogen) gir ca. 15 døgn med ensemble-spenn (p10/p90), som brukes videre
   etter at Yr slutter.

Nedbør øker typisk med høyden på Vestlandet. Det tas med som en enkel
tommelfingerregel (`NEDBØR_ØKNING_PR_100M`), og kan settes til 0.
"""
from __future__ import annotations

import math
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from typing import Any, Optional

import requests

GTS_BASE = "https://gts.nve.no/api/GridTimeSeries"
GTS_TIMEOUT = 15

#: Relativ økning i nedbør per 100 m over referansehøyden (0.05 = 5 %).
NEDBØR_ØKNING_PR_100M = 0.05
#: Nedbørsfaktoren holdes innenfor dette, så ekstreme høyder ikke gir tull.
NEDBØR_FAKTOR_MIN, NEDBØR_FAKTOR_MAKS = 0.7, 1.5
#: Forskjell fra stasjonshøyden før vi bruker seNorge i stedet for stasjonsmålingen.
HØYDE_TOLERANSE_M = 75
#: Minst så mange felles timer for å kalibrere WeatherNext mot Yr.
MIN_KALIBRERINGSTIMER = 12
#: Lengde på blokkene WeatherNext legges inn med (Yr bruker 6 t langt frem).
WN_BLOKK_TIMER = 6


# ---------------------------------------------------------------------------
# Koordinater: WGS84 -> UTM sone 33N (ETRS89/EUREF89, som seNorge bruker)
# ---------------------------------------------------------------------------

def utm33(lat: float, lon: float) -> tuple[float, float]:
    """WGS84/ETRS89 til UTM 33N (meter). Krügers serier, mm-nøyaktig i Norge."""
    a, f = 6378137.0, 1 / 298.257222101
    k0, lon0, e0 = 0.9996, math.radians(15.0), 500000.0
    n = f / (2 - f)
    A = a / (1 + n) * (1 + n**2 / 4 + n**4 / 64)
    alfa = (
        n / 2 - 2 * n**2 / 3 + 5 * n**3 / 16,
        13 * n**2 / 48 - 3 * n**3 / 5,
        61 * n**3 / 240,
    )
    phi, lam = math.radians(lat), math.radians(lon) - lon0
    e = math.sqrt(f * (2 - f))
    t = math.sinh(math.atanh(math.sin(phi)) - e * math.atanh(e * math.sin(phi)))
    xi_ = math.atan2(t, math.cos(lam))
    eta_ = math.atanh(math.sin(lam) / math.sqrt(1 + t * t))
    xi, eta = xi_, eta_
    for j, aj in enumerate(alfa, start=1):
        xi += aj * math.sin(2 * j * xi_) * math.cosh(2 * j * eta_)
        eta += aj * math.cos(2 * j * xi_) * math.sinh(2 * j * eta_)
    return e0 + k0 * A * eta, k0 * A * xi


# ---------------------------------------------------------------------------
# Nedbør og temperatur i høyden
# ---------------------------------------------------------------------------

def nedbør_faktor(moh: Optional[float], ref_moh: Optional[float],
                  økning_pr_100m: float = NEDBØR_ØKNING_PR_100M) -> float:
    """Skalering av nedbør fra referansehøyden til ønsket høyde."""
    if moh is None or ref_moh is None:
        return 1.0
    faktor = 1.0 + økning_pr_100m * (float(moh) - float(ref_moh)) / 100.0
    return max(NEDBØR_FAKTOR_MIN, min(NEDBØR_FAKTOR_MAKS, faktor))


def _utc(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str) and value:
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _stat(rad: dict[str, Any], felt: str, stat: str = "mean") -> Optional[float]:
    v = (rad.get(felt) or {}).get(stat)
    try:
        return None if v is None else float(v)
    except (TypeError, ValueError):
        return None


def temperatur_avvik(yr_intervaller: list[dict[str, Any]],
                     wn_timer: list[dict[str, Any]]) -> Optional[float]:
    """Snittavvik Yr minus WeatherNext i felles timer (bare Yrs timesintervaller).

    Yr er allerede korrigert til ønsket høyde, så avviket tar både høydeforskjell
    mot WeatherNext-cellen og systematiske forskjeller mellom modellene.
    """
    wn = {}
    for h in wn_timer or []:
        t = _utc(h.get("t"))
        v = _stat(h, "temp")
        if t is not None and v is not None:
            wn[t] = v
    diffs = []
    for iv in yr_intervaller or []:
        if iv.get("timer") != 1:
            continue
        t = _utc(iv.get("start"))
        if t in wn and iv.get("temp") is not None:
            diffs.append(float(iv["temp"]) - wn[t])
    if len(diffs) < MIN_KALIBRERINGSTIMER:
        return None
    return sum(diffs) / len(diffs)


def weathernext_intervaller(wn_timer: list[dict[str, Any]], etter: datetime, *,
                            temp_avvik: float = 0.0, nedbør_skala: float = 1.0,
                            blokk_timer: int = WN_BLOKK_TIMER) -> list[dict[str, Any]]:
    """WeatherNext-timer etter `etter`, slått sammen til blokker i samme format
    som `snow_increase.hent_intervaller`.

    Blokkene følger 00/06/12/18 UTC som Yr. Ufullstendige blokker i enden tas
    med så lenge de har minst halvparten av timene.
    """
    etter = _utc(etter)
    timer = []
    for h in wn_timer or []:
        t = _utc(h.get("t"))
        if t is None or (etter is not None and t < etter) or _stat(h, "temp") is None:
            continue
        timer.append((t, h))
    timer.sort(key=lambda x: x[0])

    blokker: dict[datetime, list[tuple[datetime, dict[str, Any]]]] = {}
    for t, h in timer:
        start = t.replace(hour=(t.hour // blokk_timer) * blokk_timer, minute=0, second=0, microsecond=0)
        if etter is not None and start < etter:
            start = etter  # første blokk starter der Yr slutter
        blokker.setdefault(start, []).append((t, h))

    ut = []
    for start in sorted(blokker):
        rader = blokker[start]
        if len(rader) < max(1, blokk_timer // 2):
            continue
        slutt = rader[-1][0] + timedelta(hours=1)
        hs = [h for _, h in rader]

        def snitt(felt: str, stat: str = "mean") -> Optional[float]:
            v = [x for h in hs if (x := _stat(h, felt, stat)) is not None]
            return sum(v) / len(v) if v else None

        def summ(stat: str) -> Optional[float]:
            v = [x for h in hs if (x := _stat(h, "regn", stat)) is not None]
            return max(0.0, sum(v)) * nedbør_skala if v else None

        retninger = [h.get("vindretning") for h in hs if h.get("vindretning") is not None]
        ut.append({
            "start": start,
            "slutt": slutt,
            "timer": (slutt - start).total_seconds() / 3600,
            "temp": snitt("temp") + temp_avvik,
            "nedbør_mm": summ("mean") or 0.0,
            "nedbør_min_mm": summ("p10"),
            "nedbør_maks_mm": summ("p90"),
            "nedbør_sannsynlighet_pct": None,
            "vind_ms": snitt("vind"),
            "vind_kast_ms": None,
            "vindretning_grader": _sirkelsnitt(retninger),
            "kilde": "weathernext",
        })
    return ut


def _sirkelsnitt(grader: list[float]) -> Optional[float]:
    if not grader:
        return None
    s = sum(math.sin(math.radians(g)) for g in grader)
    c = sum(math.cos(math.radians(g)) for g in grader)
    return math.degrees(math.atan2(s, c)) % 360


# ---------------------------------------------------------------------------
# seNorge-snødybde i ønsket høyde
# ---------------------------------------------------------------------------

_CACHE: dict[tuple, tuple[float, Any]] = {}
_CACHE_TTL = 3 * 3600
_LOCK = threading.Lock()


def _gts_celle(x: float, y: float, dag: date, session: requests.Session) -> Optional[dict[str, float]]:
    """Snødybde (cm) og cellehøyde for én seNorge-celle siste tilgjengelige dag."""
    fra = (dag - timedelta(days=3)).isoformat()
    url = f"{GTS_BASE}/{int(round(x))}/{int(round(y))}/{fra}/{dag.isoformat()}/sd.json"
    r = session.get(url, timeout=GTS_TIMEOUT)
    r.raise_for_status()
    js = r.json()
    nodata = js.get("NoDataValue")
    verdier = [v for v in (js.get("Data") or []) if v is not None and v != nodata]
    if not verdier or js.get("Altitude") is None:
        return None
    sd = float(verdier[-1])
    if str(js.get("Unit", "cm")).strip().lower() in {"mm"}:
        sd /= 10.0
    elif str(js.get("Unit", "cm")).strip().lower() in {"m"}:
        sd *= 100.0
    return {"sd_cm": sd, "moh": float(js["Altitude"])}


def tilpass_høyde(celler: list[dict[str, float]], moh: float) -> Optional[dict[str, Any]]:
    """Les av snødybde i høyden `moh` fra celler med (sd_cm, moh).

    Lineær tilpasning når cellene spenner over nok høyde. Ellers brukes cellen
    nærmest i høyde. Ekstrapolering holdes innenfor 150 m utenfor cellenes spenn.
    """
    celler = [c for c in celler if c]
    if not celler:
        return None
    høyder = [c["moh"] for c in celler]
    spenn = max(høyder) - min(høyder)
    if len(celler) >= 3 and spenn >= 80:
        mx = sum(høyder) / len(høyder)
        my = sum(c["sd_cm"] for c in celler) / len(celler)
        sxx = sum((h - mx) ** 2 for h in høyder)
        sxy = sum((c["moh"] - mx) * (c["sd_cm"] - my) for c in celler)
        stigning = sxy / sxx if sxx else 0.0
        h = min(max(moh, min(høyder) - 150), max(høyder) + 150)
        sd = my + stigning * (h - mx)
        return {"sd_cm": round(max(0.0, sd), 1), "metode": "regresjon",
                "cm_pr_100m": round(stigning * 100, 1), "celler": len(celler),
                "høyde_spenn": [round(min(høyder)), round(max(høyder))]}
    nærmest = min(celler, key=lambda c: abs(c["moh"] - moh))
    return {"sd_cm": round(max(0.0, nærmest["sd_cm"]), 1), "metode": "nærmeste celle",
            "cm_pr_100m": None, "celler": len(celler),
            "høyde_spenn": [round(min(høyder)), round(max(høyder))]}


def senorge_snødybde(lat: float, lon: float, moh: float, *, steg_m: int = 1000,
                     radius: int = 1, dag: Optional[date] = None) -> Optional[dict[str, Any]]:
    """Modellert snødybde (cm) i høyden `moh` nær punktet, fra seNorge (NVE).

    Henter (2*radius+1)^2 celler rundt punktet og tilpasser mot høyde.
    Caches i tre timer. Returnerer None hvis NVE ikke svarer.
    """
    dag = dag or datetime.now(timezone.utc).date()
    nøkkel = (round(lat, 3), round(lon, 3), int(round(moh / 10) * 10), dag)
    with _LOCK:
        treff = _CACHE.get(nøkkel)
        # Bom (NVE nede) huskes bare i ti minutter
        if treff and time.time() - treff[0] < (_CACHE_TTL if treff[1] else 600):
            return treff[1]

    x0, y0 = utm33(lat, lon)
    punkter = [(x0 + i * steg_m, y0 + j * steg_m)
               for i in range(-radius, radius + 1) for j in range(-radius, radius + 1)]
    session = requests.Session()

    def hent(p):
        try:
            return _gts_celle(p[0], p[1], dag, session)
        except Exception:
            return None

    with ThreadPoolExecutor(max_workers=min(9, len(punkter))) as ex:
        celler = list(ex.map(hent, punkter))
    resultat = tilpass_høyde([c for c in celler if c], moh)
    if resultat is not None:
        resultat["kilde"] = "seNorge (NVE)"
    with _LOCK:
        _CACHE[nøkkel] = (time.time(), resultat)
    return resultat
