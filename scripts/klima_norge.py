"""Norge i tall: rekordene for hele landet, forhåndsberegnet.

Klimasiden (/ver/klima) viser varmest, kaldest, våtest, mest vind, mest sol
og mest snø for fem perioder:

    sist   siste hele døgn, målt (Frost, P1D)
    neste  neste 24 timer, varslet (Yr per stasjon)
    uke    neste 7 døgn, varslet (Yr per stasjon)
    mnd    denne måneden så langt, målt (Frost, P1D)
    aar    i år så langt, målt (Frost, P1M + denne måneden)

Alt hentes i en bakgrunnstråd og lagres som én liten JSON (S3 eller lokalt),
slik at siden aldri venter på Frost eller Yr. Hver del har egen
oppdateringsfrekvens: målinger hver halvtime, varsel hver time, året hver
sjette time. Feiler en del, beholdes forrige versjon.
"""
from __future__ import annotations

import json
import logging
import math
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Optional
from zoneinfo import ZoneInfo

import requests

log = logging.getLogger(__name__)

OSLO = ZoneInfo("Europe/Oslo")
FROST_OBS = "https://frost.met.no/observations/v0.jsonld"
KATEGORIER = ("varm", "kald", "vaat", "vind", "sol", "sno")
PERIODER = ("sist", "neste", "uke", "mnd", "aar")
MAANEDER = ["januar", "februar", "mars", "april", "mai", "juni", "juli",
            "august", "september", "oktober", "november", "desember"]
UKEDAGER = ["mandag", "tirsdag", "onsdag", "torsdag", "fredag", "lørdag", "søndag"]

# Havområder og sokkel tas ikke med: "mest vind i Norge" skal være et sted
# folk kjenner, ikke en plattform i Nordsjøen.
_IKKE_LAND = {"NORDSJØEN", "NORSKEHAVET", "BARENTSHAVET", "KONTINENTALSOKKELEN", ""}

# Hvor gamle delene kan være før de hentes på nytt (sekunder).
MAKS_ALDER = {
    "sist": int(os.getenv("KLIMA_NORGE_OBS_TTL", "1800")),
    "mnd": int(os.getenv("KLIMA_NORGE_OBS_TTL", "1800")),
    "neste": int(os.getenv("KLIMA_NORGE_VARSEL_TTL", "3600")),
    "uke": int(os.getenv("KLIMA_NORGE_VARSEL_TTL", "3600")),
    "aar": int(os.getenv("KLIMA_NORGE_AAR_TTL", "21600")),
}
_VARSEL_MAKS_STASJONER = int(os.getenv("KLIMA_NORGE_VARSEL_STASJONER", "320"))
_VARSEL_TRAADER = int(os.getenv("KLIMA_NORGE_VARSEL_TRAADER", "12"))
_FROST_BATCH = 60

_S3_KEY = os.getenv("KLIMA_NORGE_S3_KEY", "klima-norge/payload.json")
_LOKAL = Path(__file__).resolve().parents[1] / "data" / "klima_norge.json"
_NORMALER = Path(__file__).resolve().parents[1] / "static" / "data" / "vaernormaler.json"

_LAAS = threading.RLock()
_DATA: dict[str, Any] = {}
_DATA_TS = 0.0
_OPPFRISKER = threading.Lock()
_STARTET = False

# Rimelighetsgrenser mot åpenbare målefeil.
_GRENSER = {
    "temp": (-55.0, 40.0),
    "nedbor_dogn": (0.0, 350.0),
    "kast": (0.0, 75.0),
    "sol_dogn": (0.0, 24.0),
    "sno": (0.0, 900.0),
}


# ---------------------------------------------------------------------------
# Små hjelpere
# ---------------------------------------------------------------------------

def _f(v: Any) -> Optional[float]:
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    return x if math.isfinite(x) else None


def _innenfor(v: Optional[float], navn: str) -> bool:
    if v is None:
        return False
    lo, hi = _GRENSER[navn]
    return lo <= v <= hi


def _tall(v: float, d: int = 1) -> str:
    s = f"{v:.{d}f}".replace(".", ",")
    return s.replace("-", "−")


def _dato_tekst(d: date) -> str:
    return f"{d.day}. {MAANEDER[d.month - 1]}"


def _fylke(county: str) -> str:
    c = (county or "").strip().lower()
    if not c:
        return ""
    ord_ = [w if w in {"og", "i"} else w[:1].upper() + w[1:] for w in c.split(" ")]
    return " ".join(ord_)


def _stedsnavn(rad: dict[str, Any]) -> str:
    kort = str(rad.get("shortName") or "").strip()
    if kort:
        return kort
    navn = str(rad.get("name") or "").strip()
    return " ".join(w[:1].upper() + w[1:].lower() for w in navn.split(" "))


def _nysno_cm(nedbor_mm: float, temp: Optional[float]) -> float:
    """Samme modell som snøprognosen (snow_increase): 10 mm snø per mm ved 0°,
    litt luftigere når det er kaldt, sludd mellom 0 og 1,5°."""
    if temp is None or nedbor_mm <= 0:
        return 0.0
    if temp >= 1.5:
        return 0.0
    andel = 1.0 if temp <= 0 else 1.0 - temp / 1.5
    faktor = (10.0 + max(0.0, -temp) * 0.5) * andel
    return nedbor_mm * faktor / 10.0


# ---------------------------------------------------------------------------
# Stasjoner og normaler
# ---------------------------------------------------------------------------

def _stasjoner() -> list[dict[str, Any]]:
    from ver_station_db import load_station_db

    df = load_station_db()
    ut = []
    for r in df.to_dict("records"):
        county = str(r.get("county") or "").upper().strip()
        if county in _IKKE_LAND or str(r.get("country") or "Norge") != "Norge":
            continue
        lat, lon = _f(r.get("lat")), _f(r.get("lon"))
        if lat is None or lon is None or not str(r.get("baseId") or "").startswith("SN"):
            continue
        ut.append({
            "id": str(r["baseId"]), "sted": _stedsnavn(r), "fylke": _fylke(county),
            "lat": round(lat, 4), "lon": round(lon, 4),
            "temp": bool(r.get("has_temp")), "nedbor": bool(r.get("has_precip")),
            "sol": bool(r.get("has_sun")), "sno": bool(r.get("has_snow")),
        })
    return ut


_NORMAL_CACHE: dict[str, dict[str, Any]] = {}


def _normaler() -> dict[str, dict[str, Any]]:
    if _NORMAL_CACHE:
        return _NORMAL_CACHE
    try:
        data = json.loads(_NORMALER.read_text(encoding="utf-8"))
    except Exception:
        return {}
    for s in data.get("stasjoner", []):
        _NORMAL_CACHE[str(s.get("id"))] = {"temp": s.get("temp"), "nedbor": s.get("nedbor")}
    return _NORMAL_CACHE


def _varsel_utvalg(stasjoner: list[dict[str, Any]], maks: int) -> list[dict[str, Any]]:
    """Spre varselpunktene over hele landet: ett per rute på ca. 30 x 30 km,
    stasjoner med nedbørmåling først (de har ofte lang historikk)."""
    kandidater = sorted((s for s in stasjoner if s["temp"]), key=lambda s: (not s["nedbor"], s["id"]))
    valgt: dict[tuple[int, int], dict[str, Any]] = {}
    for s in kandidater:
        rute = (int(s["lat"] / 0.3), int(s["lon"] / 0.6))
        valgt.setdefault(rute, s)
    ut = list(valgt.values())
    if len(ut) > maks:
        steg = len(ut) / maks
        ut = [ut[int(i * steg)] for i in range(maks)]
    return ut


# ---------------------------------------------------------------------------
# Frost
# ---------------------------------------------------------------------------

def _frost_auth() -> Optional[tuple[str, str]]:
    cid = os.getenv("FROST_CLIENT_ID", "").strip()
    if not cid:
        return None
    return cid, os.getenv("FROST_CLIENT_SECRET", "")


def _frost_hent(ids: list[str], elementer: list[str], referencetime: str,
                timeout: int = 40) -> list[dict[str, Any]]:
    """Alle observasjoner for elementene i intervallet: [{id, el, t, v}]."""
    auth = _frost_auth()
    if auth is None:
        raise RuntimeError("FROST_CLIENT_ID mangler")
    ut: list[dict[str, Any]] = []

    def kall(s: requests.Session, kilder: list[str], els: list[str]) -> Optional[dict[str, Any]]:
        params = {"sources": ",".join(kilder), "elements": ",".join(els), "referencetime": referencetime,
                  "timeoffsets": "default", "levels": "default", "qualities": "0,1,2,3,4"}
        for forsok in range(4):
            try:
                r = s.get(FROST_OBS, params=params, auth=auth, timeout=timeout,
                          headers={"Accept": "application/json"})
            except requests.RequestException:
                time.sleep(1 + forsok)
                continue
            if r.status_code == 200:
                return r.json()
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(2 ** forsok)
                continue
            if r.status_code == 400 and len(els) > 1:
                # Ett ukjent element ødelegger hele spørringen. Prøv dem hver for seg.
                samlet: dict[str, Any] = {"data": []}
                for el in els:
                    del_ = kall(s, kilder, [el])
                    if del_:
                        samlet["data"].extend(del_.get("data") or [])
                return samlet
            return None  # 404/412: ingen data for denne bunken
        return None

    with requests.Session() as s:
        for i in range(0, len(ids), _FROST_BATCH):
            svar = kall(s, ids[i:i + _FROST_BATCH], elementer)
            if not svar:
                continue
            for item in svar.get("data", []) or []:
                sid = str(item.get("sourceId") or "").split(":")[0]
                t = item.get("referenceTime")
                for obs in item.get("observations", []) or []:
                    v = _f(obs.get("value"))
                    if v is not None:
                        ut.append({"id": sid, "el": obs.get("elementId"),
                                   "t": obs.get("referenceTime") or t, "v": v})
    return ut


def _dag(t: str) -> date:
    """Dato for en P1D-verdi. Frost stempler døgnet med starttidspunktet
    (06 UTC for nedbør, 00/18 UTC for temperatur), så vi bruker norsk dato
    for tidspunktet pluss seks timer."""
    dt = datetime.fromisoformat(str(t).replace("Z", "+00:00"))
    return (dt + timedelta(hours=6)).astimezone(OSLO).date()


def _type(el: str) -> Optional[str]:
    el = el or ""
    if "air_temperature" in el:
        if el.startswith(("max", "best_estimate_max")):
            return "tmax"
        if el.startswith(("min", "best_estimate_min")):
            return "tmin"
        if el.startswith(("mean", "best_estimate_mean")):
            return "tmid"
    if "precipitation_amount" in el:
        return "nedbor"
    if "wind_speed_of_gust" in el:
        return "kast"
    if "duration_of_sunshine" in el:
        return "sol"
    if "surface_snow_thickness" in el:
        return "sno"
    return None


_DOGN_ELEMENTER = [
    "max(air_temperature P1D)", "min(air_temperature P1D)", "mean(air_temperature P1D)",
    "sum(precipitation_amount P1D)", "max(wind_speed_of_gust P1D)",
    "sum(duration_of_sunshine P1D)", "surface_snow_thickness",
]
_MAANED_ELEMENTER = [
    "max(air_temperature P1M)", "min(air_temperature P1M)",
    "sum(precipitation_amount P1M)", "max(wind_speed_of_gust P1M)",
    "sum(duration_of_sunshine P1M)", "max(surface_snow_thickness P1M)",
]


def _rydd(rader: Iterable[dict[str, Any]]) -> dict[str, dict[str, dict[Any, float]]]:
    """{type: {stasjon: {dato/måned: verdi}}} med målefeil fjernet."""
    ut: dict[str, dict[str, dict[Any, float]]] = {}
    for r in rader:
        typ = _type(r["el"])
        if typ is None:
            continue
        v = r["v"]
        grense = {"tmax": "temp", "tmin": "temp", "tmid": "temp", "nedbor": "nedbor_dogn",
                  "kast": "kast", "sol": "sol_dogn", "sno": "sno"}[typ]
        if "P1M" in r["el"]:
            if typ == "nedbor" and not 0 <= v <= 3000:
                continue
            if typ == "sol" and not 0 <= v <= 400:
                continue
            if typ not in ("nedbor", "sol") and not _innenfor(v, grense):
                continue
            noekkel: Any = str(r["t"])[:7]
        else:
            if not _innenfor(v, grense):
                continue
            noekkel = _dag(r["t"])
        ut.setdefault(typ, {}).setdefault(r["id"], {})[noekkel] = v
    return ut


# ---------------------------------------------------------------------------
# Rangering
# ---------------------------------------------------------------------------

def _topp(verdier: dict[str, float], meta: dict[str, dict[str, Any]], *, synkende: bool,
          n: int = 3) -> list[tuple[dict[str, Any], float]]:
    """De n beste stedene, ett per stedsnavn."""
    rekke = sorted(((sid, v) for sid, v in verdier.items() if sid in meta),
                   key=lambda x: x[1], reverse=synkende)
    ut, sett = [], set()
    for sid, v in rekke:
        navn = meta[sid]["sted"]
        if navn in sett:
            continue
        sett.add(navn)
        ut.append((meta[sid], v))
        if len(ut) == n:
            break
    return ut


def _rekord(verdier: dict[str, float], meta: dict[str, dict[str, Any]], *, synkende: bool = True,
            merk: Callable[[dict[str, Any], float], str] | None = None,
            runde: int = 1, **ekstra: Any) -> Optional[dict[str, Any]]:
    topp = _topp(verdier, meta, synkende=synkende)
    if not topp:
        return None
    s, v = topp[0]
    return {
        "v": round(v, runde), "sted": s["sted"], "fylke": s["fylke"], "lat": s["lat"], "lon": s["lon"],
        "id": s["id"], "merk": merk(s, v) if merk else "",
        "topp": [[t["sted"], round(tv, runde)] for t, tv in topp[1:]],
        **ekstra,
    }


def _kast_merk(v: float) -> str:
    if v >= 32.7:
        return "Vindkast på orkans styrke."
    if v >= 24.5:
        return "Vindkast på storms styrke. Orkan er fra 32,7 m/s."
    return "Sterkeste vindkast. Orkan er fra 32,7 m/s."


def _andel(verdier: dict[str, float], test: Callable[[float], bool]) -> Optional[int]:
    if len(verdier) < 5:
        return None
    return round(100 * sum(1 for v in verdier.values() if test(v)) / len(verdier))


def _fakta(tmin: dict[str, float], tmax: dict[str, float], ned: Optional[dict[str, float]],
           frost_tekst: str, regn_tekst: str = "", varm_grense: float = 10.0, varm_tekst: str = "") -> list[list[str]]:
    """Små tall under spennet, f.eks. hvor stor del av landet som hadde frost."""
    ut: list[list[str]] = []
    frost = _andel(tmin, lambda v: v < 0)
    if frost is not None:
        ut.append([f"{frost} %", frost_tekst])
    if ned is not None and regn_tekst:
        regn = _andel(ned, lambda v: v >= 1.0)
        if regn is not None:
            ut.append([f"{regn} %", regn_tekst])
    if tmax:
        n = sum(1 for v in tmax.values() if v > varm_grense)
        ut.append([str(n), varm_tekst or f"stasjoner over {round(varm_grense)}°"])
    return ut


def _fyll_spenn(p: dict[str, Any]) -> dict[str, Any]:
    if p.get("varm") and p.get("kald"):
        p["spenn"] = round(p["varm"]["v"] - p["kald"]["v"], 1)
    return p


# ---------------------------------------------------------------------------
# Delene
# ---------------------------------------------------------------------------

def _siste_dato(per_stasjon: dict[str, dict[Any, float]]) -> Optional[date]:
    """Nyeste dato som har med minst 60 % så mange stasjoner som den
    beste datoen. Unngår at noen få tidlige rapporter blir 'siste døgn'."""
    teller: dict[date, int] = {}
    for serie in per_stasjon.values():
        for d in serie:
            teller[d] = teller.get(d, 0) + 1
    if not teller:
        return None
    topp = max(teller.values())
    return max(d for d, n in teller.items() if n >= topp * 0.6)


def bygg_sist(stasjoner: list[dict[str, Any]], naa: Optional[datetime] = None) -> dict[str, Any]:
    naa = naa or datetime.now(timezone.utc)
    meta = {s["id"]: s for s in stasjoner}
    fra = (naa - timedelta(days=3)).strftime("%Y-%m-%dT%H:%M:%SZ")
    rader = _frost_hent(list(meta), _DOGN_ELEMENTER, f"{fra}/{naa.strftime('%Y-%m-%dT%H:%M:%SZ')}")
    data = _rydd(rader)
    normal = _normaler()

    def paa_dato(typ: str) -> tuple[Optional[date], dict[str, float]]:
        serie = data.get(typ, {})
        d = _siste_dato(serie)
        if d is None:
            return None, {}
        return d, {sid: v[d] for sid, v in serie.items() if d in v}

    p: dict[str, Any] = {"tittel": "siste døgn", "kart": "rekordene siste døgn"}
    d_tmax, tmax = paa_dato("tmax")
    d_tmin, tmin = paa_dato("tmin")
    d_ned, ned = paa_dato("nedbor")
    d_kast, kast = paa_dato("kast")
    d_sol, sol = paa_dato("sol")
    d_sno, sno = paa_dato("sno")
    hoved = d_tmax or d_ned or d_tmin

    def mnd_navn(d: Optional[date]) -> str:
        return MAANEDER[(d or naa.date()).month - 1]

    def varm_merk(s: dict[str, Any], v: float) -> str:
        n = (normal.get(s["id"]) or {}).get("temp")
        tekst = f"Høyeste maks {_dato_tekst(d_tmax)}." if d_tmax else "Høyeste maks."
        if n and d_tmax:
            tekst += f" Normal middeltemperatur i {mnd_navn(d_tmax)}: {_tall(n[d_tmax.month - 1])}°."
        return tekst

    def vaat_merk(s: dict[str, Any], v: float) -> str:
        n = (normal.get(s["id"]) or {}).get("nedbor")
        if n and d_ned and n[d_ned.month - 1] > 0:
            pst = round(100 * v / n[d_ned.month - 1])
            return f"{pst} % av det som normalt kommer i hele {mnd_navn(d_ned)}."
        return "Nedbør siste døgn."

    def sno_merk(s: dict[str, Any], v: float) -> str:
        serie = data.get("sno", {}).get(s["id"], {})
        if d_sno and (d_sno - timedelta(days=1)) in serie:
            ny = v - serie[d_sno - timedelta(days=1)]
            if ny >= 1:
                return f"Snødybde. {round(ny)} cm nysnø siste døgn."
        return "Snødybde."

    p["varm"] = _rekord(tmax, meta, merk=varm_merk)
    p["kald"] = _rekord(tmin, meta, synkende=False,
                        merk=lambda s, v: f"Laveste min {_dato_tekst(d_tmin)}." if d_tmin else "Laveste min.")
    p["vaat"] = _rekord(ned, meta, merk=vaat_merk)
    p["vind"] = _rekord(kast, meta, merk=lambda s, v: _kast_merk(v), kast=True)
    p["sol"] = _rekord(sol, meta, merk=lambda s, v: "Soltimer målt gjennom døgnet.")
    p["sno"] = _rekord(sno, meta, merk=sno_merk, runde=0)
    antall = len(set(tmax) | set(tmin) | set(ned) | set(kast))
    p["n"] = antall
    p["fakta"] = _fakta(tmin, tmax, ned, "av stasjonene hadde frost", "fikk minst 1 mm nedbør")
    if hoved:
        p["lead"] = f"Målt {UKEDAGER[hoved.weekday()]} {_dato_tekst(hoved)} ved {antall} stasjoner."
        p["dato"] = hoved.isoformat()
    else:
        p["lead"] = f"Målt siste døgn ved {antall} stasjoner."
    return _fyll_spenn(p)


def _mnd_agg(data: dict[str, dict[str, dict[Any, float]]], fra: date
             ) -> dict[str, dict[str, tuple[float, Any]]]:
    """Per type og stasjon: (verdi, dato/dekning) for dager fra og med `fra`."""
    ut: dict[str, dict[str, tuple[float, Any]]] = {}
    for typ, serier in data.items():
        for sid, serie in serier.items():
            dager = {d: v for d, v in serie.items() if isinstance(d, date) and d >= fra}
            if not dager:
                continue
            if typ == "tmax" or typ == "kast" or typ == "sno":
                d = max(dager, key=lambda k: dager[k])
                ut.setdefault(typ, {})[sid] = (dager[d], d)
            elif typ == "tmin":
                d = min(dager, key=lambda k: dager[k])
                ut.setdefault(typ, {})[sid] = (dager[d], d)
            elif typ in ("nedbor", "sol"):
                ut.setdefault(typ, {})[sid] = (sum(dager.values()), len(dager))
            elif typ == "tmid":
                ut.setdefault(typ, {})[sid] = (sum(dager.values()) / len(dager), len(dager))
    return ut


def bygg_mnd(stasjoner: list[dict[str, Any]], naa: Optional[datetime] = None
             ) -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, Any]]:
    """Denne måneden + 'Uvanlig nå'. Returnerer (periode, uvanlig, agg for året)."""
    naa = naa or datetime.now(timezone.utc)
    lokal = naa.astimezone(OSLO).date()
    # Den første i måneden finnes det knapt tall, så da viser vi forrige måned.
    start = lokal.replace(day=1) if lokal.day > 1 else (lokal - timedelta(days=1)).replace(day=1)
    slutt_dato = lokal if lokal.day > 1 else lokal - timedelta(days=1)
    meta = {s["id"]: s for s in stasjoner}
    fra = (datetime(start.year, start.month, start.day, tzinfo=OSLO) - timedelta(days=1)).astimezone(timezone.utc)
    rader = _frost_hent(list(meta), _DOGN_ELEMENTER,
                        f"{fra.strftime('%Y-%m-%dT%H:%M:%SZ')}/{naa.strftime('%Y-%m-%dT%H:%M:%SZ')}")
    data = _rydd(rader)
    agg = _mnd_agg(data, start)
    mnavn = MAANEDER[start.month - 1]
    dager_gaatt = (slutt_dato - start).days + 1
    normal = _normaler()

    def verdier(typ: str) -> dict[str, float]:
        return {sid: v for sid, (v, _) in agg.get(typ, {}).items()}

    def dato_merk(typ: str, prefiks: str) -> Callable[[dict[str, Any], float], str]:
        def f(s: dict[str, Any], v: float) -> str:
            d = agg.get(typ, {}).get(s["id"], (None, None))[1]
            return f"{prefiks} {_dato_tekst(d)}." if isinstance(d, date) else f"{prefiks}."
        return f

    def vaat_merk(s: dict[str, Any], v: float) -> str:
        n = (normal.get(s["id"]) or {}).get("nedbor")
        if n and n[start.month - 1] > 0:
            hittil = n[start.month - 1] * dager_gaatt / _dager_i_mnd(start)
            return f"{round(100 * v / hittil)} % av normalen så langt."
        return f"Sum for {mnavn} så langt."

    # Summer krever at stasjonen har rapportert de fleste dagene.
    def summer(typ: str) -> dict[str, float]:
        krav = max(1, int(dager_gaatt * 0.8))
        return {sid: v for sid, (v, n) in agg.get(typ, {}).items() if n >= krav}

    p: dict[str, Any] = {"tittel": f"i {mnavn}", "kart": f"rekordene i {mnavn}",
                         "lead": f"Rekordene så langt i {mnavn}, målt."}
    p["varm"] = _rekord(verdier("tmax"), meta, merk=dato_merk("tmax", "Høyeste maks"))
    p["kald"] = _rekord(verdier("tmin"), meta, synkende=False, merk=dato_merk("tmin", "Laveste min"))
    p["vaat"] = _rekord(summer("nedbor"), meta, merk=vaat_merk, runde=0)
    p["vind"] = _rekord(verdier("kast"), meta, merk=dato_merk("kast", "Sterkeste kast"), kast=True)
    p["sol"] = _rekord(summer("sol"), meta, merk=lambda s, v: f"Sum soltimer i {mnavn} så langt.", runde=0)
    p["sno"] = _rekord(verdier("sno"), meta, merk=dato_merk("sno", "Største snødybde"), runde=0)
    p["n"] = len(set(verdier("tmax")) | set(verdier("nedbor")))
    p["fakta"] = _fakta(verdier("tmin"), verdier("tmax"), None, f"av stasjonene har hatt frost i {mnavn}",
                        varm_tekst=f"stasjoner har hatt over 10° i {mnavn}")
    _fyll_spenn(p)

    uvanlig = _uvanlig(agg, meta, start, dager_gaatt, normal)
    return p, uvanlig, {"maaned": start.strftime("%Y-%m"), "agg": {
        typ: {sid: [v, d.isoformat() if isinstance(d, date) else d] for sid, (v, d) in serier.items()}
        for typ, serier in agg.items() if typ != "tmid"}}


def _dager_i_mnd(d: date) -> int:
    neste = (d.replace(day=28) + timedelta(days=4)).replace(day=1)
    return (neste - d.replace(day=1)).days


_STORBYER = [("SN18700", "Oslo"), ("SN50540", "Bergen"), ("SN68860", "Trondheim"),
             ("SN44560", "Stavanger"), ("SN90450", "Tromsø")]


def _uvanlig(agg: dict[str, dict[str, tuple[float, Any]]], meta: dict[str, dict[str, Any]],
             start: date, dager: int, normal: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    """Avvik fra normalen 1991–2020, laget som korte setninger."""
    if dager < 3:
        return []
    m = start.month - 1
    mnavn = MAANEDER[m]
    ut: list[dict[str, Any]] = []

    # Temperatur: snitt av døgnmiddel mot normal middeltemperatur.
    avvik: dict[str, float] = {}
    for sid, (v, n) in agg.get("tmid", {}).items():
        nt = (normal.get(sid) or {}).get("temp")
        if nt and n >= max(3, int(dager * 0.8)) and sid in meta:
            avvik[sid] = v - nt[m]

    byer = [(navn, avvik[sid]) for sid, navn in _STORBYER if sid in avvik]
    if len(byer) >= 3:
        deler = ", ".join(f"{navn} {'+' if a >= 0 else ''}{_tall(a)}°" for navn, a in byer)
        snitt = sum(a for _, a in byer) / len(byer)
        ut.append({"sted": "Storbyene", "type": "varm" if snitt >= 0 else "kald",
                   "tag": ("+" if snitt >= 0 else "") + _tall(snitt) + "°",
                   "tekst": f"mot normalt så langt i {mnavn}: {deler}."})

    for synk, typ in ((True, "varm"), (False, "kald")):
        topp = _topp(avvik, meta, synkende=synk, n=1)
        if topp and abs(topp[0][1]) >= 1.5:
            s, a = topp[0]
            ord_ = "varmere" if a > 0 else "kaldere"
            ut.append({"sted": s["sted"], "type": typ, "tag": ("+" if a > 0 else "") + _tall(a) + "°",
                       "tekst": f"har vært {_tall(abs(a))}° {ord_} enn normalt så langt i {mnavn}."})

    # Nedbør i prosent av normalen hittil i måneden.
    pst: dict[str, float] = {}
    mm: dict[str, float] = {}
    for sid, (v, n) in agg.get("nedbor", {}).items():
        nn = (normal.get(sid) or {}).get("nedbor")
        if not nn or sid not in meta or n < max(3, int(dager * 0.8)):
            continue
        hittil = nn[m] * dager / _dager_i_mnd(start)
        if nn[m] >= 30 and hittil > 5:
            pst[sid] = 100 * v / hittil
            mm[sid] = v
    if dager >= 5:
        torr = _topp(pst, meta, synkende=False, n=1)
        if torr and torr[0][1] <= 50:
            s, x = torr[0]
            ut.append({"sted": s["sted"], "type": "sol", "tag": f"{round(x)} %",
                       "tekst": f"har fått {_tall(mm[s['id']], 0)} mm i {mnavn}, {round(x)} % av det som er normalt hittil."})
        vaat = _topp(pst, meta, synkende=True, n=1)
        if vaat and vaat[0][1] >= 150:
            s, x = vaat[0]
            ut.append({"sted": s["sted"], "type": "vaat", "tag": f"{round(x)} %",
                       "tekst": f"har fått {_tall(mm[s['id']], 0)} mm i {mnavn}, {round(x)} % av det som er normalt hittil."})

    # Kalde netter: flest døgn med minimum under −20.
    return ut


def bygg_aar(stasjoner: list[dict[str, Any]], mnd_agg: Optional[dict[str, Any]],
             naa: Optional[datetime] = None) -> dict[str, Any]:
    """Året så langt: fullførte måneder fra P1M, pluss inneværende måned fra
    døgnverdiene som allerede er hentet for 'mnd'."""
    naa = naa or datetime.now(timezone.utc)
    lokal = naa.astimezone(OSLO).date()
    meta = {s["id"]: s for s in stasjoner}
    mnd_start = lokal.replace(day=1)
    agg_mnd = (mnd_agg or {}).get("agg", {}) if (mnd_agg or {}).get("maaned") == mnd_start.strftime("%Y-%m") else {}

    per: dict[str, dict[str, tuple[float, str]]] = {}
    if lokal.month > 1:
        rader = _frost_hent(list(meta), _MAANED_ELEMENTER, f"{lokal.year}-01-01/{mnd_start.isoformat()}")
        for typ, serier in _rydd(rader).items():
            for sid, serie in serier.items():
                for mnd, v in serie.items():
                    if not str(mnd).startswith(str(lokal.year)):
                        continue
                    per.setdefault(typ, {}).setdefault(sid, []).append((v, str(mnd)))  # type: ignore[arg-type]
    for typ, serier in agg_mnd.items():
        for sid, (v, d) in serier.items():
            per.setdefault(typ, {}).setdefault(sid, []).append((v, str(d)))  # type: ignore[arg-type]

    def velg(typ: str, f: Callable) -> dict[str, tuple[float, str]]:
        return {sid: f(liste) for sid, liste in per.get(typ, {}).items() if liste}  # type: ignore[arg-type]

    tmax = velg("tmax", lambda li: max(li))
    tmin = velg("tmin", lambda li: min(li))
    kast = velg("kast", lambda li: max(li))
    sno = velg("sno", lambda li: max(li))
    # Summer bare for stasjoner som har tall for (nesten) alle månedene.
    mnd_tall = lokal.month
    ned = {sid: (sum(v for v, _ in li), "") for sid, li in per.get("nedbor", {}).items()
           if len(li) >= mnd_tall - 1}
    sol = {sid: (sum(v for v, _ in li), "") for sid, li in per.get("sol", {}).items()
           if len(li) >= mnd_tall - 1}

    def naar(d: str) -> str:
        if len(d) >= 10:
            return _dato_tekst(date.fromisoformat(d[:10]))
        if len(d) == 7:
            return MAANEDER[int(d[5:7]) - 1]
        return ""

    def merk(serie: dict[str, tuple[float, str]], tekst: str) -> Callable[[dict[str, Any], float], str]:
        def f(s: dict[str, Any], v: float) -> str:
            n = naar(serie.get(s["id"], (0, ""))[1])
            return f"{tekst} ({n})." if n else f"{tekst}."
        return f

    def v(serie: dict[str, tuple[float, str]]) -> dict[str, float]:
        return {sid: x for sid, (x, _) in serie.items()}

    p: dict[str, Any] = {"tittel": "i år", "kart": "årets rekorder",
                         "lead": f"Årets rekorder i Norge, målt fra 1. januar til {_dato_tekst(lokal)}."}
    p["varm"] = _rekord(v(tmax), meta, merk=merk(tmax, "Varmeste dag i år"))
    p["kald"] = _rekord(v(tmin), meta, synkende=False, merk=merk(tmin, "Kaldeste dag i år"))
    p["vaat"] = _rekord(v(ned), meta, merk=lambda s, x: "Sum nedbør i år.", runde=0)
    p["vind"] = _rekord(v(kast), meta, merk=merk(kast, "Sterkeste kast i år"), kast=True)
    p["sol"] = _rekord(v(sol), meta, merk=lambda s, x: "Sum soltimer i år.", runde=0)
    p["sno"] = _rekord(v(sno), meta, merk=merk(sno, "Største snødybde i år"), runde=0)
    p["n"] = len(set(tmax) | set(ned))
    p["fakta"] = _fakta(v(tmin), v(tmax), None, "av stasjonene har hatt frost i år", varm_grense=25,
                        varm_tekst="stasjoner har hatt over 25° i år")
    return _fyll_spenn(p)


# ---------------------------------------------------------------------------
# Varsel fra Yr
# ---------------------------------------------------------------------------

def _sol_hoyde(t: datetime, lat: float, lon: float) -> float:
    """Solhøyde i grader (enkel formel, godt nok for soltimer)."""
    dag = t.timetuple().tm_yday
    deklinasjon = -23.44 * math.cos(math.radians(360 / 365 * (dag + 10)))
    timer = t.hour + t.minute / 60 + lon / 15
    vinkel = math.radians(15 * (timer - 12))
    la, de = math.radians(lat), math.radians(deklinasjon)
    s = math.sin(la) * math.sin(de) + math.cos(la) * math.cos(de) * math.cos(vinkel)
    return math.degrees(math.asin(max(-1.0, min(1.0, s))))


def _steg(ts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Gjør Yr-tidsserien om til ikke-overlappende steg med varighet."""
    ut = []
    tider = []
    for it in ts:
        try:
            tider.append(datetime.fromisoformat(str(it["time"]).replace("Z", "+00:00")))
        except (KeyError, ValueError):
            tider.append(None)
    for i, it in enumerate(ts):
        t = tider[i]
        if t is None:
            continue
        neste = tider[i + 1] if i + 1 < len(tider) else None
        timer = 1 if neste is None else max(1, round((neste - t).total_seconds() / 3600))
        data = it.get("data") or {}
        inst = (data.get("instant") or {}).get("details") or {}
        blokk = None
        for k, h in (("next_1_hours", 1), ("next_6_hours", 6), ("next_12_hours", 12)):
            if h >= timer and data.get(k):
                blokk = (data[k].get("details") or {}, h)
                break
        if blokk is None and neste is None:
            break
        nedbor = lo = hi = 0.0
        if blokk:
            det, h = blokk
            andel = min(1.0, timer / h)
            nedbor = (_f(det.get("precipitation_amount")) or 0.0) * andel
            lo = (_f(det.get("precipitation_amount_min")) or 0.0) * andel
            hi = (_f(det.get("precipitation_amount_max")) or nedbor) * andel
        ut.append({"t": t, "timer": timer, "temp": _f(inst.get("air_temperature")),
                   "kast": _f(inst.get("wind_speed_of_gust")), "sky": _f(inst.get("cloud_area_fraction")),
                   "nedbor": nedbor, "lo": lo, "hi": hi})
    return ut


def _varsel_for(ts: list[dict[str, Any]], lat: float, lon: float, fra: datetime, til: datetime
                ) -> dict[str, float]:
    tmax = tmin = kast = None
    nedbor = lo = hi = sol = sno = 0.0
    dekket = 0.0
    for s in _steg(ts):
        if s["t"] < fra or s["t"] >= til:
            continue
        dekket += s["timer"]
        if s["temp"] is not None:
            tmax = s["temp"] if tmax is None else max(tmax, s["temp"])
            tmin = s["temp"] if tmin is None else min(tmin, s["temp"])
        if s["kast"] is not None:
            kast = s["kast"] if kast is None else max(kast, s["kast"])
        nedbor += s["nedbor"]
        lo += s["lo"]
        hi += s["hi"]
        sno += _nysno_cm(s["nedbor"], s["temp"])
        if s["sky"] is not None:
            oppe = sum(1 for k in range(s["timer"])
                       if _sol_hoyde(s["t"] + timedelta(hours=k, minutes=30), lat, lon) > 0)
            sol += oppe * (1 - max(0.0, min(100.0, s["sky"])) / 100)
    ut: dict[str, float] = {"dekket": dekket, "nedbor": nedbor, "lo": lo, "hi": hi, "sol": sol, "sno": sno}
    if tmax is not None:
        ut["tmax"], ut["tmin"] = tmax, tmin  # type: ignore[assignment]
    if kast is not None:
        ut["kast"] = kast
    return ut


def _hent_yr(lat: float, lon: float) -> Optional[list[dict[str, Any]]]:
    from yr_forecast import _fetch_yr_timeseries

    with requests.Session() as s:
        return _fetch_yr_timeseries(lat, lon, s)


def bygg_varsel(stasjoner: list[dict[str, Any]], naa: Optional[datetime] = None,
                hent: Callable[[float, float], Optional[list]] | None = None
                ) -> tuple[dict[str, Any], dict[str, Any]]:
    naa = (naa or datetime.now(timezone.utc)).replace(minute=0, second=0, microsecond=0)
    hent = hent or _hent_yr
    utvalg = _varsel_utvalg(stasjoner, _VARSEL_MAKS_STASJONER)
    vinduer = {"neste": (naa, naa + timedelta(hours=24), 20), "uke": (naa, naa + timedelta(days=7), 150)}
    res: dict[str, dict[str, dict[str, float]]] = {"neste": {}, "uke": {}}

    def en(s: dict[str, Any]) -> tuple[str, dict[str, dict[str, float]]]:
        ts = hent(s["lat"], s["lon"])
        if not ts:
            return s["id"], {}
        return s["id"], {k: _varsel_for(ts, s["lat"], s["lon"], fra, til) for k, (fra, til, _) in vinduer.items()}

    with ThreadPoolExecutor(max_workers=_VARSEL_TRAADER) as ex:
        for fut in as_completed([ex.submit(en, s) for s in utvalg]):
            try:
                sid, r = fut.result()
            except Exception:
                continue
            for k, v in r.items():
                if v.get("dekket", 0) >= vinduer[k][2]:
                    res[k][sid] = v

    meta = {s["id"]: s for s in utvalg}
    ut = []
    for k in ("neste", "uke"):
        r = res[k]

        def felt(navn: str) -> dict[str, float]:
            return {sid: v[navn] for sid, v in r.items() if navn in v}

        neste = k == "neste"
        tid = "neste døgn" if neste else "neste 7 døgn"
        p: dict[str, Any] = {
            "tittel": tid, "kart": "det som er ventet " + ("neste døgn" if neste else "neste uke"),
            "lead": f"Varslet for {'de neste 24 timene' if neste else 'den kommende uken'} "
                    f"fra Yr, for {len(r)} steder over hele landet.",
        }

        def vaat_merk(s: dict[str, Any], v: float, r: dict = r, neste: bool = neste) -> str:
            lo, hi = r[s["id"]]["lo"], r[s["id"]]["hi"]
            spenn = f" Kan bli {_tall(lo, 0)} til {_tall(hi, 0)} mm." if hi - lo >= 2 else ""
            return ("Sum neste 24 timer." if neste else "Sum for uken.") + spenn

        p["varm"] = _rekord(felt("tmax"), meta, merk=lambda s, v: "Høyeste temperatur i varselet.")
        p["kald"] = _rekord(felt("tmin"), meta, synkende=False, merk=lambda s, v: "Laveste temperatur i varselet.")
        p["vaat"] = _rekord(felt("nedbor"), meta, merk=vaat_merk, runde=1 if neste else 0)
        p["vind"] = _rekord(felt("kast"), meta, merk=lambda s, v: _kast_merk(v), kast=True)
        p["sol"] = _rekord(felt("sol"), meta, merk=lambda s, v: "Beregnet fra skydekket i varselet.",
                           runde=1 if neste else 0)
        snoe = {sid: x for sid, x in felt("sno").items() if x >= 1}
        p["sno"] = _rekord(snoe, meta, merk=lambda s, v: "Nysnø, beregnet fra nedbør og temperatur.",
                           runde=0, nysno=True)
        p["n"] = len(r)
        p["fakta"] = _fakta(felt("tmin"), felt("tmax"), felt("nedbor"), "av stedene får frost",
                            "får minst 1 mm nedbør", varm_tekst="steder over 10°")
        ut.append(_fyll_spenn(p))
    return ut[0], ut[1]


# ---------------------------------------------------------------------------
# Lagring
# ---------------------------------------------------------------------------

def _s3() -> Optional[tuple[Any, str]]:
    bucket = (os.getenv("KLIMA_NORGE_S3_BUCKET", "").strip()
              or os.getenv("WEATHER_STATION_METRICS_S3_BUCKET", "").strip()
              or os.getenv("S3_BUCKET_NAME", "").strip())
    if not bucket:
        return None
    try:
        import boto3

        klient = boto3.client("s3", region_name=os.getenv("AWS_REGION", os.getenv("AWS_DEFAULT_REGION", "eu-north-1")))
    except Exception:
        return None
    return klient, bucket


def _les_lagret() -> dict[str, Any]:
    s3 = _s3()
    try:
        if s3 is not None:
            klient, bucket = s3
            return json.loads(klient.get_object(Bucket=bucket, Key=_S3_KEY)["Body"].read())
        if _LOKAL.exists():
            return json.loads(_LOKAL.read_text(encoding="utf-8"))
    except Exception:
        log.debug("klima_norge: fant ingen lagret payload", exc_info=True)
    return {}


def _lagre(data: dict[str, Any]) -> None:
    tekst = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
    s3 = _s3()
    if s3 is not None:
        klient, bucket = s3
        klient.put_object(Bucket=bucket, Key=_S3_KEY, Body=tekst.encode("utf-8"),
                          ContentType="application/json")
        return
    _LOKAL.parent.mkdir(parents=True, exist_ok=True)
    tmp = _LOKAL.with_suffix(".tmp")
    tmp.write_text(tekst, encoding="utf-8")
    tmp.replace(_LOKAL)


def _alder(data: dict[str, Any], del_: str) -> float:
    ts = (data.get("_oppdatert") or {}).get(del_)
    return time.time() - ts if ts else float("inf")


def oppfrisk(tving: bool = False) -> dict[str, Any]:
    """Henter de delene som er for gamle og lagrer resultatet."""
    global _DATA, _DATA_TS
    if not _OPPFRISKER.acquire(blocking=False):
        return hent()
    try:
        data = _les_lagret() or dict(_DATA)
        data.setdefault("_oppdatert", {})
        stasjoner = None
        endret = False

        def trenger(del_: str) -> bool:
            return tving or _alder(data, del_) > MAKS_ALDER[del_]

        def kjor(del_: str, fn: Callable[[], None]) -> None:
            nonlocal endret
            try:
                fn()
                data["_oppdatert"][del_] = time.time()
                endret = True
            except Exception as exc:  # behold forrige versjon
                log.warning("klima_norge: %s feilet: %s", del_, exc)

        if any(trenger(d) for d in PERIODER):
            stasjoner = _stasjoner()

        if trenger("sist") and _frost_auth():
            kjor("sist", lambda: data.__setitem__("sist", bygg_sist(stasjoner)))

        if trenger("mnd") and _frost_auth():
            def mnd() -> None:
                p, uv, agg = bygg_mnd(stasjoner)
                data["mnd"], data["uvanlig"], data["_mnd_agg"] = p, uv, agg
            kjor("mnd", mnd)

        if (trenger("neste") or trenger("uke")):
            def varsel() -> None:
                data["neste"], data["uke"] = bygg_varsel(stasjoner)
                data["_oppdatert"]["uke"] = time.time()
            kjor("neste", varsel)

        if trenger("aar") and _frost_auth():
            kjor("aar", lambda: data.__setitem__("aar", bygg_aar(stasjoner, data.get("_mnd_agg"))))

        if endret:
            data["oppdatert"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
            try:
                _lagre(data)
            except Exception as exc:
                log.warning("klima_norge: lagring feilet: %s", exc)
        with _LAAS:
            _DATA, _DATA_TS = data, time.time()
        return data
    finally:
        _OPPFRISKER.release()


def hent() -> dict[str, Any]:
    """Det siste vi har, uten nettverkskall mot Frost eller Yr. Leser lagret
    fil/S3 høyst hvert minutt, slik at alle prosesser ser samme tall."""
    global _DATA, _DATA_TS
    with _LAAS:
        if _DATA and time.time() - _DATA_TS < 60:
            return _DATA
    data = _les_lagret()
    with _LAAS:
        if data:
            _DATA = data
        _DATA_TS = time.time()
        return _DATA


def offentlig(data: Optional[dict[str, Any]] = None) -> dict[str, Any]:
    """Payloaden slik siden får den (uten interne felter)."""
    data = hent() if data is None else data
    ut: dict[str, Any] = {k: data.get(k) for k in PERIODER if data.get(k)}
    ut["uvanlig"] = data.get("uvanlig") or []
    ut["oppdatert"] = data.get("oppdatert")
    return ut


def start_warmup() -> None:
    """Bakgrunnsjobb: sjekker hvert femte minutt om noe må hentes på nytt."""
    global _STARTET
    if _STARTET or os.getenv("KLIMA_NORGE_WARMUP", "1").strip().lower() in {"0", "false", "nei", "off"}:
        return
    if os.getenv("PYTEST_CURRENT_TEST"):
        return
    _STARTET = True

    def loop() -> None:
        time.sleep(20)  # la appen starte først
        while True:
            try:
                oppfrisk()
            except Exception:
                log.exception("klima_norge: oppfrisking feilet")
            time.sleep(300)

    threading.Thread(target=loop, name="klima-norge", daemon=True).start()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    d = oppfrisk(tving=True)
    print(json.dumps(offentlig(d), ensure_ascii=False, indent=1)[:4000])
