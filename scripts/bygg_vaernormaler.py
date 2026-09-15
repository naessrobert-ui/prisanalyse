#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Bygg klimanormaler for norske værstasjoner fra Frost (MET Norway).

Kjøres én gang, og deretter på nytt når du vil ha oppdaterte tall (typisk når
et nytt kalenderår er ferdig):

    python -m scripts.bygg_vaernormaler

Bakgrunn
--------
Frost sitt offisielle *climatenormals*-endepunkt dekker 1991-2020, men har
bare 1088 nedbørstasjoner og 560 temperaturstasjoner - og mangler både
"antall døgn med >= 1 mm" og døgnmaks/døgnmin per måned.

MET publiserer derimot ferdig aggregerte *månedsobservasjoner* som dekker
langt flere stasjoner. Vi henter disse og regner normalene selv:

    sum(precipitation_amount P1M)                              ~1964 stasjoner
    number_of_days_gte(sum(precipitation_amount P1D) P1M 1.0)  ~1420 stasjoner
    mean(air_temperature P1M)                                  ~1028 stasjoner
    mean(max(air_temperature P1D) P1M)                          ~646 stasjoner
    mean(min(air_temperature P1D) P1M)                          ~672 stasjoner
    mean(wind_speed P1M)                                       se VIND_KANDIDATER
    max(wind_speed_of_gust P1M)                                se VIND_KANDIDATER

(tallene er stasjoner med minst 10 års dekning)

Vind
----
Vindnormaler er ikke like sammenlignbare som temperatur og nedbør, og bør
leses med det i bakhodet: målingene henger tett sammen med mastehøyde (10 m
er standarden, men langt fra alle følger den), le og eksponering på stedet,
og med instrumentbytter - overgangen fra kopp- til ultralydsensor gir hopp i
serien. To stasjoner få kilometer fra hverandre kan derfor skille mye på
middelvind uten at klimaet skiller tilsvarende. Derfor er kartteksten
forsiktig formulert, og stasjonens serielengde vises som for de andre
elementene.

Normalperiode
-------------
Vi bruker et rullerende vindu fra 1991 til siste hele kalenderår, og krever
minst ``MIN_AAR`` *komplette* år (alle 12 måneder til stede) per element. Det
gir langt flere stasjoner enn en streng 1991-2020-normal, og nyere stasjoner
kommer med. Antall år og faktisk periode lagres per stasjon per element, slik
at kartet kan vise det i popup - en stasjon med 12 år skal ikke leses som om
den var like solid som en med 35.

Output
------
``data/vaernormaler.parquet``      - full tabell (stasjon x element x måned)
``static/data/vaernormaler.json``  - kompakt fil som kartet laster i nettleseren
"""
from __future__ import annotations

import argparse
import json
import math
import os
import sys
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Optional

import pandas as pd
import requests
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.tettsteder import TETTSTEDER  # noqa: E402

load_dotenv(ROOT / ".env")

FROST_BASE = "https://frost.met.no"
TIMEOUT = 300
USER_AGENT = "prisanalyse.no/1.0 (kontakt@prisanalyse.no)"

# --- Elementer -------------------------------------------------------------
EL_NEDBOR = "sum(precipitation_amount P1M)"
EL_NEDBORDAGER = "number_of_days_gte(sum(precipitation_amount P1D) P1M 1.0)"
EL_TEMP = "mean(air_temperature P1M)"
EL_TMAX = "mean(max(air_temperature P1D) P1M)"
EL_TMIN = "mean(min(air_temperature P1D) P1M)"

ELEMENTER = [EL_NEDBOR, EL_NEDBORDAGER, EL_TEMP, EL_TMAX, EL_TMIN]

# Kortnavn brukt i parquet og JSON.
KORTNAVN = {
    EL_NEDBOR: "nedbor",
    EL_NEDBORDAGER: "nedbordager",
    EL_TEMP: "temp",
    EL_TMAX: "tmax",
    EL_TMIN: "tmin",
}

# Vind. To normaler, fordi de svarer på hvert sitt spørsmål:
#
#   vind      - middelvind, altså hvor luftig stedet er til vanlig. Dette er
#               den egentlige klimanormalen for vind, og den som lar seg
#               sammenligne mellom steder på samme måte som temperatur.
#   vindkast  - månedens sterkeste kast, midlet over årene: «hvor ille blir
#               det en vanlig januar her». Et *snitt* av alle kast ville vært
#               meningsløst - kast er per definisjon ytterpunkter.
#
# De månedsaggregerte vindelementene er dårligere dokumentert enn nedbør og
# temperatur, og hvilke varianter Frost faktisk fører er ikke opplagt. Derfor
# listes kandidater i prioritert rekkefølge, og ``velg_vindelementer``
# beholder den første Frost svarer på. Uten den sjekken ville ett elementnavn
# Frost ikke kjenner gitt 400 på *hele* observasjonskallet og drept
# kjøringen - også for de fem elementene som virker.
VIND_KANDIDATER = {
    "vind": [
        "mean(wind_speed P1M)",
        "mean(mean(wind_speed P1D) P1M)",
    ],
    "vindkast": [
        "max(wind_speed_of_gust P1M)",
        "max(max(wind_speed_of_gust PT1H) P1M)",
        "max(max(wind_speed_of_gust P1D) P1M)",
    ],
}

# Elementer der årsverdien er summen av månedene, ikke snittet.
SUMMERES = {"nedbor", "nedbordager"}

# Elementer der årsverdien er den høyeste måneden. For vindkast er hverken
# sum eller snitt et tall noen kan bruke: årsnormalen er kastet i den verste
# måneden - typisk januar. (Strengt tatt er det gjennomsnittet av månedsmaks
# for den måneden, ikke gjennomsnittet av årsmaks, som ligger litt høyere
# fordi den verste måneden varierer fra år til år. Forskjellen er små
# prosenter, og denne varianten faller ut av samme pipeline som resten.)
MAKSIMERES = {"vindkast"}

# --- Parametre -------------------------------------------------------------
START_AAR = 1991

# Nedre grense for å komme med i datasettet. Bevisst lav: kartet har en
# glidebryter for antall år, så det er bedre å ta med korte serier og merke
# dem enn å utelate dem. Gullfjellet (fra 2016) har 8 år og forsvant helt med
# et krav på 10. Stasjoner under ANBEFALT_AAR tegnes med hul ring i kartet.
MIN_AAR = 5
ANBEFALT_AAR = 10
# Stasjoner per Frost-kall. Målt til ~17 MB for 20 stasjoner x 35 år med fem
# elementer; med vind er det sju, så batchen er satt ned tilsvarende for å
# holde svarene på samme størrelse.
BATCH = 15
WORKERS = 5
MAKS_FORSOK = 5

# Hvor langt fra sentrum en stasjon kan ligge og fortsatt regnes som byens
# målestasjon. 15 km dekker 123 av 125 tettsteder; å øke til 25 km henter inn
# de to siste, men begynner samtidig å merke stasjoner som «Elverum» når de
# egentlig står to kommuner unna.
BY_MAKS_KM = 15.0

# Avveining mellom nærhet og serielengde i bymatchingen: hver kilometer fra
# sentrum «koster» så mange år av serien. Se koble_byer.
KM_KOSTNAD = 3.0

PARQUET_UT = ROOT / "data" / "vaernormaler.parquet"
JSON_UT = ROOT / "static" / "data" / "vaernormaler.json"


def _auth() -> tuple[str, str]:
    cid = os.getenv("FROST_CLIENT_ID")
    if not cid:
        raise SystemExit("Mangler FROST_CLIENT_ID (sett den i .env).")
    return (cid, os.getenv("FROST_CLIENT_SECRET", ""))


def _get(session: requests.Session, path: str, params: dict, auth) -> dict:
    """GET mot Frost med retry på 429 og 5xx."""
    url = f"{FROST_BASE}{path}"
    for forsok in range(MAKS_FORSOK):
        try:
            r = session.get(url, params=params, auth=auth, timeout=TIMEOUT,
                            headers={"User-Agent": USER_AGENT})
        except requests.RequestException:
            if forsok == MAKS_FORSOK - 1:
                raise
            time.sleep(2.0 * (forsok + 1))
            continue
        if r.status_code == 200:
            return r.json()
        if r.status_code == 404:
            # Frost svarer 404 når ingen av stasjonene har data i perioden.
            return {"data": []}
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(3.0 * (forsok + 1))
            continue
        raise RuntimeError(f"Frost {r.status_code} for {url}: {r.text[:300]}")
    raise RuntimeError(f"Ga opp mot {url}")


def _parse_tid(verdi: Optional[str]) -> Optional[datetime]:
    if not verdi:
        return None
    try:
        return datetime.fromisoformat(verdi.replace("Z", "+00:00"))
    except ValueError:
        return None


# ===========================================================================
# 0. Velg vindelementer
# ===========================================================================

def velg_vindelementer(session: requests.Session, auth) -> None:
    """Legg til de vindelementene Frost faktisk tilbyr i ``ELEMENTER``.

    Prøver kandidatene i ``VIND_KANDIDATER`` i rekkefølge og beholder den
    første som gir treff. Et element Frost ikke kjenner gir 400, og et som
    ingen stasjon rapporterer gir tom liste - begge deler hoppes over med en
    linje i loggen, slik at resten av kjøringen går som før.
    """
    for kort, kandidater in VIND_KANDIDATER.items():
        for element in kandidater:
            try:
                data = _get(session, "/observations/availableTimeSeries/v0.jsonld",
                            {"elements": element}, auth).get("data", [])
            except RuntimeError as exc:
                print(f"  {kort:12s} ikke tilgjengelig: {element} ({exc})")
                continue
            stasjoner = {str(r.get("sourceId", "")).split(":")[0] for r in data}
            stasjoner.discard("")
            if not stasjoner:
                print(f"  {kort:12s} ingen stasjoner: {element}")
                continue
            ELEMENTER.append(element)
            KORTNAVN[element] = kort
            print(f"  {kort:12s} bruker {element} ({len(stasjoner)} stasjoner)")
            break
        else:
            print(f"  {kort:12s} droppes - ingen av kandidatene finnes i Frost")


# ===========================================================================
# 1. Finn kandidatstasjoner
# ===========================================================================

def finn_kandidater(session: requests.Session, auth, slutt_aar: int) -> set[str]:
    """Stasjoner som har minst MIN_AAR med data innenfor normalvinduet.

    Vi bruker /observations/availableTimeSeries, som oppgir validFrom/validTo
    per tidsserie. Det er et grovt overslag - det sier ingenting om hull i
    serien - men godt nok til å slippe å spørre om observasjoner for stasjoner
    som åpenbart er for korte. Kravet om MIN_AAR *komplette* år håndheves
    senere, på faktiske data.
    """
    vindu_start = datetime(START_AAR, 1, 1, tzinfo=timezone.utc)
    vindu_slutt = datetime(slutt_aar + 1, 1, 1, tzinfo=timezone.utc)
    kandidater: set[str] = set()

    for element in ELEMENTER:
        data = _get(session, "/observations/availableTimeSeries/v0.jsonld",
                    {"elements": element}, auth).get("data", [])
        dekning: dict[str, float] = defaultdict(float)
        for rad in data:
            kilde = str(rad.get("sourceId", "")).split(":")[0]
            if not kilde:
                continue
            fra = _parse_tid(rad.get("validFrom"))
            til = _parse_tid(rad.get("validTo")) or vindu_slutt
            if fra is None:
                continue
            fra = max(fra, vindu_start)
            til = min(til, vindu_slutt)
            if til > fra:
                dekning[kilde] += (til - fra).days / 365.25
        traff = {k for k, v in dekning.items() if v >= MIN_AAR}
        kandidater |= traff
        print(f"  {KORTNAVN[element]:12s} {len(traff):5d} kandidater")

    return kandidater


# ===========================================================================
# 2. Stasjonsmetadata
# ===========================================================================

def hent_stasjoner(session: requests.Session, auth) -> dict[str, dict]:
    """Metadata for alle stasjoner, også nedlagte.

    ``validtime`` må settes eksplisitt: uten den returnerer Frost bare
    stasjoner som er i drift i dag, og vi mister rundt 500 av kandidatene -
    blant dem mange lange serier som ble avsluttet på 2000-tallet.
    """
    data = _get(session, "/sources/v0.jsonld", {
        "types": "SensorSystem",
        "validtime": "0000-01-01/9999-01-01",
    }, auth).get("data", [])
    ut: dict[str, dict] = {}
    for rad in data:
        sid = rad.get("id")
        geo = rad.get("geometry") or {}
        koord = geo.get("coordinates") or []
        if not sid or len(koord) < 2:
            continue
        # MET driver også stasjoner på Bouvetøya og i Antarktis. De er norske,
        # men hører ikke hjemme i et norgeskart - Troll ville ellers toppet
        # lista over tørreste steder med 86 mm i året. Svalbard og Jan Mayen
        # beholdes.
        if float(koord[1]) < 50:
            continue
        ut[sid] = {
            "id": sid,
            "navn": rad.get("name") or sid,
            "fylke": rad.get("county") or "",
            "kommune": rad.get("municipality") or "",
            "lon": round(float(koord[0]), 5),
            "lat": round(float(koord[1]), 5),
            "moh": rad.get("masl"),
            "aktiv": not rad.get("validTo"),
            "til": (rad.get("validTo") or "")[:10],
        }
    return ut


# ===========================================================================
# 3. Hent månedsobservasjoner
# ===========================================================================

def hent_batch(session: requests.Session, auth, kilder: list[str],
               slutt_aar: int) -> list[tuple[str, int, int, str, float]]:
    """Returnerer (stasjon, år, måned, kortnavn, verdi) for én batch."""
    svar = _get(session, "/observations/v0.jsonld", {
        "sources": ",".join(kilder),
        "elements": ",".join(ELEMENTER),
        "referencetime": f"{START_AAR}-01-01/{slutt_aar + 1}-01-01",
    }, auth)

    rader: list[tuple[str, int, int, str, float]] = []
    # Flere sensorer kan rapportere samme element samme måned. Vi beholder den
    # med lavest timeSeriesId, som er hovedserien for stasjonen.
    sett: set[tuple[str, int, int, str]] = set()

    for post in svar.get("data", []):
        kilde = str(post.get("sourceId", "")).split(":")[0]
        tid = _parse_tid(post.get("referenceTime"))
        if not kilde or tid is None:
            continue
        obs = sorted(post.get("observations") or [],
                     key=lambda o: (o.get("timeSeriesId") or 0))
        for o in obs:
            kort = KORTNAVN.get(o.get("elementId"))
            if kort is None:
                continue
            verdi = o.get("value")
            if verdi is None:
                continue
            nokkel = (kilde, tid.year, tid.month, kort)
            if nokkel in sett:
                continue
            sett.add(nokkel)
            rader.append((kilde, tid.year, tid.month, kort, float(verdi)))
    return rader


def hent_alle(auth, kilder: list[str], slutt_aar: int) -> pd.DataFrame:
    batcher = [kilder[i:i + BATCH] for i in range(0, len(kilder), BATCH)]
    alle: list[tuple] = []
    ferdig = 0
    t0 = time.time()

    def jobb(batch: list[str]) -> list[tuple]:
        with requests.Session() as s:
            return hent_batch(s, auth, batch, slutt_aar)

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {pool.submit(jobb, b): b for b in batcher}
        for fut in as_completed(futures):
            ferdig += 1
            try:
                alle.extend(fut.result())
            except Exception as exc:
                print(f"  ! batch feilet ({futures[fut][0]}...): {exc}")
            if ferdig % 10 == 0 or ferdig == len(batcher):
                print(f"  {ferdig}/{len(batcher)} batcher, {len(alle):,} obs, "
                      f"{time.time() - t0:.0f}s", flush=True)

    return pd.DataFrame(alle, columns=["stasjon", "aar", "maaned", "element", "verdi"])


# ===========================================================================
# 4. Regn ut normaler
# ===========================================================================

def regn_normaler(obs: pd.DataFrame) -> pd.DataFrame:
    """Månedsnormaler, regnet per måned slik WMO gjør det.

    Hver månedsnormal er snittet av den måneden over alle årene stasjonen har
    målt den, og krever minst ``MIN_AAR`` slike år. Alle 12 månedene må
    kvalifisere før stasjonen slipper gjennom - ellers kunne vi endt med en
    "årsnedbør" der mars manglet.

    Alternativet - å kreve at hvert *år* er komplett - gir ca. 9 % færre
    stasjoner uten at normalene blir målbart bedre: en stasjon som mistet
    november i 2004 har fortsatt en fullgod novembernormal fra de 30 andre
    årene.

    ``n_aar`` rapporteres som den svakeste måneden, så popupen ikke skryter
    på seg flere år enn den tynneste delen av normalen faktisk bygger på.
    """
    if obs.empty:
        return pd.DataFrame()

    per_maaned = (obs.groupby(["stasjon", "element", "maaned"])
                  .agg(verdi=("verdi", "mean"),
                       n_aar=("aar", "nunique"),
                       fra=("aar", "min"),
                       til=("aar", "max"))
                  .reset_index())

    kvalifiserer = per_maaned[per_maaned["n_aar"] >= MIN_AAR]
    fulle = (kvalifiserer.groupby(["stasjon", "element"])["maaned"].nunique()
             .rename("n_mnd").reset_index())
    fulle = fulle[fulle["n_mnd"] == 12][["stasjon", "element"]]
    if fulle.empty:
        return pd.DataFrame()

    normaler = kvalifiserer.merge(fulle, on=["stasjon", "element"])

    # Oppsummer perioden på stasjonsnivå, ikke per måned.
    oppsummert = (normaler.groupby(["stasjon", "element"])
                  .agg(n_aar_min=("n_aar", "min"),
                       fra_min=("fra", "min"),
                       til_maks=("til", "max"))
                  .reset_index())
    normaler = (normaler.drop(columns=["n_aar", "fra", "til"])
                .merge(oppsummert, on=["stasjon", "element"])
                .rename(columns={"n_aar_min": "n_aar", "fra_min": "fra", "til_maks": "til"}))
    normaler["verdi"] = normaler["verdi"].round(2)
    return normaler[["stasjon", "element", "maaned", "verdi", "n_aar", "fra", "til"]]


def aarsverdier(normaler: pd.DataFrame) -> pd.DataFrame:
    """Årsnormal: sum for nedbør og nedbørdager, maks for vindkast, ellers snitt.

    ``regn_normaler`` garanterer allerede at alle 12 månedene er til stede.
    """
    if normaler.empty:
        return pd.DataFrame()

    er_sum = normaler["element"].isin(SUMMERES)
    er_maks = normaler["element"].isin(MAKSIMERES)

    sum_del = normaler[er_sum].groupby(["stasjon", "element"])["verdi"].sum()
    maks_del = normaler[er_maks].groupby(["stasjon", "element"])["verdi"].max()
    snitt_del = (normaler[~er_sum & ~er_maks]
                 .groupby(["stasjon", "element"])["verdi"].mean())
    aar = pd.concat([sum_del, maks_del, snitt_del]).rename("verdi").reset_index()
    aar["maaned"] = 0  # 0 = årsverdi

    meta = normaler[["stasjon", "element", "n_aar", "fra", "til"]].drop_duplicates()
    aar = aar.merge(meta, on=["stasjon", "element"])
    aar["verdi"] = aar["verdi"].round(2)
    return aar


# ===========================================================================
# 5. Bymatching
# ===========================================================================

def _km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = p2 - p1
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


def koble_byer(stasjoner: dict[str, dict],
               aar_per_element: dict[str, dict[str, int]]) -> dict[str, dict[str, dict]]:
    """Finn målestasjonen som representerer hvert tettsted - per element.

    Matchingen må gjøres separat for hvert element. I Oslo er den nærmeste
    stasjonen til sentrum Tøyen, men Tøyen måler bare nedbør; velger vi den
    som «Oslos stasjon» forsvinner hovedstaden fra kartet i det du bytter til
    temperatur. Blindern skal representere Oslo for temperatur, Tøyen for
    nedbør.

    Rangeringen av kandidater er (lang nok serie, aktiv, beste poengsum):

    * Stasjoner med minst ``ANBEFALT_AAR`` år går foran alle kortere. Poenget
      med byfilteret er å sammenligne byer med hverandre, og en 30-årsnormal
      6 km fra sentrum sier mer om byens klima enn en 5-årsnormal 1 km unna.
    * Aktive går foran nedlagte innenfor samme nivå.
    * Deretter ``n_aar - KM_KOSTNAD * avstand``: hver kilometer fra sentrum
      koster tre år av serien. Ren avstandssortering ga feil svar to steder -
      Bergen fikk Florida UIB (1,0 km, 16 år) framfor Florida (1,1 km, 35 år)
      på hundre meters margin, og Oslo fikk Tøyen (1,3 km, 10 år) framfor
      Blindern (3,6 km, 35 år).

    Tildelingen er grådig i folketallsrekkefølge, og hver stasjon kan bare
    representere ett tettsted per element. Uten det ville Askøy «stjålet»
    Bergen - Florida fra Bergen, siden den også er Askøys nærmeste stasjon;
    nå får Bergen den, og Askøy sin nest nærmeste.

    Returnerer {stasjons-id: {element: {navn, rang, km, aar}}}.
    """
    ut: dict[str, dict[str, dict]] = defaultdict(dict)

    for element, aar_for in aar_per_element.items():
        brukt: set[str] = set()
        for by in TETTSTEDER:  # allerede sortert på folketall
            beste: Optional[tuple[int, int, float, float, str]] = None
            for sid, n_aar in aar_for.items():
                if sid in brukt:
                    continue
                st = stasjoner.get(sid)
                if not st:
                    continue
                d = _km(by["lat"], by["lon"], st["lat"], st["lon"])
                if d > BY_MAKS_KM:
                    continue
                kandidat = (
                    0 if n_aar >= ANBEFALT_AAR else 1,
                    0 if st.get("aktiv") else 1,
                    -(n_aar - KM_KOSTNAD * d),
                    d,
                    sid,
                )
                if beste is None or kandidat < beste:
                    beste = kandidat
            if beste is None:
                continue
            *_, avstand, sid = beste
            brukt.add(sid)
            ut[sid][element] = {
                "navn": by["navn"],
                "rang": by["rang"],
                "km": round(avstand, 1),
                "aar": aar_for[sid],
            }

    return dict(ut)


# ===========================================================================
# 6. Skriv ut
# ===========================================================================

def bygg_json(normaler: pd.DataFrame, stasjoner: dict[str, dict],
              byer: dict[str, dict[str, dict]], slutt_aar: int) -> dict:
    """Kompakt struktur for kartet.

    Månedsverdiene ligger som lister med 12 tall (jan..des) slik at JSON-en
    holder seg liten. null betyr at måneden mangler.

    Elementene leses ut av tabellen, ikke av ``KORTNAVN``. Ellers ville
    ``--fra-parquet`` tape vindkolonnene, siden vindelementene bare føres inn
    i ``KORTNAVN`` av ``velg_vindelementer`` under en full kjøring.
    """
    elementer = sorted(normaler["element"].unique())
    mnd_kart: dict[tuple[str, str], list[Optional[float]]] = {}
    meta_kart: dict[tuple[str, str], tuple[int, int, int]] = {}
    for (sid, el), del_df in normaler[normaler["maaned"] > 0].groupby(["stasjon", "element"]):
        serie: list[Optional[float]] = [None] * 12
        for maaned, verdi in zip(del_df["maaned"], del_df["verdi"]):
            serie[int(maaned) - 1] = float(verdi)
        mnd_kart[(sid, el)] = serie
        rad = del_df.iloc[0]
        meta_kart[(sid, el)] = (int(rad["n_aar"]), int(rad["fra"]), int(rad["til"]))

    aar_kart = {(r.stasjon, r.element): float(r.verdi)
                for r in normaler[normaler["maaned"] == 0].itertuples()}

    ut_stasjoner: list[dict] = []
    for sid in sorted({sid for sid, _ in mnd_kart}):
        st = stasjoner.get(sid)
        if not st:
            continue
        post: dict[str, Any] = {
            "id": sid,
            "navn": st["navn"],
            "fylke": st["fylke"],
            "kommune": st["kommune"],
            "lat": st["lat"],
            "lon": st["lon"],
            "moh": st["moh"],
            "aktiv": bool(st.get("aktiv")),
        }
        if st.get("til"):
            post["nedlagt"] = st["til"]
        for el in elementer:
            serie = mnd_kart.get((sid, el))
            if serie is None:
                continue
            post[el] = [None if v is None else round(v, 1) for v in serie]
            aarsverdi = aar_kart.get((sid, el))
            if aarsverdi is not None:
                post[f"{el}_aar"] = round(aarsverdi, 1)
            n_aar, fra, til = meta_kart[(sid, el)]
            post[f"{el}_n"] = n_aar
            post[f"{el}_periode"] = [fra, til]
        if sid in byer:
            post["by"] = byer[sid]
        ut_stasjoner.append(post)

    return {
        "oppdatert": date.today().isoformat(),
        "periode": [START_AAR, slutt_aar],
        "min_aar": MIN_AAR,
        "anbefalt_aar": ANBEFALT_AAR,
        "kilde": "MET Norway / Frost",
        "stasjoner": ut_stasjoner,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Bygg vaernormaler fra Frost")
    ap.add_argument("--slutt-aar", type=int, default=None,
                    help="Siste hele kalenderaar (default: i fjor)")
    ap.add_argument("--maks-stasjoner", type=int, default=None,
                    help="Begrens antall stasjoner (for testkjoering)")
    ap.add_argument("--fra-parquet", action="store_true",
                    help="Bygg bare JSON-en paa nytt fra eksisterende parquet")
    args = ap.parse_args()

    slutt_aar = args.slutt_aar or (date.today().year - 1)
    auth = _auth()

    if args.fra_parquet:
        # Nok til å endre kartformat, bymatching eller avrunding uten å laste
        # ned 850 000 observasjoner på nytt.
        if not PARQUET_UT.exists():
            print(f"Fant ikke {PARQUET_UT} - kjoer uten --fra-parquet foerst.")
            return 1
        full = pd.read_parquet(PARQUET_UT)
        with requests.Session() as s:
            stasjoner = hent_stasjoner(s, auth)
        print(f"Leste {len(full):,} rader fra {PARQUET_UT.name}")
        return _skriv_json(full, stasjoner, slutt_aar)

    print(f"Normalvindu {START_AAR}-{slutt_aar}, krever {MIN_AAR} aar per maaned.")
    with requests.Session() as s:
        print("Sjekker hvilke vindelementer Frost tilbyr...")
        velg_vindelementer(s, auth)

        print("Finner kandidatstasjoner...")
        kandidater = finn_kandidater(s, auth, slutt_aar)
        print(f"  -> {len(kandidater)} unike kandidater")

        print("Henter stasjonsmetadata...")
        stasjoner = hent_stasjoner(s, auth)
        print(f"  -> {len(stasjoner)} stasjoner i /sources")

    kilder = sorted(k for k in kandidater if k in stasjoner)
    if args.maks_stasjoner:
        kilder = kilder[:args.maks_stasjoner]
    print(f"  -> {len(kilder)} stasjoner med koordinater, henter observasjoner")

    obs = hent_alle(auth, kilder, slutt_aar)
    print(f"Hentet {len(obs):,} maanedsobservasjoner")
    if obs.empty:
        print("Ingen data - avbryter.")
        return 1

    print("Regner normaler...")
    normaler = regn_normaler(obs)
    if normaler.empty:
        print("Ingen stasjoner oppfylte kravet - avbryter.")
        return 1
    full = pd.concat([normaler, aarsverdier(normaler)], ignore_index=True)

    for kort in sorted(full["element"].unique()):
        n = full[(full["element"] == kort) & (full["maaned"] == 0)]["stasjon"].nunique()
        print(f"  {kort:12s} {n:5d} stasjoner med normal")

    PARQUET_UT.parent.mkdir(parents=True, exist_ok=True)
    full.to_parquet(PARQUET_UT, index=False)
    print(f"Skrev {PARQUET_UT.name} ({PARQUET_UT.stat().st_size / 1024:.0f} kB)")

    return _skriv_json(full, stasjoner, slutt_aar)


def _skriv_json(full: pd.DataFrame, stasjoner: dict[str, dict], slutt_aar: int) -> int:
    """Bymatching + skriv kartfila. Delt mellom full kjøring og --fra-parquet."""
    # Bymatchingen trenger serielengden per stasjon, ikke bare hvilke
    # stasjoner som har elementet.
    aar_per_element = {
        kort: (full[full["element"] == kort]
               .groupby("stasjon")["n_aar"].max().astype(int).to_dict())
        for kort in sorted(full["element"].unique())
    }
    byer = koble_byer(stasjoner, aar_per_element)
    dekket = {info["navn"] for per_el in byer.values() for info in per_el.values()}
    print(f"Koblet {len(dekket)} av {len(TETTSTEDER)} tettsteder "
          f"({len(byer)} stasjoner er bystasjon for minst ett element)")

    payload = bygg_json(full, stasjoner, byer, slutt_aar)
    JSON_UT.parent.mkdir(parents=True, exist_ok=True)
    JSON_UT.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
                       encoding="utf-8")
    print(f"Skrev {JSON_UT.name} ({JSON_UT.stat().st_size / 1024:.0f} kB, "
          f"{len(payload['stasjoner'])} stasjoner)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
