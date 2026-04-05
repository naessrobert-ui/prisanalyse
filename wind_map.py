#!/usr/bin/env python3
from __future__ import annotations

import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date as _date
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator, Optional, Literal

import folium
import pandas as pd
import requests
from requests import RequestException
from dotenv import load_dotenv
from folium.plugins import HeatMap

from ver_station_db import load_station_db, stations_in_bbox_swne

load_dotenv(dotenv_path=Path(__file__).with_name('.env'))
load_dotenv()

FROST_BASE = 'https://frost.met.no'
DEFAULT_TIMEOUT = 20
YR_COMPACT = 'https://api.met.no/weatherapi/locationforecast/2.0/compact'
YR_COMPLETE = 'https://api.met.no/weatherapi/locationforecast/2.0/complete'
YR_UA = os.getenv('METNO_USER_AGENT', 'prisanalyse.no/1.0 kontakt@prisanalyse.no')

Mode = Literal['observed', 'forecast']
Period = Literal['hour', 'day', 'month']
Metric = Literal['avg', 'gust', 'peak']
Region = Literal['all', 'south', 'mid', 'north']


@dataclass(frozen=True)
class FrostAuth:
    client_id: str
    client_secret: str = ''


def _env_auth() -> FrostAuth:
    cid = os.getenv('FROST_CLIENT_ID')
    if not cid:
        raise RuntimeError('Sett FROST_CLIENT_ID i miljø/.env.')
    return FrostAuth(client_id=cid, client_secret=os.getenv('FROST_CLIENT_SECRET', ''))


def _chunked(xs: list[str], n: int) -> Iterator[list[str]]:
    for i in range(0, len(xs), n):
        yield xs[i:i + n]


def _frost_get_json(session: requests.Session, path: str, params: dict[str, Any], *, auth: FrostAuth, timeout: int) -> dict[str, Any]:
    url = f'{FROST_BASE}{path}'
    backoff = 1.0
    for _ in range(6):
        try:
            r = session.get(
                url,
                params=params,
                auth=(auth.client_id, auth.client_secret),
                headers={'Accept': 'application/json'},
                timeout=timeout,
            )
        except RequestException:
            time.sleep(backoff)
            backoff *= 2
            continue
        if r.status_code == 200:
            return r.json()
        if r.status_code in (404, 412):
            return r.json()
        if r.status_code == 429 or 500 <= r.status_code < 600:
            time.sleep(backoff)
            backoff *= 2
            continue
        raise RuntimeError(f'Frost-feil {r.status_code}: {r.text[:300]}')
    raise RuntimeError('Frost utilgjengelig etter retries.')


def _fetch_obs(session: requests.Session, *, auth: FrostAuth, source_ids: list[str], element: str, referencetime: str, timeout: int, batch_size: int = 80) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []

    def _fetch_batch(batch: list[str]) -> None:
        if not batch:
            return
        params = {
            'sources': ','.join(batch),
            'referencetime': referencetime,
            'elements': element,
            'limit': 1000,
        }
        offset = 0
        while True:
            if offset > 0:
                params['offset'] = offset
            elif 'offset' in params:
                del params['offset']
            try:
                page = _frost_get_json(session, '/observations/v0.jsonld', params, auth=auth, timeout=timeout)
            except RuntimeError as e:
                # Frost kan svare 400 på for lange/komplekse source-lister.
                # Del batchen og prøv igjen for å unngå 500 i appen.
                if 'Frost-feil 400' in str(e) and len(batch) > 1:
                    mid = len(batch) // 2
                    _fetch_batch(batch[:mid])
                    _fetch_batch(batch[mid:])
                    return
                # Ved andre feil: hopp over denne batchen i stedet for å krasje hele route.
                return
            if page.get('@type') == 'ErrorResponse':
                break
            data = page.get('data', []) or []
            for item in data:
                sid = str(item.get('sourceId') or '').split(':')[0]
                item_rt = item.get('referenceTime')
                for obs in item.get('observations', []):
                    rows.append({
                        'sourceId': sid,
                        'referenceTime': obs.get('referenceTime') or item_rt,
                        'value': obs.get('value'),
                    })
            per_page = int(page.get('itemsPerPage') or 0)
            total = int(page.get('totalItemCount') or 0)
            if not data or per_page <= 0:
                break
            offset += per_page
            if total and offset >= total:
                break

    for batch in _chunked(source_ids, batch_size):
        _fetch_batch(batch)
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df['referenceTime'] = pd.to_datetime(df['referenceTime'], errors='coerce', utc=True)
    df['value'] = pd.to_numeric(df['value'], errors='coerce')
    return df.dropna(subset=['referenceTime', 'value'])


def _aggregate_obs(df: pd.DataFrame, *, metric: Metric) -> pd.DataFrame:
    if df.empty:
        return df
    if metric == 'gust':
        out = df.groupby('sourceId', as_index=False).agg(value=('value', 'max'), points=('value', 'size'))
    else:
        out = df.groupby('sourceId', as_index=False).agg(value=('value', 'mean'), points=('value', 'size'))
    return out


_FORECAST_CACHE: dict[tuple[float, float], tuple[float, dict[str, float]]] = {}
_FORECAST_CACHE_COMPLETE: dict[tuple[float, float], tuple[float, dict[str, float]]] = {}

# Statisk liste over norske vindstasjoner med sanntidsdata.
# Oppdater ved å kjøre update_wind_stations.py (sjelden nødvendig).
_STATIC_WIND_IDS: set[str] = {
    "SN10055", "SN10070", "SN10380", "SN10800", "SN11000", "SN11125", "SN1120", "SN1135",
    "SN11463", "SN11500", "SN120", "SN12180", "SN12280", "SN12320", "SN12550", "SN12580",
    "SN12590", "SN12666", "SN12680", "SN12960", "SN13030", "SN13110", "SN13150", "SN13160",
    "SN13390", "SN13420", "SN13655", "SN13760", "SN1380", "SN1400", "SN15050", "SN15262",
    "SN15270", "SN15890", "SN15905", "SN15951", "SN1600", "SN16040", "SN16271", "SN16400",
    "SN16560", "SN16610", "SN16611", "SN16620", "SN16845", "SN17000", "SN17050", "SN17090",
    "SN17150", "SN17280", "SN17380", "SN17400", "SN17650", "SN17850", "SN17853", "SN17875",
    "SN17895", "SN180", "SN18210", "SN18240", "SN18265", "SN18690", "SN18700", "SN18950",
    "SN19430", "SN19459", "SN1960", "SN19655", "SN19663", "SN19665", "SN19710", "SN1980",
    "SN19830", "SN19923", "SN19940", "SN20280", "SN20301", "SN20540", "SN20925", "SN20926",
    "SN20929", "SN20930", "SN20931", "SN20932", "SN20949", "SN20950", "SN20951", "SN20952",
    "SN20953", "SN20954", "SN20958", "SN20959", "SN20960", "SN20961", "SN20962", "SN210",
    "SN21680", "SN21700", "SN22890", "SN22990", "SN23300", "SN23410", "SN23420", "SN23500",
    "SN23550", "SN23680", "SN24230", "SN24350", "SN24670", "SN24710", "SN24820", "SN24890",
    "SN24960", "SN25110", "SN25112", "SN25115", "SN25140", "SN25165", "SN25540", "SN25580",
    "SN2560", "SN25630", "SN25830", "SN26450", "SN2650", "SN26650", "SN26820", "SN26900",
    "SN26950", "SN26960", "SN26970", "SN26990", "SN27010", "SN27055", "SN27075", "SN27120",
    "SN27130", "SN27161", "SN27285", "SN27315", "SN27320", "SN27420", "SN27450", "SN27470",
    "SN27480", "SN27500", "SN27730", "SN27780", "SN27860", "SN28380", "SN28750", "SN28922",
    "SN29400", "SN2970", "SN29700", "SN29720", "SN29870", "SN29885", "SN29890", "SN29950",
    "SN3003", "SN30235", "SN30242", "SN30244", "SN30246", "SN30255", "SN30280", "SN30305",
    "SN30330", "SN30650", "SN30860", "SN31400", "SN31560", "SN31620", "SN31680", "SN31700",
    "SN31853", "SN3190", "SN31970", "SN32060", "SN32220", "SN32240", "SN32290", "SN32490",
    "SN32540", "SN32890", "SN3290", "SN33300", "SN33430", "SN33555", "SN3370", "SN33890",
    "SN33950", "SN33990", "SN34095", "SN34115", "SN34130", "SN3480", "SN34810", "SN35110",
    "SN35210", "SN35860", "SN36200", "SN36330", "SN36560", "SN37230", "SN3730", "SN3731",
    "SN37580", "SN37600", "SN38140", "SN38150", "SN38250", "SN38350", "SN38730", "SN38735",
    "SN39040", "SN39100", "SN39450", "SN39750", "SN3990", "SN39970", "SN40005", "SN40250",
    "SN40510", "SN40880", "SN40905", "SN41175", "SN41770", "SN41810", "SN41825", "SN4200",
    "SN42160", "SN4260", "SN42810", "SN42930", "SN42940", "SN43010", "SN43030", "SN43350",
    "SN43490", "SN43530", "SN4385", "SN43895", "SN44080", "SN44300", "SN44485", "SN44560",
    "SN44563", "SN4460", "SN44605", "SN44610", "SN44640", "SN44710", "SN44780", "SN44970",
    "SN44985", "SN45350", "SN45460", "SN45530", "SN45770", "SN45870", "SN4590", "SN46220",
    "SN46425", "SN46430", "SN46432", "SN46510", "SN46610", "SN46800", "SN47060", "SN47220",
    "SN47225", "SN47260", "SN47272", "SN47275", "SN47300", "SN47350", "SN47480", "SN47498",
    "SN4780", "SN47890", "SN47910", "SN48069", "SN48070", "SN48120", "SN48170", "SN48330",
    "SN490381001", "SN490381002", "SN490381015", "SN490381016", "SN490381022", "SN490381023", "SN490381079", "SN490381080",
    "SN490387066", "SN490387067", "SN490387090", "SN490387091", "SN490391001", "SN490391002", "SN490391007", "SN490391008",
    "SN490391020", "SN490391021", "SN490391032", "SN49085", "SN49087", "SN4920", "SN49240", "SN49490",
    "SN4950", "SN496228003", "SN496228005", "SN496228008", "SN496228013", "SN496228016", "SN496228018", "SN496228022",
    "SN49720", "SN497459020", "SN497459330", "SN49800", "SN49855", "SN49860", "SN49865", "SN49910",
    "SN499999010", "SN50070", "SN50231", "SN50310", "SN50311", "SN50395", "SN50500", "SN50508",
    "SN50517", "SN50526", "SN50540", "SN50570", "SN50815", "SN50850", "SN50865", "SN51010",
    "SN51440", "SN51470", "SN51530", "SN51610", "SN51800", "SN51980", "SN51991", "SN52452",
    "SN52458", "SN52505", "SN52535", "SN52540", "SN52555", "SN52565", "SN52570", "SN52860",
    "SN53010", "SN53101", "SN53280", "SN5330", "SN53480", "SN53530", "SN53680", "SN53990",
    "SN54180", "SN54590", "SN54645", "SN54710", "SN54731", "SN54815", "SN5495", "SN55000",
    "SN55260", "SN55290", "SN55425", "SN55430", "SN55700", "SN55720", "SN55735", "SN55770",
    "SN55820", "SN5590", "SN55928", "SN55970", "SN56420", "SN56490", "SN5660", "SN57000",
    "SN57050", "SN57340", "SN57380", "SN57710", "SN57770", "SN57789", "SN57790", "SN57830",
    "SN58030", "SN58070", "SN58100", "SN58165", "SN58350", "SN58533", "SN58703", "SN58705",
    "SN58810", "SN58860", "SN58900", "SN59010", "SN59110", "SN59210", "SN59220", "SN59530",
    "SN59586", "SN59680", "SN59695", "SN59800", "SN59815", "SN59820", "SN59825", "SN59830",
    "SN59850", "SN60190", "SN6020", "SN60225", "SN60240", "SN60242", "SN60247", "SN60310",
    "SN60500", "SN60650", "SN60670", "SN60810", "SN60835", "SN60860", "SN60875", "SN60880",
    "SN60947", "SN60980", "SN60990", "SN61010", "SN61025", "SN61060", "SN61065", "SN61070",
    "SN61190", "SN61310", "SN61408", "SN61410", "SN61420", "SN61580", "SN6160", "SN61630",
    "SN61790", "SN61810", "SN61880", "SN62200", "SN62230", "SN62260", "SN62270", "SN62295",
    "SN62450", "SN62480", "SN62860", "SN62950", "SN62980", "SN63000", "SN63420", "SN63595",
    "SN63630", "SN63705", "SN63725", "SN63820", "SN63880", "SN63940", "SN64242", "SN64330",
    "SN64510", "SN64590", "SN64760", "SN64870", "SN64970", "SN65060", "SN65160", "SN65275",
    "SN65310", "SN65320", "SN65330", "SN65370", "SN65451", "SN65460", "SN65470", "SN65530",
    "SN65680", "SN65850", "SN65920", "SN65930", "SN65940", "SN66005", "SN66045", "SN66120",
    "SN66150", "SN66175", "SN66530", "SN66630", "SN66740", "SN66810", "SN66900", "SN670",
    "SN6700", "SN67005", "SN67140", "SN67153", "SN67175", "SN67210", "SN67270", "SN67280",
    "SN67340", "SN67560", "SN67650", "SN67890", "SN68090", "SN68175", "SN68238", "SN68290",
    "SN6840", "SN68860", "SN69035", "SN69090", "SN69100", "SN69150", "SN69190", "SN69280",
    "SN69380", "SN69520", "SN69640", "SN69655", "SN69661", "SN69730", "SN700", "SN70390",
    "SN70680", "SN70850", "SN70960", "SN70980", "SN71000", "SN71230", "SN71290", "SN71320",
    "SN71385", "SN71550", "SN71780", "SN71805", "SN71850", "SN71880", "SN71990", "SN72200",
    "SN725", "SN72580", "SN72710", "SN73210", "SN73466", "SN73550", "SN73630", "SN7420",
    "SN74350", "SN74670", "SN75140", "SN75190", "SN75220", "SN75410", "SN75550", "SN75650",
    "SN76240", "SN76245", "SN76330", "SN76450", "SN76460", "SN76530", "SN76750", "SN76909",
    "SN76914", "SN76920", "SN76922", "SN76923", "SN76925", "SN76926", "SN76927", "SN76928",
    "SN76929", "SN76930", "SN76931", "SN76933", "SN76934", "SN76935", "SN76936", "SN76937",
    "SN76938", "SN76939", "SN76946", "SN76950", "SN76954", "SN76956", "SN76957", "SN76958",
    "SN76959", "SN76960", "SN76961", "SN76962", "SN76963", "SN76964", "SN76965", "SN76966",
    "SN76967", "SN76968", "SN76969", "SN76999", "SN77000", "SN77001", "SN77005", "SN77006",
    "SN77029", "SN77032", "SN77035", "SN77037", "SN77040", "SN77041", "SN77043", "SN77046",
    "SN77048", "SN77049", "SN77051", "SN77055", "SN77150", "SN77230", "SN77280", "SN77285",
    "SN77287", "SN77295", "SN77350", "SN77390", "SN77403", "SN77405", "SN77425", "SN77490",
    "SN78050", "SN78170", "SN78360", "SN78800", "SN78910", "SN79215", "SN79220", "SN79253",
    "SN7940", "SN7950", "SN79600", "SN79610", "SN79700", "SN79764", "SN79791", "SN80050",
    "SN80102", "SN80320", "SN80610", "SN80700", "SN80705", "SN80707", "SN80730", "SN80740",
    "SN80870", "SN80880", "SN81310", "SN81315", "SN81340", "SN8140", "SN81420", "SN81650",
    "SN81760", "SN81775", "SN81780", "SN81790", "SN82000", "SN82020", "SN82110", "SN82130",
    "SN82210", "SN82220", "SN82290", "SN82410", "SN82720", "SN82970", "SN83280", "SN83600",
    "SN83710", "SN83960", "SN83970", "SN84210", "SN84380", "SN84500", "SN84630", "SN84701",
    "SN84770", "SN84880", "SN84900", "SN84905", "SN84910", "SN84925", "SN84970", "SN85040",
    "SN85380", "SN85448", "SN85450", "SN85480", "SN85545", "SN85560", "SN85640", "SN85645",
    "SN85840", "SN85890", "SN86290", "SN86310", "SN86520", "SN86598", "SN86600", "SN86740",
    "SN86810", "SN86921", "SN87000", "SN87110", "SN87117", "SN87120", "SN87315", "SN87640",
    "SN87660", "SN87740", "SN87763", "SN87772", "SN87795", "SN87950", "SN87970", "SN88010",
    "SN88023", "SN88200", "SN8840", "SN88430", "SN88450", "SN88628", "SN88650", "SN88690",
    "SN8880", "SN89010", "SN89213", "SN89233", "SN89350", "SN89360", "SN89940", "SN89985",
    "SN90110", "SN90250", "SN90270", "SN90285", "SN90295", "SN90305", "SN90360", "SN90400",
    "SN90450", "SN90480", "SN90490", "SN90491", "SN90620", "SN90630", "SN90635", "SN90680",
    "SN90720", "SN90730", "SN90735", "SN90760", "SN90770", "SN90800", "SN91010", "SN91120",
    "SN91220", "SN91251", "SN91380", "SN91420", "SN91430", "SN91460", "SN91480", "SN91490",
    "SN91502", "SN91508", "SN91515", "SN91530", "SN91551", "SN91590", "SN91670", "SN91675",
    "SN91680", "SN91685", "SN91690", "SN91695", "SN91715", "SN91724", "SN91728", "SN91729",
    "SN91740", "SN92350", "SN9250", "SN92750", "SN92760", "SN92765", "SN92840", "SN92845",
    "SN92855", "SN92860", "SN92875", "SN92895", "SN93000", "SN9303", "SN93070", "SN9310",
    "SN93140", "SN93170", "SN93200", "SN93301", "SN93611", "SN93700", "SN9380", "SN93900",
    "SN9400", "SN94020", "SN94090", "SN94185", "SN94190", "SN94195", "SN94198", "SN94220",
    "SN94230", "SN94232", "SN94235", "SN94238", "SN94242", "SN94280", "SN94430", "SN94455",
    "SN94500", "SN94620", "SN94630", "SN94635", "SN94640", "SN94645", "SN94680", "SN94720",
    "SN94805", "SN94810", "SN95350", "SN9580", "SN96220", "SN96250", "SN96310", "SN96400",
    "SN96850", "SN96890", "SN97020", "SN97120", "SN97251", "SN97350", "SN97470", "SN97710",
    "SN98090", "SN98265", "SN98360", "SN98400", "SN98550", "SN98580", "SN98595", "SN98630",
    "SN98640", "SN98645", "SN98790", "SN98974", "SN98976", "SN98978", "SN99090", "SN99310",
    "SN99370", "SN99420", "SN99460", "SN99600", "SN99670", "SN99680",
}


_WIND_SOURCE_CACHE: tuple[float, set[str]] | None = None
_LEARNED_WIND_IDS: set[str] = set()  # Stasjoner som har gitt data — vokser over tid


def _fetch_wind_source_ids(session: requests.Session, *, auth: FrostAuth, timeout: int = DEFAULT_TIMEOUT) -> set[str]:
    """
    Hent alle norske kilder som har vind-elementer tilgjengelig nå.
    Caches i 6 timer for å unngå tunge kall på hver sidevisning.
    """
    global _WIND_SOURCE_CACHE
    now_ts = time.time()
    if _WIND_SOURCE_CACHE and _WIND_SOURCE_CACHE[0] > now_ts:
        return _WIND_SOURCE_CACHE[1]

    params = {
        'country': 'NO',
        'validtime': 'now',
        'elements': 'wind_speed,wind_speed_of_gust',
    }
    data = _frost_get_json(session, '/sources/v0.jsonld', params, auth=auth, timeout=timeout)
    ids: set[str] = set()
    for item in data.get('data', []) or []:
        sid = str(item.get('id') or '').strip()
        if not sid:
            continue
        ids.add(sid.split(':')[0])
    _WIND_SOURCE_CACHE = (now_ts + 21600, ids)
    return ids


def _forecast_station(*, lat: float, lon: float, forecast_hours: int = 24, timeout: int = 12, need_gust: bool = False) -> Optional[dict[str, float]]:
    """Henter vind fra Yr. Bruker complete-API for å få gustdata i ett kall."""
    key = (round(lat, 3), round(lon, 3))
    now_ts = time.time()
    cached = _FORECAST_CACHE.get(key)
    if cached and cached[0] > now_ts:
        return cached[1]

    # Alltid bruk complete for å få alle tre metrics i ett kall
    r = requests.get(
        YR_COMPLETE,
        params={'lat': f'{lat:.4f}', 'lon': f'{lon:.4f}'},
        headers={'User-Agent': YR_UA, 'Accept': 'application/json'},
        timeout=timeout,
    )
    if r.status_code != 200:
        return None
    data = r.json()
    ts = (((data.get('properties') or {}).get('timeseries')) or [])
    if not ts:
        return None

    now = datetime.now(timezone.utc)
    horizon = now + timedelta(hours=max(6, min(int(forecast_hours or 24), 168)))
    winds: list[float] = []
    gusts: list[float] = []
    temps: list[float] = []
    precips: list[float] = []
    dirs: list[float] = []
    now_details: dict = {}  # instant data for the first timestep
    for it in ts:
        t = pd.to_datetime(it.get('time'), utc=True, errors='coerce')
        if pd.isna(t):
            continue
        t_py = t.to_pydatetime()
        if t_py < now or t_py > horizon:
            continue
        details = (((it.get('data') or {}).get('instant') or {}).get('details') or {})
        # Capture first timestep as "now"
        if not now_details:
            now_details = details
        ws = details.get('wind_speed')
        wg = details.get('wind_speed_of_gust')
        temp = details.get('air_temperature')
        wd = details.get('wind_from_direction')
        # Precipitation from next_1_hours
        n1 = ((it.get('data') or {}).get('next_1_hours') or {})
        precip = (n1.get('details') or {}).get('precipitation_amount')
        if ws is not None:
            winds.append(float(ws))
        if wg is not None:
            gusts.append(float(wg))
        if temp is not None:
            temps.append(float(temp))
        if wd is not None:
            dirs.append(float(wd))
        if precip is not None:
            precips.append(float(precip))
    if not winds and not gusts:
        return None

    def _wind_dir_txt(deg: float) -> str:
        dirs_txt = ['N','NØ','Ø','SØ','S','SV','V','NV']
        return dirs_txt[int((deg + 22.5) / 45) % 8]

    payload = {
        'avg': round(sum(winds) / max(len(winds), 1), 1) if winds else 0.0,
        'gust': round(max(gusts), 1) if gusts else None,
        'peak': round(max(winds), 1) if winds else 0.0,
        'now_wind': round(now_details.get('wind_speed', 0) or 0, 1),
        'now_gust': round(now_details.get('wind_speed_of_gust', 0) or 0, 1),
        'now_temp': round(now_details.get('air_temperature', 0) or 0, 1) if now_details.get('air_temperature') is not None else None,
        'now_dir': _wind_dir_txt(now_details['wind_from_direction']) if now_details.get('wind_from_direction') is not None else '',
        'temp_avg': round(sum(temps) / max(len(temps), 1), 1) if temps else None,
        'temp_min': round(min(temps), 1) if temps else None,
        'temp_max': round(max(temps), 1) if temps else None,
        'precip_sum': round(sum(precips), 1) if precips else 0.0,
    }
    _FORECAST_CACHE[key] = (now_ts + 1800, payload)
    return payload


def _fmt_label(mode: Mode, period: Period, metric: Metric, selected: _date, forecast_hours: int) -> str:
    if mode == 'forecast':
        if metric == 'gust':
            return f'Forventet maks vindkast neste {forecast_hours} timer (Yr)'
        if metric == 'peak':
            return f'Høyeste timesverdi neste {forecast_hours} timer (Yr)'
        return f'Gjennomsnittsvind neste {forecast_hours} timer (Yr)'
    mtxt = 'vindkast (m/s)' if metric == 'gust' else 'gjennomsnittsvind (m/s)'
    if period == 'hour':
        return f'Siste 24 timer – {mtxt}'
    if period == 'day':
        return f'Døgn {selected.isoformat()} – {mtxt}'
    return f'Måned {selected.strftime("%Y-%m")} – {mtxt}'


def warmup_wind_stations(timeout: int = 30) -> None:
    """Kjøres i bakgrunnen ved oppstart — prøver alle stasjoner og lærer hvilke som gir data."""
    import threading
    def _run():
        try:
            auth = _env_auth()
            session = requests.Session()
            # Hent alle Frost-vindstasjoner
            r = session.get(
                f'{FROST_BASE}/sources/v0.jsonld',
                params={'country': 'NO', 'elements': 'wind_speed,wind_speed_of_gust'},
                auth=auth, timeout=timeout,
            )
            if r.status_code != 200:
                return
            all_ids = list({str(s.get('id','')).split(':')[0]
                           for s in r.json().get('data', []) or []
                           if str(s.get('id','')).startswith('SN')})

            # Hent observasjoner i batches for å se hvem som svarer
            now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
            start = now - timedelta(hours=2)
            rt = f"{start.strftime('%Y-%m-%dT%H:00:00Z')}/{now.strftime('%Y-%m-%dT%H:00:00Z')}"
            batch_size = 50
            found: set[str] = set()
            for i in range(0, len(all_ids), batch_size):
                batch = all_ids[i:i+batch_size]
                try:
                    resp = session.get(
                        f'{FROST_BASE}/observations/v0.jsonld',
                        params={'sources': ','.join(batch), 'referencetime': rt,
                                'elements': 'wind_speed', 'limit': 1000},
                        auth=auth, timeout=20,
                    )
                    if resp.status_code == 200:
                        for item in resp.json().get('data', []) or []:
                            sid = str(item.get('sourceId','')).split(':')[0]
                            if sid:
                                found.add(sid)
                except Exception:
                    pass
            if len(found) >= 10:
                _LEARNED_WIND_IDS.update(found)
                print(f'[VIND] Warmup ferdig: {len(_LEARNED_WIND_IDS)} aktive vindstasjoner funnet', flush=True)
        except Exception as e:
            print(f'[VIND] Warmup feilet: {e}', flush=True)

    threading.Thread(target=_run, daemon=True).start()


def build_wind_map_html(*, mode: Mode = 'observed', period: Period = 'hour', metric: Metric = 'gust', date_str: Optional[str] = None,
                        forecast_hours: int = 24, region: Region = 'all',
                        bbox: Optional[str] = None, z: Optional[str] = None, clat: Optional[str] = None, clon: Optional[str] = None,
                        timeout: int = DEFAULT_TIMEOUT, show_heatmap: bool = True, top_n: int = 600, forecast_limit: int = 1500, observed_limit: int = 1000) -> str:
    selected = _date.today()
    if date_str:
        try:
            selected = _date.fromisoformat(date_str)
        except Exception:
            pass

    if mode not in {'observed', 'forecast'}:
        mode = 'observed'
    if period not in {'hour', 'day', 'month'}:
        period = 'hour'
    if metric not in {'avg', 'gust'}:
        metric = 'avg'

    region_defaults: dict[str, tuple[float, float, float, float]] = {
        'south': (57.0, 4.0, 62.5, 12.5),
        'mid': (62.0, 4.0, 66.7, 16.5),
        'north': (66.3, 10.0, 71.5, 31.5),
        'all': (57.0, 4.0, 71.5, 31.5),
    }
    s, w, n, e = region_defaults.get(region, region_defaults['all'])
    if bbox:
        try:
            s, w, n, e = [float(x) for x in bbox.split(',')]
        except Exception:
            pass

    stations = stations_in_bbox_swne((s, w, n, e))
    if stations.empty:
        stations = load_station_db()
    stations = stations.dropna(subset=['baseId', 'lat', 'lon']).copy()
    stations['baseId'] = stations['baseId'].astype(str)
    stations = stations[stations['baseId'].str.match(r"^SN\d+$", na=False)].copy()

    merged = pd.DataFrame()
    wind_station_count_note = ''

    auth = _env_auth()
    session = requests.Session()
    # Bruk lært cache hvis vi har data, ellers start med statisk liste.
    # Cachen vokser automatisk når nye stasjoner gir data.
    active_ids = _STATIC_WIND_IDS | _LEARNED_WIND_IDS  # Alltid union – ikke erstatt
    filtered = stations[stations['baseId'].isin(active_ids)].copy()
    if len(filtered) >= 10:
        stations = filtered

    if mode == 'observed':
        if len(stations) > observed_limit:
            stations = stations.head(observed_limit).copy()
        if period == 'hour':
            now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
            start_dt = now - timedelta(hours=24)
            end_dt = now
        elif period == 'day':
            start_dt = datetime(selected.year, selected.month, selected.day, tzinfo=timezone.utc)
            end_dt = start_dt + timedelta(days=1)
        else:
            start_dt = datetime(selected.year, selected.month, 1, tzinfo=timezone.utc)
            if selected.month == 12:
                end_dt = datetime(selected.year + 1, 1, 1, tzinfo=timezone.utc)
            else:
                end_dt = datetime(selected.year, selected.month + 1, 1, tzinfo=timezone.utc)

        referencetime = f"{start_dt.strftime('%Y-%m-%dT%H:%M:%SZ')}/{end_dt.strftime('%Y-%m-%dT%H:%M:%SZ')}"
        element = 'max(wind_speed_of_gust PT1H)' if metric == 'gust' else 'wind_speed'
        try:
            obs = _fetch_obs(
                session,
                auth=auth,
                source_ids=stations['baseId'].tolist(),
                element=element,
                referencetime=referencetime,
                timeout=timeout,
            )
            agg = _aggregate_obs(obs, metric=metric)
            if not agg.empty:
                merged = stations.merge(agg, left_on='baseId', right_on='sourceId', how='inner')
                # Lær hvilke stasjoner som ga data
                _LEARNED_WIND_IDS.update(merged['baseId'].tolist())
        except Exception:
            merged = pd.DataFrame()
    else:
        stations = stations.head(max(1, int(forecast_limit))).copy()
        rows: list[dict[str, Any]] = []
        max_workers = 24
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = {
                ex.submit(_forecast_station, lat=float(st['lat']), lon=float(st['lon']), forecast_hours=forecast_hours): st
                for _, st in stations.iterrows()
            }
            for fut in as_completed(futures):
                st = futures[fut]
                try:
                    fc = fut.result()
                except Exception:
                    fc = None
                if not fc:
                    continue
                val = fc.get(metric)
                if val is None:
                    continue
                rows.append({'baseId': st['baseId'], 'value': val, 'points': 1})
                _LEARNED_WIND_IDS.add(st['baseId'])
        if rows:
            fdf = pd.DataFrame(rows)
            merged = stations.merge(fdf, on='baseId', how='inner')
        wind_station_count_note = f'Viser {len(merged)} av {len(stations)} vindstasjoner i utsnittet.'

    if merged.empty:
        m = folium.Map(location=[64.5, 14.5], zoom_start=4, tiles='CartoDB positron')
        folium.Marker([64.5, 14.5], tooltip='Ingen vinddata tilgjengelig for valgt filter.').add_to(m)
        return m.get_root().render()

    merged = merged.sort_values('value', ascending=False).reset_index(drop=True)
    top = merged.head(max(1, int(top_n))).copy()

    center_lat = float(clat) if clat else float(top['lat'].mean())
    center_lon = float(clon) if clon else float(top['lon'].mean())
    zoom_start = int(z) if (z and str(z).isdigit()) else 5

    m = folium.Map(location=[center_lat, center_lon], zoom_start=zoom_start, tiles='CartoDB dark_matter')

    if show_heatmap:
        HeatMap(data=[[r.lat, r.lon, float(r.value)] for r in top.itertuples()], radius=17, blur=16, min_opacity=0.35).add_to(m)

    vmax = float(top['value'].max())
    for r in top.itertuples():
        val = float(r.value)
        ratio = 0 if vmax <= 0 else min(1.0, val / vmax)
        color = '#22c55e' if ratio < 0.33 else ('#f59e0b' if ratio < 0.66 else '#ef4444')
        name = getattr(r, 'name', r.baseId) or r.baseId
        # Popup med iframe til detaljert værsiden
        popup_url = (
            f"/ver/vind-popup?id={r.baseId}&mode={mode}"
            f"&lat={float(r.lat):.4f}&lon={float(r.lon):.4f}"
            f"&name={name}&hours={forecast_hours}"
        )
        popup_html = (
            f'<iframe src="{popup_url}" width="340" height="420" '
            f'style="border:none;border-radius:8px;" loading="lazy"></iframe>'
        )
        folium.CircleMarker(
            location=[float(r.lat), float(r.lon)],
            radius=4 + 8 * ratio,
            color=color,
            fill=True,
            fill_opacity=0.9,
            tooltip=f"{name}: {val:.1f} m/s",
            popup=folium.Popup(popup_html, max_width=360),
        ).add_to(m)

    import json
    label = _fmt_label(mode, period, metric, selected, forecast_hours)
    st_list = [
        {'name': getattr(r, 'name', r.baseId) or r.baseId, 'lat': float(r.lat), 'lon': float(r.lon), 'value': float(r.value)}
        for r in top.itertuples()
    ]
    st_json = json.dumps(st_list)
    fc_note = (f"<div style='margin-top:6px;color:#94a3b8;font-size:11px;'>{wind_station_count_note}</div>" if mode == 'forecast' else '')
    info = f"""
    <div id="vp" style="position:fixed;left:14px;bottom:14px;z-index:9999;background:rgba(15,23,42,.92);color:#e2e8f0;padding:12px 14px;border-radius:12px;border:1px solid #334155;max-width:260px;font:13px/1.35 system-ui;">
      <div style="font-weight:800;margin-bottom:3px;">💨 Vindkart Norge</div>
      <div style="color:#93c5fd;margin-bottom:3px;font-size:12px;">{label}</div>
      <div id="va" style="color:#64748b;font-size:11px;margin-bottom:4px;"></div>
      <ol id="vl" style="margin:0;padding-left:18px;max-height:200px;overflow-y:auto;scrollbar-width:thin;font-size:12px;"></ol>
      {fc_note}
    </div>
    <script>
    (function(){{
      var ST={st_json};
      function upd(){{
        var m=window._lm;if(!m)return;
        var b=m.getBounds();
        var v=ST.filter(function(s){{
          return s.lat>=b.getSouth()&&s.lat<=b.getNorth()&&s.lon>=b.getWest()&&s.lon<=b.getEast();
        }});
        v.sort(function(a,b){{return b.value-a.value;}});
        document.getElementById('vl').innerHTML=v.slice(0,20).map(function(s,i){{
          return '<li>'+s.name+': <b>'+s.value.toFixed(1)+'</b> m/s</li>';
        }}).join('');
        document.getElementById('va').textContent=v.length+' stasjon'+(v.length!==1?'er':'')+' i visningsområdet';
      }}
      function init(){{
        var ks=Object.keys(window);
        for(var i=0;i<ks.length;i++){{
          var v=window[ks[i]];
          if(v&&v._leaflet_id&&v.getBounds){{window._lm=v;v.on('moveend zoomend',upd);upd();return;}}
        }}
        setTimeout(init,200);
      }}
      setTimeout(init,500);
    }})();
    </script>
    """
    m.get_root().html.add_child(folium.Element(info))
    return m.get_root().render()
