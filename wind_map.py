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
_STATIC_WIND_IDS: set[str] = {'SN76930', 'SN92350', 'SN75410', 'SN28380', 'SN10380', 'SN57770', 'SN51530', 'SN36560', 'SN16610', 'SN87110', 'SN88690', 'SN65310', 'SN33890', 'SN96310', 'SN59800', 'SN90800', 'SN43010', 'SN27450', 'SN59110', 'SN71550', 'SN4780', 'SN68860', 'SN58900', 'SN17000', 'SN87640', 'SN90490', 'SN93140', 'SN25830', 'SN24890', 'SN69100', 'SN42160', 'SN17150', 'SN69380', 'SN39100', 'SN20301', 'SN98550', 'SN3190', 'SN12680', 'SN27500', 'SN75550', 'SN23420', 'SN27470', 'SN99370', 'SN20926', 'SN82290', 'SN50500', 'SN76925', 'SN94500', 'SN20925', 'SN76933', 'SN48330', 'SN80610', 'SN76923', 'SN76929', 'SN71000', 'SN6020', 'SN50540', 'SN44560', 'SN9580', 'SN18700', 'SN32060', 'SN98400', 'SN34130', 'SN60990', 'SN90450', 'SN97350', 'SN76956', 'SN37230', 'SN52535', 'SN63420', 'SN76931', 'SN65940', 'SN77035', 'SN89350', 'SN26900', 'SN76920', 'SN8140', 'SN63705', 'SN62480', 'SN13420', 'SN85380', 'SN92750', 'SN40880', 'SN76928', 'SN35860', 'SN71850', 'SN95350', 'SN47300', 'SN39040', 'SN58070', 'SN36200', 'SN76926', 'SN77040', 'SN46510', 'SN76922', 'SN55290', 'SN18950', 'SN96400'}


_WIND_SOURCE_CACHE: tuple[float, set[str]] | None = None


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


def build_wind_map_html(*, mode: Mode = 'observed', period: Period = 'hour', metric: Metric = 'gust', date_str: Optional[str] = None,
                        forecast_hours: int = 24, region: Region = 'all',
                        bbox: Optional[str] = None, z: Optional[str] = None, clat: Optional[str] = None, clon: Optional[str] = None,
                        timeout: int = DEFAULT_TIMEOUT, show_heatmap: bool = True, top_n: int = 600, forecast_limit: int = 1500, observed_limit: int = 700) -> str:
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
    # Bruk statisk liste over kjente vindstasjoner — slipper Frost /sources-kall.
    filtered = stations[stations['baseId'].isin(_STATIC_WIND_IDS)].copy()
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
