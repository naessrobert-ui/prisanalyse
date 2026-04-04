#!/usr/bin/env python3
from __future__ import annotations

import os
import time
from dataclasses import dataclass
from datetime import date as _date
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator, Optional, Literal

import folium
import pandas as pd
import requests
from dotenv import load_dotenv
from folium.plugins import HeatMap

from ver_station_db import load_station_db, stations_in_bbox_swne

load_dotenv(dotenv_path=Path(__file__).with_name('.env'))
load_dotenv()

FROST_BASE = 'https://frost.met.no'
DEFAULT_TIMEOUT = 20
YR_COMPACT = 'https://api.met.no/weatherapi/locationforecast/2.0/compact'
YR_UA = os.getenv('METNO_USER_AGENT', 'prisanalyse.no/1.0 kontakt@prisanalyse.no')

Mode = Literal['observed', 'forecast']
Period = Literal['hour', 'day', 'month']
Metric = Literal['avg', 'gust']


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
        r = session.get(url, params=params, auth=(auth.client_id, auth.client_secret), headers={'Accept': 'application/json'}, timeout=timeout)
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
    for batch in _chunked(source_ids, batch_size):
        params = {
            'sources': ','.join(batch),
            'referencetime': referencetime,
            'elements': element,
            'timeoffsets': 'default',
            'levels': 'default',
            'limit': 1000,
            'qualities': '0,1,2,3,4',
        }
        page = _frost_get_json(session, '/observations/v0.jsonld', params, auth=auth, timeout=timeout)
        if page.get('@type') == 'ErrorResponse':
            continue
        for item in page.get('data', []):
            sid = str(item.get('sourceId') or '').split(':')[0]
            item_rt = item.get('referenceTime')
            for obs in item.get('observations', []):
                rows.append({
                    'sourceId': sid,
                    'referenceTime': obs.get('referenceTime') or item_rt,
                    'value': obs.get('value'),
                })
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


def _forecast_station(session: requests.Session, *, lat: float, lon: float, timeout: int = 12) -> Optional[dict[str, float]]:
    key = (round(lat, 3), round(lon, 3))
    now_ts = time.time()
    cached = _FORECAST_CACHE.get(key)
    if cached and cached[0] > now_ts:
        return cached[1]

    r = session.get(
        YR_COMPACT,
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
    horizon = now + timedelta(hours=24)
    winds: list[float] = []
    gusts: list[float] = []
    for it in ts:
        t = pd.to_datetime(it.get('time'), utc=True, errors='coerce')
        if pd.isna(t):
            continue
        t_py = t.to_pydatetime()
        if t_py < now or t_py > horizon:
            continue
        details = (((it.get('data') or {}).get('instant') or {}).get('details') or {})
        ws = details.get('wind_speed')
        wg = details.get('wind_speed_of_gust')
        if ws is not None:
            winds.append(float(ws))
        if wg is not None:
            gusts.append(float(wg))
    if not winds and not gusts:
        return None
    payload = {
        'avg': round(sum(winds) / max(len(winds), 1), 1) if winds else 0.0,
        'gust': round(max(gusts) if gusts else max(winds), 1),
    }
    _FORECAST_CACHE[key] = (now_ts + 1800, payload)
    return payload


def _fmt_label(mode: Mode, period: Period, metric: Metric, selected: _date) -> str:
    if mode == 'forecast':
        return 'Forventet vind neste 24 timer (Yr)'
    mtxt = 'vindkast (m/s)' if metric == 'gust' else 'gjennomsnittsvind (m/s)'
    if period == 'hour':
        return f'Siste 24 timer – {mtxt}'
    if period == 'day':
        return f'Døgn {selected.isoformat()} – {mtxt}'
    return f'Måned {selected.strftime("%Y-%m")} – {mtxt}'


def build_wind_map_html(*, mode: Mode = 'observed', period: Period = 'hour', metric: Metric = 'avg', date_str: Optional[str] = None,
                        bbox: Optional[str] = None, z: Optional[str] = None, clat: Optional[str] = None, clon: Optional[str] = None,
                        timeout: int = DEFAULT_TIMEOUT, show_heatmap: bool = True, top_n: int = 80, forecast_limit: int = 120) -> str:
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

    s, w, n, e = 57.0, 4.0, 71.5, 31.5
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

    merged = pd.DataFrame()

    if mode == 'observed':
        auth = _env_auth()
        session = requests.Session()
        if period == 'hour':
            start_dt = datetime.now(timezone.utc) - timedelta(hours=24)
            end_dt = datetime.now(timezone.utc)
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
        element = 'max(wind_speed_of_gust PT1H)' if metric == 'gust' else 'mean(wind_speed PT1H)'
        obs = _fetch_obs(session, auth=auth, source_ids=stations['baseId'].tolist(), element=element, referencetime=referencetime, timeout=timeout)
        agg = _aggregate_obs(obs, metric=metric)
        if not agg.empty:
            merged = stations.merge(agg, left_on='baseId', right_on='sourceId', how='inner')
    else:
        session = requests.Session()
        rows: list[dict[str, Any]] = []
        for _, st in stations.head(forecast_limit).iterrows():
            fc = _forecast_station(session, lat=float(st['lat']), lon=float(st['lon']))
            if not fc:
                continue
            rows.append({'baseId': st['baseId'], 'value': fc[metric], 'points': 1})
        if rows:
            fdf = pd.DataFrame(rows)
            merged = stations.merge(fdf, on='baseId', how='inner')

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
        folium.CircleMarker(
            location=[float(r.lat), float(r.lon)],
            radius=4 + 8 * ratio,
            color=color,
            fill=True,
            fill_opacity=0.9,
            tooltip=f"{getattr(r, 'name', r.baseId)}: {val:.1f} m/s",
            popup=(
                f"<b>{getattr(r, 'name', r.baseId)}</b><br>"
                f"Stasjon: {r.baseId}<br>"
                f"Vind: <b>{val:.1f} m/s</b><br>"
                f"Datapunkter: {int(getattr(r, 'points', 0) or 0)}"
            ),
        ).add_to(m)

    label = _fmt_label(mode, period, metric, selected)
    top_txt = ''.join(f"<li>{getattr(r, 'name', r.baseId)}: <b>{float(r.value):.1f}</b> m/s</li>" for r in top.head(10).itertuples())
    info = f"""
    <div style="position:fixed;left:14px;bottom:14px;z-index:9999;background:rgba(15,23,42,.92);color:#e2e8f0;padding:12px 14px;border-radius:12px;border:1px solid #334155;max-width:330px;font:13px/1.35 system-ui;">
      <div style="font-weight:800;margin-bottom:4px;">💨 Vindkart Norge</div>
      <div style="color:#93c5fd;margin-bottom:8px;">{label}</div>
      <ol style="margin:0;padding-left:18px;max-height:180px;overflow:auto;">{top_txt}</ol>
      {"<div style='margin-top:8px;color:#94a3b8;font-size:11px;'>Forventet vind beregnes på et utvalg stasjoner i kartutsnittet.</div>" if mode=='forecast' else ''}
    </div>
    """
    m.get_root().html.add_child(folium.Element(info))
    return m.get_root().render()
