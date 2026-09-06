"""Timevis prisfallvakt. Kjør fra repo-roten: python -m scripts.prisfall_time.

Søker alle resultatsider; scorer bare prisfall med databaseegenskaper.
prisfall_ny.json er autoritativ, atomisk state. prisfall_gml.json er forrige kopi.
"""
from __future__ import annotations

import argparse
import copy
import fcntl
import json
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlparse

import pandas as pd
from bs4 import BeautifulSoup

from scripts import kupp_vakt as kupp
from scripts import prisfall_vakt as daily

SEARCH_URL = ('https://www.finn.no/mobility/search/car?dealer_segment=3&fuel=4'
              '&mileage_to=140000&registration_class=1&sales_form=1')


def atomic_json(path, value):
    temp = path.with_suffix('.tmp')
    with temp.open('w', encoding='utf-8') as out:
        json.dump(value, out, ensure_ascii=False, allow_nan=False)
        out.flush()
        os.fsync(out.fileno())
    os.replace(temp, path)


def scrape(url=SEARCH_URL, max_pages=500):
    """Avbryt ved nettfeil, ukjent sideformat eller gjentatt side."""
    parsed = urlparse(url)
    if parsed.hostname != 'www.finn.no' or parsed.path != '/mobility/search/car':
        raise ValueError('Forventer et FINN-bilsøk')
    query = dict(parse_qsl(parsed.query))
    if query.get('fuel') != '4':
        raise ValueError('Timevakten krever elbilfilter fuel=4')
    cars, seen_pages = {}, set()
    total = None
    with kupp._make_session() as session:
        for page in range(1, max_pages + 1):
            query['page'] = str(page)
            response = kupp._fetch(session, parsed._replace(query=urlencode(query)).geturl())
            if response is None:
                raise RuntimeError(f'FINN side {page} feilet; beholder gamle filer')
            soup = BeautifulSoup(response.text, 'lxml')
            cards = kupp._find_cards(soup)
            if not cards:
                raise RuntimeError(f'Ingen annonsekort på side {page}; beholder gamle filer')
            fingerprint = tuple(sorted(kupp._finnkode(a.get('href')) for a, _ in cards))
            if fingerprint in seen_pages:
                raise RuntimeError('FINN gjentok en resultatside; avbryter')
            seen_pages.add(fingerprint)
            if page == 1:
                match = re.search(r'(\d[\d\s\xa0]*)\s+(?:treff|annonser|biler)\b',
                                  soup.get_text(' ', strip=True), re.I)
                if match:
                    total = int(re.sub(r'\D', '', match[1]))
            for link, card in cards:
                code = kupp._finnkode(link.get('href'))
                raw_price = kupp._price(card)
                price = daily._tall(raw_price)
                year, km, fuel, _ = kupp._fordel(kupp._meta_text(card))
                # Ugyldig pris får ikke etablere en falsk grunnpris.
                if not code or price is None or price < 1500:
                    continue
                if fuel and kupp._norm_driv(fuel) != 'elektrisk':
                    continue
                if km > int(query.get('mileage_to', '140000')):
                    continue
                cars[code] = {'price': price, 'km': km if year else None}
            next_page = any(
                dict(parse_qsl(urlparse(a.get('href', '')).query)).get('page') == str(page + 1)
                for a in soup.select('a[href]'))
            if not next_page:
                # Et manglende pagineringsfelt må ikke tolkes som komplett søk.
                if total is None or len(cars) < total * 0.95:
                    raise RuntimeError('Kan ikke bekrefte komplett søk; sjekk FINN-paginering/antall')
                print(f'[prisfall_time] {page} sider, {len(cars)} gyldige priser')
                return cars
        raise RuntimeError('Sidegrensen nådd før hele søket er hentet')


def compare(previous, prices, now, search_url=SEARCH_URL):
    if previous and previous['search_url'] != search_url:
        raise ValueError('Søket er endret; bruk ny state-mappe for ny grunnpris')
    state = copy.deepcopy(previous) if previous else {
        'version': 1, 'search_url': search_url, 'cars': {}}
    for code, current in prices.items():
        price = current['price']
        record = state['cars'].get(code)
        if record is None:
            record = {'price': price, 'first_seen': now, 'history': [], 'alerted': []}
        if price != record['price']:
            record.pop('pending', None)
            old = record['price']
            if (old > price and (old - price >= daily.MIN_KR or
                                 (old - price) / old * 100 >= daily.MIN_PCT)
                    and price not in record['alerted']):
                record['pending'] = {'old': old, 'new': price, 'at': now}
        if not record['history'] or record['history'][-1]['price'] != price:
            record['history'].append({'at': now, 'price': price})
            record['history'] = record['history'][-100:]
        record.update(current, seen_at=now)
        state['cars'][code] = record
    # Fjern ventende varsler for biler utenfor det ferske søkeresultatet.
    for code, record in state['cars'].items():
        if code not in prices:
            record.pop('pending', None)
    cutoff = daily._tid(now) - pd.Timedelta(days=120)
    state['cars'] = {c: r for c, r in state['cars'].items()
                     if daily._tid(r['seen_at']) >= cutoff}
    state['updated_at'] = now
    return state


def database(directory, local_path=None):
    if local_path:
        return pd.read_parquet(local_path)
    s3 = kupp._s3()
    key = 'calc/bil/database_biler.parquet'
    etag = s3.head_object(Bucket=kupp.S3_BUCKET, Key=key)['ETag'].strip('"')
    # En fast cachefil og separat ETag. Markør skrives etter vellykket filbytte.
    path, marker = directory / 'database.parquet', directory / 'database_etag.json'
    if not path.exists() or not marker.exists() or json.loads(marker.read_text()) != etag:
        temp = directory / 'database.download'
        s3.download_file(kupp.S3_BUCKET, key, str(temp))
        pd.read_parquet(temp, columns=['FinnKode'])  # bekreft gyldig parquet
        os.replace(temp, path)
        atomic_json(marker, etag)
    return pd.read_parquet(path)


def evaluate(state, db, now):
    db = db.copy()
    db['_code'] = db['FinnKode'].map(daily._kode)
    if 'Dato_ny' in db:
        db['_last'] = pd.to_datetime(db['Dato_ny'], utc=True, errors='coerce')
        db = db.sort_values('_last', na_position='first')
    details = db.drop_duplicates('_code', keep='last').set_index('_code').to_dict('index')
    rows = []
    for code, record in state['cars'].items():
        pending = record.get('pending')
        if not pending:
            continue
        if daily._tid(pending['at']) < daily._tid(now) - pd.Timedelta(days=2):
            record.pop('pending')
            continue
        raw = details.get(code)
        if raw is None or record.get('km') is None:
            print(f'[prisfall_time] {code}: venter på databaseegenskaper/km-stand')
            continue
        raw = dict(raw)
        raw['Merke'] = raw.get('Merke', raw.get('Produsent', ''))
        raw.update(FinnKode=code, Pris=record['price'], Pris_ny=record['price'],
                   salgspris=record['price'], Kjørelengde=record['km'],
                   kjørelengde=record['km'], Solgt='NEI', Dato_ny=now)
        rows.append(raw)
    if not rows:
        return []
    scored = kupp.scorer_biler(pd.DataFrame(rows), modeller=None)
    alerts = []
    for raw in scored.to_dict('records'):
        row = daily._varselrad(raw)
        row['sted'] = daily._tekst(raw.get('Sted', raw.get('sted')))
        code = row['FinnKode']
        record = state['cars'][code]
        expected = daily._tall(row.get('forventet_pris'))
        if expected is None or expected <= 0:
            continue
        row['rabatt_kr'] = expected - row['Pris']
        row['rabatt_pct'] = row['rabatt_kr'] / expected * 100
        if not daily._attraktiv(row):
            record.pop('pending')
            continue
        row['pris_for'] = record['pending']['old']
        row['prisfall_kr'] = row['pris_for'] - row['Pris']
        row['prisfall_pct'] = row['prisfall_kr'] / row['pris_for'] * 100
        first = daily._tid(raw.get('Dato'))
        extra = ''
        if pd.notna(first) and first <= daily._tid(now):
            extra += f"\nFørst sett i databasen: {(daily._tid(now) - first).days} dager siden"
        original = daily._tall(details[code].get('Pris'))
        if original and original >= 1500:
            extra += '\nOpprinnelig databasepris: ' + f'{original:,.0f}'.replace(',', ' ') + ' kr'
        history = record['history'][-4:]
        extra += '\nObserverte priser: ' + ' → '.join(
            f"{p['price']:,.0f}".replace(',', ' ') for p in history) + ' kr'
        row['_message'] = daily._melding(row) + extra
        alerts.append(row)
    return sorted(alerts, key=lambda r: r['rabatt_kr'], reverse=True)


def run(directory, *, dry_run=False, db_path=None, search_url=SEARCH_URL):
    directory.mkdir(parents=True, exist_ok=True)
    # Også manuelle kjøringer låses mot timeren.
    with (directory / 'prisfall.lock').open('w') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        path = directory / 'prisfall_ny.json'
        previous = json.loads(path.read_text()) if path.exists() else None
        if previous and (previous.get('version') != 1 or not isinstance(previous.get('cars'), dict)):
            raise ValueError('Ukjent stateformat; avbryter')
        prices = scrape(search_url)
        if not prices:
            raise RuntimeError('Tomt søk; beholder tidligere grunnlag')
        if previous:
            recent = sum(r['seen_at'] == previous['updated_at'] for r in previous['cars'].values())
            if len(prices) < recent * 0.7:
                raise RuntimeError('Mer enn 30 % færre biler; kontroller innhentingen før videre kjøring')
        now = datetime.now(timezone.utc).isoformat()
        state = compare(previous, prices, now, search_url)
        # Lagre hendelser før scoring/sending. Nett- og modellfeil kan forsøkes igjen.
        if not dry_run:
            if previous:
                atomic_json(directory / 'prisfall_gml.json', previous)
            atomic_json(path, state)
        if previous is None:
            print(f'[prisfall_time] Første grunnlag: {len(prices)} biler; ingen varsler')
            return 0
        pending = sum('pending' in r for r in state['cars'].values())
        print(f'[prisfall_time] {len(prices)} biler; {pending} prisfall til vurdering')
        if not pending:
            return 0
        alerts = evaluate(state, database(directory, db_path), now)
        failed = False
        for row in alerts[:daily.MAX_VARSLER]:
            print(row['_message'])
            if dry_run:
                continue
            if kupp._send_pushover([row], melding=row['_message'], tittel='Prisfall på elbil'):
                record = state['cars'][row['FinnKode']]
                record['alerted'].append(row['Pris'])
                record.pop('pending', None)
                atomic_json(path, state)
            else:
                failed = True
        if not dry_run:
            atomic_json(path, state)
        print(f'[prisfall_time] {len(alerts)} attraktive prisfall; sending feilet: {failed}')
        return 1 if failed else 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--state-dir', type=Path,
                        default=Path(os.getenv('PRISFALL_TIME_DIR', 'data/prisfall-time')))
    parser.add_argument('--database', help='Valgfri lokal database_biler.parquet')
    parser.add_argument('--search-url', default=SEARCH_URL)
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()
    if daily.MIN_KR <= 0 or daily.MIN_PCT <= 0 or daily.MAX_VARSLER < 1:
        parser.error('Prisfallgrenser og maksimalt antall varsler må være positive')
    started = time.monotonic()
    try:
        return run(args.state_dir, dry_run=args.dry_run, db_path=args.database,
                   search_url=args.search_url)
    finally:
        print(f'[prisfall_time] Kjøretid: {time.monotonic() - started:.0f} sekunder')


if __name__ == '__main__':
    raise SystemExit(main())
