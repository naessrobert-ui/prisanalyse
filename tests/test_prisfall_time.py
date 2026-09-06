import json
from types import SimpleNamespace

import pandas as pd
import pytest

from scripts import prisfall_time as p

NOW = '2026-09-06T10:00:00+00:00'
LATER = '2026-09-06T11:00:00+00:00'


def prices(value):
    return {'123': {'price': value, 'km': 45000}}


def test_seed_drop_retry_price_change_and_missing():
    seed = p.compare(None, prices(300000), NOW)
    assert 'pending' not in seed['cars']['123']
    changed = p.compare(seed, prices(280000), LATER)
    assert changed['cars']['123']['pending']['old'] == 300000
    retry = p.compare(changed, prices(280000), LATER)
    assert retry['cars']['123']['pending'] == changed['cars']['123']['pending']
    assert 'pending' not in p.compare(retry, {}, LATER)['cars']['123']
    assert 'pending' not in p.compare(retry, prices(300000), LATER)['cars']['123']
    retry['cars']['123']['alerted'].append(280000)
    raised = p.compare(retry, prices(300000), LATER)
    assert 'pending' not in p.compare(raised, prices(280000), LATER)['cars']['123']


def test_changed_search_rejected_and_new_car_seeded():
    state = p.compare(None, prices(300000), NOW)
    with pytest.raises(ValueError):
        p.compare(state, prices(280000), LATER, 'other')
    new = p.compare(state, {'456': {'price': 100000, 'km': 0}}, LATER)
    assert 'pending' not in new['cars']['456']


def html(code, next_page=None, total=2):
    link = f'<a href="?page={next_page}">Neste</a>' if next_page else ''
    return (f'<h1>{total} treff</h1><article><a href="/mobility/item/{code}">Kia EV6</a>'
            '<span class="text-caption font-bold">2023 · 45 000 km · Elektrisk</span>'
            '<span>280 000 kr</span></article>' + link)


@pytest.mark.parametrize('pages,success', [
    ([html('123', 2), html('456')], True),
    ([html('123', 2), None], False),
    ([html('123', 2), html('123', 2)], False),
    ([html('123')], False),
    (['<html>Blocked</html>'], False),
])
def test_pagination_must_complete(monkeypatch, pages, success):
    responses = iter(pages)
    def fetch(*args):
        value = next(responses)
        return SimpleNamespace(text=value) if value else None
    monkeypatch.setattr(p.kupp, '_fetch', fetch)
    if success:
        assert set(p.scrape()) == {'123', '456'}
    else:
        with pytest.raises(RuntimeError):
            p.scrape()


def test_failed_scrape_preserves_both_snapshots(tmp_path, monkeypatch):
    old = p.compare(None, prices(300000), NOW)
    for name in ['prisfall_ny.json', 'prisfall_gml.json']:
        p.atomic_json(tmp_path / name, old)
    before = (tmp_path / 'prisfall_ny.json').read_bytes()
    def fail(*args):
        raise RuntimeError('network')
    monkeypatch.setattr(p, 'scrape', fail)
    with pytest.raises(RuntimeError):
        p.run(tmp_path)
    assert (tmp_path / 'prisfall_ny.json').read_bytes() == before
    assert (tmp_path / 'prisfall_gml.json').read_bytes() == before


def test_database_scoring_uses_fresh_price_km_and_history(monkeypatch):
    state = p.compare(p.compare(None, prices(300000), NOW), prices(280000), LATER)
    db = pd.DataFrame([{'FinnKode': 123, 'Produsent': 'Kia', 'Modell': 'EV6',
                        'Pris': 350000, 'kjørelengde': 40000, 'drivstoff': 'Elektrisk',
                        'årstall': 2023, 'Dato': '2026-08-01', 'sted': 'Bergen',
                        'fylke': 'Vestland'}])
    def score(rows, modeller):
        from bilradar_scorer import _normaliser_for_scoring
        result = _normaliser_for_scoring(rows)
        assert result.iloc[0]['salgspris'] == 280000
        assert result.iloc[0]['kjørelengde'] == 45000
        assert result.iloc[0]['Produsent'] == 'Kia'
        result['forventet_pris'] = 350000
        return result
    monkeypatch.setattr(p.kupp, 'scorer_biler', score)
    monkeypatch.setattr(p.daily, '_attraktiv', lambda row: True)
    alerts = p.evaluate(state, db, LATER)
    assert len(alerts) == 1
    msg = alerts[0]['_message']
    assert '45 000 km' in msg and 'Bergen' in msg
    assert '300 000 → 280 000' in msg
    assert '36 dager siden' in msg and '350 000 kr' in msg
    assert len(msg) <= 1024


def test_send_failure_retries_without_duplicate_after_success(tmp_path, monkeypatch):
    monkeypatch.setattr(p, 'scrape', lambda url: prices(300000))
    monkeypatch.setattr(p, 'database', lambda *args: None)
    assert p.run(tmp_path) == 0
    monkeypatch.setattr(p, 'scrape', lambda url: prices(280000))
    def evaluate(state, *_):
        return [{'FinnKode': '123', 'Pris': 280000, '_message': 'test'}]
    monkeypatch.setattr(p, 'evaluate', evaluate)
    monkeypatch.setattr(p.kupp, '_send_pushover', lambda *a, **kw: False)
    assert p.run(tmp_path) == 1
    assert 'pending' in json.loads((tmp_path / 'prisfall_ny.json').read_text())['cars']['123']
    monkeypatch.setattr(p.kupp, '_send_pushover', lambda *a, **kw: True)
    assert p.run(tmp_path) == 0
    monkeypatch.setattr(p, 'evaluate', lambda *a: pytest.fail('duplicate evaluation'))
    assert p.run(tmp_path) == 0
