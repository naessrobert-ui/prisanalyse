"""Prisfall gjennom flere kjøringer; all ekstern I/O erstattes med testdobler."""
import io
import json

import pandas as pd
import pytest
from botocore.exceptions import ClientError

from scripts import prisfall_vakt as p

DAY1 = "2026-09-04T05:00:00Z"
DAY2 = "2026-09-05T05:00:00Z"
NOW = "2026-09-05T06:00:00Z"


@pytest.fixture(autouse=True)
def thresholds(monkeypatch):
    monkeypatch.setattr(p.kupp, "EL_TIER_ON", False)
    monkeypatch.setattr(p.kupp, "RABATT_TRAPP", [(float("inf"), 6)])
    monkeypatch.setattr(p.kupp, "RABATT_KR_MIN", 0)
    monkeypatch.setattr(p.kupp, "UNDER_HURTIG", False)
    monkeypatch.setattr(p.kupp, "LOCATION_CODES", [])
    monkeypatch.setattr(p.kupp, "STED_FILTER", [])
    monkeypatch.setattr(p.kupp, "KURANTE", [])
    monkeypatch.setattr(p.kupp, "HJEMFYLKE_KODE", "0.22046")
    monkeypatch.setattr(p.kupp, "UTENFOR_TILLEGG_PP", 8)
    monkeypatch.setattr(p.kupp, "NABO_KODER", [])


def car(price=300000, date=DAY1, **kwargs):
    row = {"FinnKode": 123, "Produsent": "Kia", "Modell": "EV6",
           "drivstoff": "Elektrisk", "Solgt": "NEI", "salgspris": price,
           "Pris": 400000, "Pris_ny": price, "Dato_ny": date,
           "forventet_pris": 310000, "fylke": "Vestland", "sted": "Bergen",
           "årstall": 2023, "kjørelengde": 45000}
    row.update(kwargs)
    return row


def seed(*rows):
    return p.finn_prisfall(pd.DataFrame(rows or [car()]), None, NOW)[0]


def test_first_run_new_ads_and_original_price_do_not_alert():
    state, alerts = p.finn_prisfall(pd.DataFrame([car(280000)]), None, NOW)
    assert alerts == []  # opprinnelig pris 400k er ikke sammenligningsgrunnlag
    state, alerts = p.finn_prisfall(pd.DataFrame([car(280000), car(200000, FinnKode=999)]), state, NOW)
    assert alerts == []
    assert len(state["cars"]) == 2


@pytest.mark.parametrize("fuel", ["Elektrisk", " EL ", "elbil", "BEV"])
def test_ev_aliases_and_fresh_valuation(fuel):
    state = seed(car(drivstoff=fuel))
    _, alerts = p.finn_prisfall(pd.DataFrame([car(280000, DAY2, drivstoff=fuel,
                                                rabatt_pct=-99, rabatt_kr=-999)]), state, NOW)
    assert len(alerts) == 1
    row = alerts[0]
    assert row["pris_for"] == 300000
    assert row["prisfall_kr"] == 20000
    assert row["rabatt_kr"] == 30000  # beregnes på nytt
    assert row["url"] == "https://www.finn.no/mobility/item/123"
    assert state["cars"]["123"]["price"] == 300000  # ren funksjon


@pytest.mark.parametrize("fuel", ["Diesel", "Bensin", "Hybrid", "Plug-in hybrid", "PHEV", "", None, pd.NA])
def test_only_explicit_pure_evs_even_if_kupp_filter_says_ev(fuel, monkeypatch):
    monkeypatch.setattr(p.kupp, "DRIVSTOFF_FILTER", {"elektrisk"})
    state, alerts = p.finn_prisfall(pd.DataFrame([car(260000, DAY2, drivstoff=fuel)]), seed(), NOW)
    assert alerts == []
    assert state["cars"]["123"]["price"] == 300000


@pytest.mark.parametrize("old,new,expected", [(300000, 291000, 1),  # 3 %, under 10k
                                            (800000, 790000, 1),  # 10k, under 3 %
                                            (300000, 295000, 0),
                                            (300000, 300000, 0),
                                            (300000, 310000, 0)])
def test_drop_threshold_is_kr_or_percent(old, new, expected):
    _, alerts = p.finn_prisfall(pd.DataFrame([car(new, DAY2, forventet_pris=new * 1.2)]),
                               seed(car(old)), NOW)
    assert len(alerts) == expected


@pytest.mark.parametrize("price", [0, 1, None, float("nan"), float("inf"), -10])
def test_invalid_price_never_updates_baseline(price):
    state, alerts = p.finn_prisfall(pd.DataFrame([car(price, DAY2)]), seed(), NOW)
    assert alerts == []
    assert state["cars"]["123"]["price"] == 300000


def test_large_drop_is_not_necessarily_attractive():
    state, alerts = p.finn_prisfall(pd.DataFrame([car(280000, DAY2, forventet_pris=270000)]), seed(), NOW)
    assert alerts == []
    assert "pending" not in state["cars"]["123"]


def test_missing_valuation_retried_without_new_price_change():
    frame = pd.DataFrame([car(280000, DAY2, forventet_pris=None)])
    state, alerts = p.finn_prisfall(frame, seed(), NOW)
    assert alerts == []
    assert "pending" in state["cars"]["123"]
    frame["forventet_pris"] = 310000
    _, alerts = p.finn_prisfall(frame, state, NOW)
    assert len(alerts) == 1


def test_older_snapshot_cannot_roll_price_back():
    state, _ = p.finn_prisfall(pd.DataFrame([car(280000, DAY2)]), seed(), NOW)
    next_state, alerts = p.finn_prisfall(pd.DataFrame([car(250000, DAY1)]), state, NOW)
    assert alerts == []
    assert next_state["cars"]["123"]["price"] == 280000


@pytest.mark.parametrize("status", ["JA", "FJERNET", "", None])
def test_inactive_cars_cancel_pending(status):
    state, _ = p.finn_prisfall(pd.DataFrame([car(280000, DAY2)]), seed(), NOW)
    next_state, alerts = p.finn_prisfall(pd.DataFrame([car(280000, DAY2, Solgt=status)]), state, NOW)
    assert alerts == []
    assert "pending" not in next_state["cars"]["123"]


def test_absent_and_stale_cars_never_send_pending():
    frame = pd.DataFrame([car(280000, DAY2)])
    state, _ = p.finn_prisfall(frame, seed(), NOW)
    assert p.finn_prisfall(frame.iloc[:0], state, NOW)[1] == []
    assert p.finn_prisfall(frame, state, "2026-09-08T06:00:00Z")[1] == []


def test_geo_and_brand_requirements_reused(monkeypatch):
    frame = pd.DataFrame([car(280000, DAY2, fylke="Oslo")])
    assert p.finn_prisfall(frame, seed(), NOW)[1] == []  # 9.7 % < 6 + 8 pp
    monkeypatch.setattr(p.kupp, "EL_TIER_ON", True)
    monkeypatch.setattr(p.kupp, "EL_MERKER_LAV", {"kia"})
    monkeypatch.setattr(p.kupp, "EL_TERSKEL_LAV", 1)
    assert len(p.finn_prisfall(frame, seed(), NOW)[1]) == 1


def test_fylke_and_sted_filters(monkeypatch):
    monkeypatch.setattr(p.kupp, "LOCATION_CODES", ["0.20061"])
    frame = pd.DataFrame([car(280000, DAY2)])
    assert p.finn_prisfall(frame, seed(), NOW)[1] == []
    monkeypatch.setattr(p.kupp, "LOCATION_CODES", [])
    monkeypatch.setattr(p.kupp, "STED_FILTER", ["voss"])
    assert p.finn_prisfall(frame, seed(), NOW)[1] == []


class FakeS3:
    def __init__(self):
        self.objects = {}
        self.puts = 0
        self.fail_write = False

    def get_object(self, Bucket, Key):
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
        return {"Body": io.BytesIO(self.objects[Key])}

    def put_object(self, Bucket, Key, Body, **kwargs):
        if self.fail_write:
            raise RuntimeError("S3 utilgjengelig")
        self.objects[Key] = Body
        self.puts += 1


def test_full_lifecycle_failure_retry_dedupe_and_further_drop(tmp_path, monkeypatch):
    s3 = FakeS3()
    path = tmp_path / "biler.parquet"
    sent = []
    monkeypatch.setattr(p.kupp, "_send_pushover", lambda rows, **kw: sent.extend(rows) or True)
    pd.DataFrame([car()]).to_parquet(path)
    assert p.kjor(path, s3=s3, now=NOW) == 0
    assert sent == []

    pd.DataFrame([car(280000, DAY2)]).to_parquet(path)
    monkeypatch.setattr(p.kupp, "_send_pushover", lambda *a, **kw: False)
    assert p.kjor(path, s3=s3, now=NOW) == 1
    assert "pending" in p.last_state(s3)["cars"]["123"]

    monkeypatch.setattr(p.kupp, "_send_pushover", lambda rows, **kw: sent.extend(rows) or True)
    assert p.kjor(path, s3=s3, now=NOW) == 0
    assert len(sent) == 1
    assert p.kjor(path, s3=s3, now=NOW) == 0
    assert len(sent) == 1
    for price, expected_count in [(300000, 1), (280000, 1), (260000, 2)]:
        pd.DataFrame([car(price, "2026-09-05T05:30:00Z")]).to_parquet(path)
        p.kjor(path, s3=s3, now=NOW)
        assert len(sent) == expected_count


def test_dry_run_has_no_writes_or_notifications(tmp_path, monkeypatch):
    s3 = FakeS3()
    p.lagre_state(s3, seed())
    before = dict(s3.objects)
    path = tmp_path / "biler.parquet"
    pd.DataFrame([car(280000, DAY2)]).to_parquet(path)
    monkeypatch.setattr(p.kupp, "_send_pushover", lambda *a, **kw: pytest.fail("sending i dry-run"))
    assert p.kjor(path, dry_run=True, s3=s3, now=NOW) == 0
    assert s3.objects == before


def test_state_read_errors_abort_instead_of_reseeding():
    s3 = FakeS3()
    assert p.last_state(s3) is None
    s3.objects[p.STATE_KEY] = b"broken json"
    with pytest.raises(ValueError):
        p.last_state(s3)
    s3.objects[p.STATE_KEY] = json.dumps({"version": 2, "cars": {}}).encode()
    with pytest.raises(ValueError):
        p.last_state(s3)

    class DeniedS3:
        def get_object(self, **kw):
            raise ClientError({"Error": {"Code": "AccessDenied"}}, "GetObject")
    with pytest.raises(ClientError):
        p.last_state(DeniedS3())


def test_state_write_failure_prevents_sending(tmp_path, monkeypatch):
    s3 = FakeS3()
    p.lagre_state(s3, seed())
    s3.fail_write = True
    path = tmp_path / "biler.parquet"
    pd.DataFrame([car(280000, DAY2)]).to_parquet(path)
    monkeypatch.setattr(p.kupp, "_send_pushover", lambda *a, **kw: pytest.fail("send før lagring"))
    with pytest.raises(RuntimeError):
        p.kjor(path, s3=s3, now=NOW)


def test_limit_does_not_lose_unsent_candidates(tmp_path, monkeypatch):
    s3 = FakeS3()
    p.lagre_state(s3, seed(car(), car(FinnKode=456)))
    path = tmp_path / "biler.parquet"
    pd.DataFrame([car(280000, DAY2), car(275000, DAY2, FinnKode=456)]).to_parquet(path)
    sent = []
    monkeypatch.setattr(p, "MAX_VARSLER", 1)
    monkeypatch.setattr(p.kupp, "_send_pushover", lambda rows, **kw: sent.extend(rows) or True)
    p.kjor(path, s3=s3, now=NOW)
    p.kjor(path, s3=s3, now=NOW)
    assert [r["FinnKode"] for r in sent] == ["456", "123"]


def test_missing_schema_fails_and_seed_clears_pending():
    with pytest.raises(ValueError):
        p.finn_prisfall(pd.DataFrame([{"FinnKode": 123}]), seed(), NOW)
    frame = pd.DataFrame([car(280000, DAY2)])
    state, _ = p.finn_prisfall(frame, seed(), NOW)
    state, alerts = p.finn_prisfall(frame, state, NOW, seed=True)
    assert alerts == []
    assert "pending" not in state["cars"]["123"]


def test_notification_includes_actual_drop_and_uses_shared_sender(monkeypatch):
    _, alerts = p.finn_prisfall(pd.DataFrame([car(280000, DAY2)]), seed(), NOW)
    row = alerts[0]
    message = p._melding(row)
    assert "300 000 → 280 000" in message
    assert "20 000 kr" in message
    assert len(message) <= 1024
    monkeypatch.setenv("PUSHOVER_TOKEN", "test-token")
    monkeypatch.setenv("PUSHOVER_USER", "test-user")
    calls = []

    class Response:
        status_code = 200

    monkeypatch.setattr(p.kupp.requests, "post", lambda *a, **kw: calls.append(kw) or Response())
    assert p.kupp._send_pushover([row], melding=message, tittel="Prisfall")
    assert calls[0]["data"]["message"] == message
    assert calls[0]["data"]["title"] == "Prisfall"
