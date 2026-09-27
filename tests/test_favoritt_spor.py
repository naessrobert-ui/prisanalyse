"""Favoritt-sporing og populær-varsel; ingen FINN-, S3- eller Pushover-kall."""
import io
import json
from datetime import datetime, timedelta, timezone

import pytest

from scripts import favoritt_spor as f

T0 = datetime(2026, 9, 27, 10, 0, tzinfo=timezone.utc)
REGLER = [(10, 10), (30, 60)]


def bil(fk="1", **kw):
    return {"FinnKode": fk, "Merke": "Tesla", "Modell": "Model Y", "Årstall": "2022",
            "Kjørelengde": 40000, "Pris": 350000, "sted": "Bergen",
            "url": f"https://www.finn.no/mobility/item/{fk}", **kw}


def kjor(spor, nye, naa, favs, **kw):
    """favs: dict fk -> favoritter (eller callable)."""
    kall = []

    def hent(fk):
        kall.append(fk)
        return favs(fk) if callable(favs) else favs.get(fk)
    spor, pop, ferdige = f.oppdater(spor, nye, naa, hent, regler=REGLER, **kw)
    return spor, pop, ferdige, kall


def varm_spor(t=T0):
    """Spor der forrige kjøring var for 10 min siden (alder er kjent)."""
    return {"biler": {}, "sist_kjort": (t - timedelta(minutes=10)).isoformat()}


def test_parse_regler():
    assert f.parse_regler("10:10, 30:60") == [(10, 10), (30, 60)]
    assert f.parse_regler("") == []


@pytest.mark.parametrize("fav, alder, forventet", [
    (10, 0, (10, 10)), (10, 10, (10, 10)), (9, 5, None),
    (10, 20, None), (30, 60, (30, 60)), (30, 61, None), (None, 0, None),
])
def test_populaer_regel(fav, alder, forventet):
    assert f.populaer_regel(fav, alder, REGLER) == forventet


def test_ny_bil_med_mange_favoritter_varsles():
    spor, pop, _, kall = kjor(varm_spor(), [bil("1"), bil("2")], T0, {"1": 12, "2": 3})
    assert [fk for fk, _, _ in pop] == ["1"]
    assert pop[0][2] == (10, 10)
    assert kall == ["1", "2"] or sorted(kall) == ["1", "2"]
    assert spor["biler"]["1"]["maalinger"][0]["fav"] == 12


def test_30_innen_time_og_ingen_dobbeltvarsel():
    spor, pop, _, _ = kjor(varm_spor(), [bil("1")], T0, {"1": 2})
    assert pop == []
    t = T0 + timedelta(minutes=40)
    spor, pop, _, _ = kjor(spor, [], t, {"1": 31})
    assert [fk for fk, _, _ in pop] == ["1"] and pop[0][2] == (30, 60)
    spor["biler"]["1"]["populaer_varslet"] = True  # sendt
    spor, pop, _, kall = kjor(spor, [], t + timedelta(minutes=10), {"1": 40})
    assert pop == [] and kall == []  # ikke målt mer i vinduet etter varsel


def test_for_sent_gir_ikke_varsel():
    spor, _, _, _ = kjor(varm_spor(), [bil("1")], T0, {"1": 0})
    spor, pop, _, _ = kjor(spor, [], T0 + timedelta(minutes=70), {"1": 50})
    assert pop == []


def test_ukjent_alder_etter_opphold_gir_ikke_varsel():
    kald = {"biler": {}, "sist_kjort": (T0 - timedelta(hours=7)).isoformat()}
    spor, pop, _, _ = kjor(kald, [bil("1")], T0, {"1": 80})
    assert pop == []
    assert spor["biler"]["1"]["alder_kjent"] is False
    spor, pop, _, _ = kjor(spor, [], T0, {"1": 80})  # samme kjøring igjen
    assert pop == []


def test_forste_kjoring_uten_historikk_er_ukjent_alder():
    spor, pop, _, _ = kjor({}, [bil("1")], T0, {"1": 80})
    assert pop == [] and spor["sist_kjort"] == T0.isoformat()


def test_kupp_varsles_ikke_ogsaa_som_populaer():
    spor, pop, _, kall = kjor(varm_spor(), [bil("1")], T0, {"1": 99},
                              kupp_koder=["1"], forhaandsmaalt={"1": 99})
    assert pop == [] and kall == []  # gjenbruker kuppvaktens måling
    assert spor["biler"]["1"]["kupp"] is True


def test_maalepunkter_3t_24t_og_logg():
    spor, _, _, _ = kjor(varm_spor(), [bil("1")], T0, {"1": 1})
    # Etter vinduet og før 3 t: ingen måling
    _, _, _, kall = kjor(spor, [], T0 + timedelta(minutes=120), {"1": 2})
    assert kall == []
    spor, _, _, kall = kjor(spor, [], T0 + timedelta(minutes=185), {"1": 5})
    assert kall == ["1"]
    _, _, _, kall = kjor(spor, [], T0 + timedelta(minutes=195), {"1": 5})
    assert kall == []
    spor, _, ferdige, kall = kjor(spor, [], T0 + timedelta(hours=24, minutes=5), {"1": 20})
    assert kall == ["1"] and "1" in ferdige and "1" not in spor["biler"]
    assert [m["fav"] for m in ferdige["1"]["maalinger"]] == [1, 5, 20]


def test_maks_kall_prioriterer_yngste_i_vinduet():
    spor = varm_spor()
    spor["biler"]["gammel"] = f.ny_post(bil("gammel"), (T0 - timedelta(minutes=200)).isoformat(), True)
    spor["biler"]["gammel"]["maalinger"] = [{"t": "", "alder_min": 0, "fav": 0}]
    _, _, _, kall = kjor(spor, [bil("ny")], T0, {"ny": 1, "gammel": 1}, maks_kall=1)
    assert kall == ["ny"]


def test_ny_post_er_json_trygg():
    post = f.ny_post(bil("1", Merke=float("nan"), Årstall=2021.0,
                         rabatt_pct=float("nan")), T0.isoformat(), True)
    assert post["Merke"] is None and post["Årstall"] == 2021 and post["rabatt_pct"] is None
    json.dumps(post, allow_nan=False)


def test_melding():
    post = f.ny_post(bil("1", forventet_pris=400000, rabatt_pct=12.5), T0.isoformat(), True)
    post["maalinger"].append({"t": "", "alder_min": 8.0, "fav": 14})
    m = f.melding(post)
    assert "❤ 14 favoritter etter 8 min" in m
    assert "Tesla Model Y 2022" in m and "-12% mot 400 000" in m
    assert post["url"] in m


class FakeS3:
    def __init__(self):
        self.data = {}

    def get_object(self, Bucket, Key):
        if Key not in self.data:
            raise KeyError(Key)
        return {"Body": io.BytesIO(self.data[Key])}

    def put_object(self, Bucket, Key, Body, ContentType=None):
        self.data[Key] = Body


def test_kjor_sender_og_lagrer(monkeypatch):
    s3 = FakeS3()
    s3.data[f.SPOR_KEY] = json.dumps(varm_spor()).encode()
    monkeypatch.setattr(f.kupp, "FAVORITTER_ON", True)
    monkeypatch.setattr(f.kupp, "hent_favoritter", lambda fk, s=None: 15)
    sendt = []
    monkeypatch.setattr(f.kupp, "_send_pushover",
                        lambda rows, **kw: sendt.append(kw["tittel"]) or True)
    assert f.kjor(s3, [bil("1")], T0.isoformat()) == 1
    assert sendt and "Populær" in sendt[0]
    lagret = json.loads(s3.data[f.SPOR_KEY])
    assert lagret["biler"]["1"]["populaer_varslet"] is True


def test_kjor_dry_run_skriver_ikke(monkeypatch):
    s3 = FakeS3()
    monkeypatch.setattr(f.kupp, "FAVORITTER_ON", True)
    monkeypatch.setattr(f.kupp, "hent_favoritter", lambda fk, s=None: 15)
    monkeypatch.setattr(f.kupp, "_send_pushover", lambda *a, **kw: pytest.fail("sendt"))
    f.kjor(s3, [bil("1")], T0.isoformat(), dry_run=True)
    assert s3.data == {}


def test_kjor_feil_stopper_ikke(monkeypatch):
    monkeypatch.setattr(f.kupp, "FAVORITTER_ON", True)
    monkeypatch.setattr(f, "oppdater", lambda *a, **kw: 1 / 0)
    assert f.kjor(FakeS3(), [bil("1")], T0.isoformat()) == 0
