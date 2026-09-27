"""Antall favoritter fra FINNs favoritt-endepunkt (ingen nettverk)."""
from scripts import kupp_vakt as k


def test_parse_counter_fra_api():
    data = {"item": {"itemType": "Ad", "itemId": 476830592}, "counter": 12}
    assert k.parse_favoritter(data) == 12
    assert k.parse_favoritter({"counter": 0}) == 0


def test_parse_ugyldig_svar():
    for data in (None, [], {}, {"counter": None}, {"counter": "12"},
                 {"counter": -1}, {"counter": True}):
        assert k.parse_favoritter(data) is None


class _Resp:
    def __init__(self, data):
        self._data = data

    def json(self):
        return self._data


def test_hent_favoritter_bruker_api_url(monkeypatch):
    kall = []
    monkeypatch.setattr(k, "_fetch", lambda s, url, **kw: kall.append(url) or _Resp({"counter": 7}))
    assert k.hent_favoritter("476830592") == 7
    assert kall == ["https://www.finn.no/favorite-frontend-api/Ad/476830592/counter"]


def test_hent_favoritter_ugyldig_json_gir_none(monkeypatch):
    class Feil:
        def json(self):
            raise ValueError("ikke json")
    monkeypatch.setattr(k, "_fetch", lambda *a, **kw: Feil())
    assert k.hent_favoritter("1") is None


def test_berik_favoritter_feil_gir_none(monkeypatch):
    monkeypatch.setattr(k, "_fetch", lambda *a, **kw: None)
    biler = [{"FinnKode": "1"}, {"FinnKode": "2"}]
    k.berik_favoritter(biler)
    assert [b["favoritter"] for b in biler] == [None, None]


def test_meldinger_viser_favoritter():
    b = {"Merke": "Tesla", "Modell": "Model 3", "Årstall": 2021, "Kjørelengde": 50000,
         "Pris": 250000, "forventet_pris": 300000, "rabatt_pct": 16.7,
         "rabatt_kr": 50000, "url": "u", "favoritter": 14}
    assert "Favoritter: 14" in k._formater_bil(b)
    assert "❤ 14" in k._pushover_melding([b])
    b["favoritter"] = None
    assert "Favoritter" not in k._formater_bil(b)
    assert "❤" not in k._pushover_melding([b])
