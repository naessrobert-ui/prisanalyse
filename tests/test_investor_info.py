"""Tester for investor_info med falske Brreg-svar (ingen nettverk)."""

from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import investor_info as ii  # noqa: E402

ENHET = {
    "organisasjonsnummer": "123456789", "navn": "SKAGEN AS",
    "organisasjonsform": {"kode": "AS", "beskrivelse": "Aksjeselskap"},
    "naeringskode1": {"beskrivelse": "Fondsforvaltning"},
    "forretningsadresse": {"kommune": "STAVANGER"},
    "stiftelsesdato": "1993-04-01", "antallAnsatte": 120,
}
ROLLER = {"rollegrupper": [
    {"type": {"kode": "DAGL"}, "roller": [
        {"type": {"kode": "DAGL"}, "person": {"navn": {"fornavn": "Kari", "etternavn": "Nordmann"}}, "fratraadt": False},
    ]},
    {"type": {"kode": "STYR"}, "roller": [
        {"type": {"kode": "LEDE"}, "person": {"navn": {"fornavn": "Gammel", "etternavn": "Leder"}}, "fratraadt": True},
        {"type": {"kode": "LEDE"}, "person": {"navn": {"fornavn": "Ola", "mellomnavn": "B", "etternavn": "Hansen"}},
         "fratraadt": False},
        {"type": {"kode": "MEDL"}, "person": {"navn": {"fornavn": "Per", "etternavn": "Medlem"}}},
    ]},
]}
REGNSKAP = [
    {"regnskapsperiode": {"fraDato": "2023-01-01", "tilDato": "2023-12-31"},
     "resultatregnskapResultat": {"aarsresultat": 1.0}},
    {"regnskapsperiode": {"fraDato": "2024-01-01", "tilDato": "2024-12-31"}, "valuta": "NOK",
     "resultatregnskapResultat": {
         "aarsresultat": 250_000_000,
         "driftsresultat": {"driftsresultat": 300_000_000,
                            "driftsinntekter": {"sumDriftsinntekter": 900_000_000}}},
     "egenkapitalGjeld": {"egenkapital": {"sumEgenkapital": 1_200_000_000}},
     "eiendeler": {"sumEiendeler": 1_500_000_000}},
]


class FakeResp:
    def __init__(self, data, status=200):
        self._d, self.status_code = data, status

    def json(self):
        return self._d


class FakeSess:
    def __init__(self, sok=None):
        self.kall = []
        self.sok = sok if sok is not None else {"_embedded": {"enheter": [
            {"organisasjonsnummer": "123456789", "navn": "Skagen AS"},
            {"organisasjonsnummer": "999999999", "navn": "SKAGEN AS AVD BERGEN"},
        ]}}

    def get(self, url, params=None, timeout=None):
        self.kall.append(url)
        if url.endswith("/roller"):
            return FakeResp(ROLLER)
        if "regnskapsregisteret" in url:
            return FakeResp(REGNSKAP)
        if url == ii.SOK_URL:
            return FakeResp(self.sok)
        if url.endswith("/123456789"):
            return FakeResp(ENHET)
        return FakeResp(None, 404)


@pytest.fixture(autouse=True)
def _ingen_db(monkeypatch):
    ii._CACHE.clear()
    monkeypatch.setattr(ii, "hent_eiere", lambda orgnr, antall=5: [{"navn": "EIER AS", "aksjer": 10, "andel_pct": 60.0}])


def test_finn_orgnr():
    s = FakeSess()
    assert ii.finn_orgnr(s, "SKAGEN AS", "123456789") == ("123456789", "vps")
    assert ii.finn_orgnr(s, "Skagen AS.", None) == ("123456789", "navnesøk")
    assert ii.finn_orgnr(FakeSess(sok={}), "UKJENT AS", None)[0] is None
    to_like = {"_embedded": {"enheter": [{"organisasjonsnummer": "1", "navn": "X AS"},
                                          {"organisasjonsnummer": "2", "navn": "X AS"}]}}
    assert ii.finn_orgnr(FakeSess(sok=to_like), "X AS", None)[0] is None


def test_roller_og_regnskap():
    s = FakeSess()
    r = ii.hent_roller(s, "123456789")
    assert r == {"daglig_leder": "Kari Nordmann", "styreleder": "Ola B Hansen"}   # fratrådt hoppes over
    g = ii.hent_regnskap(s, "123456789")
    assert g["aar"] == 2024                                                        # nyeste år velges
    assert g["omsetning"] == 900_000_000 and g["resultat_etter_skatt"] == 250_000_000
    assert g["egenkapital"] == 1_200_000_000 and g["sum_eiendeler"] == 1_500_000_000


def test_investor_info_selskap_og_cache():
    s = FakeSess()
    d = ii.investor_info("INV1", "SKAGEN AS", "Organisasjon", "NO", None, sess=s)
    assert d["type"] == "selskap" and d["orgnr"] == "123456789" and d["orgnr_kilde"] == "navnesøk"
    assert d["enhet"]["navn"] == "SKAGEN AS" and d["enhet"]["ansatte"] == 120
    assert d["roller"]["styreleder"] == "Ola B Hansen"
    assert d["regnskap"]["egenkapital"] == 1_200_000_000
    assert d["eiere"][0]["navn"] == "EIER AS"
    assert "proff.no" in d["lenker"]["proff"] and "123456789" in d["lenker"]["brreg"]
    n = len(s.kall)
    ii.investor_info("INV1", "SKAGEN AS", "Organisasjon", "NO", None, sess=s)
    assert len(s.kall) == n                                                        # fra cache


def test_person_og_utenlandsk_slaas_ikke_opp():
    s = FakeSess()
    p = ii.investor_info("P1", "OLA NORDMANN", "Privat", "NO", "1970", sess=s)
    assert p["type"] == "person" and not s.kall
    u = ii.investor_info("U1", "VANGUARD FUND", "Organisasjon", "US", None, sess=s)
    assert u["type"] == "utenlandsk" and not s.kall


def test_brreg_nede_gir_tomme_felter():
    class Nede(FakeSess):
        def get(self, url, params=None, timeout=None):
            raise ii.requests.ConnectionError("nede")
    d = ii.investor_info("INV2", "SKAGEN AS", "Organisasjon", "NO", "123456789", sess=Nede())
    assert d["orgnr"] == "123456789"
    assert d["enhet"] is None and d["roller"] == {} and d["regnskap"] is None
