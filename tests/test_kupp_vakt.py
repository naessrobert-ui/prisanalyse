"""Tester for kupp-vakt sine filtre: trappetrinns-rabatt, drivstoff, sted, fylke."""
from __future__ import annotations

import scripts.kupp_vakt as k


# ---------------------------------------------------------------------------
# Trappetrinns-rabatt
# ---------------------------------------------------------------------------
def test_parse_trapp_sorterer_og_tolker_uendelig():
    bands = k._parse_trapp("100000:20,50000:30,:7")
    assert bands == [(50000.0, 30.0), (100000.0, 20.0), (float("inf"), 7.0)]


def test_parse_trapp_ignorerer_soppel():
    assert k._parse_trapp("") == []
    assert k._parse_trapp("bare_tull,50000:x,:9") == [(float("inf"), 9.0)]


def test_min_rabatt_for_pris_grenser():
    b = k._parse_trapp("50000:30,100000:20,150000:15,250000:10,:7")
    assert k._min_rabatt_for_pris(30_000, b) == 30
    assert k._min_rabatt_for_pris(49_999, b) == 30
    assert k._min_rabatt_for_pris(50_000, b) == 20   # grensa hører til neste bånd
    assert k._min_rabatt_for_pris(120_000, b) == 15
    assert k._min_rabatt_for_pris(200_000, b) == 10
    assert k._min_rabatt_for_pris(250_000, b) == 7
    assert k._min_rabatt_for_pris(2_000_000, b) == 7


# ---------------------------------------------------------------------------
# _er_kupp – trappa i praksis
# ---------------------------------------------------------------------------
def _rad(pris, rabatt_pct, forventet=None):
    forventet = forventet if forventet is not None else pris / (1 - rabatt_pct / 100)
    return {
        "forventet_pris": forventet,
        "salgspris": pris,
        "rabatt_pct": rabatt_pct,
        "rabatt_kr": forventet - pris,
        "hurtigpris": None,
    }


def test_er_kupp_billig_bil_krever_hoy_prosent(monkeypatch):
    monkeypatch.setattr(k, "RABATT_TRAPP",
                        k._parse_trapp("50000:30,100000:20,150000:15,250000:10,:7"))
    monkeypatch.setattr(k, "RABATT_KR_MIN", 0.0)
    monkeypatch.setattr(k, "UNDER_HURTIG", False)
    assert k._er_kupp(_rad(40_000, 25)) is False   # under 30 %
    assert k._er_kupp(_rad(40_000, 32)) is True


def test_er_kupp_dyr_bil_lav_prosent_holder(monkeypatch):
    monkeypatch.setattr(k, "RABATT_TRAPP",
                        k._parse_trapp("50000:30,100000:20,150000:15,250000:10,:7"))
    monkeypatch.setattr(k, "RABATT_KR_MIN", 0.0)
    monkeypatch.setattr(k, "UNDER_HURTIG", False)
    assert k._er_kupp(_rad(300_000, 6)) is False
    assert k._er_kupp(_rad(300_000, 8)) is True
    assert k._er_kupp(_rad(120_000, 16)) is True
    assert k._er_kupp(_rad(120_000, 14)) is False


def test_er_kupp_uten_forventet_er_ikke_kupp(monkeypatch):
    monkeypatch.setattr(k, "UNDER_HURTIG", False)
    rad = _rad(100_000, 50)
    rad["forventet_pris"] = None
    assert k._er_kupp(rad) is False


# ---------------------------------------------------------------------------
# Drivstoff- og sted-filter
# ---------------------------------------------------------------------------
def test_match_filtre_drivstoff(monkeypatch):
    monkeypatch.setattr(k, "DRIVSTOFF_FILTER", {"elektrisk"})
    monkeypatch.setattr(k, "STED_FILTER", [])
    assert k._match_filtre({"Drivstoff": "El", "sted": "Oslo"}) is True
    assert k._match_filtre({"Drivstoff": "Elektrisk", "sted": "Oslo"}) is True
    assert k._match_filtre({"Drivstoff": "Diesel", "sted": "Oslo"}) is False
    assert k._match_filtre({"Drivstoff": "", "sted": "Oslo"}) is False  # ukjent droppes


def test_match_filtre_sted(monkeypatch):
    monkeypatch.setattr(k, "DRIVSTOFF_FILTER", set())
    monkeypatch.setattr(k, "STED_FILTER", ["bergen", "voss"])
    assert k._match_filtre({"Drivstoff": "El", "sted": "Bergen ∙ Bilhus AS"}) is True
    assert k._match_filtre({"Drivstoff": "El", "sted": "Voss"}) is True
    assert k._match_filtre({"Drivstoff": "El", "sted": "Oslo"}) is False


def test_match_filtre_tomt_slipper_alt(monkeypatch):
    monkeypatch.setattr(k, "DRIVSTOFF_FILTER", set())
    monkeypatch.setattr(k, "STED_FILTER", [])
    assert k._match_filtre({"Drivstoff": "Diesel", "sted": "Kirkenes"}) is True


# ---------------------------------------------------------------------------
# Fylke -> FINN location-kode
# ---------------------------------------------------------------------------
def test_location_codes_navn_og_rakode(monkeypatch):
    monkeypatch.setenv("KUPP_FYLKE", "Vestland, Rogaland, 0.20061")
    assert k._location_codes() == ["0.22046", "0.20012", "0.20061"]


def test_location_codes_ukjent_hoppes_over(monkeypatch):
    monkeypatch.setenv("KUPP_FYLKE", "Vestland, Tulleland")
    assert k._location_codes() == ["0.22046"]


def test_selger_segment_bygges_i_url(monkeypatch):
    # privat -> dealer_segment=3 skal ligge i søke-URL
    assert k.SELGER_SEGMENT["privat"] == "3"
    monkeypatch.setattr(k, "SELGER_SUFFIX", "&dealer_segment=3")
    assert "dealer_segment=3" in k._build_url(1, "3")


def test_formater_bil_viser_sted_uansett_kolonnenavn():
    # Scoreren døper om "sted" -> "Sted"; formatereren må vise begge.
    rad_raa = {"Merke": "Kia", "Modell": "EV6", "Årstall": 2023, "Kjørelengde": 20000,
               "Pris": 400000, "forventet_pris": 450000, "rabatt_pct": 11.1,
               "hurtigpris": 440000, "innbyttepris": 380000, "sted": "Bergen",
               "url": "u"}
    rad_scoret = dict(rad_raa)
    rad_scoret.pop("sted")
    rad_scoret["Sted"] = "Bergen"
    assert "Bergen" in k._formater_bil(rad_raa)
    assert "Bergen" in k._formater_bil(rad_scoret)
    assert "Bergen" in k._pushover_melding([rad_scoret])


def test_alle_femten_fylker_har_kode():
    # 15 fylker (2024) + ascii-aliaser skal alle finnes
    for navn in ["Østfold", "Akershus", "Oslo", "Innlandet", "Buskerud",
                 "Vestfold", "Telemark", "Agder", "Rogaland", "Vestland",
                 "Møre og Romsdal", "Trøndelag", "Nordland", "Troms", "Finnmark"]:
        assert navn.lower() in k.FYLKE_LOCATION
