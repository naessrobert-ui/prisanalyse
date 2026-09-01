"""Tester for kupp-vakt sin merke-tiered elbil-terskel og graderte fylke-tillegg
(hjem / nabo / resten)."""
from __future__ import annotations

import pytest

import scripts.kupp_vakt as k


# ---------------------------------------------------------------- elbil-tier

def test_el_merke_terskel_tiers_og_alias():
    assert k._el_merke_terskel("Tesla") == k.EL_TERSKEL_LAV
    assert k._el_merke_terskel("kia") == k.EL_TERSKEL_LAV      # case-uavhengig
    assert k._el_merke_terskel("VW") == k.EL_TERSKEL_LAV       # alias -> Volkswagen
    assert k._el_merke_terskel("BMW") == k.EL_TERSKEL_MEDIUM
    assert k._el_merke_terskel("Volvo") == k.EL_TERSKEL_HOY
    assert k._el_merke_terskel("Mercedes") == k.EL_TERSKEL_HOY  # alias
    # Ukjent/nisje -> HOY-standard (var svakest i analysen)
    assert k._el_merke_terskel("Rivian") == k.EL_TERSKEL_DEFAULT


def test_basis_terskel_elbil_vs_trapp(monkeypatch):
    # Med tiering av: bruk pris-trappa uansett drivstoff
    monkeypatch.setattr(k, "EL_TIER_ON", False)
    row_el = {"salgspris": 300_000, "Drivstoff": "Elektrisk", "Merke": "Tesla"}
    assert k._basis_terskel(row_el) == k._min_rabatt_for_pris(300_000)

    # Med tiering paa: elbil bruker merke-terskel
    monkeypatch.setattr(k, "EL_TIER_ON", True)
    assert k._basis_terskel(row_el) == k.EL_TERSKEL_LAV
    # Bensinbil paavirkes ikke av tiering -> trappa
    row_bensin = {"salgspris": 300_000, "Drivstoff": "Bensin", "Merke": "Tesla"}
    assert k._basis_terskel(row_bensin) == k._min_rabatt_for_pris(300_000)


def test_basis_terskel_elbil_uten_drivstoffkolonne(monkeypatch):
    # Naar hele soeket er laast til elbil regnes alt som elbil selv om
    # Drivstoff-kolonnen mangler paa raden.
    monkeypatch.setattr(k, "EL_TIER_ON", True)
    monkeypatch.setattr(k, "DRIVSTOFF_FILTER", {"elektrisk"})
    row = {"salgspris": 300_000, "Merke": "Volvo"}  # ingen Drivstoff
    assert k._basis_terskel(row) == k.EL_TERSKEL_HOY


# ---------------------------------------------------------------- fylke-tillegg

def test_terskel_delta_gradert():
    hjem = {"100"}
    nabo = {"200"}
    assert k._terskel_delta({"FinnKode": "100"}, hjem, nabo) == 0.0
    assert k._terskel_delta({"FinnKode": "200"}, hjem, nabo) == k.NABO_TILLEGG_PP
    assert k._terskel_delta({"FinnKode": "300"}, hjem, nabo) == k.UTENFOR_TILLEGG_PP
    # Uten nabo-sett faller nabofylke tilbake til utenfor-tillegg
    assert k._terskel_delta({"FinnKode": "200"}, hjem, None) == k.UTENFOR_TILLEGG_PP
    # hjem=None -> ingen fylke-vekting
    assert k._terskel_delta({"FinnKode": "300"}, None, nabo) == 0.0


def test_terskel_delta_kurant(monkeypatch):
    monkeypatch.setattr(k, "KURANTE", ["volkswagen golf"])
    monkeypatch.setattr(k, "KURANT_LETTELSE_PP", 3.0)
    hjem = {"1"}
    row = {"FinnKode": "1", "Merke": "Volkswagen", "Modell": "Golf"}
    assert k._terskel_delta(row, hjem, None) == -3.0


# ---------------------------------------------------------------- _er_kupp

def test_er_kupp_bruker_merke_terskel(monkeypatch):
    monkeypatch.setattr(k, "EL_TIER_ON", True)
    monkeypatch.setattr(k, "DRIVSTOFF_FILTER", {"elektrisk"})
    # 3 % rabatt: kupp for Tesla (terskel 2), ikke for Volvo (terskel 12)
    tesla = {"forventet_pris": 309_000, "salgspris": 300_000,
             "rabatt_pct": 3.0, "Merke": "Tesla", "Drivstoff": "Elektrisk"}
    volvo = {"forventet_pris": 309_000, "salgspris": 300_000,
             "rabatt_pct": 3.0, "Merke": "Volvo", "Drivstoff": "Elektrisk"}
    assert k._er_kupp(tesla, 0.0) is True
    assert k._er_kupp(volvo, 0.0) is False
    # Utenfor hjemfylke (+10 pp): selv Tesla trenger 12 % -> 3 % holder ikke
    assert k._er_kupp(tesla, 10.0) is False


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main([__file__, "-v"]))
