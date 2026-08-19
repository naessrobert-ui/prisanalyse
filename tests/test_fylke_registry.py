# -*- coding: utf-8 -*-
"""Tester for fylke_registry (ingen DB-avhengighet)."""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fylke_registry import (
    kommune_innbyggere,
    kommune_navn,
    list_fylker,
    nace_seksjon,
    nace_seksjon_intervall,
    resolve_fylke,
)


def test_resolve_vestland_via_nummer():
    fylke = resolve_fylke("46")
    assert fylke is not None
    assert fylke["navn"] == "Vestland"
    assert set(fylke["kommune_prefikser"]) == {"46", "12", "14"}


def test_resolve_vestland_via_navn_og_alias():
    assert resolve_fylke("Vestland")["nummer"] == "46"
    # Historiske fylker som ble slått sammen til Vestland
    assert resolve_fylke("Hordaland")["nummer"] == "46"
    assert resolve_fylke("Sogn og Fjordane")["nummer"] == "46"


def test_resolve_vestland_via_kommunenummer_prefiks():
    # Bergen = 4601, gammelt Hordaland-nummer 1201, Sogn og Fjordane 1401
    assert resolve_fylke("4601")["nummer"] == "46"
    assert resolve_fylke("1201")["nummer"] == "46"
    assert resolve_fylke("1401")["nummer"] == "46"


def test_legacy_prefixes_are_unambiguous():
    # Ingen to fylker skal dele samme kommune-prefiks
    seen: dict[str, str] = {}
    for fylke in list_fylker():
        info = resolve_fylke(fylke["nummer"])
        for prefiks in info["kommune_prefikser"]:
            assert prefiks not in seen, f"Prefiks {prefiks} deles av flere fylker"
            seen[prefiks] = fylke["nummer"]


def test_resolve_ukjent_returnerer_none():
    assert resolve_fylke("") is None
    assert resolve_fylke("tullball") is None
    assert resolve_fylke("99") is None


def test_list_fylker_sortert_og_komplett():
    fylker = list_fylker()
    numre = [f["nummer"] for f in fylker]
    assert numre == sorted(numre)
    assert "46" in numre
    assert len(numre) == len(set(numre))


def test_nace_seksjon_mapping():
    assert nace_seksjon("47") == ("G", "Varehandel; reparasjon av motorvogner")
    assert nace_seksjon("01") == ("A", "Jordbruk, skogbruk og fiske")
    assert nace_seksjon("10")[0] == "C"  # Industri
    assert nace_seksjon("68") == ("L", "Omsetning og drift av fast eiendom")
    assert nace_seksjon("99")[0] == "U"


def test_nace_seksjon_ukjent():
    assert nace_seksjon("")[0] == "?"
    assert nace_seksjon(None)[0] == "?"
    assert nace_seksjon("04")[0] == "?"  # ingen divisjon 04


def test_nace_seksjon_intervall():
    assert nace_seksjon_intervall("L") == ("Omsetning og drift av fast eiendom", 68, 68)
    assert nace_seksjon_intervall("G")[1:] == (45, 47)
    assert nace_seksjon_intervall("c")[1:] == (10, 33)  # store/små bokstaver
    assert nace_seksjon_intervall("Å") is None
    assert nace_seksjon_intervall("") is None


def test_kommune_navn_vestland():
    assert kommune_navn("4601") == "Bergen"
    assert kommune_navn(4651) == "Stryn"   # tåler int
    assert kommune_navn("301") == "Oslo"   # zero-pad til 0301
    assert kommune_navn("9999") is None


def test_kommune_innbyggere_mangler_gir_none():
    # Datafilen er valgfri; uten den skal oppslag gi None, ikke krasje.
    assert kommune_innbyggere("4601") is None or isinstance(kommune_innbyggere("4601"), int)
