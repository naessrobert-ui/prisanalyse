"""Tester for ferskhetsfilteret bak /radar/siste ("Siste døgn").

Siden skal KUN vise biler som er nye det siste døgnet — dvs. første gang
observert (``Dato``) innen de siste ``RADAR_SISTE_TIMER`` timene. Tidligere
manglet dette filteret, så alle aktive annonser dukket opp (samme utvalg som
/radar/alle).
"""
from __future__ import annotations

import pandas as pd

from bil_routes import RADAR_SISTE_TIMER, _filtrer_ferske_biler


def _df(dato_verdier, finnkoder=None):
    if finnkoder is None:
        finnkoder = list(range(1, len(dato_verdier) + 1))
    return pd.DataFrame({"FinnKode": finnkoder, "Dato": dato_verdier})


def test_beholder_kun_biler_innen_vinduet():
    naa = pd.Timestamp("2026-08-08 12:00:00")
    df = _df(
        [
            naa - pd.Timedelta(hours=1),    # fersk
            naa - pd.Timedelta(hours=23),   # fersk (rett innenfor)
            naa - pd.Timedelta(hours=25),   # gammel (utenfor)
            naa - pd.Timedelta(days=30),    # gammel
        ],
        finnkoder=[10, 11, 12, 13],
    )
    ut = _filtrer_ferske_biler(df, timer=24, naa=naa)
    assert set(ut["FinnKode"]) == {10, 11}


def test_grensen_er_inklusiv():
    naa = pd.Timestamp("2026-08-08 12:00:00")
    df = _df([naa - pd.Timedelta(hours=24)], finnkoder=[99])
    ut = _filtrer_ferske_biler(df, timer=24, naa=naa)
    assert list(ut["FinnKode"]) == [99]


def test_dropper_rader_uten_gyldig_dato():
    naa = pd.Timestamp("2026-08-08 12:00:00")
    df = _df([naa - pd.Timedelta(hours=1), None, "ikke-en-dato"], finnkoder=[1, 2, 3])
    ut = _filtrer_ferske_biler(df, timer=24, naa=naa)
    assert list(ut["FinnKode"]) == [1]


def test_mangler_dato_kolonne_returnerer_uendret():
    df = pd.DataFrame({"FinnKode": [1, 2, 3]})
    ut = _filtrer_ferske_biler(df, timer=24)
    # Uten Dato-kolonne kan vi ikke filtrere — da beholder vi alt (med advarsel)
    # framfor å tømme siden.
    assert len(ut) == 3


def test_tom_df_er_trygt():
    df = pd.DataFrame(columns=["FinnKode", "Dato"])
    ut = _filtrer_ferske_biler(df, timer=24)
    assert ut.empty


def test_tz_bevisste_datoer_haandteres():
    naa = pd.Timestamp("2026-08-08 12:00:00")
    df = pd.DataFrame(
        {
            "FinnKode": [1, 2],
            "Dato": pd.to_datetime(
                [naa - pd.Timedelta(hours=1), naa - pd.Timedelta(hours=48)]
            ).tz_localize("Europe/Oslo"),
        }
    )
    ut = _filtrer_ferske_biler(df, timer=24, naa=naa)
    assert list(ut["FinnKode"]) == [1]


def test_standard_timer_er_24():
    assert RADAR_SISTE_TIMER == 24
