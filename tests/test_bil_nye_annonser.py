"""Tester for bil_nye_annonser - telling og hull-estimering.

Dekker de rene funksjonene (ingen S3): selgertype-klassifisering, telling av
nye annonser per dag, hull-deteksjon og jevn fordeling over innsamlingshull.
"""
from __future__ import annotations

from datetime import date

import pandas as pd

from bil_nye_annonser import (
    bygg_serie_med_estimat,
    finn_manglende_dager,
    klassifiser_selgertype,
    oppsummer_serie,
    tell_nye_per_dag,
)


# ---- klassifiser_selgertype (basert paa Selger-kolonnen) ----

def test_klassifiser_selgertype_tom_er_privat_verdi_er_bedrift():
    s = pd.Series(["", None, "   ", "Bilforhandler AS", "Bertel O. Steen", "nan"])
    ut = klassifiser_selgertype(s).tolist()
    # tom / NaN / whitespace / "nan" -> privat; ekte navn -> bedrift
    assert ut == ["privat", "privat", "privat", "bedrift", "bedrift", "privat"]


# ---- tell_nye_per_dag ----

def test_tell_nye_per_dag_grupperer_og_splitter():
    df = pd.DataFrame(
        {
            "FinnKode": ["1", "2", "3", "4", "5"],
            "Dato": [
                "2026-01-01 08:00",
                "2026-01-01 20:00",
                "2026-01-02 09:00",
                "2026-01-02 10:00",
                "2026-01-02 11:00",
            ],
            # tom Selger = privat, navn = bedrift
            "Selger": ["", "Bilhuset AS", "", "", "Auto AS"],
        }
    )
    ut = tell_nye_per_dag(df)
    assert list(ut.index) == [date(2026, 1, 1), date(2026, 1, 2)]
    assert ut.loc[date(2026, 1, 1), "privat"] == 1
    assert ut.loc[date(2026, 1, 1), "bedrift"] == 1
    assert ut.loc[date(2026, 1, 1), "total"] == 2
    assert ut.loc[date(2026, 1, 2), "privat"] == 2
    assert ut.loc[date(2026, 1, 2), "bedrift"] == 1
    assert ut.loc[date(2026, 1, 2), "total"] == 3


def test_tell_nye_per_dag_dropper_ugyldig_dato():
    df = pd.DataFrame(
        {"Dato": ["2026-01-01", "tull", None], "Selger": ["", "", ""]}
    )
    ut = tell_nye_per_dag(df)
    assert ut["total"].sum() == 1


def test_tell_nye_per_dag_tom():
    assert tell_nye_per_dag(pd.DataFrame()).empty


# ---- finn_manglende_dager ----

def test_finn_manglende_dager():
    dekket = [date(2026, 1, 1), date(2026, 1, 2), date(2026, 1, 5)]
    assert finn_manglende_dager(dekket) == [date(2026, 1, 3), date(2026, 1, 4)]


def test_finn_manglende_dager_ingen_hull():
    dekket = [date(2026, 1, 1), date(2026, 1, 2), date(2026, 1, 3)]
    assert finn_manglende_dager(dekket) == []


# ---- bygg_serie_med_estimat ----

def _counts(rader: dict[date, tuple[int, int]]) -> pd.DataFrame:
    """Hjelper: {dato: (privat, bedrift)} -> DataFrame slik tell_nye_per_dag gir."""
    idx = sorted(rader)
    data = {
        "privat": [rader[d][0] for d in idx],
        "bedrift": [rader[d][1] for d in idx],
    }
    df = pd.DataFrame(data, index=idx)
    df["total"] = df[["privat", "bedrift"]].sum(axis=1)
    return df


def test_estimat_uten_hull_beholder_heltall():
    counts = _counts({date(2026, 1, 1): (5, 3), date(2026, 1, 2): (2, 4)})
    dekket = [date(2026, 1, 1), date(2026, 1, 2)]
    serie = bygg_serie_med_estimat(counts, dekket)
    assert len(serie) == 2
    assert all(not d["estimert"] for d in serie)
    assert serie[0]["privat"] == 5 and serie[0]["bedrift"] == 3
    assert serie[1]["total"] == 6


def test_estimat_fordeler_hull_jevnt():
    # 5. dekket. 6. + 7. mangler (gap=2). 8. dekket med 40 nye (12 priv / 28 bedr).
    # Antallet paa dagen etter hullet representerer nye annonser opphopet over
    # hele hullet + selve dagen = 3 dager, saa 40 fordeles jevnt over 6.-8.
    counts = _counts({date(2026, 1, 5): (10, 10), date(2026, 1, 8): (12, 28)})
    dekket = [date(2026, 1, 5), date(2026, 1, 8)]
    serie = bygg_serie_med_estimat(counts, dekket, maks_gap=14)

    datoer = [d["dato"] for d in serie]
    assert datoer == ["2026-01-05", "2026-01-06", "2026-01-07", "2026-01-08"]

    # Foerste dag: observert, uendret
    assert serie[0]["estimert"] is False
    assert serie[0]["total"] == 20

    # De tre paafoelgende (hull + dagen etter) skal dele 40 jevnt = 13.33 hver
    for d in serie[1:]:
        assert d["estimert"] is True
        assert d["usikker"] is False
        assert d["gap_lengde"] == 2
        assert d["total"] == round(40 / 3, 2)
        assert d["privat"] == round(12 / 3, 2)
        assert d["bedrift"] == round(28 / 3, 2)

    # Summen over de estimerte dagene skal bevare totalen fra 8. (40),
    # innenfor avrundingsstoey (verdiene rundes til 2 desimaler per dag).
    assert abs(sum(d["total"] for d in serie[1:]) - 40.0) < 0.05


def test_estimat_markerer_lange_hull_usikre():
    counts = _counts({date(2026, 1, 1): (5, 5), date(2026, 1, 20): (10, 10)})
    dekket = [date(2026, 1, 1), date(2026, 1, 20)]
    serie = bygg_serie_med_estimat(counts, dekket, maks_gap=7)
    estimerte = [d for d in serie if d["estimert"]]
    assert estimerte and all(d["usikker"] for d in estimerte)
    assert all(d["gap_lengde"] == 18 for d in estimerte)


def test_oppsummering():
    counts = _counts({date(2026, 1, 1): (6, 4), date(2026, 1, 2): (2, 8)})
    serie = bygg_serie_med_estimat(counts, [date(2026, 1, 1), date(2026, 1, 2)])
    opp = oppsummer_serie(serie)
    assert opp["antall_dager"] == 2
    assert opp["sum_total"] == 20
    assert opp["sum_privat"] == 8
    assert opp["andel_privat_pct"] == 40.0  # 8 / 20
