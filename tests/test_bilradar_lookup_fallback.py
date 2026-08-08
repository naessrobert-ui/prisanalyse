"""Tester for synlig fallback i bilradar_lookup.

Når lookup-tabellen ikke kan hentes fra S3 (mangler credentials/boto3, eller S3
feiler) skal koden falle tilbake på den committede lokale fila – men logge det
tydelig, med filens alder, slik at en stille bruk av en utdatert tabell ikke går
ubemerket.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

import pandas as pd

from bilradar_lookup import (
    LOOKUP_COLUMNS,
    _les_lookup_fra_kilde,
    _lokal_fil_alder_dager,
)


def _skriv_lookup_csv(path):
    df = pd.DataFrame(
        [
            {
                "Produsent": "Tesla",
                "Modell": "Model 3",
                "variant_id": "ikke_klassifisert",
                "hjuldrift": "Bakhjulsdrift",
                "drivstoff": "Elektrisk",
                "årstall": 2022,
                "n_obs": 12,
                "median_pris": 300000,
                "median_km": 40000,
                "hurtigpris": 280000,
                "innbyttepris": 255000,
                "km_slope_pct": -0.000002,
            }
        ],
        columns=LOOKUP_COLUMNS,
    )
    df.to_csv(path, index=False)


def test_alder_paa_fersk_fil_er_omtrent_null(tmp_path):
    p = tmp_path / "prislookup.csv"
    p.write_text("x")
    alder = _lokal_fil_alder_dager(str(p))
    assert alder is not None
    assert alder < 1.0


def test_alder_paa_manglende_fil_er_none(tmp_path):
    assert _lokal_fil_alder_dager(str(tmp_path / "finnes_ikke.csv")) is None


def test_alder_respekterer_mtime(tmp_path):
    p = tmp_path / "gammel.csv"
    p.write_text("x")
    ti_dager_siden = (datetime.now() - timedelta(days=10)).timestamp()
    os.utime(p, (ti_dager_siden, ti_dager_siden))
    alder = _lokal_fil_alder_dager(str(p))
    assert 9.5 < alder < 10.5


def test_fallback_uten_s3_leser_lokal_fil_og_advarer(tmp_path, capsys):
    p = tmp_path / "prislookup.csv"
    _skriv_lookup_csv(p)
    # Ingen s3_client og ingen bucket -> S3 forsøkes aldri
    source = {"s3_client": None, "bucket": "", "key": "", "local_path": str(p)}
    df = _les_lookup_fra_kilde(source)
    assert len(df) == 1
    ut = capsys.readouterr().out
    assert "ADVARSEL" in ut  # S3 ikke tilgjengelig
    assert "lokal" in ut.lower()


def test_fallback_paa_utdatert_fil_gir_utdatert_advarsel(tmp_path, capsys):
    p = tmp_path / "prislookup.csv"
    _skriv_lookup_csv(p)
    gammelt = (datetime.now() - timedelta(days=30)).timestamp()
    os.utime(p, (gammelt, gammelt))
    source = {"s3_client": None, "bucket": "", "key": "", "local_path": str(p)}
    _les_lookup_fra_kilde(source)
    ut = capsys.readouterr().out
    assert "utdatert" in ut.lower()
    assert "30 dager" in ut


def test_manglende_lokal_fil_gir_tom_df_og_advarsel(tmp_path, capsys):
    source = {
        "s3_client": None,
        "bucket": "",
        "key": "",
        "local_path": str(tmp_path / "finnes_ikke.csv"),
    }
    df = _les_lookup_fra_kilde(source)
    assert df.empty
    ut = capsys.readouterr().out
    assert "ADVARSEL" in ut
