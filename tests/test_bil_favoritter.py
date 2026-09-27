"""Popularitet for BilRadar: favoritter per dag og økning siste time/døgn."""
import io
import json

import numpy as np
import pandas as pd

import bil_favoritter as bf

NAA = pd.Timestamp("2026-09-28 18:50")


def hist(rader):
    return pd.DataFrame(rader, columns=["FinnKode", "tid", "favoritter"]).assign(
        tid=lambda d: pd.to_datetime(d["tid"]))


def aktive(rader):
    df = pd.DataFrame(rader, columns=["FinnKode", "Dato", "Favoritter_ny", "Favoritter_dato"])
    for c in ("Dato", "Favoritter_dato"):
        df[c] = pd.to_datetime(df[c])
    return df


def test_okning_siste_time_og_dogn():
    h = hist([
        (1, "2026-09-27 02:00", 10),   # dagsmåling i går
        (1, "2026-09-28 02:00", 14),
        (1, "2026-09-28 17:50", 20),   # timeskjøring for en time siden
        (1, "2026-09-28 18:50", 26),
    ])
    df = aktive([(1, "2026-09-20", 26, "2026-09-28 18:50")])
    assert bf.okning(df, h, NAA, 1).tolist() == [6]
    assert bf.okning(df, h, NAA, 24).tolist() == [16]   # 26 - 10 (verdi i går kl. 18:50)


def test_snapshot_noen_minutter_forskjovet_treffer_forrige_time():
    h = hist([(1, "2026-09-28 17:53", 20), (1, "2026-09-28 18:50", 26)])
    df = aktive([(1, "2026-09-20", 26, "2026-09-28 18:50")])
    assert bf.okning(df, h, NAA, 1).tolist() == [6]


def test_ny_annonse_starter_paa_null():
    h = hist([(2, "2026-09-28 18:50", 15)])
    df = aktive([(2, "2026-09-28 18:50", 15, "2026-09-28 18:50")])
    assert bf.okning(df, h, NAA, 1).tolist() == [15]
    assert bf.okning(df, h, NAA, 24).tolist() == [15]


def test_ikke_maalt_i_vinduet_gir_nan():
    """Eldre annonser måles bare om natten: ingen «siste time»-verdi."""
    h = hist([(3, "2026-09-28 02:00", 40)])
    df = aktive([(3, "2026-09-01", 40, "2026-09-28 02:00")])
    assert np.isnan(bf.okning(df, h, NAA, 1).iloc[0])
    # Men døgnet er ukjent også: historikken rekker ikke tilbake til i går.
    assert np.isnan(bf.okning(df, h, NAA, 24).iloc[0])


def test_uendret_tall_gir_null_okning():
    h = hist([(4, "2026-09-26 02:00", 7)])
    df = aktive([(4, "2026-09-01", 7, "2026-09-28 02:00")])
    assert bf.okning(df, h, NAA, 24).tolist() == [0]


def test_per_dag_minst_en_dag():
    df = aktive([(1, "2026-09-18 18:50", 50, NAA), (2, "2026-09-28 16:50", 9, NAA)])
    assert bf.per_dag(df, NAA).tolist() == [5.0, 9.0]


def test_berik_uten_favorittkolonner():
    df = pd.DataFrame({"FinnKode": [1], "Dato": [NAA]})
    bf.berik(df, hist([]))
    assert df[["fav_per_dag", "fav_1t", "fav_24t"]].isna().all().all()


def test_berik_bruker_siste_snapshot_som_naa():
    h = hist([(1, "2026-09-28 17:50", 20), (1, "2026-09-28 18:50", 26)])
    df = aktive([(1, "2026-09-20", 26, "2026-09-28 18:50")])
    bf.berik(df, h)
    assert df["fav_1t"].tolist() == [6]
    assert df["fav_per_dag"].iloc[0] == round(26 / (8 + 18.83 / 24), 1)


class FakeS3:
    def __init__(self, filer):
        self.filer = filer

    def get_object(self, Bucket, Key):
        if Key not in self.filer:
            raise KeyError(Key)
        return {"Body": io.BytesIO(self.filer[Key])}


def test_last_historikk_leser_to_maaneder():
    def pq(df):
        buf = io.BytesIO()
        df.to_parquet(buf, index=False)
        return buf.getvalue()
    s3 = FakeS3({
        "calc/bil/favoritt_historikk/2026-08.parquet": pq(hist([(1, "2026-08-31 02:00", 3)])),
        "calc/bil/favoritt_historikk/2026-09.parquet": pq(hist([(1, "2026-09-01 02:00", 5)])),
    })
    h = bf.last_historikk(s3, "b", pd.Timestamp("2026-09-01 10:00"))
    assert h["favoritter"].tolist() == [3, 5]
    assert bf.last_historikk(None, "").empty


def test_radar_json_tar_med_favoritter():
    from bil_routes import _lag_json_data_fra_parquet
    df = pd.DataFrame({
        "FinnKode": [1, 2], "Produsent": ["Tesla", "Kia"], "Pris_ny": [300000, 200000],
        "Favoritter_ny": [0.0, 12.0], "fav_per_dag": [0.0, 2.44],
        "fav_1t": [np.nan, 3.0], "fav_24t": [0.0, np.nan],
    })
    biler = json.loads(_lag_json_data_fra_parquet(df))
    assert biler[0]["fv"] == 0 and biler[0]["f24"] == 0 and "f1" not in biler[0]
    assert biler[1]["fv"] == 12 and biler[1]["fpd"] == 2.4 and biler[1]["f1"] == 3
