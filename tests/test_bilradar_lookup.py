import io

import numpy as np
import pandas as pd

from bilradar_lookup import apply_lookup, last_lookup, reload_lookup, _les_csv, _normaliser_hjuldrift


_NY_SKJEMA_HEADER = (
    "Produsent,Modell,variant_id,hjuldrift,drivstoff,årstall,"
    "n_obs,median_pris,median_km,hurtigpris,innbyttepris,km_slope_pct\n"
)


class _FakeS3:
    """Minimal S3-klient for last_lookup-testene."""

    def __init__(self, csv_bytes: bytes | None = None, raise_exc: bool = False):
        self._csv = csv_bytes
        self._raise = raise_exc

    def get_object(self, Bucket, Key):  # noqa: N803 (matcher boto3-signaturen)
        if self._raise:
            raise RuntimeError("NoSuchKey")
        return {"Body": io.BytesIO(self._csv)}


def test_last_lookup_foretrekker_s3():
    reload_lookup()
    try:
        csv = (_NY_SKJEMA_HEADER +
               "Tesla,Model Y,ikke_klassifisert,Firehjul,Elektrisk,2022,10,350000,60000,320000,297500,-2e-6\n"
               ).encode("utf-8")
        fake = _FakeS3(csv_bytes=csv)
        df = last_lookup(local_path="/finnes/ikke.csv", s3_client=fake, bucket="b", key="k")
        assert len(df) == 1
        assert int(df.iloc[0]["median_pris"]) == 350_000
    finally:
        reload_lookup()


def test_last_lookup_faller_tilbake_til_lokal_ved_s3_feil(tmp_path):
    reload_lookup()
    try:
        p = tmp_path / "lu.csv"
        p.write_text(
            _NY_SKJEMA_HEADER +
            "Kia,EV6,ikke_klassifisert,Firehjul,Elektrisk,2022,10,400000,50000,380000,340000,-2e-6\n",
            encoding="utf-8",
        )
        fake = _FakeS3(raise_exc=True)  # S3 feiler -> skal bruke lokal fil
        df = last_lookup(local_path=str(p), s3_client=fake, bucket="b", key="k")
        assert len(df) == 1
        assert df.iloc[0]["Modell"] == "EV6"
    finally:
        reload_lookup()


def _basis_df():
    return pd.DataFrame([
        {
            "FinnKode": 1,
            "Produsent": "Tesla",
            "Modell": "Model Y",
            "variant_id": "ikke_klassifisert",
            "drivstoff": "Elektrisk",
            "hjuldrift": "Firehjul",
            "årstall": 2022,
            "kjørelengde": 50_000,
            "salgspris": 380_000,
            "forventet_pris": 410_000.0,  # ML-fallback
            "peer_konfidens": 200,
            "modell_nivaa": "L1",
        },
        {
            "FinnKode": 2,
            "Produsent": "Ukjent-Merke",
            "Modell": "Sjelden",
            "variant_id": "ikke_klassifisert",
            "drivstoff": "Elektrisk",
            "hjuldrift": "Tohjul",
            "årstall": 2022,
            "kjørelengde": 20_000,
            "salgspris": 200_000,
            "forventet_pris": 250_000.0,  # ML-fallback
            "peer_konfidens": 50,
            "modell_nivaa": "GEN",
        },
    ])


def _lookup():
    return pd.DataFrame([{
        "Produsent": "Tesla",
        "Modell": "Model Y",
        "variant_id": "ikke_klassifisert",
        "hjuldrift": "Firehjul",
        "drivstoff": "Elektrisk",
        "årstall": 2022,
        "n_obs": 1382,
        "median_pris": 350_000.0,
        "median_km": 60_000.0,
        "hurtigpris": 320_000.0,
        "innbyttepris": 297_500.0,
        "km_slope_pct": -2e-6,
    }])


def _forventet(median_pris, median_km, pct, km):
    return median_pris * np.exp(pct * (km - median_km))


def test_lookup_overskriver_ml_med_prosentvis_km_justering():
    df = _basis_df()
    res = apply_lookup(df, _lookup())

    # Bil 1: km 50k < median 60k -> lavere km skal gi hoyere pris (negativ slope).
    forv = _forventet(350_000, 60_000, -2e-6, 50_000)
    assert np.isclose(res.loc[0, "forventet_pris"], forv)
    assert res.loc[0, "forventet_pris"] > 350_000  # under median-km => over median-pris
    assert res.loc[0, "modell_nivaa"] == "LOOKUP"
    assert res.loc[0, "peer_konfidens"] == 1382


def test_lookup_setter_hurtig_og_innbyttepris():
    df = _basis_df()
    res = apply_lookup(df, _lookup())

    forv = _forventet(350_000, 60_000, -2e-6, 50_000)
    hurtig = _forventet(320_000, 60_000, -2e-6, 50_000)  # samme km-faktor
    assert np.isclose(res.loc[0, "hurtigpris"], hurtig)
    # Innbytte = 15 % under forventet, gulvet mot hurtig. 0.85*forventet < hurtig her.
    assert np.isclose(res.loc[0, "innbyttepris"], forv * 0.85)
    assert res.loc[0, "innbyttepris"] < res.loc[0, "forventet_pris"]


def test_lookup_ignorerer_forhandseksisterende_verdikolonner():
    """Regresjon: når df alt har (typisk NaN) hurtigpris/innbyttepris-kolonner
    — som scorer_biler initialiserer — skal apply_lookup fortsatt bruke
    lookup-verdiene, ikke de innkommende NaN-kolonnene (merge-suffiks-fella)."""
    df = _basis_df()
    df["hurtigpris"] = np.nan       # forhåndsinitialisert av kalleren
    df["innbyttepris"] = np.nan
    df["median_pris"] = np.nan      # skal heller ikke lekke inn
    res = apply_lookup(df, _lookup())
    hurtig = _forventet(320_000, 60_000, -2e-6, 50_000)
    assert np.isclose(res.loc[0, "hurtigpris"], hurtig)
    assert res.loc[0, "hurtigpris"] > 0
    assert pd.notna(res.loc[0, "innbyttepris"])


def test_lookup_lar_ikke_matchende_biler_uendret():
    df = _basis_df()
    res = apply_lookup(df, _lookup())

    # Bil 2 har ingen lookup-treff -> beholder ML-pris
    assert res.loc[1, "forventet_pris"] == 250_000.0
    assert res.loc[1, "modell_nivaa"] == "GEN"
    assert res.loc[1, "peer_konfidens"] == 50


def test_tom_lookup_endrer_ingenting():
    df = _basis_df()
    res = apply_lookup(df, pd.DataFrame())
    assert (res["forventet_pris"] == df["forventet_pris"]).all()
    assert (res["modell_nivaa"] == df["modell_nivaa"]).all()


def test_kmjustering_med_negativ_slope_oker_pris_for_lavere_km():
    df = _basis_df()
    df.loc[0, "kjørelengde"] = 30_000  # godt under median 60k
    res = apply_lookup(df, _lookup())
    forv = _forventet(350_000, 60_000, -2e-6, 30_000)
    assert np.isclose(res.loc[0, "forventet_pris"], forv)


def test_lookup_clip_til_minst_1000():
    df = _basis_df()
    df.loc[0, "kjørelengde"] = 10_000_000  # urealistisk hoy km
    res = apply_lookup(df, _lookup())
    # Skal ikke gaa under 1000 NOK uansett
    assert res.loc[0, "forventet_pris"] >= 1_000.0
    assert res.loc[0, "innbyttepris"] >= 1_000.0


def test_variant_id_ruter_til_riktig_batteripakke():
    """To Kona-varianter (39 vs 64 kWh) med ulik pris. Bilen med eksplisitt
    variant_id skal matche riktig rad og ikke blande batteripakkene."""
    df = pd.DataFrame([{
        "FinnKode": 7,
        "Produsent": "Hyundai",
        "Modell": "Kona",
        "variant_id": "hyundai-kona-64",  # stort batteri
        "drivstoff": "Elektrisk",
        "hjuldrift": "Tohjul",
        "årstall": 2021,
        "kjørelengde": 40_000,
        "salgspris": 250_000,
        "forventet_pris": 999_999.0,
        "peer_konfidens": 1,
        "modell_nivaa": "GEN",
    }])
    lookup = pd.DataFrame([
        {
            "Produsent": "Hyundai", "Modell": "Kona", "variant_id": "hyundai-kona-39",
            "hjuldrift": "Tohjul", "drivstoff": "Elektrisk", "årstall": 2021,
            "n_obs": 40, "median_pris": 200_000.0, "median_km": 40_000.0,
            "hurtigpris": 190_000.0, "innbyttepris": 170_000.0, "km_slope_pct": -3e-6,
        },
        {
            "Produsent": "Hyundai", "Modell": "Kona", "variant_id": "hyundai-kona-64",
            "hjuldrift": "Tohjul", "drivstoff": "Elektrisk", "årstall": 2021,
            "n_obs": 60, "median_pris": 260_000.0, "median_km": 40_000.0,
            "hurtigpris": 245_000.0, "innbyttepris": 221_000.0, "km_slope_pct": -3e-6,
        },
    ])
    res = apply_lookup(df, lookup)
    # km == median -> ingen justering, skal treffe 64-raden (260k), ikke 39 (200k)
    assert res.loc[0, "forventet_pris"] == 260_000.0
    assert res.loc[0, "peer_konfidens"] == 60


def test_les_csv_bakoverkompatibel_med_gammelt_skjema():
    """Gammel CSV uten variant_id/km_slope_pct/hurtigpris/innbyttepris skal
    fortsatt lastes og degradere til fornuftige defaults."""
    gammel = (
        "Produsent,Modell,hjuldrift,drivstoff,årstall,n_obs,median_pris,median_km,"
        "p25_pris,p75_pris,std_pris,km_slope,km_slope_kilde\n"
        "Tesla,Model 3,Firehjul,Elektrisk,2022,100,300000,50000,270000,330000,20000,-0.7,lokal\n"
    )
    df = _les_csv(io.StringIO(gammel))
    rad = df.iloc[0]
    assert rad["variant_id"] == "ikke_klassifisert"
    # km_slope_pct utledet fra lineaer slope / median_pris
    assert np.isclose(rad["km_slope_pct"], -0.7 / 300000)
    assert rad["hurtigpris"] == 270_000  # fra p25_pris
    assert np.isclose(rad["innbyttepris"], 300_000 * 0.85)


def test_normaliser_hjuldrift_alle_varianter():
    # FINN-detalj-formater
    assert _normaliser_hjuldrift("Forhjulsdrift") == "Tohjul"
    assert _normaliser_hjuldrift("Bakhjulsdrift") == "Tohjul"
    assert _normaliser_hjuldrift("Firehjulsdrift") == "Firehjul"
    # Database-formater
    assert _normaliser_hjuldrift("Tohjul") == "Tohjul"
    assert _normaliser_hjuldrift("Firehjul") == "Firehjul"
    # Engelske/case-varianter
    assert _normaliser_hjuldrift("FWD") == "Tohjul"
    assert _normaliser_hjuldrift("AWD") == "Firehjul"
    assert _normaliser_hjuldrift("4x4") == "Firehjul"
    # Tom/manglende
    assert _normaliser_hjuldrift(None) == "Ukjent"
    assert _normaliser_hjuldrift("") == "Ukjent"
    assert _normaliser_hjuldrift("Ukjent") == "Ukjent"


def test_lookup_matcher_pa_tvers_av_hjuldrift_format():
    """En FINN-bil med hjuldrift='Forhjulsdrift' skal matche en lookup-rad
    med hjuldrift='Tohjul'. Var hovedgrunnen til at lookup ikke virket
    paa NY-IKKE-I-DB-biler."""
    df = pd.DataFrame([{
        "FinnKode": 99,
        "Produsent": "Mazda",
        "Modell": "MX-30",
        "variant_id": "ikke_klassifisert",
        "drivstoff": "Elektrisk",
        "hjuldrift": "Forhjulsdrift",  # FINN-format
        "årstall": 2022,
        "kjørelengde": 33_000,
        "salgspris": 169_900,
        "forventet_pris": 290_000.0,  # ML-fallback
        "peer_konfidens": 100,
        "modell_nivaa": "L1",
    }])
    lookup = pd.DataFrame([{
        "Produsent": "Mazda",
        "Modell": "MX-30",
        "variant_id": "ikke_klassifisert",
        "hjuldrift": "Tohjul",  # database-format
        "drivstoff": "Elektrisk",
        "årstall": 2022,
        "n_obs": 282,
        "median_pris": 169_000.0,
        "median_km": 33_000.0,
        "hurtigpris": 160_000.0,
        "innbyttepris": 143_650.0,
        "km_slope_pct": -3e-6,
    }])
    res = apply_lookup(df, lookup)
    assert res.loc[0, "modell_nivaa"] == "LOOKUP"
    assert res.loc[0, "forventet_pris"] == 169_000  # km == median, ingen justering
