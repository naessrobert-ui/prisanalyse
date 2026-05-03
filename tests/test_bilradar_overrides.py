import pandas as pd

from bilradar_overrides import apply_overrides


def _basis_df():
    return pd.DataFrame([
        {
            "FinnKode": 1,
            "Produsent": "Tesla",
            "Modell": "Model Y Long Range AWD",
            "drivstoff": "Elektrisk",
            "Info": "Premium interiør",
            "årstall": 2022,
            "salgspris": 400_000,
            "forventet_pris": 400_000.0,
        },
        {
            "FinnKode": 2,
            "Produsent": "Tesla",
            "Modell": "Model Y Standard Range",
            "drivstoff": "Elektrisk",
            "Info": "",
            "årstall": 2022,
            "salgspris": 350_000,
            "forventet_pris": 400_000.0,
        },
    ])


def test_variant_keyword_treffer_kun_riktig_rad():
    df = _basis_df()
    overrides = pd.DataFrame([{
        "match_type": "variant",
        "merke": "Tesla",
        "modell": "Model Y",
        "drivstoff": "Elektrisk",
        "år_fra": 2020,
        "år_til": 2024,
        "variant_keyword": "Long Range",
        "justering_type": "mult",
        "verdi": 1.18,
        "kommentar": "LR-tillegg",
    }])

    res = apply_overrides(df, overrides)

    assert res.loc[0, "overstyrt"] is True or res.loc[0, "overstyrt"] == True
    assert res.loc[0, "forventet_pris"] == 400_000 * 1.18
    assert res.loc[0, "forventet_pris_raa"] == 400_000.0
    assert res.loc[0, "overstyring_kommentar"] == "LR-tillegg"
    assert res.loc[1, "forventet_pris"] == 400_000.0
    assert res.loc[1, "overstyrt"] in (False, False)


def test_tom_overrides_endrer_ingenting():
    df = _basis_df()
    res = apply_overrides(df, pd.DataFrame())
    assert (res["forventet_pris"] == df["forventet_pris"]).all()
    assert (res["overstyrt"] == False).all()


def test_absolutt_overstyrer_pris():
    df = _basis_df()
    overrides = pd.DataFrame([{
        "match_type": "absolutt",
        "merke": "Tesla",
        "modell": "Model Y",
        "drivstoff": "Elektrisk",
        "år_fra": 2020,
        "år_til": 2024,
        "variant_keyword": "",
        "justering_type": "abs",
        "verdi": 333_000,
        "kommentar": "fast",
    }])
    res = apply_overrides(df, overrides)
    assert (res["forventet_pris"] == 333_000).all()
    assert (res["overstyrt"] == True).all()
