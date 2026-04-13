import pandas as pd

from bilradar_scorer import _normaliser_kjorelengde_for_modell, scorer_biler


class DummyModel:
    def predict(self, X):
        # Pris synker med alder og km
        return 500_000 - (X[:, 0] * 1_000) - (X[:, 1] * 2)


def test_normaliser_kjorelengde_for_eldre_bil():
    assert _normaliser_kjorelengde_for_modell(42, 19) == 168_000
    assert _normaliser_kjorelengde_for_modell(5, 19) == 19


def test_scorer_biler_bruker_km_gulv_for_eldre_biler():
    df = pd.DataFrame([
        {
            "FinnKode": 1,
            "Merke": "BMW",
            "Modell": "3-Serie",
            "Drivstoff": "Bensin",
            "Kjørelengde": 19,
            "Årstall": 1984,
            "Pris": 49_000,
            "Hjuldrift": "Tohjulstrekk",
        }
    ])

    modeller = {
        "nivaa_1": {
            "BMW | 3-Serie | Bensin": {
                "model": DummyModel(),
                "hjuldrift_map": {"Tohjulstrekk": 0},
            }
        },
        "nivaa_2": {},
        "generell": None,
    }

    result = scorer_biler(df, modeller)
    expected_price = 500_000 - (42 * 1_000) - (168_000 * 2)

    assert result.iloc[0]["forventet_pris"] == expected_price
