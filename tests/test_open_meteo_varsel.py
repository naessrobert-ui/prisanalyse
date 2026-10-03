from scripts import open_meteo_varsel as om


def test_i_norge():
    assert om.i_norge(60.39, 5.32)        # Bergen
    assert om.i_norge(60.39, 5.93)        # Kvamskogen
    assert om.i_norge(69.73, 30.05)       # Kirkenes
    assert om.i_norge(78.22, 15.65)       # Longyearbyen
    assert not om.i_norge(38.35, -0.48)   # Alicante
    assert not om.i_norge(59.33, 18.07)   # Stockholm
    assert not om.i_norge(55.68, 12.57)   # København


def test_til_yr_timeserie_flere_modeller():
    payload = {"hourly": {
        "time": ["2026-10-03T12:00", "2026-10-03T13:00"],
        "temperature_2m_best_match": [27.1, None],
        "temperature_2m_ecmwf_ifs025": [26.5, 26.0],
        "precipitation_best_match": [0.4, 0.0],
        "precipitation_ecmwf_ifs025": [1.2, 0.0],
        "precipitation_icon_seamless": [0.0, 0.1],
        "wind_speed_10m_best_match": [3.0, 3.5],
        "weather_code_best_match": [61, 0],
        "is_day_best_match": [1, 1],
    }}
    ts = om.til_yr_timeserie(payload)
    assert len(ts) == 2
    a = ts[0]
    assert a["time"] == "2026-10-03T12:00:00Z"
    assert a["data"]["instant"]["details"]["air_temperature"] == 27.1
    d = a["data"]["next_1_hours"]["details"]
    assert d["precipitation_amount"] == 0.4
    assert d["precipitation_amount_min"] == 0.0
    assert d["precipitation_amount_max"] == 1.2
    assert a["data"]["next_1_hours"]["summary"]["symbol_code"] == "lightrain"
    # best_match mangler temp: faller tilbake på ECMWF
    assert ts[1]["data"]["instant"]["details"]["air_temperature"] == 26.0
    assert ts[1]["data"]["next_1_hours"]["summary"]["symbol_code"] == "clearsky_day"


def test_til_yr_timeserie_en_modell_uten_suffiks():
    payload = {"hourly": {"time": ["2026-10-03T12:00"], "temperature_2m": [20.0], "precipitation": [0.0]}}
    ts = om.til_yr_timeserie(payload)
    assert ts[0]["data"]["instant"]["details"]["air_temperature"] == 20.0
