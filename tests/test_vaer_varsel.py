from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from flask import Flask

from scripts import vaer_varsel as vv

# Søndag 27.9.2026 kl. 08:19 lokal tid (UTC+2).
NOW = datetime(2026, 9, 27, 6, 19, tzinfo=timezone.utc)
START = datetime(2026, 9, 27, 6, 0, tzinfo=timezone.utc)


def yr_payload(rain_by_hour=None, hours=60, six_hour_days=4, temp=12.0, wind=5.0, symbol="cloudy"):
    """Timesvarsel i `hours` timer fra START, deretter seks-timersblokker."""
    rain_by_hour = rain_by_hour or {}
    series = []
    for i in range(hours):
        t = START + timedelta(hours=i)
        rain = rain_by_hour.get(i, 0.0)
        series.append({"time": t.isoformat().replace("+00:00", "Z"), "data": {
            "instant": {"details": {"air_temperature": temp + (i % 24) / 10, "wind_speed": wind,
                                    "wind_from_direction": 150}},
            "next_1_hours": {"summary": {"symbol_code": "rain" if rain >= 0.1 else symbol},
                             "details": {"precipitation_amount": rain, "precipitation_amount_max": rain * 2,
                                         "probability_of_precipitation": 80 if rain else 0}},
            "next_6_hours": {"summary": {"symbol_code": symbol},
                             "details": {"precipitation_amount": 99}},  # skal ignoreres når timer finnes
        }})
    t = START + timedelta(hours=hours)
    t = t.replace(hour=(t.hour // 6 + 1) * 6 % 24) + (timedelta(days=1) if t.hour >= 18 else timedelta(0))
    for _ in range(six_hour_days * 4):
        series.append({"time": t.isoformat().replace("+00:00", "Z"), "data": {
            "instant": {"details": {"air_temperature": 10.0, "wind_speed": 3.0}},
            "next_6_hours": {"summary": {"symbol_code": "partlycloudy_day"},
                             "details": {"precipitation_amount": 1.5, "air_temperature_max": 14,
                                         "air_temperature_min": 8}},
        }})
        t += timedelta(hours=6)
    return {"properties": {"meta": {"updated_at": "2026-09-27T04:30:44Z"}, "timeseries": series}}


def google(rain_by_hour=None, temp_offset=-1.0, hours=48):
    rain_by_hour = rain_by_hour or {}
    return [{"start": (START + timedelta(hours=i)).isoformat().replace("+00:00", "Z"),
             "end": (START + timedelta(hours=i + 1)).isoformat().replace("+00:00", "Z"),
             "temp": 12.0 + (i % 24) / 10 + temp_offset, "rain": rain_by_hour.get(i, 0.0)}
            for i in range(hours)]


def test_slices_use_hourly_data_and_do_not_double_count_six_hour_blocks():
    rows = vv.parse_yr(yr_payload({0: 1.0}))
    parts = vv.slices(rows)
    assert parts[0]["rain"] == 1.0  # next_1_hours, ikke next_6_hours sine 99 mm
    assert all(parts[i]["end"] <= parts[i + 1]["start"] for i in range(len(parts) - 1))
    assert sum(p["end"] - p["start"] == timedelta(hours=6) for p in parts) > 0


def test_hours_align_google_by_utc_hour_and_flag_disagreement():
    rows = vv.parse_yr(yr_payload({3: 2.0}))
    hours = vv.build_hours(rows, google({3: 0.2}), START)
    assert len(hours) == 25
    assert hours[0]["hour"] == 8  # lokal tid
    assert hours[3]["rain"] == 2.0 and hours[3]["g_rain"] == 0.2
    assert hours[3]["disagree"] is True
    assert hours[4]["disagree"] is False


def test_rest_of_day_reports_dry_until_first_rain_and_google_drizzle():
    # Lokal kl. 23 = indeks 15.
    rows = vv.parse_yr(yr_payload({15: 0.3}))
    g = google({6: 0.04, 7: 0.04, 13: 0.07, 14: 0.19, 15: 0.15})
    result = vv.rest_of_day(rows, g, NOW)
    assert result["head"] == "Tørt til kl. 23"
    assert result["wet"] is False
    assert "Yr: 0,3 mm fra kl. 23." in result["text"]
    assert result["google"] == "Google: litt yr kl. 14–16 og fra kl. 21, til sammen 0,5 mm."


def test_rest_of_day_when_it_rains_now():
    rows = vv.parse_yr(yr_payload({0: 1.0, 1: 0.5}))
    result = vv.rest_of_day(rows, [], NOW)
    assert result["head"] == "Regn nå, tørt fra kl. 10"
    assert result["google"] is None  # Google mangler, ingen tekst


def test_night_summary_finds_peak_and_dry_time():
    # I natt = lokal 22-08 = indeks 14-23. Timer med minst halvparten av toppen: lokal 03-05 (indeks 19-20).
    rows = vv.parse_yr(yr_payload({16: 0.4, 18: 1.3, 19: 3.7, 20: 2.5, 21: 0.8}, wind=11))
    result = vv.night_or_tomorrow(rows, google({19: 3.5, 20: 3.0}), NOW)
    assert result["title"] == "I natt"
    assert result["head"] == "Regn, 9 mm"  # over 5 mm rundes til hele mm, som på Yr
    assert "Mest mellom kl. 03 og 05." in result["text"]
    assert "Tørt igjen fra kl. 06." in result["text"]
    assert "Vind opptil 11 m/s." in result["text"]
    assert result["google"] == "Google: 7 mm."


def test_late_evening_switches_to_tomorrow():
    rows = vv.parse_yr(yr_payload())
    late = datetime(2026, 9, 27, 19, 30, tzinfo=timezone.utc)  # 21:30 lokal
    assert vv.night_or_tomorrow(rows, [], late)["title"] == "I morgen"


def test_best_window_is_longest_dry_calm_run():
    rows = vv.parse_yr(yr_payload({3: 0.5}))  # regn lokal kl. 11
    best = vv.best_window(rows, NOW)
    assert best["label"] == "i dag"
    assert best["text"].startswith("kl. 12–22")


def test_days_include_observed_rain_and_google_only_when_covered():
    rows = vv.parse_yr(yr_payload({16: 2.0}))
    observed = {START - timedelta(hours=5): 0.6, START - timedelta(hours=4): 0.3}
    days = vv.build_days(rows, google({16: 1.0}), observed, NOW)
    today, tomorrow = days[0], days[1]
    assert today["label"] == "I dag" and tomorrow["label"] == "I morgen"
    assert today["observed"] == 0.9
    assert today["rain"] == 0.9  # regnet på indeks 16 er lokal kl. 00 i morgen
    assert today["periods"][0]["observed"] is True and today["periods"][0]["rain"] == 0.9
    assert tomorrow["rain"] == 2.0 and tomorrow["g_rain"] == 1.0
    assert days[2]["g_rain"] is None  # Google dekker ikke hele døgnet
    assert len(days) >= 5


def test_agreement_texts():
    rows = vv.parse_yr(yr_payload({15: 1.0, 16: 3.0, 17: 3.0}))
    same = vv.agreement(vv.build_hours(rows, google({13: 1.0, 16: 3.0, 17: 2.8}), START))
    assert same["title"] == "Yr og Google er enige om regnet"
    assert "Google tror det starter litt tidligere, kl. 21 mot kl. 23 hos Yr." in same["text"]
    differ = vv.agreement(vv.build_hours(rows, google(temp_offset=-2.0), START))
    assert differ["title"] == "Yr og Google er uenige om regnet"
    assert "Google er i snitt 2° kaldere." in differ["text"]


def test_nowcast_parsing_and_coverage():
    series = [{"time": (NOW + timedelta(minutes=m)).isoformat(), "data": {"instant": {"details": {
        "precipitation_rate": 1.2 if m == 30 else 0.0, "air_temperature": 12.7, "wind_speed": 6.5}}}}
        for m in range(0, 125, 5)]
    result = vv.parse_nowcast({"properties": {"meta": {"radar_coverage": "ok"}, "timeseries": series}}, NOW)
    assert result["available"] and result["temp"] == 12.7
    assert [s["rate"] for s in result["steps"]].count(1.2) == 1
    assert result["steps"][0]["time"] == "08:19"
    bad = vv.parse_nowcast({"properties": {"meta": {"radar_coverage": "temporarily unavailable"},
                                           "timeseries": series}}, NOW)
    assert bad["available"] is False


def test_observed_rain_only_counts_today_local_time():
    rows = [
        {"element": "sum(precipitation_amount PT1H)", "reference_time": "2026-09-26T22:00:00Z", "value": 5},  # 23-00 i går
        {"element": "sum(precipitation_amount PT1H)", "reference_time": "2026-09-26T23:00:00Z", "value": 0.4},
        {"element": "air_temperature", "reference_time": "2026-09-26T23:00:00Z", "value": 12},
    ]
    assert list(vv.observed_rain(rows, NOW).values()) == [0.4]


def test_trust_text():
    rows = [{"place": "bergen", "bucket": "alle", "element": "temp", "winner": "yr"},
            {"place": "bergen", "bucket": "alle", "element": "rain", "winner": "yr"},
            {"place": "bergen", "bucket": "alle", "element": "wind", "winner": None},
            {"place": "bergen", "bucket": "1-6t", "element": "gust", "winner": "google"}]
    result = vv.trust_text(rows, "bergen", 30)
    assert result["title"] == "Yr har truffet best i Bergen"
    assert "Yr bommet minst på temperatur og nedbør" in result["text"]
    assert vv.trust_text(rows, "kvamskogen", 30)["title"] == "Ingen klar vinner ennå"


def _app():
    app = Flask(__name__, template_folder="../templates")
    app.register_blueprint(vv.vaer_varsel)
    return app.test_client()


def test_api_survives_missing_sources_and_rejects_unknown_place():
    client = _app()
    assert client.get("/ver/api/varsel/oslo").status_code == 404
    with patch.object(vv, "_fetch_yr", return_value=yr_payload()), \
         patch.object(vv, "_fetch_nowcast", side_effect=RuntimeError), \
         patch.object(vv, "_fetch_google", side_effect=RuntimeError), \
         patch.object(vv, "_fetch_observations", side_effect=RuntimeError), \
         patch.object(vv, "_trust_rows", return_value=None):
        res = client.get("/ver/api/varsel/bergen")
    assert res.status_code == 200
    data = res.json
    assert data["nowcast"]["available"] is False
    assert data["google_available"] is False
    assert len(data["hours"]) == 25 and data["days"]
    assert res.headers["Cache-Control"] == "no-store"


def test_api_returns_503_without_yr():
    with patch.object(vv, "_fetch_yr", side_effect=RuntimeError), \
         patch.object(vv, "_fetch_nowcast", return_value=None), \
         patch.object(vv, "_fetch_google", return_value=[]), \
         patch.object(vv, "_fetch_observations", return_value=None), \
         patch.object(vv, "_trust_rows", return_value=None):
        assert _app().get("/ver/api/varsel/bergen").status_code == 503


def test_page_and_radar_routes():
    client = _app()
    page = client.get("/ver/varsel/bergen")
    assert page.status_code == 200
    assert b'const STED = "bergen"' in page.data
    assert "frame-ancestors" in page.headers["Content-Security-Policy"]
    assert client.get("/ver/varsel/oslo").status_code == 404
    assert client.get("/ver/varsel").status_code == 302
    with patch.object(vv, "_fetch_radar", return_value=b"GIF89a"):
        radar = client.get("/ver/api/radar/bergen.gif")
    assert radar.status_code == 200 and radar.mimetype == "image/gif"
    assert client.get("/ver/api/radar/oslo.gif").status_code == 404


def test_feels_like_matches_yr_example():
    # Yr viste «Føles som 11°» ved 13° og 7 m/s.
    assert round(vv.feels_like(13, 7)) == 11
    assert vv.feels_like(13, 1.0) == 13
    assert vv.feels_like(24, 8) == 24
    assert vv.feels_like(None, 5) is None


def test_days_report_max_wind_and_gust():
    rows = vv.parse_yr(yr_payload(wind=9.4))
    day = vv.build_days(rows, [], {}, NOW)[0]
    assert day["wind_max"] == 9
