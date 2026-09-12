from datetime import datetime, timezone
from unittest.mock import Mock, patch

from flask import Flask

from scripts import weather_comparison as wc


def google_hour():
    return {
        "interval": {"startTime": "2026-09-12T06:00:00Z", "endTime": "2026-09-12T07:00:00Z"},
        "temperature": {"degrees": 50, "unit": "FAHRENHEIT"},
        "wind": {"speed": {"value": 36, "unit": "KILOMETERS_PER_HOUR"}},
        "precipitation": {"qpf": {"quantity": 0.1, "unit": "INCHES"}},
    }


def test_google_units_and_missing_values():
    row = wc.normalize_google([google_hour()])[0]
    assert row["temp"] == 10
    assert row["wind"] == 10
    assert row["rain"] == 2.54
    assert row["gust"] is None
    assert row["cloud"] is None


def test_yr_six_hour_rain_is_not_invented_hourly_rain():
    result = wc.normalize_yr({"properties": {"timeseries": [{
        "time": "2026-09-12T06:00:00Z", "data": {
            "instant": {"details": {"air_temperature": 0, "wind_speed": 0}},
            "next_6_hours": {"details": {"precipitation_amount": 12}},
        },
    }]}})
    assert result[0]["rain"] is None
    assert result[0]["temp"] == 0
    assert result[0]["wind"] == 0


def test_alignment_uses_utc_interval_not_display_label():
    google = wc.normalize_google([google_hour()])
    yr = wc.normalize_yr({"properties": {"timeseries": [{
        "time": "2026-09-12T08:00:00+02:00", "data": {
            "next_1_hours": {"details": {"precipitation_amount": 0}},
        },
    }]}})
    rows = wc.aligned_hours({"yr": {"hours": yr}, "google": {"hours": google}},
                            datetime(2026, 9, 12, 6, tzinfo=timezone.utc))
    assert len(rows) == 48
    assert rows[0]["yr"]["rain"] == 0
    assert rows[0]["google"]["rain"] == 2.54
    assert rows[1]["google"] is None
    google[0]["end"] = "2026-09-12T08:00:00Z"
    assert wc.aligned_hours({"google": {"hours": google}}, datetime(2026, 9, 12, 6, tzinfo=timezone.utc))[0]["google"] is None


def test_google_pagination_uses_two_pages_and_preserves_parameters(monkeypatch):
    monkeypatch.setenv("GOOGLE_WEATHER_API_KEY", "test-private-key")
    calls = []

    def respond(url, **kwargs):
        calls.append(dict(kwargs["params"]))
        return {"forecastHours": [google_hour()], **({"nextPageToken": "second"} if len(calls) == 1 else {})}

    with patch.object(wc, "_get_json", side_effect=respond):
        result = wc._fetch("bergen", "google")
    assert len(calls) == 2
    assert "pageToken" not in calls[0]
    assert calls[1]["pageToken"] == "second"
    assert calls[0]["hours"] == calls[1]["hours"] == 48
    assert "test-private-key" not in str(result)


def test_upstream_errors_do_not_disclose_key():
    response = Mock(status_code=403)
    response.json.return_value = {"error": "secret-in-upstream-error"}
    with patch.object(wc.requests, "get", return_value=response):
        try:
            wc._get_json("https://weather.googleapis.com/", params={"key": "private-key"})
        except wc.ForecastError as exc:
            assert str(exc) == "Værleverandøren svarte HTTP 403."
        else:
            raise AssertionError("Skulle avvise HTTP 403")


def test_cache_expiry_removes_payload_and_repeated_requests_reuse_fetch():
    key = "bergen", "google"
    wc._CACHE.clear()
    with patch.object(wc, "_fetch", return_value={"hours": []}) as fetch:
        wc._provider(*key)
        wc._provider(*key)
        assert fetch.call_count == 1
        old = wc._CACHE[key]
        wc._expire(key, old)
        assert key not in wc._CACHE
        wc._provider(*key)
        wc._expire(key, old)
        assert key in wc._CACHE
    wc._CACHE.clear()


def test_api_rejects_arbitrary_locations_and_preserves_partial_data():
    app = Flask(__name__)
    app.register_blueprint(wc.weather_comparison)
    client = app.test_client()
    with patch.object(wc, "_provider") as provider:
        assert client.get("/ver/api/sammenlign?sted=anywhere").status_code == 400
        provider.assert_not_called()
    def partial(place, provider):
        return {"hours": [], "fetched_at": None, "updated_at": None,
                "expires_at": "2026-09-12T09:00:00Z",
                "error": "Google utilgjengelig" if provider == "google" else None}
    with patch.object(wc, "_provider", side_effect=partial):
        result = client.get("/ver/api/sammenlign?sted=bergen")
    assert result.status_code == 200
    assert result.headers["Cache-Control"] == "no-store"
    assert result.json["providers"]["google"]["error"] == "Google utilgjengelig"
    assert result.json["providers"]["yr"]["error"] is None
    assert len(result.json["hours"]) == 48
