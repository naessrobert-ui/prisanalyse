from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, patch

import numpy as np
import pandas as pd
import pytest

from scripts import weather_comparison as wc
from scripts import weather_scoreboard as sb


RUN = datetime(2026, 9, 12, 7, 0, tzinfo=timezone.utc)


@pytest.fixture(autouse=True)
def local_store(tmp_path, monkeypatch):
    """Ingen test skal treffe S3 eller repoets egen datamappe."""
    monkeypatch.setenv("WEATHER_SCOREBOARD_DIR", str(tmp_path))
    monkeypatch.delenv("S3_BUCKET_NAME", raising=False)
    monkeypatch.delenv("WEATHER_SCOREBOARD_S3_BUCKET", raising=False)
    return tmp_path


def forecast(hours, *, first=RUN, updated=None):
    rows = []
    for offset, values in enumerate(hours):
        start = first + timedelta(hours=offset)
        rows.append({
            "start": sb._iso(start), "end": sb._iso(start + timedelta(hours=1)),
            "temp": values.get("temp"), "rain": values.get("rain"),
            "wind": values.get("wind"), "gust": values.get("gust"), "cloud": None,
        })
    return {"hours": rows, "fetched_at": sb._iso(first), "updated_at": updated,
            "expires_at": sb._iso(first), "error": None}


def frost_item(time, observations):
    return {"sourceId": "SN50540:0", "referenceTime": time, "observations": observations}


def frost_response(items, status=200):
    response = Mock(status_code=status)
    response.json.return_value = {"data": items}
    return response


# ---------------------------------------------------------------------------
# Tidskonvensjon – den ene feilen som ville ødelagt scoren i stillhet
# ---------------------------------------------------------------------------

def test_hour_accumulated_elements_are_shifted_back_one_hour():
    """Frost-summen ved 07:00 gjelder timen 06:00–07:00, ikke 07:00–08:00."""
    raw = pd.DataFrame([
        {"place": "bergen", "reference_time": pd.Timestamp("2026-09-12T07:00Z"),
         "element": "sum(precipitation_amount PT1H)", "value": 2.4},
        {"place": "bergen", "reference_time": pd.Timestamp("2026-09-12T07:00Z"),
         "element": "max(wind_speed_of_gust PT1H)", "value": 14.0},
        {"place": "bergen", "reference_time": pd.Timestamp("2026-09-12T07:00Z"),
         "element": "air_temperature", "value": 11.4},
        {"place": "bergen", "reference_time": pd.Timestamp("2026-09-12T07:00Z"),
         "element": "wind_speed", "value": 4.5},
    ])
    table = sb.observation_table(raw).set_index("valid_start")

    assert table.loc[pd.Timestamp("2026-09-12T06:00Z"), "rain"] == 2.4
    assert table.loc[pd.Timestamp("2026-09-12T06:00Z"), "gust"] == 14.0
    # Øyeblikksverdier flyttes ikke.
    assert table.loc[pd.Timestamp("2026-09-12T07:00Z"), "temp"] == 11.4
    assert table.loc[pd.Timestamp("2026-09-12T07:00Z"), "wind"] == 4.5
    assert pd.isna(table.loc[pd.Timestamp("2026-09-12T07:00Z")].get("rain"))


def test_interval_basis_averages_the_hour_for_instant_elements():
    """Google varsler for hele timen, Yr for starten av den. Grunnlaget må kunne byttes."""
    raw = pd.DataFrame([
        {"place": "bergen", "reference_time": pd.Timestamp(f"2026-09-12T{hour:02d}:00Z"),
         "element": "air_temperature", "value": value}
        for hour, value in ((6, 10.0), (7, 12.0))
    ])
    instant = sb.observation_table(raw).set_index("valid_start")
    interval = sb.observation_table(raw, basis="interval").set_index("valid_start")

    assert instant.loc[pd.Timestamp("2026-09-12T06:00Z"), "temp"] == 10.0
    assert interval.loc[pd.Timestamp("2026-09-12T06:00Z"), "temp"] == 11.0
    # Siste time mangler en etterfølger og blir stående tom i stedet for gjettet.
    assert pd.isna(interval.loc[pd.Timestamp("2026-09-12T07:00Z"), "temp"])


def test_interval_basis_does_not_bridge_a_gap_in_the_series():
    raw = pd.DataFrame([
        {"place": "bergen", "reference_time": pd.Timestamp("2026-09-12T06:00Z"),
         "element": "air_temperature", "value": 10.0},
        {"place": "bergen", "reference_time": pd.Timestamp("2026-09-12T09:00Z"),
         "element": "air_temperature", "value": 20.0},
    ])
    table = sb.observation_table(raw, basis="interval").set_index("valid_start")

    assert pd.isna(table.loc[pd.Timestamp("2026-09-12T06:00Z"), "temp"])


def test_interval_basis_leaves_accumulated_elements_alone():
    """Nedbør og vindkast gjelder allerede hele timen hos begge leverandørene."""
    raw = pd.DataFrame([
        {"place": "bergen", "reference_time": pd.Timestamp(f"2026-09-12T{hour:02d}:00Z"),
         "element": "sum(precipitation_amount PT1H)", "value": value}
        for hour, value in ((7, 1.0), (8, 3.0))
    ])
    table = sb.observation_table(raw, basis="interval").set_index("valid_start")

    assert table.loc[pd.Timestamp("2026-09-12T06:00Z"), "rain"] == 1.0


def test_unknown_basis_is_rejected():
    with pytest.raises(ValueError):
        sb.observation_table(pd.DataFrame(), basis="tilfeldig")


# ---------------------------------------------------------------------------
# Innsamling
# ---------------------------------------------------------------------------

def test_snapshot_stores_both_providers_with_lead_hours():
    payloads = {
        "yr": forecast([{"temp": 10}, {"temp": 11}]),
        "google": forecast([{"temp": 12}, {"temp": 13}]),
    }
    with patch.object(sb, "fetch_forecast", lambda place, provider: payloads[provider]):
        summary = sb.snapshot(places=["bergen"], now=RUN + timedelta(minutes=20))

    assert summary["errors"] == {}
    stored = sb._read_day("prognoser", "2026-09-12")
    assert sorted(stored["provider"].unique()) == ["google", "yr"]
    # Kjøring 07:20 gir lead 0 for timen 07:00 og lead 1 for 08:00.
    assert sorted(stored["lead_hours"].unique().tolist()) == [0, 1]
    assert stored["run_hour"].unique().tolist() == ["2026-09-12T07:00:00Z"]


def test_snapshot_overwrites_instead_of_duplicating_the_same_hour():
    payload = forecast([{"temp": 10}])
    with patch.object(sb, "fetch_forecast", lambda place, provider: payload):
        sb.snapshot(places=["bergen"], now=RUN)
        sb.snapshot(places=["bergen"], now=RUN + timedelta(minutes=40))

    stored = sb._read_day("prognoser", "2026-09-12")
    assert len(stored) == 2  # én rad per leverandør, ikke fire


def test_snapshot_keeps_the_other_provider_when_one_fails():
    def fetch(place, provider):
        if provider == "google":
            raise wc.ForecastError("Google-varselet er ikke aktivert på serveren ennå.")
        return forecast([{"temp": 10}])

    with patch.object(sb, "fetch_forecast", fetch):
        summary = sb.snapshot(places=["bergen"], now=RUN)

    assert summary["rows"] == 1
    assert "bergen/google" in summary["errors"]
    assert sb._read_day("prognoser", "2026-09-12")["provider"].tolist() == ["yr"]


def test_observations_ignore_sub_hourly_series(monkeypatch):
    """SN50310 sender wind_speed både som PT10M og PT1H; bare timen kan pares."""
    monkeypatch.setenv("FROST_CLIENT_ID", "test-client")
    session = Mock()
    session.get.return_value = frost_response([frost_item("2026-09-12T06:00:00.000Z", [
        {"elementId": "wind_speed", "value": 2.8, "timeResolution": "PT10M", "qualityCode": 0},
        {"elementId": "wind_speed", "value": 2.3, "timeResolution": "PT1H", "qualityCode": 0},
    ])])
    with patch.object(sb, "_frost_session", return_value=session):
        rows = sb.fetch_observations("bergen", RUN - timedelta(hours=2), RUN)

    assert [row["value"] for row in rows] == [2.3]


def test_frost_failure_is_reported_without_leaking_the_query(monkeypatch):
    monkeypatch.setenv("FROST_CLIENT_ID", "hemmelig-klient-id")
    session = Mock()
    session.get.return_value = frost_response([], status=403)
    with patch.object(sb, "_frost_session", return_value=session):
        summary = sb.collect_observations(places=["bergen"], hours_back=2, now=RUN)

    assert summary["rows"] == 0
    assert "hemmelig-klient-id" not in summary["errors"]["bergen"]
    assert "403" in summary["errors"]["bergen"]


def test_missing_frost_period_is_not_an_error(monkeypatch):
    monkeypatch.setenv("FROST_CLIENT_ID", "test-client")
    session = Mock()
    session.get.return_value = frost_response([], status=404)
    with patch.object(sb, "_frost_session", return_value=session):
        summary = sb.collect_observations(places=["bergen"], hours_back=2, now=RUN)

    assert summary == {"window": ["2026-09-12T05:00:00Z", "2026-09-12T07:00:00Z"],
                       "rows": 0, "written": [], "errors": {}}


# ---------------------------------------------------------------------------
# Paring og score
# ---------------------------------------------------------------------------

def seed(rows, observations, *, place="bergen"):
    """Legg ferdige varsel- og observasjonsrader i lageret."""
    forecasts = pd.DataFrame([{
        "run_hour": sb._iso(r["run"]), "fetched_at": sb._iso(r["run"]), "place": place,
        "provider": r["provider"], "valid_start": sb._iso(r["valid"]),
        "lead_hours": int((r["valid"] - r["run"]).total_seconds() // 3600),
        "temp": r.get("temp"), "rain": r.get("rain"), "wind": r.get("wind"),
        "gust": r.get("gust"), "cloud": None, "model_updated_at": None,
    } for r in rows], columns=sb.FORECAST_COLUMNS)
    forecasts["_day"] = forecasts["run_hour"].str.slice(0, 10)
    sb._append("prognoser", forecasts, ["run_hour", "place", "provider", "valid_start"])

    raw = pd.DataFrame([{
        "place": place, "source_id": "SN50540", "element": sb.ELEMENTS[o["element"]]["frost"],
        "reference_time": sb._iso(o["valid"] + timedelta(hours=sb.ELEMENTS[o["element"]]["shift"])),
        "value": o["value"], "quality": 0, "fetched_at": sb._iso(RUN),
    } for o in observations], columns=sb.OBSERVATION_COLUMNS)
    raw["_day"] = raw["reference_time"].str.slice(0, 10)
    sb._append("observasjoner", raw, ["place", "reference_time", "element"])


def test_paired_drops_hours_where_one_provider_is_missing():
    valid = RUN + timedelta(hours=2)
    seed(
        [{"run": RUN, "provider": "yr", "valid": valid, "temp": 10},
         {"run": RUN, "provider": "yr", "valid": valid + timedelta(hours=1), "temp": 11},
         {"run": RUN, "provider": "google", "valid": valid, "temp": 12}],
        [{"element": "temp", "valid": valid, "value": 11},
         {"element": "temp", "valid": valid + timedelta(hours=1), "value": 11}],
    )
    pairs = sb.paired(RUN, RUN + timedelta(hours=12))

    assert len(pairs) == 1
    assert pairs.iloc[0]["valid_start"] == pd.Timestamp(valid)
    assert pairs.iloc[0]["err_yr"] == -1 and pairs.iloc[0]["err_google"] == 1


def test_score_reports_mae_bias_and_win_rate():
    rows, truth = [], []
    for offset in range(1, 5):
        valid = RUN + timedelta(hours=offset)
        # Yr bommer 1 grad for lavt, Google 3 grader for høyt.
        rows += [{"run": RUN, "provider": "yr", "valid": valid, "temp": 9},
                 {"run": RUN, "provider": "google", "valid": valid, "temp": 13}]
        truth.append({"element": "temp", "valid": valid, "value": 10})
    seed(rows, truth)

    result = sb.score(sb.paired(RUN, RUN + timedelta(hours=12)))
    row = result[(result["element"] == "temp") & (result["bucket"] == "1-6t")].iloc[0]

    assert row["n"] == 4
    assert row["yr_mae"] == 1.0 and row["google_mae"] == 3.0
    assert row["yr_bias"] == -1.0 and row["google_bias"] == 3.0
    assert row["yr_win_rate"] == 1.0


def test_lead_buckets_are_reported_separately():
    rows, truth = [], []
    for offset in (2, 20):
        valid = RUN + timedelta(hours=offset)
        rows += [{"run": RUN, "provider": "yr", "valid": valid, "temp": 10},
                 {"run": RUN, "provider": "google", "valid": valid, "temp": 10}]
        truth.append({"element": "temp", "valid": valid, "value": 10})
    seed(rows, truth)

    result = sb.score(sb.paired(RUN, RUN + timedelta(hours=48)))
    assert set(result["bucket"]) == {"1-6t", "13-24t", "alle"}


def test_head_to_head_is_a_draw_on_a_single_day():
    """Én dag med data sier ingenting sikkert, uansett hvor stor forskjellen ser ut."""
    frame = pd.DataFrame({
        "day": ["2026-09-12"] * 24,
        "err_yr": [0.1] * 24,
        "err_google": [5.0] * 24,
    })
    verdict = sb.head_to_head(frame)

    assert verdict.winner is None
    assert verdict.low is None and verdict.high is None
    assert verdict.diff == pytest.approx(4.9)


def test_head_to_head_names_a_winner_when_the_gap_holds_across_days():
    rng = np.random.default_rng(3)
    days = [f"2026-09-{day:02d}" for day in range(1, 15) for _ in range(24)]
    frame = pd.DataFrame({
        "day": days,
        "err_yr": rng.normal(0, 1, len(days)),
        "err_google": rng.normal(0, 1, len(days)) + 4,
    })
    verdict = sb.head_to_head(frame)

    assert verdict.winner == "yr"
    assert verdict.low > 0


def test_lag_scan_finds_a_provider_shifted_by_one_hour():
    """Er Googles nedbør en time foran? Da skal skanningen peke på det."""
    rows, truth = [], []
    observed = [0.0, 4.0, 0.0, 0.0, 4.0, 0.0, 0.0, 4.0]
    for offset, value in enumerate(observed, start=1):
        valid = RUN + timedelta(hours=offset)
        truth.append({"element": "rain", "valid": valid, "value": value})
        rows.append({"run": RUN, "provider": "yr", "valid": valid, "rain": value})
        # Google varsler timen før sin egen verdi: en times forskyvning.
        shifted = observed[offset] if offset < len(observed) else 0.0
        rows.append({"run": RUN, "provider": "google", "valid": valid, "rain": shifted})
    seed(rows, truth)

    scan = sb.lag_scan(RUN, RUN + timedelta(hours=12)).set_index("provider")

    assert scan.loc["yr", "best_lag"] == 0
    assert scan.loc["google", "best_lag"] == 1
    assert scan.loc["google", "mae_+1"] < scan.loc["google", "mae_+0"]


def test_lag_scan_is_absent_unless_asked_for():
    assert sb.report(days=3, now=RUN)["lag"] is None
    assert sb.report(days=3, now=RUN, with_lag=True)["lag"] is not None


def test_rain_skill_counts_hits_and_false_alarms():
    pairs = pd.DataFrame({
        "element": ["rain"] * 4,
        "place": ["bergen"] * 4,
        "observed": [0.0, 0.5, 2.0, 0.0],
        "yr": [0.0, 0.4, 1.0, 0.0],       # treffer alle fire
        "google": [0.3, 0.0, 3.0, 0.0],   # én falsk alarm, én bom
    })
    result = sb.rain_skill(pairs).set_index("provider")

    assert result.loc["yr", "accuracy"] == 1.0
    assert result.loc["yr", "pod"] == 1.0 and result.loc["yr", "far"] == 0.0
    assert result.loc["google", "accuracy"] == 0.5
    assert result.loc["google", "pod"] == 0.5
    assert result.loc["google", "far"] == 0.5


def test_report_without_data_is_empty_but_valid(local_store):
    data = sb.report(days=3, now=RUN)

    assert data["coverage"]["comparisons"] == 0
    assert data["coverage"]["hours"] == 0
    # Rapporten må kunne skille tom lagring fra «ingen forskjell».
    assert data["coverage"]["forecast_rows"] == 0
    assert data["coverage"]["storage"] == str(local_store)
    assert data["score"].empty and data["rain"].empty


def test_empty_storage_and_missing_pairs_print_different_reasons(capsys, local_store):
    """Tom rapport kan bety tom lagring eller manglende par. Ikke la dem se like ut."""
    from scripts import weather_scoreboard_run as run

    run._print_report(sb.report(days=3, now=RUN + timedelta(hours=6)))
    empty = capsys.readouterr().out
    assert "Fant ingen lagrede varsler" in empty
    assert "S3_BUCKET_NAME" in empty  # lokal mappe: si hvorfor den er tom

    # Bare Yr lagret: varsler finnes, men ingen time kan pares.
    valid = RUN + timedelta(hours=2)
    seed([{"run": RUN, "provider": "yr", "valid": valid, "temp": 10}],
         [{"element": "temp", "valid": valid, "value": 10}])
    run._print_report(sb.report(days=3, now=RUN + timedelta(hours=6)))
    lonely = capsys.readouterr().out
    assert "Fant ingen lagrede varsler" not in lonely
    assert "begge leverandørene" in lonely


def test_storage_prefers_s3_when_configured(monkeypatch):
    monkeypatch.setenv("S3_BUCKET_NAME", "min-boette")
    monkeypatch.setenv("WEATHER_SCOREBOARD_S3_PREFIX", "weather-scoreboard")

    assert sb.storage_location() == "s3://min-boette/weather-scoreboard"


# ---------------------------------------------------------------------------
# Delt henting
# ---------------------------------------------------------------------------

def test_fetch_forecast_rejects_unknown_place_and_provider():
    for args in (("mars", "yr"), ("bergen", "openweather")):
        with pytest.raises(wc.ForecastError):
            wc.fetch_forecast(*args)


def test_fetch_forecast_bypasses_the_page_cache():
    """Loggen må se et nytt varsel hver time, ikke det siden allerede har vist."""
    with patch.object(wc, "_fetch", return_value={"hours": [], "fetched_at": None}) as fetch:
        wc.fetch_forecast("bergen", "yr")
        wc.fetch_forecast("bergen", "yr")

    assert fetch.call_count == 2
