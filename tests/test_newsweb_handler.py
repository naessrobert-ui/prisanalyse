"""Handler i aksjen før en børsmelding (privat panel på NewsWeb-siden)."""

from __future__ import annotations

import datetime as dt
import os
import sqlite3
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import newsweb_handler as nh  # noqa: E402

D = dt.date


def _conn():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE investor (investor_id TEXT PRIMARY KEY, investor_type TEXT,
            first_name TEXT, last_name TEXT);
        CREATE TABLE security (isin TEXT PRIMARY KEY, ticker TEXT, isin_name TEXT);
        CREATE TABLE position_change (isin TEXT, investor_id TEXT, date_today TEXT,
            change_qty REAL, price_yesterday REAL, price_today REAL);
        """
    )
    conn.executemany("INSERT INTO investor VALUES (?,?,?,?)", [
        ("V1", "C", "", "VARNER INVEST AS"),
        ("A", "C", "", "AAT INVEST AS"),
        ("B", "C", "", "FOLKETRYGDFONDET"),
        ("N", "C", "", "CLEARSTREAM BANKING S.A."),
    ])
    conn.executemany("INSERT INTO security VALUES (?,?,?)", [
        ("NO0001", "ABC", "Abc ASA"),
        ("NO0002", "XYZ", "Xyz ASA"),
    ])
    conn.executemany("INSERT INTO position_change VALUES (?,?,?,?,?,?)", [
        # Viktige-investor V1 kjøper i uka før meldingen (26.10)
        ("NO0001", "V1", "2026-10-22", 10_000, 100, 0),
        ("NO0001", "V1", "2026-10-15", 5_000, 100, 0),   # før 7-dagersvinduet
        # Største kjøper/selger siste 30 dager
        ("NO0001", "B", "2026-10-05", 200_000, 50, 0),
        ("NO0001", "A", "2026-10-10", -30_000, 0, 60),    # kurs fra dagens kurs
        ("NO0001", "A", "2026-10-13", -10_000, 0, 0),     # kurs fra neste dag
        ("NO0001", "X", "2026-10-14", 1, 61, 0),
        ("NO0001", "N", "2026-10-20", 9_000_000, 50, 0),  # nominee utelates
        ("NO0001", "B", "2026-09-01", -999_999, 50, 0),   # utenfor 30 dager
        ("NO0001", "A", "2026-10-27", 99_999, 50, 0),     # etter meldingen
        ("NO0002", "B", "2026-10-20", 1_000, 10, 0),      # annen aksje
    ])
    return conn


def test_meldingsdato_er_norsk_dato():
    assert nh.meldingsdato("2026-10-25T23:30:00+00:00") == D(2026, 10, 26)
    assert nh.meldingsdato("2026-10-26T12:11:37.591000+00:00") == D(2026, 10, 26)
    assert nh.meldingsdato("ikke dato") is None
    assert nh.meldingsdato("2026-10-26T12:00:00") is None


def test_handler_for_melding():
    svar = nh.handler_for_melding(_conn(), "abc", D(2026, 10, 26), {"Viktige.csv": ["V1"]})
    assert svar["notert"] and svar["navn"] == "Abc ASA"
    assert svar["siste_data"] == "2026-10-27"

    assert [(r["eier"], r["liste"]) for r in svar["viktige"]] == [("VARNER INVEST AS", "Viktige")]
    assert svar["viktige"][0]["netto_mnok"] == pytest.approx(1.0)

    assert [r["eier"] for r in svar["kjopere"]][:2] == ["FOLKETRYGDFONDET", "VARNER INVEST AS"]
    assert svar["kjopere"][0]["netto_mnok"] == pytest.approx(10.0)
    assert svar["kjopere"][1]["netto_mnok"] == pytest.approx(1.5)
    assert all("CLEARSTREAM" not in r["eier"] for r in svar["kjopere"] + svar["selgere"])

    assert [r["eier"] for r in svar["selgere"]] == ["AAT INVEST AS"]
    assert svar["selgere"][0]["netto_mnok"] == pytest.approx(-(30_000 * 60 + 10_000 * 61) / 1e6)


def test_ikke_notert_ticker():
    svar = nh.handler_for_melding(_conn(), "COLG", D(2026, 10, 26), {"Viktige.csv": ["V1"]})
    assert svar["notert"] is False
    assert svar["viktige"] == svar["kjopere"] == svar["selgere"] == []


def test_ingen_viktige_handler():
    svar = nh.handler_for_melding(_conn(), "XYZ", D(2026, 10, 26), {"Viktige.csv": ["V1"]})
    assert svar["notert"] and svar["viktige"] == []
    assert [r["eier"] for r in svar["kjopere"]] == ["FOLKETRYGDFONDET"]


def _client(monkeypatch, logged_in):
    from flask import Flask
    import newsweb_routes as nr
    monkeypatch.setenv("SITE_ACCESS_CODE", "kode")
    app = Flask(__name__, template_folder=os.path.join(os.path.dirname(__file__), "..", "templates"))
    app.secret_key = "test"
    app.register_blueprint(nr.newsweb_bp)
    client = app.test_client()
    if logged_in:
        with client.session_transaction() as s:
            s["access_granted"] = True
    return client


def test_route_krever_innlogging(monkeypatch):
    client = _client(monkeypatch, logged_in=False)
    r = client.get("/newsweb/handler?ticker=ABC&published_at=2026-10-26T12:00:00Z")
    assert r.status_code == 403


def test_route_validerer_og_svarer(monkeypatch):
    import handler_data as hd
    import newsweb_routes as nr
    client = _client(monkeypatch, logged_in=True)
    assert client.get("/newsweb/handler?ticker=ABC;DROP&published_at=2026-10-26T12:00:00Z").status_code == 400
    assert client.get("/newsweb/handler?ticker=ABC&published_at=feil").status_code == 400

    monkeypatch.setattr(hd, "db_connect", lambda: _conn())
    monkeypatch.setattr(nr, "_trades_list_ids", lambda conn: {"Viktige.csv": ["V1"]})
    nr._trades_cache.clear()
    r = client.get("/newsweb/handler?ticker=ABC&published_at=2026-10-26T12:00:00Z")
    assert r.status_code == 200
    assert "no-store" in r.headers["Cache-Control"]
    data = r.get_json()
    assert data["viktige"][0]["eier"] == "VARNER INVEST AS"
    assert data["kjopere"][0]["eier"] == "FOLKETRYGDFONDET"
