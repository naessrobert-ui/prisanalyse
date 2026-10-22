"""Tester for handler_redaksjon: «Viktige endringer siste døgn»."""

from __future__ import annotations

import datetime as dt
import os
import sqlite3
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import handler_redaksjon as red  # noqa: E402
import handler_varsel as hv  # noqa: E402

D = dt.date


def _sig():
    rad = dict(investor_id="T1", investor="STOR EIER", isin="X", ticker="XXX", aksje="X ASA",
               start="2026-10-14", siste="2026-10-14", handelsdager=1, antall=-200_000,
               belop_mnok=-20.0, beh_for=5_000_000, beh_etter=4_800_000, andel_pct=4.0,
               pause_dager=135, ut=False, type="forste_salg", rang=1, lenke="https://x/t1")
    folg = dict(investor_id="F1", investor="FOLGE FOND", isin="X", ticker="XXX", aksje="X ASA",
                start="2026-10-14", siste="2026-10-14", handelsdager=1, antall=10_000,
                belop_mnok=1.0, beh_etter=1_010_000, type="", ut=False, lenke="https://x/f1")
    return {"fra": "2026-10-14", "til": "2026-10-14", "innstillinger": hv.Innstillinger(lister=["Beste.csv"]),
            "folgeliste": [folg], "flinke": [], "topp10": [rad], "antall_flinke_fulgt": 0,
            "antall_folgeliste": 1, "lister": ["Beste.csv"]}


@pytest.fixture(autouse=True)
def _isoler(monkeypatch, tmp_path):
    monkeypatch.setattr(red, "_CACHE_PATH", tmp_path / "redaksjon.json")
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.delenv("HANDLER_REDAKSJON_AV", raising=False)


def test_fakta_rekkefolge_og_felt():
    rader = red.fakta(_sig())
    assert [r["id"] for r in rader] == ["T1", "F1"]          # topp 10 før følgelisten
    t = rader[0]
    assert t["retning"] == "salg" and t["type"] == "Begynner å selge" and t["rang_blant_eiere"] == 1
    til_modell = red._til_modell(rader)
    assert "lenke" not in til_modell[0] and "solgt_helt_ut" not in til_modell[0]


def test_uten_nokkel_regelbasert():
    r = red.lag(_sig())
    assert r["kilde"] == "regel" and r["modell"] is None
    assert r["saker"][0]["kilder"] == ["T1"]
    assert "STOR EIER solgte XXX" == r["saker"][0]["overskrift"]
    assert "20,0 mill. kr" in r["saker"][0]["tekst"] and "nr. 1 blant eierne" in r["saker"][0]["tekst"]


def test_ai_svar_brukes_og_ukjente_kilder_fjernes(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    monkeypatch.setattr(red, "_claude", lambda sig, rader: {
        "tittel": "Storeier selger X", "ingress": "En topp 10-eier begynner å selge.",
        "saker": [{"overskrift": "STOR EIER selger", "tekst": "Solgte for 20 mill. kr.", "kilder": ["T1", "Z9"]}],
    })
    r = red.lag(_sig())
    assert r["kilde"] == "ai" and r["modell"] == red.MODELL
    assert r["saker"][0]["kilder"] == ["T1"]
    assert [x["id"] for x in r["rader"]] == ["T1", "F1"]


def test_ai_feil_gir_regelbasert(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")

    def _feil(sig, rader):
        raise RuntimeError("Claude stoppet: refusal")
    monkeypatch.setattr(red, "_claude", _feil)
    r = red.lag(_sig())
    assert r["kilde"] == "regel" and "refusal" in r["feil"]


def test_lagre_hent_siste():
    assert red.siste() is None
    a = red.lag_og_lagre(_sig())
    assert red.hent("2026-10-14", "2026-10-14")["tittel"] == a["tittel"]
    assert red.siste()["til"] == "2026-10-14"
    assert red.hent("2026-10-13", "2026-10-14") is None


def test_bakgrunn_bare_med_ai(monkeypatch):
    assert red.lag_i_bakgrunnen(_sig()) is False          # ingen nøkkel
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    startet = []
    monkeypatch.setattr(red.threading, "Thread", lambda target, **k: type("T", (), {"start": lambda s: startet.append(1)})())
    assert red.lag_i_bakgrunnen(_sig()) is True and startet == [1]
    red.lagre({**red.lag(_sig()), "kilde": "ai"})
    assert red.lag_i_bakgrunnen(_sig()) is False          # finnes allerede


def test_side(monkeypatch, tmp_path):
    flask = pytest.importorskip("flask")
    import handler_data as hd
    import handler_routes as routes

    p = str(tmp_path / "h.db")
    sqlite3.connect(p).close()
    monkeypatch.setattr(routes, "_check_db", lambda: None)
    monkeypatch.setattr(hd, "db_connect", lambda *a, **k: sqlite3.connect(p))
    monkeypatch.setattr(hd, "get_last_data_date", lambda conn: "2026-10-14")
    kall = []
    monkeypatch.setattr(hv, "finn_signaler", lambda conn, fra, til: kall.append((fra, til)) or _sig())

    app = flask.Flask(__name__)
    app.register_blueprint(routes.handler_bp)
    c = app.test_client()

    r = c.get("/handler-oslo-bors/viktige-endringer")
    html = r.get_data(as_text=True)
    assert r.status_code == 200 and "Viktige endringer siste døgn" in html
    assert "STOR EIER solgte XXX" in html and "https://x/t1" in html and "Alle signaler (2)" in html
    assert kall == [(D(2026, 10, 14), D(2026, 10, 14))]

    c.get("/handler-oslo-bors/viktige-endringer")           # lagret, lages ikke på nytt
    assert len(kall) == 1
    c.post("/handler-oslo-bors/viktige-endringer?date_from=2026-10-14&date_to=2026-10-14")
    assert len(kall) == 2


def test_epost_lenker_til_siden():
    html = hv.bygg_html(_sig() | {"topp10": [], "folgeliste": []})
    assert "/handler-oslo-bors/viktige-endringer?date_from=2026-10-14&amp;date_to=2026-10-14" in html \
        or "/handler-oslo-bors/viktige-endringer?date_from=2026-10-14&date_to=2026-10-14" in html
