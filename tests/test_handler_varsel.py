"""Tester for handler_varsel: klassifisering av nye rekker og samlet signalsøk."""

from __future__ import annotations

import datetime as dt
import os
import sqlite3
import sys

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import handler_varsel as hv  # noqa: E402

D = dt.date


def _tx(rows):
    df = pd.DataFrame(rows, columns=["investor_id", "isin", "dato", "antall", "kurs", "beh_for", "beh_etter"])
    df["belop"] = df["antall"] * df["kurs"]
    return df


def test_klassifiser_typer():
    tx = _tx([
        # A: kjøpsrekke i mars, begynner å selge i vinduet -> snur_salg
        ("A", "X", "2026-03-01", 1000, 100, 5000, 6000),
        ("A", "X", "2026-10-01", 1000, 100, 6000, 7000),
        ("A", "X", "2026-10-13", -2000, 110, 7000, 5000),
        ("A", "X", "2026-10-14", -1000, 111, 5000, 4000),
        # B: ingen handel før -> ny posisjon
        ("B", "X", "2026-10-14", 5000, 110, 0, 5000),
        # C: salg etter lang pause -> forste_salg, og helt ute
        ("C", "Y", "2026-01-05", 300, 50, 0, 300),
        ("C", "Y", "2026-10-14", -300, 60, 300, 0),
        # D: fortsetter kjøpsrekke inn i vinduet -> ingen signal
        ("D", "X", "2026-10-10", 100, 100, 1000, 1100),
        ("D", "X", "2026-10-13", 100, 100, 1100, 1200),
        # E: selger etter kjøp for 20 dager siden -> snur_salg (ikke lang pause)
        ("E", "Y", "2026-09-24", 100, 50, 900, 1000),
        ("E", "Y", "2026-10-14", -500, 60, 1000, 500),
        # F: kjøper igjen etter 100 dager med beholdning -> forste_kjop
        ("F", "Y", "2026-07-01", 100, 50, 900, 1000),
        ("F", "Y", "2026-10-14", 400, 60, 1000, 1400),
    ])
    s = hv.klassifiser(tx, D(2026, 10, 13), D(2026, 10, 14)).set_index("investor_id")
    assert set(s.index) == {"A", "B", "C", "E", "F"}

    a = s.loc["A"]
    assert a["type"] == "snur_salg"
    assert a["handelsdager"] == 2 and a["antall"] == -3000
    assert a["start"] == "2026-10-13" and a["siste"] == "2026-10-14"
    assert a["andel_pct"] == pytest.approx(3000 / 7000 * 100)
    assert a["belop_mnok"] == pytest.approx(-(2000 * 110 + 1000 * 111) / 1e6)

    assert s.loc["B", "type"] == "ny_posisjon"
    assert pd.isna(s.loc["B", "andel_pct"])

    c = s.loc["C"]
    assert c["type"] == "forste_salg" and bool(c["ut"])
    assert c["pause_dager"] == (D(2026, 10, 14) - D(2026, 1, 5)).days

    assert s.loc["E", "type"] == "snur_salg"
    assert s.loc["F", "type"] == "forste_kjop"
    assert s.loc["F", "andel_pct"] == pytest.approx(40.0)


def test_klassifiser_tom():
    assert hv.klassifiser(pd.DataFrame(), D(2026, 1, 1), D(2026, 1, 2)).empty


def _db(path):
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE investor (investor_id TEXT PRIMARY KEY, investor_type TEXT,
            first_name TEXT, last_name TEXT, country_code TEXT, raw_id TEXT);
        CREATE TABLE security (isin TEXT PRIMARY KEY, ticker TEXT, isin_name TEXT, last_price REAL);
        CREATE TABLE position_change (isin TEXT, investor_id TEXT, date_today TEXT,
            holding_today REAL, holding_yesterday REAL, price_today REAL, price_yesterday REAL,
            change_qty REAL, PRIMARY KEY (isin, investor_id, date_today));
        """
    )
    conn.executemany("INSERT INTO investor VALUES (?,?,?,?,?,?)", [
        ("F1", "Fond", "FOLGE", "FOND", "NO", None),
        ("T1", "Fond", "STOR", "EIER", "NO", None),
        ("N1", "Fond", "STATE STREET BANK", "NOMINEE", "US", None),
        ("S1", "Fond", "LITEN", "EIER", "NO", None),
    ])
    conn.execute("INSERT INTO security VALUES ('X','XXX','X ASA',100)")
    rows = []
    # Topp 3 eiere i X dagen før: T1 (5 mill), N1 (4 mill), F1 (1 mill); S1 er liten (10k)
    for inv, beh in [("T1", 5_000_000), ("N1", 4_000_000), ("F1", 1_000_000), ("S1", 10_000)]:
        rows.append(("X", inv, "2026-06-01", beh, beh - 100, 0, 90.0, 100))   # gammelt kjøp
    rows += [
        ("X", "T1", "2026-10-14", 4_800_000, 5_000_000, 0, 100.0, -200_000),  # topp 10 snur til salg
        ("X", "N1", "2026-10-14", 3_800_000, 4_000_000, 0, 100.0, -200_000),  # nominee, utelates
        ("X", "S1", "2026-10-14", 9_500, 10_000, 0, 100.0, -500),             # under terskel (5 %, 0,05 MNOK)
        ("X", "F1", "2026-10-14", 1_010_000, 1_000_000, 0, 100.0, 10_000),    # følgeliste
    ]
    conn.executemany("INSERT INTO position_change VALUES (?,?,?,?,?,?,?,?)", rows)
    conn.commit()
    conn.close()


def test_finn_signaler(tmp_path, monkeypatch):
    p = str(tmp_path / "h.db")
    _db(p)
    monkeypatch.setattr(hv, "_folgeliste_ids", lambda conn, cfg, brukt=None: ["F1"])
    monkeypatch.setattr(hv, "_flinke_ids", lambda conn, til, cfg: [])
    conn = sqlite3.connect(p)
    conn.row_factory = sqlite3.Row
    cfg = hv.Innstillinger(min_mnok=5, min_andel_pct=10, lister=["Beste.csv"])
    sig = hv.finn_signaler(conn, D(2026, 10, 14), D(2026, 10, 14), cfg)
    conn.close()

    assert [r["investor_id"] for r in sig["folgeliste"]] == ["F1"]
    assert sig["folgeliste"][0]["antall"] == 10_000
    assert [r["investor_id"] for r in sig["topp10"]] == ["T1"]
    t = sig["topp10"][0]
    assert t["rang"] == 1 and t["type"] == "forste_salg" and t["pause_dager"] == 135 and t["ticker"] == "XXX"
    assert "investor_id=T1" in t["lenke"] and "isin=X" in t["lenke"]
    assert hv.antall_signaler(sig) == 2

    html = hv.bygg_html(sig)
    assert "STOR EIER" in html and "FOLGE FOND" in html and "NOMINEE" not in html
    assert "Topp 10-eier" in hv.emne(sig) or "topp 10" in hv.emne(sig)


def test_kjor_etter_opplasting_bare_ved_nye_datoer(monkeypatch):
    startet = []
    monkeypatch.setattr(hv, "kjor", lambda fra, til, send=True: startet.append((fra, til)))
    monkeypatch.delenv("HANDLER_VARSEL_AV", raising=False)
    assert hv.kjor_etter_opplasting(D(2026, 10, 14), D(2026, 10, 14)) is False
    assert hv.kjor_etter_opplasting(None, None) is False
    assert hv.kjor_etter_opplasting(D(2026, 10, 13), D(2026, 10, 14)) is True
    # stor backfill: maks 5 dager tilbake
    assert hv.kjor_etter_opplasting(D(2026, 1, 1), D(2026, 10, 14)) is True
    import time
    time.sleep(0.2)
    assert (D(2026, 10, 14), D(2026, 10, 14)) in startet
    assert (D(2026, 10, 9), D(2026, 10, 14)) in startet
    monkeypatch.setenv("HANDLER_VARSEL_AV", "1")
    assert hv.kjor_etter_opplasting(D(2026, 10, 13), D(2026, 10, 14)) is False
