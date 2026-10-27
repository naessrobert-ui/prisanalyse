"""Tester for kursgrafen (handler_data.fetch_kursgraf og /api/kursgraf)."""

from __future__ import annotations

import datetime as dt
import os
import sqlite3
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import handler_data as hd  # noqa: E402

ISIN = "NO0010096985"


def _build_db(path: str) -> None:
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE investor (
            investor_id TEXT PRIMARY KEY, investor_type TEXT,
            first_name TEXT, last_name TEXT, country_code TEXT, raw_id TEXT
        );
        CREATE TABLE security (
            isin TEXT PRIMARY KEY, ticker TEXT, isin_name TEXT, last_price REAL
        );
        CREATE TABLE position_change (
            isin TEXT, investor_id TEXT, date_today TEXT,
            holding_today REAL, holding_yesterday REAL,
            price_today REAL, price_yesterday REAL, change_qty REAL,
            PRIMARY KEY (isin, investor_id, date_today)
        );
        """
    )
    conn.execute("INSERT INTO investor VALUES ('1001','Fond','HOLBERG NORGE','VERDIPAPIRFONDET',NULL,NULL)")
    conn.execute("INSERT INTO security VALUES (?, 'EQNR', 'Equinor', 300.0)", (ISIN,))
    rows = []
    # Kursserie via en annen eier (uten handler) fra 2026-08-20 til 2026-09-30
    d = dt.date(2026, 8, 20)
    px = 280.0
    while d <= dt.date(2026, 9, 30):
        if d.weekday() < 5:
            rows.append((ISIN, "2001", d.isoformat(), 10_000, 10_000, px + 1, px, 0))
            px += 1
        d += dt.timedelta(days=1)
    # Investor 1001: kjøp, kjøp, salg (price_today = sluttkurs på handelsdagen)
    rows += [
        (ISIN, "1001", "2026-09-07", 600_000, 500_000, 293.0, 292.0, 100_000),
        (ISIN, "1001", "2026-09-08", 650_000, 600_000, 294.0, 293.0, 50_000),
        (ISIN, "1001", "2026-09-21", 250_000, 650_000, 303.0, 302.0, -400_000),
        (ISIN, "1001", "2026-09-22", 250_000, 250_000, 304.0, 303.0, 0),   # ingen endring
    ]
    conn.executemany("INSERT INTO position_change VALUES (?,?,?,?,?,?,?,?)", rows)
    conn.commit()
    conn.close()


@pytest.fixture()
def db_path(tmp_path):
    p = tmp_path / "topchanges.db"
    _build_db(str(p))
    return str(p)


@pytest.fixture()
def conn(db_path):
    c = sqlite3.connect(db_path)
    c.row_factory = sqlite3.Row
    try:
        yield c
    finally:
        c.close()


def test_pad_days_bounds():
    assert hd.kursgraf_pad_days(dt.date(2026, 9, 1), dt.date(2026, 9, 10)) == 14
    assert hd.kursgraf_pad_days(dt.date(2026, 1, 1), dt.date(2026, 7, 1)) == 36
    assert hd.kursgraf_pad_days(dt.date(2020, 1, 1), dt.date(2026, 1, 1)) == 90


def test_fetch_kursgraf_prices_and_trades(conn):
    data = hd.fetch_kursgraf(conn, "1001", ISIN, dt.date(2026, 9, 1), dt.date(2026, 9, 30))
    assert data["ticker"] == "EQNR"
    assert data["investor"] == "HOLBERG NORGE VERDIPAPIRFONDET"
    # Kursen starter 14 dager før periodestart, for kontekst
    assert data["price_from"] == "2026-08-18"
    assert data["prices"][0]["d"] == "2026-08-20"
    assert data["prices"][-1]["d"] == "2026-09-30"
    assert all(p["p"] > 0 for p in data["prices"])

    tr = data["trades"]
    assert [t["dato"] for t in tr] == ["2026-09-07", "2026-09-08", "2026-09-21"]
    assert [t["antall"] for t in tr] == [100_000, 50_000, -400_000]
    assert tr[0]["kurs"] == pytest.approx(293.0)
    assert tr[2]["belop_mnok"] == pytest.approx(-121.2)
    # Markørene ligger på kurslinjen
    px = {p["d"]: p["p"] for p in data["prices"]}
    assert all(px[t["dato"]] == pytest.approx(t["kurs"]) for t in tr)
    assert tr[0]["beholdning_for"] == 500_000
    assert [t["beholdning"] for t in tr] == [600_000, 650_000, 250_000]


def test_fetch_kursgraf_no_trades(conn):
    data = hd.fetch_kursgraf(conn, "9999", ISIN, dt.date(2026, 9, 1), dt.date(2026, 9, 30))
    assert data["trades"] == []
    assert data["prices"]
    assert data["investor"] == "9999"


def test_api_kursgraf(db_path, monkeypatch):
    flask = pytest.importorskip("flask")
    import handler_routes as hr

    monkeypatch.setattr(hr, "_check_db", lambda: None)
    real_connect = hd.db_connect
    monkeypatch.setattr(hd, "db_connect", lambda *a, **k: real_connect(db_path))

    app = flask.Flask(__name__)
    app.register_blueprint(hr.handler_bp)
    client = app.test_client()

    r = client.get(f"/handler-oslo-bors/api/kursgraf?investor_id=1001&isin={ISIN}"
                   "&date_from=2026-09-01&date_to=2026-09-30")
    assert r.status_code == 200
    body = r.get_json()
    assert len(body["trades"]) == 3
    assert body["ticker"] == "EQNR"

    r = client.get("/handler-oslo-bors/api/kursgraf?investor_id=1001")
    assert r.status_code == 400


def test_summarize_transactions_per_isin():
    import pandas as pd
    import handler_data_beste as hdb

    tx = pd.DataFrame([
        # dato, isin, ticker, navn, qty, trade_price, last_price
        ("2026-01-05", "A", "AAA", "Aaa ASA", 1000, 100.0, 120.0),
        ("2026-02-05", "A", "AAA", "Aaa ASA", -400, 110.0, 120.0),
        ("2026-03-05", "A", "AAA", "Aaa ASA", 0, 115.0, 120.0),     # ingen endring, telles ikke
        ("2026-01-10", "B", "BBB", "Bbb ASA", 500, 50.0, 40.0),
    ], columns=["dato", "isin", "ticker", "navn", "qty", "trade_price", "last_price"])
    tx["gross_kr"] = tx["qty"].abs() * tx["trade_price"]
    tx["profit_kr"] = tx["qty"] * (tx["last_price"] - tx["trade_price"])

    g = hdb.summarize_transactions_per_isin(tx).set_index("isin")
    a, b = g.loc["A"], g.loc["B"]
    assert list(g.index) == ["A", "B"]                 # sortert etter gevinst
    assert a["handler"] == 2
    assert a["kjop_antall"] == 1000 and a["salg_antall"] == 400 and a["netto_antall"] == 600
    assert a["kjop_mnok"] == pytest.approx(0.1)
    assert a["salg_mnok"] == pytest.approx(0.044)
    assert a["netto_mnok"] == pytest.approx(0.056)
    assert a["brutto_mnok"] == pytest.approx(0.144)
    # 1000*(120-100) - 400*(120-110) = 20000 - 4000
    assert a["gevinst_mnok"] == pytest.approx(0.016)
    assert a["gevinst_pct"] == pytest.approx(16000 / 144000 * 100)
    assert a["snitt_kjop_kurs"] == pytest.approx(100.0)
    assert a["snitt_salg_kurs"] == pytest.approx(110.0)
    assert a["forste_dato"] == "2026-01-05" and a["siste_dato"] == "2026-02-05"
    assert b["gevinst_mnok"] == pytest.approx(-0.005)
    assert pd.isna(b["snitt_salg_kurs"])


def test_summarize_transactions_per_isin_empty():
    import pandas as pd
    import handler_data_beste as hdb
    assert hdb.summarize_transactions_per_isin(pd.DataFrame()).empty


def _best_db(path):
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
        ("A", "Fond", "ALFA", "FOND", "NO", None),
        ("B", "Fond", "BETA", "FOND", "NO", None),
        ("C", "Fond", "GAMMA", "FOND", "NO", None),
    ])
    conn.executemany("INSERT INTO security VALUES (?,?,?,?)", [
        ("X", "XXX", "X ASA", 120.0), ("Y", "YYY", "Y ASA", 50.0)])
    rows = [
        # A: liten, god gevinst i X
        ("X", "A", "2026-02-01", 0, 0, 0, 100.0, 1000),
        ("X", "A", "2026-02-02", 0, 0, 0, 100.0, 0),        # ingen endring, ikke en handel
        # B: stor omsetning i X, solgt netto
        ("X", "B", "2026-03-01", 0, 0, 0, 130.0, -50000),
        ("X", "B", "2026-03-02", 0, 0, 0, 125.0, 10000),
        ("X", "B", "2026-03-03", 0, 0, 0, 122.0, -5000),
        # C: kjøper i Y
        ("Y", "C", "2026-04-01", 0, 0, 0, 40.0, 20000),
    ]
    conn.executemany("INSERT INTO position_change VALUES (?,?,?,?,?,?,?,?)", rows)
    conn.commit()
    conn.close()


def test_compute_best_investors_sort_and_single_stock(tmp_path):
    import handler_data_beste as hdb

    p = str(tmp_path / "best.db")
    _best_db(p)
    c = sqlite3.connect(p)
    c.row_factory = sqlite3.Row
    cols = hdb.detect_cols(c)
    last = {"X": 120.0, "Y": 50.0}

    def run(sort_by, isin_filter=None):
        return hdb.compute_best_investors(
            conn=c, cols=cols, date_from=dt.date(2026, 1, 1), date_to=dt.date(2026, 12, 31),
            investor_ids=None, invtype_choice="Alle", country_codes=None,
            isin_filter=isin_filter, min_trades=1, min_brutto_mnok=0,
            last_price_map=last, sort_by=sort_by)

    # Én aksje: bare A og B
    r = run("gevinst_mnok", ["X"])
    assert set(r["investor_id"]) == {"A", "B"}
    a = r.set_index("investor_id").loc["A"]
    assert a["trades"] == 1                       # nullrad telles ikke
    assert a["gevinst_mnok"] == pytest.approx(0.02)

    # Mest aktive etter omsetning: B først
    assert run("brutto_mnok", ["X"])["investor_id"].iloc[0] == "B"
    assert run("trades", ["X"])["investor_id"].iloc[0] == "B"

    # Netto kjøpere / selgere over hele markedet
    assert run("netto_kjop")["investor_id"].iloc[0] == "C"
    r = run("netto_salg")
    assert r["investor_id"].iloc[0] == "B"
    b = r.set_index("investor_id").loc["B"]
    # 10000*125 - 50000*130 - 5000*122
    assert b["netto_mnok"] == pytest.approx((1_250_000 - 6_500_000 - 610_000) / 1e6)
    assert b["siste_dato"] == "2026-03-03"
    c.close()


def test_compute_best_investors_median_og_min_aksjer(tmp_path):
    import handler_data_beste as hdb

    p = str(tmp_path / "med.db")
    conn = sqlite3.connect(p)
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
        ("FLAKS", "Fond", "EN", "STOR VINNER", "NO", None),
        ("JEVN", "Fond", "JEVN", "VINNER", "NO", None),
    ])
    conn.executemany("INSERT INTO security VALUES (?,?,?,?)",
                     [("A", "A", "A", 200.0), ("B", "B", "B", 90.0), ("C", "C", "C", 90.0)])
    rows = [
        # FLAKS: dobler i A (stor), taper 10 % i B og C
        ("A", "FLAKS", "2026-02-01", 0, 0, 0, 100.0, 10_000),
        ("B", "FLAKS", "2026-02-01", 0, 0, 0, 100.0, 1_000),
        ("C", "FLAKS", "2026-02-01", 0, 0, 0, 100.0, 1_000),
        # JEVN: +5 % i alle tre via kjøp på 85,7 / 190,5
        ("A", "JEVN", "2026-02-01", 0, 0, 0, 190.0, 1_000),
        ("B", "JEVN", "2026-02-01", 0, 0, 0, 85.0, 1_000),
        ("C", "JEVN", "2026-02-01", 0, 0, 0, 85.0, 1_000),
    ]
    conn.executemany("INSERT INTO position_change VALUES (?,?,?,?,?,?,?,?)", rows)
    conn.commit()
    conn.row_factory = sqlite3.Row
    cols = hdb.detect_cols(conn)
    last = {"A": 200.0, "B": 90.0, "C": 90.0}

    def run(**kw):
        args = dict(conn=conn, cols=cols, date_from=dt.date(2026, 1, 1), date_to=dt.date(2026, 12, 31),
                    investor_ids=None, invtype_choice="Alle", country_codes=None, isin_filter=None,
                    min_trades=1, min_brutto_mnok=0, last_price_map=last)
        args.update(kw)
        return hdb.compute_best_investors(**args)

    samlet = run(sort_by="gevinst_pct", pct_metode="samlet").set_index("investor_id")
    # FLAKS samlet: (1 000 000 - 10 000 - 10 000) / 1 200 000
    assert samlet.loc["FLAKS", "gevinst_pct"] == pytest.approx(980_000 / 1_200_000 * 100)
    assert samlet.loc["FLAKS", "median_pct"] == pytest.approx(-10.0)
    assert samlet.loc["FLAKS", "treff_pct"] == pytest.approx(100 / 3)
    assert samlet.loc["JEVN", "treff_pct"] == pytest.approx(100.0)
    assert list(samlet.index)[0] == "FLAKS"           # samlet: flaks vinner

    median = run(sort_by="gevinst_pct", pct_metode="median")
    assert median["investor_id"].iloc[0] == "JEVN"    # median: den jevne vinner
    assert run(sort_by="treff_pct")["investor_id"].iloc[0] == "JEVN"

    assert len(run(min_aksjer=3)) == 2
    assert run(min_aksjer=4).empty
    conn.close()


@pytest.mark.parametrize("navn,forventet", [
    ("BofA Securities Europe SA", True),
    ("NORDNET LIVSFORSIKRING AS", True),
    ("State Street Bank and Trust Comp", True),
    ("Clients Account", True),
    ("UBS AG", True),
    ("SUBSEA 7 SA", False),          # «UBS» inni et ord skal ikke treffe
    ("FOLKETRYGDFONDET", False),
    ("SKAGEN VEKST", False),
])
def test_er_nominee(navn, forventet):
    import handler_data_beste as hdb
    assert hdb.er_nominee(navn) is forventet


def test_compute_best_investors_utelater_nominee_som_standard(tmp_path):
    import handler_data_beste as hdb

    p = str(tmp_path / "nom.db")
    _best_db(p)
    c = sqlite3.connect(p)
    c.execute("INSERT INTO investor VALUES ('N','Org','STATE STREET BANK','NOMINEE','US',NULL)")
    c.execute("INSERT INTO position_change VALUES ('X','N','2026-05-01',0,0,0,100.0,5000)")
    c.commit()
    c.row_factory = sqlite3.Row
    cols = hdb.detect_cols(c)
    args = dict(conn=c, cols=cols, date_from=dt.date(2026, 1, 1), date_to=dt.date(2026, 12, 31),
                investor_ids=None, invtype_choice="Alle", country_codes=None, isin_filter=None,
                min_trades=1, min_brutto_mnok=0, last_price_map={"X": 120.0, "Y": 50.0})
    assert "N" not in set(hdb.compute_best_investors(**args)["investor_id"])
    assert "N" in set(hdb.compute_best_investors(**args, med_nominee=True)["investor_id"])
    c.close()
