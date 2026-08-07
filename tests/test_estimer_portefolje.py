"""Tester for scripts/estimer_portefolje.py.

Bygger en liten syntetisk SQLite-DB som speiler skjemaet i handler_data
(`investor`, `security`, `position_change`) og verifiserer at portefølje-
estimatet plukker siste observerte beholdning, håndterer salg til null,
aggregerer på tvers av kontoer, priser posisjoner og rekonstruerer historikk
via ``as_of``.
"""

from __future__ import annotations

import os
import sqlite3
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

import estimer_portefolje as ep  # noqa: E402


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
    conn.executemany(
        "INSERT INTO investor(investor_id,investor_type,first_name,last_name) VALUES (?,?,?,?)",
        [
            ("1001", "Fond", "HOLBERG NORGE", "VERDIPAPIRFONDET"),
            ("1002", "Fond", "HOLBERG NORGE", "VERDIPAPIRFONDET II"),
            ("2001", "Person", "OLA", "NORDMANN"),
        ],
    )
    conn.executemany(
        "INSERT INTO security(isin,ticker,isin_name,last_price) VALUES (?,?,?,?)",
        [
            ("NO0010096985", "EQNR", "Equinor", 300.0),
            ("NO0003054108", "MOWI", "Mowi", 180.0),
            ("NO0010063308", "NHY", "Norsk Hydro", 65.0),
            ("NO0010823131", "AKSO", "Aker Solutions", None),  # ingen kurs -> upriset
        ],
    )
    rows = [
        # isin, inv, date, holding_today, holding_yest, price_today, price_yest, change
        ("NO0010096985", "1001", "2026-07-01", 900000, 850000, 295.0, 294.0, 50000),
        ("NO0010096985", "1001", "2026-08-06", 1000000, 900000, 305.0, 300.0, 100000),
        ("NO0010096985", "1002", "2026-08-06", 500000, 500000, 305.0, 300.0, 0),
        ("NO0003054108", "1001", "2026-05-01", 200000, 150000, 175.0, 174.0, 50000),
        ("NO0010063308", "1001", "2026-07-10", 300000, 0, 64.0, 63.0, 300000),
        ("NO0010063308", "1001", "2026-08-05", 0, 300000, 66.0, 65.0, -300000),  # salg til null
        ("NO0010823131", "1001", "2026-08-06", 50000, 40000, 0, 0, 10000),       # ingen kurs
    ]
    conn.executemany(
        """INSERT INTO position_change
           (isin,investor_id,date_today,holding_today,holding_yesterday,
            price_today,price_yesterday,change_qty)
           VALUES (?,?,?,?,?,?,?,?)""",
        rows,
    )
    conn.commit()
    conn.close()


@pytest.fixture()
def conn(tmp_path):
    db = tmp_path / "topchanges.db"
    _build_db(str(db))
    c = sqlite3.connect(str(db))
    c.row_factory = sqlite3.Row
    try:
        yield c
    finally:
        c.close()


def _by_isin(est):
    return {p.isin: p for p in est.positions}


def test_resolve_investors_by_name_and_id(conn):
    by_name = ep.resolve_investors(conn, search="HOLBERG")
    assert {i.investor_id for i in by_name} == {"1001", "1002"}

    by_id = ep.resolve_investors(conn, investor_ids=["2001"])
    assert [i.investor_id for i in by_id] == ["2001"]
    assert by_id[0].name == "OLA NORDMANN"


def test_resolve_unknown_id_is_kept(conn):
    # En id som ikke finnes i investor-tabellen skal fortsatt returneres,
    # siden den kan ha rader i position_change.
    res = ep.resolve_investors(conn, investor_ids=["9999"])
    assert [i.investor_id for i in res] == ["9999"]


def test_latest_holding_and_pricing(conn):
    investors = ep.resolve_investors(conn, investor_ids=["1001"])
    est = ep.estimate_portfolio(conn, investors)
    pos = _by_isin(est)

    # EQNR: siste observasjon 2026-08-06 => 1 000 000 aksjer, kurs 305
    assert pos["NO0010096985"].shares == 1_000_000
    assert pos["NO0010096985"].price == 305.0
    assert pos["NO0010096985"].value_mnok == pytest.approx(305.0)

    # NHY: siste observasjon er salg til 0 => utelatt som default
    assert "NO0010063308" not in pos


def test_sellout_included_when_requested(conn):
    investors = ep.resolve_investors(conn, investor_ids=["1001"])
    est = ep.estimate_portfolio(conn, investors, include_zero=True)
    pos = _by_isin(est)
    assert pos["NO0010063308"].shares == 0


def test_aggregation_across_accounts(conn):
    investors = ep.resolve_investors(conn, search="HOLBERG")  # 1001 + 1002
    est = ep.estimate_portfolio(conn, investors)
    eqnr = _by_isin(est)["NO0010096985"]
    # 1 000 000 (1001) + 500 000 (1002)
    assert eqnr.shares == 1_500_000
    assert sorted(eqnr.accounts) == ["1001", "1002"]


def test_unpriced_position_excluded_from_sum(conn):
    investors = ep.resolve_investors(conn, investor_ids=["1001"])
    est = ep.estimate_portfolio(conn, investors)
    akso = _by_isin(est)["NO0010823131"]
    assert akso.price is None
    assert akso.value_mnok is None
    # Sum skal kun inneholde prisede posisjoner (EQNR 305 + MOWI 35)
    assert est.total_value_mnok == pytest.approx(305.0 + 35.0)


def test_staleness_flagging(conn):
    investors = ep.resolve_investors(conn, investor_ids=["1001"])
    est = ep.estimate_portfolio(conn, investors, stale_days=60)
    # Referanse er siste dato i DB = 2026-08-06. MOWI sist sett 2026-05-01 => 97 dager.
    stale = {p.isin for p in est.stale_positions()}
    assert "NO0003054108" in stale
    assert "NO0010096985" not in stale


def test_as_of_reconstructs_history(conn):
    investors = ep.resolve_investors(conn, investor_ids=["1001"])
    est = ep.estimate_portfolio(conn, investors, as_of="2026-07-15")
    pos = _by_isin(est)
    # Pr. 2026-07-15: EQNR skal være 900 000 (obs 2026-07-01), kurs 295
    assert pos["NO0010096985"].shares == 900_000
    assert pos["NO0010096985"].price == 295.0
    # NHY var 300 000 og ennå ikke solgt (salget var 2026-08-05)
    assert pos["NO0010063308"].shares == 300_000


def test_sorted_by_value_desc(conn):
    investors = ep.resolve_investors(conn, investor_ids=["1001"])
    est = ep.estimate_portfolio(conn, investors)
    priced = [p.value_mnok for p in est.positions if p.value_mnok is not None]
    assert priced == sorted(priced, reverse=True)
    # Uprisede posisjoner skal ligge sist
    assert est.positions[-1].value_mnok is None
