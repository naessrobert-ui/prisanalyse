"""Tester for handelsdato (T+2): filens dato er oppgjørsdato, handelen var to handelsdager før."""

from __future__ import annotations

import datetime as dt
import os
import sqlite3
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import handler_data as hd  # noqa: E402

ISIN = "NO0010096985"
D = dt.date.fromisoformat


def _schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE investor (
            investor_id TEXT PRIMARY KEY, investor_type TEXT,
            first_name TEXT, last_name TEXT, country_code TEXT, raw_id TEXT
        );
        CREATE TABLE security (
            isin TEXT PRIMARY KEY, ticker TEXT, isin_name TEXT, paper_group TEXT, issuer_orgnr TEXT,
            issuer_name TEXT, registered_country TEXT, market TEXT, sector TEXT, gics_sector TEXT,
            ask_paper TEXT, issued_shares REAL
        );
        CREATE TABLE position_change (
            isin TEXT, investor_id TEXT, date_today TEXT, date_yesterday TEXT,
            holding_today REAL, holding_yesterday REAL, price_yesterday REAL,
            change_qty REAL, abs_change_qty REAL, change_percent REAL,
            flag_new_source INTEGER, flag_exit_source INTEGER, rank INTEGER, source_file TEXT,
            PRIMARY KEY (isin, investor_id, date_today)
        );
        """
    )


# Sluttkurser: 21.10 = 300, 22.10 = 301, 23.10 = 302, 26.10 = 303, 27.10 = 304
FILES = [
    # (fildato, DatoIgaar, kurs idag, kurs igaar, beh. idag, beh. igaar)
    ("2026-10-22", "2026-10-21", 301.0, 300.0, 1_000, 900),
    ("2026-10-23", "2026-10-22", 302.0, 301.0, 1_200, 1_000),
    ("2026-10-26", "2026-10-23", 303.0, 302.0, 1_500, 1_200),
    ("2026-10-27", "2026-10-26", 304.0, 303.0, 1_400, 1_500),
]


def _csv(settle, yest, p_today, p_yest, h_today, h_yest) -> bytes:
    header = "New_ID;ISIN;Ticker;ISINNAVN;DatoIdag;DatoIgaar;Beh. idag;Beh. igaar;Kurs idag;Kurs igaar;Change"
    row = f"1001;{ISIN};EQNR;Equinor;{settle};{yest};{h_today};{h_yest};{p_today};{p_yest};{h_today - h_yest}"
    return (header + "\n" + row + "\n").encode("latin-1")


@pytest.fixture()
def conn():
    c = sqlite3.connect(":memory:")
    _schema(c)
    try:
        yield c
    finally:
        c.close()


def test_kalender_helligdager():
    assert hd._paaskedag(2026) == D("2026-04-05")
    assert hd._paaskedag(2027) == D("2027-03-28")
    assert not hd.er_handelsdag(D("2026-04-02"))   # skjærtorsdag
    assert not hd.er_handelsdag(D("2026-05-14"))   # Kristi himmelfart
    assert not hd.er_handelsdag(D("2026-05-25"))   # 2. pinsedag
    assert not hd.er_handelsdag(D("2026-12-24"))
    assert hd.er_handelsdag(D("2026-10-26"))


def test_handelsdato_to_handelsdager_for_oppgjor():
    # Mandag 26.10 -> torsdag 22.10 (helgen telles ikke)
    assert hd.handelsdato_for_oppgjor(D("2026-10-26")) == D("2026-10-22")
    assert hd.handelsdato_for_oppgjor(D("2026-10-28")) == D("2026-10-26")
    # Tirsdag etter påske -> tirsdag før skjærtorsdag
    assert hd.handelsdato_for_oppgjor(D("2026-04-07")) == D("2026-03-31")
    # Over nyttår
    assert hd.handelsdato_for_oppgjor(D("2026-01-05")) == D("2025-12-30")


def test_innlesing_lagrer_handelsdato_og_sluttkurs(conn):
    hd._ensure_upload_schema(conn)
    for f in FILES:
        hd._ingest_one_csv_bytes(conn, f"TopChanges_Nordea_Invest_DAG_{f[0][2:].replace('-', '')}.csv", _csv(*f))

    rows = conn.execute(
        "SELECT date_today, date_yesterday, settle_date, price_today, price_yesterday, change_qty "
        "FROM position_change ORDER BY date_today"
    ).fetchall()
    # Filen merket 26.10 er handel 22.10, kurs = sluttkurs 22.10 og 21.10
    assert ("2026-10-22", "2026-10-21", "2026-10-26", 301.0, 300.0, 300.0) in rows
    assert ("2026-10-23", "2026-10-22", "2026-10-27", 302.0, 301.0, -100.0) in rows
    assert [r[0] for r in rows] == ["2026-10-20", "2026-10-21", "2026-10-22", "2026-10-23"]
    assert hd.get_max_date_today(conn) == D("2026-10-23")
    # Siste kurs er nyeste sluttkurs i filene, ikke kursen på siste handelsdato
    hd._refresh_security_last_price(conn)
    assert conn.execute("SELECT last_price FROM security WHERE isin=?", (ISIN,)).fetchone()[0] == 304.0

    # Samme fil på nytt gir ingen dobbeltrader
    hd._ingest_one_csv_bytes(conn, "igjen.csv", _csv(*FILES[2]))
    assert conn.execute("SELECT COUNT(*) FROM position_change").fetchone()[0] == 4
    assert not hd.trenger_handelsdato_migrering(conn)


def test_migrering_av_eksisterende_rader(conn):
    # Rader lest inn med gammel logikk: date_today = filens dato
    conn.executemany(
        "INSERT INTO position_change(isin, investor_id, date_today, date_yesterday, holding_today, "
        "holding_yesterday, price_yesterday, change_qty) VALUES (?,?,?,?,?,?,?,?)",
        [(ISIN, "1001", s, y, ht, hy, py, ht - hy) for s, y, pt, py, ht, hy in FILES],
    )
    conn.execute("ALTER TABLE position_change ADD COLUMN price_today REAL")
    for s, _y, pt, *_ in FILES:
        conn.execute("UPDATE position_change SET price_today=? WHERE date_today=?", (pt, s))
    conn.commit()
    assert hd.trenger_handelsdato_migrering(conn)

    hd._ensure_upload_schema(conn)   # kjører migreringen
    rows = conn.execute(
        "SELECT date_today, date_yesterday, settle_date, price_today, price_yesterday, change_qty "
        "FROM position_change ORDER BY date_today"
    ).fetchall()
    assert rows == [
        # 20.10 har ingen kjent sluttkurs; nærmeste etter brukes
        ("2026-10-20", "2026-10-19", "2026-10-22", 300.0, 300.0, 100.0),
        ("2026-10-21", "2026-10-20", "2026-10-23", 300.0, 300.0, 200.0),
        ("2026-10-22", "2026-10-21", "2026-10-26", 301.0, 300.0, 300.0),
        ("2026-10-23", "2026-10-22", "2026-10-27", 302.0, 301.0, -100.0),
    ]
    assert not hd.trenger_handelsdato_migrering(conn)
    assert hd.migrer_til_handelsdato(conn) == 0

    # Ny fil etter migreringen havner riktig ved siden av de flyttede radene
    hd._ingest_one_csv_bytes(conn, "ny.csv", _csv("2026-10-28", "2026-10-27", 305.0, 304.0, 1_300, 1_400))
    assert conn.execute(
        "SELECT date_today, settle_date, price_today FROM position_change WHERE settle_date='2026-10-28'"
    ).fetchone() == ("2026-10-26", "2026-10-28", 303.0)
