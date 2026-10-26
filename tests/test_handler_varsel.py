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
    monkeypatch.setattr(hv, "_folgeliste_ids", lambda conn, cfg, brukt=None, beste=None: ["F1"])
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


def test_epost_taaler_manglende_pause_og_andel():
    """Snur-signaler og nye posisjoner har ingen pause; pandas gjør det til NaN."""
    tx = _tx([
        ("A", "X", "2026-10-01", 1000, 100, 5000, 6000), ("A", "X", "2026-10-14", -2000, 110, 6000, 4000),
        ("B", "X", "2026-10-14", 5000, 110, 0, 5000),
        ("C", "Y", "2026-01-05", 300, 50, 0, 300), ("C", "Y", "2026-10-14", -300, 60, 300, 0),
    ])
    s = hv.klassifiser(tx, D(2026, 10, 14), D(2026, 10, 14))
    assert s["pause_dager"].isna().sum() == 2
    rader = [dict(r, investor=r["investor_id"], ticker="T", aksje="A", lenke="x") for r in s.to_dict("records")]
    html = hv._seksjon("Test", "", rader)
    assert "Snur: selger" in html and "Ny posisjon" in html and "første handel på 282 dager" in html
    sig = {"fra": "2026-10-14", "til": "2026-10-14", "innstillinger": hv.Innstillinger(lister=["Beste.csv"]),
           "folgeliste": [], "flinke": rader, "topp10": [], "antall_flinke_fulgt": 3, "lister": ["Beste.csv"]}
    assert "Flinke investorer som snur (3)" in hv.bygg_html(sig)


def test_datoer_som_tekst_godtas(monkeypatch):
    """handler_data.get_last_data_date returnerer ISO-tekst, ikke date."""
    assert hv.som_dato("2026-10-14") == D(2026, 10, 14)
    assert hv.som_dato(D(2026, 10, 14)) == D(2026, 10, 14)
    assert hv.som_dato(None) is None and hv.som_dato("tull") is None
    startet = []
    monkeypatch.setattr(hv, "kjor", lambda fra, til, send=True: startet.append((fra, til)))
    monkeypatch.delenv("HANDLER_VARSEL_AV", raising=False)
    assert hv.kjor_etter_opplasting("2026-10-13", "2026-10-14") is True
    import time
    time.sleep(0.2)
    assert startet == [(D(2026, 10, 14), D(2026, 10, 14))]


class _FakeSMTP:
    sendt: list = []
    feil: Exception | None = None

    def __init__(self, host, port, timeout=None):
        self.host = host

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def starttls(self):
        pass

    def login(self, u, p):
        pass

    def sendmail(self, fra, til, msg):
        if _FakeSMTP.feil:
            raise _FakeSMTP.feil
        _FakeSMTP.sendt.append((fra, til, msg))


@pytest.fixture()
def smtp_env(monkeypatch, tmp_path):
    for k, v in {"SMTP_HOST": "smtp.resend.com", "SMTP_USER": "resend", "SMTP_PASSWORD": "re_x",
                 "HANDLER_VARSEL_TIL": "meg@eksempel.no", "HANDLER_VARSEL_FRA": "varsel@eksempel.no"}.items():
        monkeypatch.setenv(k, v)
    monkeypatch.delenv("MEDIA_DIGEST_FROM", raising=False)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.setattr(hv.smtplib, "SMTP", _FakeSMTP)
    monkeypatch.setattr(hv, "_STATE_PATH", tmp_path / "state.json")
    _FakeSMTP.sendt, _FakeSMTP.feil = [], None
    return monkeypatch


def test_oppsett_status_viser_mangler(smtp_env):
    assert hv.oppsett_status()["ok"]
    smtp_env.delenv("HANDLER_VARSEL_FRA")
    st = hv.oppsett_status()
    assert not st["ok"] and any("HANDLER_VARSEL_FRA" in m for m in st["mangler"])   # «resend» er ingen adresse
    smtp_env.delenv("SMTP_PASSWORD")
    assert "SMTP_PASSWORD" in hv.oppsett_status()["mangler"]


def test_send_test_ok_og_feil(smtp_env):
    ok, m = hv.send_test()
    assert ok and len(_FakeSMTP.sendt) == 1 and _FakeSMTP.sendt[0][0] == "varsel@eksempel.no"
    _FakeSMTP.feil = hv.smtplib.SMTPSenderRefused(550, b"domain not verified", "varsel@eksempel.no")
    ok, m = hv.send_test()
    assert not ok and "domain not verified" in m


def test_kjor_lagrer_grunn_og_feil(smtp_env, tmp_path):
    p = str(tmp_path / "h.db")
    _db(p)
    smtp_env.setattr(hv, "_folgeliste_ids", lambda conn, cfg, brukt=None, beste=None: ["F1"])
    smtp_env.setattr(hv, "_flinke_ids", lambda conn, til, cfg: [])
    r = hv.kjor(D(2026, 10, 14), D(2026, 10, 14), db_path=p)
    assert r["sendt"] and hv.siste_kjoring()["sendt"]
    r = hv.kjor(D(2026, 10, 14), D(2026, 10, 14), db_path=p)
    assert not r["sendt"] and r["grunn"] == "Perioden er allerede sendt"
    _FakeSMTP.feil = hv.smtplib.SMTPAuthenticationError(535, b"invalid api key")
    r = hv.kjor(D(2026, 10, 14), D(2026, 10, 14), db_path=p, kilde="manuell", tving=True)
    assert not r["sendt"] and "invalid api key" in r["feil"]
    assert "invalid api key" in hv.siste_kjoring()["feil"]
    r = hv.kjor(D(2026, 1, 1), D(2026, 1, 1), db_path=p)
    assert r["grunn"] == "Ingen signaler i perioden"


# ---------------------------------------------------------
# Beløpsgulv for Beste.csv i følgelisten
# ---------------------------------------------------------
BESTE = ["B_SNUR", "B_AGG10", "B_AGG9", "B_UT4", "B_UT5", "B_UT6", "B_BEGGE"]
VIKTIGE = ["V1", "B_BEGGE"]   # B_BEGGE står i begge listene og behandles som Viktige


def _db_beste(path):
    """Vindu 2026-10-05 til 2026-10-14, kurs 100: 10 000 aksjer = 1 MNOK."""
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
    conn.executemany("INSERT INTO investor VALUES (?,?,?,?,?,?)",
                     [(i, "Fond", i, "FOND", "NO", None) for i in BESTE + ["V1"]])
    conn.execute("INSERT INTO security VALUES ('X','XXX','X ASA',100)")
    rows = []

    def pc(inv, dato, beh_for, endring):
        rows.append(("X", inv, dato, beh_for + endring, beh_for, 0, 100.0, endring))

    # Viktige: liten handel, skal alltid med
    pc("V1", "2026-10-14", 1_000, 100)                       # 0,01 MNOK
    pc("B_BEGGE", "2026-10-14", 1_000, 100)                  # 0,01 MNOK, i begge lister
    # Snur fra kjøp til salg, bare 1 MNOK
    pc("B_SNUR", "2026-10-01", 50_000, 10_000)
    pc("B_SNUR", "2026-10-14", 60_000, -10_000)
    # 10 handler à 1 MNOK i samme retning (fortsetter en kjøpsrekke, ingen snu) = 10 MNOK
    pc("B_AGG10", "2026-09-01", 100_000, 1_000)
    for i in range(10):
        pc("B_AGG10", f"2026-10-{5 + i:02d}", 101_000 + i * 10_000, 10_000)
    # 9 handler à 1 MNOK = 9 MNOK, under 10 og ikke snu eller utgang
    pc("B_AGG9", "2026-09-01", 100_000, 1_000)
    for i in range(9):
        pc("B_AGG9", f"2026-10-{5 + i:02d}", 101_000 + i * 10_000, 10_000)
    # Selger seg helt ut etter lang pause (forste_salg, ikke snu): 4, 5 og 6 MNOK
    for inv, beh in [("B_UT4", 40_000), ("B_UT5", 50_000), ("B_UT6", 60_000)]:
        pc(inv, "2026-01-05", 0, beh)
        pc(inv, "2026-10-14", beh, -beh)
    conn.executemany("INSERT INTO position_change VALUES (?,?,?,?,?,?,?,?)", rows)
    conn.commit()
    conn.close()


def _folg_beste(conn, cfg, brukt=None, beste=None):
    if beste is not None:
        beste.update(set(BESTE) - set(VIKTIGE))
    return sorted(set(BESTE) | set(VIKTIGE))


def _signaler_beste(tmp_path, monkeypatch, cfg=None):
    p = str(tmp_path / "beste.db")
    _db_beste(p)
    monkeypatch.setattr(hv, "_folgeliste_ids", _folg_beste)
    monkeypatch.setattr(hv, "_flinke_ids", lambda conn, til, cfg: [])
    conn = sqlite3.connect(p)
    conn.row_factory = sqlite3.Row
    try:
        return hv.finn_signaler(conn, D(2026, 10, 5), D(2026, 10, 14), cfg or hv.Innstillinger())
    finally:
        conn.close()


def test_beste_belopsgulv(tmp_path, monkeypatch, caplog):
    monkeypatch.delenv("HANDLER_VARSEL_BESTE_STOR_MNOK", raising=False)
    monkeypatch.delenv("HANDLER_VARSEL_BESTE_UTGANG_MNOK", raising=False)
    with caplog.at_level("INFO", logger=hv.__name__):
        sig = _signaler_beste(tmp_path, monkeypatch)
    rader = {r["investor_id"]: r for r in sig["folgeliste"]}

    # Viktige urørt, også når investoren står i Beste i tillegg
    assert "V1" in rader and "B_BEGGE" in rader
    # Snu under 5 MNOK beholdes
    assert rader["B_SNUR"]["type"] == "Snur: selger"
    # 10 handler à 1 MNOK summeres til 10 MNOK, nøyaktig på grensen -> med
    assert rader["B_AGG10"]["belop_mnok"] == pytest.approx(10.0)
    assert rader["B_AGG10"]["handelsdager"] == 10
    assert "B_AGG9" not in rader
    # Helt ute: under 5 fjernes, nøyaktig 5 og over beholdes
    assert "B_UT4" not in rader
    assert rader["B_UT5"]["ut"] and rader["B_UT6"]["ut"]

    f = sig["beste_filter"]
    assert f["inn_kombinasjoner"] == 6 and f["inn"] == 1 + 10 + 9 + 3
    assert (f["snur"], f["stor"], f["utgang"]) == (1, 10, 2)
    assert (f["fjernet"], f["fjernet_kombinasjoner"], f["fjernet_utgang_under"]) == (10, 2, 1)
    assert "Beste.csv: 23 handler inn" in caplog.text and "fjernet 10 handler" in caplog.text

    # E-posten bygges som før, med færre rader
    assert "Følgelisten (6)" in hv.bygg_html(sig)
    assert hv.antall_signaler(sig) == 6


def test_beste_terskler_fra_miljo(tmp_path, monkeypatch):
    monkeypatch.setenv("HANDLER_VARSEL_BESTE_STOR_MNOK", "9")
    monkeypatch.setenv("HANDLER_VARSEL_BESTE_UTGANG_MNOK", "6")
    sig = _signaler_beste(tmp_path, monkeypatch)
    ids = {r["investor_id"] for r in sig["folgeliste"]}
    assert {"B_AGG9", "B_UT6"} <= ids and not {"B_UT4", "B_UT5"} & ids


def test_beste_regel_grenser():
    cfg = hv.Innstillinger(beste_stor_mnok=10, beste_utgang_mnok=5)
    assert hv._beste_regel({"snur_kjop"}, 0.1, False, cfg) == "snur"
    assert hv._beste_regel({"snur_salg"}, -0.1, True, cfg) == "snur"
    assert hv._beste_regel(set(), 10.0, False, cfg) == "stor"
    assert hv._beste_regel(set(), -10.0, False, cfg) == "stor"
    assert hv._beste_regel(set(), 9.999, False, cfg) is None
    assert hv._beste_regel({"ut", "forste_salg"}, -5.0, True, cfg) == "utgang"
    assert hv._beste_regel({"ut"}, -4.999, True, cfg) is None
    assert hv._beste_regel({"ny_posisjon"}, 9.0, False, cfg) is None


def test_dagens_varsel_side(monkeypatch, tmp_path):
    flask = pytest.importorskip("flask")
    import handler_data as hd
    import handler_routes as routes

    p = str(tmp_path / "h.db")
    sqlite3.connect(p).close()
    monkeypatch.setattr(routes, "_check_db", lambda: None)
    monkeypatch.setattr(routes, "_VARSEL_SIDE_CACHE", {})
    monkeypatch.setattr(hd, "db_connect", lambda *a, **k: sqlite3.connect(p))
    monkeypatch.setattr(hd, "get_last_data_date", lambda conn: "2026-10-14")
    monkeypatch.setattr(hv, "siste_kjoring", lambda: {"fra": "2026-10-13", "til": "2026-10-14", "sendt": True,
                                                      "tid": "2026-10-14T07:25:00"})
    topp = dict(investor_id="T1", investor="STOR EIER", isin="X", ticker="XXX", aksje="X ASA",
                start="2026-10-14", siste="2026-10-14", handelsdager=2, antall=-200_000, belop_mnok=-20.0,
                andel_pct=4.0, pause_dager=135, ut=False, type="forste_salg", rang=1, lenke="https://x/t1")
    kall = []

    def finn(conn, fra, til):
        kall.append((fra, til))
        return {"fra": fra.isoformat(), "til": til.isoformat(), "innstillinger": hv.Innstillinger(lister=["Beste.csv"]),
                "folgeliste": [], "flinke": [], "topp10": [topp], "antall_flinke_fulgt": 0, "lister": ["Beste.csv"]}

    monkeypatch.setattr(hv, "finn_signaler", finn)
    app = flask.Flask(__name__)
    app.register_blueprint(routes.handler_bp)
    c = app.test_client()

    html = c.get("/handler-oslo-bors/dagens-varsel").get_data(as_text=True)
    assert kall == [(D(2026, 10, 13), D(2026, 10, 14))]       # samme periode som e-posten
    assert "Topp 10-eier begynner å selge" in html and "STOR EIER" in html and "https://x/t1" in html
    assert "Begynner å selge" in html and "nr. 1 blant eierne" in html and "2 handelsdager" in html
    assert "Sendt på e-post" in html and "−20,0 MNOK" in html

    c.get("/handler-oslo-bors/dagens-varsel")                 # bufret
    assert len(kall) == 1
    html = c.get("/handler-oslo-bors/dagens-varsel?date_from=2026-10-14&date_to=2026-10-14").get_data(as_text=True)
    assert kall[-1] == (D(2026, 10, 14), D(2026, 10, 14)) and "Sendt på e-post" not in html
