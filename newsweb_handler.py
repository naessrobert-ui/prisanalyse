"""Handler i aksjen før en børsmelding, til NewsWeb-oversikten.

Leser handler-databasen (position_change) når siden vises. Resultatet er
privat: det vises bare for innlogget bruker og skrives aldri til den
offentlige latest.json i GitHub.

  - Viktige/Beste: handler fra investorene i listefilene siste VIKTIGE_DAGER
    dager til og med meldingsdatoen.
  - Største kjøpere/selgere: netto per investor siste STORSTE_DAGER dager,
    uten nominee-, depot- og meglerkonti.
"""
from __future__ import annotations

import datetime as dt
from zoneinfo import ZoneInfo

import handler_data as hd
from handler_data_beste import er_nominee

VIKTIGE_DAGER = 7
STORSTE_DAGER = 30
TOPP = 3
LISTER = ("Viktige.csv", "Beste.csv")
_OSLO = ZoneInfo("Europe/Oslo")


def meldingsdato(published_at: str) -> dt.date | None:
    """Børsmeldingens dato i norsk tid."""
    try:
        when = dt.datetime.fromisoformat(str(published_at or "").replace("Z", "+00:00"))
    except ValueError:
        return None
    if not when.tzinfo:
        return None
    return when.astimezone(_OSLO).date()


def aksjer_for_ticker(conn, ticker: str) -> list[tuple[str, str]]:
    rows = conn.execute(
        "SELECT isin, COALESCE(isin_name,'') AS navn FROM security "
        "WHERE UPPER(TRIM(COALESCE(ticker,''))) = ? ORDER BY isin",
        (ticker.strip().upper(),),
    ).fetchall()
    return [(str(r["isin"]), str(r["navn"])) for r in rows]


def handler_per_investor(conn, isins: list[str], fra: dt.date, til: dt.date,
                         investor_ids: list[str] | None = None) -> list[dict]:
    """Kjøp/salg/netto i MNOK per investor for aksjene i perioden.

    Kurs: gårsdagens, ellers dagens, ellers gårsdagskurs fra neste dag (samme
    regel som Handler de beste / viktige).
    """
    if not isins:
        return []
    join = ""
    if investor_ids is not None:
        if not investor_ids:
            return []
        hd.populate_temp_selected_investors(conn, investor_ids)
        join = "JOIN temp_selected_investors t ON t.investor_id = pc.investor_id"
    ph = ",".join("?" for _ in isins)
    sql = f"""
    WITH tr AS (
        SELECT pc.investor_id, pc.isin, pc.change_qty,
               COALESCE(NULLIF(pc.price_yesterday,0), NULLIF(pc.price_today,0), (
                   SELECT MAX(p.price_yesterday) FROM position_change p
                   WHERE p.isin = pc.isin AND date(p.date_today) = date(pc.date_today,'+1 day')
                     AND COALESCE(p.price_yesterday,0) > 0)) AS kurs
        FROM position_change pc {join}
        WHERE pc.isin IN ({ph}) AND pc.date_today BETWEEN ? AND ?
          AND COALESCE(pc.change_qty,0) <> 0
    )
    SELECT tr.investor_id,
           COALESCE(i.first_name,'') AS first_name,
           COALESCE(i.last_name,'') AS last_name,
           COUNT(*) AS antall_obs,
           SUM(CASE WHEN tr.change_qty > 0 THEN tr.change_qty * tr.kurs ELSE 0 END) AS kjop,
           SUM(CASE WHEN tr.change_qty < 0 THEN -tr.change_qty * tr.kurs ELSE 0 END) AS salg
    FROM tr LEFT JOIN investor i ON i.investor_id = tr.investor_id
    WHERE COALESCE(tr.kurs,0) > 0
    GROUP BY tr.investor_id, i.first_name, i.last_name
    """
    rows = conn.execute(sql, (*isins, fra.isoformat(), til.isoformat())).fetchall()
    result = []
    for r in rows:
        kjop, salg = float(r["kjop"] or 0) / 1e6, float(r["salg"] or 0) / 1e6
        result.append({
            "investor_id": str(r["investor_id"]),
            "eier": hd.clean_name(r["first_name"], r["last_name"], str(r["investor_id"])),
            "antall_obs": int(r["antall_obs"] or 0),
            "kjop_mnok": round(kjop, 2),
            "salg_mnok": round(salg, 2),
            "netto_mnok": round(kjop - salg, 2),
        })
    return result


def siste_handelsdato(conn) -> str:
    row = conn.execute("SELECT MAX(date_today) FROM position_change").fetchone()
    return str(row[0] or "")[:10] if row else ""


def handler_for_melding(conn, ticker: str, dato: dt.date,
                        liste_ids: dict[str, list[str]]) -> dict:
    """Oppsummering for én børsmelding. liste_ids: {listenavn: investor_ids}."""
    aksjer = aksjer_for_ticker(conn, ticker)
    svar = {"ticker": ticker.upper(), "til": dato.isoformat(), "notert": bool(aksjer),
            "viktige_dager": VIKTIGE_DAGER, "storste_dager": STORSTE_DAGER,
            "viktige": [], "kjopere": [], "selgere": []}
    if not aksjer:
        return svar
    isins = [isin for isin, _ in aksjer]
    svar["navn"] = aksjer[0][1]
    svar["siste_data"] = siste_handelsdato(conn)

    fra_viktige = dato - dt.timedelta(days=VIKTIGE_DAGER - 1)
    sett: dict[str, dict] = {}
    for liste, ids in liste_ids.items():
        for rad in handler_per_investor(conn, isins, fra_viktige, dato, ids):
            # Står investoren i flere lister, vises den én gang med første liste.
            sett.setdefault(rad["investor_id"], {**rad, "liste": liste.rsplit(".", 1)[0]})
    svar["viktige"] = sorted(sett.values(), key=lambda r: -abs(r["netto_mnok"]))

    fra_storste = dato - dt.timedelta(days=STORSTE_DAGER - 1)
    alle = [r for r in handler_per_investor(conn, isins, fra_storste, dato)
            if not er_nominee(r["eier"])]
    svar["kjopere"] = sorted((r for r in alle if r["netto_mnok"] > 0),
                             key=lambda r: -r["netto_mnok"])[:TOPP]
    svar["selgere"] = sorted((r for r in alle if r["netto_mnok"] < 0),
                             key=lambda r: r["netto_mnok"])[:TOPP]
    return svar
