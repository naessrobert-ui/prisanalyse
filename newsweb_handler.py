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


def _mnok(kjop, salg) -> dict:
    kjop, salg = float(kjop or 0) / 1e6, float(salg or 0) / 1e6
    return {"kjop_mnok": round(kjop, 2), "salg_mnok": round(salg, 2),
            "netto_mnok": round(kjop - salg, 2)}


def handler_per_investor(conn, isins: list[str], fra: dt.date, til: dt.date,
                         fra_kort: dt.date) -> list[dict]:
    """Kjøp/salg/netto i MNOK per investor for aksjene, for hele perioden og
    (under "kort") fra fra_kort, i én runde mot databasen.

    Kurs: sluttkurs på handelsdagen, ellers dagen før, ellers gårsdagskurs fra neste dag (samme
    regel som Handler de beste / viktige). Neste-dags-kursene hentes bare for
    aksjene og datoene i perioden, så alt går på indeksen (isin, date_today).
    """
    if not isins:
        return []
    ph = ",".join("?" for _ in isins)
    sql = f"""
    WITH tr AS (
        SELECT investor_id, isin, date(date_today) AS d, change_qty,
               COALESCE(NULLIF(price_today,0), NULLIF(price_yesterday,0)) AS kurs0
        FROM position_change
        WHERE isin IN ({ph}) AND date_today BETWEEN ? AND ?
          AND COALESCE(change_qty,0) <> 0
    ),
    neste AS (
        SELECT isin, date(date_today) AS d, MAX(price_yesterday) AS p
        FROM position_change
        WHERE isin IN ({ph}) AND date_today >= ? AND date_today < ?
          AND price_yesterday > 0
        GROUP BY isin, date(date_today)
    ),
    priset AS (
        SELECT tr.investor_id, tr.d, tr.change_qty, COALESCE(tr.kurs0, n.p) AS kurs
        FROM tr LEFT JOIN neste n ON n.isin = tr.isin AND n.d = date(tr.d,'+1 day')
    )
    SELECT x.investor_id,
           COALESCE(i.first_name,'') AS first_name,
           COALESCE(i.last_name,'') AS last_name,
           COUNT(*) AS antall_obs,
           SUM(CASE WHEN x.change_qty > 0 THEN x.change_qty * x.kurs ELSE 0 END) AS kjop,
           SUM(CASE WHEN x.change_qty < 0 THEN -x.change_qty * x.kurs ELSE 0 END) AS salg,
           SUM(CASE WHEN x.d >= ? THEN 1 ELSE 0 END) AS antall_obs_kort,
           SUM(CASE WHEN x.d >= ? AND x.change_qty > 0 THEN x.change_qty * x.kurs ELSE 0 END) AS kjop_kort,
           SUM(CASE WHEN x.d >= ? AND x.change_qty < 0 THEN -x.change_qty * x.kurs ELSE 0 END) AS salg_kort
    FROM priset x LEFT JOIN investor i ON i.investor_id = x.investor_id
    WHERE COALESCE(x.kurs,0) > 0
    GROUP BY x.investor_id, i.first_name, i.last_name
    """
    kort = fra_kort.isoformat()
    params = (*isins, fra.isoformat(), til.isoformat(),
              *isins, (fra + dt.timedelta(days=1)).isoformat(),
              (til + dt.timedelta(days=2)).isoformat(),
              kort, kort, kort)
    result = []
    for r in conn.execute(sql, params).fetchall():
        result.append({
            "investor_id": str(r["investor_id"]),
            "eier": hd.clean_name(r["first_name"], r["last_name"], str(r["investor_id"])),
            "antall_obs": int(r["antall_obs"] or 0),
            **_mnok(r["kjop"], r["salg"]),
            "kort": {"antall_obs": int(r["antall_obs_kort"] or 0),
                     **_mnok(r["kjop_kort"], r["salg_kort"])},
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
    fra_storste = dato - dt.timedelta(days=STORSTE_DAGER - 1)
    rader = handler_per_investor(conn, isins, fra_storste, dato, fra_viktige)

    liste_for: dict[str, str] = {}
    for liste, ids in liste_ids.items():
        for ident in ids:
            # Står investoren i flere lister, vises den én gang med første liste.
            liste_for.setdefault(str(ident), liste.rsplit(".", 1)[0])
    svar["viktige"] = sorted(
        ({"investor_id": r["investor_id"], "eier": r["eier"], **r["kort"],
          "liste": liste_for[r["investor_id"]]}
         for r in rader if r["investor_id"] in liste_for and r["kort"]["antall_obs"]),
        key=lambda r: -abs(r["netto_mnok"]))

    alle = [{k: v for k, v in r.items() if k != "kort"}
            for r in rader if not er_nominee(r["eier"])]
    svar["kjopere"] = sorted((r for r in alle if r["netto_mnok"] > 0),
                             key=lambda r: -r["netto_mnok"])[:TOPP]
    svar["selgere"] = sorted((r for r in alle if r["netto_mnok"] < 0),
                             key=lambda r: r["netto_mnok"])[:TOPP]
    return svar
