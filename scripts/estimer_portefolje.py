#!/usr/bin/env python3
"""Estimer dagens portefølje for en investor fra Handler Oslo Børs-databasen.

Bakgrunn
--------
Vi mottar to typer data om norske aksjonærer:

1. En eier-fil som gir de siste beholdningene, men **kun for de 20 største
   eierne** i hvert selskap.
2. Daglige "topchanges"-filer: når en investor en gitt dag er blant de største
   *handlerne* i en aksje, får vi frem handelen **samt hvor mange aksjer
   investoren da satt med**.

I den nye databasen (SQLite, `topchanges.db`) lastes begge kildene inn i den
samme tabellen ``position_change`` med én rad per observasjon
``(investor_id, isin, date_today)`` der ``holding_today`` er beholdningen på
det tidspunktet. Det betyr at vi ikke lenger trenger å kombinere to filkilder
manuelt slik det gamle skriptet gjorde: det beste estimatet på dagens
beholdning i en aksje er ganske enkelt **den sist observerte ``holding_today``
for (investor, isin)**.

Dette skriptet gjør nettopp det, med noen forbedringer over det gamle skriptet:

* **Ferskest vinner automatisk.** Det gamle skriptet prioriterte alltid
  topp-20-eierfilen fremfor handelsobservasjoner. Her velger vi i stedet den
  nyeste observasjonen uansett kilde — hvis investoren handlet *etter* siste
  eier-snapshot, er handelens beholdning mer oppdatert, og motsatt.
* **Salg til null håndteres.** Er siste observerte beholdning 0, regnes
  posisjonen som avviklet og utelates (kan overstyres med ``--include-zero``).
* **Åpenhet om alder.** For hver posisjon vises datoen beholdningen sist ble
  observert og antall dager siden. Gamle observasjoner er mindre pålitelige
  (investoren kan ha solgt uten at vi har sett det), og disse flagges/kan
  filtreres bort.
* **Prising.** Verdi = beholdning × sist kjente kurs. Kursen hentes fra den
  ferskeste ikke-null ``price_today``/``price_yesterday`` i databasen, med
  fallback til ``security.last_price``.

Bruk
----
    # Estimer portefølje for en investor via navnesøk
    python scripts/estimer_portefolje.py --search "Holberg Norge"

    # Via eksplisitt investor-id (kan gjentas eller kommaseparert)
    python scripts/estimer_portefolje.py --investor-id 123456 --investor-id 123457

    # Estimat per en historisk dato, skriv til CSV
    python scripts/estimer_portefolje.py --search "Salt Value" \
        --as-of 2026-06-30 --csv portefolje.csv

    # Peke direkte på en DB-fil (uten handler_data / S3)
    python scripts/estimer_portefolje.py --db-path /sti/topchanges.db --search Tycoon

Databasen finnes normalt via ``handler_data`` (miljøvariabelen
``HANDLER_LOCAL_DB_PATH``, med valgfri S3-fallback). Er ``handler_data`` ikke
tilgjengelig (f.eks. manglende pandas i miljøet), brukes ``--db-path`` eller
``HANDLER_LOCAL_DB_PATH`` direkte.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import os
import sqlite3
import sys
from dataclasses import dataclass, field
from typing import Iterable, Optional


# =========================================================
# Datamodell
# =========================================================
@dataclass
class Investor:
    investor_id: str
    name: str
    investor_type: str = ""

    @property
    def label(self) -> str:
        return f"{self.name} ({self.investor_id})"


@dataclass
class Position:
    isin: str
    ticker: str
    name: str
    shares: float
    price: Optional[float]
    price_source: str
    last_seen: str            # ferskeste observasjonsdato blant bidragskontoene
    oldest_seen: str          # eldste observasjonsdato blant bidragskontoene
    days_since: Optional[int]  # dager fra oldest_seen til referansedato (verst tilfelle)
    accounts: list[str] = field(default_factory=list)

    @property
    def value(self) -> Optional[float]:
        if self.price is None:
            return None
        return self.shares * self.price

    @property
    def value_mnok(self) -> Optional[float]:
        v = self.value
        return None if v is None else v / 1_000_000.0


@dataclass
class PortfolioEstimate:
    investors: list[Investor]
    positions: list[Position]
    db_max_date: Optional[str]
    as_of: Optional[str]
    stale_days: int

    @property
    def reference_date(self) -> Optional[str]:
        """Datoen vi måler alder mot (as_of hvis satt, ellers siste dato i DB)."""
        return self.as_of or self.db_max_date

    @property
    def total_value_mnok(self) -> float:
        return sum((p.value_mnok or 0.0) for p in self.positions)

    def stale_positions(self) -> list[Position]:
        return [
            p for p in self.positions
            if p.days_since is not None and p.days_since > self.stale_days
        ]


# =========================================================
# Hjelpere
# =========================================================
def _days_between(older: Optional[str], newer: Optional[str]) -> Optional[int]:
    if not older or not newer:
        return None
    try:
        d_old = dt.date.fromisoformat(str(older)[:10])
        d_new = dt.date.fromisoformat(str(newer)[:10])
    except ValueError:
        return None
    return (d_new - d_old).days


def get_db_max_date(conn: sqlite3.Connection) -> Optional[str]:
    row = conn.execute("SELECT MAX(date_today) FROM position_change").fetchone()
    if not row or not row[0]:
        return None
    return str(row[0])[:10]


# =========================================================
# Investor-oppslag
# =========================================================
def resolve_investors(
    conn: sqlite3.Connection,
    search: Optional[str] = None,
    investor_ids: Optional[Iterable[str]] = None,
    limit: int = 50,
) -> list[Investor]:
    """Finn investorer via navnesøk (substring) og/eller eksplisitte id-er.

    Navnesøket speiler ``handler_data.search_investors`` men returnerer også
    treff kun basert på ``investor_id`` og aggregerer ikke — kalleren bestemmer
    om treffene skal summeres.
    """
    found: dict[str, Investor] = {}

    def _clean(x: Optional[str]) -> str:
        x = (x or "").strip()
        return "" if x.lower() == "nan" else x

    def _row_to_investor(r: sqlite3.Row) -> Investor:
        name = " ".join(
            p for p in [_clean(r["first_name"]), _clean(r["last_name"])] if p
        ).strip()
        return Investor(
            investor_id=str(r["investor_id"]).strip(),
            name=name or f"(id {r['investor_id']})",
            investor_type=_clean(r["investor_type"]),
        )

    if investor_ids:
        ids = [str(i).strip() for i in investor_ids if str(i).strip()]
        if ids:
            placeholders = ",".join("?" for _ in ids)
            sql = f"""
                SELECT investor_id, investor_type, first_name, last_name
                FROM investor
                WHERE investor_id IN ({placeholders})
            """
            for r in conn.execute(sql, ids).fetchall():
                inv = _row_to_investor(r)
                found[inv.investor_id] = inv
            # Ta med id-er som ikke finnes i investor-tabellen (kan likevel ha
            # rader i position_change) slik at de ikke stilltiende forsvinner.
            for i in ids:
                found.setdefault(i, Investor(investor_id=i, name=f"(id {i})"))

    if search and len(search.strip()) >= 2:
        like = f"%{search.upper().strip()}%"
        sql = """
            SELECT investor_id, investor_type, first_name, last_name
            FROM investor
            WHERE UPPER(COALESCE(investor_id,'')) LIKE ?
               OR UPPER(COALESCE(first_name,'')) LIKE ?
               OR UPPER(COALESCE(last_name,'')) LIKE ?
               OR UPPER(COALESCE(first_name,'') || ' ' || COALESCE(last_name,'')) LIKE ?
            ORDER BY COALESCE(last_name,''), COALESCE(first_name,'')
            LIMIT ?
        """
        for r in conn.execute(sql, (like, like, like, like, limit)).fetchall():
            inv = _row_to_investor(r)
            found.setdefault(inv.investor_id, inv)

    return sorted(found.values(), key=lambda i: (i.name.upper(), i.investor_id))


# =========================================================
# Kurser
# =========================================================
def _latest_prices(
    conn: sqlite3.Connection, as_of: Optional[str]
) -> dict[str, tuple[float, str]]:
    """Ferskeste kjente kurs per ISIN: {isin: (kurs, kilde-dato)}."""
    date_filter = "AND date_today <= ?" if as_of else ""
    params: tuple = (as_of,) if as_of else ()
    sql = f"""
        WITH px AS (
            SELECT
                isin,
                date_today,
                CASE
                    WHEN COALESCE(price_today, 0) > 0 THEN price_today
                    ELSE price_yesterday
                END AS price,
                ROW_NUMBER() OVER (
                    PARTITION BY isin
                    ORDER BY date_today DESC
                ) AS rn
            FROM position_change
            WHERE (COALESCE(price_today, 0) > 0 OR COALESCE(price_yesterday, 0) > 0)
              {date_filter}
        )
        SELECT isin, price, date_today FROM px WHERE rn = 1
    """
    out: dict[str, tuple[float, str]] = {}
    for r in conn.execute(sql, params).fetchall():
        if r["price"] and float(r["price"]) > 0:
            out[str(r["isin"])] = (float(r["price"]), str(r["date_today"])[:10])
    return out


def _security_meta(conn: sqlite3.Connection) -> dict[str, dict]:
    cols = {r[1] for r in conn.execute("PRAGMA table_info(security)").fetchall()}
    has_last_price = "last_price" in cols
    select_last = ", last_price" if has_last_price else ""
    out: dict[str, dict] = {}
    for r in conn.execute(
        f"SELECT isin, ticker, isin_name{select_last} FROM security"
    ).fetchall():
        out[str(r["isin"])] = {
            "ticker": (r["ticker"] or "").strip() if r["ticker"] else "",
            "name": (r["isin_name"] or "").strip() if r["isin_name"] else "",
            "last_price": (
                float(r["last_price"])
                if has_last_price and r["last_price"] not in (None, "")
                else None
            ),
        }
    return out


# =========================================================
# Kjerne: estimer portefølje
# =========================================================
def estimate_portfolio(
    conn: sqlite3.Connection,
    investors: list[Investor],
    as_of: Optional[str] = None,
    include_zero: bool = False,
    stale_days: int = 120,
) -> PortfolioEstimate:
    """Estimer beholdning per ISIN som sist observerte ``holding_today``.

    Beholdningene summeres på tvers av alle oppgitte investor-id-er (nyttig for
    fond med flere kontoer). Sett kun én investor i ``investors`` for å unngå
    sammenblanding.
    """
    db_max_date = get_db_max_date(conn)
    ref_date = as_of or db_max_date

    ids = [inv.investor_id for inv in investors]
    if not ids:
        return PortfolioEstimate([], [], db_max_date, as_of, stale_days)

    placeholders = ",".join("?" for _ in ids)
    date_filter = "AND date_today <= ?" if as_of else ""
    params: list = list(ids) + ([as_of] if as_of else [])

    # Siste observerte beholdning per (investor_id, isin).
    sql = f"""
        WITH obs AS (
            SELECT
                isin,
                investor_id,
                date_today,
                COALESCE(holding_today, 0) AS holding,
                ROW_NUMBER() OVER (
                    PARTITION BY investor_id, isin
                    ORDER BY date_today DESC
                ) AS rn
            FROM position_change
            WHERE investor_id IN ({placeholders})
              {date_filter}
        )
        SELECT isin, investor_id, date_today AS as_of_date, holding
        FROM obs
        WHERE rn = 1
    """
    rows = conn.execute(sql, params).fetchall()

    prices = _latest_prices(conn, as_of)
    meta = _security_meta(conn)

    # Aggreger per ISIN på tvers av kontoer.
    agg: dict[str, dict] = {}
    for r in rows:
        isin = str(r["isin"])
        holding = float(r["holding"] or 0.0)
        as_of_date = str(r["as_of_date"])[:10]
        bucket = agg.setdefault(
            isin,
            {"shares": 0.0, "last_seen": as_of_date, "oldest_seen": as_of_date, "accounts": []},
        )
        bucket["shares"] += holding
        bucket["accounts"].append(str(r["investor_id"]))
        if as_of_date > bucket["last_seen"]:
            bucket["last_seen"] = as_of_date
        if as_of_date < bucket["oldest_seen"]:
            bucket["oldest_seen"] = as_of_date

    positions: list[Position] = []
    for isin, b in agg.items():
        shares = b["shares"]
        if not include_zero and shares <= 0:
            continue

        m = meta.get(isin, {})
        price: Optional[float] = None
        price_source = "ingen"
        if isin in prices:
            price = prices[isin][0]
            price_source = f"handel {prices[isin][1]}"
        elif m.get("last_price"):
            price = m["last_price"]
            price_source = "security.last_price"

        positions.append(
            Position(
                isin=isin,
                ticker=m.get("ticker", ""),
                name=m.get("name", ""),
                shares=shares,
                price=price,
                price_source=price_source,
                last_seen=b["last_seen"],
                oldest_seen=b["oldest_seen"],
                # Alder måles fra eldste bidrag (mest konservativt) til referansedato.
                days_since=_days_between(b["oldest_seen"], ref_date),
                accounts=sorted(set(b["accounts"])),
            )
        )

    # Sorter etter verdi (posisjoner uten kurs nederst), så etter antall aksjer.
    positions.sort(
        key=lambda p: (p.value_mnok if p.value_mnok is not None else -1, p.shares),
        reverse=True,
    )

    return PortfolioEstimate(
        investors=investors,
        positions=positions,
        db_max_date=db_max_date,
        as_of=as_of,
        stale_days=stale_days,
    )


# =========================================================
# Utskrift / CSV
# =========================================================
def _fmt_int(x: float) -> str:
    return f"{x:,.0f}".replace(",", " ")


def _fmt_num(x: Optional[float], decimals: int = 2) -> str:
    if x is None:
        return "-"
    return f"{x:,.{decimals}f}".replace(",", " ")


def print_estimate(est: PortfolioEstimate, top: Optional[int] = None) -> None:
    print("=" * 78)
    print("Estimert portefølje — Handler Oslo Børs")
    print("=" * 78)
    if est.investors:
        print("Investor(er):")
        for inv in est.investors:
            t = f" [{inv.investor_type}]" if inv.investor_type else ""
            print(f"  - {inv.label}{t}")
    print(f"Siste dato i DB : {est.db_max_date or '-'}")
    print(f"Estimat pr.     : {est.reference_date or '-'}"
          + ("  (as-of)" if est.as_of else "  (siste tilgjengelige)"))
    print(f"Alder-terskel   : {est.stale_days} dager")
    print("-" * 78)

    rows = est.positions if top is None else est.positions[:top]
    if not rows:
        print("Ingen posisjoner funnet for valgt(e) investor(er).")
        return

    header = (
        f"{'Ticker':<10}{'ISIN':<15}{'Antall':>15}{'Kurs':>10}"
        f"{'Verdi MNOK':>13}{'Sist sett':>12}{'Dager':>7}  Navn"
    )
    print(header)
    print("-" * len(header))
    for p in rows:
        flag = " *" if (p.days_since is not None and p.days_since > est.stale_days) else ""
        print(
            f"{p.ticker:<10}{p.isin:<15}{_fmt_int(p.shares):>15}"
            f"{_fmt_num(p.price, 3):>10}{_fmt_num(p.value_mnok, 1):>13}"
            f"{p.last_seen:>12}{('-' if p.days_since is None else str(p.days_since)):>7}"
            f"  {p.name}{flag}"
        )

    print("-" * len(header))
    priced = [p for p in est.positions if p.value_mnok is not None]
    unpriced = [p for p in est.positions if p.value_mnok is None]
    print(f"Antall posisjoner : {len(est.positions)}"
          + (f"  (vist: {len(rows)})" if top and len(rows) < len(est.positions) else ""))
    print(f"Sum verdi         : {_fmt_num(est.total_value_mnok, 1)} MNOK"
          + (f"  ({len(unpriced)} uten kurs utelatt fra sum)" if unpriced else ""))
    stale = est.stale_positions()
    if stale:
        print(f"Foreldede (*)     : {len(stale)} posisjoner eldre enn "
              f"{est.stale_days} dager — usikkert estimat.")
    print("=" * 78)


def write_csv(est: PortfolioEstimate, path: str) -> None:
    with open(path, "w", newline="", encoding="utf-8-sig") as fh:
        w = csv.writer(fh, delimiter=";")
        w.writerow([
            "ticker", "isin", "navn", "antall_aksjer", "kurs", "kurs_kilde",
            "verdi_mnok", "sist_sett", "eldste_obs", "dager_siden",
            "foreldet", "antall_kontoer", "kontoer",
        ])
        for p in est.positions:
            stale = p.days_since is not None and p.days_since > est.stale_days
            w.writerow([
                p.ticker,
                p.isin,
                p.name,
                f"{p.shares:.0f}",
                "" if p.price is None else f"{p.price:.4f}".replace(".", ","),
                p.price_source,
                "" if p.value_mnok is None else f"{p.value_mnok:.2f}".replace(".", ","),
                p.last_seen,
                p.oldest_seen,
                "" if p.days_since is None else p.days_since,
                "ja" if stale else "nei",
                len(p.accounts),
                "|".join(p.accounts),
            ])


# =========================================================
# Tilkobling
# =========================================================
def _connect(db_path: Optional[str]) -> sqlite3.Connection:
    """Koble til DB. Bruk handler_data (m/S3-fallback) hvis tilgjengelig,
    ellers direkte til --db-path / HANDLER_LOCAL_DB_PATH."""
    if not db_path:
        try:
            import handler_data  # type: ignore

            conn = handler_data.db_connect()
            return conn
        except Exception as exc:  # pragma: no cover - miljøavhengig
            db_path = os.getenv("HANDLER_LOCAL_DB_PATH")
            if not db_path:
                raise SystemExit(
                    "Fant ikke database. Kunne ikke bruke handler_data "
                    f"({exc}). Oppgi --db-path eller sett HANDLER_LOCAL_DB_PATH."
                )
    if not os.path.isfile(db_path):
        raise SystemExit(f"Database ikke funnet: {db_path}")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


# =========================================================
# CLI
# =========================================================
def _parse_investor_ids(values: Optional[list[str]]) -> list[str]:
    out: list[str] = []
    for v in values or []:
        out.extend(part.strip() for part in v.split(",") if part.strip())
    return out


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Estimer dagens portefølje for en investor fra Handler Oslo Børs-DB.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--search", help="Navnesøk (substring, case-insensitivt).")
    p.add_argument(
        "--investor-id",
        action="append",
        metavar="ID",
        help="Eksplisitt investor-id (kan gjentas eller kommaseparert).",
    )
    p.add_argument("--db-path", help="Sti til topchanges.db (overstyrer handler_data).")
    p.add_argument("--as-of", help="Estimer portefølje pr. denne datoen (YYYY-MM-DD).")
    p.add_argument(
        "--stale-days", type=int, default=120,
        help="Flagg posisjoner eldre enn N dager (default 120).",
    )
    p.add_argument(
        "--drop-stale", action="store_true",
        help="Utelat posisjoner som er eldre enn --stale-days.",
    )
    p.add_argument(
        "--include-zero", action="store_true",
        help="Ta med posisjoner der siste beholdning er 0 (avviklet).",
    )
    p.add_argument(
        "--min-value-mnok", type=float, default=None,
        help="Vis kun posisjoner med verdi >= X MNOK.",
    )
    p.add_argument("--top", type=int, default=None, help="Vis kun de N største posisjonene.")
    p.add_argument(
        "--limit-search", type=int, default=50,
        help="Maks antall investor-treff for navnesøk (default 50).",
    )
    p.add_argument("--csv", help="Skriv resultat til CSV-fil (semikolon-separert).")
    p.add_argument(
        "--list-only", action="store_true",
        help="List bare matchende investorer og avslutt (ingen estimering).",
    )
    return p


def main(argv: Optional[list[str]] = None) -> int:
    args = build_arg_parser().parse_args(argv)

    investor_ids = _parse_investor_ids(args.investor_id)
    if not args.search and not investor_ids:
        print("Oppgi minst --search eller --investor-id.", file=sys.stderr)
        return 2

    if args.as_of:
        try:
            dt.date.fromisoformat(args.as_of)
        except ValueError:
            print(f"Ugyldig --as-of dato: {args.as_of}", file=sys.stderr)
            return 2

    conn = _connect(args.db_path)
    try:
        investors = resolve_investors(
            conn,
            search=args.search,
            investor_ids=investor_ids,
            limit=args.limit_search,
        )

        if not investors:
            print("Ingen investorer matchet søket.", file=sys.stderr)
            return 1

        if args.list_only:
            print(f"Matchende investorer ({len(investors)}):")
            for inv in investors:
                t = f"  [{inv.investor_type}]" if inv.investor_type else ""
                print(f"  {inv.investor_id:<12} {inv.name}{t}")
            return 0

        if len(investors) > 1:
            print(f"Merk: {len(investors)} investor-id-er matchet og summeres "
                  "(bruk --list-only for å inspisere, eller --investor-id for å "
                  "velge presist):", file=sys.stderr)
            for inv in investors:
                print(f"  - {inv.label}", file=sys.stderr)

        est = estimate_portfolio(
            conn,
            investors,
            as_of=args.as_of,
            include_zero=args.include_zero,
            stale_days=args.stale_days,
        )

        if args.drop_stale:
            est.positions = [
                p for p in est.positions
                if not (p.days_since is not None and p.days_since > args.stale_days)
            ]
        if args.min_value_mnok is not None:
            est.positions = [
                p for p in est.positions
                if (p.value_mnok or 0.0) >= args.min_value_mnok
            ]

        print_estimate(est, top=args.top)

        if args.csv:
            write_csv(est, args.csv)
            print(f"\nSkrev {len(est.positions)} posisjoner til {args.csv}")
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
