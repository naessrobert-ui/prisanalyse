# -*- coding: utf-8 -*-

import os

import boto3
import psycopg
from psycopg import sql
from flask import Blueprint, render_template, request

from regnskap_routes import (
    build_urls,
    lookup_orgnr,
    lookup_orgnr_brreg,
    lookup_proff_url_html,
    make_session,
    proff_resolve_regnskap_url,
)

aksjonaer_bp = Blueprint("aksjonaer", __name__)
SHAREHOLDER_TABLE = "shareholder_orgnr_import"


def _find_first_column(columns: set[str], candidates: list[str]) -> str | None:
    for candidate in candidates:
        if candidate in columns:
            return candidate
    return None


def _qualified_column(column_name: str | None, fallback_literal: str = "NULL") -> sql.Composable:
    if not column_name:
        return sql.SQL(fallback_literal)
    return sql.Identifier(column_name)


def _qualified_table(table_schema: str | None, table_name: str) -> sql.Composable:
    if table_schema:
        return sql.SQL("{}.{}").format(sql.Identifier(table_schema), sql.Identifier(table_name))
    return sql.Identifier(table_name)


def _load_table_columns(conn: psycopg.Connection, table_name: str) -> tuple[str | None, set[str]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT table_schema, column_name
            FROM information_schema.columns
            WHERE table_name = %s
              AND table_schema NOT IN ('information_schema', 'pg_catalog')
            ORDER BY CASE WHEN table_schema = 'public' THEN 0 ELSE 1 END, table_schema
            """,
            (table_name,),
        )
        rows = cur.fetchall()
        if not rows:
            return None, set()
        selected_schema = rows[0][0]
        selected_columns = {row[1] for row in rows if row[0] == selected_schema}
        return selected_schema, selected_columns


def _connect_regnskap_db(debug_info: dict[str, object] | None = None) -> psycopg.Connection | None:
    direct_url = (os.getenv("REGNSKAP_DATABASE_URL") or os.getenv("DATABASE_URL_REGNSKAP") or "").strip()
    if direct_url:
        if debug_info is not None:
            debug_info["internal_regnskap_connection"] = "direct_url"
        try:
            return psycopg.connect(direct_url)
        except Exception as exc:
            if debug_info is not None:
                debug_info["internal_regnskap_error"] = f"direct_url_connect_failed: {exc}"
            return None

    host = (os.getenv("RDS_HOST") or "").strip()
    if not host:
        if debug_info is not None:
            debug_info["internal_regnskap_connection"] = "missing REGNSKAP_DATABASE_URL/DATABASE_URL_REGNSKAP/RDS_HOST"
        return None

    port = int((os.getenv("RDS_PORT") or "5432").strip())
    dbname = (os.getenv("RDS_DB") or "postgres").strip()
    user = (os.getenv("RDS_USER") or "postgres").strip()
    region = (os.getenv("AWS_REGION") or os.getenv("REGION") or "eu-north-1").strip()
    if debug_info is not None:
        debug_info["internal_regnskap_connection"] = "rds_iam"
        debug_info["internal_regnskap_host"] = host
        debug_info["internal_regnskap_db"] = dbname
        debug_info["internal_regnskap_user"] = user
        debug_info["internal_regnskap_region"] = region

    try:
        token = boto3.client("rds", region_name=region).generate_db_auth_token(
            DBHostname=host,
            Port=port,
            DBUsername=user,
        )
        return psycopg.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=token,
            sslmode="require",
        )
    except Exception as exc:
        if debug_info is not None:
            debug_info["internal_regnskap_error"] = f"rds_iam_connect_failed: {exc}"
        return None


def _fetch_regnskap_batch_from_internal_db(
    orgnrs_norm: list[str],
    debug_info: dict[str, object] | None = None,
) -> dict[str, dict[str, object]]:
    if not orgnrs_norm:
        if debug_info is not None:
            debug_info["internal_regnskap_status"] = "no_orgnrs"
        return {}
    conn = _connect_regnskap_db(debug_info=debug_info)
    if not conn:
        if debug_info is not None and "internal_regnskap_status" not in debug_info:
            debug_info["internal_regnskap_status"] = "no_connection"
        return {}
    try:
        schema, cols = _load_table_columns(conn, "regnskap_siste")
        if debug_info is not None:
            debug_info["internal_regnskap_schema"] = schema
            debug_info["internal_regnskap_columns"] = sorted(cols)
        orgnr_col = _find_first_column(cols, ["orgnr"])
        profit_col = _find_first_column(cols, ["aarsresultat", "net_profit", "resultat_etter_skatt"])
        equity_col = _find_first_column(cols, ["sum_egenkapital", "equity", "egenkapital"])
        if not schema or not orgnr_col or (not profit_col and not equity_col):
            if debug_info is not None:
                debug_info["internal_regnskap_status"] = "missing_table_or_columns"
            return {}
        table_ref = _qualified_table(schema, "regnskap_siste")
        query = sql.SQL(
            """
            SELECT
                {orgnr}::text AS orgnr,
                regexp_replace({orgnr}::text, '\D', '', 'g') AS orgnr_norm,
                {profit} AS aarsresultat,
                {equity} AS sum_egenkapital
            FROM {table}
            WHERE regexp_replace({orgnr}::text, '\D', '', 'g') = ANY(%s)
            """
        ).format(
            orgnr=sql.Identifier(orgnr_col),
            profit=_qualified_column(profit_col),
            equity=_qualified_column(equity_col),
            table=table_ref,
        )
        with conn.cursor() as cur:
            cur.execute(query, (orgnrs_norm,))
            mapped = {
                str(orgnr_norm): {
                    "orgnr_raw": orgnr,
                    "orgnr_type": "internal_regnskap_db",
                    "aarsresultat": aarsresultat,
                    "sum_egenkapital": sum_egenkapital,
                }
                for orgnr, orgnr_norm, aarsresultat, sum_egenkapital in cur.fetchall()
            }
            if debug_info is not None:
                debug_info["internal_regnskap_status"] = "ok"
                debug_info["internal_regnskap_rows"] = len(mapped)
            return mapped
    except Exception as exc:
        if debug_info is not None:
            debug_info["internal_regnskap_status"] = f"query_failed: {exc}"
        return {}
    finally:
        conn.close()


def connect_db():
    """
    Bruker IAM-token hvis DB_IAM_AUTH=1.
    Ellers bruker den SHAREHOLDER_DATABASE_URL, med DATABASE_URL som fallback.
    """
    if os.getenv("DB_IAM_AUTH", "").lower() in ("1", "true", "yes"):
        host = os.environ["DBHOST"]
        port = int(os.getenv("DBPORT", "5432"))
        dbname = os.getenv("DBNAME", "postgres")
        user = os.getenv("DBUSER", "postgres")
        region = os.getenv("AWS_REGION") or os.getenv("REGION", "eu-north-1")

        token = boto3.client("rds", region_name=region).generate_db_auth_token(
            DBHostname=host,
            Port=port,
            DBUsername=user,
        )

        return psycopg.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=token,
            sslmode="require",
        )

    database_url = os.getenv("SHAREHOLDER_DATABASE_URL") or os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError(
            "SHAREHOLDER_DATABASE_URL/DATABASE_URL mangler, "
            "eller sett DB_IAM_AUTH=1 med DBHOST/DBUSER."
        )

    return psycopg.connect(database_url)


@aksjonaer_bp.route("/aksjonaer")
def aksjonaer_sok():
    q = (request.args.get("q") or "").strip()
    mode = (request.args.get("mode") or "person").strip().lower()
    if mode not in {"person", "company", "combined"}:
        mode = "person"
    debug_enabled = (request.args.get("debug") or "").strip().lower() in {"1", "true", "yes", "on"}
    selected_person = (request.args.get("person") or "").strip()
    selected_identifier = (request.args.get("pid") or "").strip()
    selected_postal_place = (request.args.get("pplace") or "").strip()
    selected_company = (request.args.get("company") or "").strip()
    selected_company_orgnr = (request.args.get("corgnr") or "").strip()
    persons = []
    companies = []
    rows = []
    owners = []
    person_totals = None
    debug_info: dict[str, object] = {"enabled": debug_enabled}
    error = None

    if q:
        if len(q) < 2:
            error = "Skriv minst 2 tegn."
        else:
            try:
                with connect_db() as conn:
                    shareholder_schema, columns = _load_table_columns(conn, SHAREHOLDER_TABLE)
                    if debug_enabled:
                        debug_info["shareholder_schema"] = shareholder_schema
                        debug_info["shareholder_columns"] = sorted(columns)

                    if not columns:
                        raise RuntimeError(f"Fant ingen kolonner i tabellen {SHAREHOLDER_TABLE}.")
                    shareholder_table_ref = _qualified_table(shareholder_schema, SHAREHOLDER_TABLE)

                    shareholder_col = _find_first_column(columns, ["shareholder_name"])
                    orgnr_col = _find_first_column(columns, ["orgnr"])
                    company_col = _find_first_column(columns, ["selskap", "company_name"])
                    share_class_col = _find_first_column(columns, ["aksjeklasse", "share_class"])
                    shares_col = _find_first_column(columns, ["shares_count", "shares_owned"])
                    company_total_col = _find_first_column(columns, ["company_shares_count", "company_total_shares"])
                    snapshot_col = _find_first_column(columns, ["snapshot_date"])
                    identifier_col = _find_first_column(
                        columns,
                        ["shareholder_identifier", "fodselsaar_orgnr", "shareholder_birth_year_or_orgnr"],
                    )
                    postal_col = _find_first_column(columns, ["postal_place", "postnr_sted", "postcode_place"])
                    country_col = _find_first_column(columns, ["country_code", "landkode"])

                    if not shareholder_col:
                        raise RuntimeError("Kolonnen shareholder_name mangler i aksjonærtabellen.")

                    with conn.cursor() as cur:
                        if mode in {"person", "combined"}:
                            person_query = sql.SQL(
                                """
                                SELECT
                                    {shareholder} AS shareholder_name,
                                    {identifier} AS shareholder_identifier,
                                    {postal} AS postal_place,
                                    {country} AS country_code,
                                    COUNT(DISTINCT {orgnr}) AS company_count,
                                    SUM(COALESCE({shares}, 0)) AS total_shares,
                                    MAX({snapshot}) AS latest_snapshot_date
                                FROM {table}
                                WHERE {shareholder} ILIKE %s
                                GROUP BY 1, 2, 3, 4
                                ORDER BY {shareholder}, total_shares DESC NULLS LAST
                                LIMIT 200;
                                """
                            ).format(
                                shareholder=sql.Identifier(shareholder_col),
                                identifier=_qualified_column(identifier_col),
                                postal=_qualified_column(postal_col),
                                country=_qualified_column(country_col),
                                orgnr=_qualified_column(orgnr_col),
                                shares=_qualified_column(shares_col, fallback_literal="0"),
                                snapshot=_qualified_column(snapshot_col),
                                table=shareholder_table_ref,
                            )
                            cur.execute(person_query, (f"%{q}%",))
                            person_columns = [desc.name for desc in cur.description]
                            persons = [dict(zip(person_columns, row)) for row in cur.fetchall()]

                        if mode in {"company", "combined"}:
                            if not company_col:
                                raise RuntimeError("Kolonnen selskap/company_name mangler i aksjonærtabellen.")
                            company_query = sql.SQL(
                                """
                                SELECT
                                    {company} AS selskap,
                                    {orgnr} AS orgnr,
                                    COUNT(DISTINCT {shareholder}) AS owner_count,
                                    SUM(COALESCE({shares}, 0)) AS total_shares,
                                    MAX({snapshot}) AS latest_snapshot_date
                                FROM {table}
                                WHERE {company} ILIKE %s OR {orgnr}::text ILIKE %s
                                GROUP BY 1, 2
                                ORDER BY total_shares DESC NULLS LAST, {company}
                                LIMIT 200;
                                """
                            ).format(
                                company=sql.Identifier(company_col),
                                orgnr=_qualified_column(orgnr_col),
                                shareholder=sql.Identifier(shareholder_col),
                                shares=_qualified_column(shares_col, fallback_literal="0"),
                                snapshot=_qualified_column(snapshot_col),
                                table=shareholder_table_ref,
                            )
                            like_q = f"%{q}%"
                            cur.execute(company_query, (like_q, like_q))
                            company_columns = [desc.name for desc in cur.description]
                            companies = [dict(zip(company_columns, row)) for row in cur.fetchall()]

                        if selected_person:
                            filters = [sql.SQL("{shareholder} = %s").format(shareholder=sql.Identifier(shareholder_col))]
                            params = [selected_person]

                            if identifier_col and selected_identifier:
                                filters.append(
                                    sql.SQL("COALESCE({identifier}::text, '') = %s").format(
                                        identifier=sql.Identifier(identifier_col)
                                    )
                                )
                                params.append(selected_identifier)

                            if postal_col and selected_postal_place:
                                filters.append(
                                    sql.SQL("COALESCE({postal}::text, '') = %s").format(postal=sql.Identifier(postal_col))
                                )
                                params.append(selected_postal_place)

                            holdings_query = sql.SQL(
                                """
                                SELECT
                                    {shareholder} AS shareholder_name,
                                    {identifier} AS shareholder_identifier,
                                    {postal} AS postal_place,
                                    {country} AS country_code,
                                    {orgnr} AS orgnr,
                                    {company} AS selskap,
                                    {share_class} AS aksjeklasse,
                                    {shares} AS shares_count,
                                    {company_total} AS company_shares_count,
                                    CASE
                                        WHEN {company_total} IS NULL
                                          OR {company_total} = 0
                                          OR {shares} IS NULL
                                        THEN NULL
                                        ELSE ROUND(({shares}::numeric / {company_total}::numeric) * 100, 2)
                                    END AS ownership_pct,
                                    {snapshot} AS snapshot_date
                                FROM {table}
                                WHERE {where_clause}
                                ORDER BY {snapshot} DESC NULLS LAST, {shares} DESC NULLS LAST
                                LIMIT 500;
                                """
                            ).format(
                                shareholder=sql.Identifier(shareholder_col),
                                identifier=_qualified_column(identifier_col),
                                postal=_qualified_column(postal_col),
                                country=_qualified_column(country_col),
                                orgnr=_qualified_column(orgnr_col),
                                company=_qualified_column(company_col),
                                share_class=_qualified_column(share_class_col),
                                shares=_qualified_column(shares_col, fallback_literal="NULL"),
                                company_total=_qualified_column(company_total_col, fallback_literal="NULL"),
                                snapshot=_qualified_column(snapshot_col),
                                table=shareholder_table_ref,
                                where_clause=sql.SQL(" AND ").join(filters),
                            )
                            cur.execute(holdings_query, params)
                            holding_columns = [desc.name for desc in cur.description]
                            rows = [dict(zip(holding_columns, row)) for row in cur.fetchall()]

                            regnskap_schema, regnskap_columns = _load_table_columns(conn, "regnskap_siste")
                            if debug_enabled:
                                debug_info["regnskap_schema"] = regnskap_schema
                                debug_info["regnskap_columns"] = sorted(regnskap_columns)
                            regnskap_orgnr_col = _find_first_column(regnskap_columns, ["orgnr"])
                            regnskap_profit_col = _find_first_column(
                                regnskap_columns,
                                ["aarsresultat", "net_profit", "resultat_etter_skatt"],
                            )
                            regnskap_equity_col = _find_first_column(
                                regnskap_columns,
                                ["sum_egenkapital", "equity", "egenkapital"],
                            )
                            regnskap_map: dict[str, dict[str, object]] = {}

                            if rows and regnskap_columns and regnskap_orgnr_col and (regnskap_profit_col or regnskap_equity_col):
                                selected_orgnrs = sorted(
                                    {
                                        "".join(ch for ch in str(r.get("orgnr") or "") if ch.isdigit())
                                        for r in rows
                                        if r.get("orgnr")
                                    }
                                )
                                if debug_enabled:
                                    debug_info["selected_orgnrs_norm"] = selected_orgnrs
                                if selected_orgnrs:
                                    regnskap_table_ref = _qualified_table(regnskap_schema, "regnskap_siste")
                                    regnskap_query = sql.SQL(
                                        """
                                        SELECT
                                            {orgnr}::text AS orgnr,
                                            regexp_replace({orgnr}::text, '\D', '', 'g') AS orgnr_norm,
                                            pg_typeof({orgnr})::text AS orgnr_type,
                                            {profit} AS aarsresultat,
                                            {equity} AS sum_egenkapital
                                        FROM {table}
                                        WHERE regexp_replace({orgnr}::text, '\D', '', 'g') = ANY(%s)
                                        """
                                    ).format(
                                        orgnr=sql.Identifier(regnskap_orgnr_col),
                                        profit=_qualified_column(regnskap_profit_col),
                                        equity=_qualified_column(regnskap_equity_col),
                                        table=regnskap_table_ref,
                                    )
                                    cur.execute(regnskap_query, (selected_orgnrs,))
                                    regnskap_map = {
                                        str(orgnr_norm): {
                                            "orgnr_raw": orgnr,
                                            "orgnr_type": orgnr_type,
                                            "aarsresultat": aarsresultat,
                                            "sum_egenkapital": sum_egenkapital,
                                        }
                                        for orgnr, orgnr_norm, orgnr_type, aarsresultat, sum_egenkapital in cur.fetchall()
                                    }
                                    if debug_enabled:
                                        debug_info["regnskap_db_hits"] = len(regnskap_map)

                                    total_profit_share = 0.0
                                    total_equity_share = 0.0
                                    profit_count = 0
                                    equity_count = 0

                                    for row in rows:
                                        org_raw = str(row.get("orgnr") or "")
                                        org = "".join(ch for ch in org_raw if ch.isdigit())
                                        ownership_pct = row.get("ownership_pct")
                                        regnskap = regnskap_map.get(org) if org else None

                                        owner_profit_share = None
                                        owner_equity_share = None
                                        if regnskap and ownership_pct is not None:
                                            fraction = float(ownership_pct) / 100.0
                                            if regnskap.get("aarsresultat") is not None:
                                                owner_profit_share = float(regnskap["aarsresultat"]) * fraction
                                                total_profit_share += owner_profit_share
                                                profit_count += 1
                                            if regnskap.get("sum_egenkapital") is not None:
                                                owner_equity_share = float(regnskap["sum_egenkapital"]) * fraction
                                                total_equity_share += owner_equity_share
                                                equity_count += 1

                                        row["owner_profit_share"] = owner_profit_share
                                        row["owner_equity_share"] = owner_equity_share
                                        row["company_aarsresultat"] = regnskap.get("aarsresultat") if regnskap else None
                                        row["company_sum_egenkapital"] = regnskap.get("sum_egenkapital") if regnskap else None
                                        row["regnskap_orgnr_raw"] = regnskap.get("orgnr_raw") if regnskap else None
                                        row["regnskap_orgnr_norm"] = org if regnskap else None

                                    missing_orgnrs = []
                                    for row in rows:
                                        if row.get("company_aarsresultat") is None and row.get("company_sum_egenkapital") is None:
                                            normalized = "".join(ch for ch in str(row.get("orgnr") or "") if ch.isdigit())
                                            if normalized:
                                                missing_orgnrs.append(normalized)

                                    for orgnr in sorted(set(missing_orgnrs))[:25]:
                                        try:
                                            api_url = url_for("regnskap.regnskap_api_search", _external=True)
                                            resp = requests.get(
                                                api_url,
                                                params={"orgnr": orgnr},
                                                timeout=6,
                                                verify=False,
                                            )
                                            if not resp.ok:
                                                continue
                                            payload = resp.json() or {}
                                            results = payload.get("results") or []
                                            if not results:
                                                continue
                                            first = results[0] or {}
                                            regnskap_map[orgnr] = {
                                                "orgnr_raw": first.get("orgnr") or orgnr,
                                                "orgnr_type": "api_fallback",
                                                "aarsresultat": first.get("net_profit"),
                                                "sum_egenkapital": first.get("equity"),
                                            }
                                        except Exception:
                                            continue
                                    if debug_enabled:
                                        debug_info["missing_orgnrs_before_api"] = sorted(set(missing_orgnrs))
                                        debug_info["regnskap_hits_after_api"] = len(regnskap_map)

                                    if missing_orgnrs:
                                        total_profit_share = 0.0
                                        total_equity_share = 0.0
                                        profit_count = 0
                                        equity_count = 0

                                        for row in rows:
                                            org = "".join(ch for ch in str(row.get("orgnr") or "") if ch.isdigit())
                                            regnskap = regnskap_map.get(org) if org else None
                                            ownership_pct = row.get("ownership_pct")
                                            owner_profit_share = None
                                            owner_equity_share = None
                                            if regnskap and ownership_pct is not None:
                                                fraction = float(ownership_pct) / 100.0
                                                if regnskap.get("aarsresultat") is not None:
                                                    owner_profit_share = float(regnskap["aarsresultat"]) * fraction
                                                    total_profit_share += owner_profit_share
                                                    profit_count += 1
                                                if regnskap.get("sum_egenkapital") is not None:
                                                    owner_equity_share = float(regnskap["sum_egenkapital"]) * fraction
                                                    total_equity_share += owner_equity_share
                                                    equity_count += 1
                                            row["owner_profit_share"] = owner_profit_share
                                            row["owner_equity_share"] = owner_equity_share
                                            row["company_aarsresultat"] = regnskap.get("aarsresultat") if regnskap else None
                                            row["company_sum_egenkapital"] = regnskap.get("sum_egenkapital") if regnskap else None
                                            row["regnskap_orgnr_raw"] = regnskap.get("orgnr_raw") if regnskap else None
                                            row["regnskap_orgnr_norm"] = org if regnskap else None
                                            row["regnskap_orgnr_type"] = regnskap.get("orgnr_type") if regnskap else None

                                        person_totals = {
                                            "sum_owner_profit_share": total_profit_share if profit_count else None,
                                            "sum_owner_equity_share": total_equity_share if equity_count else None,
                                            "companies_with_profit_data": profit_count,
                                            "companies_with_equity_data": equity_count,
                                            "companies_total": len(rows),
                                        }

                                    person_totals = {
                                        "sum_owner_profit_share": total_profit_share if profit_count else None,
                                        "sum_owner_equity_share": total_equity_share if equity_count else None,
                                        "companies_with_profit_data": profit_count,
                                        "companies_with_equity_data": equity_count,
                                        "companies_total": len(rows),
                                    }
                            elif debug_enabled:
                                debug_info["regnskap_lookup_status"] = "Ingen regnskapstabell/kolonner tilgjengelig i denne DB-tilkoblingen."

                            if rows and not regnskap_map:
                                missing_orgnrs = sorted(
                                    {
                                        "".join(ch for ch in str(row.get("orgnr") or "") if ch.isdigit())
                                        for row in rows
                                        if row.get("orgnr")
                                    }
                                )
                                regnskap_map.update(_fetch_regnskap_batch_from_internal_db(missing_orgnrs, debug_info))
                                if debug_enabled:
                                    debug_info["missing_orgnrs_before_internal_db"] = missing_orgnrs
                                    debug_info["regnskap_hits_after_internal_db"] = len(regnskap_map)

                                total_profit_share = 0.0
                                total_equity_share = 0.0
                                profit_count = 0
                                equity_count = 0

                                for row in rows:
                                    org = "".join(ch for ch in str(row.get("orgnr") or "") if ch.isdigit())
                                    regnskap = regnskap_map.get(org) if org else None
                                    ownership_pct = row.get("ownership_pct")
                                    owner_profit_share = None
                                    owner_equity_share = None
                                    if regnskap and ownership_pct is not None:
                                        fraction = float(ownership_pct) / 100.0
                                        if regnskap.get("aarsresultat") is not None:
                                            owner_profit_share = float(regnskap["aarsresultat"]) * fraction
                                            total_profit_share += owner_profit_share
                                            profit_count += 1
                                        if regnskap.get("sum_egenkapital") is not None:
                                            owner_equity_share = float(regnskap["sum_egenkapital"]) * fraction
                                            total_equity_share += owner_equity_share
                                            equity_count += 1
                                    row["owner_profit_share"] = owner_profit_share
                                    row["owner_equity_share"] = owner_equity_share
                                    row["company_aarsresultat"] = regnskap.get("aarsresultat") if regnskap else None
                                    row["company_sum_egenkapital"] = regnskap.get("sum_egenkapital") if regnskap else None
                                    row["regnskap_orgnr_raw"] = regnskap.get("orgnr_raw") if regnskap else None
                                    row["regnskap_orgnr_norm"] = org if regnskap else None
                                    row["regnskap_orgnr_type"] = regnskap.get("orgnr_type") if regnskap else None

                                person_totals = {
                                    "sum_owner_profit_share": total_profit_share if profit_count else None,
                                    "sum_owner_equity_share": total_equity_share if equity_count else None,
                                    "companies_with_profit_data": profit_count,
                                    "companies_with_equity_data": equity_count,
                                    "companies_total": len(rows),
                                }

                        if selected_company and company_col:
                            company_filters = [sql.SQL("{company} = %s").format(company=sql.Identifier(company_col))]
                            company_params = [selected_company]
                            if orgnr_col and selected_company_orgnr:
                                company_filters.append(
                                    sql.SQL("COALESCE({orgnr}::text, '') = %s").format(orgnr=sql.Identifier(orgnr_col))
                                )
                                company_params.append(selected_company_orgnr)
                            owners_query = sql.SQL(
                                """
                                SELECT
                                    {shareholder} AS shareholder_name,
                                    {identifier} AS shareholder_identifier,
                                    {postal} AS postal_place,
                                    {country} AS country_code,
                                    {shares} AS shares_count,
                                    CASE
                                        WHEN {company_total} IS NULL
                                          OR {company_total} = 0
                                          OR {shares} IS NULL
                                        THEN NULL
                                        ELSE ROUND(({shares}::numeric / {company_total}::numeric) * 100, 2)
                                    END AS ownership_pct,
                                    {snapshot} AS snapshot_date
                                FROM {table}
                                WHERE {where_clause}
                                ORDER BY {snapshot} DESC NULLS LAST, {shares} DESC NULLS LAST
                                LIMIT 500;
                                """
                            ).format(
                                shareholder=sql.Identifier(shareholder_col),
                                identifier=_qualified_column(identifier_col),
                                postal=_qualified_column(postal_col),
                                country=_qualified_column(country_col),
                                shares=_qualified_column(shares_col, fallback_literal="NULL"),
                                company_total=_qualified_column(company_total_col, fallback_literal="NULL"),
                                snapshot=_qualified_column(snapshot_col),
                                table=shareholder_table_ref,
                                where_clause=sql.SQL(" AND ").join(company_filters),
                            )
                            cur.execute(owners_query, company_params)
                            owner_columns = [desc.name for desc in cur.description]
                            owners = [dict(zip(owner_columns, row)) for row in cur.fetchall()]

            except Exception as exc:
                error = f"Klarte ikke hente aksjonærdata: {exc}"

    return render_template(
        "aksjonaer_sok.html",
        q=q,
        mode=mode,
        persons=persons,
        companies=companies,
        rows=rows,
        owners=owners,
        selected_person=selected_person,
        selected_identifier=selected_identifier,
        selected_postal_place=selected_postal_place,
        selected_company=selected_company,
        selected_company_orgnr=selected_company_orgnr,
        person_totals=person_totals,
        debug_info=debug_info,
        error=error,
    )
