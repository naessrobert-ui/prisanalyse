# -*- coding: utf-8 -*-

from __future__ import annotations

import os
import re

import boto3
import psycopg
from psycopg import sql
from flask import Blueprint, render_template, request


aksjonaer_bp = Blueprint("aksjonaer", __name__)
SHAREHOLDER_TABLE = "shareholder_orgnr_import"


# ---------------------------------------------------------------------------
# Generelle helpers
# ---------------------------------------------------------------------------
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


def _normalize_orgnr(value: object) -> str:
    return "".join(ch for ch in str(value or "") if ch.isdigit())


# ---------------------------------------------------------------------------
# Fritekst-/tokensøk
# ---------------------------------------------------------------------------
def _normalize_search_text(value: str) -> str:
    return (
        str(value or "")
        .lower()
        .replace("æ", "ae")
        .replace("ø", "o")
        .replace("å", "a")
    )


def _search_tokens(value: str) -> list[str]:
    normalized = _normalize_search_text(value)
    return re.findall(r"[0-9a-z]+", normalized)[:8]


def _normalized_column(column_name: str) -> sql.Composable:
    return sql.SQL(
        "replace(replace(replace(lower({column}), 'æ', 'ae'), 'ø', 'o'), 'å', 'a')"
    ).format(column=sql.Identifier(column_name))


def _build_token_search_where(column_name: str, tokens: list[str]) -> tuple[sql.Composable, list[str]]:
    """
    Bygger:
      normalized_column LIKE %token1%
      AND normalized_column LIKE %token2%

    Eksempel:
      q = "julie næss"
      tokens = ["julie", "naess"]
      matcher "JULIE HELENE HALSE NÆSS"
    """
    if not tokens:
        return sql.SQL("{column} ILIKE %s").format(column=sql.Identifier(column_name)), ["%%"]

    column_expr = _normalized_column(column_name)
    conditions = [
        sql.SQL("{column_expr} LIKE %s").format(column_expr=column_expr)
        for _ in tokens
    ]
    params = [f"%{token}%" for token in tokens]
    return sql.SQL(" AND ").join(conditions), params


def _build_company_filter_where(
    company_col: str,
    orgnr_col: str | None,
    value: str,
) -> tuple[sql.Composable, list[str]]:
    """
    Brukes når en person er valgt og man vil filtrere beholdningen på selskap.

    Eksempel:
      company_filter=ecocar
      company_filter=912078957
    """
    tokens = _search_tokens(value)
    company_where, company_params = _build_token_search_where(company_col, tokens)

    if orgnr_col:
        return (
            sql.SQL("(({company_where}) OR COALESCE({orgnr}::text, '') ILIKE %s)").format(
                company_where=company_where,
                orgnr=sql.Identifier(orgnr_col),
            ),
            [*company_params, f"%{value}%"],
        )

    return company_where, company_params


# ---------------------------------------------------------------------------
# DB-tilkoblinger
# ---------------------------------------------------------------------------
def connect_db() -> psycopg.Connection:
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


def _connect_regnskap_db(debug_info: dict[str, object] | None = None) -> psycopg.Connection | None:
    """
    Fallback-tilkobling til regnskapsdatabasen hvis regnskap_siste ikke finnes
    i samme DB-tilkobling som aksjonærdata.
    """
    direct_url = (
        os.getenv("REGNSKAP_DATABASE_URL")
        or os.getenv("DATABASE_URL_REGNSKAP")
        or ""
    ).strip()

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
            debug_info["internal_regnskap_connection"] = (
                "missing REGNSKAP_DATABASE_URL/DATABASE_URL_REGNSKAP/RDS_HOST"
            )
            debug_info["internal_regnskap_how_to_fix"] = (
                "Sett REGNSKAP_DATABASE_URL eller DATABASE_URL_REGNSKAP på web-appen, "
                "evt. RDS_HOST/RDS_PORT/RDS_DB/RDS_USER + AWS_REGION for IAM."
            )
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


# ---------------------------------------------------------------------------
# Regnskapsoppslag
# ---------------------------------------------------------------------------
def _fetch_regnskap_batch_from_conn(
    conn: psycopg.Connection,
    orgnrs_norm: list[str],
    debug_info: dict[str, object] | None = None,
) -> dict[str, dict[str, object]]:
    """
    Prøver å hente regnskap_siste fra samme DB-tilkobling som aksjonærdata.
    Dette er raskeste vei når tabellen finnes i samme database.
    """
    if not orgnrs_norm:
        return {}

    regnskap_schema, regnskap_columns = _load_table_columns(conn, "regnskap_siste")

    if debug_info is not None:
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

    if not regnskap_columns or not regnskap_orgnr_col or not (regnskap_profit_col or regnskap_equity_col):
        if debug_info is not None:
            debug_info["regnskap_lookup_status"] = (
                "Ingen regnskapstabell/kolonner tilgjengelig i denne DB-tilkoblingen."
            )
        return {}

    regnskap_table_ref = _qualified_table(regnskap_schema, "regnskap_siste")

    regnskap_query = sql.SQL(
        """
        SELECT
            {orgnr}::text AS orgnr,
            regexp_replace({orgnr}::text, '\\D', '', 'g') AS orgnr_norm,
            pg_typeof({orgnr})::text AS orgnr_type,
            {profit} AS aarsresultat,
            {equity} AS sum_egenkapital
        FROM {table}
        WHERE regexp_replace({orgnr}::text, '\\D', '', 'g') = ANY(%s)
        """
    ).format(
        orgnr=sql.Identifier(regnskap_orgnr_col),
        profit=_qualified_column(regnskap_profit_col),
        equity=_qualified_column(regnskap_equity_col),
        table=regnskap_table_ref,
    )

    with conn.cursor() as cur:
        cur.execute(regnskap_query, (orgnrs_norm,))
        mapped = {
            str(orgnr_norm): {
                "orgnr_raw": orgnr,
                "orgnr_type": orgnr_type,
                "aarsresultat": aarsresultat,
                "sum_egenkapital": sum_egenkapital,
            }
            for orgnr, orgnr_norm, orgnr_type, aarsresultat, sum_egenkapital in cur.fetchall()
        }

    if debug_info is not None:
        debug_info["regnskap_db_hits"] = len(mapped)

    return mapped


def _fetch_regnskap_batch_from_internal_db(
    orgnrs_norm: list[str],
    debug_info: dict[str, object] | None = None,
) -> dict[str, dict[str, object]]:
    """
    Fallback hvis aksjonær-DB ikke har regnskap_siste.

    Først prøver vi Fastapi_Backend.fetch_all, siden den allerede har fungerende
    RDS/IAM-oppsett i appen. Hvis det feiler, prøver vi egen psycopg-tilkobling.
    """
    if not orgnrs_norm:
        if debug_info is not None:
            debug_info["internal_regnskap_status"] = "no_orgnrs"
        return {}

    if debug_info is not None:
        debug_info["internal_regnskap_requested_orgnrs"] = orgnrs_norm
        debug_info["internal_regnskap_requested_count"] = len(orgnrs_norm)

    try:
        from Fastapi_Backend import fetch_all as _fb_fetch_all

        rows = _fb_fetch_all(
            """
            SELECT
                orgnr::text AS orgnr,
                regexp_replace(orgnr::text, '\\D', '', 'g') AS orgnr_norm,
                aarsresultat,
                sum_egenkapital
            FROM regnskap_siste
            WHERE regexp_replace(orgnr::text, '\\D', '', 'g') = ANY(%s)
            """,
            [list(orgnrs_norm)],
        )

        mapped = {
            str(row["orgnr_norm"]): {
                "orgnr_raw": row["orgnr"],
                "orgnr_type": "fastapi_backend_db",
                "aarsresultat": row.get("aarsresultat"),
                "sum_egenkapital": row.get("sum_egenkapital"),
            }
            for row in rows
        }

        if debug_info is not None:
            debug_info["internal_regnskap_status"] = "ok"
            debug_info["internal_regnskap_connection"] = "fastapi_backend"
            debug_info["internal_regnskap_rows"] = len(mapped)

        return mapped

    except Exception as exc:
        if debug_info is not None:
            debug_info["internal_regnskap_fastapi_error"] = str(exc)

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

        if not schema or not orgnr_col or not (profit_col or equity_col):
            if debug_info is not None:
                debug_info["internal_regnskap_status"] = "missing_table_or_columns"
            return {}

        table_ref = _qualified_table(schema, "regnskap_siste")

        query = sql.SQL(
            """
            SELECT
                {orgnr}::text AS orgnr,
                regexp_replace({orgnr}::text, '\\D', '', 'g') AS orgnr_norm,
                {profit} AS aarsresultat,
                {equity} AS sum_egenkapital
            FROM {table}
            WHERE regexp_replace({orgnr}::text, '\\D', '', 'g') = ANY(%s)
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


def _apply_regnskap_to_rows(
    rows: list[dict[str, object]],
    regnskap_map: dict[str, dict[str, object]],
) -> dict[str, object]:
    total_profit_share = 0.0
    total_equity_share = 0.0
    profit_count = 0
    equity_count = 0

    for row in rows:
        org = _normalize_orgnr(row.get("orgnr"))
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

    return {
        "sum_owner_profit_share": total_profit_share if profit_count else None,
        "sum_owner_equity_share": total_equity_share if equity_count else None,
        "companies_with_profit_data": profit_count,
        "companies_with_equity_data": equity_count,
        "companies_total": len(rows),
    }


# ---------------------------------------------------------------------------
# Route
# ---------------------------------------------------------------------------
@aksjonaer_bp.route("/aksjonaer")
def aksjonaer_sok():
    q = (request.args.get("q") or "").strip()

    mode = (request.args.get("mode") or "person").strip().lower()
    if mode not in {"person", "company", "combined"}:
        mode = "person"

    debug_enabled = (request.args.get("debug") or "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }

    selected_person = (request.args.get("person") or "").strip()
    selected_identifier = (request.args.get("pid") or "").strip()
    selected_postal_place = (request.args.get("pplace") or "").strip()
    selected_company = (request.args.get("company") or "").strip()
    selected_company_orgnr = (request.args.get("corgnr") or "").strip()

    company_filter = (request.args.get("company_filter") or "").strip()
    owner_filter = (request.args.get("owner_filter") or "").strip()

    persons: list[dict[str, object]] = []
    companies: list[dict[str, object]] = []
    rows: list[dict[str, object]] = []
    owners: list[dict[str, object]] = []
    person_totals: dict[str, object] | None = None

    debug_info: dict[str, object] = {
        "enabled": debug_enabled,
        "hint": "Legg til ?debug=1 for feilsøking / troubleshooting.",
    }

    error = None

    should_query = bool(q or selected_person or selected_company)

    if should_query:
        if q and len(q) < 2:
            error = "Skriv minst 2 tegn."
        else:
            try:
                with connect_db() as conn:
                    if debug_enabled:
                        with conn.cursor() as cur:
                            cur.execute(
                                """
                                SELECT
                                    current_database(),
                                    current_user,
                                    current_schema(),
                                    inet_server_addr()::text,
                                    inet_server_port()
                                """
                            )
                            db_name, db_user, db_schema, db_host, db_port = cur.fetchone()
                            debug_info["web_db_current_database"] = db_name
                            debug_info["web_db_current_user"] = db_user
                            debug_info["web_db_current_schema"] = db_schema
                            debug_info["web_db_host"] = db_host
                            debug_info["web_db_port"] = db_port

                            cur.execute("SELECT to_regclass('public.regnskap_siste')::text")
                            debug_info["web_db_to_regclass_regnskap_siste"] = cur.fetchone()[0]

                            cur.execute("SELECT to_regclass('public.shareholder_orgnr_import')::text")
                            debug_info["web_db_to_regclass_shareholder_orgnr_import"] = cur.fetchone()[0]

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
                    company_total_col = _find_first_column(
                        columns,
                        ["company_shares_count", "company_total_shares"],
                    )
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
                        # ---------------------------------------------------
                        # Personsøk
                        # ---------------------------------------------------
                        if q and mode in {"person", "combined"}:
                            person_tokens = _search_tokens(q)
                            person_where, person_params = _build_token_search_where(
                                shareholder_col,
                                person_tokens,
                            )

                            if debug_enabled:
                                debug_info["person_search_tokens"] = person_tokens

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
                                WHERE {person_where}
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
                                person_where=person_where,
                            )

                            cur.execute(person_query, person_params)
                            person_columns = [desc.name for desc in cur.description]
                            persons = [dict(zip(person_columns, row)) for row in cur.fetchall()]

                        # ---------------------------------------------------
                        # Selskapsøk
                        # ---------------------------------------------------
                        if q and mode in {"company", "combined"}:
                            if not company_col:
                                raise RuntimeError("Kolonnen selskap/company_name mangler i aksjonærtabellen.")

                            company_tokens = _search_tokens(q)
                            company_where, company_params = _build_token_search_where(
                                company_col,
                                company_tokens,
                            )

                            if debug_enabled:
                                debug_info["company_search_tokens"] = company_tokens

                            company_query = sql.SQL(
                                """
                                SELECT
                                    {company} AS selskap,
                                    {orgnr} AS orgnr,
                                    COUNT(DISTINCT {shareholder}) AS owner_count,
                                    SUM(COALESCE({shares}, 0)) AS total_shares,
                                    MAX({snapshot}) AS latest_snapshot_date
                                FROM {table}
                                WHERE ({company_where}) OR {orgnr}::text ILIKE %s
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
                                company_where=company_where,
                            )

                            cur.execute(company_query, [*company_params, f"%{q}%"])
                            company_columns = [desc.name for desc in cur.description]
                            companies = [dict(zip(company_columns, row)) for row in cur.fetchall()]

                        # ---------------------------------------------------
                        # Beholdning for valgt person
                        # ---------------------------------------------------
                        if selected_person:
                            filters = [
                                sql.SQL("{shareholder} = %s").format(
                                    shareholder=sql.Identifier(shareholder_col)
                                )
                            ]
                            params: list[object] = [selected_person]

                            if identifier_col and selected_identifier:
                                filters.append(
                                    sql.SQL("COALESCE({identifier}::text, '') = %s").format(
                                        identifier=sql.Identifier(identifier_col)
                                    )
                                )
                                params.append(selected_identifier)

                            if postal_col and selected_postal_place:
                                filters.append(
                                    sql.SQL("COALESCE({postal}::text, '') = %s").format(
                                        postal=sql.Identifier(postal_col)
                                    )
                                )
                                params.append(selected_postal_place)

                            if company_filter and company_col:
                                company_filter_where, company_filter_params = _build_company_filter_where(
                                    company_col,
                                    orgnr_col,
                                    company_filter,
                                )
                                filters.append(company_filter_where)
                                params.extend(company_filter_params)

                                if debug_enabled:
                                    debug_info["company_filter"] = company_filter
                                    debug_info["company_filter_tokens"] = _search_tokens(company_filter)

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

                            selected_orgnrs = sorted(
                                {
                                    _normalize_orgnr(row.get("orgnr"))
                                    for row in rows
                                    if row.get("orgnr")
                                }
                            )

                            if debug_enabled:
                                debug_info["selected_orgnrs_norm"] = selected_orgnrs

                            regnskap_map: dict[str, dict[str, object]] = {}

                            if selected_orgnrs:
                                regnskap_map.update(
                                    _fetch_regnskap_batch_from_conn(
                                        conn,
                                        selected_orgnrs,
                                        debug_info if debug_enabled else None,
                                    )
                                )

                                if not regnskap_map:
                                    if debug_enabled:
                                        debug_info["missing_orgnrs_before_internal_db"] = selected_orgnrs

                                    regnskap_map.update(
                                        _fetch_regnskap_batch_from_internal_db(
                                            selected_orgnrs,
                                            debug_info if debug_enabled else None,
                                        )
                                    )

                                    if debug_enabled:
                                        debug_info["regnskap_hits_after_internal_db"] = len(regnskap_map)

                            person_totals = _apply_regnskap_to_rows(rows, regnskap_map)

                        # ---------------------------------------------------
                        # Eiere i valgt selskap
                        # ---------------------------------------------------
                        if selected_company and company_col:
                            company_filters = [
                                sql.SQL("{company} = %s").format(company=sql.Identifier(company_col))
                            ]
                            company_params: list[object] = [selected_company]

                            if orgnr_col and selected_company_orgnr:
                                company_filters.append(
                                    sql.SQL("COALESCE({orgnr}::text, '') = %s").format(
                                        orgnr=sql.Identifier(orgnr_col)
                                    )
                                )
                                company_params.append(selected_company_orgnr)

                            if owner_filter and shareholder_col:
                                owner_filter_where, owner_filter_params = _build_token_search_where(
                                    shareholder_col,
                                    _search_tokens(owner_filter),
                                )
                                company_filters.append(owner_filter_where)
                                company_params.extend(owner_filter_params)

                                if debug_enabled:
                                    debug_info["owner_filter"] = owner_filter
                                    debug_info["owner_filter_tokens"] = _search_tokens(owner_filter)

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
        company_filter=company_filter,
        owner_filter=owner_filter,
        person_totals=person_totals,
        debug_info=debug_info,
        error=error,
    )