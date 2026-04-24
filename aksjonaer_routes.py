# -*- coding: utf-8 -*-

import os

import boto3
import psycopg
from psycopg import sql
from flask import Blueprint, render_template, request

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


def _load_table_columns(conn: psycopg.Connection, table_name: str) -> set[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
            """,
            (table_name,),
        )
        return {row[0] for row in cur.fetchall()}


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
    selected_person = (request.args.get("person") or "").strip()
    selected_identifier = (request.args.get("pid") or "").strip()
    selected_postal_place = (request.args.get("pplace") or "").strip()
    selected_company = (request.args.get("company") or "").strip()
    selected_company_orgnr = (request.args.get("corgnr") or "").strip()
    persons = []
    companies = []
    rows = []
    owners = []
    error = None

    if q:
        if len(q) < 2:
            error = "Skriv minst 2 tegn."
        else:
            try:
                with connect_db() as conn:
                    columns = _load_table_columns(conn, SHAREHOLDER_TABLE)

                    if not columns:
                        raise RuntimeError(f"Fant ingen kolonner i tabellen {SHAREHOLDER_TABLE}.")

                    shareholder_col = _find_first_column(columns, ["shareholder_name"])
                    orgnr_col = _find_first_column(columns, ["orgnr"])
                    company_col = _find_first_column(columns, ["selskap", "company_name"])
                    share_class_col = _find_first_column(columns, ["aksjeklasse", "share_class"])
                    shares_col = _find_first_column(columns, ["shares_count", "shares_owned"])
                    company_total_col = _find_first_column(columns, ["company_shares_count", "company_total_shares"])
                    snapshot_col = _find_first_column(columns, ["snapshot_date"])
                    identifier_col = _find_first_column(columns, ["shareholder_identifier", "fodselsaar_orgnr"])
                    postal_col = _find_first_column(columns, ["postal_place", "postnr_sted"])
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
                                table=sql.Identifier(SHAREHOLDER_TABLE),
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
                                table=sql.Identifier(SHAREHOLDER_TABLE),
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
                                table=sql.Identifier(SHAREHOLDER_TABLE),
                                where_clause=sql.SQL(" AND ").join(filters),
                            )
                            cur.execute(holdings_query, params)
                            holding_columns = [desc.name for desc in cur.description]
                            rows = [dict(zip(holding_columns, row)) for row in cur.fetchall()]

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
                                table=sql.Identifier(SHAREHOLDER_TABLE),
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
        error=error,
    )
