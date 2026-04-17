# -*- coding: utf-8 -*-

import os

import boto3
import psycopg
from flask import Blueprint, render_template, request

aksjonaer_bp = Blueprint("aksjonaer", __name__)


def connect_db():
    """
    Bruker IAM-token hvis DB_IAM_AUTH=1.
    Ellers bruker den DATABASE_URL.
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

    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL mangler, eller sett DB_IAM_AUTH=1 med DBHOST/DBUSER.")

    return psycopg.connect(database_url)


@aksjonaer_bp.route("/aksjonaer")
def aksjonaer_sok():
    q = (request.args.get("q") or "").strip()
    rows = []
    error = None

    if q:
        if len(q) < 2:
            error = "Skriv minst 2 tegn."
        else:
            try:
                with connect_db() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            SELECT
                                shareholder_name,
                                orgnr,
                                selskap,
                                aksjeklasse,
                                shares_count,
                                company_shares_count,
                                CASE
                                    WHEN company_shares_count IS NULL
                                      OR company_shares_count = 0
                                      OR shares_count IS NULL
                                    THEN NULL
                                    ELSE ROUND((shares_count::numeric / company_shares_count::numeric) * 100, 2)
                                END AS ownership_pct,
                                snapshot_date
                            FROM shareholder_orgnr_import
                            WHERE shareholder_name ILIKE %s
                            ORDER BY shareholder_name, selskap, shares_count DESC NULLS LAST
                            LIMIT 200;
                            """,
                            (f"%{q}%",),
                        )

                        columns = [desc.name for desc in cur.description]
                        rows = [dict(zip(columns, row)) for row in cur.fetchall()]

            except Exception as exc:
                error = f"Klarte ikke hente aksjonærdata: {exc}"

    return render_template(
        "aksjonaer_sok.html",
        q=q,
        rows=rows,
        error=error,
    )
