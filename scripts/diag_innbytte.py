"""
Diagnoseskript for /bil/innbytte: hvorfor finner ikke søket biler etter ~10. januar 2026?

Last ned database_biler.parquet fra S3 og rapporter:
  - max(Dato) og max(Dato_ny) totalt
  - rader pr måned i 2025-10 .. 2026-05 basert på Dato_ny (sluttdato/fjernet)
  - rader pr måned i 2025-10 .. 2026-05 basert på Dato (annonsestart)
  - fordeling av Solgt-flagg pr måned

Bruk:
    python -m scripts.diag_innbytte
"""
from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

import boto3
import duckdb

S3_BUCKET = os.getenv("S3_BUCKET_NAME", "prisanalyse-data")
S3_KEY = "calc/bil/database_biler.parquet"


def download_parquet() -> Path:
    local = Path(tempfile.gettempdir()) / "diag_database_biler.parquet"
    print(f"Laster ned s3://{S3_BUCKET}/{S3_KEY} -> {local}")
    s3 = boto3.client("s3")
    s3.download_file(S3_BUCKET, S3_KEY, str(local))
    print(f"Ferdig. Størrelse: {local.stat().st_size / (1024*1024):.1f} MB\n")
    return local


def main() -> int:
    path = download_parquet()
    con = duckdb.connect()

    cols = [r[0] for r in con.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{path}')"
    ).fetchall()]
    lower = {c.lower(): c for c in cols}

    def pick(*cands: str) -> str | None:
        for c in cands:
            if c.lower() in lower:
                return lower[c.lower()]
        return None

    c_dato = pick("Dato", "dato")
    c_dato_ny = pick("Dato_ny", "dato_ny")
    c_solgt = pick("Solgt", "solgt")

    if not c_dato or not c_dato_ny:
        print("Mangler Dato/Dato_ny i parquet. Kolonner funnet:")
        print(cols)
        return 1

    # Solgt kan være string ('FJERNET', 'SOLGT', 'true', ...), bool eller 0/1.
    # Normaliser til lower-cased varchar slik at vi kan rapportere distinct verdier.
    solgt_norm = f"lower(trim(CAST(\"{c_solgt}\" AS VARCHAR)))" if c_solgt else "NULL"

    # Bool-tolkning som matcher _bool_expr i bil_routes.py:
    bool_true_expr = (
        f"(CASE "
        f"  WHEN \"{c_solgt}\" IS NULL THEN FALSE "
        f"  WHEN TRY_CAST(\"{c_solgt}\" AS BOOLEAN) IS NOT NULL THEN TRY_CAST(\"{c_solgt}\" AS BOOLEAN) "
        f"  WHEN {solgt_norm} IN ('1','true','t','yes','y','ja') THEN TRUE "
        f"  ELSE FALSE END)"
    ) if c_solgt else "FALSE"

    base_cte = f"""
      WITH src AS (
        SELECT
          TRY_CAST("{c_dato}"    AS TIMESTAMP) AS dato_start,
          TRY_CAST("{c_dato_ny}" AS TIMESTAMP) AS dato_end
          {f', {solgt_norm} AS solgt_norm, {bool_true_expr} AS bool_solgt_true' if c_solgt else ''}
        FROM read_parquet('{path}')
      )
    """

    print("=== TOTALT ===")
    total = con.execute(f"""
      {base_cte}
      SELECT
        COUNT(*)              AS rader,
        MAX(dato_start)       AS max_dato_start,
        MAX(dato_end)         AS max_dato_end,
        SUM(CASE WHEN bool_solgt_true THEN 1 ELSE 0 END) AS bool_true,
        SUM(CASE WHEN NOT bool_solgt_true THEN 1 ELSE 0 END) AS bool_false_eller_null
      FROM src
    """).fetchdf()
    print(total.to_string(index=False))
    print()

    print("=== Rader pr måned (Dato_ny = sluttdato/fjernet), fra 2025-10 ===")
    pr_end = con.execute(f"""
      {base_cte}
      SELECT
        date_trunc('month', dato_end)::DATE AS maaned,
        COUNT(*)                            AS rader,
        SUM(CASE WHEN bool_solgt_true THEN 1 ELSE 0 END) AS bool_true,
        SUM(CASE WHEN NOT bool_solgt_true THEN 1 ELSE 0 END) AS bool_false_eller_null
      FROM src
      WHERE dato_end >= DATE '2025-10-01'
      GROUP BY 1
      ORDER BY 1
    """).fetchdf()
    print(pr_end.to_string(index=False) if not pr_end.empty else "(ingen rader)")
    print()

    print("=== Rader pr måned (Dato = annonsestart), fra 2025-10 ===")
    pr_start = con.execute(f"""
      {base_cte}
      SELECT
        date_trunc('month', dato_start)::DATE AS maaned,
        COUNT(*)                              AS rader,
        SUM(CASE WHEN bool_solgt_true THEN 1 ELSE 0 END) AS bool_true,
        SUM(CASE WHEN NOT bool_solgt_true THEN 1 ELSE 0 END) AS bool_false_eller_null
      FROM src
      WHERE dato_start >= DATE '2025-10-01'
      GROUP BY 1
      ORDER BY 1
    """).fetchdf()
    print(pr_start.to_string(index=False) if not pr_start.empty else "(ingen rader)")
    print()

    if c_solgt:
        print("=== Distinct Solgt-verdier (totalt) ===")
        distinct = con.execute(f"""
          {base_cte}
          SELECT solgt_norm, COUNT(*) AS rader, MIN(dato_end) AS min_dato_end, MAX(dato_end) AS max_dato_end
          FROM src
          GROUP BY 1
          ORDER BY rader DESC
        """).fetchdf()
        print(distinct.to_string(index=False))
        print()

        print("=== 10 nyeste rader, uavhengig av Solgt-flagg ===")
        nyeste = con.execute(f"""
          {base_cte}
          SELECT dato_start, dato_end, solgt_norm, bool_solgt_true
          FROM src
          ORDER BY COALESCE(dato_end, dato_start) DESC NULLS LAST
          LIMIT 10
        """).fetchdf()
        print(nyeste.to_string(index=False))

    return 0


if __name__ == "__main__":
    sys.exit(main())
