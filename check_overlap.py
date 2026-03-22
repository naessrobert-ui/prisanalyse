import csv
import os
import psycopg

CSV_PATH = "normalized_underenheter.csv"

# Les et utvalg orgnr fra CSV
csv_orgnr = set()
with open(CSV_PATH, encoding="utf-8", newline="") as f:
    reader = csv.DictReader(f)
    for i, row in enumerate(reader):
        v = (row.get("organisasjonsnummer", "") or "").strip()
        cleaned = "".join(ch for ch in v if ch.isdigit())
        if cleaned:
            csv_orgnr.add(cleaned)
        if i >= 50000:
            break

print("CSV sample distinct:", len(csv_orgnr))
print("CSV first 20:", list(sorted(csv_orgnr))[:20])

conn = psycopg.connect(os.environ["DATABASE_URL"])
cur = conn.cursor()

# Hent et utvalg orgnr fra entity
cur.execute("""
    select orgnr
    from entity
    where orgnr is not null
    limit 50000
""")
entity_orgnr = {str(row[0]).strip() for row in cur.fetchall() if row[0]}

print("ENTITY sample distinct:", len(entity_orgnr))
print("ENTITY first 20:", list(sorted(entity_orgnr))[:20])

overlap = csv_orgnr & entity_orgnr
print("OVERLAP sample:", len(overlap))
print("OVERLAP first 20:", list(sorted(overlap))[:20])

conn.close()