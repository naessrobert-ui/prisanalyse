import os
import psycopg

conn = psycopg.connect(os.environ["DATABASE_URL"])
cur = conn.cursor()

cur.execute("""
select column_name, data_type
from information_schema.columns
where table_name = 'entity' and column_name = 'orgnr'
""")
print("COLUMN:", cur.fetchall())

cur.execute("""
select orgnr
from entity
where orgnr is not null
limit 10
""")
print("SAMPLES:", cur.fetchall())

cur.execute("""
select orgnr, length(orgnr::text)
from entity
where orgnr is not null
limit 20
""")
print("LENGTHS:", cur.fetchall())

conn.close()