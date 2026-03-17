# Rask guide: last opp `topchanges.db` med script

Når web-opplasting feiler (CORS/502), bruk scriptet lokalt.

## 1) Finn verdier du trenger

- **DB-fil** på din PC (f.eks. `C:\data\topchanges_recent_60d.db`)
- **S3 URI** i format `s3://bucket/prefix/topchanges.db`

## 2) Kjør scriptet

### Enklest (anbefalt): bruk `--s3-uri`

```bash
python scripts/upload_handler_db_to_s3.py \
  --source "/path/to/topchanges_recent_60d.db" \
  --s3-uri "s3://DIN_BUCKET/DIN_PREFIX/topchanges.db"
```

### Alternativ: `--bucket` + `--key`

```bash
python scripts/upload_handler_db_to_s3.py \
  --source "/path/to/topchanges_recent_60d.db" \
  --bucket "DIN_BUCKET" \
  --key "DIN_PREFIX/topchanges.db"
```

## 3) Etter opplasting

- Scriptet skriver ut riktig `HANDLER_DB_S3_URI`.
- Sjekk at samme URI er satt i miljøvariabler i drift.

## Vanlige feil

- `Source file not found`: feil sti til `.db`-fil.
- `Ugyldig --s3-uri`: feil format; må være `s3://bucket/key`.
- `AccessDenied`: AWS-bruker/rolle mangler `s3:PutObject`.
