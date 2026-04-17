# Eierdata per selskap (orgnr) i PostgreSQL

Ja – dette er en god modell.

Hvis du lager en egen snapshot-tabell for eierfilene, får du både:

1. oppslag på **eiere per selskap** (første steg), og
2. oppslag på **hvilke selskaper en eier er inne i** (neste steg).

## Hvor laster jeg opp datafilen med alle eierne?

Kort svar: **ikke i dagens webskjema for "Oppdater DB fra CSV"** på `/handler/`.
Det skjemaet er for handler-CSV som oppdaterer `topchanges.db` (SQLite).

For eierfilen med kolonner som `Orgnr`, `Navn aksjonær`, `Antall aksjer` gjør du dette:

1. Last CSV-filen til serveren (f.eks. via SCP/SFTP eller i deploy-pipeline).
2. Kjør importkommandoen under mot Postgres.

```bash
python scripts/import_shareholders_orgnr_csv.py   --csv /path/til/eierfil.csv   --snapshot-date 2026-04-17   --database-url "$DATABASE_URL"
```

Etter import kan du søke både per selskap og per eier i SQL-eksemplene lenger nede.

## 1) Opprett tabell

Kjør SQL-filen:

```bash
psql "$DATABASE_URL" -f sql_shareholders_orgnr_schema.sql
```

Tabellen `shareholder_snapshot` er laget for formatet:

- `Orgnr`, `Selskap`, `Aksjeklasse`, `Navn aksjonær`, `Fødselsår/orgnr`,
- `Postnr/sted`, `Landkode`, `Antall aksjer`, `Antall aksjer selskap`.

## 2) Importer CSV-filen

Bruk importskriptet:

```bash
python scripts/import_shareholders_orgnr_csv.py \
  --csv path/to/eiere.csv \
  --snapshot-date 2026-04-17 \
  --database-url "$DATABASE_URL"
```

Tips:

- Legg inn `--truncate` hvis du vil overskrive samme snapshotdato.
- Siden tabellen er en snapshot-tabell, beholder du historikk ved å importere nye datoer fortløpende.

## 3) Typiske spørringer

### Hent eiere i ett selskap (seneste snapshot)

```sql
SELECT
  orgnr,
  company_name,
  shareholder_name,
  shareholder_identifier,
  shares_owned,
  company_total_shares,
  ROUND(100.0 * shares_owned / NULLIF(company_total_shares, 0), 4) AS ownership_pct,
  snapshot_date
FROM shareholder_latest_per_company
WHERE orgnr = '810034882'
ORDER BY shares_owned DESC NULLS LAST;
```

### Finn selskaper for en eier

```sql
SELECT
  shareholder_name,
  shareholder_identifier,
  orgnr,
  company_name,
  shares_owned,
  snapshot_date
FROM shareholder_snapshot
WHERE shareholder_name ILIKE '%AARRESTAD%'
ORDER BY snapshot_date DESC, shares_owned DESC NULLS LAST;
```

### Finn selskaper for fødselsår/orgnr (mer presist enn navn)

```sql
SELECT
  shareholder_identifier,
  orgnr,
  company_name,
  shares_owned,
  snapshot_date
FROM shareholder_snapshot
WHERE shareholder_identifier = '1981'
ORDER BY snapshot_date DESC, shares_owned DESC NULLS LAST;
```

## Anbefalt videreutvikling

Når dette er på plass kan du enkelt bygge API/UI på toppen:

- `GET /owners/by-company/{orgnr}`
- `GET /owners/by-owner?name=...`
- `GET /owners/by-identifier/{id}`

Da får du akkurat det du beskriver: først oppslag per selskap, deretter søk per eier med porteføljeoversikt.
