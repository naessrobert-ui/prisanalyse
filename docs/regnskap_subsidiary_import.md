# Import av CSV med datterselskap til Postgres

Hvis du har en stor CSV med morselskap/datterselskap-relasjoner, kan du laste den inn i databasen med skriptet `scripts/import_subsidiaries_csv.py`.

Skriptet gjør **ikke** direkte insert i `entity`-tabellen. I stedet oppretter det en trygg relasjonstabell:

- `subsidiary_relations`
- view: `subsidiary_children_missing_from_entity`

Dette er den tryggeste måten å få dataene inn i produksjon først, og deretter kontrollere hvor mange datterselskaper som fortsatt mangler i `entity`.

## 1. Forutsetninger

- Du må ha en fungerende Postgres-tilkobling i `DATABASE_URL`
- Python-miljøet må ha `psycopg` installert

Eksempel:

```bash
export DATABASE_URL='postgresql://<user>:<password>@<host>:5432/<db>'
```

## 2. Kjør importen

Filnavnet er valgfritt, men anbefalt navnekonvensjon er:

- relasjonsfil: `subsidiary_relations_YYYY-MM-DD.csv`
- eller norsk variant: `datterselskaper_YYYY-MM-DD.csv`

En enkel kjøring:

```bash
python scripts/import_subsidiaries_csv.py /path/til/datterselskaper_2026-03-21.csv
```

Hvis CSV-en bruker andre kolonnenavn enn standardoppsettet, kan du mappe dem eksplisitt:

```bash
python scripts/import_subsidiaries_csv.py /path/til/datterselskaper.csv \
  --parent-orgnr-col morselskap_orgnr \
  --child-orgnr-col datter_orgnr \
  --parent-name-col morselskap_navn \
  --child-name-col datter_navn \
  --source-name underselskap_2026_03
```

Skriptet forsøker ellers automatisk å finne vanlige kolonnenavn som:

- `morselskap_orgnr`
- `mor_orgnr`
- `datter_orgnr`
- `organisasjonsnummer`
- `morselskap_navn`
- `datter_navn`
- `navn`

## 3. Hva som opprettes

Skriptet oppretter:

### Tabell: `subsidiary_relations`

Kolonner:

- `parent_orgnr`
- `child_orgnr`
- `parent_name`
- `child_name`
- `source_name`
- `raw_row`
- `imported_at`
- `updated_at`

Primærnøkkel:

- `(parent_orgnr, child_orgnr)`

### View: `subsidiary_children_missing_from_entity`

Denne viser alle datterselskaper som finnes i `subsidiary_relations`, men som **ikke** finnes i `entity`.

Det er denne viewen du bør bruke for å finne ut hva som faktisk mangler i hoveddatabasen.

## 4. Kontroller resultatet

Etter import kan du kontrollere hvor mye som kom inn:

```sql
SELECT COUNT(*) FROM subsidiary_relations;
SELECT COUNT(*) FROM subsidiary_children_missing_from_entity;
```

Se noen eksempler:

```sql
SELECT *
FROM subsidiary_children_missing_from_entity
LIMIT 50;
```

## 5. Hvis du vil legge manglende datterselskaper inn i `entity`

Dette bør gjøres som et **eget steg** etter kontroll, fordi `entity` kan ha flere felt/krav enn bare orgnr og navn.

Et trygt første kontrollspørsmål er:

```sql
SELECT
  child_orgnr,
  child_name
FROM subsidiary_children_missing_from_entity
WHERE NULLIF(child_name, '') IS NOT NULL
LIMIT 100;
```

Hvis `entity`-skjemaet tillater det, kan du senere lage en egen `INSERT INTO entity (...) SELECT ...` basert på disse radene. Men det bør skje kontrollert, ikke direkte under første import.

## 6. Anbefalt produksjonsflyt

1. Last CSV inn i `subsidiary_relations`
2. Kontroller `subsidiary_children_missing_from_entity`
3. Verifiser navn/orgnr-kvalitet
4. Lag en separat merge-jobb for å upserte barn inn i `entity`
5. Rebygg eventuelle søkeindekser/materialiserte views hvis dere bruker det

## 7. Hvorfor denne modellen er best

Denne importen er laget for å være trygg i produksjon:

- ingen direkte risiko for å ødelegge `entity`
- rådata beholdes i `raw_row`
- du får en tydelig oversikt over hvilke datterselskaper som mangler i hovedtabellen
- importen kan kjøres flere ganger uten duplikater, siden den bruker `ON CONFLICT`
