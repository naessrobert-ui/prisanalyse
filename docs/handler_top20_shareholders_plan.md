# Plan for daglig import av topp-20 aksjonærer

Dette oppsettet støtter CSV-filer som kommer daglig, og gjør det mulig å hente:

- topp-20 aksjonærer per selskap (ISIN),
- beholdning per dato,
- dato for siste endring i beholdning per aksjonær.

## Anbefalt datarutine

1. Last inn alle daglige filer for de siste 10–20 dagene for kortsiktig analyse.
2. Last i tillegg inn filer ved hver månedsslutt for historikk over lengre tid.
3. Behold alle snapshots i databasen (ikke overskriv historiske datoer).

Dette gir både:

- høy oppløsning på nylige endringer,
- lavere lagringsbehov over tid.

## CSV-kolonner som nå støttes i importen

Importen i `handler_data.py` støtter nå også engelske/standardiserte kolonnenavn, inkludert:

- `name`, `investorId`, `percentage`, `noOfStocks`, `ticker`, `companyname`,
- `sharesOut`, `date`, `isin`, `ranking`, `Country code`,
- `First Name`, `Last Name`, `Date of Birth`, `ktotype`, `private`.

## Nytt API for topp-aksjonærer + siste endringsdato

Ny endpoint:

`GET /handler/api/eier-oversikt/top-shareholders?isin=<ISIN>&as_of=<YYYY-MM-DD>&limit=20`

Ny screening-endpoint (alle eller utvalg av selskaper):

`GET /handler/api/eier-oversikt/top-shareholders/scan?as_of=<YYYY-MM-DD>&since_date=<YYYY-MM-DD>&top_n=20&min_idle_days=20&direction=both&isins=NO001...,NO00...&tickers=EQNR,NOD`

Responsen inneholder blant annet:

- `name`, `investor_id`, `ranking`,
- `no_of_stocks`, `percentage`,
- `last_change_date`, `days_since_last_change`,
- `snapshot_date`, `ticker`, `company_name`.

Eksempel:

```bash
curl "http://localhost:5000/handler/api/eier-oversikt/top-shareholders?isin=NO0010096985&as_of=2026-03-31&limit=20"
```

Eksempel på scan:

```bash
curl "http://localhost:5000/handler/api/eier-oversikt/top-shareholders/scan?as_of=2026-04-09&since_date=2026-04-01&top_n=20&min_idle_days=20&direction=both"
```

## Hvor i appen du gjør dette

- **Last opp CSV-filer**: `Handler Oslo Børs` → forsiden (`/handler/`) → seksjonen **"Oppdater DB med nye CSV-filer"**.
- **Se resultater i UI**: `Eier oversikt` (`/handler/eier-oversikt`) → tab **Per aksje** → panel **"Topp aksjonærer + siste endring"**.

## Separat database for topp-20

Ja. Etter CSV-opplasting bygges det nå en separat SQLite-fil med topp-20 snapshot per aksje:

- Standard sti: `HANDLER_TOP20_DB_PATH` (default: `<workdir>/top20_shareholders.db`)
- Tabell: `top20_snapshot`
- Innhold: `snapshot_date`, `isin`, `investor_id`, `name`, `ranking`, `no_of_stocks`,
  `percentage`, `last_change_date`, `days_since_last_change`, `ticker`, `company_name`.
