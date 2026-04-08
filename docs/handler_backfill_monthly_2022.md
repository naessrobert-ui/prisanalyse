# Backfill fra månedsfiler (2022 -> i dag)

Ja, dette er en mulig og god løsning.

## Hvordan det fungerer

- Hver CSV representerer en beholdnings-snapshot for en dato/periode.
- Vi lagrer beholdning per `isin + investor_id + date_today`.
- Endring/handel identifiseres når beholdningen på en dato er ulik forrige observasjon for samme investor/aksje.
- Dermed kan vi finne siste/forrige handel også langt tilbake i historikk, så lenge du har lastet opp historiske snapshots.

## Praktisk anbefaling

1. Last opp månedsfiler fra tidligst til senest (f.eks. 2022-01 til 2026-04).
2. Bruk filnavn med dato i format:
   - `YYYY-MM-DD` (anbefalt), eller
   - `YYYY_MM_DD`, eller
   - `YYYYMMDD`
3. Etter ingest bygges snapshot-tabellen på nytt og API/UI vil vise:
   - `Siste handel`
   - `Forrige handel`
   - `Dager mellom handler`

## Viktig begrensning

- Hvis en investor bare har én historisk endring i tilgjengelige data, blir `forrige handel` tom (`NULL`), og da blir også `dager mellom handler` tom.
