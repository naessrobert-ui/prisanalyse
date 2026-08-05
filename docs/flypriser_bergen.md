# Flypriser ut fra Bergen

Samler inn lavpriser på flybilletter fra Bergen (BGO) til utvalgte
destinasjoner, per avreisemåned, og bygger opp en prishistorikk.

> 📈 For den investor-vinklede analysen (Norwegian-prisindeks, avviksdeteksjon,
> konkurrent-gap, booking-kurve og trafikktall) på `/flypriser/norwegian`,
> se [flypriser_norwegian.md](flypriser_norwegian.md). Innsamlingen deles –
> samme skript henter både Bergen-rutene og Norwegians kjernenett.

## Hvorfor ikke Norwegian direkte?

Eksempel-URL-en (`norwegian.com/no/lavpriskalender/...`) er en frontend-side.
Norwegian sitt underliggende pris-API ligger bak Akamai bot-beskyttelse og
svarer med `403 Are you human?` ved direkte skraping. Det gjør et robust
skript mot Norwegian upraktisk.

I stedet brukes **Travelpayouts / Aviasales** sitt gratis pris-API, som gir
de billigste billettene per måned og destinasjon **på tvers av alle
flyselskap** (inkludert Norwegian). Det er nettopp en lavpriskalender.

## Oppsett

1. Lag en gratis konto på <https://www.travelpayouts.com>.
2. Kopier API-tokenet (under *Developers / API access*).
3. Sett miljøvariabelen:

   ```bash
   export TRAVELPAYOUTS_TOKEN="din_token"
   ```

## Bruk

```bash
# Hele rutenettet (Bergen + Norwegians kjernenett), 6 måneder frem:
python -m scripts.flypriser_bergen

# Bare Bergen-rutene, 12 måneder:
python -m scripts.flypriser_bergen --gruppe bergen --months 12
```

Flagg:

| Flagg         | Beskrivelse                                     | Standard |
|---------------|-------------------------------------------------|----------|
| `--gruppe`    | `bergen` / `innenriks` / `norwegian_intl`       | alle     |
| `--months`    | Antall måneder fremover                          | 6        |
| `--currency`  | Valuta                                           | nok      |
| `--pause`     | Pause (sek) mellom API-kall                      | 0.35     |

## Resultat

- `data/flypriser_historikk.csv` – full historikk, én rad per tilbud
  (kolonnen `hentet_dato` gjør at du kan følge prisutvikling over tid).
- `data/flypriser_beste.csv` – billigste tilbud per rute og måned ved siste
  kjøring.

## Web-side

Blueprinten `scripts/flypriser_routes.py` (registrert i `app.py`) viser data
på **`/flypriser`** (reiser ut fra Bergen, `origin=BGO`):

- **Prismatrise** – billigste pris per destinasjon × avreisemåned, med
  fargekoding (grønt = billigst) og klikkbare lenker til søket.
- **Prisutvikling** – graf (Chart.js) som viser laveste tilgjengelige pris
  per destinasjon for hver gang skriptet er kjørt. JSON på `/flypriser/api/data`.
- Lenke til **investor-dashbordet** på `/flypriser/norwegian`.

Siden bygger seg opp etter hvert som `data/flypriser_*.csv` fylles.

## Følge prisutvikling / automatisering

### Lokalt via cron

```bash
0 7 * * *  cd /sti/til/prisanalyse && TRAVELPAYOUTS_TOKEN=... python -m scripts.flypriser_bergen
```

### I skyen via GitHub Actions

`.github/workflows/flypriser-bergen.yml` kjører innsamlingen daglig (05:00 UTC)
og committer oppdaterte CSV-er. Krever ett repo-secret:

> **Settings → Secrets and variables → Actions → New repository secret**
> Navn: `TRAVELPAYOUTS_TOKEN`, verdi: tokenet ditt.

Kan også kjøres manuelt fra **Actions**-fanen (*workflow_dispatch*).

Rutenettet ligger i `_RUTER_RAW` øverst i `scripts/flypriser_bergen.py`
(par `(A, B, gruppe)`, hentes i begge retninger) og kan enkelt utvides.
Kjør en delmengde med `--gruppe bergen` / `innenriks` / `norwegian_intl`.
