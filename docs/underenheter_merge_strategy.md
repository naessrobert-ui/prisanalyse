# Strategi for å slå sammen `underenheter` og dagens datasett

Basert på feltene du listet opp, bør vi **ikke** prøve å skrive underenheter direkte inn i dagens skjema uten normalisering først.

Den tryggeste løsningen er en trestegsmodell:

1. **Normaliser kolonnenavn** fra underenheter til dagens kanoniske feltnavn.
2. **Behold underenheter-spesifikke felt** i et eget JSON-felt (`source_extra_json`).
3. **La dagens-spesifikke felt stå tomme/null** til de fylles fra dagens hovedkilde.

## 1. Feltregler

### A. Samme navn og samme betydning

Disse kan kopieres rett over 1:1.

Eksempler:

- `navn`
- `organisasjonsnummer`
- `antallAnsatte`
- `naeringskode1.kode`
- `postadresse.postnummer`

Disse ligger i `same_fields` i mappingfilen. 【F:config/underenheter_field_map.json†L1-L36】

### B. Samme betydning, ulikt navn

Disse må renames til dagens kanoniske navn før import.

Eksempler:

- `beliggenhetsadresse.adresse` → `forretningsadresse.adresse`
- `beliggenhetsadresse.postnummer` → `forretningsadresse.postnummer`
- `registreringsdatoIEnhetsregisteret` → `registreringsdatoenhetsregisteret`
- `oppstartsdato` → `stiftelsesdato`

Disse ligger i `aliases` i mappingfilen. 【F:config/underenheter_field_map.json†L37-L47】

### C. Kun underenheter

Disse må **ikke kastes**, men bør lagres som ekstra metadata fordi de ikke finnes i dagens skjema.

Eksempler:

- `datoEierskifte`
- `nedleggelsesdato`

Disse legges i `source_extra_json` av normaliseringsscriptet. 【F:config/underenheter_field_map.json†L48-L51】【F:scripts/normalize_underenheter_csv.py†L42-L61】

### D. Kun dagens

Disse feltene finnes ikke i underenheter-kilden og skal derfor stå blanke/null etter normalisering.

Eksempler:

- `konkurs`
- `underAvvikling`
- `kapital.belop`
- `vedtektsdato`
- `erIKonsern`

Disse er definert som `dagens_only_fields` i mappingfilen. 【F:config/underenheter_field_map.json†L52-L100】

## 2. Nytt normaliseringsscript

Jeg har lagt inn et script som gjør denne transformasjonen:

```bash
python scripts/normalize_underenheter_csv.py input_underenheter.csv output_normalized.csv
```

Scriptet:

- leser underenheter-CSV,
- bruker `config/underenheter_field_map.json`,
- skriver ut en ny CSV med dagens kanoniske feltnavn,
- og legger underenheter-spesifikke felt i `source_extra_json`.

Se implementasjonen her. 【F:scripts/normalize_underenheter_csv.py†L1-L123】

## 3. Anbefalt produksjonsflyt

### Filnavn

CSV-filen kan i praksis hete hva som helst, fordi scriptet tar inn eksplisitt filsti som argument. Men jeg anbefaler denne navnekonvensjonen:

- råfil: `underenheter_YYYY-MM-DD.csv`
- normalisert fil: `normalized_underenheter_YYYY-MM-DD.csv`

Eksempel:

```bash
python scripts/normalize_underenheter_csv.py \
  underenheter_2026-03-21.csv \
  normalized_underenheter_2026-03-21.csv
```

### Steg 1 – normaliser CSV

```bash
python scripts/normalize_underenheter_csv.py \
  underenheter.csv \
  normalized_underenheter.csv
```

### Steg 2 – kjør merge mot `entity`

Jeg har lagt inn et eget merge-script:

```bash
python scripts/merge_normalized_underenheter_into_entity.py \
  normalized_underenheter.csv \
  --dry-run
```

Når du er fornøyd med tallene fra dry-run:

```bash
python scripts/merge_normalized_underenheter_into_entity.py \
  normalized_underenheter.csv
```

Scriptet merger et trygt delsett inn i `entity`:

- `organisasjonsnummer` → `orgnr`
- `navn` → `navn`
- `organisasjonsform.kode` → `orgform`
- `naeringskode1.kode` → `naeringskode`
- adresse/postnummer/kommunenummer → relevante `entity`-felt
- `antallAnsatte` → `ansatte`
- `registrertIMvaRegisteret` → `mva`
- `stiftelsesdato` → `stiftet`
- `registreringsdatoenhetsregisteret` → `registrert`

Merge-scriptet fyller bare hull og overskriver ikke eksisterende verdier når `entity` allerede har data. 【F:scripts/merge_normalized_underenheter_into_entity.py†L11-L25】【F:scripts/merge_normalized_underenheter_into_entity.py†L87-L197】

### Steg 3 – merge-policy

Når du merger mot dagens data, anbefaler jeg:

- bruk `organisasjonsnummer` som nøkkel
- **overskriv ikke** dagens felter hvis dagens verdi allerede finnes
- bruk underenheter til å **fylle hull**
- behold `source_extra_json` så du ikke mister `datoEierskifte` / `nedleggelsesdato`

## 4. Hvorfor dette er riktig løsning

Dette løser akkurat problemet du beskriver:

- overlappende felter håndteres 1:1
- like felter med ulike navn harmoniseres
- underenheter-only felter tas vare på
- dagens-only felter blir ikke feilaktig fylt med feil data
- du får et stabilt “kanonisk” skjema før DB-import
