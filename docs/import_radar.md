# ImportRadar – første kalkyleversjon

ImportRadar sammenligner normaliserte utenlandske annonser med eksisterende
BilRadar-priser. Frakt er satt til **13 000 NOK fra Tyskland**, **8 000 NOK fra
Sverige**, og marginmålet er **30 000 NOK etter oppgitte kostnader og moms**,
før selskapsskatt og faste driftskostnader. Innstillingene kan endres i
`config/import_radar.json`; frakt kan også overstyres per bil.

## Status og avgrensning

Kalkylen og BilRadar-koblingen er kjørbare. De fire opprinnelige søkelenkene
er bevart i konfigurasjonen med `collection_status=not_connected`.
**Denne versjonen henter ikke annonser, følger ikke søkefiltrene automatisk,
oppdaterer ikke valutakurser og sender ikke varsler.** Den leser en JSON-liste
med annonser fra et separat uttrekk. Ingen workflow, produksjonsrute eller
eksisterende kupp-vakt er endret. Rapporten viser alltid denne statusen.

Neste integrasjonstrinn er autorisert annonsetilgang: Mobile.de Search API
krever særskilt aktivering, og Bytbil-datakilden må avklares. E-postvarsler
fra lagrede søk kan også være en kilde etter at format og felter er kontrollert.
Vanlig brukerkonto er ikke dokumentasjon på at Search API er aktivert.
Kildene må levere faktisk selgerland, kontantpris og valuta; Mobile.de har
også annonser utenfor Tyskland. Mobile-søk 2 mangler momsfilter.

## Kjøring

Bruk prosjektets Python-miljø med pandas og numpy. Kjør fra repo-roten:

```bash
python -m scripts.import_radar annonser.json \
  --eur-nok 11.70 --sek-nok 1.05 \
  --registration-date 2026-09-06 \
  --other-costs-nok 5000 \
  --output /tmp/import-radar
```

**Kursene og øvrige kostnader i kommandoen er kun illustrasjoner**, ikke
innhentede markedskurser eller avtalte kostnader. Kursene er NOK per én EUR
eller SEK. Bruk faktisk forventet bankkurs og eventuelt `fx_buffer_pct`.
Resultatet er en HTML-rapport og JSON med hele kalkylen per bil.
Registreringsdato er planlagt første registrering i Norge, ikke kjøpsdato.
Reglene er avgrenset til 2026; senere år avvises til satsene er oppdatert.

`price_basis` er `hurtigpris` som standard. Markedspris vises også, og kan
velges eksplisitt med `--price-basis forventet_pris`. Manglende hurtigpris
erstattes ikke automatisk med en høyere markedspris.

## Kontrakt for annonseuttrekk

Eksempel på én **oppdiktet** annonse (JSON-filen må inneholde en liste):

```json
[
  {
    "source": "mobile_de",
    "listing_id": "example-1",
    "url": "https://suchen.mobile.de/fahrzeuge/details.html?id=example-1",
    "country": "DE",
    "currency": "EUR",
    "price_amount": 17850,
    "export_price_amount": 15000,
    "export_price_confirmed": true,
    "vat_reclaimable": true,
    "fuel": "electric",
    "vehicle_type": "passenger_car",
    "make": "Hyundai",
    "model": "Kona",
    "model_year": 2022,
    "mileage": 50000,
    "mileage_unit": "km",
    "drive": "FWD",
    "battery_kwh": 64,
    "variant_text": "64 kWh",
    "variant_confirmed": true,
    "damage_free": true,
    "first_registration": "2022-09-06",
    "weight_kg": 2000,
    "observed_at": "2026-09-06T10:00:00Z"
  }
]
```

- `price_amount` er kontant bruttopris i annonsevaluta, aldri månedsleie/nettopris.
- `export_price_amount` er beløpet som faktisk skal betales ved eksport etter
  avklart momsbehandling. Det brukes kun ved `export_price_confirmed=true`.
  Uten bekreftelse brukes bruttoprisen, og bilen merkes for kontroll. At
  annonsen viser fradragsberettiget moms er alene ikke en bekreftet eksportpris.
- `mileage_unit=mil` betyr svenske mil, 10 km. Kilometerstand må være kjent.
- `drive` støtter AWD, 4WD, ALL_WHEEL, FWD, RWD og 2WD.
- `make`/`model` må svare til norske modellnavn i BilRadar. Batteri, utstyr og
  varianttekst brukes av eksisterende variantklassifisering. Oversettelser av
  modellnavn skal være eksplisitte; koden gjetter ikke at ulike modellnavn er like.
- `weight_kg` er avgiftsrelevant egenvekt. Ukjent vekt eller dato gir ingen
  ferdig kalkyle, og blir aldri tolket som null avgift. Ny, uregistrert bil
  krever separat tilpasning; denne første versjonen er for bruktimport.
- `observed_at` er faktisk hentetidspunkt med tidssone. Mer enn 24 timer
  gamle annonser merkes for kontroll. Ingen tilgjengelighetskontroll foretas.
- Deduplisering bruker kilde + annonse-ID og nyeste observasjon. Samme ID hos
  to ulike nettsteder holdes separat. VIN-basert deduplisering på tvers av
  nettsteder og tilstand mellom kjøringer er ikke implementert.

## Beregning for mva-registrert bilforhandler

Forutsetning: elektrisk personbil som salgsvare med full fradragsrett for
importmoms, og ordinært salg til norsk sluttkunde før første norske registrering.
Privatimport, demonstrasjonsbil/uttak og annen avgiftsbehandling omfattes ikke.

1. Innkjøp = bekreftet eksportbeløp (ellers bruttopris) × bankkurs med buffer.
2. Vektavgift = max(egenvekt − 500, 0) × 12,71 × (1 − bruksfradrag).
3. Engangsavgift i kalkylen = vektavgift + 2 400 i vrakpant. Vrakpant får ikke
   bruksfradrag. Aldersfradrag følger standardtabellen; alternativ beregning er
   ikke implementert. 20-årstrinnet gjelder fra 1. januar i året bilen blir 20.
4. Nettokostnad = innkjøp + frakt + øvrige kostnader + reserve + engangsavgift.
5. Norsk kundepris fra modellen antas å inkludere alle avgifter. Trekk fra
   engangsavgiften. Av beløpet over 300 000 trekkes deretter 25/125 som utgående
   moms. Inntekt etter denne momsen minus nettokostnad er kalkulert margin.
6. Nødvendig kundepris finnes ved å legge marginmålet på nettokostnaden,
   skille ut engangsavgiften, legge 25 % moms på bilvederlag over 300 000,
   og legge engangsavgiften tilbake. Maks kjøpspris beregnes baklengs fra
   modellprisen med samme momsbehandling.

Importmoms er **ikke en ekstra kostnad** i denne forhandlerkalkylen fordi
full fradragsrett er forutsatt. Kontantbehov, importdeklarasjon og oppgjør av
eventuelt utenlandsk momsdepositum er ikke beregnet. Kalkylen må derfor ikke
brukes som en privatimportkalkulator eller som grunnlag for mva-meldingen.

Fraktbeløp tolkes foreløpig som kostnad etter eventuell fradragsrett. Øvrige
kostnader omfatter f.eks. transportgebyrer, kontroll/skilter, klimagassavgift,
klargjøring, garanti, finansiering og vinterhjul. `other_costs_nok=null` gjør at
de vises som foreløpig null og at bilen merkes for kontroll. Reserve og
valutabuffer er eksplisitt satt til null frem til bedre forutsetninger foreligger.

## Modellgrunnlag og kontroll

`score_norwegian_prices` bruker `bilradar_scorer.scorer_biler`, inklusive
variantklassifisering, lookup, peer-fallback og manuelle prisoverstyringer.
Den tunge ML-modellen lastes ikke av denne CLI-en. Scoreren bruker eksisterende
S3-konfigurasjon hvis tilgjengelig, ellers den innsjekkede lookup-tabellen.
Prisanslag fra en lokal tabell er ikke dokumentasjon på dagens markedsverdi.
BilRadar-loggen viser hvilken kilde som ble brukt; den lokale filens mtime kan
reflektere utsjekkingstid, og beviser ikke at salgsdataene er ferske.

Prisgrunnlaget er norske biler. Forskjeller i garanti, importhistorikk,
utstyr og oppnåelig salgspris må kontrolleres før et kjøp. Et kandidatflagg
er et regneresultat basert på oppgitte forutsetninger, ikke en kjøpsanbefaling.
Etterprøv at modellens totalpris og ditt planlagte kundetilbud har samme
kostnadsomfang, bl.a. eventuelle omregistreringsgebyrer i sammenligningsbiler.

Tester, inklusive håndregnet moms/margin og faktisk BilRadar-oppslag:

```bash
python -m unittest discover -s tests -p 'test_import_radar.py' -v
```

## Regelkilder kontrollert 6. september 2026

- [Momsgrense fra 1. januar 2026](https://www.regjeringen.no/no/aktuelt/endringer-i-lover-og-forskrifter-fra-1.-januar-2026-fra-finansdepartementet/id3144251/)
- [Vektkomponent i engangsavgiften](https://www.skatteetaten.no/person/avgifter/bil/importere/engangsavgift/hva-er-engangsavgiften/)
- [Bruksfradragstabell, med dato- og 20-årsregelen](https://www.skatteetaten.no/satser/bruksfradragstabellen/)
- [Vrakpantavgift](https://www.skatteetaten.no/satser/vrakpantavgift/)
- [Engangsavgift utenfor momsgrunnlaget, § 4-2](https://www.skatteetaten.no/rettskilder/type/handboker/merverdiavgiftshandboken/2023/M-4/M-4-2/)
- [Fradrag for kjøretøy som salgsvare](https://www.skatteetaten.no/rettskilder/type/handboker/merverdiavgiftshandboken/2023/M-8/M-8-4/M-8-4.2/)
- [Mobile.de API-tilgang](https://services.mobile.de/)
