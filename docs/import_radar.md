# ImportRadar – Søk nå og importkalkyle

ImportRadar sammenligner normaliserte utenlandske annonser med eksisterende
BilRadar-priser. Frakt er satt til **13 000 NOK fra Tyskland**, **8 000 NOK fra
Sverige**, og marginmålet er **30 000 NOK etter oppgitte kostnader og moms**,
før selskapsskatt og faste driftskostnader. Innstillingene kan endres i
`config/import_radar.json`; frakt kan også overstyres per bil.

## Status og avgrensning

Siden **`/bil/import-radar/`** er lagt til i Flask-appen med lenke fra bilmenyen.
Oppgi merke, modell, år, maks km, hjuldrift og eventuelt minste batteristørrelse.
«Søk nå» starter en jobb som leser offentlige søkeresultater og enkeltannonser
fra begge land og sammenligner dem med BilRadar. Resultatet viser kildestatus,
innkjøp/frakt, norsk hurtigpris, nødvendige kundepriser og marginer når dataene
er tilstrekkelige. Annonsert nettopris behandles som et **ubekreftet scenario**.

Begge tomme kursfelt gir automatisk henting av ECBs siste referansekurser.
Oppgi begge kursfeltene for å bruke bankkurser; valutapåslag kan legges til.
Frakt og kostnader i nettskjemaet gjelder det enkelte søket. JSON-kalkylens
konfigurasjonsfil endres ikke av skjemaet.

Søket leser første resultatside og kontrollerer høyst 20 detaljer per land,
med maks 95 sekunder per kilde. Antall ønskede treff er 1–10 per land (UI: 3, 5
eller 10). Det er **ikke en uttømmende søking av hele markedet**. Bytbil har
prisgulv på 50 000 SEK for å redusere månedsprisannonser. Detaljfeltene sjekkes
mot filtrene; ukjent batteri/hjuldrift blir ikke godkjent mot et aktivt krav.
Tysk modellår anslås fra registreringsåret og merkes for kontroll.

De fire opprinnelige søkelenkene er bevart i konfigurasjonen med
`collection_status=not_connected`; det nye skjemaet lager egne søk.
**Ingen daglig tidsplan eller varsling er aktivert.** Eksisterende kupp-vakt
og workflows er uendret. Den gamle JSON-kalkylen kan fortsatt kjøres separat.

Live nettlesertest 6. september 2026 bekreftet at begge nettsteder kunne leses
uten innlogging, inklusive filtrering, stigende prissortering og enkeltannonser.
Direkte HTTP-lesing av begge søkeresultater og detaljsider ga også HTTP 200.
Parserne er prøvd på disse faktisk hentede HTML-sidene, og hele dataløpet er
kjørt på et gjenspilt uttrekk med ekte BilRadar-oppslag. En senere, fersk full
kjøring av den ferdige nettverksjobben ble avbrutt av testmiljøets nettverks-
godkjenning. Den lokale visuelle forhåndsvisningen ble sperret av nettleser-
miljøet. Flask-endepunkter, bakgrunnsjobber, HTML-rendering og JavaScript-syntaks
er testet separat. **Full nettverkskjøring og visuell kontroll på målserveren
gjenstår før løsningen er produksjonsverifisert.** Kia EV6 er referansemodellen
som er kontrollert; andre modellnavn er ikke live-verifisert.

Mobile.de-modellfilteret løses fra én offentlig modellside og bekreftes mot
sidens modellnavn. Ukjent modell eller endret HTML gir en synlig kildefeil.
HTTP-feil og botkontroll gir stopp, uten omgåelser, nye identiteter eller
automatisk gjentakelse. Mobile.de Search API er en separat mulig integrasjon.
Kildene må levere faktisk selgerland, kontantpris og valuta; Mobile.de har
også annonser utenfor Tyskland. Mobile-søk 2 mangler momsfilter.

## Drift og kjøring uten nettsiden

Nettjobber lagres i SQLite, delt mellom Flask-workerne. Maks to jobber kan være
aktive samtidig, én per nettleserøkt. CSRF-token og økteierskap beskytter søk,
polling og nedlasting; eksisterende tilgangskode i hovedappen gjelder siden.
Jobber eldre enn 24 timer slettes ved neste søk. Avbrutte jobber får feilstatus
etter fire minutter. Standard database er `/tmp/prisanalyse-import-radar/jobs.sqlite3`;
sett `IMPORT_RADAR_DB_PATH` til en skrivbar fil på samme disk for alle workers.
Ved flere serverinstanser trengs delt jobb-/kølagring. Ingen nye pakker er
nødvendige utover eksisterende requirements (requests, BeautifulSoup, Flask,
pandas/numpy og eksisterende BilRadar-avhengigheter).

Samme søk kan kjøres fra terminalen og senere fra en tidsplanlegger:

```bash
python -m scripts.import_radar_search --make Kia --model EV6 \
  --year-from 2022 --max-km 90000 --drive AWD --min-battery-kwh 77 \
  --per-source 5 --output rapporter/import-radar
```

Kommandoen henter valutakurser automatisk og skriver HTML/JSON. `--help` viser
frakt, margin, øvrige kostnader og egne valutakurser. Begge kildefeil gir exit
1; delvis resultat lagres med kildefeil i rapporten. Rapporten har ingen
garantert lagring mellom kjøringer dersom samme filnavn brukes. Varsling,
VIN-deduplisering mellom land og lagret prishistorikk er ikke implementert.

Der bare registreringsmåned er kjent, bruker søkejobben siste dag i måneden
som konservativt anslag og setter `registration_date_estimated=true`.
Valgfri anslått egenvekt brukes bare ved manglende egenvekt og setter
`weight_estimated=true`. Begge markeringene gir alltid kontrollbehov også ved
senere bruk av JSON-kalkylen. Totalvekt brukes aldri som egenvekt.
`net_scenario_calculation` viser margin ved annonsert nettopris uten å endre
den ordinære brutto-/bekreftet-eksportkalkylen eller gi et kjøpsklart flagg.

## Verifisert annonsetest

`examples/import_radar_observed_2026-09-06.json` inneholder to faktiske annonser
lest fra detaljsidene i nettleseren. Dette er et historisk testuttrekk, ikke
en oppdatert anbefalingsliste. Søket var Kia EV6, fra 2022, maks 90 000 km,
alle hjuldrifter. Den tyske 58 kWh RWD-bilen oppfyller derfor ikke AWD-kravet
i brukerens opprinnelige EV6-søk. Bytbil-søket brukte også momsfilter.

Observerte forhold som annonsehenteren må håndtere:

- Bytbil viser primærpris uten moms i søkeresultatet når momsfilteret er på,
  men enkeltannonsen viste bruttopris først. Les prisetikettene, ikke rekkefølgen.
- Leasing, leasingovertakelse, månedsbeløp og sponsede plasseringer må skilles
  fra ordinære kontanttilbud før sortering og rangering.
- Svenske mil ganges med 10. Totalvikt er ikke avgiftsrelevant egenvekt.
- Mobile viste registreringsmåned, ikke dag. Modellår må skilles fra
  registreringsår. Bytbil viste modellår 2023 og kjøretøyår 2022 som ulike felter.

Ny `purchase_observation` i rapporten viser bruttoinnkjøp og frakt i NOK selv
om avgiftsdata mangler. Valgfri `advertised_net_amount` gir et separat
ubekreftet nettoscenario. Denne nettoprisen bekrefter ikke eksportvilkår og
påvirker aldri margin- eller kandidatberegningen. Tallene inkluderer ikke
norske avgifter, øvrige kostnader eller margin.

Gjenskaping med ECBs referansekurser 4. september 2026 (ikke bankens kjøpskurs):
1 EUR = 10,8035 NOK og 11,1005 SEK; dermed 1 SEK = 0,9732444484 NOK.
Kilder: [ECB NOK](https://www.ecb.europa.eu/stats/policy_and_exchange_rates/euro_reference_exchange_rates/html/eurofxref-graph-nok.en.html)
og [ECB SEK](https://www.ecb.europa.eu/stats/policy_and_exchange_rates/euro_reference_exchange_rates/html/eurofxref-graph-sek.en.html).

```bash
python -m scripts.import_radar examples/import_radar_observed_2026-09-06.json \
  --eur-nok 10.8035 --sek-nok 0.9732444484 \
  --registration-date 2026-09-06 --output /tmp/import-radar-observed
```

Begge biler matches til riktig batterivariant i lokal BilRadar-tabell. Tysk bil
har hurtigpris ca. 247 596 NOK og nettoinnkjøp + frakt ca. 239 506 NOK, som ikke
gir rom for marginmålet selv før norske avgifter og øvrige kostnader. Svensk
bil har hurtigpris ca. 375 235 NOK og nettoinnkjøp + frakt ca. 271 944 NOK.
Svensk eksportpris, egenvekt, utstyr/garanti og øvrige kostnader må avklares
før lønnsomhet kan fastslås. Modellprisene er testresultater fra innsjekket
lookup, ikke dokumentasjon på oppnåelig salgspris i dag.

Begge beholder status `mangler_kalkyledata`: tysk eksakt registreringsdato og
svensk egenvekt mangler. Testen gir ingen kjøpsklare kandidater og starter ingen
daglig jobb. Dette gjelder den opprinnelige JSON-filen; den nye søkejobben
kan også beregne et eksplisitt anslag fra registreringsmåned som beskrevet over.

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
