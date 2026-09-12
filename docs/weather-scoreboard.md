# Hvem traff best – Yr eller Google?

`/ver/sammenlign` viser hva de to leverandørene tror akkurat nå. Denne loggen
svarer på det neste spørsmålet: hvem hadde rett? Én gang i timen lagres begge
varslene for de neste 48 timene. Når timen er over hentes den faktiske
observasjonen fra Frost, og varslene scores mot den.

## Kjøring

```
python -m scripts.weather_scoreboard_run                      # logg (cron, 20 over hver time)
python -m scripts.weather_scoreboard_run --rapport            # scoretabell, siste 14 døgn
python -m scripts.weather_scoreboard_run --rapport --dager 60
python -m scripts.weather_scoreboard_run --rapport --fasit interval
python -m scripts.weather_scoreboard_run --rapport --json
```

### Hvor rapporten kjøres

Dataene ligger på S3, ikke på disk. Rapporten må derfor kjøres et sted som har
`S3_BUCKET_NAME` og AWS-nøklene.

**På Render** – enkleste vei. Åpne Shell på web-tjenesten for prisanalyse, der
variablene allerede er satt, og kjør kommandoen fra prosjektroten.

**Lokalt** – fra roten av prisanalyse-repoet, med prosjektets virtuelle miljø:

```
python -m scripts.weather_scoreboard_run --rapport --dager 7
```

Kommandoen laster `.env` selv. Men `.env` har i dag ikke `S3_BUCKET_NAME`, og
uten den leser rapporten en tom lokal mappe i stedet for Renders historikk. Legg
til samme bøttenavn som Render bruker:

```
S3_BUCKET_NAME=<samme bøtte som Render>
```

Rapporten skriver alltid ut hvor den leser fra, og sier fra når den ikke fant
noen lagrede varsler i det hele tatt – slik at en tom bøtte ikke kan forveksles
med «ingen forskjell mellom leverandørene».

### Cron

Cron-tjenesten `vaer-treffsikkerhet` i `render.yaml` kjører `20 * * * *`. Tjue
over er valgt fordi MET da har lagt ut timens varsel, og Frost har rukket å
publisere observasjonen for forrige hele time. Ved :00 finnes den ikke ennå.

Hver kjøring henter i tillegg de siste 12 timene med observasjoner på nytt.
Frost leverer av og til forsinket og kvalitetskontrollerer verdier i ettertid;
nye verdier erstatter gamle på samme nøkkel i stedet for å legge seg oppå.

Rapporten trenger minst tre døgn med data før den kårer en vinner, og i praksis
en uke eller to før intervallene blir smale nok til å si noe.

## Fasit

| Sted | Frost-stasjon | Avstand fra varselpunktet |
| --- | --- | --- |
| Kvamskogen | SN50310 Kvamskogen – Jonshøgdi | 1,5 km |
| Bergen | SN50540 Bergen – Florida | 1,2 km |

Begge leverer `air_temperature`, `wind_speed`, `sum(precipitation_amount PT1H)`
og `max(wind_speed_of_gust PT1H)` på timesoppløsning. Krever `FROST_CLIENT_ID`.

Skydekke scores ikke. Frost har bare `cloud_area_fraction` på seks timers
oppløsning i Bergen og ingenting på Kvamskogen, så det finnes ingen timesfasit.

## Tidskonvensjoner

Dette er stedet der en stille feil ville gjort hele scoren verdiløs.

- Varselstimen er intervallet `[valid_start, valid_start + 1t)` i UTC, samme
  nøkkel som `/ver/api/sammenlign` bruker.
- Frost `air_temperature` og `wind_speed` er øyeblikksverdier **ved**
  referansetiden, og pares mot `valid_start`.
- Frost `sum(precipitation_amount PT1H)` og `max(wind_speed_of_gust PT1H)`
  gjelder timen som **slutter** ved referansetiden, og pares derfor mot
  `valid_start + 1t`. Verifisert mot `accumulated(precipitation_amount)`:
  `sum(T) == akkumulert(T) − akkumulert(T−1t)`.
  `test_hour_accumulated_elements_are_shifted_back_one_hour` låser konvensjonen.

Observasjonene lagres rå, én rad per element og referansetid. Skulle
konvensjonen vise seg å være feil, kan alt regnes om uten å hente Frost på nytt.

## Hva som gjør sammenligningen rettferdig – og hva som ikke gjør det

**Samme timer for begge.** Bare timer der begge leverandørene har en verdi *og*
fasiten finnes blir med. Uten det kravet ville en leverandør kunne vinne på å la
være å svare når været er vanskelig. MET oppgir bare sekstimersnedbør lenger
ut i varselet; de timene faller ut for begge.

**Samme normalisering.** Loggen kaller `weather_comparison.fetch_forecast`, den
samme koden siden bruker. Enhetene er dermed konvertert likt, men cachen er
omgått slik at hver time gir et ferskt varsel.

**Øyeblikksverdi mot timesverdi.** Yr oppgir temperatur og vind som en
øyeblikksverdi ved timens start, Google som en verdi for hele timen. Frost
måler øyeblikksverdier. Det finnes altså ingen nøytral fasit for disse to
elementene, og valget er ikke uskyldig: forskjellen mellom observasjonen ved
timens start og snittet over timen er i snitt 0,3 °C og 0,4 m/s på disse
stasjonene, mot en typisk varselfeil rundt 1 °C og 1,5 m/s.

Derfor er grunnlaget valgbart. `--fasit instant` (standard) måler mot
observasjonen ved timens start, altså Yrs konvensjon. `--fasit interval` måler
mot snittet av timens start og slutt, altså Googles. **Kjør begge.** Holder
konklusjonen i begge, er den reell. Snur den, er forskjellen for liten til å
telle. Nedbør og vindkast gjelder hele timen hos begge og berøres ikke.

**Frost er METs eget målenett.** MET assimilerer disse observasjonene inn i
modellen som ligger bak Yr. Google gjør det etter alt å dømme ikke i samme grad.
Det gir Yr en fordel som ikke lar seg fjerne med norske data, og som er størst
på korte lead-tider. Les et jevnt resultat på 1–6 timer som en Google-seier.

**Punkt mot stasjon.** Varselet gjelder et punkt, observasjonen en stasjon 1–2
km unna. Den avstanden legger et gulv under begge leverandørenes feil, likt for
begge, men gjør de absolutte MAE-tallene større enn den ekte modellfeilen.

## Hva som måles

Per sted, element og lead-bøtte (1–6, 7–12, 13–24, 25–48 timer):

- **MAE** – gjennomsnittlig absolutt avvik. Hovedtallet; lavest er best.
- **Bias** – snittet av fortegnsavviket. Positivt betyr systematisk for høyt.
- **RMSE** – straffer de store bommene hardere enn MAE.
- **Yr vant** – andelen timer der Yr lå nærmere enn Google.
- **Dom** – hvem som har lavest MAE, med et 95 %-intervall for forskjellen.

Intervallet kommer fra en blokkbootstrap med hele døgn som blokker. Feilen én
time er sterkt korrelert med feilen den neste, så et vanlig konfidensintervall
ville blitt altfor smalt og gitt en «vinner» på ren støy. Spenner intervallet
over null, er svaret uavgjort – og det skal det stå.

For nedbør kommer i tillegg en kategorisk score på terskelen 0,1 mm: andel
riktige timer, POD (andelen nedbørstimer som ble varslet) og FAR (andelen
varslede nedbørstimer som ble tørre). Millimeteravvik alene skjuler dette, fordi
en leverandør som alltid varsler null får lav MAE i et tørt klima.

## Lagring

Én parquet-fil per døgn, på S3 når `S3_BUCKET_NAME` er satt, ellers lokalt under
`data/vaer_fasit/` (gitignorert). `WEATHER_SCOREBOARD_DIR` overstyrer den lokale
plasseringen.

```
weather-scoreboard/prognoser/YYYY-MM-DD.parquet      run_hour, place, provider,
                                                     valid_start, lead_hours,
                                                     temp, rain, wind, gust, cloud
weather-scoreboard/observasjoner/YYYY-MM-DD.parquet  place, reference_time,
                                                     element, value, quality
```

Skriving er idempotent: samme nøkkel erstattes, så en kjøring som går om igjen
verken dupliserer rader eller ødelegger historikken. Volumet er omtrent 4 700
varselrader og 200 observasjonsrader per døgn, i underkant av 2 MB i måneden.

## Kostnad

Loggen legger til faste Google-kall: to sider à 24 timer per sted per kjøring,
altså 4 kall i timen og rundt 2 900 i måneden. Det kommer i tillegg til det
selve siden bruker. Skal det ned, kan cron settes til for eksempel `20 */3 * * *`
– hver varselstime får fortsatt 16 ulike lead-tider å bli målt på i stedet for
48. Se Google Maps Platform sin prisside for gjeldende satser.

Uten `GOOGLE_WEATHER_API_KEY` logges Yr alene, kjøringen feiler ikke, og
`errors` i loggutskriften sier hvorfor. Det er det som skjer lokalt.

## Åpent spørsmål: hvor lenge kan Googles varsler oppbevares?

Sammenligningssiden sletter Googles timevarsel innen én time, og
`docs/weather-comparison.md` oppgir Google Maps Platform sine vilkår som
grunnen. Denne loggen bryter med det: den må beholde varselet til timen kan
scores, og beholder det i dag videre for å kunne regne på historikken. Vilkårene
er ikke lest gjennom på nytt her, så dette er ikke avklart.

Skal oppbevaringen begrenses, trengs ikke de rå varslene for å svare på hvem som
er best. Alt rapporten regner ut – MAE, bias, RMSE, andel vunne timer og
blokkbootstrappen – kan utledes av døgnvise summer per sted, element og
lead-bøtte: antall, sum av avvik, sum av absoluttavvik, sum av kvadrerte avvik
og antall timer hver leverandør vant. Bootstrappen bruker allerede bare
døgnsummer og -antall. En slik konsolidering ville latt de rå Google-verdiene
slettes etter at timen er scoret, uten at rapporten mister noe.

Det er ikke bygget, fordi det bare lønner seg dersom vilkårene faktisk krever
det – rå verdier gjør senere reanalyse (andre terskler, andre lead-bøtter,
døgnmaks i stedet for timer) mulig.

## Verifikasjon

```
python -m pytest tests/test_weather_scoreboard.py tests/test_weather_comparison.py -q
```

27 tester dekker tidskonvensjonen, begge fasitgrunnlagene, filtrering av
ti-minuttsserier, idempotent skriving, kravet om at begge leverandørene må ha
svart, metrikkene, og at bootstrappen sier uavgjort når datagrunnlaget er ett
døgn. Frost-feil testes for at klient-ID-en ikke lekker ut i feilmeldingen.

Loggingen er kjørt mot ekte MET og Frost. Google-delen er ikke kjørt mot ekte
API her, siden nøkkelen bare finnes på Render.

Referanser:
- https://frost.met.no/api.html
- https://docs.api.met.no/doc/ForecastJSON.html
- https://developers.google.com/maps/documentation/weather/reference/rest/v1/forecast.hours/lookup
