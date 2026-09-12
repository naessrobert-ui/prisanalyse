# Yr og Google for Bergen og Kvamskogen

Sammenligningssiden er `/ver/sammenlign` med valgfri `?sted=bergen` eller
`?sted=kvamskogen`. Data leveres av `/ver/api/sammenlign` for de samme to stedene.
Begge rutene er offentlige slik at Visitkvamskogen kan vise sammenligningen.
Andre sider beholder gjeldende tilgangskrav.

## Oppsett og utrulling

1. Aktiver Google Weather API og fakturering i Google Cloud.
2. Begrens API-nøkkelen til Weather API. Lagre den som
   `GOOGLE_WEATHER_API_KEY` i **Python-tjenesten for prisanalyse på Render**.
   Nøkkelen må aldri ha `VITE_`-prefiks eller bygges inn i nettleserkode.
3. Rull ut prisanalyse-endringen først. «Save only» i Render får effekt når
   denne utrullingen skjer. Kontroller begge stedene på `/ver/sammenlign`.
4. Rull deretter ut Visitkvamskogen-endringen, som viser samme side i en iframe.
   Den statiske Visitkvamskogen-tjenesten trenger ingen Google-nøkkel.

Ved eventuell IP-begrensning av nøkkelen må Google tillate Render-tjenestens
utgående IP-adresser. Nettsteds-/referrerbegrensning passer ikke til serverkall.

## Databehandling

- 48 timer fra Google, maksimalt to sider à 24 timer per oppdatering og sted.
- MET Locationforecast 2.0 `complete` hentes direkte; dette er et MET-punktvarsel,
  ikke en avlesning av nettsiden yr.no.
- Samme koordinater og UTC-intervaller hos begge leverandører. Lokal visning
  bruker alltid Europe/Oslo, inkludert sommer-/vintertid.
- Google-vind konverteres fra km/t til m/s, nedbør vises som mm vann.
- METs `next_1_hours` brukes til timevis nedbør. Sekstimersverdier splittes ikke.
  Manglende felt forblir `null`; et ufullstendig døgn får ingen nedbørssum.
- METs temperatur/vind er øyeblikksverdier ved starten av intervallet. Google
  leverer timeintervaller; forskjellen forklares under grafen.
- Google oppgir ikke modellens oppdateringstid i dette svaret. Hentetid merkes
  derfor separat fra METs `updated_at`. Ingen bestemt WeatherNext-versjon loves.
- Google-data ligger bare i prosessminne, med utløp etter 55 minutter og en
  timer som fjerner dem også uten nye forespørsler. Selve *siden* lagrer ingen
  historikk. Cron-jobben `vaer-treffsikkerhet` gjør det derimot: den lagrer
  begge varslene time for time for å kunne score dem mot observasjoner i
  ettertid. Se `docs/weather-scoreboard.md`, inkludert det åpne spørsmålet om
  hvor lenge Googles varsler kan oppbevares.
- JSON-svar bruker `Cache-Control: no-store`. Klienten laster på nytt ved utløp
  og fjerner gamle data dersom neste forespørsel feiler. Ingen localStorage.
- Ved feil hos én leverandør vises den andre fortsatt. Feil caches i ett minutt
  for å unngå gjentatte kall. Upstream-feilmeldinger med nøkkel/URL eksponeres ikke.
- Cache/lås er per serverprosess. Flere workers/instanser gir flere Google-kall;
  dette er ikke en global Redis-cache. To sider ganger to steder gir fire kall
  per oppdatering per worker når begge stedene brukes.

## Verifikasjon

`python -m pytest tests/test_weather_comparison.py -q` tester enheter, UTC-matching,
sideinndeling, manglende data, feil uten nøkkellekkasje, utløp og stedbegrensning.
Kjør i tillegg `node --check` på skriptblokken og bygg Visitkvamskogen med
`npm run build`.

Lokal validering bruker ingen privat Google-nøkkel. Ekte Google-svar og visuell
kontroll av iframe på begge produksjonsdomener må kontrolleres etter utrulling.
Nettleseren i utviklingsmiljøet kunne ikke åpne den lokale forhåndsvisningen.

Referanser:
- https://developers.google.com/maps/documentation/weather/reference/rest/v1/forecast.hours/lookup
- https://developers.google.com/maps/documentation/weather/reference/rest/v1/Wind
- https://docs.api.met.no/doc/ForecastJSON.html
- https://cloud.google.com/terms/maps-platform/eea/maps-service-terms
- https://developers.google.com/maps/documentation/weather/policies
