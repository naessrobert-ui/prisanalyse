# NewsWeb-monitor – kjører timevis i GitHub Actions

## Datakilde
Kilde: Euronext Oslo Børs NewsWeb sitt `newsreader`-API (`api3.oslo.oslobors.no/v1/newsreader`).
Det er avtalt at meldinger kan hentes. Prototypen begrenser seg til `XOSL`, henter liste (`list`), innhold (`message`) og PDF-vedlegg (`attachment`). Rentereguleringer, ordinære tilbakekjøp og finanskalendere filtreres bort. Dette er ikke en fullstendig børsinstrument-master: vurder innføring av offisiell aksjeliste før produksjon.

GitHub Actions: `Actions → NewsWeb AI monitor`.
- Planlagt kjøring i **norsk tid (Europe/Oslo)**: hvert 15. minutt kl. **06:00–09:45**, deretter **hver hele time kl. 10:00–23:00**. Ingen kjøringer mellom midnatt og 06:00. GitHub justerer automatisk for sommer-/vintertid, men kan forsinke eller droppe planlagte kjøringer.
- Manuell kjøring via `Run workflow`: velg `newsweb` og `dry_run=true`.
- Lagre rapportene fra hver kjøring via `Summary` og `Artifacts (latest.json / latest.md)`.
- ID-baserte dubletter og målestørrelser fra tidligere kvartaler beholdes i Actions-cache. Cache er **ikke varig lagring**; for senere produksjon anbefales privat database/S3.


## Overvåking av oppdateringer (22.10.2026)

Hver kjøring logger nå: antall hentet fra NewsWeb, innenfor tidsvinduet,
allerede sett i cache, nye kandidater, rutinemeldinger/filtrert, AI-analysert,
feil ved detaljhenting, AI-feil og antall som ble utsatt pga. kjøregrensen.
Grensen på 120 brukes etter at tidligere sette og utdaterte meldinger er
sortert bort, slik at gamle meldinger ikke fortrenger nye.

GitHub Actions viser en egen **NewsWeb healthcheck**-tabell i Summary.
Mellom kl. 08 og 17 på hverdager (norsk tid) blir kjøringen **rød** dersom
kilden gir null XOSL-meldinger, eller hvis nye kandidater finnes, men alle
feiler i AI/detaljhenting. Gammel kilde (>4 timer) og ingen nye analyser
gir **advarsel** uten å feilmerke normalt stille perioder.
Dette er GitHub Actions-varsler, ikke Pushover-meldinger.

Den offentlige `latest.json` får nå `last_checked_at` og
`monitor_stats` ved hver vellykket kildeavlesning, også når det ikke er
nye nyheter. `generated_at` beholdes som siste oppdatering av selve
nyhetssakene. Ingen fulltekst, private konsensusdata eller hemmeligheter
inkluderes i diagnostikken. Nettsiden oppdateres etter neste GitHub-publisering.

## PDF-analyse
Ved en ny resultatmelding:
1. Last ned vedlagt kvartalsrapport som PDF. Filstørrelse begrenses til 22 MB.
2. Trekk ut tekst fra inntil 26 sider med pypdf. Scannede PDF-er uten maskinlesbar tekst krever en separat OCR-løsning.
3. Hvis `OPENAI_API_KEY` finnes: hent omsetning, EBITDA, EBIT, resultat etter skatt, EPS og driftskontantstrøm for **kvartalet**, samt sammenlignbare tall fra samme kvartal året før.
4. Hent forrige kvartals rapport fra samme utsteder via NewsWeb hvis denne ikke allerede er i cache. Sammenlign også med forrige kvartal.
5. Lag tabell med tall, prosentendringer og kildehenvisning til PDF; ikke bland YTD, halvår, valutaer eller ikke-sammenlignbare definisjoner.

PDF-vedlegget behandles i minnet på runneren og blir **ikke publisert eller arkivert** som GitHub-artifact; bare resultat, referanse og URL. Ikke anta at nedlasting av rapporter gir rett til offentlig videredistribusjon.

## Konsensus
Hvis dere har lovlig tilgang til pre-release Bloomberg / annen estimatfeed, kan den oppgis som `CONSENSUS_JSON_URL` (GitHub repository secret), der responsen er et ticker- og kvartalsindeksert JSON-objekt:

```json
{
  "STB": {
    "2026Q3": {
      "ebit": {
        "value": 1110.0,
        "unit": "NOK million",
        "source": "EKSEMPEL – BYTT UT MED EKTE ESTIMATKILDE",
        "asof": "2026-10-20T17:00:00+02:00"
      }
    }
  }
}
```

**Tallet er kun illustrasjon og må aldri tas som faktisk konsensus.**
Programmet regner bare surprise når verdi, enhet og punkt-i-tid før børsmeldingen er dokumentert. Ellers står konsensus «ikke tilgjengelig». Automatisert Bloomberg/BQuant-eksport krever egen integrasjon og eventuelle rettigheter.

## Oppsett og sikkerhet
- Legg `OPENAI_API_KEY` i **GitHub Settings → Secrets and variables → Actions → Secrets** for reell AI-analyse. Mangler den, kjører regelbasert klassifisering, PDF leses, men nøkkeltall ekstraheres ikke.
- Pushover krever `PUSHOVER_APP_TOKEN` og `PUSHOVER_USER_KEY` som Secrets. Den er deaktivert ved standardoppsett. Den kan senere aktiveres med `NOTIFICATIONS_ENABLED=true` i Actions Variables og `NEWSWEB_DRY_RUN=false`.
- Unngå API-nøkler i kildekode og rapporter. Repositoryet `prisanalyse` er offentlig.
- Nettsiden `/newsweb/` viser fremdeles tidsstemplet manuell stikkprøve, ikke de timebaserte jobbrapportene. En framtidig godkjent, autentisert dataflyt fra Actions til privat lagring kan knyttes til nettsiden.

## Lokal test
```sh
python -m pip install "pypdf>=5,<8"
python -m unittest discover -s newsweb_monitor/tests -v
SOURCE_MODE=newsweb DRY_RUN=true python -m newsweb_monitor.monitor
```


## Dashboard på prisanalyse.no/newsweb/

GitHub Actions skriver et begrenset offentlig snapshot til `newsweb_monitor/public/latest.json` på main. Kun tickere, offentliggjorte overskrifter, korte maskin-/regelbaserte sammendrag, score og PDF-lenker; **aldri original fulltekst, PDF-binary eller konsensustall**.

Nettsiden leser dette fra raw.githubusercontent.com, med 120 sekunders mellomlagring. En kjøring med 0 nye meldinger beholder siste oversikt. Første produksjonskjøring migrerer 119 behandlede meldinger fra GitHub Actions-artifact 39610134457.

Repoet er offentlig! Ikke bruk dette som distribusjonskanal for Nordeas interne vurderinger eller betalte Bloomberg-data uten egen godkjenning. Ved behov for private analyser: flytt databasen til en privat tjeneste med innlogging.


## Nullstilling av gamle regelbaserte meldinger (21.10.2026)
Det gjøres et kontrollert engangsrestart ved å sette `NEWSWEB_GENERATION` i
`newsweb_monitor/generation.py`. Ved endring av denne versjonen starter
dedupliserings-cache og `reports_by_ticker` tomme, og nettsidens nye snapshot
erstatter den gamle historikken i stedet for å slå den sammen. Backfill av
gamle artifacts er deaktivert. AI-nøkkel er påkrevd for NewsWeb-modus;
ved AI-feil holdes meldingen tilbake og prøves igjen ved senere kjøring.
Restart gir ikke Pushover-varsler.
NB: Gamle commits og Actions artifacts ligger fortsatt i GitHub-historikken;
dette fjerner data fra den **aktive** nettsiden, ikke fra Git-historikken.


## Finansavis-utgave / journalistiske overskrifter
Fra generasjon `newsroom-2026-10-21-v2` skriver AI **headline** og **lead**
som egne felter. Nettsiden viser disse først, og skjuler selskapets opprinnelige
tittel og den analytiske vurderingen i utvidbare seksjoner. Overskrifter må
beskrive dokumenterte tall/endringer, ikke antatte konsensusoverraskelser.
En engangsoppdatering av generasjonen bygger ny historikk; gammel publisert
versjon beholdes kun i Git-commit-historikken.

## Forventninger / estimatfiler
Konsensus må dokumentere `ticker`, `period`, `metric`, `value`,
`unit`, `source`, `asof` (før meldingstidspunkt). Vi anbefaler
Bloomberg/BQuant-eksport til godkjent, privat lagring (f.eks. privat S3-bøtte).
Ingen proprietære konsensusdata skal legges i offentlig repository,
GitHub Action-artifacts eller offentlig nettside. BQuant-eksport kan senere
leses av en separat privat jobb som gir dokumentert resultatavvik i tråd med
lisens og interne retningslinjer.

## Privat Bloomberg-fil: manuell opplasting
Nettside: `/newsweb/konsensus/`. Administrator logger inn med
`NEWSWEB_CONSENSUS_ADMIN_TOKEN` (Render miljøvariabel, aldri i Git).
Legg til `NEWSWEB_CONSENSUS_BUCKET`: **egen privat AWS S3-bøtte** med alle
fire Public Access Block-flagg aktivert (sjekkes av appen før lagring).
Render-tjenestens AWS-identitet trenger `s3:PutObject`, `s3:GetObject`,
`s3:ListBucket` og `s3:GetBucketPublicAccessBlock` for bare denne bøtten.
Bruk eksisterende `AWS_REGION` med tilsvarende region.

Excel-filen må inneholde `Data`-arket, Bloomberg-symboler i A,
selskapsnavn i B, perioder (`26Q3` eller `Year`) på rad 1 og BEST-felter
på rad 2. Formlenes bufrede **tallverdier** leses, ikke Bloomberg-addin
eller eksterne linker. Regnskapsåret for `Year` må velges ved opplasting,
ellers ignoreres disse kolonnene. `#N/A` hoppes over.

VIKTIG: Den aktuelle 21.10.2026-filen har feltet `net int inc` i K2,
uten BEST-prefiks. Det er ikke et verifisert estimatfelt og **importeres
derfor ikke som konsensus**. Kvartalsvis bank-NII krever et eget,
verifisert Bloomberg BEST-estimatfelt.

Sammenligninger vises bare i administratorens nyhetsvisning, og er
merket **indikative** frem til valuta, enheter og resultatdefinisjoner er
kontrollert. Det siste estimatsnapshotet som ble lastet opp før
resultatpublisering velges automatisk; aldri tilbakedater opplastinger.

## Dagens simulering ved konsensusopplasting
Bruker eksisterende innlogging på prisanalyse.no via `SITE_ACCESS_CODE`
og `session['access_granted']`. Ingen egen `NEWSWEB_CONSENSUS_ADMIN_TOKEN`.
For sikkerhet er rutene for opplasting stengt dersom site-passordet ikke er
konfigurert, selv om resten av nettstedet åpnes.
Alle innloggede brukere med felles kode kan lese og laste opp private estimater.

Ved hver vellykket opplasting kjøres en SIMULERT gjennomgang av ALLE
meldinger i den lokale Oslo-datoen i siste publiserte NewsWeb-JSON.
Databasen antar da som TEST at estimatene var tilgjengelige 24 timer før
virkelig opplasting. Virkelig tidsstempel endres ikke.
Testresultatene vises i den private opplastingssiden og lagres **kun**
i den private S3-snapshotfilen, aldri offentlig eller i GitHub.
Det betyr at testen bruker allerede ekstraherte tall fra dagens meldinger;
mangler PDF-analyse, rapportperiode eller sammenlignbare tall markeres dette.
Resultater fra testen kan IKKE brukes som historisk konsensusoverraskelse.

## Manuell oppdatering fra NewsWeb-siden

Innloggede brukere med `SITE_ACCESS_CODE` får knappen **Kjør NewsWeb nå** på
`/newsweb/`. Knappen starter en `workflow_dispatch` på `main` og ber om
historisk gjennomgang av inntil fire tidligere kvartalsrapporter for opptil
fire aktuelle selskaper. Dette er en asynkron GitHub Actions-jobb: nettsiden
viser ikke resultatet før kjøringen er fullført og GitHub-publiseringen lykkes.

For å aktivere knappen: legg `NEWSWEB_GITHUB_TOKEN` som **secret environment
variable på Render**. Bruk et finmasket GitHub-token som kun har tilgang til
`naessrobert-ui/prisanalyse`, med **Actions: Read and write** og
**Metadata: Read**. Ikke legg tokenet i GitHub-koden eller i HTML.
Manglende token gir en tydelig feilmelding i brukergrensesnittet.

Endepunktet krever aktiv innlogging, CSRF-token og minst tre minutter mellom
forsøk per web-prosess. GitHub-workflowen har også en concurrency-gruppe.
Historisk innhenting er begrenset for å unngå høye kostnader ved hver
manuell kjøring. Planlagt oppdatering fortsetter uavhengig av knappen.

## Automatisk historikkbygging

I tillegg til knappen forsøker første planlagte NewsWeb-kjøring rundt kl. 06
(norsk tid) å hente fire tidligere kvartalsrapporter for inntil fire nylig
rapporterende selskaper. Også en kodeoppdatering på `main` starter en slik
begrenset gjennomgang. Fremdrift lagres per ticker og kvartal i GitHub Actions
state-cache, slik at senere kjøringer går videre til andre selskaper.
