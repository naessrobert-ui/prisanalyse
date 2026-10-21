# NewsWeb-monitor – kjører timevis i GitHub Actions

## Datakilde
Kilde: Euronext Oslo Børs NewsWeb sitt `newsreader`-API (`api3.oslo.oslobors.no/v1/newsreader`).
Det er avtalt at meldinger kan hentes. Prototypen begrenser seg til `XOSL`, henter liste (`list`), innhold (`message`) og PDF-vedlegg (`attachment`). Rentereguleringer, ordinære tilbakekjøp og finanskalendere filtreres bort. Dette er ikke en fullstendig børsinstrument-master: vurder innføring av offisiell aksjeliste før produksjon.

GitHub Actions: `Actions → NewsWeb AI monitor`.
- Planlagt kjøring én gang hver time, ca. 27 minutter over hel (UTC). GitHub kan forsinke en planlagt kjøring.
- Manuell kjøring via `Run workflow`: velg `newsweb` og `dry_run=true`.
- Lagre rapportene fra hver kjøring via `Summary` og `Artifacts (latest.json / latest.md)`.
- ID-baserte dubletter og målestørrelser fra tidligere kvartaler beholdes i Actions-cache. Cache er **ikke varig lagring**; for senere produksjon anbefales privat database/S3.

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
