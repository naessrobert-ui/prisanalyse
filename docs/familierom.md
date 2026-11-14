# Privat familierom på møtes.no

## Arkitektur

Render videresender bare `/familie` til HTTPS-adressen i `FAMILY_ORIGIN`.
Pi-en kjører en egen FastAPI-prosess på `127.0.0.1:8025`, med bilder og klipp i
`~/motes-familie/data/media` og metadata i SQLite. Pi-en bruker SSD-en som `/`.
Familiekoden hashes med PBKDF2. Den blir ikke sendt til GitHub eller lagret på Render.
Alle filer, også byte-range-kall for videospilling, krever en signert, HTTP-only
sesjonscookie. Den gjelder i sju døgn. Alle med familiekoden har samme tilgang og
kan slette innlegg. Bytte av koden og sesjonsnøkkelen i `access.json` logger ut alle.
Det er ett familierom, ikke en tjeneste med flere familier eller individuelle kontoer.

Ved opplasting velges **1 døgn**, **1 uke** eller **til noen sletter innlegget**.
Eksisterende innlegg beholder sin tidligere utløpstid, også det gamle valget 3 døgn.
Tidsbegrensede innlegg skjules umiddelbart ved utløp. En jobb sletter de utløpte filene
hvert minutt når tjenesten kjører, og ved neste oppstart hvis Pi-en har vært avslått.
Sletting gjelder aktive filer, ikke sikkerhetskopier eller kopier mottakerne har laget.
Ingen sikkerhetskopiering settes opp av installasjonen. Mottakerne kan laste ned,
skjermfotografere eller kopiere innhold. Dette er ikke DRM eller ende-til-ende-kryptert
lagring; HTTPS beskytter transporten. Render videresender innholdet underveis.

## Grenser

- Inntil 100 MiB per fil og 10 GiB samlet familielager, med 2 GiB fri disk som reserve.
- JPG, PNG, WebP og HEIC/HEIF (valideres som bilder), MP4/MOV og WebM (container sjekkes).
- Opptil 30 filer velges samtidig. De sendes én om gangen med fremdrift per fil.
  Ved delvis feil prøver neste forsøk bare de mislykkede filene igjen.
- HEIC/HEIF konverteres til JPEG (maks 4096 piksler på lengste side), roteres riktig
  og får opprinnelig opptakstid lagret separat. Ingen videokonvertering. Telefonklipp med HEVC kan
  måtte konverteres til MP4/H.264 for å kunne spilles på alle enheter.
- Tailscale Funnel har båndbreddebegrensninger. Egnet for små familieklipp, ikke
  et generelt videoarkiv eller ubegrenset streaming.
- Pålogging begrenses til ti mislykkede forsøk per 15 minutter for hele tjenesten.
- Pi-en og internettforbindelsen hjemme må være på for å bruke familierommet.

## Installasjon på Pi-en

Kjør som `robert`, ikke som root. Bruk publisert commit-SHA som `MOTES_REF`
for en reproduserbar installasjon. Skriptet kan lastes ned og leses før kjøring:

```bash
curl -fsSL https://raw.githubusercontent.com/naessrobert-ui/prisanalyse/main/deploy/pi/familie/install.sh -o /tmp/motes-familie-install.sh
bash /tmp/motes-familie-install.sh
```

Du velger selv en familiekode på minst 12 tegn i terminalen. Ikke del den i chat.
Skriptet ber om sudo for å installere kun `motes-familie.service`. Det endrer ikke
regnskapstjenester, porter i ruteren eller eksisterende Tailscale-konfigurasjon.
En eksisterende installasjon overskrives ikke. Data og hemmeligheter ligger utenfor
Git-repoet. Python må ha venv-støtte (`sudo apt install python3-venv` ved behov).

## HTTPS og kobling til Render

Før deling: bekreft at lokal helsesjekk svarer og at `/familie/api/items` uten
sesjon gir 401. **Serve gir bare tailnet-tilgang**; Render er ikke på tailnettet.
For denne arkitekturen må derfor enten Render få tailnet-tilgang, eller Pi-tjenesten
publiseres over HTTPS. Planlagt enkel variant er **Funnel**, med familiekodeport
for innhold. Funnel gjør tjenestens innloggingsside tilgjengelig på internett;
det gjør ikke bildefilene tilgjengelige uten sesjon. Ikke publiser en diskmappe.

Les gjeldende `tailscale funnel --help` og bekreft at ingen eksisterende konfig
overskrives før aktivering. Tailscale kan kreve at HTTPS og Funnel godkjennes i
nettleseren. En egen port (8443) kan brukes; installer aldri en ny tunnel ved å
nullstille en eksisterende Serve/Funnel-konfig. Offisiell veiledning:
https://tailscale.com/docs/features/tailscale-funnel

Når lokal innlogging og uautorisert filhenting er verifisert, kan tjenesten
publiseres med følgende kommando (HTTPS på egen port):

```bash
sudo tailscale funnel --bg --https=8443 http://127.0.0.1:8025
tailscale funnel status
```

Dette åpner innloggingssiden på internett; selve familiefilene krever familiekoden.
Godkjenn eventuell aktiveringslenke fra Tailscale i nettleseren din.

Etter aktivering får man en adresse som `https://raspberrypi.<tailnet>.ts.net:8443`.
Sett denne som `FAMILY_ORIGIN` på Render-tjenesten `prisanalyse`. Ingen familiekode
eller Pi-SSH-nøkkel skal legges inn på Render. Render-koden avviser andre URL-typer
og følger ikke upstream-redirects. Kun familierommets cookie og nødvendige media-
og forespørselshoder sendes videre; Prisanalyse-innlogging videresendes ikke.

Test hele flyten på møtes.no/familie før lenken deles med familien: innlogging,
bilde, videoklipp, utlogging, uautorisert filforespørsel og sletting. En synlig lenke
fra forsiden legges først til når forbindelsen virker.

## Drift

```bash
sudo systemctl status motes-familie --no-pager
sudo journalctl -u motes-familie -n 30 --no-pager
curl -fsS http://127.0.0.1:8025/familie/health
```

HTTP-accesslogger er avslått på Pi-en for å unngå bildetekster i URL-logger.
Sikkerhetskopiering, varsling og automatisk programvareoppdatering er ikke satt opp.

## Familiechat og sette innlegg

Familiechatten ligger i samme rom og bruker samme innlogging. Meldinger lagres på
Pi-en i sju døgn og slettes av samme ryddejobb som medier. Hvert lesekall returnerer
de siste 200 meldingene i kronologisk rekkefølge. Alle med familiekoden kan slette
meldinger. Navnet er selvvalgt, ikke en verifisert brukeridentitet. Nettleseren
sjekker nye meldinger hvert femte sekund mens siden er synlig. Det er ingen
pushvarsler, skriveindikatorer eller garanti for umiddelbar levering ved frakobling.

Videoer starter ikke automatisk; når en video startes pauses andre videoklipp.
Bilder merkes som sett når de åpnes stort, videoer etter 95 prosent eller ved slutt.
Påbegynte videoer husker avspillingsposisjonen. Status lagres i localStorage per
selvvalgt navn, nettleser og domene. Det synkroniseres ikke mellom enheter. Privat
nettlesing, slettede nettleserdata eller blokkert localStorage kan fjerne historikken.
Status er personlig på denne enheten og er ikke lesebekreftelser til resten av familien.

Eksisterende Pi-installasjoner oppdateres med følgende (bruk publisert commit-SHA
som MOTES_REF ved behov). Familiekode, database, medier og Funnel beholdes:

```bash
curl -fsSL https://raw.githubusercontent.com/naessrobert-ui/prisanalyse/main/deploy/pi/familie/update.sh -o /tmp/motes-familie-update.sh
bash /tmp/motes-familie-update.sh
```

Skriptet laster ned og syntakssjekker kode før tjenesten stoppes, beholder forrige
kode og gjenoppretter den hvis ny versjon ikke starter. Det tar ikke sikkerhetskopi
av personlige medier eller database. SQLite utvides med en ny meldingstabell ved
første oppstart; eksisterende innlegg og koder endres ikke.

## Liker og kommentarer

Bilder og videoklipp har hver sitt kommentarfelt og en liker-knapp. En liker-markering
lagres per selvvalgt navn (uavhengig av store/små bokstaver). Dette er ikke en
verifisert identitet eller en egen konto. Alle med familiekoden kan lese og slette
kommentarer; nye kommentarer tar avsendernavnet fra den signerte innloggingen.
Kommentarer og liker-markeringer følger innleggets levetid og fjernes når innlegget
utløper eller slettes. Hvert innlegg kan ha opptil 200 kommentarer. Dialogen i et
kommentarfelt oppdateres hvert femte sekund mens feltet er åpent.

## Opptaksdato og kalender

Opprinnelig opptakstid hentes fra EXIF DateTimeOriginal/DateTimeDigitized for bilder,
og fra MP4/MOV-containerens creation time for videoer. EXIF-tidssone brukes når den
finnes. Uten tidssone vises «kameratid uten tidssone», og original kalenderdag beholdes.
Opptakstiden er filens metadata, ikke en verifisert hendelsesdato. Manglende metadata
(inkludert WebM uten støttet datofelt) gir tydelig merket delingsdato som reserve;
filens endringstid brukes aldri som om den var opptakstid.

Ved første oppstart etter oppdateringen utvides SQLite med to nullable kolonner.
Opptaksdato leses også fra eksisterende, fortsatt tilgjengelige medier. Kalenderen
viser antall aktive innlegg per dato og filtrerer feeden når en dag velges. Den teller
alle aktive innlegg, også eldre innlegg utenfor feedens siste 200. Hver valgt dag viser
opptil 200 innlegg. Slettede og utløpte innlegg vises ikke. Valget «til noen sletter»
lagres som `expires=0` og utelates fra automatisk sletting; lagergrensen gjelder fortsatt.
Opptaksdato påvirker ikke tidspunktet for automatisk sletting.

Oppdateringsskriptet installerer endrede Python-avhengigheter før tjenesten stoppes.
HEIC/HEIF bruker pillow-heif/libheif. Hvis avhengighetene ikke kan installeres på Pi-en,
avbrytes oppdateringen mens gammel tjeneste fortsatt kjører. Koden byttes deretter med
samme helsesjekk og tilbakeføring som før. Sikkerhetskopi av database og medier er
fortsatt et separat driftsansvar.

## Privat slektsdatabase

«Slekt» i familierommet åpner en side bak samme familiekode. Der lastes en UTF-8
HTML-fil på opptil 10 MiB opp. Selve filen lagres som `data/slekt.html` på Pi-en,
utenfor kodekatalogen. Den beholdes ved oppdateringer og automatisk rydding.
Den publiseres ikke i GitHub. En ny opplasting erstatter gjeldende fil etter at
brukeren har bekreftet dette i nettleseren.

Den opplastede siden kjøres i en iframe med en egen, ugjennomsiktig origin.
Både iframe-attributtet og HTTP Content-Security-Policy bruker sandbox med
allow-scripts/allow-downloads, uten allow-same-origin. Siden kan derfor ikke lese
familierommets cookie, localStorage eller API-er. Dette gjelder også ved direkte
åpning av visningsadressen. Eget script og innebygde data fungerer; ekstern
nettverkstilgang er begrenset til kartbilder fra Kartverket, Esri og OpenStreetMap,
og stedsoppslag hos Geonorge. Nettleserinnstillinger/notater inne i HTML-filen
får ikke varig localStorage i denne visningen. Delte redigeringer er ikke støttet.

## Varsling om første nye innlegg

Hver enhet velger selv «Slå på varsler» og godkjenner nettleserens tillatelse.
På iPhone/iPad må familierommet normalt legges til på Hjem-skjermen og åpnes derfra.
Manifest, ikon og service worker følger tjenesten. Ingen App Store-app trengs.

Et abonnement får maksimalt ett Web Push-varsel om bilder/videoer som er lagt inn
etter siste besøk, frem til brukeren åpner familierommet igjen. Besøk fra en synlig
side oppdaterer lest-tidspunkt og undertrykker varsel mens siden er aktiv. Ved skjult
side meldes enheten som borte. En 90-sekunders aktivitetstid dekker forbindelsesbrudd;
ryddeløkken prøver også utsending hvert minutt slik at første innlegg ikke overses
når en nettleser avsluttes uten pagehide. Dette er ikke lesebekreftelser til andre.
Varslingen gjelder nye innlegg med bilder, videoer eller filer, ikke chat, kommentarer eller slektsendringer.

Bruk «Slå av varsler» for å avslutte abonnementet på enheten. Utlogging gjør det
samme. Valget og varslingshistorikken gjelder denne nettleseren/enheten; ulike
enheter har egne abonnementer. Levering avhenger av nettleserens og operativsystemets
push-tjeneste og kan forsinkes. Tillatelse kan ikke gis på vegne av familiemedlemmet.

VAPID-nøkkelen opprettes lokalt som `data/push-vapid.pem` med rettigheter 0600.
Abonnementene og sendt-siden-sist-status ligger i SQLite. Nettleserens push-adresser
valideres mot støttede leverandører før de brukes; vilkårlige URL-er avvises.
Varslet inneholder en generell beskjed, uten personnavn, bildetekst eller bildefil.
Utløpte abonnementer (404/410) fjernes. Nettverksfeil prøves igjen på senere runder.
Enhetens push-id ligger lokalt, og alle administrasjonskall krever familiekode-sesjon
og det vanlige CSRF-beskyttende forespørselshodet.

Kamera-JPEG med MPO/MPF-container godtas nå som JPEG, slik
at blant annet kompatible kamerabilder ikke avvises bare på grunn av containeren.
Vanlig JPEG og HEIC/HEIF støttes som før. Helsesjekken oppgir også kodeversjon,
slik at det er mulig å se om Pi-en faktisk er oppdatert.
## Samtaletråder og standard lagring

Familiechatten har «Ny tråd» med overskrift på opptil 100 tegn. Velg samtale
for å lese og sende meldinger i den aktuelle tråden. Eksisterende meldinger beholdes
i «Fellesprat» med tidligere utløpstid. Meldinger beholdes fortsatt i sju døgn;
overskriftene blir liggende. Trådene bruker samme familiekode og tilgang som rommet.

Nye opplastinger har «Inntil videre (ingen automatisk sletting)» som standard,
også når API-et brukes uten lagringstid. Brukeren kan velge 1 døgn eller 1 uke.
Tidligere opplastinger beholder sin valgte lagringstid. Oppdater Pi-tjenesten med
skriptet over for å aktivere endringene.

## Deling av filer

Filvelgeren støtter også HTML, PDF, Office-filer, ZIP og andre filer, innenfor samme
100 MiB-grense og lagringsvalg som medier. De vises som nedlastingslenker.
Dokumenter sendes som application/octet-stream med Content-Disposition: attachment
og nosniff; HTML kjøres ikke i familierommet. Filnavn lagres separat fra den
servergenererte diskstien. Tilgang krever samme familiekode som bilder og videoer.

## Slette samtaletråder

Alle med familiekoden kan slette en valgt tråd med «Slett tråd». Bekreftelsen
opplyser at overskriften og alle meldingene slettes for hele familien. Fellesprat
kan ikke slettes som tråd. Andre åpne nettlesere går tilbake til Fellesprat når de
oppdager slettingen. Selvvalgte navn er ikke sikre kontoer; sletting begrenses derfor
ikke til den som opprettet tråden.

Ved avvist bilde logges feiltypen og byteantallet uten filnavn, bilde eller EXIF.
Bilder over 60 megapiksler får en egen melding om oppløsning. En syntetisk JPEG
på 5568 × 4872 piksler testes både som vanlig og progressiv JPEG.

## Mediecache og oppdatering uten ombygging

Bilder, videoklipp og nedlastbare vedlegg bruker `Cache-Control: private, no-cache,
must-revalidate` og `Vary: Cookie`. Nettleseren kan lagre filen privat, men må
validere den mot serveren før HTTP-cachen gjenbrukes. ETag og Last-Modified gir en
304-respons uten filinnhold når filen er uendret. Innlogging, utløp og filens eksistens
sjekkes før 304 kan returneres. Ugyldig sesjon gir 401; utløpte/slettede filer gir 404.
Disse feilresponsene, HTML-sider og API-data beholder `private, no-store`.

Render-proxyen videresender If-None-Match, If-Modified-Since, ETag, Last-Modified og
Vary, i tillegg til tidligere range-hoder. Både Pi-koden og Render-proxyen må være
oppdatert for full cachevirkning. Ingen mediefiler lagres i Render eller en delt CDN-cache.

Ved den vanlige sjekken hvert minutt oppdateres listen over innlegg, mens eksisterende
medieelementer beholdes. Nye innlegg legges til, og slettede/utløpte innlegg fjernes.
Liker-markeringer og kommentarantall oppdateres uten å bygge om bilder/videoer.
Videoklipp får ikke src før brukeren trykker «Spill av video»; det lastes derfor ikke
videometadata eller videoinnhold bare fordi kortet vises. En pågående avspilling
beholder samme videoelement ved oppdatering. Bilder utenfor synsfeltet bruker lazy loading.
Allerede viste eller manuelt lagrede kopier kan ikke trekkes tilbake fra mottakerens enhet.

## Del videre fra mobilen

Bilder og videoklipp har «Del videre». Filen hentes først når knappen trykkes, via
samme autentiserte medieadresse og private HTTP-cache. På nettlesere som støtter
Web Share med filer åpnes enhetens vanlige delingsmeny. Selve bildefilen/klippet deles,
ikke en lenke som krever familiekoden. Filnavnets endelse følger formatet som serveres,
slik at blant annet konverterte HEIC-bilder deles som JPG.

Deling skjer i to tydelige trinn: «Del videre» henter filen med bytefremdrift og
«Avbryt henting». Når filen er klar, trykker brukeren «Åpne delingsmenyen».
Delings-API-et kalles direkte fra dette andre trykket, uten en asynkron nedlasting
som kan bruke opp mobilens midlertidige brukerhandling. Delingsknappen låses ikke
mens operativsystemet viser delingsmenyen. Ingen fremdrift på 30 sekunder, eller
henting over fem minutter, stopper hentingen med beskjed og mulighet for nytt forsøk.

«Last ned» er tilgjengelig direkte ved siden av delingsknappen og krever ikke at
hele filen først hentes som en Blob i JavaScript. Nettlesere uten delingsstøtte bruker
denne nedlastingen med en gang. Avbrutt native deling regnes ikke som feil, og den
klargjorte filen beholdes for et nytt trykk. Pågående henting avbrytes når kortet
fjernes, går ut eller brukeren logger ut. Ingen filer sendes videre før brukeren
selv velger en mottaker og fullfører delingen.
