# Referatutkast med fortløpende transkripsjon på Raspberry Pi

Dette er en separat funksjon fra den talende Yvonne. Den som trykker «Start
referat» blir opptaksvert. Det er ikke opprettet et kontosystem med bekreftet
møtelederrolle; andre deltakere kan også starte dersom det ikke allerede pågår
referat. Arrangøren må sørge for grunnlaget for behandling av møtelyden.

Alle får et tydelig varsel om at møtelyden transkriberes. Opptaket starter etter
åtte sekunder. Det er ingen avkrysningsrunde der alle må godkjenne. Nye deltakere
får samme varsel og en ny åttesekunders pause. Alle deltakere kan stoppe videre
opptak. Å stoppe sletter ikke allerede behandlede deler; utkastet kan fortsatt
lages. Opptaksverten kan slette jobben via sin hentelenke.

## Flyt og begrensninger

Nettleseren blander egen mikrofon og mottatt deltakerlyd via Web Audio.
AudioWorklet lager selvstendige PCM16 WAV-filer: 16 kHz mono, inntil 20 sekunder,
med ett sekund overlapp. Ingen video eller skjerm deles med referattjenesten.
Den dempede mikrofonens spor bidrar med stillhet. Opptaksverten må holde fanen
åpen og enheten våken. Avspilling av Yvonne på høyttaler kan gi ekko i mikrofonen;
bruk hodetelefoner for møtet. Yvonnes genererte PCM kobles ikke direkte inn i
referatmiksen.

Render validerer aktiv møtedeltaker, opptaksvert, opptaksperiode og størrelse,
og videresender til én fast Pi-origin. Pi-en krever Ed25519-signert metode, sti,
tidspunkt, nonce og body-hash. Signeringens private nøkkel finnes bare på Render;
Pi-en får kun offentlig nøkkel. Ingen OpenAI-nøkkel installeres på Pi-en.
Helseendepunktet inneholder ingen møteopplysninger.

Pi-en kølegger lyd i SQLite før den kvitterer. Sekvens og hash gjør identiske
forsøk idempotente; andre data for samme sekvens avvises. Én Whisper-prosess
bruker to CPU-tråder og lav prioritet. Ved omstart gjenopptas køen; hvert lydstykke
har høyst to forsøk. Behandlet lyd fjernes fra køen; feilmarkerte deler slettes
og markeres som hull. Dette er logisk sletting, ingen garanti for forensisk
sikker sletting på SSD.

Når opptaket stoppes, tømmes køen før teksten sendes til OpenAI Responses én
gang for referatutkast, uten nettsøk, talesyntese eller kontinuerlig GPT-Live.
`store:false` brukes. Modellens og leverandørens øvrige databehandlingsregler
gjelder fortsatt. Maks to oppsummeringsforsøk per jobb; en tidsavgrenset Pi-lease
hindrer parallelle klienter i å lage doble utkast. Et prosesskrasj rundt et betalt
API-kall kan likevel kreve et nytt forsøk. Utkastet skiller forslag fra vedtak,
markerer uavklart ansvar/frister og krever kontroll av møtelederen. Det finnes
ingen sikker taleridentifikasjon i denne blandede lydstrømmen.

Første versjon har maks ett aktivt møte, 90 minutter per møte, 32 MiB ventende
lyd totalt og 40 nye jobber per kalendermåned (UTC). Månedstelleren beholdes selv
når innhold slettes. Maks 160 000 tegn transkripsjon og 3000 output-tokens til
oppsummering. Modellpriser kan endres; dette er bruksgrenser, ikke et garantert
kronebudsjett. Transkripsjon på Pi-en har ingen API-kostnad.

Transkripsjon, gjenværende lyd og utkast utløper 24 timer etter jobbstart.
Rydding skjer fortløpende mens Pi-tjenesten kjører; når den er avslått, ryddes det
ved neste oppstart. Etter 120 sekunder uten nye lydbiter avsluttes en åpen jobb
som ufullstendig. Ved brått nettbrudd kan siste lokale lydbit mangle. Nettleseren
stopper ved vedvarende opplastingsfeil eller suspendert lydmotor; feil og hull
må ikke tolkes som et fullstendig referat.

Hentelenken inneholder en tilfeldig 256-bit tilgangskode i URL-fragmentet,
ikke i HTTP-stien. Den kan hente, avslutte og slette jobben, og skal behandles
som en privat lenke. Hash av koden lagres på Pi-en. Den kan brukes etter at
møtet er avsluttet eller Render har startet på nytt. Nettleseren husker siste
hentelenke i sessionStorage for den fanen. Last ned teksten eller kopier lenken
før fanen lukkes. Utkast deles også med deltakere som er tilkoblet når det blir
klart. Lokale endringer i tekstfeltet lagres bare ved nedlasting, ikke på server.

## Oppsett

Render på eksisterende prisanalyse-tjeneste, My Workspace:

- `MINUTES_PI_ORIGIN=https://raspberrypi.tail226bba.ts.net:10000`
- `MINUTES_SIGNING_KEY`: base64-kodet, tilfeldig Ed25519 private key (32 byte).
  Genereres og settes som hemmelig miljøvariabel, aldri i repo eller chat.
- Eksisterende `OPENAI_API_KEY` brukes bare på Render til sluttoppsummering.
- `MINUTES_SUMMARY_MODEL` er valgfri, standard `gpt-4.1-mini`.

På Pi-en brukes allerede testet Whisper fra `~/motes-whisper-test`, med
flerspråklig `base`, ikke `base.en`. Installer som vanlig bruker:

```sh
curl -fsSLo /tmp/motes-referat-install.sh https://raw.githubusercontent.com/naessrobert-ui/prisanalyse/main/deploy/pi/minutes/install.sh
bash /tmp/motes-referat-install.sh
```

Skriptet oppretter en separat tjeneste `motes-referat` på 127.0.0.1:8026,
med `Nice=10`, `CPUQuota=180%`, `MemoryMax=2G`, én worker og data i
`~/motes-referat/data`. Det laster ned offentlig nøkkel fra møtes.no over HTTPS,
sjekker at Pi-navnet stemmer, og åpner en separat Funnel på HTTPS-port 10000.
Eksisterende familierom på port 8443 endres ikke. En annen tjeneste på port
10000 overskrives ikke. Endret offentlig nøkkel avvises ved ny installasjon.
Verktøystøtte kontrolleres med lokal `tailscale funnel --help` før endringer.

Installeringsskriptet krever ikke en ny Whisper-bygging eller ny modellnedlasting.
Ingen ny betalt Render-worker eller database opprettes. Vellykket Pi-installasjon
må bekreftes i drift før knappen blir tilgjengelig på nettsiden.

## Kontroll

```sh
systemctl is-active motes-referat
curl -fsS http://127.0.0.1:8026/health
sudo journalctl -u motes-referat -n 30 --no-pager
```

Se etter feil uten å publisere privat møtelyd eller transkripsjon.
Kjør først et 2–5 minutters norsk testmøte. Bekreft varsling på to enheter,
hørbarhet for alle, godtagbar norsk tekst, tidsbruk etter stopp, nedlasting,
hentelenke, stopp fra annen deltaker og at familierommet fortsatt virker.
30–90 sekunder til utkast er et mål, ikke en målt garanti for norsk møtelyd.

Automatisk verifikasjon:

```sh
python -m pytest -q tests/test_minutes.py tests/test_meet_routes.py tests/test_johnny.py tests/test_yvonne_voice.py tests/test_meet_domene.py
PYTHONPATH=. python -m uvicorn preview_meeting_minutes:app --app-dir tests --port 8772
python tests/browser_meeting_minutes.py
```

Nettlesertesten bruker ekte AudioWorklet, blandet mikrofon/WebRTC-lyd og WAV
validering. Testserveren har simulert lokal Whisper og simulert oppsummering;
den beviser ikke norsk tekstkvalitet eller faktisk modelltilgang i produksjon.
Den er kun en eksplisitt lokal testserver og må aldri brukes i produksjon.
