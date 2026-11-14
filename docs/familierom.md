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

Innlegg skjules umiddelbart etter 1, 3 eller 7 døgn. En jobb sletter de utløpte filene
hvert minutt når tjenesten kjører, og ved neste oppstart hvis Pi-en har vært avslått.
Sletting gjelder aktive filer, ikke sikkerhetskopier eller kopier mottakerne har laget.
Ingen sikkerhetskopiering settes opp av installasjonen. Mottakerne kan laste ned,
skjermfotografere eller kopiere innhold. Dette er ikke DRM eller ende-til-ende-kryptert
lagring; HTTPS beskytter transporten. Render videresender innholdet underveis.

## Grenser

- Inntil 100 MiB per fil og 10 GiB samlet familielager, med 2 GiB fri disk som reserve.
- JPG, PNG og WebP (valideres som bilder), MP4/MOV og WebM (container sjekkes).
- Ingen videokonvertering. HEIC må eksporteres som JPG. Telefonklipp med HEVC kan
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
