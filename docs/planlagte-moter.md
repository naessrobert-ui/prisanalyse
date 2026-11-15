# Planlagte møter

Forsiden tilbyr «Møte nå», «Planlegg» og «Bli med». Planlegging krever tittel,
oppgitt arrangørnavn, tidspunkt med IANA-tidssone og varighet. Agenda er valgfri.
Dato er innen ett år og varighet 5–480 minutter. Overgang til/fra sommertid
valideres: ikke-eksisterende eller tvetydige klokkeslett avvises med forklaring.

Planlegging starter ikke mikrofon, kamera eller møterom. Tidspunkt/varighet er
kalenderinformasjon: deltakerne kan komme tidlig og fortsette etter planlagt
slutt. En deltakerlenke varer til 24 timer etter planlagt slutt. Den er stabil ved
endring av tidspunktet. Avlysning eller sletting hindrer nye oppkoblinger med
lenken; allerede tilkoblede samtaler blir ikke tvangsavsluttet.

## Invitasjoner og Mine møter

Arrangøren får vanlig deltakerlenke og en separat, privat arrangørlenke. Begge
bruker tilfeldige 256-bit kapasitetstokener. Arrangørlenken kan hente, endre,
avlyse og slette møtet; den er ikke det samme som bekreftet personlig innlogging.
Det finnes ikke identitetsbekreftelse, e-postinnlogging, RSVP eller kalenderkonto-
synkronisering i denne første versjonen. Deltakere trenger ingen konto.

Arrangørtokener og en liten oversikt over inntil 40 møter huskes i localStorage.
«Mine møter» er derfor en liste for den aktuelle nettleseren, ikke en global
kontoliste. Arrangørlenken kan åpnes på en annen enhet og legges til listen der.
Private nettleservinduer, sletting av nettleserdata eller bytte av enhet kan miste
listen. Ta vare på arrangørlenken. Deltakerlenken har ikke rett til endring.

«Del invitasjon» bruker nettleserens delefunksjon, eller kopierer teksten for
innliming i e-post, SMS eller melding. Appen sender ikke e-post eller meldinger
på brukerens vegne. Bare deltakerlenken, aldri arrangørtokenet, tas med.

Kalenderfilen `.ics` bruker UTC DTSTART/DTEND, stabil UID, økende SEQUENCE, UTF-8
med 75-oktetters linjefolding og escaped tekst. Den inkluderer agenda og
møtelenke, samt en 10-minutters kalenderpåminnelse. Kalenderklienten styrer om
og hvordan påminnelsen vises. Det er en kalenderfil, ikke en sendt Exchange-
møteforespørsel med deltakerliste og automatiske svar.

Ved endring må arrangøren selv varsle deltakerne og dele oppdatert kalenderfil.
Ved avlysning tilbys METHOD:CANCEL/STATUS:CANCELLED med samme UID. Import,
oppdatering og avlysning kan håndteres forskjellig i kalenderklientene; det
er ingen automatisk tilbakekalling av meldinger eller synk til eksisterende
Outlook-/Google-avtaler. UI minner eksplisitt om å sende ny beskjed/fil.

## Privat og varig lagring

Produksjon bruker eksisterende S3-infrastruktur, separat prefiks
`motes/plans/`. Prioritet for bøttevalg:

1. `MEET_PLANS_BUCKET` dersom eksplisitt satt.
2. `NEWSWEB_CONSENSUS_BUCKET` dersom satt (eksisterende private lagring).
3. `S3_BUCKET_NAME`, ellers appens eksisterende standard `prisanalyse-data`.

Alle fire S3 Public Access Block-innstillinger må være på. Det bekreftes før
lagring/henting, med kort cache for kontrollen. SSE AES256 og `private,no-store`
settes. Ingen møteplaner, agendaer, tilgangstokener eller reelle invitasjoner
skrives til GitHub. Objektet inneholder bare disse møtedetaljene og nødvendige
administrasjonsdata; ingen lyd eller video.

S3-tokenet for arrangør lagres bare som SHA-256-hash. Deltakertokenet lagres i
det private objektet slik at invitasjonen kan hentes igjen. Ingen bøtte eller
betalt separat tjeneste opprettes automatisk. S3/IAM må tillate privat
GetPublicAccessBlock samt GetObject/PutObject/DeleteObject under prefikset.
Objektoppdateringer bruker ETag/IfMatch; oppretting bruker IfNoneMatch for å
unngå overskriving. Ingen produksjonsfallback til RAM eller ephemeral Render-
filsystem. Feil i privat lagring gir synlig feil og hindrer falsk «lagret».

Møtedetaljer beholdes privat til arrangøren sletter dem. Avlysning beholder dem
for å kunne hente avlysningen til kalender. Sletting sletter objektet og gjør
lenkene ugyldige; nedlastede kalenderfiler eller allerede sendte invitasjoner
slettes ikke fra andres enheter. Eksisterende S3-versjonering, sikkerhetskopi og
lagringspolicyer kan beholde historiske objektversjoner.

API krever egne headers for endring og tokens i request-headers ved lesing.
Invitasjons-/arrangørlenkene har token i URL-fragment, som ikke inngår i vanlig
HTTP-sti. Eksisterende WebSocket-protokoll bruker fortsatt `inv` i query under
oppkobling, som for andre møter; unngå fullstendig query-logging. Alle plan- og
kalendersvar har no-store. Skjema/payload er begrenset, og oppretting er
ratebegrenset (20/time per kilde-IP og 200/time per prosess). Ingen automatisk
utsending eller OpenAI-kall inngår.

## Verifikasjon

```sh
python -m pytest -q tests/test_meet_plans.py tests/test_meet_routes.py tests/test_meet_domene.py tests/test_johnny.py tests/test_minutes.py tests/test_yvonne_voice.py
PYTHONPATH=. python -m uvicorn preview_meet_plans:app --app-dir tests --host 127.0.0.1 --port 8774
python tests/browser_meet_plans.py
```

Den eksplisitte lokale testserveren bruker MemoryStore og må aldri brukes som
produksjonsstartkommando. Produksjon bruker S3Store. Nettlesertesten dekker
planlegging uten mikrofontilgang, kalenderfil, deling, endring, lokal liste,
gjest uten konto, deltakelse før start, avlysning, gjenåpning via arrangørlenke,
sletting og mobil layout. Faktisk S3-lagring må kontrolleres etter deploy med
en syntetisk testplan som deretter slettes. Kalenderimport i brukerens Outlook,
Google eller Apple må prøves separat før eventuelle kompatibilitetsløfter.
