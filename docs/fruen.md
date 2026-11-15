# Fruen: privat bildeskjerm og familiebesøk

`/fruen` er et tillegg til familierommet (se `familierom.md`). En fast iPad viser
alle aktive bilder og videoklipp fra `/familie` i tilfeldig rekkefølge. Særskilt
inviterte enheter kan ringe den. Andre familiemedlemmer kan ha egne bildeskjermer
og ringe hverandre med vanlige ring- og svarknapper.

## Arkitektur og personvern

- Samme Pi-tjeneste som familierommet (`motes-familie`, port 8025). Render
  videresender `/fruen` via eksisterende `FAMILY_ORIGIN`. Ingen nye porter,
  ingen endringer i Tailscale/Funnel, og referattjenesten berøres ikke.
- Egen administratorkode, lagret som scrypt-hash i `data/fruen-admin.json` (0600).
  Koden velges i terminalen på Pi-en og skal aldri i chat, Git eller Render.
- Enheter kobles til med engangsinvitasjoner som gjelder i 30 minutter. Hver enhet
  får en egen, tilbakekallbar cookie som bare gjelder `/fruen`. Familiekoden gir
  ikke tilgang til `/fruen`, og en Fruen-enhet får ikke familierommets API.
- Bildeskjermen får bare bilder og videoer som ikke er utløpt. Dokumenter deles ikke.
- Samtaler går som WebRTC direkte mellom enhetene, eller via TURN-reléet fra
  møtes.no når nettet krever det. Pi-en formidler bare oppkoblingsmeldinger
  (polling hvert 2. sekund), og sletter dem kort etter samtalen. Ingen opptak.
  TURN-nøkler gis bare til en enhet med aktiv invitasjon.
- Kamera og mikrofon er av mellom samtaler. Innringerens kamera starter først når
  samtalen er besvart. Maks én samtale per enhet og 60 minutter per samtale.
- En annen deltaker kan likevel bruke egne opptaksverktøy. Inviter bare personer
  dere stoler på, og avklar bruken med henne og eventuelt den som ivaretar
  interessene hennes.

## Profiler

| Profil | Bruk | Knapper |
| --- | --- | --- |
| `member` (standard) | familiemedlem, mobil, egen bildeskjerm | ringe, svare, avvise, legge på |
| `auto` | hennes faste iPad | ingen; svarer selv etter navn og lydvarsel i minst 5 sekunder |

En `auto`-enhet kan ikke ringe ut. Bare innringeren legger på; bildene fortsetter etterpå.

## Nattmodus

Kl. 22–07 norsk tid (`NIGHT` i `familie/fruen.py`) blir `auto`-skjermen helt svart,
bildene står stille, og serveren avviser nye anrop til den. Familiemedlemmer ser
«Sover (nattmodus 22–07)». Et besøk som pågår kl. 22 fortsetter til innringer
legger på. Kl. 07 fortsetter bildene uten at noen må trykke. Siden kan ikke styre
iPadens baklys; Snarveier-automatisering («Angi lysstyrke» 0 % kl. 22, høyere kl. 07)
gjør det mørkere. Familiemedlemmer kan fortsatt ringe hverandre om natten.

## Installasjon på Pi-en

Krever at familierommet allerede kjører. Kjør som vanlig bruker, ikke som root.
Bruk publisert commit-SHA som `MOTES_REF`, så det er nøyaktig den gjennomgåtte
koden som installeres:

```bash
curl -fsSL https://raw.githubusercontent.com/naessrobert-ui/prisanalyse/<SHA>/deploy/pi/fruen/setup.sh -o /tmp/fruen-setup.sh
MOTES_REF=<SHA> bash /tmp/fruen-setup.sh
```

Skriptet bruker familierommets vanlige oppdatering: nye filer lastes ned og
kompileres før tjenesten stoppes, gammel kode sikkerhetskopieres, og ved feilet
helsesjekk rulles koden tilbake. `access.json`, bilder, videoer og SQLite beholdes.
Til slutt velger du administratorkoden (12 til 200 tegn) i terminalen.

Ny administratorkode senere (logger ut alle administratorøkter, enhetene beholdes):

```bash
FAMILY_DATA_DIR=~/motes-familie/data PYTHONPATH=~/motes-familie/app ~/motes-familie/venv/bin/python -m familie.fruen_setup --reset-admin
```

## Klargjøre hennes iPad

1. Åpne `https://møtes.no/fruen/admin`, lag en invitasjon med typen
   «Fruens bildeskjerm», og åpne lenken i Safari på iPaden innen 30 minutter.
2. Trykk «Koble til denne enheten», deretter «Aktiver lyd, kamera og besøk», og
   godkjenn kamera og mikrofon. Kameraet slås av igjen etter kontrollen.
3. Innstillinger → Apper → Safari → Kamera og Mikrofon: «Tillat» (eller «Aa» →
   Nettstedsinnstillinger for møtes.no). Uten dette kan Safari spørre igjen ved
   et anrop, og da er det ingen som svarer.
4. Aktiveringsknappen setter også siden i fullskjerm. Hvis ikke, trykk én gang på
   bildet. Start deretter Veiledet tilgang (Tilgjengelighet → Veiledet tilgang),
   med egen Automatisk lås: Aldri. Fast strøm, Automatisk lås: Aldri. Siden
   kan ikke vekke en låst iPad, og Safari må være synlig.
5. Prøv et besøk fra en annen godkjent enhet: navn og lydvarsel, bilde og lyd
   begge veier, og at bildene kommer tilbake når innringer legger på.

**Etter omstart, ny sideinnlasting eller oppdatering av iOS** må en pårørende åpne
`https://møtes.no/fruen/klargjor` på iPaden og trykke aktiveringsknappen igjen.
Nettlesere krever et trykk før lyd kan spilles. Til da vises bildene, men iPaden
står som «ikke tilkoblet» for innringere.

## Feilsøking

| Symptom | Sjekk |
| --- | --- |
| iPaden står som «ikke tilkoblet» | Er Safari synlig og skjermen våken? Trenger den ny aktivering (`/fruen/klargjor`)? |
| Anropet avsluttes med en gang | Kamera/mikrofon-tillatelse i Safari. Enheten må ha sendt livstegn de siste 10 sekundene. |
| «Besøket fikk ikke forbindelse» | Nett som blokkerer direkte forbindelse; sjekk at TURN er satt opp for møtes.no. |
| Bildene står stille | Nettbrudd: siste bilde blir stående til nettet er tilbake. |
| En enhet skal ikke lenger ha tilgang | Admin-siden → «Fjern tilgang». Pågående samtale avsluttes og cookien virker ikke mer. |

## Tester

```bash
python -m pytest -q tests/test_fruen.py tests/test_familie.py tests/test_familie_update.py
python -m pytest -q tests/test_fruen_browser.py   # Chrome + Playwright, falske kameraer
```

Nettlesertestene kjører den lokale demoen (`tests/preview_fruen.py`) i separate
nettleserkontekster og sjekker samtaler begge veier, automatisk svar, at lyd og
bilde faktisk mottas begge veier, at alle kameraspor er stoppet etter pålegg,
sent kamerasvar, ny samtale mens en gammel oppkobling henger, skjult side,
nettbrudd, avslått kamera, sperret enhet, brukt invitasjon og layout på iPad og mobil.
De erstatter ikke en prøve på den faktiske iPaden i Safari.

Lokal demo (aldri på Pi-en eller bundet til 0.0.0.0):

```bash
python -m uvicorn preview_fruen:app --app-dir tests --host 127.0.0.1 --port 8773
```

Åpne `http://localhost:8773/fruen/admin`, demokode `fruen-local-test-only`.
