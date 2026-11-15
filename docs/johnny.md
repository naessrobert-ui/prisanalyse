# Yvonne – valgfri spørsmål/svar-assistent i møter

Yvonne foreslås med «Foreslå Yvonne» inne i møtet. Forsiden viser bare
oppretting eller deltakelse og har ikke noe AI-valg. Hver tilkoblet deltaker
må godkjenne selv før hun blir klar.
Et nei eller «Stopp Yvonne» fjerner samtykket og stopper pågående spørsmål.
Nye deltakere og nye WebSocket-forbindelser pauser Yvonne og krever ny godkjenning.

«Spør Yvonne» tar opp bare den som trykker på knappens mikrofon, ikke andre
møtedeltakere. Et nytt trykk sender spørsmålet; etter 30 sekunder sendes det
automatisk hvis samtykket fortsatt gjelder. Spørsmålet sendes til OpenAI for
transkripsjon og et svar med nettsøk. Svar og kilder distribueres til alle i det
aktuelle rommet. OpenAI GPT-Live-1 genererer stemmen med Marin og instruksjoner om en varm, blid,
naturlig norsk fremføring. PCM16-lyd ved 24 kHz strømmes til alle som fortsatt har
godkjent. Svaret finnes også som tekst med klikkbare kilder og lokal gjenavspilling.

Denne versjonen gjør ikke kontinuerlig AI-lytting, automatisk møtereferat,
taleranalyse eller møteledelse. Spørsmålene har ingen varig samtalekontekst hos
appen. Vanlige møter virker uten AI. Eksisterende WebRTC-lyd/video er beholdt.

## Samtykke og databehandling

Samtykke knyttes til en aktiv WebSocket-deltaker. HTTP-kallet krever deltakerens
hemmelige klient-id og gyldig aktivt enstemmig samtykke. En møtelenke alene gir
ikke rett til AI-kall. Ved endret samtykke eller deltakerliste kanselleres appens
pågående HTTP-arbeid og resultat distribueres ikke. Data som allerede er sendt
til en ekstern behandler kan ikke trekkes tilbake ved å stoppe nettverkskallet.
Ingen lydfiler skrives til disk av møtes.no. API-tilbyderens gjeldende regler for
databehandling gjelder; ikke lov at ingenting lagres hos tredjeparten.
Responses-kallet bruker `store: false`. Appen lagrer ingen lyd eller AI-svar i en
database. Møtechatten i hver nettleser inneholder svaret så lenge siden er åpen.

## Oppsett på Render

Legg disse variablene på eksisterende `prisanalyse`-tjeneste:

- `OPENAI_API_KEY`: en API-nøkkel med tilgang til modellene og nettsøk. Bruk Render
  sitt hemmelighetsfelt; aldri legg nøkkelen i Git eller chat.
- `JOHNNY_ENABLED=true`: eksplisitt serveraktivering. Uten dette forblir Yvonne av.
- Valgfrie modellvalg: `JOHNNY_MODEL` (standard `gpt-4.1-mini`) og
  `JOHNNY_TRANSCRIBE_MODEL` (standard `gpt-transcribe`). Modelltilgang må testes
  med kontoen som skal betale for bruken.

Ingen API-nøkler sendes til nettleseren. API-kall medfører kostnader. Appen har
grenser på to forespørsler per rom per minutt og 20 totalt per time per prosess.
Grensene nullstilles ved deploy/omstart. Spørsmål er begrenset til 512 KiB lyd;
appens opptaker begrenser normalt lengden til 30 sekunder. Disse grensene er
ikke en erstatning for kostnadsgrenser og overvåking hos API-leverandøren.

`GET /meet/johnny/status` viser bare konfigurasjonsflagg, aldri nøkkelen. Når
serveren er aktivert, test faktisk lydspørsmål og kildehenvisninger med to
nettlesere. Test også at en ny deltaker pauser Yvonne og at «Stopp» avbryter et
pågående spørsmål. Simulerte API-svar i automatiske tester bekrefter ikke
modelltilgang, lydgjenkjenning eller kvalitet på faktasvar i produksjon.

## Verifiserte API-referanser

- Transkripsjon og nåværende modell: https://developers.openai.com/api/docs/guides/speech-to-text
- Responses-nettsøk og annoterte kilder: https://developers.openai.com/api/docs/guides/tools-web-search
- Faktamodell: https://developers.openai.com/api/docs/models/gpt-4.1-mini
- Databehandling: https://developers.openai.com/api/docs/guides/your-data

API-referansene ble lest ved implementasjonen 14. november 2026.

## GPT-Live-stemmen

Tilgangen til `gpt-live-1` må verifiseres med API-prosjektet som brukes i produksjon.
Valgfrie innstillinger er `YVONNE_VOICE_MODEL` (standard `gpt-live-1`) og
`YVONNE_VOICE` (standard `marin`). De eksisterende innstillingene `JOHNNY_ENABLED`,
`JOHNNY_MODEL` og `JOHNNY_TRANSCRIBE_MODEL` beholdes for kompatibilitet.

Stemmesesjonen får den ferdige svarteksten og syntetisk stillhet for å drive
Live-tidslinjen. Den får ikke mikrofonen eller lyd fra resten av møtet. Nettsøk
og faktasvar håndteres fortsatt av eksisterende backend. Dette er generert tale
for spørsmål/svar-flyten; det er ikke en kontinuerlig full-duplex-møtedeltaker.

Serveren begrenser én stemmesesjon til 60 sekunder med en kort oppstart-/lukkefrist.
Etter at faktisk stemmelyd er registrert, avsluttes sesjonen etter fire sekunder
med målt stillhet i PCM-lyden. Transcript-pauser brukes ikke som sluttsignal.
Dette er en praktisk avgrensning, ikke en garanti for eksakt ferdig-avspilling;
uvanlig lange talepauser kan avslutte før modellen ellers ville fortsatt.

API-sesjonen lukkes også ved tilbaketrukket samtykke. Live har separat
varighetsbasert fakturering i tillegg til modell-/søke- og transkripsjonsbruk.
Alle klientene stopper planlagt lyd ved stopp eller nye samtykkekrav. Nettleseren
kan kreve en brukerhandling før lyd kan avspilles; «Lytt til svaret» gir et lokalt
nytt forsøk. Appen bruker ikke nettleserens talesyntese som skjult reserve. Hvis
stemmegenerering feiler, vises svar og kilder med en tydelig feilmelding.

Referanser:
- https://developers.openai.com/api/docs/guides/live
- https://developers.openai.com/api/docs/guides/voice-websockets?api=live
- https://developers.openai.com/api/docs/guides/live-conversations
- https://developers.openai.com/api/docs/guides/live-prompting

## Lokal aktivering med «Yvonne»

Et eget, avslått valg «Lytt etter Yvonne på denne enheten» vises etter alles
samtykke. Dette gjelder kun den som slår på valget. Bare egen mikrofon brukes.
Navnegjenkjenning bruker nettleserens SpeechRecognition med processLocally=true.
Både tilgjengelighetskontroll og eventuell språkpakkenedlasting krever lokal
behandling. Ingen skybasert SpeechRecognition-reserve tillates. Norsk nb-NO
foretrekkes; ellers brukes en engelsk enhetspakke for gjenkjenning av selve navnet.
Nettleseren kan laste ned en språkmodell før funksjonen virker.

Si «Yvonne», ta en kort pause, og still spørsmålet. Før navnet er oppdaget blir
verken lyd eller transkripsjoner sendt av appen. Etter treff starter den vanlige
MediaRecorder-opptakeren uten lyd fra før treffet. Spørsmålet sendes etter ca.
1,8 sekunders stillhet, maksimalt 30 sekunder. Et treff uten hørbart spørsmål
forkastes etter åtte sekunder. En lokal lydnivåmåling styrer slutten på spørsmålet;
dette er en enkel terskel, og ved støy kan «Send spørsmålet» brukes manuelt.
Hun kjenner ikke samtalen før spørsmålet. Treff på navnet kan være feil eller utebli;
funksjonen er ikke en garanti for at bare tilsiktede spørsmål blir sendt.

Lytting pauses ved dempet mikrofon, andre spørsmål, AI-avspilling og skjult fane.
Stopp, frakobling og nye samtykkekrav slår valget av og forkaster pågående opptak.
Et nytt samtykke slår ikke lokal lytting på automatisk igjen. Språkpakken må støttes
av enheten; prøv oppdatert Chrome på datamaskin. Uten lokal støtte vises en melding
og det vanlige knappevalget virker fortsatt. Appen sender ikke lyd til skyen for å
omgå manglende enhetsstøtte. Lokal lytting har ingen OpenAI-minuttkostnad;
innsendte spørsmål og talte svar bruker samme API-flyt og grenser som før.

Referanse: https://developer.mozilla.org/en-US/docs/Web/API/Web_Speech_API/Using_the_Web_Speech_API#on-device_speech_recognition

Verifisering: Lokal nettlesertest med simulert on-device-motor kontrollerer ingen
spørsmålsopplasting før navnet, automatisk sending etter stillhet, forkasting av
tomt treff, demping, ny deltaker/samtykke og manuell reserve uten lokal støtte.
Dette tester appens styring; faktisk navnegjenkjenning og modellnedlasting må
også prøves på brukerens nettleser med fysisk mikrofon.

Kjør den eksplisitte testserveren fra repository-roten (med testavhengighetene installert):

```sh
PYTHONPATH=. OPENAI_API_KEY=test JOHNNY_ENABLED=true python -m uvicorn preview_yvonne_wake:app --app-dir tests --host 127.0.0.1 --port 8771
python tests/browser_yvonne_wake.py
```

Testserveren må aldri brukes som produksjonsstartkommando. Den erstatter begge
AI-providerne med syntetiske svar. Nettlesertesten bruker /usr/bin/chromium.

### Navnehint og lokal feilsøking

Der SpeechRecognition.phrases støttes, prioriteres «Yvonne», «Hey Yvonne»,
«Hei Yvonne» og «Ivonne» med SpeechRecognitionPhrase (boost 8). Hvis motoren
avviser navnehint, prøves lokal lytting uten hint; den bytter aldri til skyen.
Navnevarianten «Yvonn»/«Yvone»/«Ivon»/«Evon» godtas, men vanlige engelske ord
som «even» og «you won» aktiverer ikke automatisk opptak.

Valget «Vis hva navnegjenkjenningen hører» er av som standard. Når det er valgt,
vises opptil 160 tegn av siste lokale gjenkjenning på egen enhet i åtte sekunder.
Ingen slik diagnostikk sendes til server eller møtedeltakere, eller skrives til
nettleserlagring. Pausing, deaktivering og avslått visning tømmer feltet.
Dette hjelper med å skille manglende mikrofoninput fra feil gjenkjent navn.
Selve navnenøyaktigheten må bekreftes med fysisk mikrofon på brukerens enhet.

Referanse: https://developer.mozilla.org/en-US/docs/Web/API/SpeechRecognition/phrases
