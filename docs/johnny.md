# Johnny – valgfri spørsmål/svar-assistent i møter

Denne versjonen gir et avkrysningsvalg «Foreslå Johnny som møteassistent» før du
oppretter eller blir med i et møte. Det er av som standard. Johnny kan også
foreslås inne i møtet. Hver tilkoblet deltaker må godkjenne selv før han blir klar.
Et nei eller «Stopp Johnny» fjerner samtykket og stopper pågående spørsmål.
Nye deltakere og nye WebSocket-forbindelser pauser Johnny og krever ny godkjenning.

«Spør Johnny» tar opp bare den som trykker på knappens mikrofon, ikke andre
møtedeltakere. Et nytt trykk sender spørsmålet; etter 30 sekunder sendes det
automatisk hvis samtykket fortsatt gjelder. Spørsmålet sendes til OpenAI for
transkripsjon og et svar med nettsøk. Svar og kilder distribueres til alle i det
aktuelle rommet. Nettleserens talesyntese leser svaret høyt. Stemme og støtte for
norsk varierer mellom enheter; svaret er også tilgjengelig som tekst, med
klikkbare kildehenvisninger i møtechatten og lokal knapp for å lytte igjen.

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
- `JOHNNY_ENABLED=true`: eksplisitt serveraktivering. Uten dette forblir Johnny av.
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
nettlesere. Test også at en ny deltaker pauser Johnny og at «Stopp» avbryter et
pågående spørsmål. Simulerte API-svar i automatiske tester bekrefter ikke
modelltilgang, lydgjenkjenning eller kvalitet på faktasvar i produksjon.

## Verifiserte API-referanser

- Transkripsjon og nåværende modell: https://developers.openai.com/api/docs/guides/speech-to-text
- Responses-nettsøk og annoterte kilder: https://developers.openai.com/api/docs/guides/tools-web-search
- Faktamodell: https://developers.openai.com/api/docs/models/gpt-4.1-mini
- Databehandling: https://developers.openai.com/api/docs/guides/your-data

API-referansene ble lest ved implementasjonen 14. november 2026. Stemme er
nettleserens innebygde talesyntese; denne versjonen bruker ikke en Realtime-
eller TTS-modell for generering av stemmen.
