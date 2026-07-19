# Sør-England 2026

Mobilvennlig, delbar reiseside for turen til Eastbourne, Rye, Brighton og London.

## Adresse

Når endringen er publisert:

`https://prisanalyse.no/ferie/sor-england-2026/`

Siden er merket `noindex,nofollow`, slik at den ikke er ment å dukke opp i søkemotorer. Den er likevel offentlig for alle som kjenner lenken.

## Innhold

- aktuell reisedag og overnattingssted
- full dag-for-dag-plan med kart- og informasjonslenker
- egne hotellkort med direkte lenke, kart, bestilt rom og historikk
- korte historier om Eastbourne, Seven Sisters, Battle, Rye, Bodiam og Brighton
- offentlig reservasjonssammendrag uten bestillingsnumre
- begrenset AI-reiseassistent som svarer ut fra sidens reiseinformasjon
- delingsknapp for mobil og skrivebord
- privat S3-basert bildegalleri
- bildeopplasting beskyttet med en egen familiekode

## Miljøvariabler

### Bildeopplasting

- `FERIE_UPLOAD_CODE`: kode familien må skrive inn for å laste opp bilder

### Reiseassistent

- bruker eksisterende `ANTHROPIC_API_KEY`
- `FERIE_CHAT_ENABLED`: sett til `0` for å slå assistenten av; standard er på når API-nøkkel finnes
- `FERIE_CHAT_MODEL`: valgfri modelloverstyring; standard er `claude-sonnet-4-6`
- endepunktet tillater maksimalt seks spørsmål per IP-adresse per 30 minutter

Assistenten er instruert til ikke å finne på reservasjoner eller bookingnumre, og til å si fra når vær, tog, åpningstider eller annen ferskvare må kontrolleres live.

### S3

- `FERIE_S3_BUCKET`: valgfri egen bucket for reisen
- dersom den ikke er satt, brukes eksisterende `S3_BUCKET_NAME`
- `FERIE_S3_PREFIX`: valgfri sti, standard er `travel/south-england-2026`
- vanlige AWS-variabler brukes: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` og `AWS_REGION`

Render-tjenesten må ha minst disse S3-rettighetene for den valgte stien:

- `s3:GetObject`
- `s3:PutObject`

Bucketen kan og bør være privat. Siden lager tidsbegrensede, signerte visningslenker til bildene.

## Bildebehandling

- maksimalt åtte bilder per opplasting
- maksimalt 15 MB per originalfil
- bildene roteres etter EXIF, skaleres til maks 2000 piksler og lagres som JPEG
- opprinnelige metadata blir ikke med videre
- JPEG, PNG og WebP er tryggest; HEIC avhenger av hvilke bildedekodere som finnes i Pillow-installasjonen

## Personvern

Ikke legg bestillingsnumre, betalingsinformasjon, passdetaljer eller private dokumenter i de offentlige dataene i `ferie_routes.py`. Bildeopplastingskoden skal bare ligge som miljøvariabel i Render, aldri i GitHub.
