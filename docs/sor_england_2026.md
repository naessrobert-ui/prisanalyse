# Sør-England 2026

Mobilvennlig, delbar reiseside for turen til Eastbourne, Rye, Brighton og London.

## Adresse

Når endringen er publisert:

`https://prisanalyse.no/ferie/sor-england-2026/`

Siden er merket `noindex,nofollow`, slik at den ikke er ment å dukke opp i søkemotorer. Den er likevel offentlig for alle som kjenner lenken.

## Innhold

- aktuell reisedag og overnattingssted
- full dag-for-dag-plan med kart- og informasjonslenker
- offentlig reservasjonssammendrag uten bestillingsnumre
- delingsknapp for mobil og skrivebord
- privat S3-basert bildegalleri
- bildeopplasting beskyttet med en egen familiekode

## Miljøvariabler

### Påkrevd for bildeopplasting

- `FERIE_UPLOAD_CODE`: kode familien må skrive inn for å laste opp bilder

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
