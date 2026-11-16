# Opptak av møtet (lyd og bilde)

Knappen «Ta opp» i møtet lager et lokalt opptak i nettleseren til den som trykker.
Ingenting lastes opp til møtes.no, Render, Pi-en eller andre tjenester.

## Hva du får

Når opptaket stoppes (eller du forlater møtet), vises en dialog med filene:

- **Video av møtet** – 1280×720, alle kameraer satt sammen i et rutenett. Delt skjerm
  vises stort med deltakerne i en kolonne ved siden av. Den som snakker får grønn ramme.
- **Alle stemmer (miks)** – én lydfil, klar til å publiseres eller klippes.
- **Ett lydspor per deltaker** – for podkastredigering (Audacity, Reaper, Hindenburg,
  DaVinci Resolve osv.). Spor for deltakere som kom inn etter at opptaket startet, har
  starttidspunktet i filnavnet (`-starter-02m15s`), slik at de kan plasseres riktig.

Format: MP4/M4A (H.264/AAC) der nettleseren støtter det, ellers WebM (VP9/Opus).

## Personvern og begrensninger

- Alle i møtet ser «Opptak pågår (navn)» i toppen, et opptaksikon i deltakerlisten og en
  melding når opptaket starter og stopper. Nye deltakere får samme melding ved oppkobling.
- Den som tar opp er ansvarlig for at deltakerne er enige og for hvordan opptaket brukes.
- Filene finnes bare i fanen til de lastes ned. Lukkes fanen, er opptaket borte
  (nettleseren advarer). Maks 4 timer per opptak.
- Hver deltakers lyd tas opp slik den mottas over WebRTC (Opus), ikke som lokal
  studiokvalitet hos hver enkelt. Dempet mikrofon gir stillhet i sporet.
- Fungerer i oppdatert Chrome, Edge og Firefox på PC/Mac. Mobilnettlesere kan mangle støtte.

Serveren kjenner bare til et sant/usant-flagg `rec` i deltakerstatus, som videresendes
til de andre i rommet.
