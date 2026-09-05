# Prisfallvakt for elbiler

`scripts/prisfall_vakt.py` bruker hele resultatet fra «Scor Bilradar aktive
biler». Denne jobben utløses etter at innhentingen har oppdatert databasen,
og kjører dessuten hver time. Prisvakten gjør ingen ekstra FINN-oppslag.
Gamle annonser får derfor først oppdaget prisfall når innhentingen ser dem
igjen, normalt ved den daglige gjennomgangen.

## Hva som varsles

- Kun rene elbiler med eksplisitt drivstoff El/Elektrisk/Elbil/BEV og aktiv
  status NEI. Hybrider og ukjent drivstoff utelates.
- Omfanget følger Bilradar: dagens scoringsjobb krever gyldig alder og
  salgspris på minst 15 000 kr før bilen inngår i resultatfilen.
- Både private og forhandlere inngår. Prisvakten bruker ikke kuppvaktens
  server-side selgerfilter, siden den leser hele Bilradar-datasettet.
- Prisen må ha falt minst 10 000 kr **eller** 3 prosent siden siste gyldige
  observasjon i prisvaktens egen state. Opprinnelig annonsepris brukes ikke.
- Bilradars ferske forventede pris brukes til å beregne rabatt på nytt.
  Kuppvaktens eksisterende pris-/merkegrenser og fylkesjustering gjenbrukes.
  `KUPP_FYLKE` og `KUPP_STED` respekteres; elbilfilteret er alltid på.
- Første kjøring og nye FINN-koder lagrer bare grunnpriser. Ingen historiske
  prisfall varsles ved oppstart. Prisfall fra før oppstart fanges ikke opp.
- Et vellykket varsel huskes per FINN-kode og ny pris. Ytterligere prisfall
  kan varsles; opp og ned til en allerede varslet pris varsles ikke igjen.
- Hver bil får eget Pushover-varsel med gammel/ny pris, prisfall, beregnet
  verdi og FINN-lenke. Ingen e-post sendes fra prisvakten.

## Oppsett

Arbeidsflyten `.github/workflows/score-bilradar-aktive.yml` kjører prisvakten
etter vellykket scoring. Den bruker eksisterende AWS- og Pushover-secrets i
**prisanalyse-repoet**. Pushover-nøkler som kun ligger på Raspberry Pi-en er
ikke automatisk tilgjengelige for GitHub Actions.

Valgfrie repository variables:

| Variabel | Standard | Betydning |
| --- | --- | --- |
| `PRISFALL_MIN_KR` | `10000` | Minste prisfall i kroner |
| `PRISFALL_MIN_PCT` | `3` | Minste prisfall i prosent, alternativ til kroner |
| `PRISFALL_MAX_VARSLER` | `40` | Maks biler varslet per kjøring; resten venter |
| `KUPP_*` | Se arbeidsflyten | Samme konfigurasjonsnavn som kuppvakten |

Kuppvaktens miljøvariabler på Pi-en kan avvike fra repository variables.
Kopier eventuelle egne terskler til GitHub-variablene hvis begge skal være
identiske. Kode-defaultene endres ikke av denne utvidelsen.

Egen state ligger i `calc/bil/prisfall_vakt_state.json` i samme S3-bøtte som
Bilradar. Den er adskilt fra kuppvakten for nye annonser. Gamle state-rader
beholdes i 120 dager etter siste observasjon. Ingen grunnpris settes fra
tomme/ugyldige priser eller priser under 1 500 kr.

## Manuell kontroll

Fra rotmappen i prisanalyse, med samme AWS-miljø som vanlig:

```bash
# Les siste Bilradar-fil fra S3; vis treff uten sending eller lagring
python -m scripts.prisfall_vakt --dry-run

# Bruk en lokal fil fra dagens scoring
python -m scripts.prisfall_vakt --input /tmp/bilradar_aktive.parquet --dry-run

# Normal kjøring (håndteres allerede av arbeidsflyten)
python -m scripts.prisfall_vakt
```

`--seed` nullstiller sammenligningsgrunnlaget uten sending. Det sletter også
tidligere varslingshistorikk; bruk det bare ved en bevisst omstart. Manuell
scoring med `upload=false` hopper helt over prisvakten.

## Feil og gjentatte kjøringer

Prisvakten avbryter ved lesefeil eller korrupt state; bare `NoSuchKey` tolkes
som første kjøring. Ventende prisfall lagres før sending, og et sendt varsel
kvitteres i state straks etterpå. Manglende Pushover-oppsett eller mislykket
sending gir feilstatus og nytt forsøk ved neste kjøring. Manglende verdsettelse
beholder også prisfallet for ny vurdering.

Varsler krever at bilen finnes i det gjeldende resultatet, har en observasjon
fra siste to døgn og at prisfallet er høyst to døgn gammelt. Endret pris
erstatter tidligere ventende hendelse. Eldre snapshots kan ikke spole tilbake
lagret pris. Et kort avbrudd mellom vellykket Pushover-sending og lagring av
kvittering kan likevel gi et duplikat; Pushover og S3 er separate systemer.
Ved flere Pushover-mottakere følger sendingen kuppvaktens eksisterende regel:
minst én vellykket mottaker regnes som sendt.

Arbeidsflytens concurrency-lås dekker både scoring og varsling, og pågående
kjøringer avbrytes ikke. Ikke start en parallell Pi-cron med samme state-nøkkel.

Tester (ingen FINN-, AWS- eller Pushover-kall):

```bash
python -m pytest -q tests/test_prisfall_vakt.py tests/test_kupp_vakt.py tests/test_kupp_vakt_tier.py
```
