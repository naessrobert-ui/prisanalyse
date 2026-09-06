# Timevis prisfallvakt på Raspberry Pi

Søket er brukerens FINN-URL med dealer_segment=3, fuel=4, mileage_to=140000,
registration_class=1 og sales_form=1. Alle resultatsidene leses, også eldre
annonser. Ingen ekstra annonseoppslag eller FINN-API-avtale kreves av koden.
FINN kan endre HTML-format, begrense søket eller avvise forespørsler; derfor
må første fulle innhenting verifiseres på Raspberry Pi før timeren aktiveres.

## Flyt

Første kjøring lagrer grunnpriser uten varsler. Neste vellykkede innhenting
kopierer forrige state til `prisfall_gml.json` og erstatter `prisfall_ny.json`
atomisk. De inneholder FINN-kode, pris, kilometerstand og historikk.
Gamle filer beholdes ved nettfeil, ufullstendig paginering og mistenkelig
stort fall i antall annonser. De to filene er aldri tomme mellom kjøringer.
Autoritativ state er alltid `prisfall_ny.json`; `gml` er en forrige kopi.

Prisfall på minst 10 000 kr ELLER 3 % vurderes med eksisterende kuppmotor
og fylkes-/merkekrav. Egenskapene hentes fra databasen bare når prisfall
finnes, med lokal ETag-cache for å unngå gjentatt nedlasting. Kun kandidatene
scores, med fersk FINN-pris og kilometerstand. Biler uten databaseegenskaper
eller kilometerstand venter på neste kjøring (maks to døgn).

Pushover viser km, gammel/ny pris, prisfall, beregnet verdi, FINN-lenke,
dager siden første databaseobservasjon, opprinnelig databasepris og de fire
siste observerte timeprisene. Første observasjon er ikke nødvendigvis
annonsens publiseringsdato. Mellomliggende historikk før oppstart finnes
ikke i timevakten. Samme pris varsles bare én gang per bil; videre nedgang
kan gi nytt varsel. Mislykket sending forsøkes på nytt hvis bilen fortsatt
finnes med samme pris. Et krasj rett etter sending før kvittering kan gi
duplikat. Fravær fra søket tolkes ikke som bekreftet salg.

## Installere

Fra oppdatert repo-rot på Pi:

```bash
git pull --ff-only
sudo bash deploy/pi/install-prisfall-time.sh
sudo systemctl start prisfall-time.service
sudo journalctl -u prisfall-time.service -n 80 --no-pager
```

Gjenbruker `/etc/kupp-vakt/kupp-vakt.env` hvis den finnes, samt repoets `.env`.
Valgfrie overstyringer i `/etc/prisfall-time.env` (root-eid, chmod 600):
AWS-oppsett, PUSHOVER_TOKEN, PUSHOVER_USER, PRISFALL_MIN_KR,
PRISFALL_MIN_PCT, PRISFALL_MAX_VARSLER og eksisterende KUPP_* terskler.
Kuppvaktens URL-sidebegrensning brukes ikke; timevakten leser hele søket.
Sørg for at KUPP_* samsvarer med GitHub-variablene til dagsvakten.

Etter logglinjen `Første grunnlag: ... biler; ingen varsler`:

```bash
sudo systemctl enable --now prisfall-time.timer
systemctl list-timers prisfall-time.timer
```

Timeren kjører klokken syv minutter over hver time, hele døgnet. Første
senere prisreduksjon kan varsles. Opptil nesten én time pluss kjøretid kan
gå før oppdagelse; salg mellom to oppslag kan fortsatt gå tapt.

Det tidligere prisfallsteget i GitHub-jobben er slått av etter verifisert
overgang. Bilradar-scoring og den eksisterende kuppvakten for nye annonser
kjører fortsatt som før.

Manuell kontroll uten sending/endring av snapshots:

```bash
.venv-prisfall/bin/python -m scripts.prisfall_time --dry-run --state-dir /var/lib/prisfall-time
```

Kjør som samme bruker som tjenesten. Slett eller rediger aldri snapshots
under en kjøring. Ved endret søke-URL brukes en ny state-mappe.
