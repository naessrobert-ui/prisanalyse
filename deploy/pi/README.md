# Kupp-vakt på Raspberry Pi

Kjør kupp-vakt lokalt på Pi-en (blackberrypi) i stedet for GitHub Actions.
GitHubs `*/10`-cron er «best effort» og hopper ofte over kjøringer – Pi-en gir
deg pålitelig 10-minutters-kadens.

Kupp-vakt henter de nyeste FINN-annonsene, scorer dem med den samlede motoren
(lookup/variant + peer-WLS, ingen tung ML-modell), og sender **Pushover**-varsel
om biler som er billige mot modellen. Lookup-tabell, peer-koeffisienter og
state hentes fra S3 – Pi-en trenger derfor bare de lette avhengighetene og
AWS-nøkler.

**Kjøretid:** hvert 10. minutt mellom **06:00 og 23:59** (står stille om natten).

---

## Alternativ A – systemd (anbefalt)

Robust: `Persistent=true` tar igjen en hoppet kjøring hvis Pi-en var av, og alt
logges i journald.

```bash
cd ~/prisanalyse          # eller der repoet ligger på Pi-en
git pull                  # hent siste kode
cd deploy/pi
sudo ./install-kupp-vakt.sh
```

Skriptet:
1. lager et eget venv (`.venv-kupp`) og installerer `requirements-kupp-vakt.txt`,
2. legger env-malen til `/etc/kupp-vakt/kupp-vakt.env` (rettigheter 600),
3. installerer og starter `kupp-vakt.timer`.

Sett REPO_DIR/RUN_USER eksplisitt om nødvendig:

```bash
sudo REPO_DIR=/home/pi/prisanalyse RUN_USER=pi ./install-kupp-vakt.sh
```

### Etter installasjon

1. **Fyll inn nøkler:**
   ```bash
   sudo nano /etc/kupp-vakt/kupp-vakt.env
   ```
   Minst `AWS_*`, `S3_BUCKET_NAME`, `PUSHOVER_TOKEN` og `PUSHOVER_USER`.

2. **Test at varsling virker** (sender ett demo-varsel, rører ikke FINN/state):
   ```bash
   cd ~/prisanalyse
   sudo -u pi env $(grep -v '^#' /etc/kupp-vakt/kupp-vakt.env | xargs) \
     .venv-kupp/bin/python -m scripts.kupp_vakt --test
   ```

3. **Seed** (valgfritt – første ordinære kjøring seeder uansett selv, uten
   varsler):
   ```bash
   sudo systemctl start kupp-vakt.service
   ```

### Overvåking

```bash
systemctl list-timers kupp-vakt.timer     # når går den neste gang?
systemctl status kupp-vakt.service        # siste kjøring
journalctl -u kupp-vakt.service -n 80     # logg fra siste kjøringer
journalctl -u kupp-vakt.service -f        # følg live
```

### Endre terskler og filtre

Rediger env-fila (`/etc/kupp-vakt/kupp-vakt.env`, eller `~/projects/kupp_env`
ved cron-oppsett) og lagre. Ingen restart trengs – hver kjøring leser fila på nytt.

| Variabel | Hva |
|----------|-----|
| `KUPP_RABATT_TRAPP` | Trappetrinns rabattkrav etter pris. Default `50000:30,100000:20,150000:15,250000:7,:6` = <50k krever 30 %, <100k 20 %, <150k 15 %, <250k 7 %, ellers 6 %. |
| `KUPP_RABATT_KR_MIN` | Valgfri flat kroneterskel i tillegg (0 = av). |
| `KUPP_UNDER_HURTIG` | `1` = varsle også hvis pris < hurtigpris (default `0`). |
| `KUPP_SELGER` | Selger-type: `privat` (default), `merkeforhandler`, `annet` eller `alle`. Kupp finnes hos private. Server-side. |
| `KUPP_DRIVSTOFF` | Kun disse drivstoffene, f.eks. `Elektrisk` eller `Elektrisk,Hybrid`. Tom = alle. |
| `KUPP_FYLKE` | Kun disse fylkene, f.eks. `Vestland,Rogaland` (eller rå FINN-kode `0.22046`). Tom = hele landet. Server-side hardt filter. |
| `KUPP_STED` | Delstreng på poststed/område, f.eks. `Bergen,Voss`. Finere enn fylke. |
| `KUPP_HJEMFYLKE` | Fylket der vanlig terskel gjelder (default `Vestland`). Biler *utenfor* krever større rabatt (se under). |
| `KUPP_UTENFOR_TILLEGG_PP` | Ekstra rabattkrav i prosentpoeng for biler i *resten* av landet (default `8`, `0` = av). Krever `KUPP_FYLKE` tom. |
| `KUPP_NABOFYLKE` | Nabofylker med kortere reise som får et *mindre* tillegg enn resten, f.eks. `Rogaland,Møre og Romsdal`. Tom = ingen nabo-nivå. |
| `KUPP_NABO_TILLEGG_PP` | Ekstra rabattkrav (prosentpoeng) for nabofylkene (default `5`). |
| `KUPP_EL_TIER` | `1` slår på merke-tiered rabattkrav for elbiler (erstatter pris-trappa for EV). Fra `kupp_backtest --per produsent`. Default av. |
| `KUPP_EL_MERKER_LAV` / `_MEDIUM` / `_HOY` | Elbil-merker per tier (komma-sep.). Sterke merker → lav terskel. |
| `KUPP_EL_TERSKEL_LAV` / `_MEDIUM` / `_HOY` / `_DEFAULT` | Rabattkrav i % per tier (default `2`/`6`/`12`/`12`). Ukjent EV-merke → DEFAULT. |
| `KUPP_KURANTE` | «Merke Modell»-fragmenter (komma-sep.) som får lavere krav fordi de er lette å omsette, f.eks. `Volkswagen Golf,Toyota RAV4`. |
| `KUPP_KURANT_LETTELSE_PP` | Hvor mye lavere rabattkrav (prosentpoeng) kurante modeller får (default `3`). |

Gyldige fylkesnavn: Østfold, Akershus, Oslo, Innlandet, Buskerud, Vestfold,
Telemark, Agder, Rogaland, Vestland, Møre og Romsdal, Trøndelag, Nordland,
Troms, Finnmark.

**Hjemfylke vs. hardt fylkesfilter:** `KUPP_FYLKE=Vestland` gir *bare* Vestland-biler.
Vil du heller se hele landet, men slippe færre langvekksbiler gjennom, la `KUPP_FYLKE`
stå tom og bruk `KUPP_HJEMFYLKE=Vestland` + `KUPP_UTENFOR_TILLEGG_PP`. Da beholder
Vestland-biler trappa, mens biler utenfor må ned et ekstra hakk i pris (dekker frakt).
Eksempel: en bil til 300 000 krever 6 % i Vestland, men 6 + 8 = 14 % utenfor. En kurant
Golf til 120 000 krever 15 % normalt, men 12 % siden den er lett å selge videre. Vekten
per bil vises i loggen og i `--vis-alle`.

### Se hva som kjennetegner biler som blir solgt (tuning-grunnlag)

Hver bil vakten varsler om logges nå til S3 (`calc/bil/kupp_vakt_logg.json`) med
egenskapene sine (rabatt, pris, merke, fylke, selger, drivstoff, km, år). Kjør
etteranalysen for å se hvilke av disse som faktisk blir solgt på FINN – da vet du
hvilke terskler/filtre som treffer, og hvilke som bare gir støy:

```bash
cd ~/prisanalyse
sudo -u pi env $(grep -v '^#' /etc/kupp-vakt/kupp-vakt.env | xargs) \
  .venv-kupp/bin/python -m scripts.kupp_analyse            # rapport til skjerm
  # --csv  skriver også per-bil CSV til S3 (calc/bil/kupp_vakt_analyse.csv)
  # --limit 30  tester på et utvalg;  --verbose  viser status per bil
```

Rapporten bryter ned **solgt-andel** og **median dager til solgt** etter rabatt,
pris, drivstoff, selger, fylke, sted, merke, årsmodell og kjørelengde. Høy
solgt-andel i en gruppe = vakten finner ekte kupp der; lav andel = kandidat for
strengere terskel eller å filtreres bort.

> Loggen bygges fra og med denne oppdateringen, og inneholder kun bilene vakten
> faktisk varsler om. Er den fortsatt tom, sier analysen fra og stopper – den
> bruker *ikke* state-fila som grunnlag (den teller alle sette annonser, ikke
> bare kupp). Full nedbrytning kommer etter noen dager med logging; kjør analysen
> når som helst, f.eks. manuelt eller en gang i uka via en egen timer. Vil du ha
> med varsler fra før loggingen, se «Bakfyll fra Pushover» under.

#### Bakfyll gamle varsler fra Pushover

Varsler sendt før loggingen ble slått på finnes bare som Pushover-meldinger på
telefonen. Teksten inneholder likevel FinnKode, merke/modell, år, km, sted, pris,
rabatt og forventet pris. Lim inn meldingene i en tekstfil (sett gjerne en
`== 2026-08-08 ==`-datolinje foran gruppene fra samme dag) og importer dem inn i
loggen – eksisterende rader røres ikke:

```bash
python -m scripts.kupp_import_pushover meldinger.txt --dry-run   # forhåndsvis
python -m scripts.kupp_import_pushover meldinger.txt             # skriv til S3
```

Pushover har ingen bulk-eksport / historikk-API for allerede leverte varsler, så
teksten må kopieres ut av appen manuelt. Trenger du ikke akkurat de dagene, er det
enkleste bare å la den nye loggen bygge seg opp.

### Endre tidsvindu

Rediger `OnCalendar` i `/etc/systemd/system/kupp-vakt.timer`, så:
```bash
sudo systemctl daemon-reload && sudo systemctl restart kupp-vakt.timer
```

### Avinstaller

```bash
sudo systemctl disable --now kupp-vakt.timer
sudo rm /etc/systemd/system/kupp-vakt.{service,timer}
sudo systemctl daemon-reload
```

---

## Alternativ B – vanlig cron

Hvis du heller vil bruke cron (som `konsolider_data.py`). Bruk et venv, og last
env-fila i kommandoen:

```cron
# Kupp-vakt hvert 10. min, 06:00-23:59
*/10 6-23 * * * cd /home/pi/prisanalyse && set -a && . /etc/kupp-vakt/kupp-vakt.env && set +a && /home/pi/prisanalyse/.venv-kupp/bin/python -m scripts.kupp_vakt >> /home/pi/kupp_vakt.log 2>&1
```

Alternativt kan du legge en `.env`-fil i repo-rota (kupp-vakt leser den
automatisk via `python-dotenv`), og droppe `set -a … set +a`-biten. Husk i så
fall at `.env` **ikke** skal committes.

---

## Slå av GitHub-kjøringen

Når Pi-en har overtatt, skru av den planlagte GitHub-kjøringen så du ikke får
dobbelt opp (begge deler deler samme state-fil i S3, så det er ikke farlig – men
unødvendig): kommenter ut `schedule`-blokken i
`.github/workflows/kupp-vakt.yml`, eller deaktiver workflowen under
**Actions → Kupp-vakt → ⋯ → Disable workflow**. `workflow_dispatch` (manuell
kjøring / `--test`) beholdes uansett.

---

## Feilsøking

| Symptom | Sjekk |
|--------|-------|
| Ingen varsler | Første kjøring seeder alltid (ingen varsler). Deretter varsles kun annonser som er *nye* siden forrige kjøring. |
| `--test` gir ingen push | `PUSHOVER_TOKEN`/`PUSHOVER_USER` i env-fila. Token = app-token fra pushover.net/apps, user = user key fra forsiden. |
| AWS-feil / tom lookup | `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`/`AWS_REGION`/`S3_BUCKET_NAME`. |
| Tjenesten kjører ikke | `systemctl list-timers kupp-vakt.timer` og `journalctl -u kupp-vakt.service`. |
