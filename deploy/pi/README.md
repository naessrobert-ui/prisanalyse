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
| `KUPP_FYLKE` | Kun disse fylkene, f.eks. `Vestland,Rogaland` (eller rå FINN-kode `0.22046`). Tom = hele landet. Server-side filter. |
| `KUPP_STED` | Delstreng på poststed/område, f.eks. `Bergen,Voss`. Finere enn fylke. |

Gyldige fylkesnavn: Østfold, Akershus, Oslo, Innlandet, Buskerud, Vestfold,
Telemark, Agder, Rogaland, Vestland, Møre og Romsdal, Trøndelag, Nordland,
Troms, Finnmark.

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
