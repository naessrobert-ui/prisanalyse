# Automatisk daglig opplasting av TopChanges-CSV

Dette setter opp en jobb som hver virkedag plukker nyeste
`TopChanges_Nordea_Invest_DAG_YYMMDD.csv` fra nettverksdelingen og sender
den til appen, som oppdaterer databasen og laster den opp til S3 – uten at
noen trenger å trykke på «Oppdater DB fra CSV».

Skript: `scripts/upload_topchanges_auto.py` (kun Python-standardbibliotek –
ingen `pip install` nødvendig).

---

## 1) Server-side: sett opplastings-token i Render

Endepunktet er nå låst med en token, slik at bare den automatiske jobben
(eller en innlogget nettleser) kan laste opp.

1. Render → tjenesten (prisanalyse) → **Environment**.
2. Legg til:
   - Key: `HANDLER_UPLOAD_TOKEN`
   - Value: en lang, tilfeldig streng. Generer f.eks. slik:
     ```bash
     python -c "import secrets; print(secrets.token_urlsafe(32))"
     ```
3. **Save / Deploy**.

> Så lenge `HANDLER_UPLOAD_TOKEN` ikke er satt, er endepunktet åpent som før
> (men nettsidene er uansett beskyttet av tilgangskoden). Sett tokenet for å
> aktivere maskin-til-maskin-låsen.

---

## 2) Velg maskin: server vs. PC

Jobben må kjøre på en maskin som:
- er **påslått** på det planlagte tidspunktet, og
- har tilgang til nettverksdelingen `\\no-fileserver\...\Eiere`.

| | Nordea-server | Din Nordea-PC |
|---|---|---|
| Kjører uten at du er pålogget | ✅ | ✅ (med riktig oppsett) |
| Alltid på | ✅ | ❌ må stå på kl. 07:00 |
| Robusthet | Best | OK |

**Anbefalt:** en server som alltid står på. En PC fungerer, men bare hvis
den er på (ikke avslått/dvale) når jobben skal kjøre.

---

## 3) ⚠️ Bruk UNC-sti, ikke tilkoblet stasjon (I:)

`I:` er en tilkoblet stasjon som bare finnes i din innloggede økt. Når
jobben kjører **uten** at du er pålogget, finnes ikke `I:`. Bruk derfor
nettverksstien direkte:

```
I stedet for:  I:\6_EQUITIES\Database\Eiere
bruk:          \\no-fileserver\NIMNOR\6_EQUITIES\Database\Eiere
```

Kontoen jobben kjører under må ha lesetilgang til denne delingen.
(Juster `NIMNOR`/stien hvis den faktiske UNC-adressen er en annen – sjekk i
Explorer: adressefeltet viser `\\server\share\...`.)

---

## 4) Test manuelt først

Fra maskinen som skal kjøre jobben, i PowerShell/CMD:

```bat
set HANDLER_UPLOAD_TOKEN=<samme token som i Render>
python scripts\upload_topchanges_auto.py --dir "\\no-fileserver\NIMNOR\6_EQUITIES\Database\Eiere" --dry-run
```

`--dry-run` finner nyeste fil og logger hva den *ville* gjort, uten å laste
opp. Ser det riktig ut, kjør uten `--dry-run` for en ekte test. Kjør på nytt
og den skal si «allerede lastet opp».

Nyttige flagg: `--force` (last opp selv om datoen er sendt), `--url`,
`--state`, `--log`.

---

## 5) Opprett oppgaven i Windows Task Scheduler

1. Åpne **Task Scheduler** → **Create Task…** (ikke «Basic Task»).
2. **General**:
   - Navn: `TopChanges daglig opplasting`.
   - Velg **Run whether user is logged on or not** (dette er det som gjør
     at den kjører uten at du er pålogget).
   - Hak av **Run with highest privileges** om nødvendig for nettverkstilgang.
3. **Triggers** → **New…**:
   - Daily, start kl. **07:00** (filene lander ca. 06:06).
   - (Valgfritt) Begrens til ukedager – men jobben hopper uansett pent over
     dager uten ny fil.
4. **Actions** → **New…**:
   - Action: **Start a program**.
   - Program/script: `python` (eller full sti til `python.exe`).
   - Add arguments:
     ```
     "C:\sti\til\prisanalyse\scripts\upload_topchanges_auto.py" --dir "\\no-fileserver\NIMNOR\6_EQUITIES\Database\Eiere"
     ```
   - Legg skriptet et fast sted på maskinen (det trenger ikke hele repoet).
5. **Settings**: hak av «Run task as soon as possible after a scheduled
   start is missed».
6. Lagre. Du blir bedt om passordet til kontoen (nødvendig for å kjøre uten
   pålogging).

### Token og sti til Task-kontoen

Skriptet leser `HANDLER_UPLOAD_TOKEN` fra miljøet. Sett den som en
**system-miljøvariabel** på maskinen (System Properties → Environment
Variables → System variables), så den er tilgjengelig for Task-kontoen –
ikke bare for din egen bruker.

---

## 6) Kontroll i drift

- **Logg:** `%LOCALAPPDATA%\topchanges_upload\upload.log` (eller stien du ga
  med `--log`).
- **Status-fil:** `sist_opplastet.txt` samme sted – inneholder sist
  opplastede dato (YYMMDD).
- **Task Scheduler → History / Last Run Result:** `0x0` = OK. Alt annet
  betyr feil – se loggen.
- **I appen:** forsiden til Handler viser «Siste dato i aktiv DB» og «Siste
  oppdatering på S3-objekt» – disse skal følge dagens dato etter kjøring.

---

## Feilsøking

| Symptom | Årsak / løsning |
|---|---|
| «Mappen finnes ikke / ikke tilgjengelig» | Jobben bruker `I:` i stedet for UNC, eller kontoen mangler tilgang til delingen. |
| HTTP 401 i loggen | `HANDLER_UPLOAD_TOKEN` på maskinen matcher ikke verdien i Render. |
| HTTP 400 «HANDLER_DB_S3_URI er ikke satt» | S3-mål mangler i Render-miljøet. |
| Kjører når du er pålogget, men ikke ellers | «Run whether user is logged on or not» ikke valgt, eller `I:` brukt i stedet for UNC. |
| «Ingen filer som matcher …» | Helg/helligdag uten ny fil (normalt), eller feil mappe/mønster. |
