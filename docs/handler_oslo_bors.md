# Handler Oslo Børs fra prisanalyse.no

Du trenger **ikke** kopiere filene fra `handler` inn i `prisanalyse`.

Anbefalt oppsett er at `handler` kjører som egen Streamlit-app (egen prosess/port), og at `prisanalyse` peker til den via miljøvariabel.

## 1) Kjør handler-appen separat
Eksempel (på server):

```bash
cd /path/to/handler
streamlit run app.py --server.port 8501 --server.address 0.0.0.0
```

## 2) Pek prisanalyse til handler
Sett miljøvariabel på prisanalyse-tjenesten (primært):

```bash
HANDLER_OSLO_BORS_URL=https://din-host/handler
```

eller direkte til streamlit-port (kun internt/enkeltoppsett):

```bash
HANDLER_OSLO_BORS_URL=http://127.0.0.1:8501
```

Deretter restart prisanalyse.

Alternative navn som også støttes: `HANDLER_OSLO_BORS_APP_URL`, `HANDLER_URL`, `STREAMLIT_HANDLER_URL`.

Hvis ingen env er satt, vises en oppsettside med tydelig beskjed i UI.

## 3) Hvordan brukeren åpner appen
- Meny: **Bil → Handler Oslo Børs**
- URL: `/handler-oslo-bors/`

Standard er redirect til `HANDLER_OSLO_BORS_URL` (anbefalt for Streamlit).

## 4) Deep links (undersider/parametre)
Hvis du trenger sti eller parametre, støttes dette:

```text
/handler-oslo-bors/rapport/dag?symbol=EQNR.OL
```

Dette videresendes til handler-appen.

## 5) Midlertidig test uten env-var
Du kan teste direkte:

```text
/handler-oslo-bors/?app_url=https://din-app.example.com
```

## 6) Når må du kopiere filer?
Kun hvis du bevisst vil slå sammen prosjektene til én app.
I normal drift anbefales **ikke** kopiering; hold `handler` som separat repo/tjeneste.


## 7) Starte handler "direkte" via prisanalyse
Ja, med disse endringene kan brukeren åpne handler direkte fra prisanalyse-menyen.

Hvis du også vil at prisanalyse skal forsøke å starte handler-prosessen automatisk når den ikke kjører,
kan du sette:

```bash
HANDLER_OSLO_BORS_AUTOSTART_CMD="streamlit run app.py --server.port 8501 --server.address 0.0.0.0"
HANDLER_OSLO_BORS_AUTOSTART_CWD="/path/to/handler"
# valgfritt: hvor lenge prisanalyse venter etter startforsøk (sekunder)
HANDLER_OSLO_BORS_AUTOSTART_WAIT="2.5"
```

Da vil første kall til `/handler-oslo-bors/` forsøke å starte handler (kun når `app_url` ikke sendes i query).


## 8) Database: lokal vs S3
Kort svar:
- **Raskest i drift**: lokal disk på server (f.eks. `/tmp/topchanges_sqlite_work/topchanges.db`).
- **Best for distribusjon/backup**: S3 som kilde, og appen henter filen ned lokalt ved oppstart.

Nye miljøvariabler for Flask-varianten av Handler:

```bash
# Lokal sti (brukes direkte hvis filen finnes)
HANDLER_LOCAL_DB_PATH=/tmp/topchanges_sqlite_work/topchanges.db

# Valgfritt: fallback til S3 hvis lokal fil mangler
HANDLER_DB_S3_URI=s3://mitt-bucket/topchanges/topchanges.db
HANDLER_DB_S3_REGION=eu-west-1
HANDLER_DB_S3_AUTO_DOWNLOAD=1
```

Hvordan det fungerer:
1. Appen sjekker `HANDLER_LOCAL_DB_PATH`.
2. Hvis filen mangler og `HANDLER_DB_S3_URI` er satt, lastes DB ned til lokal sti automatisk.
3. Deretter kjører alle spørringer mot lokal fil (lavere latency enn å lese direkte fra nett).

> Merk: Appen kan ikke hente filer direkte fra din private PC uten at du først laster dem opp et sted (S3, SCP, rsync, osv.).


## 9) Hvor legger jeg inn eksakt lokal filsti?
Legg den i miljøvariabelen `HANDLER_LOCAL_DB_PATH` på **serveren der Flask-appen kjører** (ikke på din egen PC).

Vanligst er å bruke `.env` i prosjektroten (samme sted som `app.py`):

```env
HANDLER_LOCAL_DB_PATH=/tmp/topchanges_sqlite_work/topchanges.db
```

Eksempel på Windows-server:

```env
HANDLER_LOCAL_DB_PATH=C:\topchanges_sqlite_work\topchanges.db
```

Etter at du har lagret `.env`, restart appen/Gunicorn.


## 10) Ja — på Render skal dette settes i **Environment Variables**
Ja, du skal sette dette i Render under tjenesten din:

1. Gå til Render → **Web Service** (prisanalyse).
2. Velg **Environment**.
3. Legg til variabel:
   - Key: `HANDLER_LOCAL_DB_PATH`
   - Value: full sti til DB-filen på serveren (eksempel: `/tmp/topchanges_sqlite_work/topchanges.db`)
4. (Valgfritt) legg til S3 fallback:
   - `HANDLER_DB_S3_URI=s3://bucket/path/topchanges.db`
   - `HANDLER_DB_S3_REGION=eu-west-1`
5. Trykk **Save / Deploy** og restart tjenesten.

Viktig:
- Dette er **ikke** en egen setting inne i "Render template" i appen.
- Det er en vanlig miljøvariabel i Render, som leses av `load_dotenv`/`os.getenv` i appen.


## 11) Hvordan lese Render-loggen for denne feilen
Linjene du sendte er hovedsakelig **access logs** (HTTP 200/503), ikke detaljert app-feil.

Når DB feiler nå, logger appen også en egen warning med diagnostikk, f.eks.:

```text
Handler DB utilgjengelig: {'path': '/tmp/topchanges_sqlite_work/topchanges.db', 'path_exists': False, 'parent_exists': True, 's3_uri_configured': False, ...}
```

Tolkning:
- `path_exists: False` => filen finnes ikke på den stien.
- `parent_exists: False` => mappen finnes ikke (feil sti eller ikke opprettet).
- `s3_uri_configured: False` => ingen S3 fallback er satt.

Hvis du får 503 på `/handler-oslo-bors/api/search-investors`, er årsaken nesten alltid at DB-filen ikke ligger på `HANDLER_LOCAL_DB_PATH`, eller at S3 fallback ikke er konfigurert/tilgjengelig.
