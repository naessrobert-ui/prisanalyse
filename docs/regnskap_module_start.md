# Starte regnskapsmodulen

Ja — den kan kjøres **som en del av den store appen**.

Du har nå to alternativer:

## Alternativ A: Samlet app (anbefalt)

Kjører Flask-hovedappen + FastAPI-regnskapsmodulen i samme prosess (ASGI).

### 1) Miljøvariabler

```bash
export DATABASE_URL='postgresql://<user>:<password>@<host>:5432/<db>'
export FLASK_SECRET_KEY='change-me'
export PORT='8000'
```

### 2) Installer avhengigheter

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 3) Start samlet app

```bash
uvicorn asgi:app --host 0.0.0.0 --port ${PORT:-8000}
```


### Viktig om `DATABASE_URL`

I samlet ASGI-modus vil hovedappen starte selv om `DATABASE_URL` mangler, men `regnskap-api` blir da satt i fallback-modus med feilmelding på `/regnskap-api/health`.

### 4) Endepunkter

- Flask (eksisterende): `http://localhost:8000/`
- FastAPI regnskap: `http://localhost:8000/regnskap-api/health`
- FastAPI docs: `http://localhost:8000/regnskap-api/docs`

---

## Alternativ B: Kun regnskapsmodulen (isolert)

```bash
export DATABASE_URL='postgresql://<user>:<password>@<host>:5432/<db>'
python Fastapi_Backend.py
```

Health check:

```bash
curl http://localhost:8000/health
```

---

## Deploy (Procfile)

For å kjøre samlet app med gunicorn i produksjon, bruk ASGI-worker og `asgi:app`.

```Procfile
web: gunicorn -k uvicorn.workers.UvicornWorker asgi:app
```

Hvis du fortsatt vil kjøre ren Flask-variant, behold eksisterende `web: gunicorn app:app`.
