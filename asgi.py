"""ASGI entrypoint that serves the large Flask app and FastAPI regnskapsmodul together."""

from fastapi import FastAPI
from a2wsgi import WSGIMiddleware

from app import app as flask_app
from Fastapi_Backend import app as regnskap_api

app = FastAPI(title="Prisanalyse Combined ASGI", version="1.0.0")

# Flask app keeps existing routes on '/'
app.mount("/", WSGIMiddleware(flask_app))

# FastAPI regnskapsmodul exposed under dedicated prefix to avoid route collisions.
app.mount("/regnskap-api", regnskap_api)
