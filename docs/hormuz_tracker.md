# Hormuz Tracker (integrasjon)

Denne appen har en Hormuz-modul på `/hormuz/`.

## Datakilde
- SQLite (default): `data/hormuz_ais.sqlite`
- API i appen: `GET /hormuz/api/traffic?hours=24`
- Status: `GET /hormuz/api/status`

## Miljøvariabler
- `AISTREAM_API_KEY` (påkrevd for innhenting)
- `HORMUZ_DB_PATH` (valgfri, absolutt sti anbefalt i prod)
- `HORMUZ_MAP_PATH` (valgfri)

Eksempel (Render/produksjon):

```bash
HORMUZ_DB_PATH=/var/data/hormuz_ais.sqlite
```

## Innsamling av AIS-data
Kjør manuelt:

```bash
python scripts/hormuz/collect_ais.py --minutes 10 --include-static
```

Eller fra UI:
- Åpne `/hormuz/`
- Trykk **Hent ferske AIS nå**

(Bruker `POST /hormuz/api/bootstrap` under panseret.)
Tips: Du kan gi lengre innhenting ved å kalle:
`POST /hormuz/api/bootstrap?minutes=3&max_messages=400`

## Folium-kart
Bygg kartfil:

```bash
python scripts/hormuz/build_map.py --hours 12 --latest-only --trails
```

Visning i app:
- `/hormuz/map`
- Kartet prøver nå å bygge seg automatisk hvis filen mangler.
- Tving rebuild: `/hormuz/map?rebuild=1`

## Merknad om retningstall
`northbound`/`southbound` i API-et beregnes heuristisk fra COG/heading (<180 / >=180).

## Feilsøking: får ikke ekte data
- Hvis du ser `DB mangler`, betyr det at filen ikke finnes på den stien appen bruker.
- Sjekk `/hormuz/api/status` for faktisk `db_path`.
- Sett `HORMUZ_DB_PATH` til persistent disk i produksjon.
- Kjør innhenting: `python scripts/hormuz/collect_ais.py --minutes 10 --include-static` eller knappen **Hent ferske AIS nå**.
- Timeout fra AISStream betyr ofte bare at det kom få meldinger i et kort vindu. Prøv `minutes=3` eller vent litt og prøv igjen.


## Slik tester du lenkene
Bruk disse direkte i nettleser eller curl:

```bash
curl -s https://prisanalyse.no/hormuz/api/db-path
curl -s https://prisanalyse.no/hormuz/api/status
curl -s "https://prisanalyse.no/hormuz/api/traffic?hours=24"
curl -s -X POST https://prisanalyse.no/hormuz/api/bootstrap
```

Hvis `api/db-path` viser en sti som ikke finnes (`db_exists=false`), sett `HORMUZ_DB_PATH` til riktig persistent disksti.
