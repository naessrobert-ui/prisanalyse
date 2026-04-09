# Hormuz Tracker (integrasjon)

Denne appen har en Hormuz-modul på `/hormuz/`.

## Datakilde
- SQLite: `data/hormuz_ais.sqlite`
- API i appen: `GET /hormuz/api/traffic?hours=24`

## Innsamling av AIS-data
Kjør:

```bash
python scripts/hormuz/collect_ais.py --minutes 10 --include-static
```

Sett API-nøkkel via miljøvariabel:

```bash
export AISTREAM_API_KEY=din_nokkel
```

## Folium-kart
Bygg kartfil:

```bash
python scripts/hormuz/build_map.py --hours 12 --latest-only --trails
```

Visning i app:
- `/hormuz/map`

## Merknad om retningstall
`northbound`/`southbound` i API-et beregnes heuristisk fra COG/heading (<180 / >=180).


## Feilsøking: demo i stedet for ekte data
- `GET /hormuz/api/traffic` returnerer nå feilstatus hvis DB/tabell mangler.
- Sjekk status-endepunkt: `GET /hormuz/api/status`.
- Hvis DB er tom, kjør innsamling igjen: `python scripts/hormuz/collect_ais.py --minutes 10 --include-static`.
- Deretter oppdater siden `/hormuz/` og trykk **Last ekte data (SQLite)**.
