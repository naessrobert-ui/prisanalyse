# -*- coding: utf-8 -*-
"""AI-oppsummert værvarsel for de faste stedene (/ver/api/ai-varsel/<sted>).

Claude lager ikke tallene. Den leser det samme grunnlaget som varselsiden
(Yr, nowcast/radar, Google, WeatherNext, målinger og treffsikkerhet) og
skriver to korte tekster:

* kort:      de neste timene og døgnet (praktisk: når regner det, paraply?)
* overblikk: trenden de neste 1-2 ukene, med ærlig usikkerhet

Teksten lages høyst én gang i timen per sted og holdes i minnet, så antall
API-kall er uavhengig av trafikken. Uten ANTHROPIC_API_KEY svarer endepunktet
at funksjonen er av, og siden viser ingenting.
"""
from __future__ import annotations

import json
import logging
import os
import threading
from datetime import datetime, timezone
from typing import Any, Optional

logger = logging.getLogger(__name__)

MODELL = os.environ.get("VAER_AI_MODELL", "claude-sonnet-5-5")
MAKS_TOKENS = 600
#: Grunnlaget kuttes til dette (tegn) for å holde kallet lite og billig.
MAKS_INPUT = 30_000

SYSTEM = """Du er en erfaren norsk meteorolog som skriver korte værvarsler for blirdetfint.no.
Du får ferdige data fra flere kilder. Du lager ikke egne prognoser, men vurderer, sammenstiller og forklarer.

Skriv på norsk bokmål, enkelt og konkret, som til en venn. Bruk klokkeslett og ukedager, ikke datoer.
Ikke bruk tankestrek (–/—), emoji eller overskrifter. Ikke nevn JSON, felter eller "dataene".

Lag to tekster:
1. "kort": 2-3 setninger om de neste timene og resten av døgnet. Start med det viktigste for noen som
   skal ut nå (regner det, når slutter/begynner det, vind). Si fra hvis kildene er uenige, og hvem du stoler
   mest på ut fra treffsikkerheten hvis den finnes.
2. "overblikk": 2-4 setninger om de neste 1-2 ukene: hovedtrekk, når det ser best/verst ut, og hvor sikkert
   det er. Bruk sannsynlighetsord (trolig, kanskje, usikkert) når kildene spriker eller det er langt frem.
   Sammenlign bare med "normalt" hvis grunnlaget faktisk inneholder normaler.

Svar med kun et JSON-objekt: {"kort": "...", "overblikk": "..."}"""

_cache: dict[str, tuple[str, dict[str, Any]]] = {}
_laaser: dict[str, threading.Lock] = {}
_laas = threading.Lock()
#: Etter en feil prøves det ikke igjen for stedet før dette har gått (sekunder).
PAUSE_ETTER_FEIL = 600
_feilet: dict[str, float] = {}


def aktivert() -> bool:
    return bool((os.environ.get("ANTHROPIC_API_KEY") or "").strip())


def grunnlag(payload: dict[str, Any]) -> str:
    """Det Claude får se: varselsidens payload uten de tunge visningsfeltene."""
    nowcast = payload.get("nowcast") or {}
    steps = nowcast.get("steps") or []
    data = {
        "sted": (payload.get("place") or {}).get("name"),
        "tidspunkt": payload.get("generated_at"),
        "naa": payload.get("now"),
        "maalt_nedbor_i_dag": payload.get("observed"),
        "radar_neste_2_timer": {"tilgjengelig": nowcast.get("available"), "steg": steps[::2]},
        "resten_av_dagen": payload.get("rest_of_day"),
        "i_natt_eller_i_morgen": payload.get("later"),
        "beste_vindu": payload.get("best"),
        "timer": payload.get("hours"),
        "dager": payload.get("days"),
        "weathernext_dager": payload.get("wn_days"),
        "enighet_yr_google": payload.get("agreement"),
        "treffsikkerhet": payload.get("trust"),
        "i_dag_score": payload.get("today_score"),
    }
    tekst = json.dumps(data, ensure_ascii=False, default=str, separators=(",", ":"))
    return tekst[:MAKS_INPUT]


def tolk_svar(tekst: str) -> Optional[dict[str, str]]:
    """Plukker JSON-objektet ut av svaret, også om modellen pakker det inn."""
    start, slutt = tekst.find("{"), tekst.rfind("}")
    if start < 0 or slutt <= start:
        return None
    try:
        obj = json.loads(tekst[start:slutt + 1])
    except ValueError:
        return None
    kort, overblikk = str(obj.get("kort") or "").strip(), str(obj.get("overblikk") or "").strip()
    if not kort and not overblikk:
        return None
    # Husregel for siden: ingen tankestreker.
    vask = lambda s: s.replace(" – ", ", ").replace(" — ", ", ").replace("—", ", ").replace("–", "-")
    return {"kort": vask(kort), "overblikk": vask(overblikk)}


def _spor_claude(payload: dict[str, Any]) -> Optional[dict[str, str]]:
    import anthropic

    client = anthropic.Anthropic()
    msg = client.messages.create(
        model=MODELL,
        max_tokens=MAKS_TOKENS,
        system=SYSTEM,
        messages=[{"role": "user", "content": grunnlag(payload)}],
    )
    tekst = "".join(getattr(b, "text", "") for b in msg.content)
    return tolk_svar(tekst)


def hent(sted: str, lag_payload, now: Optional[datetime] = None, spor=None) -> Optional[dict[str, Any]]:
    """AI-tekstene for stedet denne timen. Lages høyst én gang per time og sted."""
    now = now or datetime.now(timezone.utc)
    time_noekkel = now.strftime("%Y-%m-%dT%H")
    treff = _cache.get(sted)
    if treff and treff[0] == time_noekkel:
        return treff[1]
    with _laas:
        laas = _laaser.setdefault(sted, threading.Lock())
    with laas:
        treff = _cache.get(sted)
        if treff and treff[0] == time_noekkel:
            return treff[1]
        if now.timestamp() - _feilet.get(sted, 0) < PAUSE_ETTER_FEIL:
            return treff[1] if treff else None
        try:
            tekster = (spor or _spor_claude)(lag_payload())
        except Exception as exc:  # nettverk, kvote, ugyldig svar
            logger.warning("AI-varsel for %s feilet: %s", sted, exc)
            tekster = None
        if not tekster:
            _feilet[sted] = now.timestamp()
            # Behold forrige times tekst heller enn ingenting.
            return treff[1] if treff else None
        resultat = {**tekster, "laget": now.isoformat(), "modell": MODELL}
        _cache[sted] = (time_noekkel, resultat)
        return resultat
