# -*- coding: utf-8 -*-
from datetime import datetime, timedelta, timezone

from scripts import ai_varsel


def setup_function(_):
    ai_varsel._cache.clear()
    ai_varsel._feilet.clear()


def test_tolk_svar_finner_json_og_fjerner_tankestrek():
    tekst = 'Her er det: {"kort": "Tørt til 15 – så regn.", "overblikk": "Trolig fint i helgen."}'
    svar = ai_varsel.tolk_svar(tekst)
    assert svar == {"kort": "Tørt til 15, så regn.", "overblikk": "Trolig fint i helgen."}
    assert ai_varsel.tolk_svar("ikke json") is None


def test_hent_kaller_claude_en_gang_per_time():
    kall = []
    spor = lambda payload: kall.append(payload) or {"kort": "k", "overblikk": "o"}
    now = datetime(2026, 11, 1, 20, 5, tzinfo=timezone.utc)
    a = ai_varsel.hent("bergen", lambda: {"x": 1}, now=now, spor=spor)
    b = ai_varsel.hent("bergen", lambda: {"x": 2}, now=now + timedelta(minutes=40), spor=spor)
    c = ai_varsel.hent("bergen", lambda: {"x": 3}, now=now + timedelta(hours=1), spor=spor)
    assert a["kort"] == "k" and b is a and c is not a
    assert len(kall) == 2


def test_feil_gir_forrige_tekst_og_pause():
    now = datetime(2026, 11, 1, 20, 5, tzinfo=timezone.utc)
    ok = ai_varsel.hent("bergen", dict, now=now, spor=lambda p: {"kort": "gammel", "overblikk": ""})
    kall = []

    def feiler(_):
        kall.append(1)
        raise RuntimeError("nede")

    senere = now + timedelta(hours=1)
    assert ai_varsel.hent("bergen", dict, now=senere, spor=feiler) is ok
    assert ai_varsel.hent("bergen", dict, now=senere + timedelta(minutes=2), spor=feiler) is ok
    assert len(kall) == 1  # pause etter feil


def test_grunnlag_er_kompakt_json():
    tekst = ai_varsel.grunnlag({"place": {"name": "Bergen"}, "nowcast": {"steps": list(range(24))},
                                "detail_hours": ["tungt"] * 1000})
    assert '"sted":"Bergen"' in tekst and "tungt" not in tekst
