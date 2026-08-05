# -*- coding: utf-8 -*-
"""Web-sider for flypriser.

  /flypriser            – prismatrise ut fra Bergen (destinasjon × måned)
  /flypriser/norwegian  – investor-dashboard for Norwegian (DY):
                          prisindeks, avviksdeteksjon, konkurrent-gap, booking-kurve
"""

from __future__ import annotations

import csv
import threading
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from flask import Blueprint, jsonify, render_template

from scripts.flypriser_analyse import full_analyse
from scripts.norwegian_trafikk import analyse as trafikk_analyse

flypriser_bp = Blueprint("flypriser", __name__, url_prefix="/flypriser")

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
HISTORIKK_CSV = DATA_DIR / "flypriser_historikk.csv"
BESTE_CSV = DATA_DIR / "flypriser_beste.csv"

CACHE_SECONDS = 300
_cache: dict = {"bergen": (0.0, None), "norwegian": (0.0, None)}
_lock = threading.Lock()


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _les_csv(sti: Path) -> list[dict]:
    if not sti.exists():
        return []
    with sti.open(encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _int(v):
    try:
        return int(round(float(v)))
    except (TypeError, ValueError):
        return None


def _bygg_bergen() -> dict:
    """Prismatrise for reiser UT fra Bergen (origin=BGO)."""
    beste = [r for r in _les_csv(BESTE_CSV) if r.get("origin") == "BGO"]
    historikk = [r for r in _les_csv(HISTORIKK_CSV) if r.get("origin") == "BGO"]
    if not beste:
        return {"ok": False, "generated_at": _utc_now_iso(),
                "error": "Ingen prisdata ennå. Kjør «python -m scripts.flypriser_bergen».",
                "maaneder": [], "matrise": [], "historikk": []}

    maaneder = sorted({r["avreise_maaned"] for r in beste})
    pris: dict[str, dict[str, dict]] = defaultdict(dict)
    navn: dict[str, str] = {}
    for r in beste:
        d = r["destinasjon"]
        navn[d] = r.get("destinasjon_navn") or d
        p = _int(r.get("pris"))
        if p is None:
            continue
        celle = {"pris": p, "lenke": r.get("lenke") or None}
        if r["avreise_maaned"] not in pris[d] or p < pris[d][r["avreise_maaned"]]["pris"]:
            pris[d][r["avreise_maaned"]] = celle

    matrise = []
    for d in sorted(navn, key=lambda x: navn[x]):
        rp = [c["pris"] for c in pris[d].values()]
        matrise.append({"kode": d, "navn": navn[d],
                        "celler": {m: pris[d].get(m) for m in maaneder},
                        "min": min(rp) if rp else None})

    per_dato: dict[str, dict[str, int]] = defaultdict(dict)
    for r in historikk:
        p = _int(r.get("pris"))
        if p is None:
            continue
        d, dato = r["destinasjon"], r.get("hentet_dato")
        if not dato:
            continue
        if d not in per_dato[dato] or p < per_dato[dato][d]:
            per_dato[dato][d] = p
    serier = []
    for d in sorted(navn, key=lambda x: navn[x]):
        punkter = sorted((dato, per_dato[dato][d]) for dato in per_dato if d in per_dato[dato])
        if punkter:
            serier.append({"kode": d, "navn": navn[d],
                           "punkter": [{"dato": dt, "pris": p} for dt, p in punkter]})

    return {"ok": True, "generated_at": _utc_now_iso(),
            "sist_hentet": max((r.get("hentet_dato", "") for r in beste), default=""),
            "valuta": (beste[0].get("valuta") or "nok").upper(),
            "maaneder": maaneder, "matrise": matrise, "historikk": serier,
            "antall_destinasjoner": len(matrise)}


def _cached(key: str, bygg):
    now = time.monotonic()
    with _lock:
        ts, payload = _cache[key]
        if payload and now - ts < CACHE_SECONDS:
            return payload
        payload = bygg()
        _cache[key] = (now, payload)
        return payload


@flypriser_bp.route("/")
def flypriser_side():
    return render_template("flypriser_bergen.html")


@flypriser_bp.route("/api/data")
def flypriser_data():
    return jsonify(_cached("bergen", _bygg_bergen))


@flypriser_bp.route("/norwegian/")
def norwegian_side():
    return render_template("flypriser_norwegian.html")


@flypriser_bp.route("/norwegian/api")
def norwegian_data():
    payload = _cached("norwegian", lambda: {
        **full_analyse(), "trafikk": trafikk_analyse(), "generated_at": _utc_now_iso()})
    return jsonify(payload)
