# -*- coding: utf-8 -*-
"""Web-side for flypriser ut fra Bergen.

Leser CSV-ene som scripts/flypriser_bergen.py produserer i data/ og
serverer en prismatrise (destinasjon × avreisemåned) samt prisutvikling
over tid (billigste tilgjengelige billett pr. henting).
"""

from __future__ import annotations

import csv
import threading
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from flask import Blueprint, jsonify, render_template

flypriser_bp = Blueprint("flypriser", __name__, url_prefix="/flypriser")

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
HISTORIKK_CSV = DATA_DIR / "flypriser_bergen.csv"
BESTE_CSV = DATA_DIR / "flypriser_bergen_beste.csv"

CACHE_SECONDS = 300
_cache: dict = {"loaded_at": 0.0, "payload": None}
_cache_lock = threading.Lock()


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _les_csv(sti: Path) -> list[dict]:
    if not sti.exists():
        return []
    with sti.open(encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _til_int(verdi) -> int | None:
    try:
        return int(round(float(verdi)))
    except (TypeError, ValueError):
        return None


def _bygg_payload() -> dict:
    beste = _les_csv(BESTE_CSV)
    historikk = _les_csv(HISTORIKK_CSV)

    if not beste:
        return {
            "ok": False,
            "generated_at": _utc_now_iso(),
            "error": "Ingen prisdata ennå. Kjør «python -m scripts.flypriser_bergen».",
            "destinasjoner": [],
            "maaneder": [],
            "matrise": [],
            "historikk": [],
        }

    # --- Prismatrise: destinasjon × avreisemåned (billigste tilbud) ---
    maaneder = sorted({r["avreise_maaned"] for r in beste})
    # billigste pris pr. (destinasjon, måned) med lenke
    pris: dict[str, dict[str, dict]] = defaultdict(dict)
    dest_navn: dict[str, str] = {}
    for r in beste:
        d = r["destinasjon_kode"]
        dest_navn[d] = r.get("destinasjon_navn") or d
        p = _til_int(r.get("pris"))
        if p is None:
            continue
        celle = {"pris": p, "lenke": r.get("lenke") or None, "avreise": r.get("avreise")}
        eksisterende = pris[d].get(r["avreise_maaned"])
        if eksisterende is None or p < eksisterende["pris"]:
            pris[d][r["avreise_maaned"]] = celle

    matrise = []
    for d in sorted(dest_navn, key=lambda x: dest_navn[x]):
        rad_priser = [p["pris"] for p in pris[d].values()]
        matrise.append({
            "kode": d,
            "navn": dest_navn[d],
            "celler": {m: pris[d].get(m) for m in maaneder},
            "min": min(rad_priser) if rad_priser else None,
        })

    # --- Prisutvikling: billigste tilgjengelige billett pr. henting, pr. destinasjon ---
    # (min pris på tvers av alle måneder for hver hentet_dato)
    per_dato: dict[str, dict[str, int]] = defaultdict(dict)
    for r in historikk:
        p = _til_int(r.get("pris"))
        if p is None:
            continue
        d = r["destinasjon_kode"]
        dato = r.get("hentet_dato")
        if not dato:
            continue
        naa = per_dato[d].get(dato)
        if naa is None or p < naa:
            per_dato[d][dato] = p

    historikk_serier = []
    for d in sorted(per_dato, key=lambda x: dest_navn.get(x, x)):
        punkter = sorted(per_dato[d].items())
        historikk_serier.append({
            "kode": d,
            "navn": dest_navn.get(d, d),
            "punkter": [{"dato": dato, "pris": p} for dato, p in punkter],
        })

    sist_hentet = max((r.get("hentet_dato", "") for r in beste), default="")
    return {
        "ok": True,
        "generated_at": _utc_now_iso(),
        "sist_hentet": sist_hentet,
        "valuta": (beste[0].get("valuta") or "nok").upper(),
        "maaneder": maaneder,
        "matrise": matrise,
        "historikk": historikk_serier,
        "antall_destinasjoner": len(matrise),
    }


def _payload(force: bool = False) -> dict:
    now = time.monotonic()
    with _cache_lock:
        if not force and _cache["payload"] and now - _cache["loaded_at"] < CACHE_SECONDS:
            return _cache["payload"]
        payload = _bygg_payload()
        _cache.update({"loaded_at": now, "payload": payload})
        return payload


@flypriser_bp.route("/")
def flypriser_side():
    return render_template("flypriser_bergen.html")


@flypriser_bp.route("/api/data")
def flypriser_data():
    return jsonify(_payload())
