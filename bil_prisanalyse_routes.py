"""
bil_prisanalyse_routes.py - Analyseportal for prisutvikling per biltype
========================================================================

Leser daglige aggregat-parquet fra s3://prisanalyse-data/calc/bil/aggregat/
(en partisjon per dag, bygget av scripts/bygg_daglig_aggregat.py) og lar
brukeren filtrere paa Produsent, Modell, aarstall, drivstoff, hjuldrift,
Karosseri, variant og km-bin for aa se prisutvikling og volum over tid.

Endepunkter:
  GET /bil/prisanalyse                       - HTML-side
  GET /bil/prisanalyse/api/filteralternativer  - distinct verdier per kolonne
  GET /bil/prisanalyse/api/tidsserie?...     - aggregert tidsserie + endringer

Datasettet caches i minnet og refresheses hver time. Hele aggregatet er
typisk under 50 MB - lett aa holde i RAM paa en Render web-instans.
"""

from __future__ import annotations

import io
import threading
from datetime import datetime, timedelta
from typing import Any

import boto3
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from flask import Blueprint, jsonify, render_template, request

from config import AWS_KEY, AWS_REGION, AWS_SECRET, S3_BUCKET_NAME

bil_prisanalyse_bp = Blueprint(
    "bil_prisanalyse", __name__, url_prefix="/bil/prisanalyse"
)

AGGREGAT_PREFIX = "calc/bil/aggregat/"
CACHE_TTL_SEKUNDER = 3600  # 1 time

# Kolonner som brukes som filter (alle valgfrie unntatt at minst noen maa
# vaere satt for at responsen skal ha mening)
FILTER_KOLONNER = [
    "Produsent",
    "Modell",
    "drivstoff",
    "hjuldrift",
    "Karosseri",
    "variant_id",
    "km_bin",
]

_CACHE: dict[str, Any] = {
    "df": None,
    "loaded_at": None,
    "filteralternativer": None,
}
_CACHE_LOCK = threading.Lock()


def _s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SECRET,
        region_name=AWS_REGION,
    )


def _last_aggregat_fra_s3() -> pd.DataFrame:
    """Last alle dag-partisjoner i ett. Henter parallelt vil vaere raskere
    paa stoerre datasett, men er ikke noedvendig naar vi snakker MB."""
    s3 = _s3_client()
    rammer: list[pd.DataFrame] = []
    paginator = s3.get_paginator("list_objects_v2")
    for side in paginator.paginate(Bucket=S3_BUCKET_NAME, Prefix=AGGREGAT_PREFIX):
        for o in side.get("Contents", []):
            if not o["Key"].endswith(".parquet"):
                continue
            try:
                obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key=o["Key"])
                table = pq.read_table(pa.BufferReader(obj["Body"].read()))
                rammer.append(table.to_pandas())
            except Exception as e:
                print(f"[prisanalyse] Klarte ikke lese {o['Key']}: {e}")

    if not rammer:
        return pd.DataFrame()

    df = pd.concat(rammer, ignore_index=True)
    df["dato"] = pd.to_datetime(df["dato"]).dt.date
    return df


def _bygg_filteralternativer(df: pd.DataFrame) -> dict[str, list]:
    """Returner sortert liste av distinct verdier per filterkolonne, samt
    aarsintervall."""
    out: dict[str, list] = {}
    for kol in FILTER_KOLONNER:
        if kol not in df.columns:
            print(f"[prisanalyse] ADVARSEL: kolonne '{kol}' mangler i aggregat")
            out[kol] = []
            continue
        unike = df[kol].dropna().unique()
        vals = sorted(str(v) for v in unike if str(v).strip())
        out[kol] = vals
        print(
            f"[prisanalyse] '{kol}': {len(unike)} unike raa-verdier, "
            f"{len(vals)} etter filtrering, eksempler={vals[:5]}"
        )

    # aarstall som intervall - bruk percentil for aa unngaa outliers
    # (vi har sett 1951 i datasett fordi noen veteran-EV-er er registrert)
    if "aarstall" in df.columns:
        aar = pd.to_numeric(df["aarstall"], errors="coerce").dropna().astype(int)
        if len(aar) > 50:
            out["aarstall_min"] = int(aar.quantile(0.01))
            out["aarstall_maks"] = int(aar.quantile(0.99))
        elif not aar.empty:
            out["aarstall_min"] = int(aar.min())
            out["aarstall_maks"] = int(aar.max())
        else:
            out["aarstall_min"] = 2010
            out["aarstall_maks"] = datetime.now().year
    else:
        out["aarstall_min"] = 2010
        out["aarstall_maks"] = datetime.now().year

    # Modeller per produsent for kaskaderende dropdown
    if "Produsent" in df.columns and "Modell" in df.columns:
        modeller_per_produsent: dict[str, list[str]] = {}
        for prod, grp in df.groupby("Produsent")["Modell"]:
            modeller_per_produsent[str(prod)] = sorted(
                set(str(m) for m in grp.dropna().unique() if str(m).strip())
            )
        out["modeller_per_produsent"] = modeller_per_produsent

    # Varianter per (produsent, modell)
    if all(k in df.columns for k in ("Produsent", "Modell", "variant_id")):
        varianter: dict[str, list[str]] = {}
        for (prod, mod), grp in df.groupby(["Produsent", "Modell"])["variant_id"]:
            key = f"{prod}::{mod}"
            varianter[key] = sorted(
                set(str(v) for v in grp.dropna().unique() if str(v).strip())
            )
        out["varianter_per_modell"] = varianter

    return out


def _hent_cache() -> pd.DataFrame:
    """Returner DataFrame fra cache, last paa nytt hvis stale."""
    with _CACHE_LOCK:
        now = datetime.utcnow()
        loaded_at = _CACHE.get("loaded_at")
        if (
            _CACHE.get("df") is not None
            and loaded_at is not None
            and (now - loaded_at).total_seconds() < CACHE_TTL_SEKUNDER
        ):
            return _CACHE["df"]

        print("[prisanalyse] Laster aggregat fra S3...")
        df = _last_aggregat_fra_s3()
        _CACHE["df"] = df
        _CACHE["loaded_at"] = now
        _CACHE["filteralternativer"] = _bygg_filteralternativer(df)
        print(f"[prisanalyse] Lastet {len(df):,} aggregat-rader")
        return df


# ---- Filtrering og aggregering ----

def _bruk_filter(df: pd.DataFrame, args) -> pd.DataFrame:
    out = df
    for kol in FILTER_KOLONNER:
        verdi = args.get(kol)
        if verdi and verdi != "alle":
            out = out[out[kol] == verdi]

    aar_fra = args.get("aarstall_fra", type=int)
    aar_til = args.get("aarstall_til", type=int)
    if aar_fra is not None:
        out = out[out["aarstall"] >= aar_fra]
    if aar_til is not None:
        out = out[out["aarstall"] <= aar_til]

    return out


def _vektet_snitt(verdi: pd.Series, vekt: pd.Series) -> float:
    """Vektet snitt av verdi (typisk median_pris) med vekt (n_annonser).
    Brukes naar flere undergrupper matcher samme filter."""
    v = verdi.dropna()
    w = vekt.reindex(v.index).fillna(0)
    if w.sum() == 0:
        return float("nan")
    return float((v * w).sum() / w.sum())


def _bygg_tidsserie(delsett: pd.DataFrame) -> dict[str, Any]:
    if delsett.empty:
        return {
            "datoer": [],
            "median_pris": [],
            "p25_pris": [],
            "p75_pris": [],
            "n_annonser": [],
            "endring_30d_pct": None,
            "endring_90d_pct": None,
            "siste_median": None,
            "siste_n": None,
        }

    # For hver dato: vektet snitt av medianer (vekt = n_annonser), sum av
    # annonser, og vektede percentiler. Dette er en pragmatisk approksimasjon
    # naar flere undergrupper matcher samme filter; ved snevert filter (ett
    # spesifikt segment) blir det presist.
    rader: list[dict[str, Any]] = []
    for dato, g in delsett.groupby("dato", sort=True):
        n_sum = int(g["n_annonser"].sum())
        rader.append({
            "dato": dato.isoformat(),
            "median": round(_vektet_snitt(g["median_pris"], g["n_annonser"])),
            "p25": round(_vektet_snitt(g["p25_pris"], g["n_annonser"])),
            "p75": round(_vektet_snitt(g["p75_pris"], g["n_annonser"])),
            "n": n_sum,
        })

    ts = pd.DataFrame(rader).sort_values("dato")
    if ts.empty:
        return {"datoer": [], "median_pris": [], "p25_pris": [], "p75_pris": [], "n_annonser": []}

    siste = ts.iloc[-1]
    siste_dato = pd.to_datetime(siste["dato"]).date()

    def _endring(dager: int) -> float | None:
        maal = siste_dato - timedelta(days=dager)
        # Velg naermeste rad rundt maaldatoen (innenfor +/- 7 dager)
        tidlig = ts[pd.to_datetime(ts["dato"]).dt.date <= maal]
        if tidlig.empty:
            return None
        ref = tidlig.iloc[-1]
        if not ref["median"] or pd.isna(ref["median"]):
            return None
        return float((siste["median"] - ref["median"]) / ref["median"] * 100)

    return {
        "datoer": ts["dato"].tolist(),
        "median_pris": ts["median"].tolist(),
        "p25_pris": ts["p25"].tolist(),
        "p75_pris": ts["p75"].tolist(),
        "n_annonser": ts["n"].tolist(),
        "endring_30d_pct": _endring(30),
        "endring_90d_pct": _endring(90),
        "siste_median": int(siste["median"]) if pd.notna(siste["median"]) else None,
        "siste_n": int(siste["n"]),
        "siste_dato": siste["dato"],
    }


# ---- Endepunkter ----

@bil_prisanalyse_bp.route("/")
def vis_portal():
    df = _hent_cache()
    har_data = not df.empty
    return render_template(
        "bil_prisanalyse.html",
        har_data=har_data,
        antall_dager=int(df["dato"].nunique()) if har_data else 0,
        antall_rader=len(df),
    )


@bil_prisanalyse_bp.route("/api/filteralternativer")
def api_filteralternativer():
    _hent_cache()
    return jsonify(_CACHE.get("filteralternativer") or {})


@bil_prisanalyse_bp.route("/api/tidsserie")
def api_tidsserie():
    df = _hent_cache()
    if df.empty:
        return jsonify({"feil": "Ingen aggregat tilgjengelig - kjor scripts/bygg_daglig_aggregat.py forst"}), 503

    delsett = _bruk_filter(df, request.args)
    serie = _bygg_tidsserie(delsett)
    serie["antall_undergrupper_matchet"] = (
        int(delsett[FILTER_KOLONNER + ["aarstall"]].drop_duplicates().shape[0])
        if not delsett.empty else 0
    )
    return jsonify(serie)


@bil_prisanalyse_bp.route("/api/refresh", methods=["POST"])
def api_refresh():
    """Tving relasting av cachen (admin-funksjon)."""
    with _CACHE_LOCK:
        _CACHE["df"] = None
        _CACHE["loaded_at"] = None
    _hent_cache()
    return jsonify({"ok": True, "loaded_at": _CACHE["loaded_at"].isoformat()})


@bil_prisanalyse_bp.route("/api/debug")
def api_debug():
    """Diagnose-info om aggregatet og filterkolonnene. Brukes til feilsoking
    naar dropdowns ikke vises som forventet."""
    df = _hent_cache()
    info: dict[str, Any] = {
        "loaded_at": _CACHE["loaded_at"].isoformat() if _CACHE.get("loaded_at") else None,
        "antall_rader": int(len(df)),
        "antall_dager": int(df["dato"].nunique()) if not df.empty and "dato" in df.columns else 0,
        "kolonner_i_aggregat": list(df.columns) if not df.empty else [],
    }
    per_kol: dict[str, Any] = {}
    if not df.empty:
        for kol in FILTER_KOLONNER + ["aarstall"]:
            if kol not in df.columns:
                per_kol[kol] = "KOLONNE MANGLER"
                continue
            unike = df[kol].dropna().unique()
            per_kol[kol] = {
                "antall_unike": int(len(unike)),
                "dtype": str(df[kol].dtype),
                "eksempler": [str(v) for v in list(unike)[:10]],
                "antall_tomme_strenger": int((df[kol].astype(str).str.strip() == "").sum()),
            }
    info["per_filter_kol"] = per_kol
    return jsonify(info)
