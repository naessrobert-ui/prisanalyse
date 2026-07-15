"""
bil_prisanalyse_routes.py - Analyseportal for prisutvikling per biltype
========================================================================

Leser daglige aggregat-parquet fra s3://prisanalyse-data/calc/bil/aggregat/
(en partisjon per dag, bygget av scripts/bygg_daglig_aggregat.py) og lar
brukeren filtrere paa Produsent, Modell, aarstall, drivstoff, hjuldrift,
Karosseri, variant og km-bin for aa se prisutvikling og volum over tid.

Endepunkter:
  GET /bil/prisanalyse                       - hovedside med filter og grafer
  GET /bil/prisanalyse/marked                - markedsoversikt (topplister)
  GET /bil/prisanalyse/sammenligning         - sammenlign to biltyper side om side
  GET /bil/prisanalyse/api/filteralternativer  - distinct verdier per kolonne
  GET /bil/prisanalyse/api/tidsserie?...     - aggregert tidsserie + endringer
  GET /bil/prisanalyse/api/eksport.csv?...   - last ned tidsserie som CSV
  GET /bil/prisanalyse/api/markedsbevegelser?dager=N
                                             - topp 20 prisfallere/-okere og
                                             volumeksplosjoner/-fall

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
from flask import Blueprint, Response, jsonify, render_template, request

import bil_nye_annonser
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


def _filter_etikett(args) -> str:
    """Bygg et kompakt navn paa det filtrerte settet for filnavn/label.
    Eks. 'Tesla_Model3_tesla-m3-lr' eller 'alle' hvis ingen filter."""
    biter = []
    for kol in ("Produsent", "Modell", "variant_id"):
        v = args.get(kol)
        if v and v != "alle":
            biter.append(str(v).replace(" ", "").replace("/", "-"))
    aar_fra = args.get("aarstall_fra")
    aar_til = args.get("aarstall_til")
    if aar_fra or aar_til:
        biter.append(f"{aar_fra or ''}-{aar_til or ''}")
    return "_".join(biter) if biter else "alle"


@bil_prisanalyse_bp.route("/api/eksport.csv")
def api_eksport_csv():
    """Returner CSV med daglig tidsserie for det filtrerte settet. Samme
    filter-params som /api/tidsserie. Inkluderer metadata-header med hvilket
    filter som ble brukt og hvor mange undergrupper som matchet."""
    df = _hent_cache()
    if df.empty:
        return Response("# Ingen aggregat tilgjengelig\n", mimetype="text/csv", status=503)

    delsett = _bruk_filter(df, request.args)
    if delsett.empty:
        return Response(
            "# Ingen rader matchet filteret\n",
            mimetype="text/csv",
            status=200,
        )

    serie = _bygg_tidsserie(delsett)
    if not serie["datoer"]:
        return Response("# Ingen tidsserie kunne bygges\n", mimetype="text/csv", status=200)

    # Bygg DataFrame for ren CSV-utskrift
    ut = pd.DataFrame({
        "dato": serie["datoer"],
        "median_pris": serie["median_pris"],
        "p25_pris": serie["p25_pris"],
        "p75_pris": serie["p75_pris"],
        "n_annonser": serie["n_annonser"],
    })

    # Header-kommentarer med hvilke filtre som ble brukt
    filter_tekst = []
    for k in FILTER_KOLONNER + ["aarstall_fra", "aarstall_til"]:
        v = request.args.get(k)
        if v and v != "alle":
            filter_tekst.append(f"{k}={v}")
    n_grupper = int(delsett[FILTER_KOLONNER + ["aarstall"]].drop_duplicates().shape[0])

    head = [
        f"# Bil-prisanalyse eksport",
        f"# Filter: {', '.join(filter_tekst) if filter_tekst else '(ingen)'}",
        f"# Undergrupper matchet: {n_grupper}",
        f"# Endring 30 dager: {serie.get('endring_30d_pct')}",
        f"# Endring 90 dager: {serie.get('endring_90d_pct')}",
    ]
    csv_body = ut.to_csv(index=False, sep=";", decimal=",")
    body = "\n".join(head) + "\n" + csv_body

    etikett = _filter_etikett(request.args)
    fn = f"prisanalyse_{etikett}_{datetime.utcnow():%Y%m%d}.csv"
    return Response(
        body,
        mimetype="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{fn}"'},
    )


# ---- Markedsbevegelser (topplister) ----

# Finere gruppenoekler enn bare (Produsent, Modell, variant_id) - inkluderer
# aarstall og hjuldrift slik at vi sammenligner like biler mellom de to
# datoene. Tidligere ble 2018-Kona og 2024-Kona slaatt sammen, hvilket
# skapte "merkelige verdier" naar miksen av aarganger endret seg over tid
# (Simpsons paradoks).
MARKED_GRUPPENOKLER = ["Produsent", "Modell", "variant_id", "aarstall", "hjuldrift"]
MARKED_MIN_VOLUM_DEFAULT = 10  # default min antall annonser i hver av de to dagene
MARKED_MIN_VOLUM_MIN = 3       # nedre grense for justerbar min_volum
MARKED_MIN_VOLUM_MAX = 200     # ovre grense
MARKED_TOPP_N = 20


def _vektet_median_per_gruppe_dag(df: pd.DataFrame) -> pd.DataFrame:
    """Reduser aggregat til en rad per (Produsent, Modell, variant_id, dato)
    med vektet median (vekt=n_annonser) og samlet n_annonser. Bruker pre-
    beregnet vekting saa sortering paa endring blir konsistent."""
    if df.empty:
        return df

    g = df.groupby(MARKED_GRUPPENOKLER + ["dato"], dropna=False, observed=True)

    sum_n = g["n_annonser"].sum()
    sum_vektet = (df["median_pris"].astype(float) * df["n_annonser"]).groupby(
        [df[k] for k in MARKED_GRUPPENOKLER] + [df["dato"]]
    ).sum()

    ut = pd.DataFrame({
        "n": sum_n,
        "vektet_median": sum_vektet / sum_n.replace(0, np.nan),
    }).reset_index()
    return ut


def _sparkline_for_gruppe(per_dag: pd.DataFrame, gruppe: dict[str, Any],
                          fra: Any, til: Any) -> list[dict]:
    """Returner [{"dato": "...", "median": int, "n": int}, ...] for en
    spesifikk gruppe i intervallet. `gruppe` skal ha samtlige
    MARKED_GRUPPENOKLER-verdier."""
    mask = (per_dag["dato"] >= fra) & (per_dag["dato"] <= til)
    for kol in MARKED_GRUPPENOKLER:
        mask = mask & (per_dag[kol] == gruppe[kol])
    s = per_dag.loc[mask].sort_values("dato")
    if s.empty:
        return []
    return [
        {
            "dato": d.isoformat() if hasattr(d, "isoformat") else str(d),
            "median": int(round(m)) if pd.notna(m) else None,
            "n": int(n),
        }
        for d, m, n in zip(s["dato"], s["vektet_median"], s["n"])
    ]


def _bygg_markedsbevegelser(
    df: pd.DataFrame,
    dager: int,
    min_volum: int = MARKED_MIN_VOLUM_DEFAULT,
) -> dict[str, Any]:
    """Returner fire topplister: prisfallere, prisoekere, volumeksplosjoner,
    volumfall - basert paa endring mellom siste dato og naermeste dato dager
    tilbake. `min_volum` er minimum antall annonser i baade siste og ref-dato
    for at en gruppe skal vaere med - hoyere verdi gir mindre stoey men
    faerre biltyper i listene."""
    tomt = {
        "dager": dager,
        "min_volum": min_volum,
        "siste_dato": None,
        "ref_dato": None,
        "antall_grupper_med_data": 0,
        "prisfallere": [],
        "prisokere": [],
        "volumeksplosjoner": [],
        "volumfall": [],
    }
    if df.empty or "dato" not in df.columns:
        return tomt

    per_dag = _vektet_median_per_gruppe_dag(df)
    if per_dag.empty:
        return tomt

    datoer = sorted(per_dag["dato"].unique())
    if not datoer:
        return tomt

    siste = datoer[-1]
    maal = siste - timedelta(days=dager)
    # Naermeste tilgjengelige ref-dato (<= maal); fall til foerste hvis ingen
    eldre = [d for d in datoer if d <= maal]
    if not eldre:
        return tomt
    ref = eldre[-1]

    siste_df = per_dag[per_dag["dato"] == siste].copy()
    ref_df = per_dag[per_dag["dato"] == ref].copy()

    samlet = pd.merge(
        siste_df, ref_df,
        on=MARKED_GRUPPENOKLER,
        suffixes=("_siste", "_ref"),
        how="inner",
    )
    samlet = samlet[
        (samlet["n_siste"] >= min_volum)
        & (samlet["n_ref"] >= min_volum)
        & samlet["vektet_median_siste"].notna()
        & samlet["vektet_median_ref"].notna()
        & (samlet["vektet_median_ref"] > 0)
    ].copy()

    if samlet.empty:
        tomt["siste_dato"] = siste.isoformat()
        tomt["ref_dato"] = ref.isoformat()
        return tomt

    samlet["endring_pris_pct"] = (
        (samlet["vektet_median_siste"] - samlet["vektet_median_ref"])
        / samlet["vektet_median_ref"] * 100
    )
    samlet["endring_volum_pct"] = (
        (samlet["n_siste"] - samlet["n_ref"]) / samlet["n_ref"] * 100
    )

    def _rad_til_dict(row: pd.Series, sparkline: bool = True) -> dict[str, Any]:
        gruppe = {kol: row[kol] for kol in MARKED_GRUPPENOKLER}
        # Tom aarstall -> None (gjør JSON renere enn pd.NA / NaN)
        aar = row["aarstall"]
        aar_int = int(aar) if pd.notna(aar) else None
        d = {
            "produsent": str(row["Produsent"]),
            "modell": str(row["Modell"]),
            "variant_id": str(row["variant_id"]),
            "aarstall": aar_int,
            "hjuldrift": str(row["hjuldrift"]) if pd.notna(row["hjuldrift"]) else None,
            "median_siste": int(round(row["vektet_median_siste"])),
            "median_ref": int(round(row["vektet_median_ref"])),
            "endring_pris_pct": round(float(row["endring_pris_pct"]), 2),
            "n_siste": int(row["n_siste"]),
            "n_ref": int(row["n_ref"]),
            "endring_volum_pct": round(float(row["endring_volum_pct"]), 1),
        }
        if sparkline:
            d["sparkline"] = _sparkline_for_gruppe(per_dag, gruppe, ref, siste)
        return d

    prisfall = samlet.sort_values("endring_pris_pct", ascending=True).head(MARKED_TOPP_N)
    prisoke = samlet.sort_values("endring_pris_pct", ascending=False).head(MARKED_TOPP_N)
    voleks = samlet.sort_values("endring_volum_pct", ascending=False).head(MARKED_TOPP_N)
    volfall = samlet.sort_values("endring_volum_pct", ascending=True).head(MARKED_TOPP_N)

    return {
        "dager": dager,
        "min_volum": min_volum,
        "siste_dato": siste.isoformat(),
        "ref_dato": ref.isoformat(),
        "antall_grupper_med_data": int(len(samlet)),
        "prisfallere": [_rad_til_dict(r) for _, r in prisfall.iterrows()],
        "prisokere": [_rad_til_dict(r) for _, r in prisoke.iterrows()],
        "volumeksplosjoner": [_rad_til_dict(r) for _, r in voleks.iterrows()],
        "volumfall": [_rad_til_dict(r) for _, r in volfall.iterrows()],
    }


@bil_prisanalyse_bp.route("/api/markedsbevegelser")
def api_markedsbevegelser():
    df = _hent_cache()
    if df.empty:
        return jsonify({"feil": "Ingen aggregat tilgjengelig"}), 503
    try:
        dager = int(request.args.get("dager", "30"))
    except ValueError:
        dager = 30
    dager = max(7, min(dager, 365))
    try:
        min_volum = int(request.args.get("min_volum", str(MARKED_MIN_VOLUM_DEFAULT)))
    except ValueError:
        min_volum = MARKED_MIN_VOLUM_DEFAULT
    min_volum = max(MARKED_MIN_VOLUM_MIN, min(min_volum, MARKED_MIN_VOLUM_MAX))
    return jsonify(_bygg_markedsbevegelser(df, dager, min_volum))


@bil_prisanalyse_bp.route("/marked")
def vis_marked():
    df = _hent_cache()
    har_data = not df.empty
    return render_template(
        "bil_prisanalyse_marked.html",
        har_data=har_data,
    )


@bil_prisanalyse_bp.route("/sammenligning")
def vis_sammenligning():
    df = _hent_cache()
    har_data = not df.empty
    return render_template(
        "bil_prisanalyse_sammenligning.html",
        har_data=har_data,
    )


@bil_prisanalyse_bp.route("/api/refresh", methods=["POST"])
def api_refresh():
    """Tving relasting av cachen (admin-funksjon)."""
    with _CACHE_LOCK:
        _CACHE["df"] = None
        _CACHE["loaded_at"] = None
    with _NYE_CACHE_LOCK:
        _NYE_CACHE["data"] = None
        _NYE_CACHE["loaded_at"] = None
    _hent_cache()
    return jsonify({"ok": True, "loaded_at": _CACHE["loaded_at"].isoformat()})


# ---- Nye annonser per dag (privat vs. bedrift) ----

# Egen cache: dette datasettet bygges fra database_biler.parquet +
# bil_time.parquet og er uavhengig av pris-aggregatet over.
_NYE_CACHE: dict[str, Any] = {"data": None, "loaded_at": None, "maks_gap": None}
_NYE_CACHE_LOCK = threading.Lock()
NYE_MAKS_GAP_MIN = 1
NYE_MAKS_GAP_MAX = 60


def _hent_nye_annonser(maks_gap: int) -> dict[str, Any]:
    """Returner nye-annonser-datasettet fra cache, bygg paa nytt hvis stale
    eller hvis maks_gap er endret siden forrige bygg."""
    with _NYE_CACHE_LOCK:
        now = datetime.utcnow()
        loaded_at = _NYE_CACHE.get("loaded_at")
        if (
            _NYE_CACHE.get("data") is not None
            and loaded_at is not None
            and _NYE_CACHE.get("maks_gap") == maks_gap
            and (now - loaded_at).total_seconds() < CACHE_TTL_SEKUNDER
        ):
            return _NYE_CACHE["data"]

        print(f"[nye-annonser] Bygger datasett (maks_gap={maks_gap})...")
        data = bil_nye_annonser.bygg_nye_annonser(maks_gap=maks_gap)
        _NYE_CACHE["data"] = data
        _NYE_CACHE["loaded_at"] = now
        _NYE_CACHE["maks_gap"] = maks_gap
        print(
            f"[nye-annonser] Ferdig: {data['oppsummering']['antall_dager']} dager, "
            f"{data['oppsummering']['antall_estimerte_dager']} estimerte"
        )
        return data


@bil_prisanalyse_bp.route("/nye")
def vis_nye_annonser():
    return render_template("bil_prisanalyse_nye.html")


@bil_prisanalyse_bp.route("/api/nye-annonser")
def api_nye_annonser():
    try:
        maks_gap = int(request.args.get("maks_gap", str(bil_nye_annonser.STANDARD_MAKS_GAP)))
    except ValueError:
        maks_gap = bil_nye_annonser.STANDARD_MAKS_GAP
    maks_gap = max(NYE_MAKS_GAP_MIN, min(maks_gap, NYE_MAKS_GAP_MAX))
    try:
        data = _hent_nye_annonser(maks_gap)
    except Exception as e:
        print(f"[nye-annonser] FEIL under bygging: {e}")
        return jsonify({"feil": f"Klarte ikke bygge datasettet: {e}"}), 503
    return jsonify(data)


@bil_prisanalyse_bp.route("/api/nye-annonser.csv")
def api_nye_annonser_csv():
    try:
        maks_gap = int(request.args.get("maks_gap", str(bil_nye_annonser.STANDARD_MAKS_GAP)))
    except ValueError:
        maks_gap = bil_nye_annonser.STANDARD_MAKS_GAP
    maks_gap = max(NYE_MAKS_GAP_MIN, min(maks_gap, NYE_MAKS_GAP_MAX))
    try:
        data = _hent_nye_annonser(maks_gap)
    except Exception as e:
        return Response(f"# Klarte ikke bygge datasettet: {e}\n", mimetype="text/csv", status=503)

    serie = data.get("serie", [])
    if not serie:
        return Response("# Ingen data\n", mimetype="text/csv", status=200)

    ut = pd.DataFrame(serie)[
        ["dato", "privat", "bedrift", "ukjent", "total", "estimert", "usikker", "gap_lengde"]
    ]
    opp = data.get("oppsummering", {})
    head = [
        "# Nye bil-annonser per dag (privat vs. bedrift)",
        f"# Maks gap for estimering: {maks_gap} dager",
        f"# Dager totalt: {opp.get('antall_dager')}  (estimerte: {opp.get('antall_estimerte_dager')})",
        f"# Andel privat: {opp.get('andel_privat_pct')} %",
        "# estimert=True: dagen ligger i et innsamlingshull og er jevnt fordelt",
    ]
    body = "\n".join(head) + "\n" + ut.to_csv(index=False, sep=";", decimal=",")
    fn = f"nye_annonser_{datetime.utcnow():%Y%m%d}.csv"
    return Response(
        body,
        mimetype="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{fn}"'},
    )


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
