"""
bilradar_scorer.py – Felles scoring-logikk for BilRadar
========================================================
Brukes av:
  - bil_routes.py (Flask, live scoring av siste døgn)
  - generer_bilradar.py (batch, scoring av alle biler)

Støtter både joblib (.joblib) og pickle (.pkl) — foretrekker joblib.
Laster prismodell fra lokal disk (med S3-fallback), cacher i minne.
"""

import io
import os
import pickle
import threading
from datetime import datetime

import numpy as np
import pandas as pd

from bilradar_overrides import apply_overrides, last_overrides

GOOD_DEAL_THRESHOLD = 10
MIN_KM_PER_YEAR_FOR_MODEL = 4_000
OVERRIDES_LOCAL_PATH = os.path.join(os.path.dirname(__file__), "data", "pris_overstyring.csv")


def _normaliser_kjorelengde_for_modell(alder, kjorelengde):
    """Hindrer urealistisk lav km fra å gi for høy predikert pris."""
    alder_arr = np.asarray(alder, dtype=float)
    km_arr = np.asarray(kjorelengde, dtype=float)

    min_km = np.maximum(alder_arr, 0) * MIN_KM_PER_YEAR_FOR_MODEL
    # Behold rapportert km for nye biler, men legg gulv for eldre kjøretøy.
    justert = np.where(alder_arr >= 10, np.maximum(km_arr, min_km), km_arr)
    justert = np.maximum(justert, 0)

    if np.ndim(justert) == 0:
        return float(justert)
    return justert

# ---- Modell-cache (trådsikker, lastes én gang) ----
_MODEL_LOCK = threading.Lock()
_MODEL_CACHE = {
    "modeller": None,
    "loaded_at": None,
}


def _last_fra_fil(path: str):
    """Last modell fra enten .joblib eller .pkl — velger format automatisk."""
    ext = os.path.splitext(path)[1].lower()
    if ext == ".joblib":
        try:
            import joblib
            return joblib.load(path)
        except ImportError:
            raise ImportError("joblib er ikke installert. Kjør: pip install joblib")
    else:
        with open(path, "rb") as f:
            return pickle.load(f)


def _last_fra_bytes(data: bytes, hint_ext: str = ".pkl"):
    """Last modell fra bytes-objekt (S3). Velger format basert på hint."""
    ext = hint_ext.lower()
    if ext == ".joblib":
        try:
            import joblib
            return joblib.load(io.BytesIO(data))
        except ImportError:
            raise ImportError("joblib er ikke installert. Kjør: pip install joblib")
    else:
        return pickle.loads(data)


def _logg_modell_info(modeller):
    n1 = len(modeller.get("nivaa_1", {}))
    n2 = len(modeller.get("nivaa_2", {}))
    har_gen = modeller.get("generell") is not None
    fmt = "joblib" if modeller.get("_format") == "joblib" else "pickle"
    print(f"[BilRadar] Modell lastet: Nivå 1: {n1} | Nivå 2: {n2} | Generell: {'Ja' if har_gen else 'Nei'}")


# ---- Innlastingsfunksjoner ----

def last_modell_fra_s3(s3_client, bucket: str, key: str):
    """Last prismodell fra S3. Støtter .joblib og .pkl. Cacher i minne."""
    with _MODEL_LOCK:
        if _MODEL_CACHE["modeller"] is not None:
            return _MODEL_CACHE["modeller"]

        print(f"[BilRadar] Laster prismodell fra s3://{bucket}/{key} ...")
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        data = obj["Body"].read()

        hint_ext = os.path.splitext(key)[1]
        modeller = _last_fra_bytes(data, hint_ext)

        _logg_modell_info(modeller)
        _MODEL_CACHE["modeller"] = modeller
        _MODEL_CACHE["loaded_at"] = datetime.now()
        return modeller


def last_modell_lokal_eller_s3(local_path: str, s3_client=None, bucket: str = "", key: str = ""):
    """
    Last prismodell — prøver i denne rekkefølgen:
      1. Oppgitt local_path  (.joblib eller .pkl)
      2. Samme sti men med .joblib-endelse (automatisk oppgradering)
      3. Nedlasting fra S3 til /tmp/  (cacher lokalt for neste restart)
    """
    with _MODEL_LOCK:
        if _MODEL_CACHE["modeller"] is not None:
            return _MODEL_CACHE["modeller"]

        # 1. Prøv oppgitt sti
        if local_path and os.path.exists(local_path):
            print(f"[BilRadar] Laster fra: {local_path}")
            modeller = _last_fra_fil(local_path)

        # 2. Prøv .joblib-variant av samme sti
        elif local_path:
            joblib_path = os.path.splitext(local_path)[0] + ".joblib"
            if os.path.exists(joblib_path):
                print(f"[BilRadar] Fant joblib-variant: {joblib_path}")
                modeller = _last_fra_fil(joblib_path)
            elif s3_client and bucket and key:
                # 3. Last fra S3 og cache lokalt
                print(f"[BilRadar] Laster fra S3: s3://{bucket}/{key}")
                obj = s3_client.get_object(Bucket=bucket, Key=key)
                data = obj["Body"].read()

                # Cache til /tmp/ for raskere neste oppstart
                cache_path = os.path.join("/tmp", os.path.basename(key))
                try:
                    with open(cache_path, "wb") as f:
                        f.write(data)
                    print(f"[BilRadar] Cachet til disk: {cache_path}")
                except Exception:
                    pass  # /tmp ikke tilgjengelig — fortsett uten disk-cache

                hint_ext = os.path.splitext(key)[1]
                modeller = _last_fra_bytes(data, hint_ext)
            else:
                raise FileNotFoundError(
                    f"Prismodell ikke funnet: lokal={local_path}, S3={bucket}/{key}"
                )
        else:
            raise FileNotFoundError("Ingen local_path oppgitt og ingen S3-konfigurasjon.")

        _logg_modell_info(modeller)
        _MODEL_CACHE["modeller"] = modeller
        _MODEL_CACHE["loaded_at"] = datetime.now()
        return modeller


def reload_modell():
    """Tvinger re-lasting av modell ved neste kall."""
    with _MODEL_LOCK:
        _MODEL_CACHE["modeller"] = None
        _MODEL_CACHE["loaded_at"] = None


# ---- Prediksjon ----

def prediker_pris(bil: dict, modeller: dict):
    """Prediker forventet pris for én bil. Returnerer (pris, nivå)."""
    seg1 = bil.get("segment_1", "")
    seg2 = bil.get("segment_2", "")

    if seg1 in modeller.get("nivaa_1", {}):
        m = modeller["nivaa_1"][seg1]
        hjul_enc = m["hjuldrift_map"].get(bil.get("hjuldrift", "Ukjent"), -1)
        model_km = _normaliser_kjorelengde_for_modell(bil["alder"], bil["kjørelengde"])
        X = np.array([[bil["alder"], model_km, hjul_enc]])
        return float(m["model"].predict(X)[0]), "Nivå 1"

    if seg2 in modeller.get("nivaa_2", {}):
        m = modeller["nivaa_2"][seg2]
        hjul_enc = m["hjuldrift_map"].get(bil.get("hjuldrift", "Ukjent"), -1)
        model_km = _normaliser_kjorelengde_for_modell(bil["alder"], bil["kjørelengde"])
        X = np.array([[bil["alder"], model_km, hjul_enc]])
        return float(m["model"].predict(X)[0]), "Nivå 2"

    if modeller.get("generell") is not None:
        m = modeller["generell"]
        prod_enc = m["prod_map"].get(bil.get("Produsent", "Ukjent"), -1)
        modell_enc = m["modell_map"].get(bil.get("Modell", "Ukjent"), -1)
        driv_enc = m["driv_map"].get(bil.get("drivstoff", "Ukjent"), -1)
        hjul_enc = m["hjul_map"].get(bil.get("hjuldrift", "Ukjent"), -1)
        model_km = _normaliser_kjorelengde_for_modell(bil["alder"], bil["kjørelengde"])
        X = np.array([[bil["alder"], model_km,
                        prod_enc, modell_enc, driv_enc, hjul_enc]])
        return float(m["model"].predict(X)[0]), "Generell"

    return None, "Ingen modell"


# ---- Scoring av DataFrame ----

def scorer_biler(df: pd.DataFrame, modeller: dict, threshold: int = GOOD_DEAL_THRESHOLD) -> pd.DataFrame:
    """Scorer alle biler i en DataFrame. Returnerer df med scoring-kolonner."""
    df = df.copy()

    # Standardiser kolonnenavn (tåler både daglig- og time-format)
    col_map = {
        "årstall": "Årstall", "kjørelengde": "Kjørelengde", "girkasse": "Girkasse",
        "drivstoff": "Drivstoff", "selger": "Selger", "sted": "Sted",
        "hjuldrift": "Hjuldrift", "garanti": "Garanti", "forhandler": "Forhandler",
        "Garanti (mnd)": "Garanti", "Forhandler type": "Forhandler",
        "Service oppgitt": "Service", "Info": "Info",
    }
    for old, new in col_map.items():
        if old in df.columns and new not in df.columns:
            df = df.rename(columns={old: new})

    # Fjern duplikater
    if "FinnKode" in df.columns:
        df = df.drop_duplicates(subset="FinnKode", keep="last")

    # Fjern solgte biler
    if "Pris" in df.columns:
        df = df[~df["Pris"].astype(str).str.lower().str.contains("solgt", na=False)]

    # Klargjør features
    df["Produsent"] = df["Merke"].fillna("Ukjent").astype(str).str.strip() if "Merke" in df.columns else "Ukjent"
    df["Modell"] = df["Modell"].fillna("Ukjent").astype(str).str.strip() if "Modell" in df.columns else "Ukjent"
    df["drivstoff"] = df["Drivstoff"].fillna("Ukjent").astype(str).str.strip() if "Drivstoff" in df.columns else "Ukjent"
    df["kjørelengde"] = pd.to_numeric(df.get("Kjørelengde", 0), errors="coerce").fillna(0)
    df["årstall"] = pd.to_numeric(df.get("Årstall", 0), errors="coerce").fillna(0)
    df["alder"] = datetime.now().year - df["årstall"]
    df["salgspris"] = pd.to_numeric(df.get("Pris", 0), errors="coerce").fillna(0)
    df["hjuldrift"] = df["Hjuldrift"].fillna("Ukjent").astype(str).str.strip() if "Hjuldrift" in df.columns else "Ukjent"

    # Fjern biler uten pris
    df = df[df["salgspris"] > 0]

    # Segmentnøkler
    df["segment_1"] = df["Produsent"] + " | " + df["Modell"] + " | " + df["drivstoff"]
    df["segment_2"] = df["Produsent"] + " | " + df["drivstoff"]

    # Prediker – batch per segment (50-100x raskere enn iterrows)
    df["forventet_pris"] = np.nan
    df["modell_nivaa"] = "Ingen modell"

    nivaa_1 = modeller.get("nivaa_1", {})
    nivaa_2 = modeller.get("nivaa_2", {})
    generell = modeller.get("generell")

    # Nivå 1: batch per (Produsent | Modell | drivstoff)
    for seg, m in nivaa_1.items():
        mask = df["segment_1"] == seg
        if not mask.any():
            continue
        sub = df.loc[mask]
        hjul_enc = sub["hjuldrift"].map(m["hjuldrift_map"]).fillna(-1).astype(int)
        model_km = _normaliser_kjorelengde_for_modell(sub["alder"].values, sub["kjørelengde"].values)
        X = np.column_stack([sub["alder"].values, model_km, hjul_enc.values])
        df.loc[mask, "forventet_pris"] = m["model"].predict(X)
        df.loc[mask, "modell_nivaa"] = "Nivå 1"

    # Nivå 2: batch per (Produsent | drivstoff) – kun de som ikke fikk nivå 1
    ingen_pred = df["forventet_pris"].isna()
    for seg, m in nivaa_2.items():
        mask = ingen_pred & (df["segment_2"] == seg)
        if not mask.any():
            continue
        sub = df.loc[mask]
        hjul_enc = sub["hjuldrift"].map(m["hjuldrift_map"]).fillna(-1).astype(int)
        model_km = _normaliser_kjorelengde_for_modell(sub["alder"].values, sub["kjørelengde"].values)
        X = np.column_stack([sub["alder"].values, model_km, hjul_enc.values])
        df.loc[mask, "forventet_pris"] = m["model"].predict(X)
        df.loc[mask, "modell_nivaa"] = "Nivå 2"

    # Generell: alle som fremdeles mangler prediksjon
    if generell is not None:
        ingen_pred = df["forventet_pris"].isna()
        if ingen_pred.any():
            m = generell
            sub = df.loc[ingen_pred]
            prod_enc   = sub["Produsent"].map(m["prod_map"]).fillna(-1).astype(int)
            modell_enc = sub["Modell"].map(m["modell_map"]).fillna(-1).astype(int)
            driv_enc   = sub["drivstoff"].map(m["driv_map"]).fillna(-1).astype(int)
            hjul_enc   = sub["hjuldrift"].map(m["hjul_map"]).fillna(-1).astype(int)
            X = np.column_stack([
                sub["alder"].values, _normaliser_kjorelengde_for_modell(sub["alder"].values, sub["kjørelengde"].values),
                prod_enc.values, modell_enc.values, driv_enc.values, hjul_enc.values
            ])
            df.loc[ingen_pred, "forventet_pris"] = m["model"].predict(X)
            df.loc[ingen_pred, "modell_nivaa"] = "Generell"

    overrides = last_overrides(local_path=OVERRIDES_LOCAL_PATH)
    df = apply_overrides(df, overrides)

    mask = df["forventet_pris"].notna() & (df["forventet_pris"] > 0)
    df.loc[mask, "rabatt_kr"] = df.loc[mask, "forventet_pris"] - df.loc[mask, "salgspris"]
    df.loc[mask, "rabatt_pct"] = (df.loc[mask, "rabatt_kr"] / df.loc[mask, "forventet_pris"]) * 100

    return df


# ---- JSON for HTML ----

def lag_json_data(df: pd.DataFrame) -> str:
    """Konverterer scoret DataFrame til kompakt JSON for HTML-template."""
    cars = []
    for _, row in df.iterrows():
        car = {
            "i": int(row["FinnKode"]) if pd.notna(row.get("FinnKode")) else 0,
            "b": str(row.get("Bilmerke", "")) if pd.notna(row.get("Bilmerke")) else "",
            "m": str(row.get("Merke", "")) if pd.notna(row.get("Merke")) else "",
            "mo": str(row.get("Modell", "")) if pd.notna(row.get("Modell")) else "",
            "nf": str(row.get("Info", "")) if pd.notna(row.get("Info")) else "",
            "a": int(row["årstall"]) if pd.notna(row.get("årstall")) and row["årstall"] > 0 else 0,
            "k": int(row["kjørelengde"]) if pd.notna(row.get("kjørelengde")) else 0,
            "g": str(row.get("Girkasse", "")) if pd.notna(row.get("Girkasse")) else "",
            "d": str(row.get("drivstoff", "")) if pd.notna(row.get("drivstoff")) else "",
            "hj": str(row.get("Hjuldrift", "")) if pd.notna(row.get("Hjuldrift")) else "",
            "ka": str(row.get("Karosseri", "")) if pd.notna(row.get("Karosseri")) else "",
            "p": int(row["salgspris"]) if pd.notna(row.get("salgspris")) else 0,
            "s": str(row.get("Selger", "")) if pd.notna(row.get("Selger")) else "",
            "st": str(row.get("Sted", "")) if pd.notna(row.get("Sted")) else "",
            "fy": str(row.get("Fylke", "")) if pd.notna(row.get("Fylke")) else "",
            "fh": str(row.get("Forhandler", "")) if pd.notna(row.get("Forhandler")) else "",
            "im": str(row.get("BildeURL", "")) if pd.notna(row.get("BildeURL")) else "",
            "ep": round(row["forventet_pris"]) if pd.notna(row.get("forventet_pris")) else 0,
            "r": round(row["rabatt_pct"], 1) if pd.notna(row.get("rabatt_pct")) else 0,
            "ml": str(row.get("modell_nivaa", "none")) if pd.notna(row.get("modell_nivaa")) else "none",
        }
        car = {k: v for k, v in car.items() if v != "" and v != 0 and v != "none"}
        if "p" not in car:
            car["p"] = 0
        if "r" not in car:
            car["r"] = 0
        cars.append(car)

    import json
    return json.dumps(cars, ensure_ascii=False, separators=(",", ":"))
