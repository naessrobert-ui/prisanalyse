"""
bilradar_scorer.py — Felles scoring-logikk for BilRadar
========================================================
Brukes av:
  - bil_routes.py (Flask, live scoring av siste døgn)
  - generer_bilradar.py (batch, scoring av alle biler)

Laster prismodell fra S3 (pickle), scorer biler, genererer JSON for HTML.
"""

import io
import os
import pickle
import threading
from datetime import datetime

import numpy as np
import pandas as pd

GOOD_DEAL_THRESHOLD = 10

# ---- Modell-cache (trådsikker, lastes én gang) ----
_MODEL_LOCK = threading.Lock()
_MODEL_CACHE = {
    "modeller": None,
    "loaded_at": None,
}


def last_modell_fra_s3(s3_client, bucket: str, key: str):
    """Last prismodell fra S3. Cacher i minne — lastes kun én gang."""
    with _MODEL_LOCK:
        if _MODEL_CACHE["modeller"] is not None:
            return _MODEL_CACHE["modeller"]

        print(f"[BilRadar] Laster prismodell fra s3://{bucket}/{key} ...")
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        modeller = pickle.loads(obj["Body"].read())

        n1 = len(modeller.get("nivaa_1", {}))
        n2 = len(modeller.get("nivaa_2", {}))
        har_gen = modeller.get("generell") is not None
        print(f"[BilRadar] Modell lastet: Nivå 1: {n1} | Nivå 2: {n2} | Generell: {'Ja' if har_gen else 'Nei'}")

        _MODEL_CACHE["modeller"] = modeller
        _MODEL_CACHE["loaded_at"] = datetime.now()
        return modeller


def last_modell_lokal_eller_s3(local_path: str, s3_client=None, bucket: str = "", key: str = ""):
    """
    Last prismodell fra lokal fil først, fallback til S3.
    Cacher i minne — lastes kun én gang.
    """
    with _MODEL_LOCK:
        if _MODEL_CACHE["modeller"] is not None:
            return _MODEL_CACHE["modeller"]

        # Prøv lokal fil først (mye raskere)
        if local_path and os.path.exists(local_path):
            print(f"[BilRadar] Laster prismodell fra lokal fil: {local_path} ...")
            with open(local_path, "rb") as f:
                modeller = pickle.load(f)
        elif s3_client and bucket and key:
            print(f"[BilRadar] Lokal fil ikke funnet, laster fra s3://{bucket}/{key} ...")
            obj = s3_client.get_object(Bucket=bucket, Key=key)
            modeller = pickle.loads(obj["Body"].read())
        else:
            raise FileNotFoundError(
                f"Prismodell ikke funnet: lokal={local_path}, S3={bucket}/{key}"
            )

        n1 = len(modeller.get("nivaa_1", {}))
        n2 = len(modeller.get("nivaa_2", {}))
        har_gen = modeller.get("generell") is not None
        print(f"[BilRadar] Modell lastet: Nivå 1: {n1} | Nivå 2: {n2} | Generell: {'Ja' if har_gen else 'Nei'}")

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
        X = np.array([[bil["alder"], bil["kjørelengde"], hjul_enc]])
        return float(m["model"].predict(X)[0]), "Nivå 1"

    if seg2 in modeller.get("nivaa_2", {}):
        m = modeller["nivaa_2"][seg2]
        hjul_enc = m["hjuldrift_map"].get(bil.get("hjuldrift", "Ukjent"), -1)
        X = np.array([[bil["alder"], bil["kjørelengde"], hjul_enc]])
        return float(m["model"].predict(X)[0]), "Nivå 2"

    if modeller.get("generell") is not None:
        m = modeller["generell"]
        prod_enc = m["prod_map"].get(bil.get("Produsent", "Ukjent"), -1)
        modell_enc = m["modell_map"].get(bil.get("Modell", "Ukjent"), -1)
        driv_enc = m["driv_map"].get(bil.get("drivstoff", "Ukjent"), -1)
        hjul_enc = m["hjul_map"].get(bil.get("hjuldrift", "Ukjent"), -1)
        X = np.array([[bil["alder"], bil["kjørelengde"],
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

    # Prediker
    resultater = []
    for idx, row in df.iterrows():
        bil = row.to_dict()
        pris_pred, nivaa = prediker_pris(bil, modeller)
        resultater.append({"idx": idx, "forventet_pris": pris_pred, "modell_nivaa": nivaa})

    pred_df = pd.DataFrame(resultater).set_index("idx")
    df = df.join(pred_df)

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
