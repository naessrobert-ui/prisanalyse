"""
bilradar_scorer.py – Felles scoring-logikk for BilRadar
========================================================
Brukes av:
  - bil_routes.py (Flask, live scoring av siste døgn)
  - generer_bilradar.py (batch, scoring av alle biler)

Leser FlipModels-pickle (markedspris + hurtigpris) bygget av
scripts/tren_prismodell.py, og skriver kolonnene:

  forventet_pris  – median markedspris fra peer-gruppen
  hurtigpris      – pris for salg innen 3 dager (lavbound)
  peer_konfidens  – antall salg modellen for dette segmentet ble trent på
  modell_nivaa    – "L1" / "L2" / "GEN" / "Ingen modell"

Modellen lastes fra disk eller S3 én gang og caches i minnet. All
prediksjon kjøres batch per segment for fart.
"""

import io
import os
import pickle
import threading
from datetime import datetime

import numpy as np
import pandas as pd

from bilradar_modell_skjema import (
    FlipModels,
    GEN_FEATURES_CAT,
    SEG_FEATURES_CAT,
    SEG_FEATURES_NUM,
    SegmentModel,
)
from bilradar_lookup import apply_lookup, last_lookup
from bilradar_overrides import apply_overrides, last_overrides

GOOD_DEAL_THRESHOLD = 10
INNBYTTE_RABATT = 0.15  # innbyttepris = forventet_pris * (1 - INNBYTTE_RABATT)
OVERRIDES_LOCAL_PATH = os.path.join(os.path.dirname(__file__), "data", "pris_overstyring.csv")
LOOKUP_LOCAL_PATH = os.path.join(os.path.dirname(__file__), "data", "prislookup.csv")
MIN_PRIS_FLOOR = 1_000.0  # forhindre at expm1-prediksjon faller til null/negativt

# ---- Modell-cache (trådsikker, lastes én gang) ----
_MODEL_LOCK = threading.Lock()
_MODEL_CACHE = {
    "modeller": None,
    "loaded_at": None,
}


def _er_joblib(navn: str) -> bool:
    return navn.lower().endswith(".joblib")


def _last_pickle_fra_fil(path: str) -> FlipModels:
    """Last FlipModels fra disk. Velger joblib eller pickle basert på
    filendelse — joblib-fila kan være lz4-komprimert."""
    if _er_joblib(path):
        import joblib
        return joblib.load(path)
    with open(path, "rb") as f:
        return pickle.load(f)


def _last_pickle_fra_bytes(data: bytes, navn: str = "") -> FlipModels:
    """Last FlipModels fra bytes. navn brukes til formatdeteksjon
    (joblib hvis .joblib-endelse, ellers pickle)."""
    if _er_joblib(navn):
        import joblib
        return joblib.load(io.BytesIO(data))
    return pickle.loads(data)


def _logg_modell_info(modeller: FlipModels):
    n_m_l1 = len(modeller.market_l1)
    n_m_l2 = len(modeller.market_l2)
    har_m_gen = modeller.market_general is not None
    print(
        f"[BilRadar] FlipModels lastet | trent: {modeller.trained_at or '?'} | "
        f"market L1={n_m_l1} L2={n_m_l2} GEN={'Ja' if har_m_gen else 'Nei'} | "
        f"hurtigpris: lookup-tabell"
    )


def _bygg_drivstoff_normalisering(modeller: FlipModels) -> dict:
    """Inspiser segmentnøklene og lag mapping fra lavere-case-form
    ("el", "elektrisk", "elektrisitet") til den varianten modellen
    faktisk er trent på. Velger den formen som dekker flest treningsdata.

    Treningsskriptet normaliserer ikke "Elektrisk" vs "Elektrisitet" før
    segmentering, så avhengig av hva parquet-en inneholder kan modellen
    bare ha den ene varianten — eller en blanding der den ene dominerer.
    Live-scoring må bruke den dominerende formen for å treffe L1/L2.
    """
    forms_n_obs: dict = {}  # trent_form -> total n_obs (på tvers av L1/L2)

    def _add(seg_key: str, n_obs: int):
        parts = seg_key.split(" | ")
        if not parts:
            return
        d = parts[-1].strip()
        if not d or d.lower() == "ukjent":
            return
        if d.lower() in ("elektrisk", "elektrisitet"):
            forms_n_obs[d] = forms_n_obs.get(d, 0) + n_obs

    for k, sm in modeller.market_l1.items():
        _add(k, sm.n_obs)
    for k, sm in modeller.market_l2.items():
        _add(k, sm.n_obs)

    out: dict = {}
    if forms_n_obs:
        best = max(forms_n_obs.items(), key=lambda kv: kv[1])[0]
        out["el"] = best
        out["elektrisk"] = best
        out["elektrisitet"] = best
        print(
            f"[BilRadar] Drivstoff-normalisering: 'El' → {best!r} "
            f"(modeller per form: {forms_n_obs})"
        )
    return out


def _hent_drivstoff_normalisering(modeller: FlipModels) -> dict:
    """Cachet versjon av _bygg_drivstoff_normalisering — bygges én gang
    per FlipModels-objekt og lagres som attributt på modellen."""
    cached = getattr(modeller, "_drivstoff_norm_cache", None)
    if cached is not None:
        return cached
    norm = _bygg_drivstoff_normalisering(modeller)
    try:
        modeller._drivstoff_norm_cache = norm  # type: ignore[attr-defined]
    except Exception:
        pass
    return norm


# ---- Innlastingsfunksjoner ----

def last_modell_fra_s3(s3_client, bucket: str, key: str) -> FlipModels:
    """Last FlipModels-pickle fra S3. Cacher i minne."""
    with _MODEL_LOCK:
        if _MODEL_CACHE["modeller"] is not None:
            return _MODEL_CACHE["modeller"]

        print(f"[BilRadar] Laster prismodell fra s3://{bucket}/{key} ...")
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        modeller = _last_pickle_fra_bytes(obj["Body"].read(), navn=key)

        _logg_modell_info(modeller)
        _MODEL_CACHE["modeller"] = modeller
        _MODEL_CACHE["loaded_at"] = datetime.now()
        return modeller


def last_modell_lokal_eller_s3(local_path: str, s3_client=None, bucket: str = "", key: str = "") -> FlipModels:
    """
    Last FlipModels-pickle — prøver i denne rekkefølgen:
      1. Oppgitt local_path
      2. Nedlasting fra S3 til /tmp/  (cacher lokalt for neste restart)
    """
    with _MODEL_LOCK:
        if _MODEL_CACHE["modeller"] is not None:
            return _MODEL_CACHE["modeller"]

        if local_path and os.path.exists(local_path):
            print(f"[BilRadar] Laster fra: {local_path}")
            modeller = _last_pickle_fra_fil(local_path)
        elif s3_client and bucket and key:
            print(f"[BilRadar] Laster fra S3: s3://{bucket}/{key}")
            obj = s3_client.get_object(Bucket=bucket, Key=key)
            data = obj["Body"].read()

            cache_path = os.path.join("/tmp", os.path.basename(key))
            try:
                with open(cache_path, "wb") as f:
                    f.write(data)
                print(f"[BilRadar] Cachet til disk: {cache_path}")
            except Exception:
                pass

            modeller = _last_pickle_fra_bytes(data, navn=key)
        else:
            raise FileNotFoundError(
                f"Prismodell ikke funnet: lokal={local_path}, S3={bucket}/{key}"
            )

        _logg_modell_info(modeller)
        _MODEL_CACHE["modeller"] = modeller
        _MODEL_CACHE["loaded_at"] = datetime.now()
        return modeller


def reload_modell():
    """Tvinger re-lasting av modell ved neste kall."""
    with _MODEL_LOCK:
        _MODEL_CACHE["modeller"] = None
        _MODEL_CACHE["loaded_at"] = None


# ---- Forprosessering ----

_RANGE_CACHE_RE = None


def _parse_rekkevidde(val) -> float:
    """Plukker første tall fra strings som '350 km', 'ca 420', 'N/A'. Returnerer 0 ved tom."""
    if pd.isna(val):
        return 0.0
    s = str(val).lower().replace(",", ".")
    digits = "".join(ch if (ch.isdigit() or ch == ".") else " " for ch in s).split()
    if not digits:
        return 0.0
    try:
        return float(digits[0])
    except Exception:
        return 0.0


def _normaliser_for_scoring(df: pd.DataFrame) -> pd.DataFrame:
    """Standardiser kolonnenavn og fyll inn features modellen forventer.
    Returnerer en kopi med alle nødvendige kolonner."""
    df = df.copy()

    col_map = {
        "årstall": "Årstall", "kjørelengde": "Kjørelengde", "girkasse": "Girkasse",
        "drivstoff": "Drivstoff", "selger": "Selger", "sted": "Sted",
        "hjuldrift": "Hjuldrift", "garanti": "Garanti", "forhandler": "Forhandler",
        "Garanti (mnd)": "Garanti", "Forhandler type": "Forhandler",
        "Service oppgitt": "Service", "Info": "Info",
        "Karosseri": "Karosseri", "fylke": "Fylke",
    }
    for old, new in col_map.items():
        if old in df.columns and new not in df.columns:
            df = df.rename(columns={old: new})

    if "FinnKode" in df.columns:
        df = df.drop_duplicates(subset="FinnKode", keep="last")

    if "Pris" in df.columns:
        df = df[~df["Pris"].astype(str).str.lower().str.contains("solgt", na=False)]

    df["Produsent"] = (
        df["Merke"].fillna("Ukjent").astype(str).str.strip()
        if "Merke" in df.columns else "Ukjent"
    )
    df["Modell"] = (
        df["Modell"].fillna("Ukjent").astype(str).str.strip()
        if "Modell" in df.columns else "Ukjent"
    )
    df["drivstoff"] = (
        df["Drivstoff"].fillna("Ukjent").astype(str).str.strip()
        if "Drivstoff" in df.columns else "Ukjent"
    )
    df["hjuldrift"] = (
        df["Hjuldrift"].fillna("Ukjent").astype(str).str.strip()
        if "Hjuldrift" in df.columns else "Ukjent"
    )
    df["girkasse"] = (
        df["Girkasse"].fillna("Ukjent").astype(str).str.strip()
        if "Girkasse" in df.columns else "Ukjent"
    )
    df["Karosseri"] = (
        df["Karosseri"].fillna("Ukjent").astype(str).str.strip()
        if "Karosseri" in df.columns else "Ukjent"
    )
    df["forhandler_type"] = (
        df["Forhandler"].fillna("Ukjent").astype(str).str.strip()
        if "Forhandler" in df.columns else "Ukjent"
    )
    df["fylke"] = (
        df["Fylke"].fillna("Ukjent").astype(str).str.strip()
        if "Fylke" in df.columns else "Ukjent"
    )

    df["kjørelengde"] = pd.to_numeric(df.get("Kjørelengde", 0), errors="coerce").fillna(0)
    df["årstall"] = pd.to_numeric(df.get("Årstall", 0), errors="coerce").fillna(0)
    df["alder"] = (datetime.now().year - df["årstall"]).clip(lower=0)
    df["km_per_aar"] = df["kjørelengde"] / (df["alder"] + 1)
    df["salgspris"] = pd.to_numeric(df.get("Pris", 0), errors="coerce").fillna(0)

    if "Garanti" in df.columns:
        df["garanti_mnd"] = pd.to_numeric(df["Garanti"], errors="coerce").fillna(0).clip(lower=0)
    else:
        df["garanti_mnd"] = 0.0

    if "rekkevidde_str" in df.columns:
        df["rekkevidde_km"] = df["rekkevidde_str"].apply(_parse_rekkevidde)
    elif "rekkevidde_km" in df.columns:
        df["rekkevidde_km"] = pd.to_numeric(df["rekkevidde_km"], errors="coerce").fillna(0)
    else:
        df["rekkevidde_km"] = 0.0

    df = df[df["salgspris"] > 0]

    df["segment_1"] = df["Produsent"] + " | " + df["Modell"] + " | " + df["drivstoff"]
    df["segment_2"] = df["Produsent"] + " | " + df["drivstoff"]

    return df


# ---- Vektorisert prediksjon ----

def _bygg_X(sub: pd.DataFrame, is_general: bool) -> pd.DataFrame:
    """Lag input-DataFrame til en sklearn Pipeline. Pipelinen skal ha
    ColumnTransformer som plukker ut de riktige kolonnene selv."""
    cat_cols = GEN_FEATURES_CAT if is_general else SEG_FEATURES_CAT
    cols = SEG_FEATURES_NUM + cat_cols
    X = sub.reindex(columns=cols).copy()
    for c in SEG_FEATURES_NUM:
        X[c] = pd.to_numeric(X[c], errors="coerce").fillna(0.0)
    for c in cat_cols:
        X[c] = X[c].fillna("Ukjent").astype(str)
    return X


def _predict_segment_batch(
    df: pd.DataFrame,
    seg_models: dict,
    segment_col: str,
    only_mask: pd.Series,
    is_general: bool,
    target_pris: str,
    target_n: str,
):
    """Skriv prediksjoner inn i df[target_pris] og df[target_n] for alle
    rader i `only_mask` der segment_col matcher en nøkkel i seg_models.
    Vektorisert per segment."""
    for seg_key, seg_model in seg_models.items():
        mask = only_mask & (df[segment_col] == seg_key)
        if not mask.any():
            continue
        sub = df.loc[mask]
        X = _bygg_X(sub, is_general=is_general)
        log_pred = seg_model.model.predict(X)
        pris = np.maximum(np.expm1(log_pred), MIN_PRIS_FLOOR)
        df.loc[mask, target_pris] = pris
        df.loc[mask, target_n] = seg_model.n_obs


def _predict_general(
    df: pd.DataFrame,
    seg_model: SegmentModel | None,
    only_mask: pd.Series,
    target_pris: str,
    target_n: str,
):
    if seg_model is None:
        return
    mask = only_mask & df[target_pris].isna()
    if not mask.any():
        return
    sub = df.loc[mask]
    X = _bygg_X(sub, is_general=True)
    log_pred = seg_model.model.predict(X)
    pris = np.maximum(np.expm1(log_pred), MIN_PRIS_FLOOR)
    df.loc[mask, target_pris] = pris
    df.loc[mask, target_n] = seg_model.n_obs


def _scor_pris_kategori(
    df: pd.DataFrame,
    l1: dict,
    l2: dict,
    general: SegmentModel | None,
    target_pris: str,
    target_n: str,
    target_nivaa: str | None = None,
):
    """Fyll target_pris/target_n for alle rader. Bruk L1 først, så L2, så generell."""
    df[target_pris] = np.nan
    df[target_n] = pd.NA
    if target_nivaa:
        df[target_nivaa] = "Ingen modell"

    # L1
    mask_open = df[target_pris].isna()
    _predict_segment_batch(df, l1, "segment_1", mask_open, False, target_pris, target_n)
    if target_nivaa:
        df.loc[df[target_pris].notna() & (df[target_nivaa] == "Ingen modell"), target_nivaa] = "L1"

    # L2 (kun rader L1 ikke traff)
    mask_open = df[target_pris].isna()
    _predict_segment_batch(df, l2, "segment_2", mask_open, False, target_pris, target_n)
    if target_nivaa:
        df.loc[df[target_pris].notna() & (df[target_nivaa] == "Ingen modell"), target_nivaa] = "L2"

    # Generell
    mask_open = df[target_pris].isna()
    _predict_general(df, general, mask_open, target_pris, target_n)
    if target_nivaa:
        df.loc[df[target_pris].notna() & (df[target_nivaa] == "Ingen modell"), target_nivaa] = "GEN"


# ---- Hovedinngang: scor en DataFrame ----

def scorer_biler(df: pd.DataFrame, modeller: FlipModels | None = None, threshold: int = GOOD_DEAL_THRESHOLD) -> pd.DataFrame:
    """Scorer alle biler i en DataFrame med markedspris + hurtigpris.

    Prioritet på forventet_pris: lookup/variant-tabellen (primær) → peer-WLS
    koeffisient-tabell (lett fallback) → ML-modellen `modeller` (valgfri).
    Send modeller=None for å score uten den tunge ML-modellen (brukes av
    /finn-sok) — da dekkes den lange halen av peer-WLS i stedet.
    Hurtigpris/innbyttepris kommer fra lookup-tabellen.

    Returnerer df utvidet med:
      forventet_pris, hurtigpris, innbyttepris, peer_konfidens,
      modell_nivaa, rabatt_kr, rabatt_pct, forventet_pris_raa, overstyrt
    """
    df = _normaliser_for_scoring(df)

    if df.empty:
        return df

    if modeller is not None:
        # Normaliser drivstoff til den varianten modellen er trent på, slik at
        # segmentnøkler som "Mazda | MX-30 | Elektrisitet" matcher selv om
        # treningsdata bruker "Elektrisk" (eller omvendt). Bygg segmentene på
        # nytt etter normalisering.
        drivstoff_norm = _hent_drivstoff_normalisering(modeller)
        if drivstoff_norm:
            df["drivstoff"] = df["drivstoff"].apply(
                lambda d: drivstoff_norm.get(str(d).strip().lower(), d)
            )
            df["segment_1"] = df["Produsent"] + " | " + df["Modell"] + " | " + df["drivstoff"]
            df["segment_2"] = df["Produsent"] + " | " + df["drivstoff"]

        # Markedspris (ML)
        _scor_pris_kategori(
            df,
            modeller.market_l1,
            modeller.market_l2,
            modeller.market_general,
            target_pris="forventet_pris",
            target_n="peer_konfidens",
            target_nivaa="modell_nivaa",
        )
    else:
        # Ingen ML-modell: init kolonnene, la lookup + peer-WLS fylle dem.
        df["forventet_pris"] = np.nan
        df["peer_konfidens"] = pd.NA
        df["modell_nivaa"] = "Ingen modell"

    # Hurtigpris settes av lookup-tabellen nedenfor. Init som tom slik at
    # biler uten lookup-treff rett og slett ikke får hurtigpris.
    df["hurtigpris"] = np.nan

    # Lookup-priser (transparente median-salg per gruppe). Overskriver
    # ML-prediksjonen for biler som har match — testkjoringer paa Vestlandet
    # viste at lookup vinner paa både MAE og bias, og spesielt der ML faller
    # til GEN-fallback (nye/sjeldne modeller).
    lookup = last_lookup(local_path=LOOKUP_LOCAL_PATH)
    df = apply_lookup(df, lookup)

    # Peer-WLS fallback for biler som fortsatt mangler forventet_pris (dekker
    # den lange halen når ML-modellen ikke er lastet, f.eks. på /finn-sok).
    try:
        from bilradar_peer import apply_peer
        df = apply_peer(df)
    except Exception as e:  # pragma: no cover - defensiv
        print(f"[BilRadar] Peer-fallback hoppet over ({e})")

    # Manuelle overstyringer på markedspris (hurtigpris berøres ikke)
    overrides = last_overrides(local_path=OVERRIDES_LOCAL_PATH)
    df = apply_overrides(df, overrides)

    mask = df["forventet_pris"].notna() & (df["forventet_pris"] > 0)
    df.loc[mask, "rabatt_kr"] = df.loc[mask, "forventet_pris"] - df.loc[mask, "salgspris"]
    df.loc[mask, "rabatt_pct"] = (df.loc[mask, "rabatt_kr"] / df.loc[mask, "forventet_pris"]) * 100

    # Innbyttepris: 15 % under (den evt. overstyrte) forventede prisen, men
    # aldri over hurtigprisen. Regnes for alle rader med et prisanslag, slik
    # at både lookup- og ML-fallback-biler får et innbytteanslag.
    if "hurtigpris" not in df.columns:
        df["hurtigpris"] = np.nan
    innbytte = df["forventet_pris"] * (1 - INNBYTTE_RABATT)
    innbytte = pd.concat([innbytte, df["hurtigpris"]], axis=1).min(axis=1)
    df["innbyttepris"] = innbytte.where(mask).clip(lower=MIN_PRIS_FLOOR)

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
            "hp": round(row["hurtigpris"]) if pd.notna(row.get("hurtigpris")) else 0,
            "ib": round(row["innbyttepris"]) if pd.notna(row.get("innbyttepris")) else 0,
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
