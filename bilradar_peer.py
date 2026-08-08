"""
bilradar_peer.py – Peer-WLS koeffisient-tabell for live-fallback
================================================================
Speiler lookup-tabellen: en liten, forhaandsberegnet tabell som lar
scorer_biler prise biler UTEN lookup-treff uten aa laste den tunge
ML-modellen (~150 MB). Koeffisientene bygges av scripts/... nei — av
bil_kupp_analyse.py (samme peer-WLS som /radar) og lastes opp til S3.

Per peer-gruppe lagres en lineaer log-pris-modell:
    log(pris) = beta0 + beta1 * alder + beta2 * (km_norm / 100000)

To nivaaer (tier), matches i rekkefoelge:
    tier 1: (Produsent, Modell, drivstoff, hjuldrift)
    tier 2: (Produsent, Modell, drivstoff)   – hjuldrift ignoreres

CSV-kolonner:
    prod_key, modell_key, driv_key, hjul_key, tier, n_obs, beta0, beta1, beta2

Noeklene er normaliserte (lowercase/strip, hjuldrift -> Tohjul/Firehjul) slik
at live FINN-data matcher treningsdata (samme grep som i bilradar_lookup).
"""

import io
import os
import threading
from datetime import datetime

import numpy as np
import pandas as pd

from bilradar_lookup import _normaliser_hjuldrift

# Maa matche bil_kupp_analyse.km_normalisert / MIN_KM_PER_AAR.
MIN_KM_PER_AAR = 4_000

PEER_COLUMNS = [
    "prod_key", "modell_key", "driv_key", "hjul_key",
    "tier", "n_obs", "beta0", "beta1", "beta2",
]

PEER_LOCAL_PATH = os.path.join(os.path.dirname(__file__), "data", "peer_koeffisienter.csv")
PEER_S3_BUCKET = os.getenv("S3_BUCKET_NAME", "")
PEER_S3_KEY = os.getenv("BILRADAR_PEER_S3_KEY", "calc/bil/peer_koeffisienter.csv")
# TTL: 0 = last én gang; default 6t gir deploy-fri oppdatering (som lookup).
PEER_TTL_SECONDS = int(os.getenv("BILRADAR_PEER_TTL_SECONDS", "21600") or 0)

_PEER_LOCK = threading.Lock()
_PEER_CACHE = {"df": None, "loaded_at": None, "source": None}


def km_normalisert(alder, km) -> np.ndarray:
    """Hindrer urealistisk lav km paa eldre biler (kopi av bil_kupp_analyse-
    logikken saa live-prising matcher treningen)."""
    alder = np.asarray(alder, dtype=float)
    km = np.asarray(km, dtype=float)
    gulv = np.maximum(alder, 0) * MIN_KM_PER_AAR
    return np.where(alder >= 10, np.maximum(km, gulv), np.maximum(km, 0))


def _norm_key(series: pd.Series) -> pd.Series:
    return series.fillna("Ukjent").astype(str).str.strip().str.lower()


def _les_csv(buf_or_path) -> pd.DataFrame:
    df = pd.read_csv(buf_or_path)
    df.columns = [c.strip() for c in df.columns]
    for col in PEER_COLUMNS:
        if col not in df.columns:
            df[col] = np.nan
    for c in ["prod_key", "modell_key", "driv_key", "hjul_key"]:
        df[c] = df[c].fillna("").astype(str).str.strip()
    for c in ["tier", "n_obs", "beta0", "beta1", "beta2"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=["beta0", "beta1", "beta2", "tier"])
    return df.reset_index(drop=True)


def _maybe_s3_client(s3_client, bucket: str):
    if s3_client is not None:
        return s3_client
    if not bucket:
        return None
    try:
        import boto3
        return boto3.client("s3")
    except Exception:
        return None


def _les_fra_kilde(source: dict) -> pd.DataFrame:
    df = pd.DataFrame(columns=PEER_COLUMNS)
    client = _maybe_s3_client(source.get("s3_client"), source.get("bucket", ""))
    bucket = source.get("bucket", "")
    key = source.get("key", "")
    local_path = source.get("local_path", "")
    if client and bucket and key:
        try:
            obj = client.get_object(Bucket=bucket, Key=key)
            df = _les_csv(io.BytesIO(obj["Body"].read()))
            print(f"[BilRadar] Peer-koeff lastet ({len(df)} grupper) fra s3://{bucket}/{key}")
            return df
        except Exception as e:
            print(f"[BilRadar] Ingen peer-koeff fra S3 ({e}) – prøver lokal fil")
    if local_path and os.path.exists(local_path):
        try:
            df = _les_csv(local_path)
            print(f"[BilRadar] Peer-koeff lastet ({len(df)} grupper) fra {local_path}")
        except Exception as e:
            print(f"[BilRadar] Klarte ikke lese {local_path}: {e}")
    return df


def last_peer_koef(local_path: str = "", s3_client=None, bucket: str = "", key: str = "") -> pd.DataFrame:
    """Last peer-koeffisient-tabellen (S3 først, lokal fil som fallback), cachet
    med TTL. Kilden huskes slik at TTL-relasting fra scorer_biler treffer S3."""
    with _PEER_LOCK:
        prev = _PEER_CACHE["source"] or {}
        if not prev or s3_client is not None or bucket or key or local_path:
            _PEER_CACHE["source"] = {
                "local_path": local_path or prev.get("local_path", ""),
                "s3_client": s3_client if s3_client is not None else prev.get("s3_client"),
                "bucket": bucket or prev.get("bucket") or PEER_S3_BUCKET,
                "key": key or prev.get("key") or PEER_S3_KEY,
            }
        source = _PEER_CACHE["source"]

        cached = _PEER_CACHE["df"]
        loaded_at = _PEER_CACHE["loaded_at"]
        if cached is not None and loaded_at is not None:
            fersk = PEER_TTL_SECONDS <= 0 or (
                (datetime.now() - loaded_at).total_seconds() < PEER_TTL_SECONDS
            )
            if fersk:
                return cached

        df = _les_fra_kilde(source)
        _PEER_CACHE["df"] = df
        _PEER_CACHE["loaded_at"] = datetime.now()
        return df


def reload_peer_koef():
    with _PEER_LOCK:
        _PEER_CACHE["df"] = None
        _PEER_CACHE["loaded_at"] = None


def _predikter(rows: pd.DataFrame, koef: pd.DataFrame, on_keys: list) -> pd.Series:
    """Slaa opp koeffisienter for rows paa on_keys og returner predikert pris
    (indeksert som rows). NaN der ingen match."""
    venstre = rows.reset_index()  # bevar original indeks i kolonnen 'index'
    merged = venstre.merge(koef, on=on_keys, how="left")
    alder = pd.to_numeric(merged.get("alder"), errors="coerce").fillna(0).values
    km = pd.to_numeric(merged.get("kjørelengde"), errors="coerce").fillna(0).values
    km_norm = km_normalisert(alder, km)
    log_pred = merged["beta0"] + merged["beta1"] * alder + merged["beta2"] * (km_norm / 100_000.0)
    pris = np.exp(log_pred)
    pris = pd.Series(pris.values, index=merged["index"].values)
    # Ugyldige/urealistiske -> NaN
    pris = pris.where(np.isfinite(pris) & (pris > 1_000) & (pris < 50_000_000))
    return pris


def apply_peer(df: pd.DataFrame, koef: pd.DataFrame | None = None) -> pd.DataFrame:
    """Fyll forventet_pris for biler som fortsatt mangler den, ved hjelp av
    peer-WLS-koeffisientene. Tier 1 (m/hjuldrift) først, så tier 2. Setter
    modell_nivaa = 'PEER-T1'/'PEER-T2'. Idempotent: tom tabell = ingen endring."""
    if koef is None:
        koef = last_peer_koef(local_path=PEER_LOCAL_PATH)
    if koef is None or koef.empty:
        return df

    df = df.copy()
    if "forventet_pris" not in df.columns:
        df["forventet_pris"] = np.nan
    if "modell_nivaa" not in df.columns:
        df["modell_nivaa"] = "Ingen modell"

    # Normaliserte oppslagsnøkler (samme som i tabellen)
    work = df.copy()
    work["prod_key"] = _norm_key(work.get("Produsent", pd.Series(index=work.index, dtype=object)))
    work["modell_key"] = _norm_key(work.get("Modell", pd.Series(index=work.index, dtype=object)))
    work["driv_key"] = _norm_key(work.get("drivstoff", pd.Series(index=work.index, dtype=object)))
    hjul = work.get("hjuldrift", pd.Series(index=work.index, dtype=object))
    work["hjul_key"] = hjul.map(_normaliser_hjuldrift).fillna("Ukjent").astype(str).str.strip().str.lower()

    t1 = koef[koef["tier"] == 1][["prod_key", "modell_key", "driv_key", "hjul_key", "beta0", "beta1", "beta2"]]
    t2 = koef[koef["tier"] == 2][["prod_key", "modell_key", "driv_key", "beta0", "beta1", "beta2"]]

    # Tier 1
    mangler = df["forventet_pris"].isna()
    if mangler.any() and not t1.empty:
        pred = _predikter(work.loc[mangler], t1, ["prod_key", "modell_key", "driv_key", "hjul_key"])
        traff = pred.dropna()
        if not traff.empty:
            df.loc[traff.index, "forventet_pris"] = traff.values
            df.loc[traff.index, "modell_nivaa"] = "PEER-T1"

    # Tier 2 (kun de som fortsatt mangler)
    mangler = df["forventet_pris"].isna()
    if mangler.any() and not t2.empty:
        pred = _predikter(work.loc[mangler], t2, ["prod_key", "modell_key", "driv_key"])
        traff = pred.dropna()
        if not traff.empty:
            df.loc[traff.index, "forventet_pris"] = traff.values
            df.loc[traff.index, "modell_nivaa"] = "PEER-T2"

    return df
