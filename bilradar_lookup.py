"""
bilradar_lookup.py – Transparent lookup-prising fra salgsdata
=============================================================
Leser en CSV-tabell (lokal eller S3) bygget av scripts/lag_prislookup.py
og bruker den til aa estimere markedspris direkte fra median-salg per
(Produsent, Modell, hjuldrift, drivstoff, aarstall)-gruppe.

Ved oppslag: pris = median_pris + km_slope * (kjorelengde - median_km)

Brukes som primaer prisestimering i bilradar_scorer; ML-modellen
beholdes som fallback for biler hvor lookup ikke har data.

CSV-format (kolonner):
    Produsent, Modell, hjuldrift, drivstoff, aarstall,
    n_obs, median_pris, median_km, km_slope, ...

Caches paa samme maate som overrides-tabellen — last_lookup() er
trådsikker og kan kalles fritt fra Flask-routes.
"""

import io
import os
import threading
from datetime import datetime

import numpy as np
import pandas as pd

LOOKUP_KEYS = ["Produsent", "Modell", "variant_id", "hjuldrift", "drivstoff", "årstall"]
LOOKUP_VALS = ["n_obs", "median_pris", "median_km", "hurtigpris", "innbyttepris", "km_slope_pct"]
LOOKUP_COLUMNS = LOOKUP_KEYS + LOOKUP_VALS

# Innbyttepris = forventet_pris * (1 - INNBYTTE_RABATT), gulvet mot hurtigpris.
INNBYTTE_RABATT = 0.15

_LOOKUP_LOCK = threading.Lock()
_LOOKUP_CACHE = {"df": None, "loaded_at": None}


def _normaliser_hjuldrift(verdi) -> str:
    """Mapper alle hjuldrift-varianter til "Tohjul"/"Firehjul"/"Ukjent".

    Treningsdata (database_biler.parquet) bruker "Tohjul"/"Firehjul",
    mens FINN-detaljparseren leverer "Forhjulsdrift"/"Bakhjulsdrift"/
    "Firehjulsdrift" — uten normalisering matcher lookup-tabellen ingenting
    paa "NY – IKKE I DB"-biler.
    """
    if verdi is None:
        return "Ukjent"
    s = str(verdi).strip().lower()
    if not s or s in ("ukjent", "nan", "none"):
        return "Ukjent"
    if "firehjul" in s or "awd" in s or s == "4x4":
        return "Firehjul"
    if "tohjul" in s or "forhjul" in s or "bakhjul" in s or "fwd" in s or "rwd" in s:
        return "Tohjul"
    return str(verdi).strip()


def _les_csv(buf_or_path) -> pd.DataFrame:
    df = pd.read_csv(buf_or_path)
    df.columns = [c.strip() for c in df.columns]

    # Bakoverkompatibilitet med tabeller bygget foer variant/hurtigpris/log-slope:
    #  - mangler variant_id  -> alt "ikke_klassifisert" (grov nøkkel som foer)
    #  - mangler km_slope_pct -> utled fra lineaer km_slope / median_pris
    #  - mangler hurtigpris   -> p25_pris hvis vi har den, ellers median_pris
    #  - mangler innbyttepris -> median_pris * (1 - INNBYTTE_RABATT)
    if "variant_id" not in df.columns:
        df["variant_id"] = "ikke_klassifisert"
    if "km_slope_pct" not in df.columns:
        median_pris_tmp = pd.to_numeric(df.get("median_pris"), errors="coerce")
        lin = pd.to_numeric(df.get("km_slope"), errors="coerce")
        df["km_slope_pct"] = (lin / median_pris_tmp).where(median_pris_tmp > 0, 0.0)
    if "hurtigpris" not in df.columns:
        df["hurtigpris"] = pd.to_numeric(
            df.get("p25_pris", df.get("median_pris")), errors="coerce"
        )
    if "innbyttepris" not in df.columns:
        df["innbyttepris"] = pd.to_numeric(df.get("median_pris"), errors="coerce") * (
            1 - INNBYTTE_RABATT
        )

    for col in LOOKUP_COLUMNS:
        if col not in df.columns:
            df[col] = np.nan

    for c in ["Produsent", "Modell", "drivstoff"]:
        df[c] = df[c].fillna("Ukjent").astype(str).str.strip()
    df["variant_id"] = df["variant_id"].fillna("ikke_klassifisert").astype(str).str.strip()
    df["hjuldrift"] = df["hjuldrift"].map(_normaliser_hjuldrift)
    df["årstall"] = pd.to_numeric(df["årstall"], errors="coerce").astype("Int64")
    for c in ["n_obs", "median_pris", "median_km", "hurtigpris", "innbyttepris", "km_slope_pct"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.dropna(subset=["median_pris", "median_km", "km_slope_pct", "årstall"])
    # Fyll hurtig/innbytte defensivt hvis enkeltrader mangler dem.
    df["hurtigpris"] = df["hurtigpris"].fillna(df["median_pris"])
    df["innbyttepris"] = df["innbyttepris"].fillna(df["median_pris"] * (1 - INNBYTTE_RABATT))
    return df.reset_index(drop=True)


def last_lookup(local_path: str = "", s3_client=None, bucket: str = "", key: str = "") -> pd.DataFrame:
    """Last lookup-tabell. Returnerer tom DataFrame hvis intet finnes (ikke en feil)."""
    with _LOOKUP_LOCK:
        if _LOOKUP_CACHE["df"] is not None:
            return _LOOKUP_CACHE["df"]

        df = pd.DataFrame(columns=LOOKUP_COLUMNS)

        if local_path and os.path.exists(local_path):
            try:
                df = _les_csv(local_path)
                print(f"[BilRadar] Lookup lastet ({len(df)} grupper) fra {local_path}")
            except Exception as e:
                print(f"[BilRadar] Klarte ikke lese {local_path}: {e}")
        elif s3_client and bucket and key:
            try:
                obj = s3_client.get_object(Bucket=bucket, Key=key)
                df = _les_csv(io.BytesIO(obj["Body"].read()))
                print(f"[BilRadar] Lookup lastet ({len(df)} grupper) fra s3://{bucket}/{key}")
            except Exception as e:
                print(f"[BilRadar] Ingen lookup fra S3 ({e}) – fortsetter uten")

        _LOOKUP_CACHE["df"] = df
        _LOOKUP_CACHE["loaded_at"] = datetime.now()
        return df


def reload_lookup():
    """Tving re-lasting ved neste kall (etter at CSV er endret)."""
    with _LOOKUP_LOCK:
        _LOOKUP_CACHE["df"] = None
        _LOOKUP_CACHE["loaded_at"] = None


def _klassifiser_variant_kolonne(df: pd.DataFrame) -> pd.Series:
    """Gi hver rad en variant_id slik at oppslaget treffer riktig
    batteripakke/utstyrsvariant for elbil. Biler uten katalogtreff (all
    bensin/diesel + elbiler utenfor katalogen) faar "ikke_klassifisert" og
    matcher da de grove gruppene i lookup-tabellen. Faller stille tilbake til
    "ikke_klassifisert" hvis klassifisereren ikke kan importeres."""
    if "variant_id" in df.columns and df["variant_id"].notna().any():
        return df["variant_id"].fillna("ikke_klassifisert").astype(str).str.strip()
    try:
        from bil_variant_klassifiserer import (
            klassifiser_varianter,
            last_variantkatalog,
        )
        vid, _kilde = klassifiser_varianter(df, last_variantkatalog())
        return pd.Series(vid, index=df.index).fillna("ikke_klassifisert").astype(str).str.strip()
    except Exception as e:  # pragma: no cover - defensiv
        print(f"[BilRadar] Variant-klassifisering hoppet over ({e})")
        return pd.Series(["ikke_klassifisert"] * len(df), index=df.index)


def apply_lookup(df: pd.DataFrame, lookup: pd.DataFrame) -> pd.DataFrame:
    """Overskriv forventet_pris/hurtigpris/innbyttepris/peer_konfidens/
    modell_nivaa for biler som matcher en (merke, modell, variant_id,
    hjuldrift, drivstoff, aarstall)-gruppe i lookup. Prisen km-justeres
    prosentvis (log-rom): pris = median_pris * exp(km_slope_pct * (km -
    median_km)). Biler uten match beholder ML-prediksjon. Idempotent: tom
    lookup = ingen endring."""
    if lookup is None or lookup.empty:
        return df

    df = df.copy()

    # Sikre samme dtype paa begge sider
    venstre = df.copy()
    for c in ["Produsent", "Modell", "drivstoff"]:
        if c in venstre.columns:
            venstre[c] = venstre[c].fillna("Ukjent").astype(str).str.strip()
    if "hjuldrift" in venstre.columns:
        venstre["hjuldrift"] = venstre["hjuldrift"].map(_normaliser_hjuldrift)
    venstre["variant_id"] = _klassifiser_variant_kolonne(venstre).values
    venstre["årstall"] = pd.to_numeric(venstre.get("årstall"), errors="coerce").astype("Int64")
    venstre["_idx"] = np.arange(len(venstre))

    merged = venstre.merge(
        lookup[LOOKUP_COLUMNS], on=LOOKUP_KEYS, how="left", suffixes=("", "_lu"),
    )
    merged = merged.sort_values("_idx").set_index("_idx")

    mask = merged["median_pris"].notna() & merged["km_slope_pct"].notna()
    if not mask.any():
        return df

    km = pd.to_numeric(merged.get("kjørelengde"), errors="coerce").fillna(0)
    # Prosentvis (log-rom) km-justering: felles faktor for markeds- og hurtigpris.
    km_faktor = np.exp(merged["km_slope_pct"] * (km - merged["median_km"]))

    forventet = (merged["median_pris"] * km_faktor).clip(lower=1_000.0)
    hurtig = (merged["hurtigpris"] * km_faktor).clip(lower=1_000.0)
    # Innbytte: 15 %% under forventet, men aldri over den km-justerte hurtigprisen.
    innbytte = pd.concat(
        [forventet * (1 - INNBYTTE_RABATT), hurtig], axis=1
    ).min(axis=1).clip(lower=1_000.0)

    m = mask.values
    df.loc[m, "forventet_pris"] = forventet[mask].values
    df.loc[m, "peer_konfidens"] = merged.loc[mask, "n_obs"].values

    if "hurtigpris" not in df.columns:
        df["hurtigpris"] = np.nan
    df.loc[m, "hurtigpris"] = hurtig[mask].values

    if "innbyttepris" not in df.columns:
        df["innbyttepris"] = np.nan
    df.loc[m, "innbyttepris"] = innbytte[mask].values

    if "modell_nivaa" not in df.columns:
        df["modell_nivaa"] = "Ingen modell"
    df.loc[m, "modell_nivaa"] = "LOOKUP"

    return df
