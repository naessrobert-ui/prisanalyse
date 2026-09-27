"""
bil_favoritter.py – popularitet (favoritter) for BilRadar.

Kilder (skrevet av finn-scraper/konsolider_data.py):
  * database_biler.parquet: Favoritter_ny (siste måling), Favoritter_dato
    (når siste måling ble gjort), Dato (først sett).
  * calc/bil/favoritt_historikk/ÅÅÅÅ-MM.parquet: endringslogg
    (FinnKode, tid, favoritter) – én rad når tallet først måles og hver
    gang det endrer seg.

Beregner per aktiv annonse:
  fav_per_dag – Favoritter_ny / dager ute (minst 1 dag)
  fav_1t      – økning siste time  (kun annonser målt i timeskrapen)
  fav_24t     – økning siste døgn

Tidsreferansen er siste snapshot i historikken (ikke klokka), så tallene er
konsistente med dataene og uavhengige av tidssone.
"""
from __future__ import annotations

import io

import numpy as np
import pandas as pd

HISTORIKK_PREFIX = "calc/bil/favoritt_historikk/"
# Snapshot-tidene varierer noen minutter; uten slakk ville «1 time siden»
# ofte bomme på forrige timeskjøring og i stedet treffe den før.
SLAKK = pd.Timedelta(minutes=15)


def last_historikk(s3, bucket: str, naa: pd.Timestamp | None = None) -> pd.DataFrame:
    """Les denne og forrige måneds endringslogg. Tom DataFrame ved feil."""
    naa = pd.Timestamp(naa) if naa is not None else pd.Timestamp.now()
    maaneder = {naa.strftime("%Y-%m"), (naa - pd.DateOffset(months=1)).strftime("%Y-%m")}
    deler = []
    for m in sorted(maaneder):
        try:
            obj = s3.get_object(Bucket=bucket, Key=f"{HISTORIKK_PREFIX}{m}.parquet")
            deler.append(pd.read_parquet(io.BytesIO(obj["Body"].read())))
        except Exception as e:  # mangler (første måned) eller ingen tilgang
            print(f"      [favoritter] Ingen historikk for {m}: {type(e).__name__}")
    if not deler:
        return pd.DataFrame(columns=["FinnKode", "tid", "favoritter"])
    hist = pd.concat(deler, ignore_index=True)
    hist["FinnKode"] = pd.to_numeric(hist["FinnKode"], errors="coerce")
    hist["tid"] = pd.to_datetime(hist["tid"], errors="coerce")
    hist["favoritter"] = pd.to_numeric(hist["favoritter"], errors="coerce")
    return hist.dropna().astype({"FinnKode": "int64"})


def _verdi_ved(hist: pd.DataFrame, tidspunkt: pd.Timestamp) -> pd.Series:
    """Favoritter per FinnKode slik de var ved tidspunktet (siste rad <= tid)."""
    foer = hist[hist["tid"] <= tidspunkt]
    if foer.empty:
        return pd.Series(dtype="float64")
    return foer.sort_values("tid").groupby("FinnKode")["favoritter"].last()


def okning(df: pd.DataFrame, hist: pd.DataFrame, naa: pd.Timestamp,
           timer: float) -> pd.Series:
    """Økning i favoritter siste `timer` timer, indeksert som df.

    NaN når annonsen ikke er målt innenfor vinduet (f.eks. eldre annonser
    som bare måles i dagsskrapen, for «siste time»), eller når historikken
    ikke rekker tilbake til vinduets start."""
    grense = naa - pd.Timedelta(hours=timer) + SLAKK
    fk = pd.to_numeric(df["FinnKode"], errors="coerce")
    siste = pd.to_numeric(df.get("Favoritter_ny"), errors="coerce")
    maalt = pd.to_datetime(df.get("Favoritter_dato"), errors="coerce")
    forst_sett = pd.to_datetime(df.get("Dato"), errors="coerce")

    base = fk.map(_verdi_ved(hist, grense))
    # Ny annonse i vinduet: den startet på 0 favoritter.
    base = base.where(base.notna() | ~(forst_sett >= grense), 0.0)
    ut = siste - base
    gyldig = siste.notna() & (maalt >= grense)
    return ut.where(gyldig).clip(lower=0)


def per_dag(df: pd.DataFrame, naa: pd.Timestamp) -> pd.Series:
    siste = pd.to_numeric(df.get("Favoritter_ny"), errors="coerce")
    forst_sett = pd.to_datetime(df.get("Dato"), errors="coerce")
    dager = ((naa - forst_sett).dt.total_seconds() / 86400).clip(lower=1)
    return (siste / dager).round(1)


def berik(df: pd.DataFrame, hist: pd.DataFrame) -> pd.DataFrame:
    """Legg til fav_per_dag, fav_1t og fav_24t (in place + returnerer df)."""
    if "Favoritter_ny" not in df.columns:
        for c in ("fav_per_dag", "fav_1t", "fav_24t"):
            df[c] = np.nan
        return df
    maalt = pd.to_datetime(df.get("Favoritter_dato"), errors="coerce")
    kandidater = [t for t in (hist["tid"].max() if not hist.empty else pd.NaT,
                              maalt.max()) if pd.notna(t)]
    naa = max(kandidater) if kandidater else pd.Timestamp.now()
    df["fav_per_dag"] = per_dag(df, naa)
    df["fav_1t"] = okning(df, hist, naa, 1)
    df["fav_24t"] = okning(df, hist, naa, 24)
    return df
