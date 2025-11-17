# rekordrask_parquet.py
import json
from datetime import datetime, date
from typing import Tuple

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from config import AWS_KEY, AWS_SECRET, AWS_REGION, S3_BUCKET_NAME

FINN_BASE_URL = "https://www.finn.no/mobility/item/"
PARQUET_KEY = "calc/bil/bil_time.parquet"


def _get_s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SECRET,
        region_name=AWS_REGION,
    )


def _les_parquet_fra_s3() -> pd.DataFrame:
    """
    Leser Parquet-filen med time-snapshots av Finn-annonser fra S3.
    Forventer at den minst inneholder:
      - FinnKode (eller finnkode)
      - Merke / Modell / Årstall / Kjørelengde / Drivstoff / Pris / Forhandler type
      - enten 'snapshot_time' (ISO) eller 'dato' (YYYY-MM-DD)
    """
    s3 = _get_s3_client()
    obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key=PARQUET_KEY)
    data = obj["Body"].read()

    table = pq.read_table(pa.BufferReader(data))
    df = table.to_pandas()

    # Normaliser kolonnenavn litt (f.eks. fra CSV -> Parquet)
    df.columns = [str(c) for c in df.columns]

    # Forsøk å standardisere FinnKode-feltet
    if "FinnKode" in df.columns:
        df["FinnKode"] = df["FinnKode"].astype(str)
    elif "finnkode" in df.columns:
        df["FinnKode"] = df["finnkode"].astype(str)
    else:
        raise ValueError("Parquet mangler kolonne 'FinnKode' / 'finnkode'.")

    return df


def _ensure_time_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Sørger for at vi har både:
      - snapshot_time: datetime (når vi så annonsen)
      - dato: date (YYYY-MM-DD)
    Vi støtter to varianter:
      1) df har 'snapshot_time' (ISO-streng)
      2) df har 'dato' (YYYY-MM-DD) – da lager vi snapshot_time midt på dagen
    """
    if "snapshot_time" in df.columns:
        df["snapshot_time"] = pd.to_datetime(df["snapshot_time"], errors="coerce")
        df["dato"] = df["snapshot_time"].dt.date
    elif "dato" in df.columns:
        df["dato"] = pd.to_datetime(df["dato"], errors="coerce").dt.date
        df["snapshot_time"] = pd.to_datetime(df["dato"]) + pd.to_timedelta(12, unit="h")
    else:
        raise ValueError(
            "Parquet mangler både 'snapshot_time' og 'dato'. "
            "Utvid skriptet som lager Parquet til å inkludere minst én av dem."
        )

    # Sleng ut rader uten gyldig dato
    df = df[df["snapshot_time"].notna()]
    df = df[df["dato"].notna()]
    return df


def _is_sold(pris_val) -> bool:
    """
    Avgjør om en rad representerer at bilen er 'Solgt'.
    Vi tolker dette som:
      - Pris-feltet inneholder teksten 'solgt' (uansett case), eller
      - Pris (eller pris_num) er 0
    """
    if pd.isna(pris_val):
        return False

    # Tekstvariant – typisk 'Solgt'
    s = str(pris_val).strip().lower()
    if "solgt" in s:
        return True

    # Rent tall (0)
    try:
        v = int(s.replace(" ", ""))
        return v == 0
    except ValueError:
        return False


def _extract_numeric(val):
    """Trygg konvertering til heltall (eller None)."""
    if pd.isna(val):
        return None
    s = str(val).strip()
    s = s.replace(" ", "").replace("\xa0", "")
    if not s:
        return None
    try:
        return int(s)
    except ValueError:
        return None


def bygg_visning_for_solgte_fra_parquet(startdato: date) -> pd.DataFrame:
    """
    Leser Parquet med alle snapshots og bygger en tabell over biler som faktisk er solgt,
    med beregnet 'timer_til_salg' per FinnKode.

    Returnerer DataFrame med kolonner som frontend forventer:
      - FinnKode
      - Finn        (lenke til annonsen)
      - Merke
      - Modell
      - Årsmodell
      - Km
      - Pris
      - Drivstoff
      - "Forhandler type"
      - timer_til_salg
      - foerste_gang_sett (ISO)
    """

    df = _les_parquet_fra_s3()
    df = _ensure_time_columns(df)

    # Normaliser noen kolonnenavn vi trenger
    # (både store/små bokstaver og norsk/engelsk variant)
    colmap = {}

    def _first_existing(names):
        for n in names:
            if n in df.columns:
                return n
        return None

    colmap["Merke"] = _first_existing(["Merke", "produsent", "Produsent"])
    colmap["Modell"] = _first_existing(["Modell", "modell"])
    colmap["Årstall"] = _first_existing(["Årstall", "årstall", "Aarstall"])
    colmap["Kjørelengde"] = _first_existing(["Kjørelengde", "kjørelengde", "Km"])
    colmap["Drivstoff"] = _first_existing(["Drivstoff", "drivstoff"])
    colmap["Pris"] = _first_existing(["Pris", "pris", "pris_num"])
    colmap["Forhandler type"] = _first_existing(
        ["Forhandler type", "Forhandler", "forhandler_type"]
    )

    required_keys = ["Merke", "Modell", "Årstall", "Kjørelengde", "Pris"]
    for k in required_keys:
        if colmap[k] is None:
            print(f"ADVARSEL: Fant ikke kolonne for {k} i Parquet – bruker tomme verdier der.")
            # vi lar det være None, så fylles det inn som "" senere

    records = []

    # Gruppér på FinnKode og beregn tid til salg
    for finnkode, grp in df.groupby("FinnKode"):
        g = grp.sort_values("snapshot_time").copy()
        if g.empty:
            continue

        # Første gang vi så annonsen
        first_row = g.iloc[0]
        first_time = first_row["snapshot_time"]

        # Finn første rad der vi tolker bilen som 'solgt'
        pris_col = colmap["Pris"]
        if pris_col is None or pris_col not in g.columns:
            continue

        sold_mask = g[pris_col].apply(_is_sold)
        sold_rows = g[sold_mask]

        if sold_rows.empty:
            # Aldri observert som "Solgt" → hopp over (bilen er nok fortsatt aktiv)
            continue

        sale_row = sold_rows.iloc[0]
        sale_time = sale_row["snapshot_time"]

        # Hvis salget skjedde før ønsket startdato -> hopp over
        if sale_time.date() < startdato:
            continue

        # Timer til salg
        timer_til_salg = (sale_time - first_time).total_seconds() / 3600.0
        if timer_til_salg < 0:
            # Noe rart med rekkefølgen – hopp over
            continue

        # Plukk ut felter
        merke = sale_row.get(colmap["Merke"]) if colmap["Merke"] else ""
        modell = sale_row.get(colmap["Modell"]) if colmap["Modell"] else ""
        aar = sale_row.get(colmap["Årstall"]) if colmap["Årstall"] else None
        km = sale_row.get(colmap["Kjørelengde"]) if colmap["Kjørelengde"] else None
        pris_raw = sale_row.get(colmap["Pris"]) if colmap["Pris"] else None
        drivstoff = sale_row.get(colmap["Drivstoff"]) if colmap["Drivstoff"] else ""
        forhandler_type = (
            sale_row.get(colmap["Forhandler type"]) if colmap["Forhandler type"] else ""
        )

        # Konverter tall
        aar_int = _extract_numeric(aar)
        km_int = _extract_numeric(km)
        pris_int = _extract_numeric(pris_raw)

        records.append(
            {
                "FinnKode": str(finnkode),
                "Finn": FINN_BASE_URL + str(finnkode),
                "Merke": merke or "",
                "Modell": modell or "",
                "Årsmodell": aar_int,
                "Km": km_int,
                "Pris": pris_int,
                "Drivstoff": drivstoff or "",
                "Forhandler type": forhandler_type or "",
                "timer_til_salg": float(timer_til_salg),
                "foerste_gang_sett": first_time.isoformat(),
            }
        )

    if not records:
        return pd.DataFrame()

    vis_df = pd.DataFrame.from_records(records)

    # Sorter: raskest først
    vis_df = vis_df.sort_values("timer_til_salg", ascending=True).reset_index(drop=True)
    return vis_df
