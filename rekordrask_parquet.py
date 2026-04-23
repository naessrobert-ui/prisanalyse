# rekordrask_parquet.py
# Tilpasset ny datamodell: database_biler.parquet har én rad per FinnKode
# med kolonnene Dato (første gang sett), Dato_ny (siste gang sett) og Solgt.
#
# Solgt-verdier i rå-data: "JA", "NEI", "FJERNET".
#   "JA"       -> definitivt solgt
#   "FJERNET"  -> annonsen er borte fra Finn. Tolkes som solgt (matcher
#                 gammel CSV-logikk "forsvunnet => solgt"), men kan skrus
#                 av med INKLUDER_FJERNET_SOM_SOLGT = False.
#   "NEI"      -> aktiv, ikke solgt.

from datetime import date
from typing import Iterable

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from config import AWS_KEY, AWS_SECRET, AWS_REGION, S3_BUCKET_NAME

FINN_BASE_URL = "https://www.finn.no/mobility/item/"
PARQUET_KEY = "calc/bil/database_biler.parquet"

# Tolker "FJERNET" som solgt? Matcher gammel CSV-adferd.
INKLUDER_FJERNET_SOM_SOLGT = True


def _get_s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SECRET,
        region_name=AWS_REGION,
    )


def _les_parquet(columns: Iterable[str] | None = None) -> pd.DataFrame:
    """
    Leser database_biler.parquet fra S3. Hvis columns oppgis, leses bare
    de kolonnene (472k rader * 102 kolonner er mye aa dra unodig).
    """
    s3 = _get_s3_client()
    obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key=PARQUET_KEY)
    table = pq.read_table(
        pa.BufferReader(obj["Body"].read()),
        columns=list(columns) if columns else None,
    )
    df = table.to_pandas()
    df.columns = [str(c) for c in df.columns]
    return df


def _as_int_series(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce").astype("Int64")


def bygg_visning_for_solgte_fra_parquet(startdato: date) -> pd.DataFrame:
    """
    Bygger visning over solgte biler basert paa database_biler.parquet.

    Parametre
    ---------
    startdato : date
        Kun biler der salget (Dato_ny) er >= denne datoen tas med.

    Returnerer
    ----------
    DataFrame med kolonnene frontend forventer:
        FinnKode, Finn, Merke, Modell, Aarsmodell, Km, Pris,
        Drivstoff, Forhandler type, timer_til_salg, foerste_gang_sett
    """

    # Les bare det vi trenger - sparer minne paa Render-instansen
    ønskede_kolonner = [
        "FinnKode",
        "Produsent",
        "Modell",
        "årstall",
        "kjørelengde",
        "drivstoff",
        "forhandler_type",
        "Dato",
        "Dato_ny",
        "Pris",
        "Pris_ny",
        "Solgt",
    ]

    df = _les_parquet(columns=ønskede_kolonner)

    # --- Normaliser Solgt-kolonnen ---
    df["_solgt_norm"] = (
        df["Solgt"].astype(str).str.strip().str.upper()
    )

    solgt_verdier = {"JA"}
    if INKLUDER_FJERNET_SOM_SOLGT:
        solgt_verdier.add("FJERNET")

    er_solgt = df["_solgt_norm"].isin(solgt_verdier)

    # --- Datofilter paa salgstidspunkt ---
    df["Dato"] = pd.to_datetime(df["Dato"], errors="coerce")
    df["Dato_ny"] = pd.to_datetime(df["Dato_ny"], errors="coerce")

    startdato_ts = pd.Timestamp(startdato)
    innenfor_periode = df["Dato_ny"] >= startdato_ts

    # Gyldige tidsstempler begge veier
    gyldig_tid = df["Dato"].notna() & df["Dato_ny"].notna()

    maske = er_solgt & innenfor_periode & gyldig_tid
    s = df.loc[maske].copy()

    print(
        f"[parquet] totalt={len(df):,} | "
        f"solgt({'JA+FJERNET' if INKLUDER_FJERNET_SOM_SOLGT else 'kun JA'})="
        f"{int(er_solgt.sum()):,} | "
        f"etter datofilter({startdato})={len(s):,}"
    )

    if s.empty:
        return pd.DataFrame()

    # --- Tid til salg ---
    diff = (s["Dato_ny"] - s["Dato"]).dt.total_seconds() / 3600.0
    s["timer_til_salg"] = diff.clip(lower=0).astype(float)

    # --- Bygg frontend-tabell ---
    # Solgt-status: "JA" = bekreftet solgt, "FJERNET" = annonse borte (~solgt,
    # men kan ogsaa vaere trukket tilbake). Lar frontend velge visning.
    ut = pd.DataFrame(
        {
            "FinnKode": s["FinnKode"].astype(str),
            "Finn": FINN_BASE_URL + s["FinnKode"].astype(str),
            "Merke": s["Produsent"].fillna("").astype(str),
            "Modell": s["Modell"].fillna("").astype(str),
            "Årsmodell": _as_int_series(s["årstall"]),
            "Km": _as_int_series(s["kjørelengde"]),
            # Bruk Pris_ny som "faktisk pris" - det er den siste observerte.
            # Fall tilbake til Pris hvis Pris_ny mangler.
            "Pris": _as_int_series(
                s["Pris_ny"].where(s["Pris_ny"].notna(), s["Pris"])
            ),
            "Drivstoff": s["drivstoff"].fillna("").astype(str),
            "Forhandler type": s["forhandler_type"].fillna("").astype(str),
            "Solgt status": s["_solgt_norm"],  # "JA" eller "FJERNET"
            "timer_til_salg": s["timer_til_salg"],
            "foerste_gang_sett": s["Dato"].dt.strftime("%Y-%m-%dT%H:%M:%S"),
            "siste_gang_sett": s["Dato_ny"].dt.strftime("%Y-%m-%dT%H:%M:%S"),
        }
    )

    ut = ut.sort_values("timer_til_salg", ascending=True).reset_index(drop=True)
    return ut
