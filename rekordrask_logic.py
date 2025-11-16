# rekordrask_logic.py
import io
from datetime import datetime, timedelta, timezone, date
from functools import lru_cache

import boto3
import numpy as np
import pandas as pd

BUCKET_NAME = "prisanalyse-data"
PREFIX_DAGLIG = "raw/bil-daglig/"
PREFIX_TIME = "raw/bil-time/"
FINN_KODE_KOLONNE_NAVN = "FinnKode"
FINN_BASE_URL = "https://www.finn.no/mobility/item/"


# -------------------------------------------------
# Normalisering / kolonnemapping
# -------------------------------------------------

def _normkey(s: str) -> str:
    if s is None:
        return ""
    t = (
        s.strip()
        .replace(" ", "")
        .replace("_", "")
        .replace("-", "")
        .replace("ø", "o")
        .replace("Ø", "o")
        .replace("æ", "ae")
        .replace("Æ", "ae")
        .replace("å", "a")
        .replace("Å", "a")
    )
    return t.lower()


def normalize_finnkode_series(s: pd.Series) -> pd.Series:
    s = s.astype(str).str.strip()
    s = s.str.extract(r"(\d+)", expand=False)
    s = s.fillna("").str.lstrip("0")
    return s


def _find_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    colmap_norm = {_normkey(c): c for c in df.columns}
    for cand in candidates:
        real = colmap_norm.get(_normkey(cand))
        if real in df.columns:
            return real
    return None


def _ensure_standard_cols(df: pd.DataFrame) -> pd.DataFrame:
    # FinnKode
    fk_src = _find_col(
        df,
        [
            "finnkode",
            "finn_kode",
            "finnid",
            "finnannonseid",
            "finnannonse",
            "finn",
            "annonseid",
            "annonse_id",
            "finnkodeid",
        ],
    )
    if fk_src and fk_src != FINN_KODE_KOLONNE_NAVN:
        df.rename(columns={fk_src: FINN_KODE_KOLONNE_NAVN}, inplace=True)

    # Årsmodell
    ym_src = _find_col(
        df,
        ["årsmodell", "aarsmodell", "arsmodell", "årstall", "arstall", "modellaar", "modellår"],
    )
    if ym_src and ym_src != "Årsmodell":
        df.rename(columns={ym_src: "Årsmodell"}, inplace=True)
    if "Årsmodell" not in df.columns:
        df["Årsmodell"] = pd.NA

    # Km
    km_src = _find_col(
        df,
        ["kjørelengde", "kjorelengde", "kjoerelengde", "km", "kilometer", "odo", "odometer"],
    )
    if km_src and km_src != "Km":
        df.rename(columns={km_src: "Km"}, inplace=True)
    if "Km" not in df.columns:
        df["Km"] = pd.NA

    # Pris
    if "Pris" not in df.columns:
        price_src = _find_col(df, ["pris", "price", "belop", "beløp"])
        if price_src:
            df.rename(columns={price_src: "Pris"}, inplace=True)
        else:
            df["Pris"] = pd.NA

    # Merke / Modell / Drivstoff
    if "Merke" not in df.columns:
        m_src = _find_col(df, ["merke", "brand", "make"])
        df["Merke"] = df[m_src] if m_src else pd.NA
    if "Modell" not in df.columns:
        mo_src = _find_col(df, ["modell", "model"])
        df["Modell"] = df[mo_src] if mo_src else pd.NA
    if "Drivstoff" not in df.columns:
        d_src = _find_col(df, ["drivstoff", "fuel"])
        df["Drivstoff"] = df[d_src] if d_src else pd.NA

    # Tittel / Info
    if "Tittel" not in df.columns:
        t_src = _find_col(df, ["tittel", "title", "annonsetittel", "info"])
        df["Tittel"] = df[t_src] if t_src else pd.NA
    else:
        info_src = _find_col(df, ["info"])
        if info_src:
            df["Tittel"] = df["Tittel"].fillna(df[info_src])

    # Forhandler type
    ftype = _find_col(df, ["Forhandler type", "Forhandlertype", "forhandler_type"])
    if ftype and ftype != "Forhandler type":
        df.rename(columns={ftype: "Forhandler type"}, inplace=True)
    if "Forhandler type" not in df.columns:
        df["Forhandler type"] = pd.NA

    # Normaliser FinnKode
    if FINN_KODE_KOLONNE_NAVN in df.columns:
        df[FINN_KODE_KOLONNE_NAVN] = normalize_finnkode_series(df[FINN_KODE_KOLONNE_NAVN])

    return df


# -------------------------------------------------
# S3-lesing
# -------------------------------------------------

def _read_csv_from_s3(key: str) -> pd.DataFrame:
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
    df = pd.read_csv(io.BytesIO(obj["Body"].read()), encoding="utf-16", sep=";")
    df = _ensure_standard_cols(df)
    return df


def hent_og_sorter_filer_fra_s3(bucket: str, prefix: str):
    s3 = boto3.client("s3")
    try:
        resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        if "Contents" not in resp:
            return []
        return sorted(resp["Contents"], key=lambda o: o["LastModified"], reverse=True)
    except Exception as e:
        print(f"Feil under henting av filer fra {prefix}: {e}")
        return []


# -------------------------------------------------
# Datasett-bygging
# -------------------------------------------------

@lru_cache(maxsize=16)
def bygg_datasets(startdato_for_analyse: date):
    daglig_filer = hent_og_sorter_filer_fra_s3(BUCKET_NAME, PREFIX_DAGLIG)
    time_filer = hent_og_sorter_filer_fra_s3(BUCKET_NAME, PREFIX_TIME)

    if not daglig_filer or not time_filer:
        print("[bygg_datasets] Fant ingen filer i S3 (daglig/time).")
        return (
            pd.DataFrame(columns=[FINN_KODE_KOLONNE_NAVN]),
            pd.DataFrame(),
            pd.DataFrame(),
            None,
            None,
        )

    nyeste_daglig = daglig_filer[0]
    nyeste_time = time_filer[0]

    df_daglig_ny = _read_csv_from_s3(nyeste_daglig["Key"])
    df_time_ny = _read_csv_from_s3(nyeste_time["Key"])

    if (
        FINN_KODE_KOLONNE_NAVN not in df_daglig_ny.columns
        and FINN_KODE_KOLONNE_NAVN not in df_time_ny.columns
    ):
        print(
            f"[bygg_datasets] Mangler '{FINN_KODE_KOLONNE_NAVN}' i nyeste filer.\n"
            f"Daglig kolonner: {list(df_daglig_ny.columns)}\n"
            f"Time kolonner:   {list(df_time_ny.columns)}"
        )
        return (
            pd.DataFrame(columns=[FINN_KODE_KOLONNE_NAVN]),
            pd.DataFrame(),
            pd.DataFrame(),
            nyeste_daglig["Key"],
            nyeste_time["Key"],
        )

    # Aktive annonser i siste (daglig + time) – disse er IKKE solgt
    aktive_ids = pd.Index([], dtype="object")
    if FINN_KODE_KOLONNE_NAVN in df_daglig_ny.columns:
        aktive_ids = aktive_ids.union(
            df_daglig_ny[FINN_KODE_KOLONNE_NAVN].dropna().unique()
        )
    if FINN_KODE_KOLONNE_NAVN in df_time_ny.columns:
        aktive_ids = aktive_ids.union(
            df_time_ny[FINN_KODE_KOLONNE_NAVN].dropna().unique()
        )

    df_usolgt = pd.DataFrame(
        {FINN_KODE_KOLONNE_NAVN: pd.Series(aktive_ids, dtype="object")}
    )
    df_usolgt[FINN_KODE_KOLONNE_NAVN] = normalize_finnkode_series(
        df_usolgt[FINN_KODE_KOLONNE_NAVN]
    )

    # Historikk fra valgt dato (unntatt nyeste timefil)
    frames = []
    start_aware = datetime.combine(
        startdato_for_analyse, datetime.min.time()
    ).replace(tzinfo=timezone.utc)

    for fil_obj in time_filer[1:]:
        if fil_obj["LastModified"] < start_aware:
            break
        try:
            df = _read_csv_from_s3(fil_obj["Key"])
            df["tidspunkt"] = fil_obj["LastModified"]
            frames.append(df)
        except Exception as e:
            print(f"[bygg_datasets] Feil ved lesing av {fil_obj['Key']}: {e}")
            continue

    if not frames:
        print("[bygg_datasets] Ingen historikkfiler innenfor periode – returnerer bare df_usolgt.")
        return df_usolgt, pd.DataFrame(), pd.DataFrame(), nyeste_daglig["Key"], nyeste_time["Key"]

    alle_historikk = pd.concat(frames, ignore_index=True)
    if FINN_KODE_KOLONNE_NAVN not in alle_historikk.columns:
        print(
            f"[bygg_datasets] Mangler '{FINN_KODE_KOLONNE_NAVN}' i historikk. "
            f"Kolonner: {list(alle_historikk.columns)}"
        )
        return df_usolgt, pd.DataFrame(), pd.DataFrame(), nyeste_daglig["Key"], nyeste_time["Key"]

    alle_historikk[FINN_KODE_KOLONNE_NAVN] = normalize_finnkode_series(
        alle_historikk[FINN_KODE_KOLONNE_NAVN]
    )
    alle_historikk = alle_historikk.sort_values(
        [FINN_KODE_KOLONNE_NAVN, "tidspunkt"]
    )

    # Pris_eff
    alle_historikk["Pris_num"] = pd.to_numeric(
        alle_historikk.get("Pris"), errors="coerce"
    )
    pris_ffill = alle_historikk.groupby(FINN_KODE_KOLONNE_NAVN)["Pris_num"].ffill()
    pris_bfill = alle_historikk.groupby(FINN_KODE_KOLONNE_NAVN)["Pris_num"].bfill()
    alle_historikk["Pris_eff"] = (
        alle_historikk["Pris_num"].fillna(pris_ffill).fillna(pris_bfill)
    )

    # ------------------ SOLGT-logikk ------------------
    aktive_set = set(
        df_usolgt[FINN_KODE_KOLONNE_NAVN].dropna().astype(str).values
    )
    i_siste_mask = alle_historikk[FINN_KODE_KOLONNE_NAVN].astype(str).isin(
        aktive_set
    )

    # eksplisitt "Solgt" i Pris
    pris_series = alle_historikk.get(
        "Pris", pd.Series("", index=alle_historikk.index)
    ).astype(str)
    pris_norm = pris_series.str.replace(r"\s+", "", regex=True).str.lower()
    eksplisitt_solgt_mask = pris_norm.str.contains("solgt")

    # Selger-type (eksakt "privat" i kolonnen "Forhandler type")
    ftype = (
        alle_historikk.get("Forhandler type", pd.Series("", index=alle_historikk.index))
        .astype(str)
        .str.strip()
        .str.lower()
    )
    er_privat = ftype.eq("privat")

    # Forsvunnet + PRIVAT => solgt. Forsvunnet + forhandler/merkeforhandler => IKKE solgt
    forsvunnet_privat_solgt = (~i_siste_mask) & er_privat

    sold_mask = eksplisitt_solgt_mask | forsvunnet_privat_solgt

    # Fallback om det mot formodning ikke blir én eneste solgt
    if not sold_mask.any():
        print("[bygg_datasets] ADVARSEL: sold_mask er helt tom – faller tilbake til KUN eksplisitt 'solgt' i Pris.")
        sold_mask = eksplisitt_solgt_mask

    df_ny_solgt = alle_historikk.loc[sold_mask].copy()
    df_ny_usolgt = alle_historikk.loc[~sold_mask].copy()

    print(
        "[bygg_datasets] startdato =", startdato_for_analyse,
        "| alle_historikk =", len(alle_historikk),
        "| df_usolgt (aktive) =", len(df_usolgt),
        "| df_ny_solgt =", len(df_ny_solgt),
        "| df_ny_usolgt =", len(df_ny_usolgt),
        "| daglig_key =", nyeste_daglig["Key"],
        "| time_key =", nyeste_time["Key"],
    )

    return (
        df_usolgt,
        df_ny_usolgt,
        df_ny_solgt,
        nyeste_daglig["Key"],
        nyeste_time["Key"],
    )


def bygg_visning_for_solgte(df_ny_solgt: pd.DataFrame) -> pd.DataFrame:
    if df_ny_solgt.empty:
        print("[bygg_visning_for_solgte] df_ny_solgt er tom.")
        return df_ny_solgt

    df = df_ny_solgt.copy()
    df["tidspunkt"] = pd.to_datetime(df["tidspunkt"], errors="coerce")

    g = (
        df.groupby(FINN_KODE_KOLONNE_NAVN)["tidspunkt"]
        .agg(["min", "max"])
        .reset_index()
    )
    g.rename(
        columns={"min": "foerste_gang_sett", "max": "siste_gang_sett"}, inplace=True
    )
    g["timer_til_salg"] = (
        (g["siste_gang_sett"] - g["foerste_gang_sett"])
        .dt.total_seconds()
        .div(3600)
        .round()
        .astype("Int64")
    )

    siste = (
        df.sort_values(by="tidspunkt", ascending=False)
        .drop_duplicates(subset=FINN_KODE_KOLONNE_NAVN, keep="first")
        .copy()
    )

    res = pd.merge(
        siste,
        g[[FINN_KODE_KOLONNE_NAVN, "foerste_gang_sett", "timer_til_salg"]],
        on=FINN_KODE_KOLONNE_NAVN,
        how="left",
    )

    res["Pris_tekst"] = res.get("Pris", pd.Series(pd.NA)).astype(str)
    pris_kilde = pd.to_numeric(res.get("Pris_eff", res.get("Pris")), errors="coerce")
    res["Pris"] = pris_kilde.round(0).astype("Int64")

    res["Km"] = pd.to_numeric(res.get("Km", pd.Series(pd.NA)), errors="coerce").astype(
        "Int64"
    )
    res["Årsmodell"] = pd.to_numeric(
        res.get("Årsmodell", pd.Series(pd.NA)), errors="coerce"
    ).astype("Int64")
    res["Finn"] = FINN_BASE_URL + res[FINN_KODE_KOLONNE_NAVN].astype(str)

    res.drop(columns=["tidspunkt"], inplace=True, errors="ignore")

    ønsket = [
        FINN_KODE_KOLONNE_NAVN,
        "Finn",
        "Merke",
        "Modell",
        "Årsmodell",
        "Km",
        "Pris",
        "Pris_tekst",
        "Drivstoff",
        "Forhandler type",
        "foerste_gang_sett",
        "timer_til_salg",
        "Tittel",
        "Info",
    ]
    front = [c for c in ønsket if c in res.columns]
    rest = [c for c in res.columns if c not in front]
    res = res[front + rest]

    print("[bygg_visning_for_solgte] rader inn =", len(df_ny_solgt), "| rader ut =", len(res))

    return res
