import re
import os
import json
import io
import gc
from datetime import datetime, date, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup

# BilRadar scoring er fjernet fra konsolideringen: verdsettelsen gjøres nå
# i skyen (GitHub Action "Scor Bilradar aktive biler") med den samlede
# lookup/variant-motoren, og /radar/siste live-scorer resten. Denne jobben
# laster kun opp database_biler.parquet og trigger scoringen (se nederst).

"""
Unified konsolidering (daily + hourly) -> calc/bil/database_biler.parquet

Leser:
    raw/bil-daglig/biler_alle_dd-mm-YYYY.csv   (tolkes som snapshot kl 07:00)
    raw/bil-time/biler_siste_dd-mm-YYYY_HHMM.csv (snapshot HH:MM fra filnavn)

Inkrementell:
    * Hvis database finnes: les den og prosesser kun filer med observed_at > last_processed
    * Hvis ikke: bygg fra scratch fra START_DATE

Prislogikk:
    * Pris=0 / tom oppdaterer IKKE pris-felter
    * Første pris > 0 -> Pris (startpris, endres aldri)
    * Siste pris > 0 -> Pris_ny (nåværende pris)

Feltfyll:
    * Oppdater kun manglende felt (ikke overskriv gode verdier med tomme)
    * Daily kan overskrive "autoritative" felt (hjuldrift, karosseri, fylke, forhandler)

Timestamps:
    * Dato          = første gang bilen ble observert
    * Dato_ny       = sist observert (daily eller hourly), brukes til FJERNET-logikk

Status (Solgt):
    * "JA"      = eksplisitt "solgt"-signal i prisfelt
    * "NEI"     = sett i nyeste daily/hourly-kjøring
    * "FJERNET" = ikke sett på >=2 dager (tåler glipp i én daily)
"""

# ===================== Konfig =====================
S3_BUCKET = "prisanalyse-data"

# GitHub-dispatch: trigg sky-scoring (bilradar_aktive.parquet) etter opplasting.
GITHUB_REPO = os.getenv("GITHUB_DISPATCH_REPO", "naessrobert-ui/prisanalyse")
GITHUB_DISPATCH_TOKEN = os.getenv("GITHUB_DISPATCH_TOKEN")

S3_DAILY_PREFIX = "raw/bil-daglig/"
S3_HOURLY_PREFIX = "raw/bil-time/"

S3_OUTPUT_PREFIX = "calc/bil/"
S3_OUTPUT_PREFIX2 = "calc/"

OUTPUT_PARQUET_KEY = f"{S3_OUTPUT_PREFIX}database_biler.parquet"
OUTPUT_METADATA_KEY = f"{S3_OUTPUT_PREFIX2}metadata.json"
OUTPUT_STATE_KEY = f"{S3_OUTPUT_PREFIX}state_database_biler.json"

DAILY_PATTERN = r"biler_alle_(\d{2})-(\d{2})-(\d{4})\.csv$"
HOURLY_PATTERN = r"biler_siste_(\d{2})-(\d{2})-(\d{4})_(\d{2})(\d{2})\.csv$"

START_DATE = date(2025, 9, 7)


# ===================== Berikelse (kun for nye biler) =====================
MAX_WORKERS_FINN = 10
MAX_WORKERS_SVV = 10

FINN_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "nb-NO,nb;q=0.9,en-US;q=0.8,en;q=0.7",
}

FINN_BASE_URL = "https://www.finn.no/mobility/item/{}"
SVV_API_KEY = "094532a8-2343-4b4a-93f7-e284b1e0ec85"

# Felter som hentes for nye biler fra FINN / SVV.
ENRICHMENT_OUTPUT_FIELDS = [
    "url",
    "omregistrering",
    "pris_eks_omreg",
    "årsavgift",
    "vekt_kg",
    "seter",
    "dører",
    "bagasjerom_l",
    "farge",
    "bilen_staar_i",
    "eu_frist",
    "avgiftsklasse",
    "regnr",
    "reg_norge",
    "eiere_antall",
    "effekt_hk",
    "batterikapasitet_kwh",
    "rekkevidde_wltp_km",
    "kontakt_email",
    "kontakt_telefon",
    "svv_regnr",
    "svv_vin",
    "svv_kjennemerke_ordinart",
    "svv_kjennemerke_personlig",
    "svv_kjennemerke_alle",
    "svv_registreringsstatus",
    "svv_registrert_forste_gang_norge",
    "svv_registrert_forste_gang",
    "svv_registrert_pa_eier",
    "svv_merke",
    "svv_merke_kode",
    "svv_handelsbetegnelse",
    "svv_typebetegnelse",
    "svv_kjoretoy_typebeskrivelse",
    "svv_kjoretoy_teknisk_kode",
    "svv_kjoretoy_avgiftskode_navn",
    "svv_kjoretoy_avgiftskode_verdi",
    "svv_bruktimportert",
    "svv_importland_navn",
    "svv_importland_kode",
    "svv_import_kilometerstand",
    "svv_import_tidligere_kjennemerke",
    "svv_import_tidligere_vognkortnr",
    "svv_forstegang_reg_dato_utland",
    "svv_fortolling_beskrivelse",
    "svv_fortolling_referanse",
    "svv_lengde_mm",
    "svv_bredde_mm",
    "svv_hoyde_mm",
    "svv_egenvekt_kg",
    "svv_egenvekt_minimum_kg",
    "svv_nyttelast_kg",
    "svv_tillatt_totalvekt_kg",
    "svv_tillatt_tilhenger_med_brems_kg",
    "svv_tillatt_tilhenger_uten_brems_kg",
    "svv_antall_motorer",
    "svv_slagvolum_cm3",
    "svv_antall_sylindre",
    "svv_motor_kode",
    "svv_motor_arbeidsprinsipp",
    "svv_drivstoff_navn",
    "svv_maks_netto_effekt_kw",
    "svv_hybridkategori",
    "svv_euroklasse",
    "svv_elektrisk_rekkevidde_km",
    "svv_forbruk_blandet_l_100km",
    "svv_co2_blandet_g_km",
    "svv_sitteplasser_totalt",
    "svv_sitteplasser_foran",
    "svv_kontrollfrist",
    "svv_sist_eu_godkjent",
    "svv_antall_aksler",
    "svv_antall_aksler_med_drift",
    "svv_dekk_standard_foran",
    "svv_dekk_standard_bak",
    "svv_dekkdimensjoner_unike",
    "svv_dekk_alle_kombinasjoner",
]

# Overlapp mellom snapshot/master og berikelsesdata som kun skal fylles når de mangler.
ENRICHMENT_FILL_TARGETS = {
    "Produsent": ["Produsent"],
    "Modell": ["Modell"],
    "årstall": ["årstall"],
    "kjørelengde": ["kjørelengde"],
    "drivstoff": ["drivstoff"],
    "girkasse": ["girkasse"],
    "hjuldrift": ["hjuldrift"],
    "Karosseri": ["Karosseri"],
    "selger": ["selger"],
    "Pris": ["Pris"],
    "Pris_ny": ["Pris_ny"],
    "seter": ["seter", "svv_sitteplasser_totalt"],
    "farge": ["farge"],
    "regnr": ["regnr", "svv_regnr", "svv_kjennemerke_ordinart"],
    "reg_norge": ["reg_norge", "svv_registrert_forste_gang_norge", "svv_registrert_forste_gang"],
    "batterikapasitet_kwh": ["batterikapasitet_kwh"],
    "rekkevidde_wltp_km": ["rekkevidde_wltp_km", "svv_elektrisk_rekkevidde_km"],
    "svv_vin": ["svv_vin"],
    "svv_kjennemerke_personlig": ["svv_kjennemerke_personlig"],
    "svv_registrert_forste_gang_norge": ["svv_registrert_forste_gang_norge"],
    "svv_registrert_pa_eier": ["svv_registrert_pa_eier"],
    "svv_bruktimportert": ["svv_bruktimportert"],
    "svv_importland_navn": ["svv_importland_navn"],
    "svv_sitteplasser_totalt": ["svv_sitteplasser_totalt"],
    "svv_antall_aksler_med_drift": ["svv_antall_aksler_med_drift"],
}

ZERO_IS_MISSING_COLUMNS = {
    "Pris",
    "Pris_ny",
    "årstall",
    "kjørelengde",
    "rekkevidde_str",
    "seter",
    "batterikapasitet_kwh",
    "rekkevidde_wltp_km",
    "svv_sitteplasser_totalt",
    "svv_antall_aksler_med_drift",
}

NULL_LIKE_STRINGS = {
    "",
    " ",
    "na",
    "n/a",
    "nan",
    "none",
    "null",
    "nil",
    "ukjent",
    "ikke oppgitt",
    "ikke spesifisert",
    "-",
    "--",
}

_ENRICH_CACHE_BY_FINNKODE: dict[str, dict] = {}
_SVV_CACHE_BY_IDENT: dict[str, dict | None] = {}
_SVV_API_WARNING_PRINTED = False


# --------------------- helpers ---------------------
def _as_str(v):
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return ""
    s = str(v).strip()
    if s.lower() in {"", "nan", "none", "<na>"}:
        return ""
    return s


def _as_int(x, default=None):
    if x is None:
        return default
    if isinstance(x, (int, np.integer)):
        return int(x)
    try:
        s = str(x).strip()
        if s == "" or s.lower() in {"nan", "none", "<na>"}:
            return default
        if re.search(r"\D", s):
            digits = re.sub(r"\D", "", s)
            if digits == "":
                return default
            s = digits
        return int(s)
    except Exception:
        return default


def _is_missing(v) -> bool:
    return _as_str(v) == ""


def parse_observed_at_from_key(key: str):
    filename = key.split("/")[-1]
    m = re.search(DAILY_PATTERN, filename)
    if m:
        dd, mm, yyyy = map(int, m.groups())
        return datetime(yyyy, mm, dd, 7, 0, 0), "daily"

    m = re.search(HOURLY_PATTERN, filename)
    if m:
        dd, mm, yyyy, hh, minute = m.groups()
        dd, mm, yyyy, hh, minute = map(int, (dd, mm, yyyy, hh, minute))
        return datetime(yyyy, mm, dd, hh, minute, 0), "hourly"

    return None


def s3_list_csv_objects(s3, prefix: str):
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix)
    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".csv"):
                continue
            if obj.get("Size", 0) == 0:
                continue
            yield key


def s3_read_csv_utf16_semicolon(s3, key: str) -> pd.DataFrame:
    resp = s3.get_object(Bucket=S3_BUCKET, Key=key)
    content = resp["Body"].read().decode("utf-16")
    df = pd.read_csv(io.StringIO(content), sep=";", on_bad_lines="warn", dtype=str)
    return df


def try_load_state(s3) -> dict:
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=OUTPUT_STATE_KEY)
        raw = obj["Body"].read().decode("utf-8")
        return json.loads(raw)
    except Exception:
        return {}


def save_state(s3, state: dict):
    buf = json.dumps(state, ensure_ascii=False, indent=2).encode("utf-8")
    s3.put_object(Bucket=S3_BUCKET, Key=OUTPUT_STATE_KEY, Body=buf)


def try_load_existing_master(s3) -> pd.DataFrame | None:
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=OUTPUT_PARQUET_KEY)
        data = obj["Body"].read()
        df = pd.read_parquet(io.BytesIO(data))

        if "FinnKode" in df.columns:
            df["FinnKode"] = pd.to_numeric(df["FinnKode"], errors="coerce")
            df = df[df["FinnKode"].notna()].copy()
            df["FinnKode"] = df["FinnKode"].astype("int64")

        return df
    except Exception:
        return None


def to_int_or_none(val: str):
    s = _as_str(val)
    if s == "":
        return None
    digits = re.sub(r"\D", "", s)
    if digits == "":
        return None
    try:
        return int(digits)
    except Exception:
        return None


def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]

    rename = {
        "info": "overskrift",
        "rekkevidde": "rekkevidde_str",
        "bildeurl": "bildeurl",
        "garanti (mnd)": "garanti_mnd",
        "forhandler type": "forhandler_type",
        "service oppgitt": "service_oppgitt",
        "service.": "service",
    }
    df = df.rename(columns=rename)

    expected = [
        "finnkode", "bilmerke", "merke", "modell", "overskrift",
        "årstall", "kjørelengde", "girkasse", "drivstoff", "rekkevidde_str",
        "pris", "selger", "fylke", "sted", "garanti_mnd", "garanti",
        "forhandler", "forhandler_type", "hjuldrift", "karosseri",
        "service", "service_oppgitt", "bildeurl",
    ]
    for c in expected:
        if c not in df.columns:
            df[c] = pd.NA

    # FinnKode -> digits
    df["finnkode"] = (
        df["finnkode"]
        .astype(str)
        .str.replace(r"\D", "", regex=True)
        .str.lstrip("0")
        .str.strip()
    )
    df = df[df["finnkode"] != ""]
    df = df.dropna(subset=["finnkode"])

    if df.empty:
        return df

    # merke fallback
    df["merke"] = df["merke"].astype(str).str.strip().replace({"": pd.NA, "nan": pd.NA})
    df["bilmerke"] = df["bilmerke"].astype(str).str.strip().replace({"": pd.NA, "nan": pd.NA})
    df["modell"] = df["modell"].astype(str).str.strip().replace({"": pd.NA, "nan": pd.NA})
    df["overskrift"] = df["overskrift"].astype(str).str.strip().replace({"": pd.NA, "nan": pd.NA})

    df.loc[df["merke"].isna() & df["bilmerke"].notna(), "merke"] = df["bilmerke"]

    # drivstoff-kvalitet
    df.loc[df["drivstoff"].astype(str).str.contains("km|rekkevidde", case=False, na=False), "drivstoff"] = pd.NA

    return df


def normalize_solgt_value(v) -> str:
    s = _as_str(v).lower()
    if s in {"ja", "yes", "true", "1"}:
        return "JA"
    if s in {"nei", "no", "false", "0"}:
        return "NEI"
    if s in {"fjernet", "removed"}:
        return "FJERNET"
    return ""


# --------------------- Berikelse av nye biler ---------------------
def extract_int(text):
    if text is None:
        return None
    digits = "".join(filter(str.isdigit, str(text)))
    return int(digits) if digits else None


def get_nested_safe(data, keys, default=None):
    current = data
    for key in keys:
        if isinstance(current, dict):
            current = current.get(key)
        elif isinstance(current, list) and isinstance(key, int):
            try:
                current = current[key]
            except IndexError:
                return default
        else:
            return default
        if current is None:
            return default
    return current if current is not None else default


def flatten_svv_data(svv_data: dict) -> dict:
    if not svv_data:
        return {}

    flat: dict[str, object] = {}

    flat["svv_regnr"] = get_nested_safe(svv_data, ["kjoretoyId", "kjennemerke"])
    flat["svv_vin"] = get_nested_safe(svv_data, ["kjoretoyId", "understellsnummer"])

    kjennemerkeliste = svv_data.get("kjennemerke", [])
    personlig_plate = None
    ordinart_plate = None
    alle_plater: list[str] = []

    if isinstance(kjennemerkeliste, list):
        for item in kjennemerkeliste:
            if not isinstance(item, dict):
                continue
            plate = item.get("kjennemerke")
            kategori = item.get("kjennemerkekategori")
            if plate:
                alle_plater.append(plate)
            if kategori == "KJORETOY" and ordinart_plate is None:
                ordinart_plate = plate
            if kategori == "PERSONLIG" and personlig_plate is None:
                personlig_plate = plate

    flat["svv_kjennemerke_ordinart"] = ordinart_plate or flat["svv_regnr"]
    flat["svv_kjennemerke_personlig"] = personlig_plate
    flat["svv_kjennemerke_alle"] = ", ".join(alle_plater) if alle_plater else None

    flat["svv_registreringsstatus"] = get_nested_safe(
        svv_data, ["registrering", "registreringsstatus", "kodeBeskrivelse"]
    )

    reg_f_norge = get_nested_safe(
        svv_data, ["forstegangsregistrering", "registrertForstegangNorgeDato"]
    )
    reg_f_generell = get_nested_safe(
        svv_data, ["godkjenning", "forstegangsGodkjenning", "forstegangRegistrertDato"]
    )

    flat["svv_registrert_forste_gang_norge"] = reg_f_norge
    flat["svv_registrert_forste_gang"] = reg_f_generell or reg_f_norge

    reg_pa_eier = get_nested_safe(
        svv_data, ["registrering", "registrertForstegangPaEierskap"]
    )
    flat["svv_registrert_pa_eier"] = reg_pa_eier[:10] if reg_pa_eier else None

    generelt = get_nested_safe(
        svv_data,
        ["godkjenning", "tekniskGodkjenning", "tekniskeData", "generelt"],
        {},
    )
    flat["svv_merke"] = get_nested_safe(generelt, ["merke", 0, "merke"])
    flat["svv_merke_kode"] = get_nested_safe(generelt, ["merke", 0, "merkeKode"])
    flat["svv_handelsbetegnelse"] = get_nested_safe(generelt, ["handelsbetegnelse", 0])
    flat["svv_typebetegnelse"] = get_nested_safe(generelt, ["typebetegnelse"])

    kjoretoyklassifisering = get_nested_safe(
        svv_data, ["godkjenning", "tekniskGodkjenning", "kjoretoyklassifisering"], {}
    )
    flat["svv_kjoretoy_typebeskrivelse"] = kjoretoyklassifisering.get("beskrivelse")
    flat["svv_kjoretoy_teknisk_kode"] = get_nested_safe(
        kjoretoyklassifisering, ["tekniskKode", "kodeVerdi"]
    )
    flat["svv_kjoretoy_avgiftskode_navn"] = get_nested_safe(
        kjoretoyklassifisering, ["kjoretoyAvgiftsKode", "kodeNavn"]
    )
    flat["svv_kjoretoy_avgiftskode_verdi"] = get_nested_safe(
        kjoretoyklassifisering, ["kjoretoyAvgiftsKode", "kodeVerdi"]
    )

    fg = get_nested_safe(svv_data, ["godkjenning", "forstegangsGodkjenning"], {})
    bruktimport = fg.get("bruktimport") or {}

    flat["svv_bruktimportert"] = bool(bruktimport)
    flat["svv_importland_navn"] = get_nested_safe(bruktimport, ["importland", "landNavn"])
    flat["svv_importland_kode"] = get_nested_safe(bruktimport, ["importland", "landkode"])
    flat["svv_import_kilometerstand"] = bruktimport.get("kilometerstand")
    flat["svv_import_tidligere_kjennemerke"] = bruktimport.get("tidligereUtenlandskKjennemerke")
    flat["svv_import_tidligere_vognkortnr"] = bruktimport.get("tidligereUtenlandskVognkortNummer")
    flat["svv_forstegang_reg_dato_utland"] = fg.get("forstegangRegistrertDato")

    fortolling = fg.get("fortollingOgMva") or {}
    flat["svv_fortolling_beskrivelse"] = fortolling.get("beskrivelse")
    flat["svv_fortolling_referanse"] = fortolling.get("fortollingsreferanse")

    dimensjoner = get_nested_safe(
        svv_data, ["godkjenning", "tekniskGodkjenning", "tekniskeData", "dimensjoner"], {}
    )
    flat["svv_lengde_mm"] = dimensjoner.get("lengde")
    flat["svv_bredde_mm"] = dimensjoner.get("bredde")
    flat["svv_hoyde_mm"] = dimensjoner.get("hoyde")

    vekter = get_nested_safe(
        svv_data, ["godkjenning", "tekniskGodkjenning", "tekniskeData", "vekter"], {}
    )
    flat["svv_egenvekt_kg"] = vekter.get("egenvekt")
    flat["svv_egenvekt_minimum_kg"] = vekter.get("egenvektMinimum")
    flat["svv_nyttelast_kg"] = vekter.get("nyttelast")
    flat["svv_tillatt_totalvekt_kg"] = vekter.get("tillattTotalvekt")
    flat["svv_tillatt_tilhenger_med_brems_kg"] = vekter.get("tillattTilhengervektMedBrems")
    flat["svv_tillatt_tilhenger_uten_brems_kg"] = vekter.get("tillattTilhengervektUtenBrems")

    motor_og_drivverk = get_nested_safe(
        svv_data,
        ["godkjenning", "tekniskGodkjenning", "tekniskeData", "motorOgDrivverk"],
        {},
    )
    motor_liste = motor_og_drivverk.get("motor") or []

    flat["svv_antall_motorer"] = len(motor_liste) if motor_liste else None
    motor0 = motor_liste[0] if motor_liste else {}

    flat["svv_slagvolum_cm3"] = motor0.get("slagvolum")
    flat["svv_antall_sylindre"] = motor0.get("antallSylindre")
    flat["svv_motor_kode"] = motor0.get("motorKode")
    flat["svv_motor_arbeidsprinsipp"] = get_nested_safe(motor0, ["arbeidsprinsipp", "kodeBeskrivelse"])

    drivstoff_liste0 = motor0.get("drivstoff") or []
    drivstoff0 = drivstoff_liste0[0] if drivstoff_liste0 else {}
    flat["svv_drivstoff_navn"] = get_nested_safe(drivstoff0, ["drivstoffKode", "kodeBeskrivelse"])
    flat["svv_maks_netto_effekt_kw"] = drivstoff0.get("maksNettoEffekt")
    flat["svv_hybridkategori"] = get_nested_safe(motor_og_drivverk, ["hybridKategori", "kodeBeskrivelse"])

    miljodata = get_nested_safe(
        svv_data, ["godkjenning", "tekniskGodkjenning", "tekniskeData", "miljodata"], {}
    )
    flat["svv_euroklasse"] = get_nested_safe(miljodata, ["euroKlasse", "kodeBeskrivelse"])

    flat["svv_elektrisk_rekkevidde_km"] = None
    grupper = miljodata.get("miljoOgdrivstoffGruppe") or []
    if isinstance(grupper, list):
        for grp in grupper:
            drivstoffkode = get_nested_safe(grp, ["drivstoffKodeMiljodata", "kodeBeskrivelse"])
            if drivstoffkode and "Elektrisk" in str(drivstoffkode):
                fu = get_nested_safe(grp, ["forbrukOgUtslipp", 0, "wltpKjoretoyspesifikk"], {})
                flat["svv_elektrisk_rekkevidde_km"] = (
                    fu.get("rekkeviddeKmBlandetkjoring") or fu.get("nedcRekkeviddeKm")
                )
                break

    forbruk = get_nested_safe(miljodata, ["miljoOgdrivstoffGruppe", 0, "forbrukOgUtslipp", 0], {})
    wltp = forbruk.get("wltpKjoretoyspesifikk") or {}
    flat["svv_forbruk_blandet_l_100km"] = (
        wltp.get("forbrukVektetKombinert") or forbruk.get("forbrukBlandetKjoring")
    )
    flat["svv_co2_blandet_g_km"] = (
        wltp.get("co2VektetKombinert") or forbruk.get("co2BlandetKjoring")
    )

    persontall = get_nested_safe(
        svv_data, ["godkjenning", "tekniskGodkjenning", "tekniskeData", "persontall"], {}
    )
    flat["svv_sitteplasser_totalt"] = persontall.get("sitteplasserTotalt")
    flat["svv_sitteplasser_foran"] = persontall.get("sitteplasserForan")

    kontroll = svv_data.get("periodiskKjoretoyKontroll") or {}
    flat["svv_kontrollfrist"] = kontroll.get("kontrollfrist")
    flat["svv_sist_eu_godkjent"] = kontroll.get("sistGodkjent")

    akslinger = get_nested_safe(
        svv_data, ["godkjenning", "tekniskGodkjenning", "tekniskeData", "akslinger"], {}
    )
    flat["svv_antall_aksler"] = akslinger.get("antallAksler")

    antall_med_drift = 0
    akselgrupper = akslinger.get("akselGruppe") or []
    if isinstance(akselgrupper, list):
        for grp in akselgrupper:
            akselliste = get_nested_safe(grp, ["akselListe", "aksel"], [])
            for aksel in akselliste or []:
                if isinstance(aksel, dict) and aksel.get("drivAksel"):
                    antall_med_drift += 1
    flat["svv_antall_aksler_med_drift"] = antall_med_drift or None

    dekk_kombinasjoner = get_nested_safe(
        svv_data,
        ["godkjenning", "tekniskGodkjenning", "tekniskeData", "dekkOgFelg", "akselDekkOgFelgKombinasjon"],
        [],
    )

    standard_foran = None
    standard_bak = None
    alle_kombinasjoner_tekst: list[str] = []
    unike_dekkdimensjoner: set[str] = set()

    if isinstance(dekk_kombinasjoner, list):
        for idx, komb in enumerate(dekk_kombinasjoner, start=1):
            aksel_liste = get_nested_safe(komb, ["akselDekkOgFelg"], [])
            aksel_tekster: list[str] = []
            if not isinstance(aksel_liste, list):
                continue
            for aksel in aksel_liste:
                if not isinstance(aksel, dict):
                    continue
                aksel_id = aksel.get("akselId")
                dekkdim = aksel.get("dekkdimensjon")
                felgdim = aksel.get("felgdimensjon")
                hast = aksel.get("hastighetskodeDekk")
                belast = aksel.get("belastningskodeDekk")
                innpress = aksel.get("innpress")
                aksel_tekst = f"aksel {aksel_id}: {dekkdim} {felgdim} {belast}{hast}, ET{innpress}"
                aksel_tekster.append(aksel_tekst)
                if dekkdim:
                    unike_dekkdimensjoner.add(str(dekkdim).strip())
                if idx == 1:
                    combo_str = f"{dekkdim} {felgdim} {belast}{hast}, ET{innpress}"
                    if aksel_id == 1 and standard_foran is None:
                        standard_foran = combo_str
                    if aksel_id == 2 and standard_bak is None:
                        standard_bak = combo_str
            if aksel_tekster:
                alle_kombinasjoner_tekst.append(f"Komb {idx}: " + " | ".join(aksel_tekster))

    flat["svv_dekk_standard_foran"] = standard_foran
    flat["svv_dekk_standard_bak"] = standard_bak
    flat["svv_dekkdimensjoner_unike"] = ", ".join(sorted(unike_dekkdimensjoner)) if unike_dekkdimensjoner else None
    flat["svv_dekk_alle_kombinasjoner"] = "; ".join(alle_kombinasjoner_tekst) if alle_kombinasjoner_tekst else None

    return flat


def hent_detaljert_info_fra_annonse(bil_info: dict, session_req: requests.Session) -> dict:
    try:
        response = session_req.get(bil_info["url"], headers=FINN_HEADERS, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "html.parser")

        totalpris_p = soup.find("p", string="Totalpris")
        if (
            totalpris_p
            and totalpris_p.find_next_sibling("h2")
            and totalpris_p.find_next_sibling("h2").find("span")
        ):
            pris_tekst = totalpris_p.find_next_sibling("h2").find("span").get_text()
            bil_info["pris"] = extract_int(pris_tekst) or 0

        seller_name = None
        seller_email = None
        seller_phone = None

        try:
            data_script = soup.find("script", {"data-company-profile-data": True})
            if data_script and data_script.string:
                json_data = json.loads(data_script.string)
                seller_name = get_nested_safe(json_data, ["companyProfile", "orgName"], None)
                seller_email = (
                    get_nested_safe(json_data, ["companyProfile", "contact", "email"], None)
                    or get_nested_safe(json_data, ["contact", "email"], None)
                )
                phones = get_nested_safe(json_data, ["contact", "phonesWithLabels"], [])
                if isinstance(phones, list):
                    for p in phones:
                        if isinstance(p, dict) and p.get("phone"):
                            seller_phone = p["phone"]
                            break
        except (json.JSONDecodeError, TypeError):
            pass

        if not seller_name or seller_name == "Mangler":
            private_seller_div = soup.find("div", {"data-testid": "private-seller-name"})
            if private_seller_div:
                seller_name = private_seller_div.get_text(strip=True)

        if seller_name and seller_name != "Mangler":
            bil_info["selger"] = " ".join(str(seller_name).split())
        if seller_email:
            bil_info["kontakt_email"] = seller_email
        if seller_phone:
            bil_info["kontakt_telefon"] = seller_phone

        spec_header = soup.find(
            lambda tag: tag.name in ["h2", "h3"] and "Spesifikasjoner" in tag.get_text()
        )
        if spec_header and spec_header.find_next_sibling("dl"):
            spec_list = spec_header.find_next_sibling("dl")
            reg_nr, chassis_nr = None, None

            for dt in spec_list.find_all("dt"):
                dd = dt.find_next_sibling("dd")
                if not dd:
                    continue

                key = dt.get_text(strip=True)
                val = dd.get_text(strip=True)

                if "Omregistrering" in key:
                    bil_info["omregistrering"] = extract_int(val)
                elif "Pris eksl. omreg." in key:
                    bil_info["pris_eks_omreg"] = extract_int(val)
                elif "Årsavgift" in key:
                    bil_info["årsavgift"] = val
                elif key == "Merke":
                    bil_info["merke"] = val
                elif key == "Modell":
                    bil_info["modell"] = val
                elif key == "Modellår":
                    bil_info["modellår"] = extract_int(val) or val
                elif key == "Karosseri":
                    bil_info["karosseri"] = val
                elif key == "Drivstoff":
                    bil_info["drivstoff"] = val
                elif key == "Girkasse":
                    bil_info["girkasse"] = val
                elif key == "Hjuldrift":
                    bil_info["hjuldrift"] = val
                elif key == "Vekt":
                    bil_info["vekt_kg"] = extract_int(val) or val
                elif key == "Seter":
                    bil_info["seter"] = extract_int(val) or val
                elif key == "Dører":
                    bil_info["dører"] = extract_int(val) or val
                elif "Størrelse på bagasjerom" in key:
                    bil_info["bagasjerom_l"] = extract_int(val) or val
                elif key == "Farge":
                    bil_info["farge"] = val
                elif key == "Bilen står i":
                    bil_info["bilen_staar_i"] = val
                elif "Neste frist for EU-kontroll" in key:
                    bil_info["eu_frist"] = val
                elif key == "Avgiftsklasse":
                    bil_info["avgiftsklasse"] = val
                elif "Registreringsnummer" in key or "Registreringsnr." in key:
                    reg_nr = val
                    bil_info["regnr"] = val
                elif "Chassis nr. (VIN)" in key:
                    chassis_nr = val
                    bil_info["vin"] = val
                elif "1. gang registrert" in key:
                    bil_info["reg_norge"] = val
                elif "Eiere" in key:
                    bil_info["eiere_antall"] = extract_int(val) or val

                if "Kilometerstand" in key:
                    bil_info["km"] = extract_int(val) or 0
                elif "Effekt" in key:
                    bil_info["effekt_hk"] = extract_int(val)
                elif "Batterikapasitet" in key:
                    bil_info["batterikapasitet_kwh"] = extract_int(val)
                elif "Rekkevidde (WLTP" in key:
                    bil_info["rekkevidde_wltp_km"] = extract_int(val)

            bil_info["identifikator"] = reg_nr if reg_nr else chassis_nr

        return bil_info

    except requests.RequestException:
        return bil_info


def hent_kjoretoydata_svv(identifikator: str, session_req: requests.Session):
    global _SVV_API_WARNING_PRINTED

    if not identifikator:
        return None
    if not SVV_API_KEY:
        if not _SVV_API_WARNING_PRINTED:
            print("[Berikelse] ADVARSEL: SVV_API_KEY mangler. Hopper over SVV-oppslag.")
            _SVV_API_WARNING_PRINTED = True
        return None

    identifikator_clean = str(identifikator).strip().upper().replace(" ", "")
    query_param = (
        "kjennemerke"
        if len(identifikator_clean) == 7 and identifikator_clean[:2].isalpha()
        else "understellsnummer"
    )
    url = (
        "https://www.vegvesen.no/ws/no/vegvesen/kjoretoy/felles/datautlevering/"
        f"enkeltoppslag/kjoretoydata?{query_param}={identifikator_clean}"
    )
    headers = {"SVV-Authorization": f"Apikey {SVV_API_KEY}"}
    try:
        response = session_req.get(url, headers=headers, timeout=15)
        if response.status_code == 200:
            j = response.json()
            if j and j.get("kjoretoydataListe"):
                return j["kjoretoydataListe"][0]
    except requests.RequestException:
        pass
    except ValueError:
        pass
    return None


def _build_enrichment_record(raw: dict) -> dict:
    record = {
        "FinnKode": _as_int(raw.get("FinnKode")),
        "url": raw.get("url"),
        "Produsent": raw.get("merke"),
        "Modell": raw.get("modell"),
        "årstall": raw.get("modellår"),
        "Karosseri": raw.get("karosseri"),
        "drivstoff": raw.get("drivstoff"),
        "girkasse": raw.get("girkasse"),
        "hjuldrift": raw.get("hjuldrift"),
        "kjørelengde": raw.get("km"),
        "selger": raw.get("selger"),
        "Pris": raw.get("pris"),
        "Pris_ny": raw.get("pris"),
        "seter": raw.get("seter") or raw.get("svv_sitteplasser_totalt"),
        "farge": raw.get("farge"),
        "regnr": raw.get("regnr") or raw.get("svv_regnr") or raw.get("svv_kjennemerke_ordinart"),
        "reg_norge": raw.get("reg_norge") or raw.get("svv_registrert_forste_gang_norge"),
        "batterikapasitet_kwh": raw.get("batterikapasitet_kwh"),
        "rekkevidde_wltp_km": raw.get("rekkevidde_wltp_km") or raw.get("svv_elektrisk_rekkevidde_km"),
        "svv_vin": raw.get("svv_vin") or raw.get("vin"),
        "svv_kjennemerke_personlig": raw.get("svv_kjennemerke_personlig"),
        "svv_registrert_forste_gang_norge": raw.get("svv_registrert_forste_gang_norge"),
        "svv_registrert_pa_eier": raw.get("svv_registrert_pa_eier"),
        "svv_bruktimportert": raw.get("svv_bruktimportert"),
        "svv_importland_navn": raw.get("svv_importland_navn"),
        "svv_sitteplasser_totalt": raw.get("svv_sitteplasser_totalt"),
        "svv_antall_aksler_med_drift": raw.get("svv_antall_aksler_med_drift"),
    }
    for field in ENRICHMENT_OUTPUT_FIELDS:
        if field not in record:
            record[field] = raw.get(field)
    return record


def _value_missing_mask(series: pd.Series, column_name: str | None = None) -> pd.Series:
    if series.dtype == bool:
        return series.isna()

    if pd.api.types.is_object_dtype(series) or pd.api.types.is_string_dtype(series):
        s = series.astype("string").str.strip()
        mask = s.isna() | s.str.lower().isin(NULL_LIKE_STRINGS)
        if column_name in ZERO_IS_MISSING_COLUMNS:
            num = pd.to_numeric(s, errors="coerce")
            mask = mask | num.fillna(0).le(0)
        return mask

    mask = series.isna()
    if column_name in ZERO_IS_MISSING_COLUMNS:
        num = pd.to_numeric(series, errors="coerce")
        mask = mask | num.fillna(0).le(0)
    return mask


def _ensure_columns(df: pd.DataFrame, columns: list[str] | set[str]):
    for col in columns:
        if col not in df.columns:
            df[col] = pd.NA


def _to_nullable_bool(v):
    if pd.isna(v):
        return pd.NA
    if isinstance(v, (bool, np.bool_)):
        return bool(v)
    s = str(v).strip().lower()
    if s in {"", "nan", "none", "<na>", "null"}:
        return pd.NA
    if s in {"true", "1", "ja", "yes"}:
        return True
    if s in {"false", "0", "nei", "no"}:
        return False
    return pd.NA


BOOLEAN_COLUMNS = {"svv_bruktimportert"}


def _coerce_incoming_for_target(result: pd.DataFrame, target: str, source_col: str) -> pd.Series:
    src = result[source_col]

    if target in BOOLEAN_COLUMNS:
        if target in result.columns:
            result[target] = result[target].map(_to_nullable_bool).astype("boolean")
        return src.map(_to_nullable_bool).astype("boolean")

    target_series = result[target]

    if pd.api.types.is_string_dtype(target_series):
        vals = src.astype("string")
        vals = vals.str.strip()
        vals = vals.mask(vals.str.lower().isin(NULL_LIKE_STRINGS), pd.NA)
        return vals

    if pd.api.types.is_integer_dtype(target_series):
        vals = pd.to_numeric(src, errors="coerce")
        non_na = vals.dropna()
        if not non_na.empty and ((non_na % 1) != 0).any():
            vals = vals.round()
        return vals.astype("Int64")

    if pd.api.types.is_float_dtype(target_series):
        return pd.to_numeric(src, errors="coerce").astype("Float64")

    return src


def enrich_new_rows_from_finn_and_svv(new_rows: pd.DataFrame) -> pd.DataFrame:
    if new_rows is None or new_rows.empty or "FinnKode" not in new_rows.columns:
        return new_rows

    result = new_rows.copy()
    result["FinnKode"] = pd.to_numeric(result["FinnKode"], errors="coerce")
    result = result[result["FinnKode"].notna()].copy()
    if result.empty:
        return result
    result["FinnKode"] = result["FinnKode"].astype("int64")

    finnkoder = [str(int(x)) for x in result["FinnKode"].dropna().unique().tolist()]
    to_fetch = [fk for fk in finnkoder if fk not in _ENRICH_CACHE_BY_FINNKODE]

    if to_fetch:
        print(f"  [Berikelse] Henter FINN/SVV-data for {len(to_fetch)} nye biler ...")
        biler = [{"FinnKode": fk, "url": FINN_BASE_URL.format(fk)} for fk in to_fetch]

        biler_med_detaljer: list[dict] = []
        with requests.Session() as s, ThreadPoolExecutor(max_workers=MAX_WORKERS_FINN) as executor:
            futures = {executor.submit(hent_detaljert_info_fra_annonse, bil, s): bil for bil in biler}
            for fut in as_completed(futures):
                biler_med_detaljer.append(fut.result())

        identifikatorer = sorted(
            {
                str(b.get("identifikator")).strip()
                for b in biler_med_detaljer
                if b.get("identifikator") and str(b.get("identifikator")).strip() not in _SVV_CACHE_BY_IDENT
            }
        )

        if identifikatorer:
            with requests.Session() as s, ThreadPoolExecutor(max_workers=MAX_WORKERS_SVV) as executor:
                futures = {executor.submit(hent_kjoretoydata_svv, ident, s): ident for ident in identifikatorer}
                for fut in as_completed(futures):
                    ident = futures[fut]
                    try:
                        _SVV_CACHE_BY_IDENT[ident] = fut.result()
                    except Exception:
                        _SVV_CACHE_BY_IDENT[ident] = None

        for bil in biler_med_detaljer:
            ident = bil.get("identifikator")
            if ident:
                bil.update(flatten_svv_data(_SVV_CACHE_BY_IDENT.get(str(ident).strip())))
            rec = _build_enrichment_record(bil)
            fk = rec.get("FinnKode")
            if fk is not None:
                _ENRICH_CACHE_BY_FINNKODE[str(int(fk))] = rec

    enrich_records = []
    for fk in finnkoder:
        rec = _ENRICH_CACHE_BY_FINNKODE.get(fk, {"FinnKode": int(fk)})
        enrich_records.append(rec)

    if not enrich_records:
        return result

    enrich_df = pd.DataFrame(enrich_records).drop_duplicates(subset="FinnKode", keep="last")
    overlap_cols = [c for c in enrich_df.columns if c != "FinnKode" and c in result.columns]
    rename_overlap = {c: f"{c}__enrich" for c in overlap_cols}
    enrich_df = enrich_df.rename(columns=rename_overlap)

    result = result.merge(enrich_df, on="FinnKode", how="left")

    for target, _sources in ENRICHMENT_FILL_TARGETS.items():
        _ensure_columns(result, [target])
        current_missing = _value_missing_mask(result[target], target)
        for source in _sources:
            source_col = rename_overlap.get(source, source)
            if source_col not in result.columns:
                continue
            incoming_vals = _coerce_incoming_for_target(result, target, source_col)
            incoming_missing = _value_missing_mask(incoming_vals, source)
            fill_mask = current_missing & ~incoming_missing
            if fill_mask.any():
                result.loc[fill_mask, target] = incoming_vals.loc[fill_mask]
                current_missing = _value_missing_mask(result[target], target)

    # Fjern hjelpekolonner som kun ble brukt til fill
    helper_cols = [c for c in result.columns if c.endswith("__enrich")]
    if helper_cols:
        result = result.drop(columns=helper_cols)

    enriched_hits = int(
        pd.Series(finnkoder).isin(pd.Series(list(_ENRICH_CACHE_BY_FINNKODE.keys()))).sum()
    )
    print(f"  [Berikelse] Ferdig for {enriched_hits}/{len(finnkoder)} nye biler")
    return result



# --------------------- Vektorisert merge ---------------------

# Kolonner som kun daily kan overskrive eksisterende verdier
DAILY_OVERWRITE_COLS = ["hjuldrift", "Karosseri", "fylke", "forhandler"]

# Alle felt som fylles inn (fill-only — ikke overskriv)
FILL_COLS = [
    "Produsent", "Modell", "Overskrift", "årstall", "kjørelengde",
    "drivstoff", "girkasse", "rekkevidde_str", "selger", "sted",
    "garanti_mnd", "forhandler_type", "BildeURL",
]

# Mapping fra CSV-kolonne til master-kolonne
CSV_TO_MASTER = {
    "merke": "Produsent",
    "modell": "Modell",
    "overskrift": "Overskrift",
    "årstall": "årstall",
    "kjørelengde": "kjørelengde",
    "drivstoff": "drivstoff",
    "girkasse": "girkasse",
    "hjuldrift": "hjuldrift",
    "rekkevidde_str": "rekkevidde_str",
    "karosseri": "Karosseri",
    "selger": "selger",
    "fylke": "fylke",
    "sted": "sted",
    "garanti_mnd": "garanti_mnd",
    "garanti": "garanti_mnd",   # fallback
    "forhandler": "forhandler",
    "forhandler_type": "forhandler_type",
    "bildeurl": "BildeURL",
}


def merge_snapshot_into_master(master_df: pd.DataFrame, snap_df: pd.DataFrame,
                                observed_at: datetime, source: str) -> pd.DataFrame:
    """
    Vektorisert merge av ett snapshot inn i master_df.
    Erstatter den gamle Python-løkken over alle rader.
    """
    snap = snap_df.copy()

    # FinnKode som int64
    snap["finnkode"] = pd.to_numeric(snap["finnkode"], errors="coerce")
    snap = snap[snap["finnkode"].notna()].copy()
    snap["finnkode"] = snap["finnkode"].astype("int64")
    snap = snap.drop_duplicates(subset="finnkode", keep="last")

    if snap.empty:
        return master_df

    # Pris-parsing
    pris_raw = snap["pris"].fillna("").astype(str)
    snap["_explicit_sold"] = pris_raw.str.lower().str.contains("solgt")
    snap["_valid_price"] = (
        pris_raw.str.replace(r"\D", "", regex=True)
               .replace("", np.nan)
               .pipe(pd.to_numeric, errors="coerce")
    )
    snap["_valid_price"] = snap["_valid_price"].where(snap["_valid_price"] > 0)

    # Rename CSV-kolonner til master-navn
    snap = snap.rename(columns=CSV_TO_MASTER)
    # Behold kun relevante kolonner
    keep = ["finnkode", "_explicit_sold", "_valid_price"] + [
        c for c in CSV_TO_MASTER.values() if c in snap.columns
    ]
    snap = snap[[c for c in keep if c in snap.columns]]
    snap = snap.rename(columns={"finnkode": "FinnKode"})

    # ---- Del opp i nye og eksisterende ----
    existing_fks = set(master_df["FinnKode"].values) if not master_df.empty else set()
    is_new = ~snap["FinnKode"].isin(existing_fks)
    new_snap = snap[is_new].copy()
    upd_snap = snap[~is_new].copy()

    # new_rows samles opp og legges til master ETTER alle oppdateringer
    new_rows = None

    # ---- NYE BILER ----
    if not new_snap.empty:
        new_rows = new_snap.copy()
        new_rows["Dato"] = observed_at
        new_rows["Dato_ny"] = observed_at
        new_rows["Pris"] = new_rows["_valid_price"].fillna(0).astype("int64")
        new_rows["Pris_ny"] = new_rows["Pris"]
        new_rows["Solgt"] = np.where(new_rows["_explicit_sold"], "JA", "NEI")
        new_rows = new_rows.drop(columns=["_explicit_sold", "_valid_price"], errors="ignore")

        # Berik KUN biler som ikke finnes i parquet fra før.
        new_rows = enrich_new_rows_from_finn_and_svv(new_rows)

        print(f"  -> {len(new_rows)} nye biler")

    # ---- EKSISTERENDE BILER ----
    # Viktig: ALDRI bruk FinnKode som DataFrame-index under oppdateringer.
    # Pandas .loc med ikke-unik index introduserer duplikatrader og kolonne-duplikater
    # som downstream pd.concat() feiler på. Bruk i stedet boolean masker + .map() via
    # en Series(FinnKode -> verdi) for alle oppslag og tilordninger.
    if not upd_snap.empty and not master_df.empty:
        # Alltid dedupliser begge sider mot FinnKode
        master_df = master_df.drop_duplicates(subset="FinnKode", keep="last").copy()
        upd_snap  = upd_snap.drop_duplicates(subset="FinnKode", keep="last").copy()

        _ensure_columns(
            master_df,
            set(FILL_COLS + DAILY_OVERWRITE_COLS + list(ENRICHMENT_FILL_TARGETS.keys()) + ["Pris", "Pris_ny", "Solgt", "Dato", "Dato_ny"])
        )

        upd_fk_set = set(upd_snap["FinnKode"].values)

        # Oppdater Dato_ny for alle oppdaterte biler
        master_df.loc[master_df["FinnKode"].isin(upd_fk_set), "Dato_ny"] = observed_at

        # Explicit sold
        sold_fk_set = set(upd_snap.loc[upd_snap["_explicit_sold"], "FinnKode"].values)
        if sold_fk_set:
            master_df.loc[master_df["FinnKode"].isin(sold_fk_set), "Solgt"] = "JA"

        # Resurrection: bil som var solgt dukker opp i daily uten solgt-signal
        if source == "daily":
            not_sold_fk_set = upd_fk_set - sold_fk_set
            resurrection_mask = (
                master_df["FinnKode"].isin(not_sold_fk_set) &
                (master_df["Solgt"] == "JA")
            )
            master_df.loc[resurrection_mask, "Solgt"] = "NEI"

        # Prisoppdatering (kun for ikke-solgte i dette snapshot)
        active_upd = upd_snap[~upd_snap["_explicit_sold"]][["FinnKode", "_valid_price"]].dropna(subset=["_valid_price"])
        if not active_upd.empty:
            # Series: FinnKode -> ny_pris (ingen duplikater etter drop_duplicates over)
            price_map = active_upd.set_index("FinnKode")["_valid_price"].astype("int64")
            upd_price_mask = master_df["FinnKode"].isin(price_map.index)

            # Pris_ny = siste gyldige pris
            master_df.loc[upd_price_mask, "Pris_ny"] = (
                master_df.loc[upd_price_mask, "FinnKode"].map(price_map)
            )

            # Pris (startpris) = kun sett hvis 0 eller mangler
            zero_start_mask = upd_price_mask & (
                master_df["Pris"].fillna(0).astype("int64") <= 0
            )
            master_df.loc[zero_start_mask, "Pris"] = (
                master_df.loc[zero_start_mask, "FinnKode"].map(price_map)
            )


        # Fill-only kolonner (ikke overskriv eksisterende verdier)
        # KRITISK: cast til master-kolonnas dtype for å unngå FutureWarning som
        # korruperer pandas' interne block-struktur og skaper duplikat-kolonner.
        fill_cols_present = [c for c in FILL_COLS if c in upd_snap.columns]
        if fill_cols_present:
            for col in fill_cols_present:
                col_map = (
                    upd_snap.set_index("FinnKode")[col]
                    .replace("", np.nan)
                    .dropna()
                )
                if col_map.empty:
                    continue
                fill_mask = (
                    master_df["FinnKode"].isin(col_map.index) &
                    (master_df[col].isna() | (master_df[col].astype(str).str.strip() == ""))
                )
                if not fill_mask.any():
                    continue
                mapped_vals = master_df.loc[fill_mask, "FinnKode"].map(col_map)
                # Cast til eksisterende dtype for å unngå block-korrupsjon
                target_dtype = master_df[col].dtype
                try:
                    if pd.api.types.is_integer_dtype(target_dtype):
                        mapped_vals = pd.to_numeric(mapped_vals, errors="coerce").fillna(0).astype(target_dtype)
                    elif pd.api.types.is_float_dtype(target_dtype):
                        mapped_vals = pd.to_numeric(mapped_vals, errors="coerce").astype(target_dtype)
                except Exception:
                    pass
                master_df.loc[fill_mask, col] = mapped_vals

        # Daily-overwrite kolonner
        if source == "daily":
            for col in DAILY_OVERWRITE_COLS:
                if col not in upd_snap.columns:
                    continue
                col_map = (
                    upd_snap.set_index("FinnKode")[col]
                    .replace("", np.nan)
                    .dropna()
                )
                if col_map.empty:
                    continue
                ow_mask = master_df["FinnKode"].isin(col_map.index)
                if not ow_mask.any():
                    continue
                mapped_vals = master_df.loc[ow_mask, "FinnKode"].map(col_map)
                target_dtype = master_df[col].dtype
                try:
                    if pd.api.types.is_integer_dtype(target_dtype):
                        mapped_vals = pd.to_numeric(mapped_vals, errors="coerce").fillna(0).astype(target_dtype)
                    elif pd.api.types.is_float_dtype(target_dtype):
                        mapped_vals = pd.to_numeric(mapped_vals, errors="coerce").astype(target_dtype)
                except Exception:
                    pass
                master_df.loc[ow_mask, col] = mapped_vals

        print(f"  -> {len(upd_snap)} oppdaterte biler")

    # ---- Bygg resultat: legg nye rader til oppdatert master ----
    # Sikre at master_df ikke har duplikat-kolonner (kan oppstå fra pandas block-korrupsjon)
    if master_df.columns.duplicated().any():
        master_df = master_df.loc[:, ~master_df.columns.duplicated(keep="first")]

    if new_rows is not None and not new_rows.empty:
        # Sikre konsistente kolonner mellom master og nye rader
        if new_rows.columns.duplicated().any():
            new_rows = new_rows.loc[:, ~new_rows.columns.duplicated(keep="first")]
        if master_df.empty:
            result = new_rows.reset_index(drop=True)
        else:
            # Unngå FutureWarning fra pandas når en av sidene kun har tomme/NA-kolonner.
            all_cols = list(dict.fromkeys(list(master_df.columns) + list(new_rows.columns)))
            master_aligned = master_df.reindex(columns=all_cols)
            new_aligned = new_rows.reindex(columns=all_cols)
            result = pd.concat([master_aligned, new_aligned], ignore_index=True)
            del master_aligned, new_aligned
            gc.collect()
    else:
        result = master_df

    return result


# --------------------- main ---------------------
def trigger_bilradar_scoring():
    """Ber GitHub Actions score aktive biler paa nytt (bilradar_aktive.parquet)
    med den samlede lookup/variant-motoren, etter at database_biler.parquet er
    oppdatert. Krever en PAT i GITHUB_DISPATCH_TOKEN (classic: repo-scope, eller
    fine-grained: Contents=write). Ufarlig aa hoppe over hvis token mangler."""
    if not GITHUB_DISPATCH_TOKEN:
        print("[dispatch] GITHUB_DISPATCH_TOKEN mangler - hopper over scoring-trigger")
        return
    try:
        resp = requests.post(
            f"https://api.github.com/repos/{GITHUB_REPO}/dispatches",
            headers={
                "Authorization": f"Bearer {GITHUB_DISPATCH_TOKEN}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
            },
            json={"event_type": "database-updated"},
            timeout=15,
        )
        if resp.status_code == 204:
            print("[dispatch] Bilradar-scoring trigget (event_type=database-updated)")
        else:
            print(f"[dispatch] Uventet svar fra GitHub: {resp.status_code} {resp.text[:200]}")
    except requests.RequestException as e:
        print(f"[dispatch] Klarte ikke trigge scoring: {e}")


def oppdater_database():
    s3 = boto3.client("s3")

    # 1) last existing
    existing_df = try_load_existing_master(s3)
    state = try_load_state(s3)

    if existing_df is None or existing_df.empty:
        print("Fant ingen eksisterende database – bygger fra scratch.")
        master_df = pd.DataFrame()
        last_processed = datetime(1900, 1, 1, 0, 0, 0)
    else:
        print(f"Laster eksisterende database ({len(existing_df)} rader).")
        if "Solgt" not in existing_df.columns:
            existing_df["Solgt"] = "NEI"
        # Normaliser Solgt-verdier vektorisert (erstattet to_dict-løkken)
        existing_df["Solgt"] = existing_df["Solgt"].apply(
            lambda x: normalize_solgt_value(x) or "NEI"
        )
        master_df = existing_df

        lp = state.get("last_processed_observed_at")
        if lp:
            try:
                last_processed = datetime.fromisoformat(lp)
            except Exception:
                last_processed = (
                    existing_df["Dato_ny"].max().to_pydatetime()
                    if "Dato_ny" in existing_df.columns else datetime(1900, 1, 1)
                )
        else:
            last_processed = (
                existing_df["Dato_ny"].max().to_pydatetime()
                if "Dato_ny" in existing_df.columns else datetime(1900, 1, 1)
            )

    today = date.today()

    # 2) list objects in both prefixes
    candidates = []
    for prefix in (S3_DAILY_PREFIX, S3_HOURLY_PREFIX):
        for key in s3_list_csv_objects(s3, prefix):
            parsed = parse_observed_at_from_key(key)
            if not parsed:
                continue
            observed_at, source = parsed
            if observed_at.date() < START_DATE or observed_at.date() > today:
                continue
            candidates.append((observed_at, source, key))

    if not candidates:
        print("Fant ingen filer å vurdere. Avslutter.")
        return

    # 3) filter to new files
    new_files = [(t, src, k) for (t, src, k) in candidates if t > last_processed]
    new_files.sort(key=lambda x: x[0])

    # Prosesser alle hourly fram til og med foerste daily, saa stopp.
    # Daily-snapshots har tusenvis av nye biler; en av gangen holder minnet nede.
    kapp = len(new_files)
    for _i, (_t, _src, _k) in enumerate(new_files):
        if _src == "daily":
            kapp = _i + 1
            break
    if kapp < len(new_files):
        print(f"Prosesserer {kapp} av {len(new_files)} filer (stopper etter foerste daily).")
        new_files = new_files[:kapp]

    if not new_files:
        print(f"Ingen nye filer etter {last_processed.isoformat()}. Avslutter.")
        return

    daily_count = sum(1 for _, src, _ in new_files if src == "daily")
    hourly_count = sum(1 for _, src, _ in new_files if src == "hourly")
    print(f"Skal prosessere {len(new_files)} nye filer "
          f"({daily_count} daily, {hourly_count} hourly) "
          f"etter {last_processed.isoformat()}.")

    max_processed = last_processed

    # 4) process in chronological order
    for observed_at, source, key in new_files:
        print(f"Leser {source}: {key} (observed_at={observed_at})")
        try:
            df = s3_read_csv_utf16_semicolon(s3, key)
        except Exception as e:
            print(f"  FEIL ved lesing: {e}")
            continue

        if df.empty:
            print("  Tom fil, hopper over.")
            continue

        df = normalize_df(df)
        if df.empty:
            print("  Ingen gyldige rader, hopper over.")
            continue

        # Vektorisert merge (erstatter Python-løkken)
        master_df = merge_snapshot_into_master(master_df, df, observed_at, source)
        del df
        gc.collect()

        if observed_at > max_processed:
            max_processed = observed_at

    # 5) bygg dataframe og fiks dtypes
    if master_df is None or master_df.empty:
        print("Ingen biler funnet. Avslutter.")
        return

    # ENFORCE FinnKode int64
    if "FinnKode" in master_df.columns:
        master_df["FinnKode"] = pd.to_numeric(master_df["FinnKode"], errors="coerce")
        bad = int(master_df["FinnKode"].isna().sum())
        if bad:
            print(f"ADVARSEL: dropper {bad} rader med ugyldig FinnKode før lagring")
            master_df = master_df[master_df["FinnKode"].notna()].copy()
        master_df["FinnKode"] = master_df["FinnKode"].astype("int64")

    # fix dtypes – dates
    for c in ["Dato", "Dato_ny"]:
        if c in master_df.columns:
            master_df[c] = pd.to_datetime(master_df[c], errors="coerce")

    # fix dtypes – ints

    for c in ["årstall", "kjørelengde", "rekkevidde_str", "Pris", "Pris_ny"]:
        if c in master_df.columns:
            master_df[c] = pd.to_numeric(master_df[c], errors="coerce").fillna(0).astype("int64")

    nullable_int_cols = [
        "seter",
        "omregistrering",
        "pris_eks_omreg",
        "vekt_kg",
        "dører",
        "bagasjerom_l",
        "eiere_antall",
        "effekt_hk",
        "rekkevidde_wltp_km",
        "svv_import_kilometerstand",
        "svv_lengde_mm",
        "svv_bredde_mm",
        "svv_hoyde_mm",
        "svv_egenvekt_kg",
        "svv_egenvekt_minimum_kg",
        "svv_nyttelast_kg",
        "svv_tillatt_totalvekt_kg",
        "svv_tillatt_tilhenger_med_brems_kg",
        "svv_tillatt_tilhenger_uten_brems_kg",
        "svv_antall_motorer",
        "svv_slagvolum_cm3",
        "svv_antall_sylindre",
        "svv_elektrisk_rekkevidde_km",
        "svv_sitteplasser_totalt",
        "svv_sitteplasser_foran",
        "svv_antall_aksler",
        "svv_antall_aksler_med_drift",
    ]
    for c in nullable_int_cols:
        if c in master_df.columns:
            ser = pd.to_numeric(master_df[c], errors="coerce")
            # Nullable Int64 tåler ikke desimalverdier. Hvis en kilde leverer 12.5 e.l.,
            # rundes verdien til nærmeste heltall i stedet for å krasje hele kjøringen.
            non_na = ser.dropna()
            if not non_na.empty and ((non_na % 1) != 0).any():
                print(f"ADVARSEL: {c} inneholder desimaler; runder til nærmeste heltall før lagring")
                ser = ser.round()
            master_df[c] = ser.astype("Int64")

    for c in ["batterikapasitet_kwh", "svv_maks_netto_effekt_kw", "svv_forbruk_blandet_l_100km", "svv_co2_blandet_g_km"]:
        if c in master_df.columns:
            master_df[c] = pd.to_numeric(master_df[c], errors="coerce").astype("Float64")

    # fix dtypes – booleans
    for c in ["svv_bruktimportert"]:
        if c in master_df.columns:
            master_df[c] = master_df[c].map(_to_nullable_bool).astype("boolean")

    # 6) compute Solgt status vektorisert (erstatter apply(status_row))
    if "Dato_ny" in master_df.columns and master_df["Dato_ny"].notna().any():
        latest_observed_date = master_df["Dato_ny"].max().date()
        diff_days = (
            latest_observed_date - master_df["Dato_ny"].dt.date
        ).apply(lambda d: d.days if pd.notna(d) else 999)

        solgt_norm = master_df["Solgt"].apply(normalize_solgt_value)
        master_df["Solgt"] = np.select(
            [
                solgt_norm == "JA",
                diff_days >= 2,
            ],
            ["JA", "FJERNET"],
            default="NEI",
        )
    else:
        master_df["Solgt"] = master_df["Solgt"].apply(
            lambda x: normalize_solgt_value(x) or "NEI"
        )

    # Oppsummering
    total = len(master_df)
    n_active = (master_df["Solgt"] == "NEI").sum()
    n_sold = (master_df["Solgt"] == "JA").sum()
    n_removed = (master_df["Solgt"] == "FJERNET").sum()
    print(f"\nDatabase-oppsummering:")
    print(f"  Totalt:          {total}")
    print(f"  Aktive (NEI):    {n_active}")
    print(f"  Solgt (JA):      {n_sold}")
    print(f"  Fjernet:         {n_removed}")

    CATEGORICAL_COLS = [
        "Produsent", "Modell", "drivstoff", "hjuldrift", "Karosseri",
        "girkasse", "fylke", "sted", "selger", "forhandler", "forhandler_type",
        "Solgt", "farge",
        "svv_merke", "svv_drivstoff_navn", "svv_importland_navn",
        "svv_kjoretoy_typebeskrivelse", "svv_euroklasse", "svv_hybridkategori",
        "svv_registreringsstatus", "svv_motor_arbeidsprinsipp",
    ]
    for _c in CATEGORICAL_COLS:
        if _c in master_df.columns:
            try:
                if master_df[_c].nunique(dropna=True) < len(master_df) * 0.5:
                    master_df[_c] = master_df[_c].astype("category")
            except Exception:
                pass
    gc.collect()
# 7) save parquet
    print(f"\nLagrer database til s3://{S3_BUCKET}/{OUTPUT_PARQUET_KEY}")
    parquet_buffer = io.BytesIO()
    master_df.to_parquet(parquet_buffer, index=False, engine="pyarrow")
    parquet_buffer.seek(0)
    s3.put_object(Bucket=S3_BUCKET, Key=OUTPUT_PARQUET_KEY, Body=parquet_buffer.getvalue())
    print("Parquet lagret.")

    # Trigg sky-scoring med det samme, slik at nye biler faar verdsettelse raskt.
    trigger_bilradar_scoring()

    # 8) update state
    state["last_processed_observed_at"] = max_processed.isoformat()
    state["updated_at"] = datetime.now().isoformat(timespec="seconds")
    state["total_cars"] = total
    state["active_cars"] = int(n_active)
    save_state(s3, state)
    print(f"State oppdatert: last_processed_observed_at={state['last_processed_observed_at']}")

    # 9) metadata
    print("Genererer metadata...")

    def safe_int(val):
        try:
            return int(val)
        except Exception:
            return 0

    def unique_sorted(col_name):
        if col_name not in master_df.columns:
            return []
        vals = master_df[col_name].dropna().astype(str).unique().tolist()
        return sorted([v for v in vals if v and v.lower() not in {"", "nan", "none"}])

    metadata = {
        "produsenter": unique_sorted("Produsent"),
        "models_by_prod": (
            master_df.dropna(subset=["Produsent", "Modell"])
            .groupby("Produsent")["Modell"]
            .apply(lambda x: sorted(set(str(i) for i in x if _as_str(i) != "")))
            .to_dict()
            if {"Produsent", "Modell"}.issubset(master_df.columns) else {}
        ),
        "drivstoff_opts": unique_sorted("drivstoff"),
        "hjuldrift_opts": unique_sorted("hjuldrift"),
        "fylke_opts": unique_sorted("fylke"),
        "karosseri_opts": unique_sorted("Karosseri"),
        "year_min": safe_int(master_df["årstall"].min()) if "årstall" in master_df.columns and not master_df.empty else 0,
        "year_max": safe_int(master_df["årstall"].max()) if "årstall" in master_df.columns and not master_df.empty else 0,
        "km_min": safe_int(master_df["kjørelengde"].min()) if "kjørelengde" in master_df.columns and not master_df.empty else 0,
        "km_max": safe_int(master_df["kjørelengde"].max()) if "kjørelengde" in master_df.columns and not master_df.empty else 0,
        "total_cars": total,
        "active_cars": int(n_active),
        "latest_dt": (
            master_df["Dato_ny"].max().strftime("%Y-%m-%d")
            if "Dato_ny" in master_df.columns and master_df["Dato_ny"].notna().any()
            else None
        ),
    }

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=OUTPUT_METADATA_KEY,
        Body=json.dumps(metadata, ensure_ascii=False, indent=4).encode("utf-8"),
    )
    print("Metadata lagret.\nFerdig!")


if __name__ == "__main__":
    oppdater_database()
