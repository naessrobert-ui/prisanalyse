"""
bil_nye_annonser.py - Antall nye bil-annonser per dag (privat vs. bedrift)
==========================================================================

Bygger en daglig tidsserie over hvor mange *nye* annonser som dukket opp paa
Finn hver dag, fordelt paa private selgere og bedrifter/forhandlere.

Definisjon
----------
En annonse regnes som "ny paa dag D" dersom foerste gang vi observerte den
(FinnKode) faller paa dag D. Grunnkilden er ``calc/bil/database_biler.parquet``
som har akkurat én rad per FinnKode med kolonnen ``Dato`` (foerste gang sett)
og ``Selger``. Fordi filen allerede er de-duplisert paa FinnKode slipper vi aa
telle samme annonse to ganger.

Selgertype
----------
Selgertype avgjoeres av ``Selger``-kolonnen: er den **tom** er annonsen fra en
**privat** selger; har den en verdi (et forhandler-/bedriftsnavn) er den fra en
**bedrift**. (Tidligere brukte vi ``forhandler_type``, men den kolonnen var tom
for de fleste radene og ga nesten bare "ukjent".)

Datakilder (i prioritert rekkefoelge)
-------------------------------------
1. ``calc/bil/database_biler.parquet``  - hovedkilde, én rad per FinnKode med
   ``Dato`` = foerste gang sett og ``Selger``.
2. ``calc/bil/bil_time.parquet``        - timesopploest historikk. Fanger opp
   annonser som ble lagt ut og forsvant igjen *innen samme doegn*, og som
   derfor aldri rakk aa komme med i en daglig snapshot. FinnKoder som ikke
   finnes i hovedkilden legges til med sin tidligste observasjon her.
3. ``raw/bil-daglig/`` + ``raw/bil-time/`` - selve raa-filene brukes kun til aa
   fastslaa *hvilke dager innsamlingen faktisk kjorte* (dekkede dager). En dag
   uten fil er en dag der vi ikke samlet inn data.

Manglende dager
---------------
Noen dager mangler innsamling. Nye annonser som ble lagt ut i et slikt hull,
og som fortsatt var aktive da innsamlingen tok seg opp igjen, faar foerste-
observasjon paa *foerste dag etter hullet* - de hoper seg altsaa opp der.

For aa unngaa en kunstig topp fordeler vi antallet paa dagen etter hullet jevnt
utover hele hullet (hullets dager + selve dagen etter). Eks: mangler mandag,
tirsdag og onsdag, og torsdag har 40 nye annonser, saa faar man-tor 10 hver.
Disse dagene markeres ``estimert=True``. Er hullet stoerre enn ``maks_gap``
markeres de i tillegg som ``usikker=True`` (antakelsen om jevn fordeling blir
svakere jo lengre hullet er).

Modulen er delt i rene funksjoner (testbare uten S3) og et tynt S3-lag.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any, Iterable

import numpy as np
import pandas as pd

# ---- S3-noekler ----
HOVED_PARQUET_KEY = "calc/bil/database_biler.parquet"
TIME_PARQUET_KEY = "calc/bil/bil_time.parquet"
RAW_DAGLIG_PREFIX = "raw/bil-daglig/"
RAW_TIME_PREFIX = "raw/bil-time/"

# Buckets vi fordeler annonsene paa
BUCKETS = ["privat", "bedrift"]

# Default: hull opp til denne lengden (i dager) fylles med jevn fordeling uten
# ekstra advarsel. Lengre hull fylles fortsatt, men markeres usikre.
STANDARD_MAKS_GAP = 14


# =====================================================================
#  Rene funksjoner (ingen S3 / IO - enkle aa teste)
# =====================================================================

def klassifiser_selgertype(serie: pd.Series) -> pd.Series:
    """Avgjoer selgertype ut fra ``Selger``-kolonnen: {privat, bedrift}.

    - tom / NaN / bare whitespace  -> ``privat``   (ingen selgernavn oppgitt)
    - en hvilken som helst verdi   -> ``bedrift``  (forhandler-/bedriftsnavn)
    """
    s = serie.astype(str).str.strip().str.lower()
    tom = serie.isna() | s.isna() | s.isin(["", "nan", "none", "<na>"])
    resultat = np.where(tom.to_numpy(), "privat", "bedrift")
    return pd.Series(resultat, index=serie.index)


def tell_nye_per_dag(
    df: pd.DataFrame,
    dato_kol: str = "Dato",
    selger_kol: str = "Selger",
) -> pd.DataFrame:
    """Tell nye annonser per (foerste-sett-dato, selgertype).

    Parametre
    ---------
    df : DataFrame med én rad per FinnKode. Maa ha ``dato_kol`` (foerste gang
         sett) og ``selger_kol`` (``Selger`` - tom = privat, verdi = bedrift).

    Returnerer
    ----------
    DataFrame indeksert paa ``date`` med kolonnene ``privat``, ``bedrift`` og
    ``total`` (heltall). Rader uten gyldig dato droppes.
    """
    tom_ramme = pd.DataFrame(columns=BUCKETS + ["total"])
    tom_ramme.index.name = "dag"
    if df is None or df.empty or dato_kol not in df.columns:
        return tom_ramme

    dato = pd.to_datetime(df[dato_kol], errors="coerce")
    gyldig = dato.notna()
    if not gyldig.any():
        return tom_ramme

    if selger_kol in df.columns:
        bucket = klassifiser_selgertype(df.loc[gyldig, selger_kol])
    else:
        # Uten Selger-kolonne kan vi ikke skille; anta privat (ingen navn).
        bucket = pd.Series("privat", index=df.index[gyldig])

    tmp = pd.DataFrame(
        {
            "dag": dato[gyldig].dt.normalize().dt.date,
            "bucket": bucket.values,
        }
    )
    piv = (
        tmp.groupby(["dag", "bucket"]).size().unstack(fill_value=0)
    )
    for b in BUCKETS:
        if b not in piv.columns:
            piv[b] = 0
    piv = piv[BUCKETS]
    piv["total"] = piv.sum(axis=1)
    piv.index.name = "dag"
    return piv.sort_index()


def finn_manglende_dager(dekkede_dager: Iterable[date]) -> list[date]:
    """Returner sorterte kalenderdager som ligger *mellom* min og max av de
    dekkede dagene, men som selv ikke er dekket."""
    dekket = sorted({_som_date(d) for d in dekkede_dager})
    if len(dekket) < 2:
        return []
    dekket_sett = set(dekket)
    manglende: list[date] = []
    d = dekket[0]
    slutt = dekket[-1]
    while d <= slutt:
        if d not in dekket_sett:
            manglende.append(d)
        d += timedelta(days=1)
    return manglende


def bygg_serie_med_estimat(
    raw_counts: pd.DataFrame,
    dekkede_dager: Iterable[date],
    maks_gap: int = STANDARD_MAKS_GAP,
    utelat_forste_dag: bool = False,
) -> list[dict[str, Any]]:
    """Bygg en sammenhengende daglig serie med estimering over hull.

    Parametre
    ---------
    raw_counts : DataFrame indeksert paa ``date`` med kolonnene privat/bedrift/
                 total (typisk fra :func:`tell_nye_per_dag`).
    dekkede_dager : dagene der innsamlingen faktisk kjorte. Definerer tidslinjen
                 og hvilke hull som skal fylles. Er den tom brukes datoene i
                 ``raw_counts`` som fallback (ingen hull-fylling da).
    maks_gap : hull opp til denne lengden fylles uten ekstra advarsel; lengre
                 hull fylles fortsatt, men markeres ``usikker=True``.
    utelat_forste_dag : dropp den aller foerste dagen i serien. Den foerste
                 innsamlingsdagen er en oppstarts-/census-dag der *hele* den
                 eksisterende beholdningen faar "foerste gang sett" = den dagen,
                 og gir en kunstig topp (titusener) som ikke er reelle nye
                 annonser. Settes True i produksjon.

    Returnerer
    ----------
    Liste av dict (én per dag, kronologisk):
        {dato, privat, bedrift, total, estimert, usikker, gap_lengde}
    Verdier for estimerte dager er floats (jevnt fordelt); ellers heltall.
    """
    counts = raw_counts.copy()
    if not counts.empty:
        counts.index = [_som_date(d) for d in counts.index]
    for b in BUCKETS + ["total"]:
        if b not in counts.columns:
            counts[b] = 0

    dekket = sorted({_som_date(d) for d in dekkede_dager})
    # Fallback: uten kjente innsamlingsdager bruker vi datoene i dataene selv.
    if not dekket:
        dekket = sorted(counts.index)
    # Sikre at dager med data alltid regnes som dekket.
    dekket = sorted(set(dekket) | set(counts.index))
    if not dekket:
        return []

    def _rad(dag: date) -> dict[str, float]:
        if dag in counts.index:
            r = counts.loc[dag]
            return {b: float(r.get(b, 0)) for b in BUCKETS + ["total"]}
        return {b: 0.0 for b in BUCKETS + ["total"]}

    resultat: dict[date, dict[str, Any]] = {}
    forrige: date | None = None
    for cur in dekket:
        rad = _rad(cur)
        gap = 0 if forrige is None else (cur - forrige).days - 1
        if gap > 0:
            # Fordel dagens (opphopede) antall jevnt over hullet + selve dagen.
            n_dager = gap + 1
            usikker = gap > maks_gap
            fordelte_dager = [forrige + timedelta(days=i + 1) for i in range(n_dager)]
            for d in fordelte_dager:
                resultat[d] = {
                    "dato": d.isoformat(),
                    **{b: round(rad[b] / n_dager, 2) for b in BUCKETS + ["total"]},
                    "estimert": True,
                    "usikker": usikker,
                    "gap_lengde": gap,
                }
        else:
            resultat[cur] = {
                "dato": cur.isoformat(),
                **{b: int(round(rad[b])) for b in BUCKETS + ["total"]},
                "estimert": False,
                "usikker": False,
                "gap_lengde": 0,
            }
        forrige = cur

    serie = [resultat[d] for d in sorted(resultat)]
    if utelat_forste_dag and len(serie) > 1:
        serie = serie[1:]
    return serie


def oppsummer_serie(serie: list[dict[str, Any]]) -> dict[str, Any]:
    """Regn ut noen enkle noekkeltall for en ferdig serie."""
    if not serie:
        return {
            "antall_dager": 0,
            "antall_estimerte_dager": 0,
            "sum_total": 0,
            "sum_privat": 0,
            "sum_bedrift": 0,
            "snitt_per_dag": 0.0,
            "andel_privat_pct": None,
        }
    sum_privat = sum(d["privat"] for d in serie)
    sum_bedrift = sum(d["bedrift"] for d in serie)
    sum_total = sum(d["total"] for d in serie)
    return {
        "antall_dager": len(serie),
        "antall_estimerte_dager": sum(1 for d in serie if d["estimert"]),
        "sum_total": round(sum_total, 1),
        "sum_privat": round(sum_privat, 1),
        "sum_bedrift": round(sum_bedrift, 1),
        "snitt_per_dag": round(sum_total / len(serie), 1),
        "andel_privat_pct": round(100 * sum_privat / sum_total, 1) if sum_total else None,
    }


def _som_date(d: Any) -> date:
    """Konverter date/datetime/Timestamp/str til ren ``date``."""
    if isinstance(d, date) and not isinstance(d, datetime):
        return d
    if isinstance(d, datetime):
        return d.date()
    ts = pd.Timestamp(d)
    return ts.date()


# =====================================================================
#  S3-lag (IO) - orkestrerer de rene funksjonene over
# =====================================================================

def _s3_client():
    import boto3

    from config import AWS_KEY, AWS_REGION, AWS_SECRET

    return boto3.client(
        "s3",
        aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SECRET,
        region_name=AWS_REGION,
    )


def _bucket() -> str:
    from config import S3_BUCKET_NAME

    return S3_BUCKET_NAME


def _pick(kolonner: Iterable[str], kandidater: Iterable[str]) -> str | None:
    lower = {str(c).lower(): c for c in kolonner}
    for kand in kandidater:
        if kand.lower() in lower:
            return lower[kand.lower()]
    return None


def _les_parquet_kolonner(s3, key: str, kandidat_kolonner: dict[str, list[str]]):
    """Les et parquet fra S3 og returner (df, colmap). ``kandidat_kolonner``
    kartlegger kanonisk navn -> liste av mulige faktiske kolonnenavn. Kun de
    kolonnene som trengs leses inn."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    obj = s3.get_object(Bucket=_bucket(), Key=key)
    buf = pa.BufferReader(obj["Body"].read())
    skjema = pq.read_schema(buf)
    tilgjengelig = list(skjema.names)

    colmap: dict[str, str] = {}
    for kanon, kandidater in kandidat_kolonner.items():
        traff = _pick(tilgjengelig, kandidater)
        if traff:
            colmap[kanon] = traff

    buf.seek(0)
    table = pq.read_table(buf, columns=list(colmap.values()) or None)
    df = table.to_pandas()
    df.columns = [str(c) for c in df.columns]
    return df, colmap


def last_forstesett_df(s3) -> pd.DataFrame:
    """Bygg DataFrame med én rad per FinnKode: {Dato (foerste sett), Selger}.
    Kombinerer hovedparquet med timesparquet for aa fange opp kortlivde
    annonser som aldri kom med i en daglig snapshot."""
    # --- Hovedkilde ---
    hoved, colmap = _les_parquet_kolonner(
        s3,
        HOVED_PARQUET_KEY,
        {
            "finnkode": ["FinnKode", "finnkode"],
            "dato": ["Dato", "dato"],
            "selger": ["Selger", "selger"],
        },
    )
    if "dato" not in colmap:
        raise RuntimeError(
            f"{HOVED_PARQUET_KEY} mangler en Dato-kolonne (foerste gang sett)"
        )
    if "selger" not in colmap:
        print(
            f"[nye-annonser] ADVARSEL: {HOVED_PARQUET_KEY} mangler Selger-kolonne "
            "- alt telles som privat"
        )

    ut = pd.DataFrame(
        {
            "FinnKode": (
                hoved[colmap["finnkode"]].astype(str)
                if "finnkode" in colmap
                else hoved.index.astype(str)
            ),
            "Dato": pd.to_datetime(hoved[colmap["dato"]], errors="coerce"),
            "Selger": (
                hoved[colmap["selger"]]
                if "selger" in colmap
                else ""
            ),
        }
    )
    kjente_finnkoder = set(ut["FinnKode"])

    # --- Timeskilde: legg til FinnKoder vi ikke allerede har ---
    try:
        ekstra = _hent_kortlivde_fra_time(s3, kjente_finnkoder)
        if not ekstra.empty:
            ut = pd.concat([ut, ekstra], ignore_index=True)
            print(
                f"[nye-annonser] +{len(ekstra):,} kortlivde annonser fra "
                f"{TIME_PARQUET_KEY} (ikke i hovedfilen)"
            )
    except Exception as e:  # timeskilden er en bonus - skal aldri velte alt
        print(f"[nye-annonser] Klarte ikke berike fra {TIME_PARQUET_KEY}: {e}")

    return ut


def _hent_kortlivde_fra_time(s3, kjente_finnkoder: set[str]) -> pd.DataFrame:
    """Fra timesparquet: FinnKoder som IKKE finnes i hovedfilen, med sin
    tidligste observerte tidspunkt som Dato."""
    df, colmap = _les_parquet_kolonner(
        s3,
        TIME_PARQUET_KEY,
        {
            "finnkode": ["FinnKode", "finnkode"],
            "tid": ["snapshot_time", "tidspunkt", "dato", "Dato", "timestamp"],
            "selger": ["Selger", "selger"],
        },
    )
    if "finnkode" not in colmap or "tid" not in colmap:
        return pd.DataFrame(columns=["FinnKode", "Dato", "Selger"])

    df["FinnKode"] = df[colmap["finnkode"]].astype(str)
    df["_tid"] = pd.to_datetime(df[colmap["tid"]], errors="coerce")
    df = df[df["_tid"].notna() & ~df["FinnKode"].isin(kjente_finnkoder)]
    if df.empty:
        return pd.DataFrame(columns=["FinnKode", "Dato", "Selger"])

    # Tidligste observasjon per FinnKode = foerste gang sett
    idx = df.groupby("FinnKode")["_tid"].idxmin()
    forste = df.loc[idx]
    return pd.DataFrame(
        {
            "FinnKode": forste["FinnKode"].values,
            "Dato": forste["_tid"].values,
            "Selger": (
                forste[colmap["selger"]].values
                if "selger" in colmap
                else ""
            ),
        }
    )


def list_innsamlingsdager(s3) -> set[date]:
    """Returner settet av dager der innsamlingen faktisk kjorte, utledet fra
    raa-filene i ``raw/bil-daglig/`` og ``raw/bil-time/`` (LastModified-dato).
    Samme konvensjon som scripts/bygg_daglig_aggregat.list_raw_csvs."""
    dager: set[date] = set()
    paginator = s3.get_paginator("list_objects_v2")
    for prefix in (RAW_DAGLIG_PREFIX, RAW_TIME_PREFIX):
        for side in paginator.paginate(Bucket=_bucket(), Prefix=prefix):
            for o in side.get("Contents", []):
                if o["Size"] > 0 and str(o["Key"]).lower().endswith(".csv"):
                    dager.add(o["LastModified"].date())
    return dager


def bygg_nye_annonser(s3=None, maks_gap: int = STANDARD_MAKS_GAP) -> dict[str, Any]:
    """Full pipeline: les kilder fra S3, tell nye per dag, fyll hull, og
    returner et JSON-klart resultat."""
    if s3 is None:
        s3 = _s3_client()

    df = last_forstesett_df(s3)
    raw_counts = tell_nye_per_dag(df)

    try:
        dekkede = list_innsamlingsdager(s3)
    except Exception as e:
        print(f"[nye-annonser] Klarte ikke liste innsamlingsdager: {e}")
        dekkede = set()

    serie = bygg_serie_med_estimat(
        raw_counts, dekkede, maks_gap=maks_gap, utelat_forste_dag=True
    )
    return {
        "serie": serie,
        "oppsummering": oppsummer_serie(serie),
        "maks_gap": maks_gap,
        "antall_annonser_totalt": int(len(df)),
        "generert": datetime.utcnow().isoformat(),
    }
