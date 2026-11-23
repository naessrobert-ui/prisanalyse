"""Lange værserier: Bergen siden 1861, dag for dag.

Grunnlaget er døgnverdier (nedbør, og temperatur der den finnes) for en
kjede av stasjoner som til sammen dekker hele perioden. Nedbøren for Bergen
er lagt inn fra Roberts regneark (data/historikk/bergen_rr.csv.gz, hentet
fra eKlima/Frost). Nye dager og temperaturene hentes fra Frost i bakgrunnen
og lagres (S3, ellers lokal fil), så beregningene aldri venter på nettet.

Beregningen tar en vilkårlig periode i året (f.eks. 15. oktober til 15.
november, også over nyttår) og regner ut sum, regndager og temperatur for
hvert år, med rangering, snitt og normal 1991-2020.
"""
from __future__ import annotations

import io
import logging
import os
import threading
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)

_ROT = Path(__file__).resolve().parents[1]
_MAAN = ["januar", "februar", "mars", "april", "mai", "juni", "juli", "august",
         "september", "oktober", "november", "desember"]

SERIER: dict[str, dict[str, Any]] = {
    "bergen": {
        "navn": "Bergen",
        "lat": 60.383, "lon": 5.333,
        # (stasjon, fra, til). Til = None betyr fortsatt i drift.
        "kjede": [("SN50545", "1861-01-01", "1895-11-03"),
                  ("SN50550", "1895-11-04", "1903-12-31"),
                  ("SN50560", "1904-01-01", "1982-12-31"),
                  ("SN50540", "1983-01-01", None)],
        "stasjonsnavn": {"SN50545": "Bergen", "SN50550": "Bergen", "SN50560": "Bergen - Fredriksberg",
                         "SN50540": "Bergen - Florida"},
        "seed": _ROT / "data" / "historikk" / "bergen_rr.csv.gz",
    },
}

_KOLONNER = ["rr", "tmid", "tmin", "tmax"]
_LAAS = threading.RLock()
_DATA: dict[str, pd.DataFrame] = {}
_SIST_FORSOK: dict[str, float] = {}
_LASTET: dict[str, float] = {}
_STATUS: dict[str, dict[str, Any]] = {}
LES_PAA_NYTT = 600
_JOBB = threading.Lock()
_STARTET = False
OPPDATER_HVERT = int(os.getenv("HISTORIKK_OPPDATER_SEK", "3600"))


# ---------------------------------------------------------------------------
# Lagring
# ---------------------------------------------------------------------------

def _s3() -> Optional[tuple[Any, str]]:
    bucket = (os.getenv("HISTORIKK_S3_BUCKET", "").strip()
              or os.getenv("WEATHER_STATION_METRICS_S3_BUCKET", "").strip()
              or os.getenv("S3_BUCKET_NAME", "").strip())
    if not bucket:
        return None
    try:
        import boto3

        return boto3.client("s3", region_name=os.getenv("AWS_REGION", os.getenv("AWS_DEFAULT_REGION", "eu-north-1"))), bucket
    except Exception:
        return None


def _lokal(serie: str) -> Path:
    return _ROT / "data" / "historikk" / f"{serie}.parquet"


def _les_seed(serie: str) -> pd.DataFrame:
    df = pd.read_csv(SERIER[serie]["seed"], parse_dates=["dato"])
    df = df.set_index("dato").sort_index()
    for k in _KOLONNER:
        if k not in df:
            df[k] = np.nan
    return df[_KOLONNER + ["stasjon"]]


def _les_lagret(serie: str) -> Optional[pd.DataFrame]:
    try:
        s3 = _s3()
        if s3 is not None:
            klient, bucket = s3
            body = klient.get_object(Bucket=bucket, Key=f"historikk/{serie}.parquet")["Body"].read()
            return pd.read_parquet(io.BytesIO(body))
        if _lokal(serie).exists():
            return pd.read_parquet(_lokal(serie))
    except Exception:
        log.debug("historikk: ingen lagret serie for %s", serie, exc_info=True)
    return None


def _lagre(serie: str, df: pd.DataFrame) -> None:
    buf = io.BytesIO()
    df.to_parquet(buf)
    s3 = _s3()
    if s3 is not None:
        klient, bucket = s3
        klient.put_object(Bucket=bucket, Key=f"historikk/{serie}.parquet", Body=buf.getvalue())
        return
    _lokal(serie).parent.mkdir(parents=True, exist_ok=True)
    _lokal(serie).write_bytes(buf.getvalue())


def hent(serie: str) -> pd.DataFrame:
    """Serien fra minnet (lastes én gang). Starter en oppdatering i bakgrunnen
    hvis siste dag er gammel, uten å vente på den."""
    with _LAAS:
        df = _DATA.get(serie)
        gammel = time.time() - _LASTET.get(serie, 0) > LES_PAA_NYTT
    if df is None or gammel:
        # Flere prosesser deler samme lagrede serie. Den som oppdaterer lagrer,
        # de andre leser den inn igjen her (høyst hvert tiende minutt).
        lagret = _les_lagret(serie)
        if lagret is not None and len(lagret) and (df is None or lagret.index.max() >= df.index.max()):
            df = lagret
        elif df is None:
            df = _les_seed(serie)
        with _LAAS:
            _DATA[serie] = df
            _LASTET[serie] = time.time()
    if _trenger_oppdatering(serie, df):
        threading.Thread(target=oppdater, args=(serie,), daemon=True, name=f"historikk-{serie}").start()
    return df


def _trenger_oppdatering(serie: str, df: pd.DataFrame) -> bool:
    if os.getenv("PYTEST_CURRENT_TEST") or not os.getenv("FROST_CLIENT_ID"):
        return False
    if time.time() - _SIST_FORSOK.get(serie, 0) < OPPDATER_HVERT:
        return False
    i_gaar = (datetime.now(timezone.utc) - timedelta(days=1)).date()
    mangler_temp = df["tmid"].notna().sum() < 1000
    return df.index.max().date() < i_gaar or mangler_temp


# ---------------------------------------------------------------------------
# Frost
# ---------------------------------------------------------------------------

def _frost(stasjon: str, elementer: list[str], fra: date, til: date) -> list[dict[str, Any]]:
    from scripts.klima_norge import _frost_hent

    return _frost_hent([stasjon], elementer, f"{fra.isoformat()}/{(til + timedelta(days=1)).isoformat()}", timeout=90)


def _som_dag(t: str) -> date:
    """Klimadøgnet en P1D-verdi hører til (samme regel som klimasiden)."""
    from scripts.klima_norge import _dag

    return _dag(t)


def _nedbor_forskyvning(nye: dict[date, float], gamle: pd.Series) -> int:
    """Frost og eKlima kan datere døgnnedbøren ulikt. Finn forskyvningen
    (-1, 0 eller +1 dag) som gir best samsvar med dagene vi allerede har."""
    beste, treff_beste = 0, -1
    for skift in (0, -1, 1):
        treff = 0
        for d, v in nye.items():
            gammel = gamle.get(pd.Timestamp(d + timedelta(days=skift)))
            if gammel is not None and not np.isnan(gammel) and abs(gammel - v) < 0.05:
                treff += 1
        if treff > treff_beste:
            beste, treff_beste = skift, treff
    return beste


def oppdater(serie: str) -> Optional[pd.DataFrame]:
    """Henter nye dager (og manglende temperaturer) fra Frost og lagrer."""
    if not _JOBB.acquire(blocking=False):
        return None
    try:
        _SIST_FORSOK[serie] = time.time()
        with _LAAS:
            df = _DATA.get(serie)
        if df is None:
            df = _les_lagret(serie)
            if df is None or not len(df):
                df = _les_seed(serie)
        df = df.copy()
        cfg = SERIER[serie]
        stasjon = cfg["kjede"][-1][0]
        i_dag = datetime.now(timezone.utc).date()

        def publiser(hva: str) -> None:
            with _LAAS:
                _DATA[serie] = df.copy()
                _LASTET[serie] = time.time()
            try:
                _lagre(serie, df)
                _STATUS[serie]["lagret"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
            except Exception as exc:
                _STATUS[serie]["feil"] = f"lagring: {exc}"
                log.warning("historikk: lagring feilet: %s", exc)
            _STATUS[serie]["steg"] = hva

        _STATUS[serie] = {"startet": datetime.now(timezone.utc).isoformat(timespec="seconds"), "feil": None}

        # 1) Nye dager med nedbør og temperatur for stasjonen som er i drift.
        #    Lagres med en gang, så brukerne får dem før den lange jobben under.
        siste = df.index.max().date()
        if siste < i_dag - timedelta(days=1):
            fra = siste - timedelta(days=30)
            rader = _frost(stasjon, ["sum(precipitation_amount P1D)", "mean(air_temperature P1D)",
                                     "min(air_temperature P1D)", "max(air_temperature P1D)"], fra, i_dag)
            _STATUS[serie]["frost_rader"] = len(rader)
            if _flett_inn(df, rader, stasjon, i_dag):
                publiser("nye dager")

        # 2) Temperatur bakover, ti år om gangen, for hele stasjonskjeden.
        if df["tmid"].notna().sum() < len(df) * 0.5:
            if _fyll_temperatur(df, cfg, i_dag, lambda: publiser("temperatur")):
                publiser("temperatur ferdig")
        _STATUS[serie]["ferdig"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
        return df
    except Exception as exc:
        _STATUS.setdefault(serie, {})["feil"] = f"{type(exc).__name__}: {exc}"
        log.exception("historikk: oppdatering av %s feilet", serie)
        return None
    finally:
        _JOBB.release()


def _flett_inn(df: pd.DataFrame, rader: list[dict[str, Any]], stasjon: str, i_dag: date) -> int:
    nedbor: dict[date, float] = {}
    temp: dict[str, dict[date, float]] = {"tmid": {}, "tmin": {}, "tmax": {}}
    for r in rader:
        el = r["el"] or ""
        if "precipitation_amount" in el:
            nedbor[datetime.fromisoformat(str(r["t"]).replace("Z", "+00:00")).date()] = r["v"]
        elif "air_temperature" in el:
            k = "tmid" if el.startswith("mean") else "tmin" if el.startswith("min") else "tmax"
            temp[k][_som_dag(r["t"])] = r["v"]
    skift = _nedbor_forskyvning(nedbor, df["rr"]) if nedbor else 0
    nye = 0
    for d, v in nedbor.items():
        dag = d + timedelta(days=skift)
        if dag >= i_dag or not 0 <= v < 400:
            continue
        ts = pd.Timestamp(dag)
        if ts not in df.index:
            df.loc[ts, ["rr", "stasjon"]] = [round(v, 1), stasjon]
            nye += 1
    for k, serie in temp.items():
        for d, v in serie.items():
            ts = pd.Timestamp(d)
            if d >= i_dag or not -40 < v < 40:
                continue
            if ts not in df.index:
                df.loc[ts, ["rr", "stasjon"]] = [np.nan, stasjon]
            if pd.isna(df.at[ts, k]):
                df.at[ts, k] = round(v, 1)
                nye += 1
    if nye:
        # Nye rader havner bakerst. Uten sortering feiler datoutsnitt senere.
        df.sort_index(inplace=True)
    return nye


def _fyll_temperatur(df: pd.DataFrame, cfg: dict[str, Any], i_dag: date,
                     underveis: Optional[Any] = None) -> int:
    nye = 0
    for stasjon, fra_s, til_s in cfg["kjede"]:
        if nye and underveis:
            underveis()  # lagre det vi har etter hver stasjon
        fra = date.fromisoformat(fra_s)
        til = date.fromisoformat(til_s) if til_s else i_dag
        start = fra
        while start <= til:
            slutt = min(til, date(start.year + 9, 12, 31))
            bit = df.loc[pd.Timestamp(start):pd.Timestamp(slutt), "tmid"]
            if bit.notna().sum() < len(bit) * 0.9:
                try:
                    rader = _frost(stasjon, ["mean(air_temperature P1D)", "min(air_temperature P1D)",
                                             "max(air_temperature P1D)"], start, slutt)
                    rader = [r for r in rader if start <= _som_dag(r["t"]) <= slutt]
                    nye += _flett_inn(df, rader, stasjon, i_dag)
                except Exception as exc:
                    log.info("historikk: temperatur %s %s–%s: %s", stasjon, start, slutt, exc)
            start = slutt + timedelta(days=1)
    return nye


# ---------------------------------------------------------------------------
# Beregning
# ---------------------------------------------------------------------------

def _md(m: int, d: int) -> int:
    return m * 100 + d


def _dato_tekst(m: int, d: int) -> str:
    return f"{d}. {_MAAN[m - 1]}"


def _gyldig(aar: int, m: int, d: int) -> date:
    while True:
        try:
            return date(aar, m, d)
        except ValueError:
            d -= 1


def beregn(df: pd.DataFrame, fra: tuple[int, int], til: tuple[int, int], *, grense: float = 0.1,
           normal: tuple[int, int] = (1991, 2020)) -> dict[str, Any]:
    """Sum nedbør, regndager og temperatur for perioden `fra`–`til` (måned, dag)
    hvert år. Går perioden over nyttår, hører den til året den slutter i.

    Pågår perioden nå, sammenlignes alle år med de samme dagene som har data i
    år (f.eks. 1.–22. november), så "i år" ikke straffes for dager som ikke
    har kommet ennå."""
    siste = df.index.max().date()
    over_nyttaar = _md(*fra) > _md(*til)

    def start_for(aar: int) -> date:
        return _gyldig(aar - 1 if over_nyttaar else aar, fra[0], fra[1])

    # "I år" er perioden som inneholder siste dag med data, ellers sist avsluttede.
    aar_naa = siste.year + (1 if over_nyttaar and _md(siste.month, siste.day) >= _md(*fra) else 0)
    if siste < start_for(aar_naa):
        aar_naa -= 1
    pagaar = start_for(aar_naa) <= siste < _gyldig(aar_naa, *til)
    lengde = (siste - start_for(aar_naa)).days if pagaar else None

    def slutt_for(aar: int) -> date:
        # Pågår perioden, tar vi like mange dager fra start hvert år.
        return start_for(aar) + timedelta(days=lengde) if lengde is not None else _gyldig(aar, *til)

    forste = df.index.min().date()
    rader = []
    for aar in range(forste.year, aar_naa + 1):
        s, e = start_for(aar), slutt_for(aar)
        if s < forste:
            continue
        bit = df.loc[pd.Timestamp(s):pd.Timestamp(e)]
        dager = (e - s).days + 1
        rr = bit["rr"].dropna()
        rader.append({"aar": aar, "dager": dager, "n_rr": len(rr), "rr": rr.sum() if len(rr) else np.nan,
                      "regndager": int((rr >= grense).sum()), "n_t": int(bit["tmid"].notna().sum()),
                      "tmid": bit["tmid"].mean(), "tmax": bit["tmax"].max(), "tmin": bit["tmin"].min()})
    tab = pd.DataFrame(rader).set_index("aar")
    ok_rr = tab["n_rr"] >= tab["dager"] * 0.9
    ok_t = tab["n_t"] >= tab["dager"] * 0.9
    i_periode = np.zeros(len(df), dtype=bool)
    for aar in tab.index:
        i_periode |= (df.index >= pd.Timestamp(start_for(aar))) & (df.index <= pd.Timestamp(slutt_for(aar)))
    d = df.loc[i_periode]
    rr = d["rr"]
    sl = slutt_for(aar_naa)

    def rang(kol: str, ok: pd.Series, synkende: bool) -> Optional[int]:
        s = tab.loc[ok, kol].dropna()
        if aar_naa not in s.index:
            return None
        return int((s > s[aar_naa]).sum() + 1) if synkende else int((s < s[aar_naa]).sum() + 1)

    def topp(kol: str, ok: pd.Series, synkende: bool, n: int = 10) -> list[list[Any]]:
        s = tab.loc[ok, kol].dropna().sort_values(ascending=not synkende).head(n)
        return [[int(a), round(float(v), 1)] for a, v in s.items()]

    def snitt(kol: str, ok: pd.Series, fra_aar: Optional[int] = None, til_aar: Optional[int] = None) -> Optional[float]:
        s = tab.loc[ok, kol].dropna()
        if fra_aar:
            s = s[(s.index >= fra_aar) & (s.index <= til_aar)]
        if aar_naa in s.index and not fra_aar:
            s = s.drop(aar_naa)
        return round(float(s.mean()), 1) if len(s) else None

    # Største døgnverdier i perioden, alle år.
    dognrekord = [[ts.date().isoformat(), float(v)] for ts, v in rr.nlargest(10).items()]
    varmeste_dag = d["tmax"].dropna().nlargest(5)
    kaldeste_dag = d["tmin"].dropna().nsmallest(5)

    aar_liste = [{
        "aar": int(a),
        "rr": None if not ok_rr[a] else round(float(r.rr), 1),
        "regndager": None if not ok_rr[a] else int(r.regndager),
        "tmid": None if not ok_t[a] or pd.isna(r.tmid) else round(float(r.tmid), 1),
        "tmax": None if not ok_t[a] or pd.isna(r.tmax) else round(float(r.tmax), 1),
        "tmin": None if not ok_t[a] or pd.isna(r.tmin) else round(float(r.tmin), 1),
    } for a, r in tab.iterrows()]

    nf, nt = normal
    return {
        "periode": {"fra": f"{fra[0]:02d}-{fra[1]:02d}", "til": f"{til[0]:02d}-{til[1]:02d}",
                    "brukt_til": f"{sl.month:02d}-{sl.day:02d}", "pagaar": pagaar, "over_nyttaar": over_nyttaar,
                    "tekst": f"{_dato_tekst(*fra)} til {_dato_tekst(sl.month, sl.day)}", "grense": grense},
        "i_aar": aar_naa, "siste_dag": siste.isoformat(),
        "aar": aar_liste,
        "nedbor": {
            "verdi": next((x["rr"] for x in aar_liste if x["aar"] == aar_naa), None),
            "regndager": next((x["regndager"] for x in aar_liste if x["aar"] == aar_naa), None),
            "rang_vaat": rang("rr", ok_rr, True), "rang_regndager": rang("regndager", ok_rr, True),
            "antall_aar": int(ok_rr.sum()),
            "snitt": snitt("rr", ok_rr), "normal": snitt("rr", ok_rr, nf, nt),
            "snitt_regndager": snitt("regndager", ok_rr), "normal_regndager": snitt("regndager", ok_rr, nf, nt),
            "vaateste": topp("rr", ok_rr, True), "torreste": topp("rr", ok_rr, False),
            "flest_regndager": topp("regndager", ok_rr, True), "faerrest_regndager": topp("regndager", ok_rr, False),
            "dognrekord": dognrekord,
            "fra_aar": int(tab.index[ok_rr].min()) if ok_rr.any() else None,
        },
        "temperatur": None if not ok_t.any() else {
            "verdi": next((x["tmid"] for x in aar_liste if x["aar"] == aar_naa), None),
            "rang_varm": rang("tmid", ok_t, True), "antall_aar": int(ok_t.sum()),
            "snitt": snitt("tmid", ok_t), "normal": snitt("tmid", ok_t, nf, nt),
            "varmeste": topp("tmid", ok_t, True), "kaldeste": topp("tmid", ok_t, False),
            "varmeste_dag": [[ts.date().isoformat(), float(v)] for ts, v in varmeste_dag.items()],
            "kaldeste_dag": [[ts.date().isoformat(), float(v)] for ts, v in kaldeste_dag.items()],
            "fra_aar": int(tab.index[ok_t].min()),
        },
    }


def info(serie: str) -> dict[str, Any]:
    df = hent(serie)
    cfg = SERIER[serie]
    t = df["tmid"].dropna()
    return {"serie": serie, "navn": cfg["navn"], "fra": df.index.min().date().isoformat(),
            "til": df.index.max().date().isoformat(),
            "temp_fra": t.index.min().date().isoformat() if len(t) else None,
            "stasjoner": [{"id": s, "navn": cfg["stasjonsnavn"].get(s, s), "fra": a, "til": b} for s, a, b in cfg["kjede"]]}


def status(serie: str) -> dict[str, Any]:
    """Til feilsøking: hva serien inneholder og hvordan siste oppdatering gikk."""
    df = hent(serie)
    return {**info(serie), "rader": int(len(df)), "temp_dager": int(df["tmid"].notna().sum()),
            "frost_konfigurert": bool(os.getenv("FROST_CLIENT_ID")), "s3": _s3() is not None,
            "siste_forsok": datetime.fromtimestamp(_SIST_FORSOK[serie], timezone.utc).isoformat(timespec="seconds")
            if serie in _SIST_FORSOK else None,
            "oppdatering": _STATUS.get(serie)}


def start_warmup() -> None:
    """Henter nye dager og temperaturene bakover kort etter oppstart, deretter
    hver sjette time. Brukerne trenger ikke vente på dette."""
    global _STARTET
    if _STARTET or os.getenv("PYTEST_CURRENT_TEST") or os.getenv("HISTORIKK_WARMUP", "1") == "0":
        return
    _STARTET = True

    def loop() -> None:
        time.sleep(90)
        while True:
            for serie in SERIER:
                if os.getenv("FROST_CLIENT_ID"):
                    oppdater(serie)
            time.sleep(6 * 3600)

    threading.Thread(target=loop, name="historikk", daemon=True).start()
