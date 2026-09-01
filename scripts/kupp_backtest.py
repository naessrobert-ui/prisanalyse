"""
scripts/kupp_backtest.py – Retrospektiv fasit for kupp-treffsikkerhet
=====================================================================
Gaar gjennom alle biler som er lagt ut siden en gitt dato (default 1. august),
scorer dem paa nytt med kupp-motoren (lookup/variant + peer-WLS), flagger hvilke
som *ville* blitt definert som kupp gitt dagens regler, og maaler hvor stor
andel av kuppene som ble solgt innen X doegn (default 2).

Svarer paa:
  1. Hvor raskt ble de flaggede kupp-bilene solgt? (48-timers salgsrate)
  2. Er det et moenster? (rate brutt ned paa drivstoff, pris, fylke, rabatt,
     konfidens, produsent ...)
  3. Hvilke elbiler ble solgt innen 2 doegn UTEN aa bli flagget som kupp – og
     hva kjennetegner dem? (bommede hurtigsalg)

VIKTIG FORBEHOLD (look-ahead):
  "Dagens regler" betyr at motoren trenes paa alle solgte biler siste 365 dager
  – inkludert biler som ble solgt i selve maalevinduet. Det gir et lite
  optimistisk pris-estimat. Bruk --frys for aa i stedet fryse modellen til salg
  FOER fra-datoen (ekte walk-forward), som fjerner det meste av forbeholdet.

Kjoering:
  python -m scripts.kupp_backtest                       # leser fra S3
  python -m scripts.kupp_backtest --input db.parquet    # lokal parquet
  python -m scripts.kupp_backtest --fra 2026-08-01 --dager 2 --utdir ./ut
  python -m scripts.kupp_backtest --frys                # walk-forward-modell
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import date, datetime
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import bil_kupp_analyse as motor  # noqa: E402
from bil_nye_annonser import klassifiser_selgertype  # noqa: E402

# Rabatt-trapp: samme default som kupp-vakt (scripts/kupp_vakt.py). Inlines her
# saa vi slipper aa dra inn scraping-avhengighetene (requests/bs4) fra vakten.
#   < 50k -> 30 %, < 100k -> 20 %, < 150k -> 15 %, < 250k -> 7 %, ellers -> 6 %.
RABATT_TRAPP_DEFAULT = "50000:30,100000:20,150000:15,250000:7,:6"

SOLGT_VERDIER = {"JA", "FJERNET"}


# ======================================================================
# Rene hjelpefunksjoner (ingen S3 / I/O – enhetstestbare)
# ======================================================================

def parse_trapp(spec: str) -> list[tuple[float, float]]:
    """Tolk "maxpris:minprosent,..." til sortert liste av (oevre_grense, min_pct)."""
    bands: list[tuple[float, float]] = []
    for ledd in (spec or "").split(","):
        ledd = ledd.strip()
        if not ledd or ":" not in ledd:
            continue
        pmax, pct = ledd.split(":", 1)
        pmax, pct = pmax.strip(), pct.strip()
        try:
            pct_f = float(pct)
        except ValueError:
            continue
        if pmax in ("", "inf", "*"):
            upper = float("inf")
        else:
            try:
                upper = float(pmax)
            except ValueError:
                continue
        bands.append((upper, pct_f))
    bands.sort(key=lambda b: b[0])
    return bands


def min_rabatt_for_pris(pris: float, bands: list[tuple[float, float]]) -> float:
    """Paakrevd minste rabatt-prosent for en gitt salgspris (fra trappa)."""
    if not bands:
        return float("inf")
    for upper, pct in bands:
        if pris < upper:
            return pct
    return bands[-1][1]


def norm_drivstoff(s) -> str:
    """Normaliser drivstoff til kanoniske noekler (elektrisk/hybrid/... )."""
    s = str(s or "").strip().lower()
    if s in ("el", "elektrisk", "elbil", "bev", "elektrisitet"):
        return "elektrisk"
    if "plug" in s or "ladbar" in s or "phev" in s:
        return "plug-in hybrid"
    if "hybrid" in s or s in ("hev", "mhev"):
        return "hybrid"
    if s in ("diesel", "tdi", "hdi"):
        return "diesel"
    if s in ("bensin", "petrol"):
        return "bensin"
    return s


def _finn_selger_kolonne(df: pd.DataFrame) -> str | None:
    for cand in ("Selger", "selger"):
        if cand in df.columns:
            return cand
    return None


def er_privat_mask(df: pd.DataFrame) -> pd.Series:
    """True for private selgere (tom Selger-kolonne). Mangler kolonnen antas
    alt privat (samme fail-open som resten av appen)."""
    col = _finn_selger_kolonne(df)
    if col is None:
        return pd.Series(True, index=df.index)
    return klassifiser_selgertype(df[col]).eq("privat")


def beregn_rabatt(df: pd.DataFrame) -> pd.DataFrame:
    """Legg til rabatt_kr / rabatt_pct der forventet_pris finnes og er positiv.
    rabatt = (forventet_pris - salgspris) / forventet_pris * 100."""
    df = df.copy()
    fp = pd.to_numeric(df.get("forventet_pris"), errors="coerce")
    sp = pd.to_numeric(df.get("salgspris"), errors="coerce")
    gyldig = fp.notna() & (fp > 0) & sp.notna()
    df["rabatt_kr"] = np.where(gyldig, fp - sp, np.nan)
    df["rabatt_pct"] = np.where(gyldig, (fp - sp) / fp * 100.0, np.nan)
    return df


def flagg_kupp(
    df: pd.DataFrame,
    bands: list[tuple[float, float]],
    kun_privat: bool = True,
    maks_rabatt_pct: float | None = 70.0,
) -> pd.Series:
    """Bool-serie: ville bilen blitt flagget som kupp gitt reglene?

    Krav:
      - forventet_pris finnes og rabatt_pct er beregnet
      - rabatt_pct >= trappekravet for salgsprisen
      - (valgfritt) rabatt_pct <= maks_rabatt_pct (drop mistenkelige feil-lista)
      - (valgfritt) privat selger (kupp-vakt varsler default kun privat)
    """
    fp = pd.to_numeric(df.get("forventet_pris"), errors="coerce")
    sp = pd.to_numeric(df.get("salgspris"), errors="coerce")
    rab = pd.to_numeric(df.get("rabatt_pct"), errors="coerce")

    krav = sp.apply(lambda p: min_rabatt_for_pris(float(p), bands)
                    if pd.notna(p) else np.inf)
    er = fp.notna() & (fp > 0) & rab.notna() & (rab >= krav)
    if maks_rabatt_pct is not None:
        er &= rab <= maks_rabatt_pct
    if kun_privat:
        er &= er_privat_mask(df)
    return er.fillna(False)


def solgt_innen(df: pd.DataFrame, dager: int, kun_ja: bool = False) -> pd.Series:
    """Bool-serie: er bilen solgt innen `dager` doegn?
    Solgt = 'JA' (og 'FJERNET' med mindre kun_ja). Bruker dager_til_salg
    (= Dato_ny - Dato) <= dager."""
    solgt_verdier = {"JA"} if kun_ja else SOLGT_VERDIER
    solgt = df["Solgt"].astype(str).str.strip().str.upper().isin(solgt_verdier)
    d = pd.to_numeric(df.get("dager_til_salg"), errors="coerce")
    return (solgt & d.notna() & (d <= dager)).fillna(False)


def standard_fra_dato(ref: date) -> date:
    """Siste 1. august paa eller foer ref-datoen."""
    if ref.month >= 8:
        return date(ref.year, 8, 1)
    return date(ref.year - 1, 8, 1)


# ======================================================================
# Scoring (gjenbruker motoren i bil_kupp_analyse)
# ======================================================================

def _forbered_kandidater(cand: pd.DataFrame) -> pd.DataFrame:
    cand = cand.copy()
    cand["forventet_pris"] = np.nan
    cand["hurtigpris"] = np.nan
    cand["innbyttepris"] = np.nan
    cand["modell_nivaa"] = pd.Series("Ingen modell", index=cand.index, dtype="object")
    cand["peer_n"] = 0
    cand["peer_tier"] = 0
    cand["peer_dager_til_salg_median"] = np.nan
    return cand


def _sett_konfidens(cand: pd.DataFrame) -> pd.DataFrame:
    """Samme konfidens-logikk som bil_kupp_analyse.kjor_analyse."""
    konf = pd.Series(0, index=cand.index, dtype="int8")
    t1 = cand["peer_tier"] == 1
    t2 = cand["peer_tier"] == 2
    n = cand["peer_n"]
    konf[t1 & (n >= motor.KONF_HOY_N)] = 1
    konf[t1 & (n >= motor.KONF_OK_N) & (n < motor.KONF_HOY_N)] = 2
    konf[t2 & (n >= motor.KONF_HOY_N)] = 2
    konf[t2 & (n >= motor.KONF_OK_N) & (n < motor.KONF_HOY_N)] = 3
    konf[cand["modell_nivaa"] == "LOOKUP"] = 1
    cand["peer_konfidens"] = konf
    return cand


def scor_kandidater(cand: pd.DataFrame, df_train: pd.DataFrame,
                    bruk_overrides: bool = True) -> pd.DataFrame:
    """Scor kandidat-biler med lookup (Tier 0) + peer-WLS (Tier 1/2)."""
    cand = _forbered_kandidater(cand)

    n0 = motor.kjor_lookup(cand)
    n1 = motor.kjor_tier(cand, df_train,
                         ["Produsent", "Modell", "drivstoff", "hjuldrift"],
                         tier_nr=1, bare_uforklarte=True)
    n2 = motor.kjor_tier(cand, df_train,
                         ["Produsent", "Modell", "drivstoff"],
                         tier_nr=2, bare_uforklarte=True)
    print(f"      Scoret: Tier0(lookup)={n0:,}  Tier1={n1:,}  Tier2={n2:,}")

    wls = cand["peer_tier"].isin([1, 2]) & (cand["modell_nivaa"] != "LOOKUP")
    cand.loc[wls, "modell_nivaa"] = "PEER-WLS-T" + cand.loc[wls, "peer_tier"].astype(str)

    if bruk_overrides:
        try:
            cand = motor.apply_overrides(
                cand, motor.last_overrides(local_path=motor.OVERRIDES_LOCAL_PATH)
            )
        except Exception as e:  # overrides er "nice to have" – ikke la det stoppe
            print(f"      [overrides] hoppet over: {e}")

    cand = _sett_konfidens(cand)
    return cand


# ======================================================================
# Analyse
# ======================================================================

def _rate(mask_utvalg: pd.Series, mask_solgt: pd.Series) -> dict:
    n = int(mask_utvalg.sum())
    n_solgt = int((mask_utvalg & mask_solgt).sum())
    return {"n": n, "n_solgt": n_solgt,
            "andel_pct": round(100 * n_solgt / n, 1) if n else None}


def _prisbaand(sp: float) -> str:
    if pd.isna(sp):
        return "ukjent"
    for grense, navn in [(50_000, "<50k"), (100_000, "50-100k"),
                         (150_000, "100-150k"), (250_000, "150-250k"),
                         (400_000, "250-400k")]:
        if sp < grense:
            return navn
    return ">=400k"


def _rabattbotte(r: float) -> str:
    if pd.isna(r):
        return "ingen score"
    for grense, navn in [(0, "priset over modell"), (5, "0-5%"), (10, "5-10%"),
                         (15, "10-15%"), (20, "15-20%"), (30, "20-30%")]:
        if r < grense:
            return navn
    return ">=30%"


def bygg_moenster(elig: pd.DataFrame, kupp_col: str, solgt_col: str) -> pd.DataFrame:
    """Salgsrate (innen X doegn) for kupp-bilene brutt ned per dimensjon."""
    kupp = elig[elig[kupp_col]].copy()
    rader = []

    def legg_til(dim: str, serie: pd.Series):
        for verdi, sub in kupp.groupby(serie):
            n = len(sub)
            n_solgt = int(sub[solgt_col].sum())
            rader.append({
                "dimensjon": dim, "verdi": str(verdi), "n_kupp": n,
                "n_solgt_innen": n_solgt,
                "andel_pct": round(100 * n_solgt / n, 1) if n else None,
            })

    legg_til("drivstoff", kupp["drivstoff"].map(norm_drivstoff))
    legg_til("prisbaand", kupp["salgspris"].map(_prisbaand))
    legg_til("rabattbotte", kupp["rabatt_pct"].map(_rabattbotte))
    legg_til("konfidens", kupp.get("peer_konfidens", pd.Series(index=kupp.index)))
    legg_til("modell_nivaa", kupp["modell_nivaa"])
    if "fylke" in kupp.columns:
        legg_til("fylke", kupp["fylke"].fillna("ukjent").astype(str))
    legg_til("produsent", kupp["Produsent"].astype(str))

    ut = pd.DataFrame(rader)
    if ut.empty:
        return ut
    # Sorter: innen hver dimensjon, stoerst utvalg foerst
    ut = ut.sort_values(["dimensjon", "n_kupp"], ascending=[True, False])
    return ut.reset_index(drop=True)


def analyser_bommede_elbiler(elig: pd.DataFrame, kupp_col: str,
                             solgt_col: str) -> tuple[pd.DataFrame, dict]:
    """Elbiler solgt innen X doegn som IKKE ble flagget som kupp.
    Returnerer (detaljer, profil)."""
    ev = elig[elig["drivstoff"].map(norm_drivstoff) == "elektrisk"].copy()
    rask = ev[ev[solgt_col]]
    bommet = rask[~rask[kupp_col]].copy()

    # Hvorfor ble de ikke flagget?
    fp = pd.to_numeric(bommet.get("forventet_pris"), errors="coerce")
    rab = pd.to_numeric(bommet.get("rabatt_pct"), errors="coerce")
    aarsak = pd.Series("annet", index=bommet.index, dtype="object")
    aarsak[fp.isna()] = "uten score (modell-gap)"
    aarsak[fp.notna() & (rab <= 0)] = "ikke underpriset (priset >= modell)"
    naer = fp.notna() & (rab > 0)
    aarsak[naer] = "under terskel (naer-bom)"
    # Privat/forhandler-utestenging: hadde nok rabatt, men ble filtrert paa selger
    bands = parse_trapp(RABATT_TRAPP_DEFAULT)
    krav = bommet["salgspris"].apply(
        lambda p: min_rabatt_for_pris(float(p), bands) if pd.notna(p) else np.inf)
    nok_rabatt = rab.notna() & (rab >= krav) & (rab <= 70)
    ikke_privat = ~er_privat_mask(bommet)
    aarsak[nok_rabatt & ikke_privat] = "nok rabatt, men forhandler"
    # "under terskel" = hadde en rabatt, men under kravet (og privat)
    aarsak[naer & (rab < krav)] = "under terskel (naer-bom)"
    bommet["bom_aarsak"] = aarsak
    # Gap til terskel: kun meningsfullt for ekte naer-bom (positiv rabatt under krav)
    naer_bom = (aarsak == "under terskel (naer-bom)")
    bommet["gap_til_terskel_pct"] = np.where(naer_bom, krav - rab, np.nan)

    n_ev_rask = int(len(rask))
    n_bommet = int(len(bommet))
    n_truffet = n_ev_rask - n_bommet
    profil = {
        "elbiler_solgt_innen": n_ev_rask,
        "flagget_kupp": n_truffet,
        "bommet": n_bommet,
        "recall_pct": round(100 * n_truffet / n_ev_rask, 1) if n_ev_rask else None,
        "aarsak_fordeling": bommet["bom_aarsak"].value_counts().to_dict(),
        "median_salgspris": _med(bommet["salgspris"]),
        "median_km": _med(bommet.get("kjørelengde")),
        "median_alder": _med(bommet.get("alder")),
        "median_rabatt_pct": _med(bommet.get("rabatt_pct")),
        "median_gap_til_terskel_pct": _med(bommet.get("gap_til_terskel_pct")),
        "topp_produsent": bommet["Produsent"].astype(str).value_counts().head(8).to_dict(),
    }
    return bommet, profil


def _med(s) -> float | None:
    if s is None:
        return None
    v = pd.to_numeric(s, errors="coerce").median()
    return round(float(v), 1) if pd.notna(v) else None


# ======================================================================
# Orkestrering
# ======================================================================

def kjor_backtest(
    input_path: str | None,
    fra: date,
    dager: int,
    utdir: str,
    frys: bool = False,
    kun_privat: bool = True,
    kun_ja: bool = False,
    maks_rabatt_pct: float | None = 70.0,
    bruk_overrides: bool = True,
) -> dict:
    ref_dato = pd.Timestamp(datetime.now().date())
    df = motor.les_og_klargjor(input_path, ref_dato)

    # Robust dato-parsing (parquet har som regel datetime allerede)
    for c in ("Dato", "Dato_ny"):
        df[c] = pd.to_datetime(df[c], errors="coerce")

    fra_ts = pd.Timestamp(fra)
    df["_solgt_norm"] = df["Solgt"].astype(str).str.strip().str.upper()

    # ---- Treningsdata (samme filter som produksjon) ----
    train_mask = (
        df["_solgt_norm"].isin(SOLGT_VERDIER)
        & df["Dato_ny"].notna()
        & ((ref_dato - df["Dato_ny"]).dt.days <= motor.MAX_HISTORIE_DAGER)
        & df["salgspris"].notna()
        & (df["salgspris"] >= motor.MIN_SALGSPRIS)
        & df["alder"].notna()
    )
    if frys:
        # Walk-forward: modellen "ser" kun salg FOER maalevinduet starter.
        train_mask &= df["Dato_ny"] < fra_ts
    df_train = df[train_mask].copy()
    df_train["vekt"] = motor.beregn_vekter(df_train, ref_dato)
    print(f"      Treningsbiler: {len(df_train):,}"
          + (" (fryst til foer fra-dato)" if frys else " (siste 365 dager)"))

    # ---- Kandidater: alt lagt ut siden fra-datoen ----
    cand = df[df["Dato"].notna() & (df["Dato"] >= fra_ts)].copy()
    print(f"      Kandidater lagt ut siden {fra}: {len(cand):,}")
    cand = scor_kandidater(cand, df_train, bruk_overrides=bruk_overrides)
    cand = beregn_rabatt(cand)

    bands = parse_trapp(RABATT_TRAPP_DEFAULT)
    cand["er_kupp"] = flagg_kupp(cand, bands, kun_privat=kun_privat,
                                 maks_rabatt_pct=maks_rabatt_pct)
    cand["solgt_innen"] = solgt_innen(cand, dager, kun_ja=kun_ja)
    cand["er_privat"] = er_privat_mask(cand)

    # ---- Sensurering: kun biler som fikk minst `dager` doegn paa aa selge ----
    siste_dato = df["Dato_ny"].max()
    cutoff = siste_dato - pd.Timedelta(days=dager)
    cand["_eligible"] = (
        (cand["Dato"] <= cutoff)
        & cand["salgspris"].notna()
        & (cand["salgspris"] >= motor.MIN_SALGSPRIS)
    )
    elig = cand[cand["_eligible"]].copy()
    print(f"      Kvalifiserte for {dager}-doegns maaling (lagt ut <= {cutoff.date()}): "
          f"{len(elig):,}")

    # ---- Hovedtall ----
    kupp_rate = _rate(elig["er_kupp"], elig["solgt_innen"])
    ikke_rate = _rate(~elig["er_kupp"], elig["solgt_innen"])
    lift = (kupp_rate["andel_pct"] / ikke_rate["andel_pct"]
            if (kupp_rate["andel_pct"] and ikke_rate["andel_pct"]) else None)

    moenster = bygg_moenster(elig, "er_kupp", "solgt_innen")
    bommet, ev_profil = analyser_bommede_elbiler(elig, "er_kupp", "solgt_innen")

    # ---- Skriv utdata ----
    os.makedirs(utdir, exist_ok=True)
    kupp_kol = ["FinnKode", "Produsent", "Modell", "årstall", "kjørelengde",
                "drivstoff", "hjuldrift", "fylke", "sted", "salgspris",
                "forventet_pris", "rabatt_pct", "peer_konfidens", "modell_nivaa",
                "er_privat", "Solgt", "dager_til_salg", "solgt_innen",
                "Dato", "Dato_ny", "url"]
    kupp_kol = [c for c in kupp_kol if c in elig.columns]
    kupp_ut = elig[elig["er_kupp"]][kupp_kol].sort_values("rabatt_pct", ascending=False)
    p_kupp = os.path.join(utdir, "kupp_flagget.csv")
    p_moenster = os.path.join(utdir, "kupp_moenster.csv")
    p_bommet = os.path.join(utdir, "bommet_elbiler.csv")
    kupp_ut.to_csv(p_kupp, index=False, sep=";", encoding="utf-8-sig")
    moenster.to_csv(p_moenster, index=False, sep=";", encoding="utf-8-sig")
    bom_kol = [c for c in kupp_kol + ["bom_aarsak", "gap_til_terskel_pct"]
               if c in bommet.columns]
    bommet[bom_kol].sort_values("dager_til_salg").to_csv(
        p_bommet, index=False, sep=";", encoding="utf-8-sig")

    # ---- Sammendrag ----
    print()
    print("=" * 64)
    print(f"KUPP-FASIT  ({fra} -> {siste_dato.date()},  solgt innen {dager} doegn)")
    print("=" * 64)
    print(f"Kandidater lagt ut i perioden:     {len(cand):,}")
    print(f"  - kvalifiserte for maaling:      {len(elig):,}")
    print(f"  - flagget som kupp:              {int(elig['er_kupp'].sum()):,}")
    print()
    print(f"Kupp solgt innen {dager} doegn:          "
          f"{kupp_rate['n_solgt']:,}/{kupp_rate['n']:,}  = {kupp_rate['andel_pct']} %")
    print(f"Ikke-kupp solgt innen {dager} doegn:     "
          f"{ikke_rate['n_solgt']:,}/{ikke_rate['n']:,}  = {ikke_rate['andel_pct']} %")
    if lift:
        print(f"Loeft (kupp / ikke-kupp):          {lift:.1f}x")
    print()
    print(f"--- Elbiler solgt innen {dager} doegn (recall) ---")
    print(f"Totalt raske elbiler:              {ev_profil['elbiler_solgt_innen']:,}")
    print(f"  - fanget som kupp:               {ev_profil['flagget_kupp']:,}  "
          f"(recall {ev_profil['recall_pct']} %)")
    print(f"  - BOMMET:                        {ev_profil['bommet']:,}")
    print(f"  Aarsak til bom:                  {ev_profil['aarsak_fordeling']}")
    print(f"  Median (bommede): pris={ev_profil['median_salgspris']}, "
          f"km={ev_profil['median_km']}, alder={ev_profil['median_alder']}, "
          f"rabatt%={ev_profil['median_rabatt_pct']}, "
          f"gap_til_terskel%={ev_profil['median_gap_til_terskel_pct']}")
    print(f"  Topp produsenter (bommet):       {ev_profil['topp_produsent']}")
    print()
    print("Moenster (topp per dimensjon) – se ogsaa CSV:")
    if not moenster.empty:
        with pd.option_context("display.max_rows", 60, "display.width", 200):
            print(moenster.to_string(index=False))
    print()
    print(f"Filer skrevet til {utdir}/:")
    print(f"  - {os.path.basename(p_kupp)}      (alle flaggede kupp + salgsstatus)")
    print(f"  - {os.path.basename(p_moenster)}     (rate per dimensjon)")
    print(f"  - {os.path.basename(p_bommet)}    (bommede hurtigsolgte elbiler)")

    return {
        "kupp_rate": kupp_rate, "ikke_rate": ikke_rate, "lift": lift,
        "ev_profil": ev_profil, "n_kandidater": int(len(cand)),
        "n_eligible": int(len(elig)),
    }


def main():
    p = argparse.ArgumentParser(
        description="Retrospektiv fasit for kupp-treffsikkerhet.")
    p.add_argument("--input", default=None,
                   help="Lokal database_biler.parquet (default: hent fra S3).")
    p.add_argument("--fra", default=None,
                   help="Fra-dato (YYYY-MM-DD). Default: siste 1. august.")
    p.add_argument("--dager", type=int, default=2, help="Salgsvindu i doegn (default 2).")
    p.add_argument("--utdir", default=".", help="Mappe for CSV-utdata (default: .).")
    p.add_argument("--frys", action="store_true",
                   help="Frys modellen til salg FOER fra-datoen (walk-forward).")
    p.add_argument("--selger", choices=["privat", "alle"], default="privat",
                   help="Kupp kun for private (default) eller alle selgere.")
    p.add_argument("--kun-ja", action="store_true",
                   help="Regn kun 'JA' som solgt (utelat 'FJERNET').")
    p.add_argument("--maks-rabatt", type=float, default=70.0,
                   help="Drop kupp med rabatt over dette (mistenkelig). 0 = av.")
    p.add_argument("--ingen-overrides", action="store_true",
                   help="Ikke bruk manuelle pris-overstyringer.")
    args = p.parse_args()

    ref = date.today()
    fra = (datetime.strptime(args.fra, "%Y-%m-%d").date()
           if args.fra else standard_fra_dato(ref))
    maks = None if args.maks_rabatt in (0, 0.0) else args.maks_rabatt

    kjor_backtest(
        input_path=args.input,
        fra=fra,
        dager=args.dager,
        utdir=args.utdir,
        frys=args.frys,
        kun_privat=(args.selger == "privat"),
        kun_ja=args.kun_ja,
        maks_rabatt_pct=maks,
        bruk_overrides=not args.ingen_overrides,
    )


if __name__ == "__main__":
    main()
