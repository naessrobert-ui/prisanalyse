"""
bil_kupp_analyse.py — Finner underprisede biler i markedet.

Skriver bilradar_aktive.parquet (kilde for /radar) med SAMME motor som
/finn-sok og /innbytte: lookup/variant-tabellen er primær, peer-gruppe-WLS
er fallback for biler uten lookup-treff.

Algoritme:
  0. Tier 0 (primær): lookup/variant-tabellen (bilradar_lookup) — riktig
     batteripakke/rekkevidde for elbil, prosentvis km-justering, hurtigpris
     og innbyttepris. Dekker det lookup-tabellen har data for.
  1. Tier 1 fallback: (Produsent, Modell, drivstoff, hjuldrift)
     -> WLS log(pris) ~ alder + km_norm, mot tidsvektet historikk (JA+FJERNET)
  2. Tier 2 fallback: (Produsent, Modell, drivstoff) — for små grupper
  + Manuelle overstyringer (bilradar_overrides) som i live-scoringen.

Output:
  - parquet med forventet_pris, hurtigpris, innbyttepris, rabatt_kr,
    rabatt_pct, modell_nivaa, peer_n, peer_tier, peer_konfidens,
    peer_dager_til_salg_median for alle aktive biler
  - CSV topplist sortert på rabatt_pct (filtrert på konfidens 1-2)

Kjøring:
  python bil_kupp_analyse.py                       # leser fra S3
  python bil_kupp_analyse.py --input <lokal.parquet>
  python bil_kupp_analyse.py --top 500
"""
from __future__ import annotations

import argparse
import io
import os
import sys
import tempfile
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

from bilradar_lookup import apply_lookup, last_lookup
from bilradar_overrides import apply_overrides, last_overrides

S3_KEY_INPUT = "calc/bil/database_biler.parquet"
S3_KEY_OUTPUT = "calc/bil/bilradar_aktive.parquet"
# Peer-WLS koeffisient-tabell (live-fallback for /finn-sok). Må matche
# BILRADAR_PEER_S3_KEY-defaulten i bilradar_peer.py.
S3_KEY_PEER = os.getenv("BILRADAR_PEER_S3_KEY", "calc/bil/peer_koeffisienter.csv")
DEFAULT_OUTPUT_PARQUET = r"C:\Users\Rober\Downloads\bilradar_aktive.parquet"
DEFAULT_OUTPUT_CSV = r"C:\Users\Rober\Downloads\bil_kupp_topplist.csv"
DEFAULT_TOP = 200

# Lokale kilder for lookup/overrides (committed fallback; S3 foretrekkes i drift).
_DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
LOOKUP_LOCAL_PATH = os.path.join(_DATA_DIR, "prislookup.csv")
OVERRIDES_LOCAL_PATH = os.path.join(_DATA_DIR, "pris_overstyring.csv")
INNBYTTE_RABATT = 0.15  # innbyttepris = forventet_pris * (1 - INNBYTTE_RABATT)

MIN_PEERS = 8
MAX_HISTORIE_DAGER = 365
HALVERINGSTID_DAGER = 120
FJERNET_VEKTSCALE = 0.85
MIN_KM_PER_AAR = 4_000
MIN_SALGSPRIS = 15_000          # under dette er det mest sannsynlig feil-listinger
MISTENKELIG_RABATT_PCT = 70.0   # rabatt over dette flagges som mistenkelig pris
TOPPLIST_MAX_RABATT_PCT = 70.0  # samme grense brukes for topplisten

KONF_HOY_N = 20  # tier 1 + n >= 20 -> konfidens 1
KONF_OK_N = 8    # tier 1 + n >= 8  -> konfidens 2 (eller tier 2 + n >= 20)


def km_normalisert(alder, km):
    """Hindrer urealistisk lav km på eldre biler fra å gi for høy predikert pris."""
    alder = np.asarray(alder, dtype=float)
    km = np.asarray(km, dtype=float)
    gulv = np.maximum(alder, 0) * MIN_KM_PER_AAR
    return np.where(alder >= 10, np.maximum(km, gulv), np.maximum(km, 0))


def _s3_klient():
    import boto3
    from config import AWS_KEY, AWS_SECRET, AWS_REGION
    return boto3.client(
        "s3", region_name=AWS_REGION,
        aws_access_key_id=AWS_KEY, aws_secret_access_key=AWS_SECRET,
    )


def _hent_parquet_fra_s3() -> str:
    """Laster ned database_biler.parquet fra S3 til lokal /tmp-cache.
    Bruker ETag for å unngå unødvendig nedlasting. Returnerer lokal sti."""
    from config import S3_BUCKET_NAME
    s3 = _s3_klient()
    head = s3.head_object(Bucket=S3_BUCKET_NAME, Key=S3_KEY_INPUT)
    etag = (head.get("ETag") or "").strip('"')

    cache_dir = Path(tempfile.gettempdir()) / "bil_kupp_analyse"
    cache_dir.mkdir(exist_ok=True)
    local_path = cache_dir / f"database_biler_{etag}.parquet"

    if local_path.exists():
        print(f"      S3 cache hit (ETag {etag[:8]}): {local_path}")
        return str(local_path)

    for old in cache_dir.glob("database_biler_*.parquet"):
        try:
            old.unlink()
        except OSError:
            pass

    print(f"      Laster s3://{S3_BUCKET_NAME}/{S3_KEY_INPUT} (ETag {etag[:8]}) ...")
    obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key=S3_KEY_INPUT)
    data = obj["Body"].read()
    local_path.write_bytes(data)
    print(f"      Lagret {len(data) / 1024 / 1024:.1f} MB -> {local_path}")
    return str(local_path)


def _last_opp_til_s3(local_path: str, s3_key: str) -> None:
    from config import S3_BUCKET_NAME
    s3 = _s3_klient()
    print(f"      Laster opp -> s3://{S3_BUCKET_NAME}/{s3_key} ...")
    with open(local_path, "rb") as f:
        s3.put_object(
            Bucket=S3_BUCKET_NAME, Key=s3_key, Body=f.read(),
            ContentType="application/octet-stream",
        )
    print(f"      OK")


def les_og_klargjor(path: str | None, ref_dato: pd.Timestamp) -> pd.DataFrame:
    print("[1/6] Leser parquet ...")
    if not path:
        path = _hent_parquet_fra_s3()
    df = pd.read_parquet(path)
    print(f"      {len(df):,} rader, {df.shape[1]} kolonner ({path})")

    # database_biler.parquet lagrer mange strengkolonner som 'category'. fillna/
    # map med nye verdier feiler paa Categorical, saa konverter til object foerst.
    cat_cols = [c for c in df.columns if isinstance(df[c].dtype, pd.CategoricalDtype)]
    if cat_cols:
        df[cat_cols] = df[cat_cols].astype(object)

    for col in ["Produsent", "Modell", "drivstoff", "hjuldrift"]:
        df[col] = (
            df[col].fillna("Ukjent").astype(str).str.strip()
            .replace({"": "Ukjent", "None": "Ukjent", "nan": "Ukjent"})
        )

    df["årstall"] = pd.to_numeric(df["årstall"], errors="coerce")
    df["kjørelengde"] = pd.to_numeric(df["kjørelengde"], errors="coerce")
    df["Pris"] = pd.to_numeric(df["Pris"], errors="coerce")
    df["Pris_ny"] = pd.to_numeric(df["Pris_ny"], errors="coerce")

    df["salgspris"] = df["Pris_ny"].where(df["Pris_ny"].fillna(0) > 0, df["Pris"])
    df["alder"] = (ref_dato.year - df["årstall"]).clip(lower=0)
    df["km_norm"] = km_normalisert(
        df["alder"].fillna(0).values, df["kjørelengde"].fillna(0).values
    )

    # Dato = først sett, Dato_ny = sist sett (sannsynlig solgt-dato for JA/FJERNET)
    days_to_sell = (df["Dato_ny"] - df["Dato"]).dt.days
    df["dager_til_salg"] = days_to_sell.where(days_to_sell.between(0, 365), np.nan)

    return df


def beregn_vekter(df_train: pd.DataFrame, ref_dato: pd.Timestamp) -> np.ndarray:
    # Bruk sist sett (Dato_ny) som "transaksjonstidspunkt" — det er da bilen forsvant
    days_since = (ref_dato - df_train["Dato_ny"]).dt.days.clip(lower=0).values
    decay = np.power(0.5, days_since / HALVERINGSTID_DAGER)
    fjernet_mask = df_train["Solgt"].values == "FJERNET"
    return np.where(fjernet_mask, FJERNET_VEKTSCALE, 1.0) * decay


def fit_wls(X: np.ndarray, y: np.ndarray, w: np.ndarray) -> np.ndarray:
    sw = np.sqrt(w)
    Xw = X * sw[:, None]
    yw = y * sw
    beta, *_ = np.linalg.lstsq(Xw, yw, rcond=None)
    return beta


def _features(sub: pd.DataFrame) -> np.ndarray:
    return np.column_stack([
        np.ones(len(sub)),
        sub["alder"].values,
        sub["km_norm"].values / 100_000.0,
    ])


def bygg_peer_koef_tabell(df_train: pd.DataFrame) -> pd.DataFrame:
    """Eksporter peer-WLS-koeffisienter per gruppe til en liten tabell som
    /finn-sok bruker som live-fallback (uten aa laste ML-modellen). Samme fit
    som kjor_tier, men lagret som beta0/beta1/beta2 med normaliserte noekler
    slik at live FINN-data matcher treningen."""
    from bilradar_lookup import _normaliser_hjuldrift

    d = df_train.copy()
    d["prod_key"] = d["Produsent"].astype(str).str.strip().str.lower()
    d["modell_key"] = d["Modell"].astype(str).str.strip().str.lower()
    d["driv_key"] = d["drivstoff"].astype(str).str.strip().str.lower()
    d["hjul_key"] = (
        d["hjuldrift"].map(_normaliser_hjuldrift).astype(str).str.strip().str.lower()
    )

    tiers = [
        (1, ["prod_key", "modell_key", "driv_key", "hjul_key"]),
        (2, ["prod_key", "modell_key", "driv_key"]),
    ]
    rader: list[dict] = []
    for tier_nr, keys in tiers:
        for key, sub in d.groupby(keys, sort=False, observed=True):
            if len(sub) < MIN_PEERS:
                continue
            X = _features(sub)
            y = np.log(sub["salgspris"].values)
            w = sub["vekt"].values
            if not np.all(np.isfinite(y)) or w.sum() <= 0:
                continue
            try:
                beta = fit_wls(X, y, w)
            except np.linalg.LinAlgError:
                continue
            if not np.all(np.isfinite(beta)):
                continue
            kv = key if isinstance(key, tuple) else (key,)
            row = dict(zip(keys, kv))
            row.setdefault("hjul_key", "")
            row.update(
                tier=tier_nr, n_obs=int(len(sub)),
                beta0=round(float(beta[0]), 6),
                beta1=round(float(beta[1]), 6),
                beta2=round(float(beta[2]), 6),
            )
            rader.append(row)

    cols = ["prod_key", "modell_key", "driv_key", "hjul_key",
            "tier", "n_obs", "beta0", "beta1", "beta2"]
    return pd.DataFrame(rader, columns=cols)


def kjor_lookup(df_aktive: pd.DataFrame) -> int:
    """Tier 0 (primær): scor aktive biler med lookup/variant-tabellen — samme
    motor som /finn-sok og /innbytte. Setter forventet_pris, hurtigpris og
    modell_nivaa='LOOKUP' for biler med treff. Returnerer antall dekket.

    Rører kun forventet_pris/hurtigpris/modell_nivaa på df_aktive — peer_n/
    peer_tier/peer_konfidens (WLS-semantikk) berøres ikke, selv om apply_lookup
    internt setter peer_konfidens på sin egen kopi."""
    lookup = last_lookup(local_path=LOOKUP_LOCAL_PATH)
    if lookup is None or lookup.empty:
        print("      Ingen lookup-tabell tilgjengelig — hopper over Tier 0")
        return 0

    # apply_lookup klassifiserer variant live og leser Produsent/Modell/
    # drivstoff/hjuldrift/årstall/kjørelengde (+ rekkevidde/Overskrift/kWh for
    # variant). Alle finnes allerede på df_aktive fra parquet-en.
    scored = apply_lookup(df_aktive.copy(), lookup)
    if "modell_nivaa" not in scored.columns:
        return 0
    mask = (scored["modell_nivaa"] == "LOOKUP").values

    idx = df_aktive.index[mask]
    df_aktive.loc[idx, "forventet_pris"] = scored.loc[mask, "forventet_pris"].values
    if "hurtigpris" in scored.columns:
        df_aktive.loc[idx, "hurtigpris"] = scored.loc[mask, "hurtigpris"].values
    df_aktive.loc[idx, "modell_nivaa"] = "LOOKUP"
    return int(mask.sum())


def kjor_tier(
    df_aktive: pd.DataFrame,
    df_train: pd.DataFrame,
    tier_keys: list,
    tier_nr: int,
    bare_uforklarte: bool,
) -> int:
    """Fitter en WLS-modell per peer-gruppe og predikerer for aktive biler.
    Returnerer antall aktive biler som fikk prediksjon."""
    train_groups = df_train.groupby(tier_keys, sort=False, observed=True)
    train_keys = set(train_groups.groups.keys())

    if bare_uforklarte:
        akt_mask = df_aktive["forventet_pris"].isna()
    else:
        akt_mask = pd.Series(True, index=df_aktive.index)

    if not akt_mask.any():
        return 0

    n_dekket = 0
    for key, akt_sub in df_aktive[akt_mask].groupby(tier_keys, sort=False, observed=True):
        if key not in train_keys:
            continue
        train_sub = train_groups.get_group(key)
        if len(train_sub) < MIN_PEERS:
            continue

        X = _features(train_sub)
        y = np.log(train_sub["salgspris"].values)
        w = train_sub["vekt"].values
        if not np.all(np.isfinite(y)) or w.sum() <= 0:
            continue
        try:
            beta = fit_wls(X, y, w)
        except np.linalg.LinAlgError:
            continue

        Xp = _features(akt_sub)
        log_pred = Xp @ beta
        pred = np.exp(log_pred)
        # Drop urealistiske prediksjoner
        gyldig = np.isfinite(pred) & (pred > 1000) & (pred < 50_000_000)
        if not gyldig.any():
            continue

        idx = akt_sub.index[gyldig]
        df_aktive.loc[idx, "forventet_pris"] = pred[gyldig]
        df_aktive.loc[idx, "peer_n"] = len(train_sub)
        df_aktive.loc[idx, "peer_tier"] = tier_nr
        df_aktive.loc[idx, "peer_dager_til_salg_median"] = train_sub["dager_til_salg"].median()
        n_dekket += int(gyldig.sum())

    return n_dekket


def kjor_analyse(
    input_path: str | None,
    output_parquet: str,
    output_csv: str,
    top_n: int,
    last_opp: bool = True,
) -> None:
    ref_dato = pd.Timestamp(datetime.now().date())
    df = les_og_klargjor(input_path, ref_dato)

    print("[2/6] Klargjør treningsdata (JA + FJERNET innenfor "
          f"{MAX_HISTORIE_DAGER} dager, salgspris > {MIN_SALGSPRIS:,}) ...")
    df_train = df[df["Solgt"].isin(["JA", "FJERNET"])].copy()
    df_train = df_train[
        df_train["Dato_ny"].notna()
        & ((ref_dato - df_train["Dato_ny"]).dt.days <= MAX_HISTORIE_DAGER)
        & df_train["salgspris"].notna()
        & (df_train["salgspris"] >= MIN_SALGSPRIS)
        & df_train["alder"].notna()
    ].copy()
    df_train["vekt"] = beregn_vekter(df_train, ref_dato)
    print(f"      {len(df_train):,} treningsbiler")

    # Eksporter peer-WLS-koeffisienter som /finn-sok bruker som live-fallback.
    try:
        peer_koef = bygg_peer_koef_tabell(df_train)
        peer_local = os.path.join(_DATA_DIR, "peer_koeffisienter.csv")
        os.makedirs(_DATA_DIR, exist_ok=True)
        peer_koef.to_csv(peer_local, index=False, encoding="utf-8-sig")
        print(f"      Peer-koeff: {len(peer_koef):,} grupper -> {peer_local}")
        if last_opp:
            _last_opp_til_s3(peer_local, S3_KEY_PEER)
    except Exception as e:
        print(f"      [peer] klarte ikke bygge/laste opp koeffisienter: {e}")

    print("[3/6] Klargjør aktive biler (Solgt=NEI) ...")
    df_aktive = df[df["Solgt"] == "NEI"].copy()
    df_aktive = df_aktive[
        df_aktive["salgspris"].notna()
        & (df_aktive["salgspris"] >= MIN_SALGSPRIS)
        & df_aktive["alder"].notna()
    ].copy()
    df_aktive["forventet_pris"] = np.nan
    df_aktive["hurtigpris"] = np.nan
    df_aktive["innbyttepris"] = np.nan
    df_aktive["modell_nivaa"] = pd.Series("Ingen modell", index=df_aktive.index, dtype="object")
    df_aktive["peer_n"] = 0
    df_aktive["peer_tier"] = 0
    df_aktive["peer_dager_til_salg_median"] = np.nan
    print(f"      {len(df_aktive):,} aktive biler")

    print("[4/6] Tier 0: lookup/variant (primær motor) ...")
    n0 = kjor_lookup(df_aktive)
    print(f"      Tier 0 (lookup) dekket {n0:,} biler")

    print("      Tier 1: Produsent + Modell + drivstoff + hjuldrift (WLS-fallback) ...")
    n1 = kjor_tier(
        df_aktive, df_train,
        ["Produsent", "Modell", "drivstoff", "hjuldrift"], tier_nr=1,
        bare_uforklarte=True,
    )
    print(f"      Tier 1 dekket {n1:,} ekstra biler")

    print("[5/6] Tier 2: Produsent + Modell + drivstoff (WLS-fallback) ...")
    n2 = kjor_tier(
        df_aktive, df_train,
        ["Produsent", "Modell", "drivstoff"], tier_nr=2,
        bare_uforklarte=True,
    )
    print(f"      Tier 2 dekket {n2:,} ekstra biler")
    # WLS-fallback får modell_nivaa satt fra peer_tier (LOOKUP-rader beholdes).
    wls_mask = (df_aktive["peer_tier"].isin([1, 2])) & (df_aktive["modell_nivaa"] != "LOOKUP")
    df_aktive.loc[wls_mask, "modell_nivaa"] = "PEER-WLS-T" + df_aktive.loc[wls_mask, "peer_tier"].astype(str)

    print("[6/6] Overstyringer, rabatt og innbytte, lagrer ...")
    # Manuelle overstyringer på forventet_pris — samme lag som live-scoringen.
    df_aktive = apply_overrides(df_aktive, last_overrides(local_path=OVERRIDES_LOCAL_PATH))

    mask = df_aktive["forventet_pris"].notna() & (df_aktive["forventet_pris"] > 0)
    df_aktive["rabatt_kr"] = np.nan
    df_aktive["rabatt_pct"] = np.nan
    df_aktive.loc[mask, "rabatt_kr"] = (
        df_aktive.loc[mask, "forventet_pris"] - df_aktive.loc[mask, "salgspris"]
    )
    df_aktive.loc[mask, "rabatt_pct"] = (
        df_aktive.loc[mask, "rabatt_kr"] / df_aktive.loc[mask, "forventet_pris"] * 100
    )

    # Innbyttepris uniformt (som i scorer_biler): 15 % under (evt. overstyrt)
    # forventet pris, gulvet mot hurtigpris når den finnes.
    innbytte = df_aktive["forventet_pris"] * (1 - INNBYTTE_RABATT)
    innbytte = pd.concat([innbytte, df_aktive["hurtigpris"]], axis=1).min(axis=1)
    df_aktive["innbyttepris"] = innbytte.where(mask)

    konf = pd.Series(0, index=df_aktive.index, dtype="int8")
    t1 = df_aktive["peer_tier"] == 1
    t2 = df_aktive["peer_tier"] == 2
    konf[t1 & (df_aktive["peer_n"] >= KONF_HOY_N)] = 1
    konf[t1 & (df_aktive["peer_n"] >= KONF_OK_N) & (df_aktive["peer_n"] < KONF_HOY_N)] = 2
    konf[t2 & (df_aktive["peer_n"] >= KONF_HOY_N)] = 2
    konf[t2 & (df_aktive["peer_n"] >= KONF_OK_N) & (df_aktive["peer_n"] < KONF_HOY_N)] = 3
    # Lookup/variant er den betrodde primærmotoren -> høy konfidens.
    konf[df_aktive["modell_nivaa"] == "LOOKUP"] = 1
    df_aktive["peer_konfidens"] = konf

    # Flag mistenkelig høy "rabatt" — typisk feil-listinger eller bait
    df_aktive["mistenkelig_pris"] = (
        df_aktive["rabatt_pct"].fillna(0) > MISTENKELIG_RABATT_PCT
    ).astype("int8")

    out_cols = [
        "FinnKode", "Produsent", "Modell", "Overskrift",
        "årstall", "kjørelengde",
        "girkasse", "drivstoff", "hjuldrift", "Karosseri",
        "Pris", "Pris_ny", "salgspris",
        "Dato", "Dato_ny",
        "fylke", "sted", "selger", "forhandler", "BildeURL", "url",
        "Solgt",
        "forventet_pris", "hurtigpris", "innbyttepris",
        "rabatt_kr", "rabatt_pct", "modell_nivaa",
        "peer_n", "peer_tier", "peer_konfidens",
        "peer_dager_til_salg_median", "mistenkelig_pris",
    ]
    out_cols = [c for c in out_cols if c in df_aktive.columns]
    df_aktive[out_cols].to_parquet(output_parquet, index=False)
    print(f"      Lagret {len(df_aktive):,} biler -> {output_parquet}")

    if last_opp:
        _last_opp_til_s3(output_parquet, S3_KEY_OUTPUT)

    topp = df_aktive[
        df_aktive["forventet_pris"].notna()
        & df_aktive["peer_konfidens"].between(1, 2)
        & (df_aktive["rabatt_pct"] > 0)
        & (df_aktive["rabatt_pct"] <= TOPPLIST_MAX_RABATT_PCT)
    ].sort_values("rabatt_pct", ascending=False).head(top_n)
    topp[out_cols].to_csv(output_csv, index=False, sep=";", encoding="utf-8-sig")
    print(f"      Topplist (n={len(topp)}) -> {output_csv}")

    print()
    print("===== SAMMENDRAG =====")
    print(f"Aktive biler totalt:     {len(df_aktive):,}")
    print(f"Med forventet pris:      {df_aktive['forventet_pris'].notna().sum():,}")
    print(f"  - Tier 0 (lookup):     {(df_aktive['modell_nivaa'] == 'LOOKUP').sum():,}")
    print(f"  - Tier 1 (WLS):        {(df_aktive['peer_tier'] == 1).sum():,}")
    print(f"  - Tier 2 (WLS):        {(df_aktive['peer_tier'] == 2).sum():,}")
    print(f"  - Uten match:          {df_aktive['forventet_pris'].isna().sum():,}")
    print(f"Med hurtigpris/innbytte: {df_aktive['innbyttepris'].notna().sum():,}")
    print()
    print("Konfidens-fordeling (0=ingen, 1=høy, 2=ok, 3=lav):")
    print(df_aktive["peer_konfidens"].value_counts().sort_index().to_string())
    print()
    print(f"Mistenkelige listinger (rabatt > {MISTENKELIG_RABATT_PCT:.0f}%): "
          f"{int(df_aktive['mistenkelig_pris'].sum()):,}")
    print()
    print(f"Topp 15 kuppliste (konfidens 1-2, rabatt 0-{TOPPLIST_MAX_RABATT_PCT:.0f}%):")
    show_cols = [
        "Produsent", "Modell", "årstall", "kjørelengde", "salgspris",
        "forventet_pris", "rabatt_pct", "peer_n", "peer_konfidens",
        "peer_dager_til_salg_median",
    ]
    show_cols = [c for c in show_cols if c in topp.columns]
    with pd.option_context("display.max_rows", 20, "display.max_columns", None,
                           "display.width", 200, "display.float_format",
                           lambda x: f"{x:,.0f}" if abs(x) > 100 else f"{x:.1f}"):
        print(topp[show_cols].head(15).to_string(index=False))


def main():
    parser = argparse.ArgumentParser(
        description="Finn underprisede biler i markedet (leser fra S3 by default).",
    )
    parser.add_argument(
        "--input", default=None,
        help="Lokal parquet-sti (hvis ikke satt: lastes fra S3)",
    )
    parser.add_argument("--output-parquet", default=DEFAULT_OUTPUT_PARQUET)
    parser.add_argument("--output-csv", default=DEFAULT_OUTPUT_CSV)
    parser.add_argument("--top", type=int, default=DEFAULT_TOP)
    parser.add_argument(
        "--no-upload", action="store_true",
        help=f"Ikke last opp til s3://.../{S3_KEY_OUTPUT}",
    )
    args = parser.parse_args()
    if args.input and not Path(args.input).exists():
        print(f"FEIL: Inputfil ikke funnet: {args.input}", file=sys.stderr)
        sys.exit(2)
    kjor_analyse(
        args.input, args.output_parquet, args.output_csv, args.top,
        last_opp=not args.no_upload,
    )


if __name__ == "__main__":
    main()
