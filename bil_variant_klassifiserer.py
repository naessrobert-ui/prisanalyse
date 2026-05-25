"""
bil_variant_klassifiserer.py - Klassifiserer EV-annonser til batterivarianter
=============================================================================

Bygger paa observasjonen at oppgitt rekkevidde er en stoeyende proxy for det
vi egentlig vil gruppere paa: batteripakken. To Kona med samme 64 kWh batteri
er samme bil, selv om annonsene oppgir 450 og 502 km.

Klassifiseringen er trenivaa per annonse:
  1) Enkel: bare en variant matcher (Produsent, Modell, aarstall).
  2) Alias: annonseteksten (Modell + Info) inneholder en kjent variant-alias
     som "Long Range", "LR", "64 kWh", "Maximum Range", etc.
  3) Rekkevidde: fall tilbake til den katalog-varianten med naermest
     rekkevidde_typisk. Brukes naar verken aarstall eller tekst skiller.

Katalogen ligger som data/bil_varianter.csv og kan utvides med PR.

Bruk:
    from bil_variant_klassifiserer import last_variantkatalog, klassifiser_varianter

    katalog = last_variantkatalog()
    variant_id, kilde = klassifiser_varianter(df, katalog)
    df["variant_id"] = variant_id
    df["variant_kilde"] = kilde
"""

from __future__ import annotations

import os
import re
import threading
from typing import Tuple

import numpy as np
import pandas as pd

VARIANTKATALOG_COLUMNS = [
    "variant_id",
    "Produsent",
    "Modell",
    "aar_fra",
    "aar_til",
    "batteri_kwh",
    "rekkevidde_typisk",
    "aliases",
    "kommentar",
]

VARIANTKATALOG_LOCAL_PATH = os.path.join(
    os.path.dirname(__file__), "data", "bil_varianter.csv"
)

_KATALOG_LOCK = threading.Lock()
_KATALOG_CACHE: dict = {"df": None, "path": None}


def _les_katalog_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    df.columns = [c.strip() for c in df.columns]
    for col in VARIANTKATALOG_COLUMNS:
        if col not in df.columns:
            df[col] = ""
    df = df[VARIANTKATALOG_COLUMNS].copy()

    for col in ("aar_fra", "aar_til"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["batteri_kwh"] = pd.to_numeric(df["batteri_kwh"], errors="coerce")
    df["rekkevidde_typisk"] = pd.to_numeric(df["rekkevidde_typisk"], errors="coerce")

    df = df.dropna(subset=["aar_fra", "aar_til", "rekkevidde_typisk"])
    df = df[df["variant_id"].str.strip().astype(bool)]
    df["Produsent"] = df["Produsent"].str.strip()
    df["Modell"] = df["Modell"].str.strip()
    df["aliases"] = df["aliases"].fillna("")
    return df.reset_index(drop=True)


def last_variantkatalog(path: str | None = None) -> pd.DataFrame:
    """Last variantkatalogen (med cache). Returnerer tom DataFrame hvis ikke funnet."""
    path = path or VARIANTKATALOG_LOCAL_PATH
    with _KATALOG_LOCK:
        if _KATALOG_CACHE["df"] is not None and _KATALOG_CACHE["path"] == path:
            return _KATALOG_CACHE["df"]

        if not os.path.exists(path):
            print(f"[variant] Katalog ikke funnet: {path} - kjorer uten")
            df = pd.DataFrame(columns=VARIANTKATALOG_COLUMNS)
        else:
            try:
                df = _les_katalog_csv(path)
                print(f"[variant] Lastet {len(df)} varianter fra {path}")
            except Exception as e:
                print(f"[variant] Klarte ikke lese {path}: {e}")
                df = pd.DataFrame(columns=VARIANTKATALOG_COLUMNS)

        _KATALOG_CACHE["df"] = df
        _KATALOG_CACHE["path"] = path
        return df


def reload_variantkatalog() -> None:
    """Tving relasting ved neste kall."""
    with _KATALOG_LOCK:
        _KATALOG_CACHE["df"] = None
        _KATALOG_CACHE["path"] = None


_ALIAS_RE_CACHE: dict[str, re.Pattern] = {}


def _alias_regex(alias: str) -> re.Pattern:
    """Cached, case-insensitive ordgrense-regex for en alias."""
    key = alias.lower()
    if key not in _ALIAS_RE_CACHE:
        # Tillat at "65,4 kWh" matcher selv om kommaet er omgitt av tall.
        # \b virker daarlig naar alias inneholder komma/punktum, saa vi
        # bruker en lookaround-variant som krever ikke-bokstav rundt.
        _ALIAS_RE_CACHE[key] = re.compile(
            r"(?<![a-z0-9])" + re.escape(key) + r"(?![a-z0-9])",
            flags=re.IGNORECASE,
        )
    return _ALIAS_RE_CACHE[key]


def klassifiser_varianter(
    df: pd.DataFrame,
    katalog: pd.DataFrame | None = None,
) -> Tuple[pd.Series, pd.Series]:
    """Klassifiser hver rad i df til en variant_id.

    Forventer kolonnene Produsent, Modell, aarstall, og helst rekkevidde_km
    og Info. Manglende kolonner tolereres som tomme strenger / NaN.

    Returnerer to Series som er paralle med df.index:
      variant_id    - streng (variant_id) eller pd.NA hvis ingen treff i katalog
      variant_kilde - "enkel" | "alias" | "rekkevidde" | "ukjent" | pd.NA
                      pd.NA betyr "modellen finnes ikke i katalog".
                      "ukjent" betyr "modellen finnes, men vi klarte ikke aa
                      velge mellom variantene fordi baade tekst og rekkevidde
                      manglet".
    """
    if katalog is None:
        katalog = last_variantkatalog()

    n = len(df)
    out_id: np.ndarray = np.empty(n, dtype=object)
    out_src: np.ndarray = np.empty(n, dtype=object)
    out_id[:] = None
    out_src[:] = None

    if katalog is None or katalog.empty or n == 0:
        return (
            pd.Series(out_id, index=df.index, dtype="object"),
            pd.Series(out_src, index=df.index, dtype="object"),
        )

    # Foerhaandsregn lave/numeriske kolonner en gang. Soketeksten samler
    # alle tilgjengelige variant-relevante felter; ulike datakilder har ulike
    # kolonner (annonsene har Overskrift, gamle dumper har Info,
    # SVV-anriket data har svv_handelsbetegnelse).
    modell_raw = df.get("Modell", pd.Series([""] * n, index=df.index))
    produsent_raw = df.get("Produsent", pd.Series([""] * n, index=df.index))

    modell_lo = modell_raw.fillna("").astype(str).str.lower()
    produsent_lo_arr = produsent_raw.fillna("").astype(str).str.strip().str.lower().values

    tekst_kilder = [modell_lo]
    for kolonne in ("Overskrift", "Info", "svv_handelsbetegnelse"):
        if kolonne in df.columns:
            tekst_kilder.append(
                df[kolonne].fillna("").astype(str).str.lower()
            )
    sok_tekst = tekst_kilder[0]
    for s in tekst_kilder[1:]:
        sok_tekst = sok_tekst + " " + s
    sok_tekst = sok_tekst.values  # ndarray av str

    aar = pd.to_numeric(
        df.get("aarstall", df.get("årstall")), errors="coerce"
    ).to_numpy(dtype=float, na_value=np.nan)

    # Rekkevidde: foretrekk wltp, fall til rekkevidde_km. Filtrer urealistiske
    # verdier (folk skriver km-stand i feil felt; vi har sett opp til 514000).
    rekke_raw = None
    for kolonne in ("rekkevidde_km", "rekkevidde_wltp_km", "rekkevidde_str"):
        if kolonne in df.columns:
            kand = pd.to_numeric(df[kolonne], errors="coerce")
            rekke_raw = kand if rekke_raw is None else rekke_raw.where(rekke_raw.notna(), kand)
    if rekke_raw is None:
        rekke_raw = pd.Series([np.nan] * n, index=df.index, dtype=float)
    rekke = (
        rekke_raw.where((rekke_raw >= 50) & (rekke_raw <= 1200), np.nan)
        .to_numpy(dtype=float, na_value=np.nan)
    )

    # Batterikapasitet i kWh - mest paalitelige signal naar det er fylt ut.
    if "batterikapasitet_kwh" in df.columns:
        kwh_serie = pd.to_numeric(df["batterikapasitet_kwh"], errors="coerce")
        # Sanity: EV-batterier er typisk 15-200 kWh. Alt utenfor er feil.
        kwh = (
            kwh_serie.where((kwh_serie >= 15) & (kwh_serie <= 200), np.nan)
            .to_numpy(dtype=float, na_value=np.nan)
        )
    else:
        kwh = np.full(n, np.nan, dtype=float)

    # Pre-prosesser katalog: gruppe per (produsent_lo, modell_lo). Sorter
    # slik at LENGRE modell-strenger prosesseres foerst - da vinner mer
    # spesifikke treff (f.eks. "Niro EV" prosesseres foer "Niro", "Q4 e-tron"
    # foer "e-tron"). I kombinasjon med skip-naar-satt-logikken under sikrer
    # det longest-match-vinner semantikk.
    kat = katalog.assign(
        _p_lo=katalog["Produsent"].astype(str).str.strip().str.lower(),
        _m_lo=katalog["Modell"].astype(str).str.strip().str.lower(),
    )
    kat = kat.assign(_m_len=kat["_m_lo"].str.len()).sort_values(
        "_m_len", ascending=False, kind="stable"
    ).drop(columns=["_m_len"])

    for (p_lo, m_lo), kand_df in kat.groupby(["_p_lo", "_m_lo"], sort=False):
        # Match katalog-modell mot annonse-Modell med ordgrense - hindrer at
        # "iX" treffer "iX3" og at "e-Niro" treffer "Niro EV". Bruker samme
        # ordgrense-definisjon som alias-matchingen.
        m_pat = re.compile(
            r"(?<![a-z0-9])" + re.escape(m_lo) + r"(?![a-z0-9])"
        )
        base_mask = (produsent_lo_arr == p_lo) & modell_lo.str.contains(
            m_pat.pattern, regex=True, na=False
        ).values
        idx_match = np.flatnonzero(base_mask)
        if idx_match.size == 0:
            continue

        # Forhaandskonverter kandidater til en lett liste av dict +
        # forkompilerte alias-regexer.
        kand: list[dict] = []
        for _, v in kand_df.iterrows():
            aliases_raw = str(v.get("aliases") or "")
            alias_patterns = [
                _alias_regex(a.strip())
                for a in aliases_raw.split("|")
                if a.strip()
            ]
            kand.append(
                {
                    "variant_id": v["variant_id"],
                    "aar_fra": float(v["aar_fra"]),
                    "aar_til": float(v["aar_til"]),
                    "batteri_kwh": float(v.get("batteri_kwh") or 0) or float("nan"),
                    "rekkevidde_typisk": float(v["rekkevidde_typisk"]),
                    "alias_patterns": alias_patterns,
                }
            )

        for i in idx_match:
            # Skip hvis allerede klassifisert av en mer spesifikk modell-treff
            # (vi prosesserer lengre modell-strenger foerst).
            if out_id[i] is not None:
                continue

            a = aar[i]
            if np.isnan(a):
                applicable = kand
            else:
                applicable = [
                    k for k in kand if k["aar_fra"] <= a <= k["aar_til"]
                ]
            if not applicable:
                continue
            if len(applicable) == 1:
                out_id[i] = applicable[0]["variant_id"]
                out_src[i] = "enkel"
                continue

            # Niva 1 - batterikapasitet i kWh (mest paalitelige signal).
            # Filtrer kandidater til de innenfor +/-5 kWh av oppgitt verdi.
            kwh_i = kwh[i]
            filtrert_kwh: list[dict] = []
            if not np.isnan(kwh_i):
                filtrert_kwh = [
                    k for k in applicable
                    if not np.isnan(k["batteri_kwh"])
                    and abs(k["batteri_kwh"] - kwh_i) <= 5.0
                ]
                if len(filtrert_kwh) == 1:
                    out_id[i] = filtrert_kwh[0]["variant_id"]
                    out_src[i] = "kwh"
                    continue
                if filtrert_kwh:
                    # Flere varianter innenfor toleransen (f.eks. Model 3 LR og
                    # Performance har samme batteri) - bruk denne mindre listen
                    # videre i alias/rekkevidde-trinnene.
                    applicable = filtrert_kwh

            # Niva 2 - alias-treff i Modell + Overskrift + svv-felter
            tekst_i = sok_tekst[i]
            chosen = None
            for k in applicable:
                if any(p.search(tekst_i) for p in k["alias_patterns"]):
                    chosen = k["variant_id"]
                    break
            if chosen is not None:
                out_id[i] = chosen
                out_src[i] = "kwh+alias" if filtrert_kwh else "alias"
                continue

            # Niva 3 - naermeste rekkevidde. Rekkevidde over 1200 km er
            # filtrert ut allerede (datafeil); 0 betyr typisk ikke-utfylt.
            r = rekke[i]
            if not np.isnan(r) and r > 0:
                best = min(
                    applicable, key=lambda k: abs(k["rekkevidde_typisk"] - r)
                )
                out_id[i] = best["variant_id"]
                out_src[i] = "kwh+rekkevidde" if filtrert_kwh else "rekkevidde"
            else:
                out_id[i] = None
                out_src[i] = "ukjent"

    return (
        pd.Series(out_id, index=df.index, dtype="object"),
        pd.Series(out_src, index=df.index, dtype="object"),
    )
