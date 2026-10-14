# handler_varsel.py
"""E-postvarsel for handler på Oslo Børs, kjøres etter opplasting av nye data.

Ser på handlene i de nye datoene (vinduet) og sender én samlet e-post, bare hvis
noe er av betydning:

  1. Følgeliste: alle handler fra investorene i de eksisterende listene
     (Beste.csv, Viktige.csv eller listene i HANDLER_VARSEL_LISTER).
  2. Flinke investorer som snur: de beste investorene siste år (rangert på
     gevinst %) som starter en ny handelsrekke: ny posisjon, første kjøp/salg
     på lenge, eller snur fra kjøp til salg og omvendt. Også helt ute.
  3. Topp 10-eier begynner å selge: en salgsrekke starter hos en investor som
     var blant de 10 største eierne i aksjen dagen før.

Bare starten på en rekke varsles, ikke hver dag den pågår.

Miljøvariabler (samme SMTP-oppsett som flypris-varselet):
    SMTP_HOST, SMTP_PORT (587), SMTP_USER, SMTP_PASSWORD
    HANDLER_VARSEL_TIL     mottaker(e), kommaseparert (fallback MEDIA_DIGEST_TO)
    HANDLER_VARSEL_FRA     avsender (fallback MEDIA_DIGEST_FROM / SMTP_USER)
    HANDLER_VARSEL_AV      sett til 1 for å skru av automatisk varsling
Terskler (valgfrie):
    HANDLER_VARSEL_LISTER        listefiler for følgelisten (default: Beste.csv,Viktige.csv)
    HANDLER_VARSEL_MIN_MNOK      minste beløp for signal i del 2 og 3 (default 5)
    HANDLER_VARSEL_MIN_ANDEL     eller minste andel av beholdningen i % (default 10)
    HANDLER_VARSEL_PAUSE_DAGER   «første på lenge» = ingen handel på så mange dager (default 90)
    HANDLER_VARSEL_MAKS_OPPHOLD  opphold som bryter en rekke, dager (default 7)
    HANDLER_VARSEL_ANTALL_FLINKE hvor mange flinke investorer som følges (default 50)
    HANDLER_VARSEL_EKSKLUDER     navnemønstre som utelates i del 2 og 3 (nominee o.l.)

Kommandolinje:
    python -m handler_varsel --fra 2026-10-13 --til 2026-10-14            # vis, ikke send
    python -m handler_varsel --fra 2026-10-13 --til 2026-10-14 --send
    python -m handler_varsel --test                                       # test-e-post
"""
from __future__ import annotations

import datetime as dt
import html
import json
import logging
import os
import smtplib
import sqlite3
import threading
import urllib.parse
from dataclasses import dataclass, field
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

import pandas as pd

import handler_data as hd

_LOG = logging.getLogger(__name__)

BASE_URL = os.environ.get("PRISANALYSE_BASE_URL", "https://prisanalyse.no").rstrip("/")


TYPE_TEKST = {
    "ny_posisjon": "Ny posisjon",
    "forste_kjop": "Kjøper igjen etter pause",
    "snur_kjop": "Snur: kjøper",
    "snur_salg": "Snur: selger",
    "forste_salg": "Begynner å selge",
    "ut": "Solgt seg helt ut",
}
_TYPE_PRIORITET = {"ut": 0, "snur_salg": 1, "forste_salg": 2, "ny_posisjon": 3, "snur_kjop": 4, "forste_kjop": 5}


def _env_float(navn: str, default: float) -> float:
    try:
        return float(os.environ.get(navn, "").strip() or default)
    except (TypeError, ValueError):
        return default


@dataclass
class Innstillinger:
    min_mnok: float = field(default_factory=lambda: _env_float("HANDLER_VARSEL_MIN_MNOK", 5.0))
    min_andel_pct: float = field(default_factory=lambda: _env_float("HANDLER_VARSEL_MIN_ANDEL", 10.0))
    pause_dager: int = field(default_factory=lambda: int(_env_float("HANDLER_VARSEL_PAUSE_DAGER", 90)))
    maks_opphold: int = field(default_factory=lambda: int(_env_float("HANDLER_VARSEL_MAKS_OPPHOLD", 7)))
    antall_flinke: int = field(default_factory=lambda: int(_env_float("HANDLER_VARSEL_ANTALL_FLINKE", 50)))
    lookback_dager: int = 365
    lister: list[str] = field(default_factory=lambda: [
        x.strip() for x in (os.environ.get("HANDLER_VARSEL_LISTER") or "Beste.csv,Viktige.csv").split(",")
        if x.strip()
    ])
    ekskluder: list[str] | None = field(default_factory=lambda: [
        x.strip().upper() for x in os.environ.get("HANDLER_VARSEL_EKSKLUDER", "").split(",") if x.strip()
    ] or None)  # None = felles liste i handler_data_beste.NOMINEE_MONSTRE


# =========================================================
# Data
# =========================================================
def hent_handler(conn, fra: dt.date, til: dt.date, investor_ids: list[str] | None = None,
                 par: list[tuple[str, str]] | None = None, kun_salg: bool = False) -> pd.DataFrame:
    """Handler (endring ≠ 0) i perioden, valgfritt begrenset til investorer eller (investor, isin)-par."""
    join = ""
    if investor_ids is not None:
        conn.execute("CREATE TEMP TABLE IF NOT EXISTS _hv_inv(investor_id TEXT PRIMARY KEY)")
        conn.execute("DELETE FROM _hv_inv")
        conn.executemany("INSERT OR IGNORE INTO _hv_inv VALUES (?)", [(i,) for i in investor_ids])
        join = "JOIN _hv_inv f ON f.investor_id = pc.investor_id"
    elif par is not None:
        conn.execute("CREATE TEMP TABLE IF NOT EXISTS _hv_par(investor_id TEXT, isin TEXT, PRIMARY KEY(investor_id, isin))")
        conn.execute("DELETE FROM _hv_par")
        conn.executemany("INSERT OR IGNORE INTO _hv_par VALUES (?,?)", par)
        join = "JOIN _hv_par f ON f.investor_id = pc.investor_id AND f.isin = pc.isin"
    salg = "AND pc.change_qty < 0" if kun_salg else ""
    sql = f"""
        SELECT pc.investor_id, pc.isin, substr(pc.date_today,1,10) AS dato,
               pc.change_qty AS antall,
               COALESCE(NULLIF(pc.price_yesterday,0), NULLIF(pc.price_today,0)) AS kurs,
               COALESCE(pc.holding_yesterday,0) AS beh_for,
               COALESCE(pc.holding_today,0) AS beh_etter
        FROM position_change pc {join}
        WHERE pc.date_today BETWEEN ? AND ? AND COALESCE(pc.change_qty,0) <> 0 {salg}
    """
    rows = conn.execute(sql, (fra.isoformat(), til.isoformat())).fetchall()
    cols = ["investor_id", "isin", "dato", "antall", "kurs", "beh_for", "beh_etter"]
    df = pd.DataFrame([tuple(r) for r in rows], columns=cols)
    if df.empty:
        return df
    df["investor_id"] = df["investor_id"].astype(str).str.strip()
    df["isin"] = df["isin"].astype(str).str.strip()
    for c in ("antall", "kurs", "beh_for", "beh_etter"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["kurs"] = df["kurs"].fillna(0.0)
    df["belop"] = df["antall"] * df["kurs"]
    return df


def klassifiser(tx: pd.DataFrame, fra: dt.date, til: dt.date,
                maks_opphold: int = 7, pause_dager: int = 90) -> pd.DataFrame:
    """Finn rekker som starter i vinduet [fra, til], og exits.

    tx må inneholde handler fra før vinduet (lookback) for at vi skal vite hva som
    er nytt. Returnerer én rad per signal med type, start, beløp og andel av beholdning.
    """
    cols = ["investor_id", "isin", "type", "start", "siste", "handelsdager", "antall", "belop_mnok",
            "beh_for", "beh_etter", "andel_pct", "pause_dager", "ut"]
    if tx is None or tx.empty:
        return pd.DataFrame(columns=cols)
    d = tx.copy()
    d["_t"] = pd.to_datetime(d["dato"])
    d = d.sort_values(["investor_id", "isin", "_t"])
    t_fra, t_til = pd.Timestamp(fra), pd.Timestamp(til)

    ut: list[dict] = []
    for (inv, isin), g in d.groupby(["investor_id", "isin"], sort=False):
        rader = g.to_dict("records")
        cur: dict | None = None
        for i, r in enumerate(rader):
            if r["_t"] < t_fra or r["_t"] > t_til:
                continue
            prev = rader[i - 1] if i > 0 else None
            kjop = r["antall"] > 0
            gap = (r["_t"] - prev["_t"]).days if prev is not None else None
            ny_rekke = prev is None or (prev["antall"] > 0) != kjop or gap > maks_opphold
            if ny_rekke:
                lang_pause = prev is None or gap >= pause_dager
                if kjop:
                    typ = ("ny_posisjon" if (r["beh_for"] or 0) <= 0 else "forste_kjop") if lang_pause \
                        else ("snur_kjop" if prev["antall"] < 0 else None)
                else:
                    typ = "forste_salg" if lang_pause else ("snur_salg" if prev["antall"] > 0 else None)
                cur = None
                if typ:
                    cur = {
                        "investor_id": inv, "isin": isin, "type": typ,
                        "start": r["dato"], "siste": r["dato"], "handelsdager": 1,
                        "antall": r["antall"], "belop": r["belop"],
                        "beh_for": r["beh_for"], "beh_etter": r["beh_etter"],
                        "pause_dager": gap if lang_pause else None, "ut": False,
                    }
                    ut.append(cur)
            elif cur is not None:
                cur["siste"] = r["dato"]
                cur["handelsdager"] += 1
                cur["antall"] += r["antall"]
                cur["belop"] += r["belop"]
                cur["beh_etter"] = r["beh_etter"]

            if r["antall"] < 0 and (r["beh_etter"] or 0) <= 0 < (r["beh_for"] or 0):
                if cur is not None and cur["isin"] == isin and cur["investor_id"] == inv and cur["antall"] < 0:
                    cur["ut"] = True
                else:
                    ut.append({
                        "investor_id": inv, "isin": isin, "type": "ut", "start": r["dato"], "siste": r["dato"],
                        "handelsdager": 1, "antall": r["antall"], "belop": r["belop"],
                        "beh_for": r["beh_for"], "beh_etter": r["beh_etter"], "pause_dager": None, "ut": True,
                    })

    if not ut:
        return pd.DataFrame(columns=cols)
    s = pd.DataFrame(ut)
    s["belop_mnok"] = s["belop"] / 1_000_000
    start_beh = s["beh_for"].where(s["beh_for"] > 0)
    s["andel_pct"] = (s["antall"].abs() / start_beh * 100).where(s["antall"] < 0)
    # For kjøp: økning i % av beholdningen før kjøpet (ny posisjon gir ingen andel)
    s.loc[s["antall"] > 0, "andel_pct"] = (s["antall"] / start_beh * 100)[s["antall"] > 0]
    return s[cols]


def _over_terskel(s: pd.DataFrame, cfg: Innstillinger) -> pd.Series:
    if s.empty:
        return pd.Series(dtype=bool)
    return (s["belop_mnok"].abs() >= cfg.min_mnok) | (s["andel_pct"].fillna(0) >= cfg.min_andel_pct) | s["ut"]


def _navn(conn, investor_ids: list[str]) -> dict[str, str]:
    if not investor_ids:
        return {}
    out: dict[str, str] = {}
    for i in range(0, len(investor_ids), 500):
        chunk = investor_ids[i:i + 500]
        ph = ",".join("?" * len(chunk))
        for r in conn.execute(
            f"SELECT investor_id, first_name, last_name FROM investor WHERE investor_id IN ({ph})", chunk
        ).fetchall():
            out[str(r[0]).strip()] = hd.clean_name(r[1] or "", r[2] or "", str(r[0]))
    return out


def _aksjer(conn, isins: list[str]) -> dict[str, tuple[str, str]]:
    if not isins:
        return {}
    ph = ",".join("?" * len(isins))
    return {
        str(r[0]).strip(): (r[1] or "", r[2] or "")
        for r in conn.execute(f"SELECT isin, ticker, isin_name FROM security WHERE isin IN ({ph})", isins).fetchall()
    }


def _rang_dagen_for(conn, isin: str, dato: str, investor_id: str) -> int | None:
    """Investorens plass blant eierne i aksjen målt på beholdning dagen før handelen."""
    rows = conn.execute(
        """SELECT investor_id FROM position_change
           WHERE isin=? AND substr(date_today,1,10)=? AND COALESCE(holding_yesterday,0) > 0
           ORDER BY holding_yesterday DESC LIMIT 20""",
        (isin, dato),
    ).fetchall()
    ids = [str(r[0]).strip() for r in rows]
    return ids.index(investor_id) + 1 if investor_id in ids else None


def _folgeliste_ids(conn, cfg: Innstillinger, brukt: list[str] | None = None) -> list[str]:
    tilgjengelige = set(hd.list_csv_files())
    ids: list[str] = []
    for liste in cfg.lister:
        if liste not in tilgjengelige:
            continue
        path = hd.resolve_list_csv_path(liste)
        if not path:
            continue
        df_list = hd.read_csv_guess(path)
        modus = "Beste" if Path(liste).stem.strip().lower() == "beste" else "Viktige"
        ids.extend(hd.resolve_investor_ids(conn, hd.extract_owner_patterns(modus, df_list)))
        if brukt is not None:
            brukt.append(liste)
    return sorted(set(ids))


def _flinke_ids(conn, til: dt.date, cfg: Innstillinger) -> list[str]:
    import handler_data_beste as hdb
    cols = hdb.detect_cols(conn)
    last_map, _ = hdb.build_last_price_cache(conn, cols)
    res = hdb.compute_best_investors(
        conn=conn, cols=cols, date_from=til - dt.timedelta(days=cfg.lookback_dager), date_to=til,
        investor_ids=None, invtype_choice="Alle", country_codes=None, isin_filter=None,
        min_trades=10, min_brutto_mnok=50, last_price_map=last_map, sort_by="gevinst_pct",
    )
    if res.empty:
        return []
    res = res[res["gevinst_mnok"] > 0]
    return res["investor_id"].astype(str).head(cfg.antall_flinke).tolist()


# =========================================================
# Signaler
# =========================================================
def finn_signaler(conn, fra: dt.date, til: dt.date, cfg: Innstillinger | None = None) -> dict:
    cfg = cfg or Innstillinger()
    lookback_fra = fra - dt.timedelta(days=cfg.lookback_dager)
    kl = dict(maks_opphold=cfg.maks_opphold, pause_dager=cfg.pause_dager)

    # 1) Følgeliste: alle handler
    brukte_lister: list[str] = []
    folg_ids = _folgeliste_ids(conn, cfg, brukte_lister)
    folg_tx = hent_handler(conn, lookback_fra, til, investor_ids=folg_ids) if folg_ids else pd.DataFrame()
    folg_sig = klassifiser(folg_tx, fra, til, **kl)
    folg_rader: list[dict] = []
    if not folg_tx.empty:
        vindu = folg_tx[(folg_tx["dato"] >= fra.isoformat()) & (folg_tx["dato"] <= til.isoformat())]
        for (inv, isin), g in vindu.groupby(["investor_id", "isin"]):
            g = g.sort_values("dato")
            sig = folg_sig[(folg_sig["investor_id"] == inv) & (folg_sig["isin"] == isin)]
            folg_rader.append({
                "investor_id": inv, "isin": isin,
                "start": g["dato"].iloc[0], "siste": g["dato"].iloc[-1], "handelsdager": len(g),
                "antall": g["antall"].sum(), "belop_mnok": g["belop"].sum() / 1_000_000,
                "beh_etter": g["beh_etter"].iloc[-1],
                "type": ", ".join(TYPE_TEKST[t] for t in sig["type"]) if not sig.empty else "",
                "ut": bool((g["beh_etter"] <= 0).iloc[-1] and g["antall"].sum() < 0),
            })
    folg_set = set(folg_ids)

    # 2) Flinke investorer som snur
    flinke = [i for i in _flinke_ids(conn, til, cfg) if i not in folg_set]
    fl_tx = hent_handler(conn, lookback_fra, til, investor_ids=flinke) if flinke else pd.DataFrame()
    fl_sig = klassifiser(fl_tx, fra, til, **kl)
    fl_sig = fl_sig[_over_terskel(fl_sig, cfg)] if not fl_sig.empty else fl_sig

    # 3) Topp 10-eier begynner å selge (alle investorer)
    salg_vindu = hent_handler(conn, fra, til, kun_salg=True)
    t10_sig = pd.DataFrame()
    if not salg_vindu.empty:
        par = sorted({(r.investor_id, r.isin) for r in salg_vindu.itertuples()
                      if r.investor_id not in folg_set and r.investor_id not in set(flinke)})
        if par:
            par_tx = hent_handler(conn, lookback_fra, til, par=par)
            s = klassifiser(par_tx, fra, til, **kl)
            s = s[s["type"].isin(["snur_salg", "forste_salg", "ut"])]
            s = s[_over_terskel(s, cfg)] if not s.empty else s
            if not s.empty:
                s = s.copy()
                s["rang"] = [_rang_dagen_for(conn, r.isin, r.start, r.investor_id) for r in s.itertuples()]
                t10_sig = s[s["rang"].notna() & (s["rang"] <= 10)]

    # Navn og filtrering av nominee-konti i del 2 og 3
    alle_inv = sorted({r["investor_id"] for r in folg_rader}
                      | set(fl_sig.get("investor_id", [])) | set(t10_sig.get("investor_id", [])))
    alle_isin = sorted({r["isin"] for r in folg_rader}
                       | set(fl_sig.get("isin", [])) | set(t10_sig.get("isin", [])))
    navn = _navn(conn, alle_inv)
    aksjer = _aksjer(conn, alle_isin)

    import handler_data_beste as hdb

    def ekskludert(inv: str) -> bool:
        return hdb.er_nominee(navn.get(inv, inv), cfg.ekskluder)

    def berik(rader: list[dict]) -> list[dict]:
        out = []
        for r in rader:
            r = dict(r)
            r["investor"] = navn.get(r["investor_id"], r["investor_id"])
            r["ticker"], r["aksje"] = aksjer.get(r["isin"], ("", ""))
            r["lenke"] = lenke(r["investor_id"], r["investor"], r["isin"], r["start"], til)
            out.append(r)
        return out

    def sorter(df: pd.DataFrame) -> list[dict]:
        if df.empty:
            return []
        df = df.copy()
        df["_p"] = df["type"].map(_TYPE_PRIORITET).fillna(9)
        df["_b"] = df["belop_mnok"].abs()
        df = df.sort_values(["_p", "_b"], ascending=[True, False])
        return [r for r in df.drop(columns=["_p", "_b"]).to_dict("records") if not ekskludert(r["investor_id"])]

    folg_rader.sort(key=lambda r: -abs(r["belop_mnok"]))
    return {
        "fra": fra.isoformat(), "til": til.isoformat(),
        "innstillinger": cfg,
        "folgeliste": berik(folg_rader),
        "flinke": berik(sorter(fl_sig)),
        "topp10": berik(sorter(t10_sig)),
        "antall_flinke_fulgt": len(flinke),
        "antall_folgeliste": len(folg_ids),
        "lister": brukte_lister or cfg.lister,
    }


def lenke(investor_id: str, label: str, isin: str, start: str, til: dt.date) -> str:
    fra = (dt.date.fromisoformat(start) - dt.timedelta(days=120)).isoformat()
    q = urllib.parse.urlencode({
        "investor_id": investor_id, "label": label, "isin": isin,
        "date_from": fra, "date_to": til.isoformat(),
    })
    return f"{BASE_URL}/handler-oslo-bors/per-eier?{q}"


def antall_signaler(sig: dict) -> int:
    return len(sig["folgeliste"]) + len(sig["flinke"]) + len(sig["topp10"])


# =========================================================
# E-post
# =========================================================
def _fmt(v: float, dec: int = 1) -> str:
    s = f"{v:,.{dec}f}".replace(",", " ").replace(".", ",")
    return s.replace("-", "−")


def _fmt_antall(q: float) -> str:
    a = abs(q)
    if a >= 1e6:
        s = _fmt(a / 1e6, 2).rstrip("0").rstrip(",") + " mill"
    elif a >= 1e3:
        s = _fmt(a / 1e3, 0) + "k"
    else:
        s = _fmt(a, 0)
    return ("+" if q > 0 else "−") + s


def _rad_html(r: dict, vis_type: bool = True, ekstra: str = "") -> str:
    farge = "#2563eb" if r["antall"] > 0 else "#dc2626"
    pil = "▲" if r["antall"] > 0 else "▼"
    typ = r.get("type") or ""
    typ_txt = TYPE_TEKST.get(typ, typ)
    if r.get("ut") and typ != "ut":
        typ_txt = (typ_txt + ", helt ute") if typ_txt else "Helt ute"
    detaljer = []
    if r.get("andel_pct") and r["andel_pct"] == r["andel_pct"]:
        detaljer.append(f"{_fmt(r['andel_pct'], 0)} % av beholdningen")
    if r.get("pause_dager"):
        detaljer.append(f"første handel på {int(r['pause_dager'])} dager")
    if r.get("handelsdager", 1) > 1:
        detaljer.append(f"{r['handelsdager']} handelsdager")
    if ekstra:
        detaljer.append(ekstra)
    return (
        "<tr>"
        f"<td style='padding:6px 8px;border-bottom:1px solid #e5e7eb;color:{farge};font-weight:700'>{pil}</td>"
        f"<td style='padding:6px 8px;border-bottom:1px solid #e5e7eb'>"
        f"<a href='{html.escape(r['lenke'])}' style='color:#111827;font-weight:600;text-decoration:none'>"
        f"{html.escape(r['investor'])}</a><br>"
        f"<span style='color:#6b7280'>{html.escape(r['ticker'] or r['isin'])} {html.escape(r['aksje'])}</span></td>"
        f"<td style='padding:6px 8px;border-bottom:1px solid #e5e7eb;white-space:nowrap'>"
        f"{_fmt_antall(r['antall'])}<br><span style='color:#6b7280'>{_fmt(r['belop_mnok'], 1)} MNOK</span></td>"
        f"<td style='padding:6px 8px;border-bottom:1px solid #e5e7eb'>"
        + (f"<strong>{html.escape(typ_txt)}</strong><br>" if vis_type and typ_txt else "")
        + f"<span style='color:#6b7280'>{html.escape(', '.join(detaljer))}</span></td>"
        "</tr>"
    )


def _seksjon(tittel: str, forklaring: str, rader: list[dict], vis_type: bool = True, ekstra=None) -> str:
    if not rader:
        return ""
    body = "".join(_rad_html(r, vis_type, ekstra(r) if ekstra else "") for r in rader[:40])
    mer = f"<p style='color:#6b7280'>… og {len(rader) - 40} til.</p>" if len(rader) > 40 else ""
    return (
        f"<h3 style='margin:22px 0 4px'>{html.escape(tittel)} ({len(rader)})</h3>"
        f"<p style='margin:0 0 8px;color:#6b7280;font-size:13px'>{html.escape(forklaring)}</p>"
        "<table style='border-collapse:collapse;width:100%;font-size:14px;table-layout:fixed'>"
        "<colgroup><col style='width:28px'><col style='width:42%'><col style='width:22%'><col></colgroup>"
        f"{body}</table>{mer}"
    )


def bygg_html(sig: dict) -> str:
    cfg: Innstillinger = sig["innstillinger"]
    periode = sig["fra"] if sig["fra"] == sig["til"] else f"{sig['fra']} til {sig['til']}"
    return (
        "<div style='font-family:-apple-system,Segoe UI,Arial,sans-serif;max-width:720px;color:#111827'>"
        f"<h2 style='margin:0 0 4px'>📈 Handler Oslo Børs, {html.escape(periode)}</h2>"
        "<p style='margin:0;color:#6b7280;font-size:13px'>Trykk på et navn for kursgraf og alle handler i aksjen.</p>"
        + _seksjon("Følgelisten", f"Alle handler fra investorene i {', '.join(sig.get('lister') or cfg.lister)}.",
                   sig["folgeliste"])
        + _seksjon("Flinke investorer som snur",
                   f"De {sig['antall_flinke_fulgt']} beste investorene siste år (gevinst %) som starter en ny "
                   f"retning. Minst {_fmt(cfg.min_mnok, 0)} MNOK eller {_fmt(cfg.min_andel_pct, 0)} % av beholdningen.",
                   sig["flinke"])
        + _seksjon("Topp 10-eier begynner å selge",
                   "Investor blant de 10 største eierne i aksjen dagen før salget startet.",
                   sig["topp10"], ekstra=lambda r: f"nr. {int(r['rang'])} blant eierne")
        + f"<p style='margin-top:24px'><a href='{BASE_URL}/handler-oslo-bors/'>Åpne Handler Oslo Børs</a></p>"
        "</div>"
    )


def emne(sig: dict) -> str:
    deler = []
    if sig["topp10"]:
        deler.append(f"{len(sig['topp10'])} topp 10-salg")
    if sig["flinke"]:
        deler.append(f"{len(sig['flinke'])} snur")
    if sig["folgeliste"]:
        deler.append(f"{len(sig['folgeliste'])} fra følgelisten")
    return f"📈 Handler Oslo Børs {sig['til']}: " + ", ".join(deler)


def _mottakere() -> list[str]:
    raw = os.environ.get("HANDLER_VARSEL_TIL") or os.environ.get("MEDIA_DIGEST_TO") or ""
    return [a.strip() for a in raw.replace(";", ",").split(",") if a.strip()]


def send_mail(subject: str, html_body: str) -> bool:
    smtp_host = os.environ.get("SMTP_HOST", "").strip()
    mottakere = _mottakere()
    if not smtp_host or not mottakere:
        _LOG.info("Handler-varsel: SMTP_HOST/HANDLER_VARSEL_TIL ikke satt, sender ikke.")
        return False
    smtp_user = os.environ.get("SMTP_USER", "")
    sender = os.environ.get("HANDLER_VARSEL_FRA") or os.environ.get("MEDIA_DIGEST_FROM") or smtp_user
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = ", ".join(mottakere)
    msg.attach(MIMEText(html_body, "html", "utf-8"))
    with smtplib.SMTP(smtp_host, int(os.environ.get("SMTP_PORT", "587")), timeout=30) as server:
        server.starttls()
        if smtp_user:
            server.login(smtp_user, os.environ.get("SMTP_PASSWORD", ""))
        server.sendmail(sender, mottakere, msg.as_string())
    _LOG.info("Handler-varsel sendt til %s", ", ".join(mottakere))
    return True


# =========================================================
# Kjøring
# =========================================================
_STATE_PATH = Path(hd.HANDLER_LIST_CACHE_DIR) / "handler_varsel_sendt.json"
_LOCK = threading.Lock()


def _allerede_sendt(fra: dt.date, til: dt.date) -> bool:
    try:
        sendt = json.loads(_STATE_PATH.read_text(encoding="utf-8")).get("sendt", [])
    except Exception:
        return False
    return f"{fra}:{til}" in sendt


def _merk_sendt(fra: dt.date, til: dt.date) -> None:
    try:
        data = json.loads(_STATE_PATH.read_text(encoding="utf-8")) if _STATE_PATH.exists() else {}
    except Exception:
        data = {}
    sendt = (data.get("sendt") or [])[-200:] + [f"{fra}:{til}"]
    try:
        _STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
        _STATE_PATH.write_text(json.dumps({"sendt": sendt}), encoding="utf-8")
    except Exception:
        _LOG.warning("Klarte ikke lagre handler-varsel-status", exc_info=True)


def kjor(fra: dt.date, til: dt.date, send: bool = True, db_path: str | None = None) -> dict:
    """Finn signaler for vinduet og send e-post hvis det er noe. Returnerer et sammendrag."""
    with _LOCK:
        conn = sqlite3.connect(db_path or hd.HANDLER_DB_PATH, timeout=60)
        conn.row_factory = sqlite3.Row
        try:
            sig = finn_signaler(conn, fra, til)
        finally:
            conn.close()
        n = antall_signaler(sig)
        resultat = {"fra": fra.isoformat(), "til": til.isoformat(), "signaler": n, "sendt": False,
                    "folgeliste": len(sig["folgeliste"]), "flinke": len(sig["flinke"]), "topp10": len(sig["topp10"])}
        if n and send and not _allerede_sendt(fra, til):
            if send_mail(emne(sig), bygg_html(sig)):
                _merk_sendt(fra, til)
                resultat["sendt"] = True
        return resultat


def kjor_etter_opplasting(datoer_for: dt.date | None, datoer_etter: dt.date | None) -> bool:
    """Start varsling i bakgrunnen når en opplasting har gitt nye datoer. Returnerer True hvis startet."""
    if os.environ.get("HANDLER_VARSEL_AV", "").strip() in {"1", "true", "ja"}:
        return False
    if datoer_etter is None or (datoer_for is not None and datoer_etter <= datoer_for):
        return False
    fra = (datoer_for + dt.timedelta(days=1)) if datoer_for else datoer_etter
    # Ved store backfill-opplastinger: varsle bare for siste 5 dager
    fra = max(fra, datoer_etter - dt.timedelta(days=5))

    def _jobb():
        try:
            res = kjor(fra, datoer_etter, send=True)
            _LOG.info("Handler-varsel ferdig: %s", res)
        except Exception:
            _LOG.exception("Handler-varsel feilet for %s til %s", fra, datoer_etter)

    threading.Thread(target=_jobb, name="handler-varsel", daemon=True).start()
    return True


def main(argv: list[str] | None = None) -> int:
    import argparse
    p = argparse.ArgumentParser(description="E-postvarsel for handler på Oslo Børs")
    p.add_argument("--fra", type=dt.date.fromisoformat)
    p.add_argument("--til", type=dt.date.fromisoformat)
    p.add_argument("--send", action="store_true", help="send e-post (ellers bare vis)")
    p.add_argument("--html", help="skriv e-posten til denne filen")
    p.add_argument("--test", action="store_true", help="send en test-e-post")
    a = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO)

    if a.test:
        ok = send_mail("📈 Handler Oslo Børs: testvarsel",
                       "<p>Oppsettet virker. Du får e-post når nye handelsdata gir signaler.</p>")
        print("Sendt" if ok else "Ikke sendt (sjekk SMTP_HOST og HANDLER_VARSEL_TIL)")
        return 0

    hd.ensure_local_db(hd.HANDLER_DB_PATH)
    conn = hd.db_connect()
    til = a.til or hd.get_max_date_today(conn)
    conn.close()
    fra = a.fra or til
    if a.send:
        print(kjor(fra, til, send=True))
        return 0
    conn = hd.db_connect()
    try:
        sig = finn_signaler(conn, fra, til)
    finally:
        conn.close()
    print(emne(sig) if antall_signaler(sig) else "Ingen signaler.")
    for k in ("folgeliste", "flinke", "topp10"):
        for r in sig[k]:
            print(f"  [{k}] {r['investor']} | {r['ticker']} | {_fmt_antall(r['antall'])} | "
                  f"{_fmt(r['belop_mnok'])} MNOK | {TYPE_TEKST.get(r.get('type'), r.get('type') or '')}")
    if a.html:
        Path(a.html).write_text(bygg_html(sig), encoding="utf-8")
        print("Skrev", a.html)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
