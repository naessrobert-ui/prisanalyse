# bolig_streamlit_app.py
# En-fil Streamlit-app: tabell + drilldown + kart + raskere daglig serie (numpy-masker)
#
# Kjør lokalt:
#   python -m streamlit run bolig_streamlit_app.py
#
# Avhengigheter (typisk):
#   pip install streamlit boto3 botocore pandas pyarrow numpy matplotlib folium streamlit-folium streamlit-aggrid

import io
import os
from dataclasses import dataclass
from typing import Literal, Optional

import numpy as np
import pandas as pd
import streamlit as st

import boto3
from botocore.config import Config

import matplotlib.pyplot as plt

import folium
from streamlit_folium import st_folium
from st_aggrid import AgGrid, DataReturnMode, GridOptionsBuilder, GridUpdateMode


# ----------------- Config -----------------
@dataclass(frozen=True)
class AppConfig:
    s3_bucket: str = os.environ.get("BOLIG_S3_BUCKET", "prisanalyse-data")
    master_key: str = os.environ.get(
        "BOLIG_MASTER_KEY", "calc/bolig/bolig_master/bolig_master.parquet"
    )
    # kart bounds (Norge-ish)
    min_lat: float = 57.0
    max_lat: float = 72.0
    min_lon: float = 4.0
    max_lon: float = 32.0


CFG = AppConfig()


# ----------------- Data loading -----------------
@st.cache_data(show_spinner=False)
def load_master_s3(bucket: str, key: str) -> pd.DataFrame:
    config = Config(
        retries={"max_attempts": 10, "mode": "standard"},
        connect_timeout=30,
        read_timeout=120,
        tcp_keepalive=True,
    )
    s3 = boto3.client("s3", config=config)
    obj = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_parquet(io.BytesIO(obj["Body"].read()))


@st.cache_data(show_spinner=False)
def load_master_local(path: str) -> pd.DataFrame:
    return pd.read_parquet(path)


def extract_poststed(address: pd.Series) -> pd.Series:
    """
    Poststed = teksten etter siste komma i address. Fjerner 4-sifret postnummer.
    """
    s = address.astype(str)
    s = s.str.split(",").str[-1]
    s = s.str.replace(r"\b\d{4}\b", "", regex=True)
    s = s.str.strip()
    s = s.replace({"": pd.NA, "None": pd.NA, "nan": pd.NA})
    return s


def _parse_number_series(series: pd.Series) -> pd.Series:
    s = series.astype(str)
    s = s.str.replace("\u00a0", "", regex=False)
    s = s.str.replace("kr", "", regex=False, case=False)
    s = s.str.replace(" ", "", regex=False)
    s = s.str.replace(r"[^0-9,.-]", "", regex=True)

    both_mask = s.str.contains(".", regex=False) & s.str.contains(",", regex=False)
    decimal_comma_mask = both_mask & (s.str.rfind(",") > s.str.rfind("."))
    decimal_dot_mask = both_mask & ~decimal_comma_mask

    s.loc[decimal_comma_mask] = s.loc[decimal_comma_mask].str.replace(".", "", regex=False)
    s.loc[decimal_comma_mask] = s.loc[decimal_comma_mask].str.replace(",", ".", regex=False)
    s.loc[decimal_dot_mask] = s.loc[decimal_dot_mask].str.replace(",", "", regex=False)

    comma_only_mask = (~both_mask) & s.str.contains(",", regex=False)
    s.loc[comma_only_mask] = s.loc[comma_only_mask].str.replace(",", ".", regex=False)

    thousands_dot_mask = s.str.match(r"^-?\d{1,3}(\.\d{3})+$", na=False)
    s.loc[thousands_dot_mask] = s.loc[thousands_dot_mask].str.replace(".", "", regex=False)

    return pd.to_numeric(s, errors="coerce")


def _parse_area_m2(df: pd.DataFrame) -> pd.Series:
    area_col = next((c for c in ["size", "areal", "areal_m2", "bruksareal", "kvm", "area"] if c in df.columns), None)
    if area_col is None:
        return pd.Series(np.nan, index=df.index, dtype=float)

    area = df[area_col].astype(str)
    area = area.str.replace("m²", "", regex=False)
    area = area.str.replace("m2", "", regex=False)
    area = area.str.replace(" ", "", regex=False)
    area = area.str.replace(",", ".", regex=False)
    return pd.to_numeric(area, errors="coerce")


def normalize_master(df: pd.DataFrame) -> pd.DataFrame:
    """
    Sikrer forventede kolonner, typer og derivert "sted".
    """
    d = df.copy()

    required = [
        "finnkode",
        "fylke",
        "kommune_nr",
        "kommune_navn",
        "address",
        "full_title",
        "totalpris",
        "m2_pris",
        "ny_brukt",
        "latitude",
        "longitude",
        "dato_første",
        "dato_siste",
        "pris_første",
        "pris_ny",
        "dato_prisendring",
    ]
    for c in required:
        if c not in d.columns:
            d[c] = pd.NA

    # Dates
    d["dato_første"] = pd.to_datetime(d["dato_første"], errors="coerce").dt.normalize()
    d["dato_siste"] = pd.to_datetime(d["dato_siste"], errors="coerce").dt.normalize()
    d["dato_prisendring"] = pd.to_datetime(
        d["dato_prisendring"], errors="coerce"
    ).dt.normalize()

    # Numerics
    for col in [
        "totalpris",
        "m2_pris",
        "latitude",
        "longitude",
        "pris_første",
        "pris_ny",
    ]:
        d[col] = _parse_number_series(d[col])

    area_m2 = _parse_area_m2(d)
    with np.errstate(divide="ignore", invalid="ignore"):
        calc_m2 = d["totalpris"] / area_m2
    plausible_area = (area_m2 >= 15) & (area_m2 <= 1000)
    plausible_calc = (calc_m2 >= 5_000) & (calc_m2 <= 300_000)
    suspect_m2 = d["m2_pris"].isna() | (d["m2_pris"] <= 1_000) | (d["m2_pris"] >= 300_000)
    d.loc[suspect_m2 & plausible_area & plausible_calc, "m2_pris"] = calc_m2[suspect_m2 & plausible_area & plausible_calc]

    d.loc[(d["m2_pris"] < 5_000) | (d["m2_pris"] > 300_000), "m2_pris"] = np.nan

    # Strings
    for c in ["fylke", "kommune_navn", "ny_brukt", "address", "full_title"]:
        d[c] = d[c].astype(str).replace({"None": "", "nan": ""})

    d["finnkode"] = d["finnkode"].astype(str)

    # Sted (poststed)
    d["sted"] = extract_poststed(d["address"])
    mask = d["sted"].isna() | (d["sted"].astype(str).str.strip() == "")
    d.loc[mask, "sted"] = d["kommune_navn"]

    return d


def apply_ny_brukt_filter(df: pd.DataFrame, choice: str) -> pd.DataFrame:
    if choice == "Begge":
        return df.copy()

    s = df["ny_brukt"].astype(str).str.strip().str.lower()
    if choice == "Brukt":
        return df[s == "brukt"].copy()
    # "Nybygg"
    return df[s.isin(["nybygg", "ny", "nytt"])].copy()


Level = Literal["Fylke", "Kommune", "Sted"]


def group_key(df: pd.DataFrame, level: Level) -> pd.Series:
    if level == "Fylke":
        return df["fylke"].fillna("").astype(str)
    if level == "Kommune":
        return df["kommune_navn"].fillna("").astype(str)
    return df["sted"].fillna("").astype(str)


def filter_by_level(df: pd.DataFrame, level: Level, value: str) -> pd.DataFrame:
    value = str(value)
    if level == "Fylke":
        return df[df["fylke"].fillna("").astype(str) == value].copy()
    if level == "Kommune":
        return df[df["kommune_navn"].fillna("").astype(str) == value].copy()
    return df[df["sted"].fillna("").astype(str) == value].copy()


def snapshot_metrics(df: pd.DataFrame, day: pd.Timestamp, level: Level) -> pd.DataFrame:
    """
    Snapshot på én dag: aktive annonser og noen aggregater per group.
    """
    day = pd.to_datetime(day).normalize()
    d = df.dropna(subset=["dato_første", "dato_siste"]).copy()

    active = d[(d["dato_første"] <= day) & (d["dato_siste"] >= day)].copy()
    if active.empty:
        return pd.DataFrame(
            columns=[
                "group",
                "active_count",
                "median_totalpris",
                "median_m2",
                "mean_days_on_market",
            ]
        )

    active["group"] = group_key(active, level)
    active["days_on_market_today"] = (day - active["dato_første"]).dt.days

    out = (
        active.groupby("group", dropna=False)
        .agg(
            active_count=("finnkode", "count"),
            median_totalpris=("totalpris", "median"),
            median_m2=("m2_pris", "median"),
            mean_days_on_market=("days_on_market_today", "mean"),
        )
        .reset_index()
    )

    return out


def pct_change(end: pd.Series, start: pd.Series) -> pd.Series:
    start = pd.to_numeric(start, errors="coerce")
    end = pd.to_numeric(end, errors="coerce")
    base = start.replace({0: np.nan})
    pct = (end - start) / base * 100.0
    return pct.round(0)


def build_table(
    df: pd.DataFrame, level: Level, start_day: pd.Timestamp, end_day: pd.Timestamp
) -> pd.DataFrame:
    start_day = pd.to_datetime(start_day).normalize()
    end_day = pd.to_datetime(end_day).normalize()

    m_start = snapshot_metrics(df, start_day, level).set_index("group")
    m_end = snapshot_metrics(df, end_day, level).set_index("group")

    groups = m_start.index.union(m_end.index)
    out = pd.DataFrame({"Sted": groups.astype(str)})

    def get(name: str, frame: pd.DataFrame, default=np.nan) -> pd.Series:
        if frame.empty:
            return pd.Series([default] * len(out))
        s = frame.reindex(groups)[name]
        return s.reset_index(drop=True)

    med_m2_end = get("median_m2", m_end)
    med_tp_end = get("median_totalpris", m_end)
    act_end = get("active_count", m_end)
    mean_dom_end = get("mean_days_on_market", m_end)

    med_m2_start = get("median_m2", m_start)
    act_start = get("active_count", m_start)
    mean_dom_start = get("mean_days_on_market", m_start)

    out["Med m² nå"] = med_m2_end
    out["Med pris nå"] = med_tp_end
    out["Aktive nå"] = act_end
    out["Snitt tid nå"] = pd.to_numeric(mean_dom_end, errors="coerce").round(0)

    out["Δm²"] = med_m2_end - med_m2_start
    out["Δm²%"] = pct_change(med_m2_end, med_m2_start)

    out["Δakt"] = act_end - act_start
    out["Δakt%"] = pct_change(act_end, act_start)

    out["Δtid"] = (
        pd.to_numeric(mean_dom_end, errors="coerce")
        - pd.to_numeric(mean_dom_start, errors="coerce")
    ).round(0)
    out["Δtid%"] = pct_change(mean_dom_end, mean_dom_start)

    out = out[out["Sted"].astype(str).str.strip() != ""].copy()
    out = out.sort_values("Aktive nå", ascending=False, na_position="last")
    return out


def format_table(table: pd.DataFrame) -> pd.DataFrame:
    show = table.copy()

    for col in ["Med m² nå", "Med pris nå", "Δm²"]:
        if col in show.columns:
            show[col] = pd.to_numeric(show[col], errors="coerce").round(0)

    for col in ["Δm²%", "Δakt%", "Δtid%"]:
        if col in show.columns:
            show[col] = pd.to_numeric(show[col], errors="coerce").astype("Int64")

    for col in ["Aktive nå", "Δakt"]:
        if col in show.columns:
            show[col] = pd.to_numeric(show[col], errors="coerce").astype("Int64")

    for col in ["Snitt tid nå", "Δtid"]:
        if col in show.columns:
            show[col] = pd.to_numeric(show[col], errors="coerce").astype("Int64")

    return show


# ----------------- Raskere daglig serie -----------------
@st.cache_data(show_spinner=False)
def daily_series_fast(
    df: pd.DataFrame, start_day: pd.Timestamp, end_day: pd.Timestamp
) -> pd.DataFrame:
    start_day = pd.to_datetime(start_day).normalize()
    end_day = pd.to_datetime(end_day).normalize()
    days = pd.date_range(start_day, end_day, freq="D")

    d = df.dropna(subset=["dato_første", "dato_siste"]).copy()
    if d.empty:
        return pd.DataFrame(
            columns=[
                "dato",
                "active_count",
                "median_m2",
                "mean_m2",
                "median_totalpris",
                "mean_totalpris",
            ]
        )

    d["dato_første"] = pd.to_datetime(d["dato_første"], errors="coerce").dt.normalize()
    d["dato_siste"] = pd.to_datetime(d["dato_siste"], errors="coerce").dt.normalize()

    start_i = d["dato_første"].values.astype("datetime64[D]").astype(np.int64)
    end_i = d["dato_siste"].values.astype("datetime64[D]").astype(np.int64)

    m2 = pd.to_numeric(d.get("m2_pris"), errors="coerce").to_numpy(dtype=float)
    tp = pd.to_numeric(d.get("totalpris"), errors="coerce").to_numpy(dtype=float)

    day_i = days.values.astype("datetime64[D]").astype(np.int64)

    rows = []
    for di, day in zip(day_i, days):
        mask = (start_i <= di) & (end_i >= di)
        if not np.any(mask):
            rows.append(
                {
                    "dato": day,
                    "active_count": 0,
                    "median_m2": np.nan,
                    "mean_m2": np.nan,
                    "median_totalpris": np.nan,
                    "mean_totalpris": np.nan,
                }
            )
            continue

        m2v = m2[mask]
        tpv = tp[mask]

        m2_median = float(np.nanmedian(m2v)) if np.isfinite(m2v).any() else np.nan
        m2_mean = float(np.nanmean(m2v)) if np.isfinite(m2v).any() else np.nan

        tp_median = float(np.nanmedian(tpv)) if np.isfinite(tpv).any() else np.nan
        tp_mean = float(np.nanmean(tpv)) if np.isfinite(tpv).any() else np.nan

        rows.append(
            {
                "dato": day,
                "active_count": int(mask.sum()),
                "median_m2": m2_median,
                "mean_m2": m2_mean,
                "median_totalpris": tp_median,
                "mean_totalpris": tp_mean,
            }
        )

    return pd.DataFrame(rows)


def active_on_day(df: pd.DataFrame, day: pd.Timestamp) -> pd.DataFrame:
    day = pd.to_datetime(day).normalize()
    d = df.dropna(subset=["dato_første", "dato_siste"]).copy()
    d["dato_første"] = pd.to_datetime(d["dato_første"], errors="coerce").dt.normalize()
    d["dato_siste"] = pd.to_datetime(d["dato_siste"], errors="coerce").dt.normalize()
    return d[(d["dato_første"] <= day) & (d["dato_siste"] >= day)].copy()


def filter_geo_bounds(d: pd.DataFrame) -> pd.DataFrame:
    ok = d["latitude"].between(CFG.min_lat, CFG.max_lat, inclusive="both") & d[
        "longitude"
    ].between(CFG.min_lon, CFG.max_lon, inclusive="both")
    return d[ok].dropna(subset=["latitude", "longitude"]).copy()


def make_map(
    points: pd.DataFrame, end_day: pd.Timestamp, tiles: str = "OpenStreetMap"
) -> folium.Map:
    end_day = pd.to_datetime(end_day).normalize()

    center_lat = float(points["latitude"].median()) if not points.empty else 64.0
    center_lon = float(points["longitude"].median()) if not points.empty else 11.0

    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=6,
        control_scale=True,
        tiles=tiles,
    )

    from folium.plugins import MarkerCluster

    cluster = MarkerCluster(name="Boliger").add_to(m)

    for _, r in points.iterrows():
        d_first = pd.to_datetime(r.get("dato_første"), errors="coerce")
        d_last = pd.to_datetime(r.get("dato_siste"), errors="coerce")

        d_first_str = d_first.date().isoformat() if pd.notna(d_first) else ""
        d_last_str = d_last.date().isoformat() if pd.notna(d_last) else ""

        sold = bool(pd.notna(d_last) and d_last.normalize() < end_day)

        days = ""
        if pd.notna(d_first):
            ref_end = d_last.normalize() if (sold and pd.notna(d_last)) else end_day
            days = str(int((ref_end - d_first.normalize()).days))

        pris_first = pd.to_numeric(r.get("pris_første"), errors="coerce")
        pris_new = pd.to_numeric(r.get("pris_ny"), errors="coerce")
        d_price = pd.to_datetime(r.get("dato_prisendring"), errors="coerce")

        changed = False
        if pd.notna(pris_first) and pd.notna(pris_new) and pris_new != pris_first:
            if (
                pd.notna(d_price)
                and pd.notna(d_first)
                and d_price.normalize() != d_first.normalize()
            ):
                changed = True

        price_change_line = ""
        if changed:
            price_change_line = (
                f"Prisendring: {pris_first:,.0f} → {pris_new:,.0f}<br>"
                f"Dato prisendring: {(d_price.date().isoformat() if pd.notna(d_price) else '')}<br>"
            )

        status_txt = "SOLGT" if sold else "AKTIV"

        tp = pd.to_numeric(r.get("totalpris"), errors="coerce")
        m2v = pd.to_numeric(r.get("m2_pris"), errors="coerce")

        lines = []
        title = str(r.get("full_title", "") or "")
        addr = str(r.get("address", "") or "")
        lines.append(f"<b>{title}</b><br>")
        lines.append(f"{addr}<br>")

        if pd.notna(tp):
            lines.append(f"Totalpris: {tp:,.0f}<br>")
        else:
            lines.append("Totalpris: <br>")

        if pd.notna(m2v):
            lines.append(f"m²-pris: {m2v:,.0f}<br>")
        else:
            lines.append("m²-pris: <br>")

        lines.append(f"Publisert (først sett): {d_first_str}<br>")
        lines.append(f"Sist sett: {d_last_str}<br>")
        lines.append(f"Status: <b>{status_txt}</b><br>")
        lines.append(f"Dager: {days}<br>")
        if price_change_line:
            lines.append(price_change_line)
        lines.append(f"Finnkode: {r.get('finnkode','')}")

        popup_html = "".join(lines)

        folium.CircleMarker(
            location=[float(r["latitude"]), float(r["longitude"])],
            radius=4,
            fill=True,
            fill_opacity=0.85,
            popup=folium.Popup(popup_html, max_width=420),
        ).add_to(cluster)

    folium.LayerControl().add_to(m)
    return m


def plot_daily(series: pd.DataFrame, title: str):
    if series.empty:
        st.info("Ingen tidsserie å vise.")
        return

    fig1 = plt.figure()
    plt.plot(series["dato"], series["median_m2"], label="Median m²-pris")
    plt.plot(series["dato"], series["mean_m2"], label="Snitt m²-pris")
    plt.legend()
    plt.title(f"{title} – m²-pris per dag")
    plt.xlabel("Dato")
    plt.ylabel("Kr per m²")
    plt.xticks(rotation=45)
    plt.tight_layout()
    st.pyplot(fig1)

    fig1b = plt.figure()
    plt.plot(series["dato"], series["median_totalpris"], label="Median totalpris")
    plt.plot(series["dato"], series["mean_totalpris"], label="Snitt totalpris")
    plt.legend()
    plt.title(f"{title} – totalpris per dag")
    plt.xlabel("Dato")
    plt.ylabel("Kr")
    plt.xticks(rotation=45)
    plt.tight_layout()
    st.pyplot(fig1b)

    fig2 = plt.figure()
    plt.plot(series["dato"], series["active_count"], label="Aktive annonser")
    plt.legend()
    plt.title(f"{title} – aktive annonser per dag")
    plt.xlabel("Dato")
    plt.ylabel("Antall")
    plt.xticks(rotation=45)
    plt.tight_layout()
    st.pyplot(fig2)


def get_selected_place(grid_response: dict) -> Optional[str]:
    selected_rows = grid_response.get("selected_rows", None)
    if selected_rows is None:
        return None

    if isinstance(selected_rows, pd.DataFrame):
        if selected_rows.empty:
            return None
        return selected_rows.iloc[0].get("Sted", None)

    if isinstance(selected_rows, list):
        if len(selected_rows) == 0:
            return None
        row0 = selected_rows[0] or {}
        return row0.get("Sted", None)

    if isinstance(selected_rows, dict):
        return selected_rows.get("Sted", None)

    return None


# ----------------- Streamlit entrypoint -----------------
def run_app():
    st.set_page_config(page_title="Bolig – tabell + kart + drilldown", layout="wide")

    st.markdown(
        """
        <style>
          html, body, [class*="css"]  { font-size: 18px !important; }
          .block-container { padding-top: 1.2rem; padding-bottom: 1.2rem; }
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.title("Boligpriser – tabell + kart (klikk på rad for detaljer)")

    with st.sidebar:
        st.header("Datakilde")
        source = st.radio("Velg kilde", ["S3 master", "Lokal fil"], index=0)

        if source == "Lokal fil":
            local_path = st.text_input(
                "Sti til bolig_master.parquet", value="bolig_master.parquet"
            )
            load_btn = st.button("Last data", type="primary")
        else:
            st.caption(f"S3: s3://{CFG.s3_bucket}/{CFG.master_key}")
            load_btn = st.button("Last data", type="primary")

        st.divider()
        st.header("Kart")
        tiles = st.selectbox(
            "Kartstil", ["OpenStreetMap", "CartoDB Voyager", "CartoDB Positron"], index=0
        )

        st.divider()
        st.header("Ytelse")
        st.caption("Tips: stort date-intervall + stort sted kan bli tungt.")
        max_days_hint = st.checkbox("Varsle ved > 365 dager", value=True)

    if load_btn:
        with st.spinner("Laster data..."):
            try:
                raw = (
                    load_master_local(local_path)
                    if source == "Lokal fil"
                    else load_master_s3(CFG.s3_bucket, CFG.master_key)
                )
                df0 = normalize_master(raw)
                st.session_state["df"] = df0
                st.success(f"Lastet {len(df0):,} rader")
            except Exception as e:
                st.error(f"Klarte ikke å laste/lese master: {e}")

    df = st.session_state.get("df")
    if df is None:
        st.info("Trykk **Last data** i venstremenyen for å starte.")
        st.stop()

    min_date = df["dato_første"].min()
    max_date = df["dato_siste"].max()
    if pd.isna(min_date) or pd.isna(max_date):
        st.error("Mangler dato_første/dato_siste i master.")
        st.stop()

    st.subheader("Valg")
    c1, c2, c3, c4, c5 = st.columns([1.1, 1.2, 1.2, 1.2, 1.4])
    with c1:
        level: Level = st.selectbox("Detaljnivå", ["Fylke", "Kommune", "Sted"], index=0)
    with c2:
        start_day = st.date_input(
            "Start",
            value=min_date.date(),
            min_value=min_date.date(),
            max_value=max_date.date(),
        )
    with c3:
        end_day = st.date_input(
            "Slutt",
            value=max_date.date(),
            min_value=min_date.date(),
            max_value=max_date.date(),
        )
    with c4:
        nybrukt_choice = st.selectbox("Boligtype", ["Brukt", "Nybygg", "Begge"], index=0)
    with c5:
        search = st.text_input("Søk i sted", value="")

    start_ts = pd.to_datetime(start_day).normalize()
    end_ts = pd.to_datetime(end_day).normalize()

    if end_ts < start_ts:
        st.error("Slutt-dato må være >= start-dato.")
        st.stop()

    if max_days_hint and (end_ts - start_ts).days > 365:
        st.warning("Du har valgt et intervall på over 365 dager. Tidsserie kan bli treg.")

    fdf = apply_ny_brukt_filter(df, nybrukt_choice)

    st.caption("Klikk på en rad i tabellen for å få graf + kart for valgt sted.")

    tab1, tab2 = st.tabs(["📋 Tabell", "🗺️ Kart (alle aktive slutt-dato)"])

    with tab1:
        with st.spinner("Beregner tabell..."):
            table = build_table(fdf, level, start_ts, end_ts)

        if search.strip():
            q = search.strip().lower()
            table = table[
                table["Sted"].astype(str).str.lower().str.contains(q, na=False)
            ].copy()

        show = format_table(table)

        gb = GridOptionsBuilder.from_dataframe(show)
        gb.configure_default_column(sortable=True, filter=True, resizable=True)
        gb.configure_selection(selection_mode="single", use_checkbox=False)
        gb.configure_grid_options(domLayout="normal")
        gb.configure_pagination(enabled=False)
        grid_options = gb.build()

        grid_response = AgGrid(
            show,
            gridOptions=grid_options,
            data_return_mode=DataReturnMode.FILTERED_AND_SORTED,
            update_mode=GridUpdateMode.SELECTION_CHANGED,
            height=520,
            fit_columns_on_grid_load=True,
            allow_unsafe_jscode=False,
            try_to_convert_back_to_original_types=False,
        )

        selected_place = get_selected_place(grid_response)

        st.download_button(
            "Last ned som CSV",
            data=show.to_csv(index=False).encode("utf-8"),
            file_name=(
                f"bolig_tabell_{level.lower()}_{nybrukt_choice.lower()}_"
                f"{start_day}_{end_day}.csv"
            ),
            mime="text/csv",
        )

        st.divider()
        st.subheader("Detaljer (drilldown)")

        if not selected_place:
            st.info("Klikk på en rad i tabellen over for å se graf + kart.")
        else:
            sub = filter_by_level(fdf, level, str(selected_place))
            if sub.empty:
                st.warning("Fant ingen rader for valgt sted.")
            else:
                title = f"{level}: {selected_place} ({nybrukt_choice})"
                st.write(f"Valgt: **{title}**")

                with st.spinner("Beregner tidsserie..."):
                    ser = daily_series_fast(sub, start_ts, end_ts)
                plot_daily(ser, title)

                st.subheader("Kart – aktive boliger på slutt-dato")
                day_active = active_on_day(sub, end_ts)
                day_active = filter_geo_bounds(day_active)

                st.write(f"Aktive med gyldige koordinater: **{len(day_active):,}**")

                max_points = st.slider(
                    "Maks punkter (kart)",
                    500,
                    20000,
                    5000,
                    500,
                    key="max_points_detail",
                )
                if len(day_active) > max_points:
                    day_active = day_active.sample(max_points, random_state=42)

                m = make_map(day_active, end_day=end_ts, tiles=tiles)
                st_folium(m, width=None, height=650)

    with tab2:
        st.caption(
            "Kartet viser aktive boliger på valgt **Slutt-dato** for hele datasettet "
            "(etter Brukt/Nybygg-filter)."
        )
        active_all = active_on_day(fdf, end_ts)
        active_all = filter_geo_bounds(active_all)

        st.write(f"Aktive med gyldige koordinater: **{len(active_all):,}**")

        max_points_all = st.slider(
            "Maks punkter å tegne",
            min_value=500,
            max_value=20000,
            value=5000,
            step=500,
            key="max_points_all",
        )
        if len(active_all) > max_points_all:
            active_all = active_all.sample(max_points_all, random_state=42)

        m_all = make_map(active_all, end_day=end_ts, tiles=tiles)
        st_folium(m_all, width=None, height=700)


# Viktig: ingen Streamlit-kjøring ved import (gunicorn/import skal ikke trigge appen)
if __name__ == "__main__":
    run_app()
