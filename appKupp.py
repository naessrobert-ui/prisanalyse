import streamlit as st
import pandas as pd
import numpy as np
import folium
from folium.plugins import MarkerCluster
from streamlit_folium import st_folium
from sklearn.neighbors import BallTree

# Henter siste boligfil fra S3 / prisanalyse-data
from bolig_data import load_latest_bolig_df

# --------------------------------------------------
# KONFIGURASJON
# --------------------------------------------------
st.set_page_config(
    page_title="Underprisradar – Boliger i Norge",
    page_icon="📉",
    layout="wide"
)

st.title("📉 Underprisradar: Boliger for salg i Norge")
st.markdown(
    "Finn boliger som ser **underprisede** ut sammenlignet med lignende boliger "
    "i samme område – både på fylkesnivå og relativt til **nabo-boliger**."
)

# --------------------------------------------------
# DATAINNLESING
# --------------------------------------------------
@st.cache_data(ttl=3600)
def load_data():
    """
    Laster siste bolig_X_*.csv fra S3 via bolig_data.load_latest_bolig_df().
    """
    df = load_latest_bolig_df()
    if df is None or df.empty:
        st.error("Fant ingen bolig_X_*.csv i S3 – kan ikke vise underprisradar.")
        st.stop()

    df.columns = df.columns.str.strip()
    return df


# --------------------------------------------------
# DATARENSING
# --------------------------------------------------
def clean_data(df_raw: pd.DataFrame) -> pd.DataFrame:
    df = df_raw.copy()

    # Normaliser kolonnenavn
    df.columns = [c.strip() for c in df.columns]

    # --- 1. M2-pris ---
    if "M2-pris" not in df.columns:
        candidate_cols = [c for c in df.columns if "m2" in c.lower() and "pris" in c.lower()]
        if candidate_cols:
            df.rename(columns={candidate_cols[0]: "M2-pris"}, inplace=True)
        else:
            st.error("Fant ingen kolonne for M2-pris i dataene.")
            st.stop()

    df["M2-pris"] = df["M2-pris"].astype(str)
    df["M2-pris"] = df["M2-pris"].str.replace("kr", "", regex=False, case=False)
    df["M2-pris"] = df["M2-pris"].str.replace(" ", "", regex=False)
    df["M2-pris"] = df["M2-pris"].str.replace(".", "", regex=False)   # fjern tusenskille-punktum
    df["M2-pris"] = df["M2-pris"].str.replace(",", ".", regex=False)  # evt. komma til desimal
    df["M2-pris"] = pd.to_numeric(df["M2-pris"], errors="coerce")

    # --- 2. Koordinater ---
    lat_col = None
    lon_col = None
    for c in df.columns:
        cl = c.lower()
        if "lat" in cl and lat_col is None:
            lat_col = c
        if ("lon" in cl or "lng" in cl or "long" in cl) and lon_col is None:
            lon_col = c

    if lat_col is None or lon_col is None:
        st.error("Fant ikke kolonner for latitude/longitude i dataene.")
        st.stop()

    for col in [lat_col, lon_col]:
        df[col] = df[col].astype(str).str.replace(",", ".", regex=False)
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df.rename(columns={lat_col: "latitude", lon_col: "longitude"}, inplace=True)

    # --- 3. Areal-kolonne (for størrelsesbånd & underpris i kroner) ---
    area_col = "size"  # antatt navn i rådata

    if area_col not in df.columns:
        st.error(f"Fant ikke arealkolonnen '{area_col}' i dataene.")
        st.stop()

    df.rename(columns={area_col: "areal_m2"}, inplace=True)

    df["areal_m2"] = df["areal_m2"].astype(str)
    df["areal_m2"] = df["areal_m2"].str.replace("m²", "", regex=False)
    df["areal_m2"] = df["areal_m2"].str.replace("m2", "", regex=False)
    df["areal_m2"] = df["areal_m2"].str.replace(",", ".", regex=False)
    df["areal_m2"] = df["areal_m2"].str.replace(" ", "", regex=False)
    df["areal_m2"] = pd.to_numeric(df["areal_m2"], errors="coerce")

    # --- 4. Fylke, boligtype, eierform ---
    for col in ["fylke", "boligtype", "eierform"]:
        if col in df.columns:
            df[col] = df[col].fillna("Ukjent").astype(str)
        else:
            df[col] = "Ukjent"

    # --- 5. Dager på markedet (valgfritt) ---
    days_col = None
    for cand in df.columns:
        cl = cand.lower()
        if "dager" in cl and ("marked" in cl or "til salgs" in cl):
            days_col = cand
            break
    if days_col is not None:
        df.rename(columns={days_col: "dager_på_markedet"}, inplace=True)

    # --- 6. Filtrer vekk åpenbart feil data ---
    df = df.dropna(subset=["latitude", "longitude", "M2-pris"])
    df = df[df["M2-pris"] > 5000]
    df = df[
        (df["latitude"] > 57)
        & (df["latitude"] < 72)
        & (df["longitude"] > 4)
        & (df["longitude"] < 32)
    ]

    return df


# --------------------------------------------------
# GLOBAL UNDERPRISING (FYLKE / SEGMENT)
# --------------------------------------------------
def add_underpricing_metrics(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    size_bins = [0, 40, 60, 80, 100, 150, 1000]
    size_labels = ["0-40", "40-60", "60-80", "80-100", "100-150", "150+"]

    df["størrelsesbånd"] = pd.cut(
        df["areal_m2"], bins=size_bins, labels=size_labels, right=False
    )

    group_cols = ["fylke", "boligtype", "eierform", "størrelsesbånd"]

    stats = (
        df.groupby(group_cols)["M2-pris"]
        .agg(referanse_M2="median", antall_i_segment="size")
        .reset_index()
    )

    df = df.merge(stats, on=group_cols, how="left")

    df["underpris_pct"] = (df["referanse_M2"] - df["M2-pris"]) / df["referanse_M2"]
    df["underpris_kr"] = (df["referanse_M2"] - df["M2-pris"]) * df["areal_m2"]

    return df


# --------------------------------------------------
# LOKAL UNDERPRISING (NABO-BOLIGER) – brukes på filtrert utvalg
# --------------------------------------------------
def add_local_price_metric(
    df: pd.DataFrame,
    n_neighbors: int = 50,
    max_distance_km: float = 3.0,
    min_neighbors: int = 10,
) -> pd.DataFrame:
    """
    Legger til lokal referansepris basert på m²-pris for nærmeste naboer
    innen gitt radius (max_distance_km) for *denne* DataFrame-en (filtrert).
    """
    df = df.copy()

    if len(df) < min_neighbors + 1:
        df["lokal_referanse_M2"] = np.nan
        df["lokal_antall_naboer"] = 0
        df["lokal_underpris_pct"] = np.nan
        df["lokal_underpris_kr"] = np.nan
        return df

    coords = np.radians(df[["latitude", "longitude"]].values)
    tree = BallTree(coords, metric="haversine")

    k = min(n_neighbors, len(df))
    dist_rad, ind = tree.query(coords, k=k)
    dist_km = dist_rad * 6371.0  # jordradius i km

    lokal_ref = []
    lokal_n = []

    m2_values = df["M2-pris"].values

    for i in range(len(df)):
        mask = dist_km[i] <= max_distance_km
        nabo_idx = ind[i][mask]
        nabo_idx = [j for j in nabo_idx if j != i]

        if len(nabo_idx) >= min_neighbors:
            priser = m2_values[nabo_idx]
            lokal_ref.append(np.median(priser))
            lokal_n.append(len(nabo_idx))
        else:
            lokal_ref.append(np.nan)
            lokal_n.append(len(nabo_idx))

    df["lokal_referanse_M2"] = lokal_ref
    df["lokal_antall_naboer"] = lokal_n

    df["lokal_underpris_pct"] = (
        df["lokal_referanse_M2"] - df["M2-pris"]
    ) / df["lokal_referanse_M2"]
    df["lokal_underpris_kr"] = (
        df["lokal_referanse_M2"] - df["M2-pris"]
    ) * df["areal_m2"]

    return df


# --------------------------------------------------
# LAST OG FORBEREDE DATA (kun global metrics -> raskere start)
# --------------------------------------------------
with st.spinner("Laster og analyserer boligdata (globalt nivå)..."):
    raw = load_data()
    df = clean_data(raw)
    df = add_underpricing_metrics(df)

if len(df) == 0:
    st.error("Ingen gyldige data etter rensing.")
    st.stop()

st.success(f"Data lastet: {len(df):,} boliger".replace(",", " "))

# --------------------------------------------------
# SIDEBAR – FILTRE
# --------------------------------------------------
st.sidebar.header("Filtre")

fylker = ["Alle"] + sorted(df["fylke"].dropna().unique().tolist())
valgt_fylke = st.sidebar.selectbox("Fylke", fylker, index=0)

boligtyper = ["Alle"] + sorted(df["boligtype"].dropna().unique().tolist())
valgt_boligtype = st.sidebar.selectbox("Boligtype", boligtyper, index=0)

eierformer = ["Alle"] + sorted(df["eierform"].dropna().unique().tolist())
valgt_eierform = st.sidebar.selectbox("Eierform", eierformer, index=0)

max_dager = None
if "dager_på_markedet" in df.columns:
    max_dager = st.sidebar.number_input(
        "Maks dager på markedet", min_value=1, max_value=365, value=90
    )

st.sidebar.markdown("---")
referansevalg = st.sidebar.radio(
    "Hva vil du rangere på?",
    ["Fylke/segment", "Lokal (nabo-boliger)"],
    help="Lokal referanse bruker m²-pris hos boliger i geografisk nærhet.",
)

min_segment_størrelse = st.sidebar.slider(
    "Min. antall boliger i segment (for fylke/segment-modus)",
    min_value=5,
    max_value=50,
    value=15,
)

min_lokal_naboer = st.sidebar.slider(
    "Min. antall lokale naboer (for lokal-modus)",
    min_value=5,
    max_value=80,
    value=20,
)

min_underpris_pct = st.sidebar.slider(
    "Minst underpris (%)",
    min_value=0,
    max_value=50,
    value=5,
    help="Filtrer bort de som bare er marginalt under prisnivået.",
)

# --- Lokale kupp i dyre områder ---
st.sidebar.markdown("### Lokale kupp i dyre områder")

kun_dyre_omraader = st.sidebar.checkbox(
    "Vis kun kupp i dyre områder",
    value=False,
    help="Krever at prisnivået i området er høyt, i tillegg til at boligen er underpriset.",
)

if referansevalg == "Fylke/segment":
    min_dyrt_nivå = st.sidebar.number_input(
        "Min. segmentnivå (kr/m²) for å regnes som dyrt område",
        min_value=20000,
        max_value=150000,
        value=60000,
        step=5000,
    )
else:
    min_dyrt_nivå = st.sidebar.number_input(
        "Min. lokalt prisnivå (kr/m²) for å regnes som dyrt område",
        min_value=20000,
        max_value=150000,
        value=60000,
        step=5000,
    )

top_n = st.sidebar.slider("Antall kandidater (topp underpris)", 10, 300, 50, step=10)

# --------------------------------------------------
# FILTRERING (FØR EVT. LOKAL NABO-ANALYSE)
# --------------------------------------------------
sub = df.copy()

if valgt_fylke != "Alle":
    sub = sub[sub["fylke"] == valgt_fylke]

if valgt_boligtype != "Alle":
    sub = sub[sub["boligtype"] == valgt_boligtype]

if valgt_eierform != "Alle":
    sub = sub[sub["eierform"] == valgt_eierform]

if max_dager is not None and "dager_på_markedet" in sub.columns:
    sub = sub[sub["dager_på_markedet"] <= max_dager]

# --------------------------------------------------
# LOKAL NABO-ANALYSE KUN HVIS VALGT
# --------------------------------------------------
if referansevalg == "Lokal (nabo-boliger)":
    with st.spinner(
        "Beregner lokal referansepris (nabo-boliger) for valgt område..."
    ):
        sub = add_local_price_metric(
            sub,
            n_neighbors=50,
            max_distance_km=3.0,
            min_neighbors=min_lokal_naboer,
        )

# --------------------------------------------------
# RANGERING + DYRT-OMRÅDE-FILTER
# --------------------------------------------------
if referansevalg == "Fylke/segment":
    sub = sub[
        (sub["antall_i_segment"] >= min_segment_størrelse)
        & (sub["underpris_pct"] > min_underpris_pct / 100)
    ]
    if kun_dyre_omraader:
        sub = sub[sub["referanse_M2"] >= min_dyrt_nivå]
    sort_col = "underpris_pct"
else:
    if "lokal_underpris_pct" not in sub.columns:
        st.warning("Ingen lokal data tilgjengelig for dette utvalget.")
        st.stop()

    sub = sub[
        (sub["lokal_antall_naboer"] >= min_lokal_naboer)
        & (sub["lokal_underpris_pct"] > min_underpris_pct / 100)
    ]
    if kun_dyre_omraader:
        sub = sub[sub["lokal_referanse_M2"] >= min_dyrt_nivå]
    sort_col = "lokal_underpris_pct"

if len(sub) == 0:
    st.warning(
        "Ingen boliger matcher filtrene og kravene til underpris/datagrunnlag."
    )
    st.stop()

sub = sub.sort_values(sort_col, ascending=False).head(top_n)

# --------------------------------------------------
# VIS KART
# --------------------------------------------------
st.subheader(f"Topp {len(sub)} mest underprisede boliger etter filtrering")

map_center_lat = sub["latitude"].mean()
map_center_lon = sub["longitude"].mean()

m = folium.Map(
    location=[map_center_lat, map_center_lon],
    zoom_start=5.5,
    tiles="cartodbpositron",
)

marker_cluster = MarkerCluster().add_to(m)

for _, row in sub.iterrows():
    adresse = row.get("adresse", "Ukjent adresse")
    postnr = row.get("postnummer", "")
    sted = f"{adresse}, {postnr}" if postnr not in (None, "", np.nan) else adresse

    m2 = row["M2-pris"]
    ref = row["referanse_M2"]
    up_pct = row["underpris_pct"] * 100
    up_kr = row["underpris_kr"]
    areal = row["areal_m2"]

    lok_ref = row.get("lokal_referanse_M2")
    lok_up_pct = row.get("lokal_underpris_pct")
    lok_up_kr = row.get("lokal_underpris_kr")

    # FINN-lenke fra finnkode
    finnkode = row.get("finnkode")
    finn_url = None
    finnline = ""
    if pd.notna(finnkode):
        finn_str = str(int(float(finnkode)))
        finn_url = (
            f"https://www.finn.no/realestate/homes/ad.html?finnkode={finn_str}"
        )
        finnline = (
            f"<a href='{finn_url}' target='_blank'>Åpne på FINN (kode {finn_str})</a>"
        )

    popup_lines = [
        f"<b>{sted}</b>",
        f"{row.get('boligtype', '')} – {row.get('eierform', '')}",
        f"Areal: {areal:.0f} m²",
        f"M²-pris: {m2:,.0f} kr/m²".replace(",", " "),
        f"Segment-ref (fylke m.m.): {ref:,.0f} kr/m²".replace(",", " "),
        f"Underpris vs segment: {up_pct:.1f} % (~{up_kr:,.0f} kr)".replace(",", " "),
    ]

    if pd.notna(lok_ref):
        popup_lines += [
            "<hr>",
            f"Lokal ref (naboer): {lok_ref:,.0f} kr/m²".replace(",", " "),
            f"Underpris lokalt: {lok_up_pct*100:.1f} % (~{lok_up_kr:,.0f} kr)".replace(
                ",", " "
            ),
        ]

    if finnline:
        popup_lines.append("<br>" + finnline)

    popup_html = "<br>".join(popup_lines)

    if referansevalg == "Fylke/segment":
        tooltip_ref = f"{up_pct:.1f}% under segment-ref."
    else:
        if pd.notna(lok_up_pct):
            tooltip_ref = f"{(lok_up_pct*100):.1f}% under lokal-ref."
        else:
            tooltip_ref = "Ingen lokal-ref (for få naboer)."

    tooltip_html = (
        f"<b>{sted}</b><br>"
        f"{row.get('boligtype','')} – {row.get('eierform','')}<br>"
        f"{areal:.0f} m², {m2:,.0f} kr/m²<br>"
        f"{tooltip_ref}"
    ).replace(",", " ")

    folium.CircleMarker(
        location=[row["latitude"], row["longitude"]],
        radius=10,
        color=None,
        fill=True,
        fill_opacity=0.85,
        popup=folium.Popup(popup_html, max_width=350),
        tooltip=folium.Tooltip(tooltip_html, sticky=True, direction="top"),
    ).add_to(marker_cluster)

st_folium(m, width=1100, height=600)

# --------------------------------------------------
# TABELLVISNING – med klikkbar FINN-lenke
# --------------------------------------------------
st.markdown("### Detaljert liste over kandidater")

vis_cols = [
    "adresse",
    "postnummer",
    "fylke",
    "boligtype",
    "eierform",
    "areal_m2",
    "M2-pris",
    "referanse_M2",
    "underpris_pct",
    "underpris_kr",
    "lokal_referanse_M2",
    "lokal_underpris_pct",
    "lokal_underpris_kr",
    "antall_i_segment",
    "lokal_antall_naboer",
    "finnkode",
]
vis_cols = [c for c in vis_cols if c in sub.columns]

vis = sub[vis_cols].copy()

# tall-formatering
for col in [
    "M2-pris",
    "referanse_M2",
    "underpris_kr",
    "lokal_referanse_M2",
    "lokal_underpris_kr",
]:
    if col in vis.columns:
        vis[col] = vis[col].round(0).astype("Int64")

if "underpris_pct" in vis.columns:
    vis["underpris_pct"] = (vis["underpris_pct"] * 100).round(1)
if "lokal_underpris_pct" in vis.columns:
    vis["lokal_underpris_pct"] = (vis["lokal_underpris_pct"] * 100).round(1)

# bygg full FINN-url i egen kolonne
if "finnkode" in vis.columns:

    def make_finn_url(x):
        if pd.isna(x):
            return ""
        try:
            fk_str = str(int(float(x)))
        except Exception:
            return ""
        return (
            f"https://www.finn.no/realestate/homes/ad.html?finnkode={fk_str}"
        )

    vis["FINN-lenke"] = vis["finnkode"].apply(make_finn_url)

    finn_link_col = st.column_config.LinkColumn(
        "FINN-lenke",
        help="Klikk for å åpne annonsen på FINN",
    )
    column_config = {"FINN-lenke": finn_link_col}
else:
    column_config = {}

st.data_editor(
    vis,
    hide_index=True,
    use_container_width=True,
    disabled=True,  # gjør tabellen read-only
    column_config=column_config,
)

# Eksport som CSV (samme data som i tabellen)
csv = vis.to_csv(index=False).encode("utf-8-sig")
st.download_button(
    label="📥 Last ned liste som CSV",
    data=csv,
    file_name="underpris_kandidater.csv",
    mime="text/csv",
)
