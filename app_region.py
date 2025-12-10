import streamlit as st
import pandas as pd
import numpy as np
import folium
from streamlit_folium import st_folium
from bolig_data import load_latest_bolig_df   # behold denne

# --------------------------------------------------
# KONFIG
# --------------------------------------------------
st.set_page_config(
    page_title="Boligstatistikk per fylke, sted og gate",
    page_icon="📊",
    layout="wide"
)

st.title("📊 Boligstatistikk per fylke, sted og gate")
st.markdown(
    "Utforsk **kvadratmeterpris**, **totalpris** og **dager på markedet** "
    "for boliger i Norge – per fylke, per sted (fra adresse) og per gate+sted. "
    "Klikk deg videre til kart for å se enkeltboliger."
)

# --------------------------------------------------
# DATAINNLESING & RENSING
# --------------------------------------------------

@st.cache_data(ttl=3600)
def load_data():
    """Laster siste boligfil fra S3 via bolig_data.load_latest_bolig_df()."""
    df = load_latest_bolig_df()

    if df is None or df.empty:
        st.error("Fant ingen bolig_X_*.csv i S3 – kan ikke vise priser per sted.")
        st.stop()

    df.columns = df.columns.str.strip()
    return df


def clean_and_enrich(df_raw: pd.DataFrame) -> pd.DataFrame:
    df = df_raw.copy()

    # Normaliser kolonnenavn
    df.columns = [c.strip() for c in df.columns]

    # --- M2-pris ---
    if "M2-pris" not in df.columns:
        st.error("Fant ikke kolonnen 'M2-pris' i bolig.csv.")
        st.stop()

    df["M2-pris"] = (
        df["M2-pris"]
        .astype(str)
        .str.replace("kr", "", regex=False, case=False)
        .str.replace(" ", "", regex=False)
        .str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False)
    )
    df["M2-pris"] = pd.to_numeric(df["M2-pris"], errors="coerce")

    # --- Totalpris ---
    if "totalpris" in df.columns:
        df["totalpris"] = (
            df["totalpris"]
            .astype(str)
            .str.replace("kr", "", regex=False, case=False)
            .str.replace(" ", "", regex=False)
            .str.replace(".", "", regex=False)
            .str.replace(",", ".", regex=False)
        )
        df["totalpris"] = pd.to_numeric(df["totalpris"], errors="coerce")
    else:
        df["totalpris"] = np.nan

    # --- Fylke ---
    if "fylke" not in df.columns:
        df["fylke"] = "Ukjent"
    df["fylke"] = df["fylke"].fillna("Ukjent").astype(str)

    # --- Boligtype ---
    if "boligtype" not in df.columns:
        df["boligtype"] = "Ukjent"
    df["boligtype"] = df["boligtype"].fillna("Ukjent").astype(str)

    # Fjern boligtyper vi ikke vil ha med i statistikken
    df = df[~df["boligtype"].isin(["Andre", "Ukjent"])]

    # --- NY/Brukt ---
    if "NY/Brukt" not in df.columns:
        df["NY/Brukt"] = "Ukjent"
    df["NY/Brukt"] = df["NY/Brukt"].fillna("Ukjent").astype(str).str.strip()

    def normalize_nybrukt(x: str) -> str:
        xl = x.lower()
        if "ny" in xl:
            return "Nybygg"
        if "brukt" in xl:
            return "Brukt"
        return "Ukjent"

    df["NY/Brukt"] = df["NY/Brukt"].apply(normalize_nybrukt)

    # --- Adresse (for sted og gate) ---
    if "address" not in df.columns:
        df["address"] = ""
    df["address"] = df["address"].fillna("").astype(str)

    # Sted = det som står etter siste komma (f.eks. 'Oslo' i 'Ullevålsveien 114, Oslo')
    def extract_sted(addr: str) -> str:
        if not addr:
            return ""
        parts = addr.split(",")
        if len(parts) < 2:
            return ""
        return parts[-1].strip()

    df["sted"] = df["address"].apply(extract_sted)

    # Gate = det som står før første komma, men uten husnummer
    # 'Ullevålsveien 114A, Oslo' -> 'Ullevålsveien'
    def extract_gate(addr: str) -> str:
        if not addr:
            return ""
        gate_raw = addr.split(",")[0].strip()
        tokens = gate_raw.split()
        tokens_uten_nr = [t for t in tokens if not any(ch.isdigit() for ch in t)]
        if not tokens_uten_nr:
            return gate_raw
        return " ".join(tokens_uten_nr)

    df["gate"] = df["address"].apply(extract_gate)

    # Kombinert gate + sted, slik at f.eks. "Kirkeveien" skilles per sted
    def make_gate_sted(row):
        gate = row.get("gate", "").strip()
        sted = row.get("sted", "").strip()
        if gate and sted:
            return f"{gate}, {sted}"
        return gate or sted

    df["gate_sted"] = df.apply(make_gate_sted, axis=1)

    # --- Dager på markedet ---
    if "publisert_dato" in df.columns:
        publisert = pd.to_datetime(
            df["publisert_dato"],
            errors="coerce",
            dayfirst=True,
            utc=True,
        )
        today = pd.Timestamp.now(tz="UTC").normalize()
        df["dager_på_markedet"] = (today - publisert).dt.days
    else:
        df["dager_på_markedet"] = np.nan

    # --- Koordinater for kart ---
    for col in ["latitude", "longitude"]:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.replace(",", ".", regex=False)
                .str.replace(" ", "", regex=False)
            )
            df[col] = pd.to_numeric(df[col], errors="coerce")
        else:
            df[col] = np.nan

    # Filtrer vekk åpenbart meningsløse m²-priser
    df = df[df["M2-pris"] > 5000]

    return df


with st.spinner("Laster og bearbeider data..."):
    raw = load_data()
    df = clean_and_enrich(raw)

st.success(f"Data lastet: {len(df):,} boliger".replace(",", " "))

# --------------------------------------------------
# HJELPEFUNKSJON: KARTVISNING
# --------------------------------------------------
def vis_kart_for_boliger(df_props: pd.DataFrame, tittel: str):
    df_map = df_props.dropna(subset=["latitude", "longitude"])
    if df_map.empty:
        st.info("Ingen koordinater tilgjengelig for disse boligene.")
        return

    center_lat = df_map["latitude"].mean()
    center_lon = df_map["longitude"].mean()

    m = folium.Map(location=[center_lat, center_lon], zoom_start=13, tiles="cartodbpositron")

    for _, r in df_map.iterrows():
        adresse = r.get("address", "")
        m2 = r.get("M2-pris", np.nan)
        tot = r.get("totalpris", np.nan)
        dager = r.get("dager_på_markedet", np.nan)
        finnkode = r.get("finnkode", None)

        m2_txt = f"{m2:,.0f} kr/m²".replace(",", " ") if pd.notna(m2) else "ukjent"
        tot_txt = f"{tot:,.0f} kr".replace(",", " ") if pd.notna(tot) else "ukjent"
        dager_txt = f"{int(dager)} dager" if pd.notna(dager) else "ukjent"

        finn_html = ""
        if pd.notna(finnkode):
            try:
                fk_str = str(int(float(finnkode)))
                url = f"https://www.finn.no/realestate/homes/ad.html?finnkode={fk_str}"
                finn_html = f'<br><a href="{url}" target="_blank">Åpne på FINN (kode {fk_str})</a>'
            except Exception:
                pass

        popup_html = (
            f"<b>{adresse}</b><br>"
            f"M²-pris: {m2_txt}<br>"
            f"Totalpris: {tot_txt}<br>"
            f"Dager på markedet: {dager_txt}"
            f"{finn_html}"
        )

        folium.CircleMarker(
            location=[r["latitude"], r["longitude"]],
            radius=10,
            color="blue",
            weight=1,
            fill=True,
            fill_opacity=0.85,
            popup=folium.Popup(popup_html, max_width=300),
            tooltip=f"{adresse} – {m2_txt}",
        ).add_to(m)

    st.markdown(f"#### Kart – {tittel}")
    st_folium(m, width=900, height=600)

# --------------------------------------------------
# SIDEBAR-FILTRE
# --------------------------------------------------
# Nybygg / brukt — nå gjelder filteret HELE datasettet videre
st.sidebar.subheader("Nye/brukt filter")
ny_filter = st.sidebar.radio(
    "Velg boligtyper som skal være med",
    ["Alle boliger", "Kun nybygg", "Kun brukt"],
    index=0
)

# Filter brukes på en separat scope-dataframe
df_scope = df.copy()
if ny_filter == "Kun nybygg":
    df_scope = df_scope[df_scope["NY/Brukt"] == "Nybygg"]
elif ny_filter == "Kun brukt":
    df_scope = df_scope[df_scope["NY/Brukt"] == "Brukt"]


# Fylke-filter
fylker = ["Alle fylker"] + sorted(df["fylke"].unique().tolist())
valgt_fylke = st.sidebar.selectbox("Fylke (for detaljer)", fylker, index=0)

if valgt_fylke != "Alle fylker":
    df_scope = df[df["fylke"] == valgt_fylke].copy()
else:
    df_scope = df.copy()

# Boligtype-filter
boligtyper_unike = sorted(df_scope["boligtype"].unique().tolist())
boligvalg = st.sidebar.multiselect(
    "Boligtyper som skal være med",
    options=boligtyper_unike,
    default=boligtyper_unike
)
df_scope = df_scope[df_scope["boligtype"].isin(boligvalg)]

# Minimum antall gyldige observasjoner per sted/gate
min_antall = st.sidebar.number_input(
    "Minimum antall gyldige observasjoner per sted/gate",
    min_value=3, max_value=200, value=10, step=1
)

# Antall rader i topp/bunn-lister
top_n = st.sidebar.slider("Antall topp/bunn steder/gater", 5, 50, 10, step=1)

st.sidebar.markdown(f"**Antall boliger etter filter:** {len(df_scope)}")

if len(df_scope) == 0:
    st.warning("Ingen boliger matcher filtrene.")
    st.stop()

# --------------------------------------------------
# AGGREGASJONSFUNKSJON
# --------------------------------------------------
# --------------------------------------------------
# AGGREGASJONSFUNKSJON
# --------------------------------------------------
def aggregate_group(df_in: pd.DataFrame, group_cols):
    agg = (
        df_in
        .groupby(group_cols)
        .agg(
            antall=("M2-pris", "count"),
            antall_m2=("M2-pris", "count"),
            antall_totalpris=("totalpris", "count"),
            antall_dager=("dager_på_markedet", "count"),
            median_m2=("M2-pris", "median"),
            gj_snitt_m2=("M2-pris", "mean"),
            median_totalpris=("totalpris", "median"),
            median_dager=("dager_på_markedet", "median"),
        )
        .reset_index()
    )

    # Rounding
    for col in ["median_m2", "gj_snitt_m2", "median_totalpris", "median_dager"]:
        if col in agg.columns:
            agg[col] = agg[col].round(0)

    # Tusenskilleformat for bedre lesbarhet
    for display_col in ["median_m2", "gj_snitt_m2", "median_totalpris"]:
        if display_col in agg.columns:
            agg[display_col] = agg[display_col].apply(
                lambda x: f"{int(x):,}".replace(",", " ") if pd.notna(x) else x
            )

    return agg




METRIC_TO_COUNT_COL = {
    "median_m2": "antall_m2",
    "median_totalpris": "antall_totalpris",
    "median_dager": "antall_dager",
}

# --------------------------------------------------
# TABS: Fylke / Sted / Gate
# --------------------------------------------------
tab_fylke, tab_sted, tab_gate = st.tabs(["Fylke", "Sted (fra adresse)", "Gate+sted"])

# --------------------------------------------------
# TAB 1: FYLKE
# --------------------------------------------------
with tab_fylke:
    st.subheader("Statistikk per fylke og boligtype")

    df_fylke_base = df_scope.copy()
    if boligvalg:
        df_fylke_base = df_fylke_base[df_fylke_base["boligtype"].isin(boligvalg)]

    agg_fylke = aggregate_group(df_fylke_base, ["fylke", "boligtype"])
    agg_fylke = agg_fylke.sort_values(["fylke", "boligtype"])

    st.dataframe(
        agg_fylke[["fylke", "boligtype", "antall", "median_m2", "gj_snitt_m2",
                   "median_totalpris", "median_dager"]],
        use_container_width=True
    )

    st.markdown(
        """
        **Forklaring:**

        - *antall*: antall boliger med gyldig m²-pris i datasettet for fylke + boligtype  
        - *median_m2*: median kvadratmeterpris  
        - *gj_snitt_m2*: gjennomsnittlig kvadratmeterpris  
        - *median_totalpris*: median totalpris  
        - *median_dager*: median antall dager annonsen har ligget ute
        """
    )

# --------------------------------------------------
# TAB 2: STED
# --------------------------------------------------
with tab_sted:
    st.subheader("Statistikk per sted (basert på adressens sted-del)")

    df_sted = df_scope[df_scope["sted"] != ""].copy()

    if len(df_sted) == 0:
        st.info("Ingen registrerte sted-navn i adressene for dette utvalget.")
    else:
        agg_sted = aggregate_group(df_sted, ["sted"])

        if len(agg_sted) == 0:
            st.info("Ingen steder med nok data til å beregne statistikk.")
        else:
            metrikk_valg = st.selectbox(
                "Velg metrikk for topp/bunn-lister (sted)",
                ["median_m2", "median_totalpris", "median_dager"],
                format_func=lambda x: {
                    "median_m2": "Median m²-pris",
                    "median_totalpris": "Median totalpris",
                    "median_dager": "Median dager på markedet",
                }[x]
            )

            count_col = METRIC_TO_COUNT_COL[metrikk_valg]
            agg_sted_f = agg_sted[agg_sted[count_col] >= min_antall]
            agg_sted_f = agg_sted_f.dropna(subset=[metrikk_valg])

            if len(agg_sted_f) == 0:
                st.info(
                    f"Ingen steder har minst {min_antall} gyldige observasjoner for valgt metrikk."
                )
            else:
                col1, col2 = st.columns(2)

                with col1:
                    st.markdown("**Topp steder** (høye verdier)")
                    topp = agg_sted_f.sort_values(metrikk_valg, ascending=False).head(top_n)
                    st.dataframe(
                        topp[["sted", "antall", "median_m2", "gj_snitt_m2",
                              "median_totalpris", "median_dager"]],
                        use_container_width=True
                    )

                with col2:
                    st.markdown("**Bunn steder** (lave verdier)")
                    bunn = agg_sted_f.sort_values(metrikk_valg, ascending=True).head(top_n)
                    st.dataframe(
                        bunn[["sted", "antall", "median_m2", "gj_snitt_m2",
                              "median_totalpris", "median_dager"]],
                        use_container_width=True
                    )

                st.markdown(
                    f"Viser topp/bunn steder med minst **{min_antall}** gyldige "
                    f"observasjoner for valgt metrikk."
                )

                # --- Kart for valgt sted ---
                st.markdown("---")
                st.markdown("#### Kart for valgt sted")

                sted_options = (
                    pd.concat([topp["sted"], bunn["sted"]])
                    .drop_duplicates()
                    .tolist()
                )

                if sted_options:
                    valgt_sted_kart = st.selectbox(
                        "Velg sted for kartvisning",
                        sted_options,
                        index=0,
                    )

                    df_sted_props = df_sted[df_sted["sted"] == valgt_sted_kart].copy()
                    vis_kart_for_boliger(df_sted_props, f"Sted: {valgt_sted_kart}")
                else:
                    st.info("Ingen steder tilgjengelig for kartvisning.")

# --------------------------------------------------
# TAB 3: GATE + STED
# --------------------------------------------------
with tab_gate:
    st.subheader("Statistikk per gate+sted (gatenavn uten nummer, pluss sted)")

    df_gate = df_scope[df_scope["gate_sted"] != ""].copy()

    if len(df_gate) == 0:
        st.info("Ingen gate-informasjon tilgjengelig i adressene.")
    else:
        agg_gate = aggregate_group(df_gate, ["gate_sted"])

        if len(agg_gate) == 0:
            st.info("Ingen gater med nok data til å beregne statistikk.")
        else:
            metrikk_valg_g = st.selectbox(
                "Velg metrikk for topp/bunn-lister (gate+sted)",
                ["median_m2", "median_totalpris", "median_dager"],
                format_func=lambda x: {
                    "median_m2": "Median m²-pris",
                    "median_totalpris": "Median totalpris",
                    "median_dager": "Median dager på markedet",
                }[x]
            )

            count_col_g = METRIC_TO_COUNT_COL[metrikk_valg_g]
            agg_gate_f = agg_gate[agg_gate[count_col_g] >= min_antall]
            agg_gate_f = agg_gate_f.dropna(subset=[metrikk_valg_g])

            if len(agg_gate_f) == 0:
                st.info(
                    f"Ingen gate+sted-kombinasjoner har minst {min_antall} gyldige "
                    f"observasjoner for valgt metrikk."
                )
            else:
                col1, col2 = st.columns(2)

                with col1:
                    st.markdown("**Topp gate+sted** (høye verdier)")
                    topp_g = agg_gate_f.sort_values(metrikk_valg_g, ascending=False).head(top_n)
                    topp_g_disp = topp_g.rename(columns={"gate_sted": "gate_sted_navn"})
                    st.dataframe(
                        topp_g_disp[["gate_sted_navn", "antall", "median_m2", "gj_snitt_m2",
                                     "median_totalpris", "median_dager"]],
                        use_container_width=True
                    )

                with col2:
                    st.markdown("**Bunn gate+sted** (lave verdier)")
                    bunn_g = agg_gate_f.sort_values(metrikk_valg_g, ascending=True).head(top_n)
                    bunn_g_disp = bunn_g.rename(columns={"gate_sted": "gate_sted_navn"})
                    st.dataframe(
                        bunn_g_disp[["gate_sted_navn", "antall", "median_m2", "gj_snitt_m2",
                                     "median_totalpris", "median_dager"]],
                        use_container_width=True
                    )

                st.markdown(
                    f"Viser topp/bunn gate+sted med minst **{min_antall}** gyldige "
                    f"observasjoner for valgt metrikk."
                )

                # --- Kart for valgt gate+sted ---
                st.markdown("---")
                st.markdown("#### Kart for valgt gate+sted")

                gate_options = (
                    pd.concat([topp_g["gate_sted"], bunn_g["gate_sted"]])
                    .drop_duplicates()
                    .tolist()
                )

                if gate_options:
                    valgt_gate_kart = st.selectbox(
                        "Velg gate+sted for kartvisning",
                        gate_options,
                        index=0,
                    )

                    df_gate_props = df_gate[df_gate["gate_sted"] == valgt_gate_kart].copy()
                    vis_kart_for_boliger(df_gate_props, f"Gate+sted: {valgt_gate_kart}")
                else:
                    st.info("Ingen gate+sted tilgjengelig for kartvisning.")
