import streamlit as st
import pandas as pd
import folium
from folium.plugins import HeatMap
from streamlit_folium import st_folium

# Henter siste boligfil fra S3 / prisanalyse-data
from bolig_data import load_latest_bolig_df

# --- KONFIGURASJON AV APPEN ---
st.set_page_config(
    page_title="Boligpris Heatmap Norge",
    page_icon="🏠",
    layout="wide"
)


# --- FUNKSJON FOR Å LASTE DATA ---
@st.cache_data(ttl=3600)  # Cache data i 1 time for bedre ytelse
def load_data():
    """
    Laster inn boligdata fra siste bolig_X_*.csv via bolig_data.load_latest_bolig_df().
    """
    df = load_latest_bolig_df()
    if df is None or df.empty:
        st.error("Fant ingen bolig_X_*.csv i S3 – kan ikke vise varmekart.")
        st.stop()

    df.columns = df.columns.str.strip()
    return df


# --- DATARENSING ---
def clean_data(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Renser dataene og sikrer at de er i riktig format for analyse og kart.
    Dette er kritisk for at appen skal virke med ekte data.
    """
    df = df_raw.copy()

    # --- 1. Håndtere M2-pris ---
    if "M2-pris" not in df.columns:
        st.error("Fant ikke kolonnen 'M2-pris' i boligdataene.")
        st.stop()

    # Sikre at det er tekst først, så vi kan bruke tekst-metoder
    df["M2-pris"] = df["M2-pris"].astype(str)
    # Fjern 'kr', mellomrom (tusen-skille) og eventuelle punktum brukt som tusenskille
    df["M2-pris"] = df["M2-pris"].str.replace("kr", "", regex=False, case=False)
    df["M2-pris"] = df["M2-pris"].str.replace(" ", "", regex=False)
    # Hvis noen bruker komma som desimal i m2-pris:
    df["M2-pris"] = df["M2-pris"].str.replace(",", ".", regex=False)

    # Konverter til tall (numeric). 'coerce' betyr at hvis den ikke klarer det, blir det NaN (tomt)
    df["M2-pris"] = pd.to_numeric(df["M2-pris"], errors="coerce")

    # --- 2. Håndtere Koordinater (Latitude/Longitude) ---
    for col in ["latitude", "longitude"]:
        if col not in df.columns:
            st.error(f"Fant ikke kolonnen '{col}' i boligdataene.")
            st.stop()

        # Konverter til tekst først
        df[col] = df[col].astype(str)
        # Bytt komma med punktum
        df[col] = df[col].str.replace(",", ".", regex=False)
        # Konverter til tall
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # --- 3. Fjerne ugyldige data ---
    # Vi må fjerne rader som nå mangler pris eller koordinater etter konverteringen
    df = df.dropna(subset=["latitude", "longitude", "M2-pris"])

    # Fjerne urealistiske priser (f.eks. pris på 0 eller 1 kr, som ofte er feil i data)
    # Setter grensen til 5000 kr/m2 for å fjerne de verste feilene og "lokkepriser"
    df = df[df["M2-pris"] > 5000]

    # Fjerne ugyldige koordinater (utenfor Norge grovt sett, eller 0,0)
    df = df[
        (df["latitude"] > 57)
        & (df["latitude"] < 72)
        & (df["longitude"] > 4)
        & (df["longitude"] < 32)
    ]

    # --- 4. Fylle tomme kategorier ---
    # Hvis fylke, boligtype etc mangler, fyll med "Ukjent" så filteret virker
    fill_cols = ["fylke", "boligtype", "NY/Brukt", "eierform"]
    for col in fill_cols:
        if col in df.columns:
            df[col] = df[col].fillna("Ukjent").astype(str)

    return df


# --- HOVEDAPP ---
st.title("🏠 Geografisk Varmekart: Norske Boligpriser")
st.markdown(
    "Visualisering av **kvadratmeterpris (M²-pris)** basert på siste tilgjengelige "
    "boligfil (bolig_X_*.csv) fra datasettet."
)

# Laste og rense data
with st.spinner("Laster og vasker boligdata... dette kan ta noen sekunder..."):
    raw_data = load_data()
    df = clean_data(raw_data)

if len(df) == 0:
    st.error(
        "Ingen data igjen etter rensing. "
        "Sjekk at 'M2-pris', 'latitude' og 'longitude' i dataene inneholder gyldige tall."
    )
    st.stop()

st.success(f"Data lastet! Viser {len(df)} boliger klare for analyse.")

# --- SIDEBAR FILTRE ---
st.sidebar.header("Filtrer Visning")

# Filter 1: Fylke (hvis kolonnen finnes)
if "fylke" in df.columns:
    alle_fylker = sorted(df["fylke"].unique().tolist())
    valgte_fylker = st.sidebar.multiselect("Velg Fylke(r):", alle_fylker, default=alle_fylker)
    if valgte_fylker:
        df = df[df["fylke"].isin(valgte_fylker)]

# Filter 2: Boligtype
if "boligtype" in df.columns:
    alle_typer = sorted(df["boligtype"].unique().tolist())
    # Velger de vanligste som default for ikke å overvelde kartet
    default_typer = [t for t in alle_typer if t in ["Leilighet", "Enebolig", "Rekkehus"]]
    if not default_typer:
        default_typer = alle_typer

    valgte_typer = st.sidebar.multiselect(
        "Velg Boligtype:", alle_typer, default=default_typer
    )
    if valgte_typer:
        df = df[df["boligtype"].isin(valgte_typer)]

# Filter 3: Nytt eller Brukt
if "NY/Brukt" in df.columns:
    ny_brukt_valg = st.sidebar.radio("Nytt eller Brukt:", ["Alle", "Brukt", "Nybygg"])
    if ny_brukt_valg == "Brukt":
        df = df[df["NY/Brukt"] == "Brukt"]
    elif ny_brukt_valg == "Nybygg":
        df = df[df["NY/Brukt"] == "Nybygg"]

st.sidebar.markdown("---")

# Filter 4: Pris-slider
if len(df) > 0:
    min_price_data = int(df["M2-pris"].min())
    max_price_data = int(df["M2-pris"].max())

    # Setter fornuftige defaults for slideren
    default_min = max(min_price_data, 20000)
    default_max = min(max_price_data, 200000)

    price_filter = st.sidebar.slider(
        "M²-pris område (kr):",
        min_value=min_price_data,
        max_value=max_price_data,
        value=(default_min, default_max),
        step=5000,
    )
    # Filtrer data basert på slideren
    filtered_df = df[
        (df["M2-pris"] >= price_filter[0]) & (df["M2-pris"] <= price_filter[1])
    ]
else:
    filtered_df = df
    st.sidebar.warning("Ingen data matcher filtrene over.")

# --- KART-OPPSETT ---
if len(filtered_df) > 0:
    # Finn senteret for kartet
    map_center_lat = filtered_df["latitude"].mean()
    map_center_lon = filtered_df["longitude"].mean()

    # Lag grunnkartet. Bruker "cartodbpositron" for et rent bakgrunnskart
    m = folium.Map(
        location=[map_center_lat, map_center_lon],
        zoom_start=6,
        tiles="cartodbpositron",
    )

    # Forbered data for HeatMap. Liste av [lat, lon, pris]
    heat_data = filtered_df[["latitude", "longitude", "M2-pris"]].values.tolist()

    # Fargeskala
    color_gradient = {0.2: "blue", 0.4: "cyan", 0.6: "lime", 0.8: "yellow", 1.0: "red"}

    # Lag HeatMap-laget
    HeatMap(
        heat_data,
        name="M2 Pris Varmekart",
        min_opacity=0.3,
        max_zoom=15,
        radius=18,  # Juster opp for større "blobs", ned for mer presisjon
        blur=15,  # Juster for mer/mindre glidende overgang
        gradient=color_gradient,
    ).add_to(m)

    # --- VIS KARTET ---
    st.subheader(f"Viser {len(filtered_df)} boliger etter filtrering")
    st_folium(m, width=1000, height=650)

    # --- METRIKKER ---
    st.markdown("### Nøkkeltall for utvalget")
    col1, col2, col3 = st.columns(3)
    col1.metric(
        "Snitt M²-pris",
        f"{int(filtered_df['M2-pris'].mean()):,} kr".replace(",", " "),
    )
    col2.metric(
        "Høyeste M²-pris",
        f"{int(filtered_df['M2-pris'].max()):,} kr".replace(",", " "),
    )
    col3.metric("Antall boliger", f"{len(filtered_df)}")

else:
    st.warning(
        "Ingen boliger matcher de valgte filtrene. "
        "Prøv å justere filtrene i menyen til venstre."
    )
