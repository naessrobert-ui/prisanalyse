# bolig_kupp_service.py
import numpy as np
import pandas as pd
import folium
from folium.plugins import MarkerCluster
from sklearn.neighbors import BallTree


# --------------------------------------------------
# DATARENSING
# --------------------------------------------------
def clean_data(df_raw: pd.DataFrame) -> pd.DataFrame:
    df = df_raw.copy()

    # Normaliser kolonnenavn
    df.columns = [c.strip() for c in df.columns]

    # --- 1. M2-pris ---
    if "M2-pris" not in df.columns:
        candidate_cols = [
            c for c in df.columns
            if "m2" in c.lower() and "pris" in c.lower()
        ]
        if candidate_cols:
            df.rename(columns={candidate_cols[0]: "M2-pris"}, inplace=True)
        else:
            raise ValueError("Fant ingen kolonne for M2-pris i boligdataene.")

    df["M2-pris"] = df["M2-pris"].astype(str)
    df["M2-pris"] = df["M2-pris"].str.replace("kr", "", regex=False, case=False)
    df["M2-pris"] = df["M2-pris"].str.replace(" ", "", regex=False)
    df["M2-pris"] = df["M2-pris"].str.replace(".", "", regex=False)
    df["M2-pris"] = df["M2-pris"].str.replace(",", ".", regex=False)
    df["M2-pris"] = pd.to_numeric(df["M2-pris"], errors="coerce")

    # --- 2. Koordinater ---
    lat_col = None
    lon_col = None
    for c in df.columns:
        cl = c.lower()
        if "lat" in cl and lat_col is None:
            lat_col = c
        if any(x in cl for x in ["lon", "lng", "long"]) and lon_col is None:
            lon_col = c

    if lat_col is None or lon_col is None:
        raise ValueError("Fant ikke latitude/longitude-kolonner i boligdataene.")

    for col in [lat_col, lon_col]:
        df[col] = df[col].astype(str).str.replace(",", ".", regex=False)
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df.rename(columns={lat_col: "latitude", lon_col: "longitude"}, inplace=True)

    # --- 3. Areal-kolonne ---
    area_col = "size"
    if area_col not in df.columns:
        raise ValueError("Fant ikke arealkolonnen 'size' i boligdataene.")

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

    # --- 5. Dager på markedet (hvis eksisterer) ---
    # Vi forventer evt. at bolig_routes allerede har laget 'dager_paa_markedet'
    days_cand = None
    for cand in df.columns:
        cl = cand.lower()
        if "dager" in cl and ("marked" in cl or "til salgs" in cl):
            days_cand = cand
            break

    if days_cand is not None and days_cand != "dager_paa_markedet":
        df.rename(columns={days_cand: "dager_paa_markedet"}, inplace=True)

    # --- 6. Filtrer vekk åpenbart feil data ---
    df = df.dropna(subset=["latitude", "longitude", "M2-pris", "areal_m2"])
    df = df[df["M2-pris"] > 5000]
    df = df[
        (df["latitude"] > 57)
        & (df["latitude"] < 72)
        & (df["longitude"] > 4)
        & (df["longitude"] < 32)
    ]

    return df


# --------------------------------------------------
# GLOBAL UNDERPRISING (FYLKE/SEGMENT)
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
# LOKAL UNDERPRISING (NABO-BOLIGER)
# --------------------------------------------------
def add_local_price_metric(
    df: pd.DataFrame,
    n_neighbors: int = 50,
    max_distance_km: float = 3.0,
    min_neighbors: int = 10,
) -> pd.DataFrame:
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
# HOVEDFUNKSJON – brukes av Flask-route
# --------------------------------------------------
def build_underprisradar(
    df_raw: pd.DataFrame,
    *,
    valgt_fylke: str = "Alle",
    valgt_boligtype: str = "Alle",
    valgt_eierform: str = "Alle",
    max_dager: int | None = None,
    referansevalg: str = "segment",  # "segment" eller "lokal"
    min_segment_størrelse: int = 15,
    min_lokal_naboer: int = 20,
    min_underpris_pct: float = 5.0,
    kun_dyre_omraader: bool = False,
    min_dyrt_nivå: int = 60000,
    top_n: int = 50,
):
    """
    Returnerer:
      - sub: filtrert DataFrame med underpris-metrikker
      - map_html: HTML-streng for Folium-kart
    """
    df = clean_data(df_raw)
    df = add_underpricing_metrics(df)

    # Filteringsnivå
    sub = df.copy()

    if valgt_fylke != "Alle":
        sub = sub[sub["fylke"] == valgt_fylke]

    if valgt_boligtype != "Alle":
        sub = sub[sub["boligtype"] == valgt_boligtype]

    if valgt_eierform != "Alle":
        sub = sub[sub["eierform"] == valgt_eierform]

    if max_dager is not None and "dager_paa_markedet" in sub.columns:
        sub = sub[sub["dager_paa_markedet"] <= max_dager]

    if len(sub) == 0:
        return sub, None

    # Lokal naboanalyse hvis valgt
    if referansevalg == "lokal":
        sub = add_local_price_metric(
            sub,
            n_neighbors=50,
            max_distance_km=3.0,
            min_neighbors=min_lokal_naboer,
        )

    # Rangering + dyrt område-filter
    if referansevalg == "segment":
        sub = sub[
            (sub["antall_i_segment"] >= min_segment_størrelse)
            & (sub["underpris_pct"] > min_underpris_pct / 100.0)
        ]
        if kun_dyre_omraader:
            sub = sub[sub["referanse_M2"] >= min_dyrt_nivå]
        sort_col = "underpris_pct"
    else:
        if "lokal_underpris_pct" not in sub.columns:
            return sub.iloc[0:0], None

        sub = sub[
            (sub["lokal_antall_naboer"] >= min_lokal_naboer)
            & (sub["lokal_underpris_pct"] > min_underpris_pct / 100.0)
        ]
        if kun_dyre_omraader:
            sub = sub[sub["lokal_referanse_M2"] >= min_dyrt_nivå]
        sort_col = "lokal_underpris_pct"

    if len(sub) == 0:
        return sub, None

    sub = sub.sort_values(sort_col, ascending=False).head(top_n)

    # Bygg kart
    map_center_lat = sub["latitude"].mean()
    map_center_lon = sub["longitude"].mean()

    m = folium.Map(
        location=[map_center_lat, map_center_lon],
        zoom_start=5.5,
        tiles="cartodbpositron",
    )

    marker_cluster = MarkerCluster().add_to(m)

    for _, row in sub.iterrows():
        adresse = row.get("adresse", row.get("address", "Ukjent adresse"))
        postnr = row.get("postnummer", "")
        sted = (
            f"{adresse}, {postnr}"
            if postnr not in (None, "", np.nan)
            else adresse
        )

        m2 = row["M2-pris"]
        ref = row.get("referanse_M2")
        up_pct = row.get("underpris_pct")
        up_kr = row.get("underpris_kr")
        areal = row["areal_m2"]

        lok_ref = row.get("lokal_referanse_M2")
        lok_up_pct = row.get("lokal_underpris_pct")
        lok_up_kr = row.get("lokal_underpris_kr")

        finnkode = row.get("finnkode")
        finn_url = None
        finnline = ""
        if pd.notna(finnkode):
            try:
                fk_str = str(int(float(finnkode)))
                finn_url = (
                    f"https://www.finn.no/realestate/homes/ad.html?finnkode={fk_str}"
                )
                finnline = (
                    f"<a href='{finn_url}' target='_blank'>"
                    f"Åpne på FINN (kode {fk_str})</a>"
                )
            except Exception:
                pass

        popup_lines = [
            f"<b>{sted}</b>",
            f"{row.get('boligtype', '')} – {row.get('eierform', '')}",
            f"Areal: {areal:.0f} m²",
            f"M²-pris: {m2:,.0f} kr/m²".replace(",", " "),
        ]

        if pd.notna(ref) and pd.notna(up_pct) and pd.notna(up_kr):
            popup_lines += [
                f"Segment-ref (fylke m.m.): {ref:,.0f} kr/m²".replace(",", " "),
                f"Underpris vs segment: {up_pct*100:.1f} % (~{up_kr:,.0f} kr)".replace(
                    ",", " "
                ),
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

        if referansevalg == "segment":
            tooltip_ref = (
                f"{up_pct*100:.1f}% under segment-ref."
                if pd.notna(up_pct)
                else ""
            )
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

    map_html = m.get_root().render()
    return sub, map_html
