import streamlit as st
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import boto3
import io
from datetime import date, datetime, timedelta

# ======================================================
# 1. KONFIGURASJON (Fyll inn det samme som i skraperen)
# ======================================================

# Bytt ut disse med dine faktiske nøkler!
AWS_KEY = "AKIA6I7OP6NWGKAXVUXE"
AWS_SECRET = "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"
AWS_REGION = "eu-north-1"  # Eller "us-east-1" osv.
S3_BUCKET_NAME = "prisanalyse-data"

PARQUET_KEY = "calc/bil/bil_time.parquet"
FINN_BASE_URL = "https://www.finn.no/mobility/item/"

GRUPPERINGSNIVAAER = {
    "1. Produsent": ["Merke"],
    "2. Produsent + Modell": ["Merke", "Modell"],
    "3. Prod + Modell + År": ["Merke", "Modell", "År"],
    "4. Prod + Modell + Drivstoff + År": ["Merke", "Modell", "Drivstoff", "År"]
}


# ======================================================
# 2. BAKEND LOGIKK
# ======================================================

def _get_s3_client():
    return boto3.client (
        "s3",
        aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SECRET,
        region_name=AWS_REGION,
    )


@st.cache_data (ttl=3600)
def last_data_fra_s3():
    try:
        s3 = _get_s3_client ()
        obj = s3.get_object (Bucket=S3_BUCKET_NAME, Key=PARQUET_KEY)
        data = obj["Body"].read ()
        table = pq.read_table (pa.BufferReader (data))
        df = table.to_pandas ()
        df.columns = [str (c).strip () for c in df.columns]

        if "snapshot_time" in df.columns:
            df["snapshot_time"] = pd.to_datetime (df["snapshot_time"], errors="coerce")
        else:
            return pd.DataFrame ()
        return df
    except Exception as e:
        st.error (f"Feil ved henting: {e}")
        return pd.DataFrame ()


def _is_sold_strict(pris_val) -> bool:
    if pd.isna (pris_val): return False
    return "solgt" in str (pris_val).lower ()


def _extract_numeric(val):
    if pd.isna (val): return 0
    s = str (val).strip ().replace (" ", "").replace ("\xa0", "").replace ("kr", "")
    try:
        return int (float (s.split ("(")[0]))
    except ValueError:
        return 0


@st.cache_data (ttl=3600)
def analyser_markedet(startdato: date, max_timer: float, grupperings_kolonner: list):
    df = last_data_fra_s3 ()
    if df.empty: return pd.DataFrame (), pd.DataFrame ()

    cols = df.columns
    c_merke = next ((c for c in ["Merke", "produsent"] if c in cols), "Merke")
    c_modell = next ((c for c in ["Modell", "modell", "Info"] if c in cols), "Modell")
    c_pris = next ((c for c in ["Pris", "pris_num"] if c in cols), None)
    c_aar = next ((c for c in ["Årstall", "årstall"] if c in cols), "Årstall")
    c_driv = next ((c for c in ["Drivstoff", "drivstoff"] if c in cols), "Drivstoff")
    c_forh = next ((c for c in ["Forhandler type", "selger"] if c in cols), "Ukjent")

    col_map = {"Merke": c_merke, "Modell": c_modell, "År": c_aar, "Drivstoff": c_driv}
    df_group_cols = [col_map.get (k, k) for k in grupperings_kolonner]

    stats_data = {}
    sold_cars_list = []

    # Grupper per FinnKode
    for finnkode, grp in df.groupby ("FinnKode"):
        g = grp.sort_values ("snapshot_time")
        if g.empty: continue

        # Vi ignorerer biler som ble solgt FØR startdato
        if g.iloc[-1]["snapshot_time"].date () < startdato:
            continue

        first_row = g.iloc[0]

        # Lag gruppenøkkel
        group_values = []
        for col in df_group_cols:
            val = str (first_row.get (col, "Ukjent"))
            group_values.append (val)
        group_key = tuple (group_values)

        if group_key not in stats_data:
            stats_data[group_key] = {"Total": 0, "Solgt": 0, "SumTimer": 0.0, "SumPris": 0}

        stats_data[group_key]["Total"] += 1

        if not c_pris or c_pris not in g.columns: continue

        sold_rows = g[g[c_pris].apply (_is_sold_strict)]

        if not sold_rows.empty:
            sale_row = sold_rows.iloc[0]
            first_time = first_row["snapshot_time"]
            sale_time = sale_row["snapshot_time"]

            if sale_time.date () >= startdato:
                timer_ute = (sale_time - first_time).total_seconds () / 3600.0
                if timer_ute < 0: timer_ute = 0

                if timer_ute <= max_timer:
                    stats_data[group_key]["Solgt"] += 1
                    stats_data[group_key]["SumTimer"] += timer_ute

                    # Prøv å hente pris fra den første raden (før den ble "Solgt")
                    # Hvis den ble importert som "Solgt" direkte, vil prisen være 0.
                    pris = _extract_numeric (first_row.get (c_pris))
                    stats_data[group_key]["SumPris"] += pris

                    sold_cars_list.append ({
                        "FinnKode": str (finnkode),
                        "Link": f"{FINN_BASE_URL}{finnkode}",
                        "GruppeKey": group_key,
                        "Merke": str (first_row.get (c_merke, "")),
                        "Modell": str (first_row.get (c_modell, "")),
                        "Forhandler": str (first_row.get (c_forh, "")),
                        "Salgspris": pris,
                        "Timer til salg": round (timer_ute, 1),
                        "Salgsdato": sale_time.date ()
                    })

    stats_rows = []
    for key, val in stats_data.items ():
        if val["Solgt"] > 0:
            avg_time = val["SumTimer"] / val["Solgt"]
            avg_price = val["SumPris"] / val["Solgt"]
            andel = (val["Solgt"] / val["Total"]) * 100 if val["Total"] > 0 else 0

            row = dict (zip (grupperings_kolonner, key))
            row.update ({
                "Totalt observert": val["Total"],
                "Antall Raskt Solgt": val["Solgt"],
                "Andel Solgt (%)": round (andel, 1),
                "Snitt Timer": round (avg_time, 1),
                "Snitt Pris": int (avg_price),
                "GruppeKey": key
            })
            stats_rows.append (row)

    return pd.DataFrame (stats_rows), pd.DataFrame (sold_cars_list)


# ======================================================
# 3. FRONTEND
# ======================================================

def main():
    st.set_page_config (page_title="Markedsanalyse", layout="wide")
    st.title ("🚀 Markedsanalyse: Hva selges raskt?")

    # --- SIDEBAR ---
    with st.sidebar:
        st.header ("Innstillinger")
        valgt_nivaa_navn = st.radio ("Gruppering:", list (GRUPPERINGSNIVAAER.keys ()), index=3)
        valgte_kolonner = GRUPPERINGSNIVAAER[valgt_nivaa_navn]
        st.divider ()
        max_timer = st.number_input ("Maks timer til salg:", value=48.0, step=1.0)
        dager_tilbake = st.slider ("Periode (dager):", 1, 60, 14)
        startdato = date.today () - timedelta (days=dager_tilbake)
        st.divider ()
        sort_option = st.selectbox ("Sorter tabell etter:",
                                    ["Antall Raskt Solgt", "Andel Solgt (%)", "Alfabetisk"])

        if st.button ("Oppdater"):
            st.cache_data.clear ()
            st.rerun ()

    # --- DATA PROCESS ---
    with st.spinner ("Analyserer data..."):
        stats_df, details_df = analyser_markedet (startdato, max_timer, valgte_kolonner)

    if stats_df.empty:
        st.info ("Ingen biler funnet som matcher kriteriene.")
        return

    # --- SORTERING ---
    if sort_option == "Antall Raskt Solgt":
        stats_df = stats_df.sort_values (by="Antall Raskt Solgt", ascending=False)
    elif sort_option == "Andel Solgt (%)":
        stats_df = stats_df.sort_values (by=["Andel Solgt (%)", "Totalt observert"], ascending=[False, False])
    else:
        stats_df = stats_df.sort_values (by=valgte_kolonner[0], ascending=True)

    stats_display = stats_df.drop (columns=["GruppeKey"])

    cols = list (stats_display.columns)
    tekst_cols = valgte_kolonner
    tall_cols = [c for c in cols if c not in tekst_cols]
    stats_display = stats_display[tekst_cols + tall_cols]

    # --- VISNING ---
    st.subheader (f"Oversikt ({len (stats_df)} grupper)")
    st.write ("💡 *Klikk på en rad for å se bilene.*")

    col_config = {
        "Andel Solgt (%)": st.column_config.ProgressColumn ("Andel Solgt", format="%.1f%%", min_value=0, max_value=100),
        "Snitt Pris": st.column_config.NumberColumn ("Snitt Pris", format="%d kr"),
        "Snitt Timer": st.column_config.NumberColumn ("Snitt Tid", format="%.1f t"),
    }

    event = st.dataframe (
        stats_display,
        column_config=col_config,
        use_container_width=True,
        hide_index=True,
        on_select="rerun",
        selection_mode="single-row"
    )

    # --- HER ER FEILRETTINGEN ---
    # Vi sjekker om 'selection' finnes i event-objektet og om det inneholder rader
    selected_rows = event.get ("selection", {}).get ("rows", [])

    if selected_rows:
        selected_idx = selected_rows[0]
        selected_row = stats_df.iloc[selected_idx]
        selected_key = selected_row["GruppeKey"]

        subset = details_df[details_df["GruppeKey"] == selected_key].sort_values ("Timer til salg")

        st.divider ()
        overskrift = " ".join (selected_key) if isinstance (selected_key, tuple) else str (selected_key)
        st.markdown (f"### 🕵️ Detaljer for: {overskrift}")

        # Hvis snittprisen er 0, vis en melding om hvorfor
        if selected_row["Snitt Pris"] == 0:
            st.caption (
                "Obs: Pris er 0 kr fordi disse bilene ble importert til systemet som 'Solgt' (ingen historisk pris funnet).")

        st.dataframe (
            subset[[
                "Link", "Timer til salg", "Salgspris", "Salgsdato", "Forhandler", "Merke", "Modell"
            ]],
            column_config={
                "Link": st.column_config.LinkColumn ("Annonse", display_text="Åpne"),
                "Timer til salg": st.column_config.NumberColumn ("Tid ute", format="%.1f t"),
                "Salgspris": st.column_config.NumberColumn ("Pris", format="%d kr"),
                "Salgsdato": st.column_config.DateColumn ("Dato")
            },
            hide_index=True,
            use_container_width=True
        )


if __name__ == "__main__":
    main ()