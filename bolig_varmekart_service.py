# bolig_varmekart_service.py
import pandas as pd


def clean_data(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Renser dataene slik at de kan brukes til varmekartet.
    Dette er i praksis det du hadde i app_varme.py, bare uten Streamlit.
    """
    df = df_raw.copy()

    # --- 1. M2-pris ---
    if "M2-pris" not in df.columns:
        raise ValueError("Fant ikke kolonnen 'M2-pris' i boligdata.")

    df["M2-pris"] = (
        df["M2-pris"]
        .astype(str)
        .str.replace("kr", "", regex=False, case=False)
        .str.replace(" ", "", regex=False)
        .str.replace(",", ".", regex=False)
    )
    df["M2-pris"] = pd.to_numeric(df["M2-pris"], errors="coerce")

    # --- 2. Koordinater ---
    for col in ["latitude", "longitude"]:
        if col not in df.columns:
            raise ValueError(f"Fant ikke kolonnen '{col}' i boligdata.")

        df[col] = (
            df[col]
            .astype(str)
            .str.replace(",", ".", regex=False)
        )
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # --- 3. Fjerne ugyldige rader ---
    df = df.dropna(subset=["latitude", "longitude", "M2-pris"])
    df = df[df["M2-pris"] > 5000]
    df = df[
        (df["latitude"] > 57)
        & (df["latitude"] < 72)
        & (df["longitude"] > 4)
        & (df["longitude"] < 32)
    ]

    # --- 4. Fylle tomme kategorier ---
    for col in ["fylke", "boligtype", "NY/Brukt", "eierform"]:
        if col in df.columns:
            df[col] = df[col].fillna("Ukjent").astype(str)

    return df
