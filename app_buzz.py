import streamlit as st
import pandas as pd
import numpy as np
import re
from collections import Counter
import matplotlib.pyplot as plt
from wordcloud import WordCloud

# Hent siste boligfil fra S3
from bolig_data import load_latest_bolig_df

# --------------------------------------------------
# KONFIG
# --------------------------------------------------
st.set_page_config(
    page_title="Buzzord i boligannonser",
    page_icon="💬",
    layout="wide"
)

st.title("💬 Buzzord-analyse i boligannonser")
st.markdown(
    "Analyser hvilke ord som går igjen i overskriftene (`full_title`) "
    "for **billige** vs **dyre** boliger – basert på m²-pris eller totalpris."
)

# --------------------------------------------------
# DATAINNLESING
# --------------------------------------------------
@st.cache_data(ttl=3600)
def load_data():
    """
    Laster inn boligdata fra siste bolig_X_*.csv via bolig_data.load_latest_bolig_df().
    """
    df = load_latest_bolig_df()
    if df is None or df.empty:
        st.error("Fant ingen bolig_X_*.csv i S3 – kan ikke gjøre buzzord-analyse.")
        st.stop()

    df.columns = df.columns.str.strip()
    return df


def clean_price_and_title(df_raw: pd.DataFrame) -> pd.DataFrame:
    df = df_raw.copy()

    # --- M2-pris ---
    if "M2-pris" not in df.columns:
        st.error("Fant ikke kolonnen 'M2-pris' i boligdataene.")
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

    # --- Totalpris (hvis finnes) ---
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

    # --- full_title ---
    if "full_title" not in df.columns:
        st.error("Fant ikke kolonnen 'full_title' i boligdataene.")
        st.stop()
    df["full_title"] = df["full_title"].astype(str)

    # --- fylke ---
    if "fylke" not in df.columns:
        df["fylke"] = "Ukjent"
    else:
        df["fylke"] = df["fylke"].fillna("Ukjent").astype(str)

    # --- address (kan brukes til stedsnavn) ---
    if "address" not in df.columns:
        df["address"] = ""
    else:
        df["address"] = df["address"].fillna("").astype(str)

    # filtrer bort helt meningsløse m2-priser
    df = df[df["M2-pris"] > 5000]

    return df


raw = load_data()
df = clean_price_and_title(raw)

st.success(f"Data lastet: {len(df):,} boliger med tittel og prisdata".replace(",", " "))


# --------------------------------------------------
# BYGG STEDSNAVN-STOPPLISTE (fra fylke + address)
# --------------------------------------------------
def build_place_stopwords(df_all: pd.DataFrame) -> set:
    words = set()

    # Fylkenavn
    for v in df_all["fylke"].dropna().astype(str):
        for tok in re.split(r"[^0-9a-zA-ZæøåÆØÅ]+", v):
            tok = tok.strip().lower()
            if len(tok) > 2:
                words.add(tok)

    # Adresse-komponenter
    for v in df_all["address"].dropna().astype(str):
        v = v.lower()
        v = re.sub(r"[^0-9a-zæøå]+", " ", v)
        for tok in v.split():
            if len(tok) > 2:
                words.add(tok)

    return words


PLACE_STOPWORDS = build_place_stopwords(df)

# --------------------------------------------------
# SIDEBAR: FYLKE, METRIKK, METODE
# --------------------------------------------------
st.sidebar.header("Valg av område og prisklasse")

# Fylke-filter
fylker = ["Hele Norge"] + sorted(df["fylke"].dropna().unique().tolist())
valgt_fylke = st.sidebar.selectbox("Fylke", fylker, index=0)

if valgt_fylke == "Hele Norge":
    df_scope = df.copy()
else:
    df_scope = df[df["fylke"] == valgt_fylke].copy()
    st.sidebar.markdown(f"Antall boliger i valgt fylke: **{len(df_scope)}**")

if len(df_scope) < 100:
    st.warning("Veldig få boliger i valgt område – analysen blir litt mer støyete.")

# Velg pris-metrikk
metrikknavn = st.sidebar.radio(
    "Hvilken pris skal definere dyr/billig?",
    ["M²-pris (M2-pris)", "Totalpris (totalpris)"]
)
if "M²-pris" in metrikknavn:
    metric_col = "M2-pris"
    metric_label = "kr/m²"
else:
    metric_col = "totalpris"
    metric_label = "kr"

df_scope = df_scope.dropna(subset=[metric_col])
metric_values = df_scope[metric_col].values

if len(metric_values) == 0:
    st.error(f"Ingen gyldige verdier for {metric_col} i valgt område.")
    st.stop()

# Hvordan definere prisklasse?
valg_metode = st.sidebar.radio(
    "Måte å definere prisklasse",
    ["Prosent (kvantiler)", "Direkte intervall i kroner"],
)

# Fjern stedsnavn?
fjern_stedsnavn = st.sidebar.checkbox(
    "Fjern stedsnavn (fra fylke/adresse) fra ord-analysen",
    value=True
)

# --------------------------------------------------
# PRISKLASSE: PROSENTBASERT
# --------------------------------------------------
if valg_metode == "Prosent (kvantiler)":
    pct = st.sidebar.number_input(
        "Andel (prosent) som skal være billig/dyr gruppe",
        min_value=1, max_value=40, value=10, step=1
    )

    gruppevalg = st.sidebar.radio(
        "Hvilken gruppe vil du analysere?",
        [
            f"Billigste {pct} %",
            f"Dyreste {pct} %",
            f"Midterste ({pct}–{100-pct} %)"
        ]
    )

    q_low = np.percentile(metric_values, pct)
    q_high = np.percentile(metric_values, 100 - pct)

    if gruppevalg.startswith("Billigste"):
        df_sel = df_scope[df_scope[metric_col] <= q_low]
    elif gruppevalg.startswith("Dyreste"):
        df_sel = df_scope[df_scope[metric_col] >= q_high]
    else:
        df_sel = df_scope[(df_scope[metric_col] > q_low) & (df_scope[metric_col] < q_high)]

    gruppebeskrivelse = gruppevalg

# --------------------------------------------------
# PRISKLASSE: DIREKTE INTERVALL
# --------------------------------------------------
else:
    # Bruk 1. og 99. percentil som default
    low_bound = int(np.percentile(metric_values, 1))
    high_bound = int(np.percentile(metric_values, 99))

    step = 1000 if metric_col == "M2-pris" else 50_000

    min_val, max_val = st.sidebar.slider(
        "Velg prisintervall",
        min_value=int(low_bound),
        max_value=int(high_bound),
        value=(int(low_bound), int(high_bound)),
        step=step,
        help="Intervall utenfor dette kan inneholde ekstreme outliers."
    )

    df_sel = df_scope[(df_scope[metric_col] >= min_val) & (df_scope[metric_col] <= max_val)]
    gruppebeskrivelse = f"Intervall {min_val:,}–{max_val:,} {metric_label}".replace(",", " ")

# --------------------------------------------------
# SJEKK UTVALG
# --------------------------------------------------
st.sidebar.markdown(f"**Antall boliger i prisklassen:** {len(df_sel)}")

if len(df_sel) < 30:
    st.warning("Utvalget er lite – resultater bør tolkes med varsomhet.")
if len(df_sel) == 0:
    st.stop()

st.markdown(
    f"**Analyserer:** {gruppebeskrivelse} i "
    f"{'hele Norge' if valgt_fylke=='Hele Norge' else valgt_fylke} "
    f"(basert på {metrikknavn})."
)

# --------------------------------------------------
# NLP / TOKENISERING
# --------------------------------------------------
BASE_STOPWORDS = {
    "og", "i", "på", "med", "til", "fra", "for", "av", "som", "en", "et", "den",
    "det", "de", "vi", "du", "er", "har", "kan", "må", "om", "år", "rom",
    "nye", "ny", "flott", "lekker", "meget", "stor", "pen", "fin",
    "bolig", "leilighet", "enebolig", "tomannsbolig", "rekkehus", "selges",
    "tilsalgs", "til", "salg", "midt", "sentral", "sentralbeliggende",
    "visning", "torsdag", "søndag", "mandag", "fredag", "lørdag",
}

if fjern_stedsnavn:
    STOPWORDS = BASE_STOPWORDS | PLACE_STOPWORDS
else:
    STOPWORDS = BASE_STOPWORDS


def tokenize(text: str):
    text = text.lower()
    text = re.sub(r"[^0-9a-zæøå]+", " ", text)
    tokens = text.split()
    return [t for t in tokens if len(t) > 2 and t not in STOPWORDS]


def count_tokens(series: pd.Series) -> Counter:
    c = Counter()
    for t in series:
        c.update(tokenize(t))
    return c


# --------------------------------------------------
# FREKVENSER FOR VALGT GRUPPE OG REST
# --------------------------------------------------
counts_sel = count_tokens(df_sel["full_title"])

df_rest = df_scope.loc[~df_scope.index.isin(df_sel.index)]
counts_rest = count_tokens(df_rest["full_title"])

if not counts_sel:
    st.warning("Fant ingen ord å analysere i valgt prisklasse.")
    st.stop()

# --------------------------------------------------
# DIFFERANSEORD
# --------------------------------------------------
def compute_diff_words(counts_a: Counter, counts_b: Counter,
                       min_total: int = 5, top_n: int = 30) -> pd.DataFrame:
    all_words = set(counts_a) | set(counts_b)
    total_a = sum(counts_a.values())
    total_b = sum(counts_b.values())
    alpha = 0.5  # smoothing

    rows = []
    for w in all_words:
        tot = counts_a[w] + counts_b[w]
        if tot < min_total:
            continue

        p_a = (counts_a[w] + alpha) / (total_a + alpha * len(all_words))
        p_b = (counts_b[w] + alpha) / (total_b + alpha * len(all_words))

        ratio = p_a / p_b
        log2_ratio = np.log2(ratio)
        rows.append((w, counts_a[w], counts_b[w], log2_ratio))

    if not rows:
        return pd.DataFrame(columns=["ord", "treff_valgt", "treff_rest", "log2_forhold", "relativ_faktor"])

    df_diff_ = pd.DataFrame(rows, columns=["ord", "treff_valgt", "treff_rest", "log2_forhold"])
    df_diff_["relativ_faktor"] = (2 ** df_diff_["log2_forhold"]).round(2)
    df_diff_ = df_diff_.sort_values("log2_forhold", ascending=False).head(top_n)
    return df_diff_


df_diff = compute_diff_words(counts_sel, counts_rest, min_total=5, top_n=40)

# --------------------------------------------------
# WORD CLOUD + TOPPLISTE + DIFFERANSEORD
# --------------------------------------------------
col_wc, col_top, col_diff = st.columns([2, 1, 1])

with col_wc:
    st.subheader("Word cloud for valgt prisklasse")
    text_for_wc = " ".join([word for word, freq in counts_sel.items() for _ in range(freq)])
    wc = WordCloud(
        width=1000,
        height=500,
        background_color="white",
        collocations=False
    ).generate(text_for_wc)

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.imshow(wc, interpolation="bilinear")
    ax.axis("off")
    st.pyplot(fig)

with col_top:
    st.subheader("Mest brukte ord (valgt gruppe)")
    top_n = st.slider("Antall ord", min_value=10, max_value=100, value=30, step=5)
    common_words = counts_sel.most_common(top_n)
    df_words = pd.DataFrame(common_words, columns=["ord", "antall"])
    st.dataframe(df_words, use_container_width=True)

with col_diff:
    st.subheader("Differanseord")
    st.markdown(
        "Ord som er **relativt vanligere** i valgt prisklasse enn i resten "
        f"av utvalget i {'Norge' if valgt_fylke=='Hele Norge' else valgt_fylke}."
    )
    if len(df_diff) == 0:
        st.info("Ingen tydelige differanseord (for få forekomster).")
    else:
        st.dataframe(
            df_diff[["ord", "treff_valgt", "treff_rest", "relativ_faktor"]], use_container_width=True
        )

# --------------------------------------------------
# TEKST-RAPPORT / SAMMENDRAG
# --------------------------------------------------
st.markdown("### 📄 Automatisk tekst-rapport")

median_sel = df_sel[metric_col].median()
median_rest = df_rest[metric_col].median() if len(df_rest) > 0 else np.nan

retning = "høyere" if median_sel > median_rest else "lavere"
diff_prosent = (
    (median_sel - median_rest) / median_rest * 100
    if not np.isnan(median_rest) and median_rest > 0 else 0
)

# Plukk ut de 5–7 mest markante differanseordene
n_highlight = 7
highlight = df_diff.head(n_highlight)

if len(highlight) == 0:
    st.write("For få tydelige differanseord til å lage en meningsfull tekstlig oppsummering.")
else:
    # Del ordene litt etter styrke
    strong = highlight[highlight["relativ_faktor"] >= 3]["ord"].tolist()
    medium = highlight[
        (highlight["relativ_faktor"] < 3) & (highlight["relativ_faktor"] >= 1.5)
    ]["ord"].tolist()

    område_txt = "i hele Norge" if valgt_fylke == "Hele Norge" else f"i {valgt_fylke}"
    metrikk_txt = "m²-pris" if metric_col == "M2-pris" else "totalpris"

    lines = []

    lines.append(
        f"I denne analysen ser vi på **{gruppebeskrivelse.lower()}** "
        f"{område_txt}, målt etter **{metrikk_txt}**."
    )

    if not np.isnan(median_rest) and median_rest > 0:
        lines.append(
            f"Median {metrikk_txt} i denne gruppen er omtrent "
            f"{median_sel:,.0f} {metric_label} mot {median_rest:,.0f} {metric_label} "
            f"i resten av markedet – altså ca. {diff_prosent:+.1f} % {retning}."
            .replace(",", " ")
        )

    if strong:
        lines.append(
            f"Ord som peker seg spesielt ut i denne gruppen, sammenlignet med resten, "
            f"er blant annet **{', '.join(strong)}**. "
            "Disse forekommer typisk tre ganger så ofte eller mer i denne prisklassen."
        )

    if medium:
        lines.append(
            f"I tillegg ser vi at ord som **{', '.join(medium)}** "
            "også er klart vanligere her enn i resten av markedet."
        )

    if fjern_stedsnavn:
        lines.append(
            "Stedsnavn er filtrert bort, slik at forskjellene i hovedsak reflekterer "
            "hvordan boligene markedsføres – ikke hvor de ligger."
        )

    st.markdown("\n\n".join(lines))

st.markdown(
    """
**Tips til bruk:**

- Velg *Dyreste 10 %* i et dyrt fylke (f.eks. Oslo) og se hvilke ord som dominerer – typisk premium-markedsføring.  
- Bytt til *Billigste 10 %* og se hvilke ord som klumper seg på den siden – gjerne *oppussingsobjekt*, *potensial* osv.  
- Skru av «Fjern stedsnavn» hvis du vil se hvilke bydeler som driver prisene.
"""
)
