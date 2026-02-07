import os

import streamlit as st
from PIL import Image

# --- GLOBAL PAGE CONFIG ---
st.set_page_config(page_title="Boliganalyse", layout="wide")

# --- BACKGROUND FULL SCREEN ---
page_bg_img = """
<style>
[data-testid="stAppViewContainer"] {
    background-image: url('https://images.unsplash.com/photo-1526401485004-46910ecc8e51');
    background-size: cover;
    background-position: center;
    background-attachment: fixed;
}

[data-testid="stHeader"] {background-color: rgba(0,0,0,0)}
[data-testid="stSidebar"] {background-color: rgba(28, 28, 28, 0.92)}

.block-container {
    background: rgba(0,0,0,0.55) !important;
    border-radius: 12px;
    padding: 40px 60px;
    margin-top: 40px;
}
a {text-decoration: none}
</style>
"""
st.markdown(page_bg_img, unsafe_allow_html=True)

# --- TITLE ---
st.markdown("""
<h1 style="text-align:center; color:white; font-size:52px;">
Boliganalyse
</h1>
<p style="text-align:center; color:#f1f1f1; font-size:20px;">
Velg analysemetode for boligmarkedet i Norge
</p>
""", unsafe_allow_html=True)

st.write("")
st.write("")

# --- MENU GRID ---
historikk_url = os.environ.get("BOLIG_HISTORIKK_URL", "http://localhost:8501")

col1, col2, col3 = st.columns(3)

with col1:
    st.markdown("""
    <div style="background: rgba(30,30,60,0.85); padding:30px; border-radius:18px;">
        <h3 style="color:white;">🏠 Boliger for salg</h3>
        <p style="color:#ddd; font-size:15px;">
            Filter og analyser dagens annonser – m²-pris,
            totalpris og dager på markedet.
        </p>
        <a href="/bolig"><button style="width:100%; padding:12px; background:#005eff; color:white; border:none; border-radius:6px;">
            Start analyse
        </button></a>
    </div>
    """, unsafe_allow_html=True)

with col2:
    st.markdown(f"""
    <div style="background: rgba(30,30,60,0.85); padding:30px; border-radius:18px;">
        <h3 style="color:white;">📈 Historisk utvikling</h3>
        <p style="color:#ddd; font-size:15px;">
            Se prisutvikling over tid per fylke og samlet for Norge.
        </p>
        <a href="{historikk_url}"><button style="width:100%; padding:12px; background:#005eff; color:white; border:none; border-radius:6px;">
            Åpne historikk
        </button></a>
    </div>
    """, unsafe_allow_html=True)

with col3:
    st.markdown("""
    <div style="background: rgba(30,30,60,0.85); padding:30px; border-radius:18px;">
        <h3 style="color:white;">📍 Priser per sted</h3>
        <p style="color:#ddd; font-size:15px;">
            Median m²-pris og totalpris per fylke, sted og gate.
        </p>
        <a href="/region"><button style="width:100%; padding:12px; background:#005eff; color:white; border:none; border-radius:6px;">
            Åpne sted-analyse
        </button></a>
    </div>
    """, unsafe_allow_html=True)

# --- ROW 2 ---
col4, col5, col6 = st.columns(3)

with col4:
    st.markdown("""
    <div style="background: rgba(30,30,60,0.85); padding:30px; border-radius:18px;">
        <h3 style="color:white;">💸 Underprisradar</h3>
        <p style="color:#ddd; font-size:15px;">
            Finn boliger priset under referansenivå – både fylkesvis og lokalt.
        </p>
        <a href="/kupp"><button style="width:100%; padding:12px; background:#005eff; color:white; border:none; border-radius:6px;">
            Finn kupp
        </button></a>
    </div>
    """, unsafe_allow_html=True)


with col5:
    st.markdown("""
    <div style="background: rgba(30,30,60,0.85); padding:30px; border-radius:18px;">
        <h3 style="color:white;">🧠 Buzzord</h3>
        <p style="color:#ddd; font-size:15px;">
            Ord og differanseord som kjennetegner prisnivå.
        </p>
        <a href="/buzz"><button style="width:100%; padding:12px; background:#005eff; color:white; border:none; border-radius:6px;">
            Se buzzordanalyse
        </button></a>
    </div>
    """, unsafe_allow_html=True)

with col6:
    st.markdown("""
    <div style="background: rgba(30,30,60,0.85); padding:30px; border-radius:18px;">
        <h3 style="color:white;">🌍 Varmekart</h3>
        <p style="color:#ddd; font-size:15px;">
            Geografisk oversikt over m²-priser i Norge.
        </p>
        <a href="/heatmap"><button style="width:100%; padding:12px; background:#005eff; color:white; border:none; border-radius:6px;">
            Vis varmekart
        </button></a>
    </div>
    """, unsafe_allow_html=True)
