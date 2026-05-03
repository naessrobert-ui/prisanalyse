#!/usr/bin/env python3
"""
Endringer i sunshine_map.py for å støtte prognose-modi.

DIFF — legg til/endre følgende seksjoner i din eksisterende fil.
"""

# ===========================================================================
# 1. LEGG TIL i imports (øverst, etter eksisterende imports):
# ===========================================================================

# from sunshine_forecast import fetch_sunshine_forecast, ForecastMode

# ===========================================================================
# 2. OPPDATER Mode-type:
# ===========================================================================

# Gammel:
#   Mode = Literal["last24h", "day", "mtd", "ytd"]
#
# Ny:
#   Mode = Literal["last24h", "day", "mtd", "ytd", "next24h", "next48h", "next7d"]

FORECAST_MODES = {"next24h", "next48h", "next7d"}

# ===========================================================================
# 3. OPPDATER build_sunshine_map_html() — legg til forecast-gren FØR
#    eksisterende mode-if-blokken:
# ===========================================================================

PATCH_build_sunshine_map_html = '''
def build_sunshine_map_html(
    date_str: Optional[str] = None,
    *,
    mode: Mode = "day",
    bbox: Optional[str] = None,
    z: Optional[str] = None,
    clat: Optional[str] = None,
    clon: Optional[str] = None,
    timeout: int = DEFAULT_TIMEOUT,
    batch_size: int = 80,
    limit: int = 1000,
    qualities: str = "0,1,2,3,4",
    cluster: bool = False,
    show_heatmap: bool = True,
    heat_radius: int = 25,
    heat_blur: int = 18,
    heat_clip_hours: float = 12.0,
) -> str:
    _ = (z, clat, clon)

    # ------------------------------------------------------------------
    # PROGNOSE-MODI: next24h / next48h / next7d
    # Bruker YR Locationforecast i stedet for Frost observasjoner.
    # ------------------------------------------------------------------
    if mode in FORECAST_MODES:
        from sunshine_forecast import fetch_sunshine_forecast

        # Tittel og heatmap clip
        _titles = {
            "next24h": "Forventet solskinn – neste 24 timer",
            "next48h": "Forventet solskinn – neste 48 timer",
            "next7d":  "Forventet solskinn – neste 7 dager",
        }
        _clips = {"next24h": 12.0, "next48h": 20.0, "next7d": 60.0}
        title = _titles[mode]
        heat_clip = _clips.get(mode, heat_clip_hours)

        # Stasjonsliste
        if bbox:
            bbox_swne = _parse_bbox_swne(bbox)
            st = stations_in_bbox_swne_with(bbox_swne, require_sun=True)
        else:
            st = load_station_db()
            if not st.empty:
                st = st[st["has_sun"] == True].copy()

        if st.empty:
            return make_info_map(
                title="Ingen solskinn-stasjoner",
                message="Fant ingen stasjoner med has_sun=True i stasjonsdatabasen.",
                mode=str(mode),
                date_str="",
            )

        st = st.dropna(subset=["baseId", "lat", "lon"]).copy()

        # Hent prognose fra YR (parallelt)
        merged = fetch_sunshine_forecast(st, mode=mode, timeout=timeout)

        if merged.empty:
            return make_info_map(
                title="Ingen prognosedata",
                message="Klarte ikke hente YR-prognose for noen stasjon.",
                mode=str(mode),
                date_str="",
            )

        return make_map(
            merged,
            title=title,
            out_html=None,
            cluster=cluster,
            heatmap_show=show_heatmap,
            heat_radius=heat_radius,
            heat_blur=heat_blur,
            heat_clip_hours=heat_clip,
            top_n=10,
            mode=str(mode),
            date_str="",
        )

    # ------------------------------------------------------------------
    # EKSISTERENDE KODE (observasjoner via Frost) følger uendret herfra
    # ------------------------------------------------------------------
    day_str = date_str or _date.today().isoformat()
    ... # resten av funksjonen uendret
'''

# ===========================================================================
# 4. OPPDATER Flask-ruten (solskinn_routes.py eller tilsvarende):
# ===========================================================================

PATCH_flask_route = '''
# I Flask-ruten som håndterer /ver/solskinn-kart,
# legg til de nye modi i valideringen:

VALID_MODES = {"last24h", "day", "mtd", "ytd", "next24h", "next48h", "next7d"}

@ver_bp.route("/solskinn-kart")
def solskinn_kart():
    mode = request.args.get("mode", "last24h")
    if mode not in VALID_MODES:
        mode = "last24h"
    date_str = request.args.get("date", "")
    bbox = request.args.get("bbox", "")
    html = build_sunshine_map_html(
        date_str=date_str or None,
        mode=mode,
        bbox=bbox or None,
    )
    return html
'''

# ===========================================================================
# 5. OPPDATER UI — legg til knapper i mode-velgeren i frontend-templaten:
# ===========================================================================

PATCH_ui_buttons = '''
<!-- I Jinja2-templaten der brukeren velger mode, legg til: -->

<div class="mode-group">
  <span class="mode-label">Prognose</span>
  <a href="/ver/solskinn-kart?mode=next24h"
     class="btn {% if mode == 'next24h' %}btn-active{% endif %}">
    Neste 24t
  </a>
  <a href="/ver/solskinn-kart?mode=next48h"
     class="btn {% if mode == 'next48h' %}btn-active{% endif %}">
    Neste 48t
  </a>
  <a href="/ver/solskinn-kart?mode=next7d"
     class="btn {% if mode == 'next7d' %}btn-active{% endif %}">
    Neste 7 dager
  </a>
</div>
'''

print("Patch-fil lest. Se kommentarene over for endringene.")
