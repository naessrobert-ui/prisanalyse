"""
sunshine_map_changes.py
========================
Eksakte endringer i sunshine_map.py for å støtte next24h / next48h / next7d.
Tre steder å endre.
"""

# ===========================================================================
# ENDRING 1 — Legg til import (etter eksisterende imports, linje ~17)
# ===========================================================================

# Fra:
#   from ver_station_db import load_station_db, stations_in_bbox_swne_with
#
# Til:
#   from ver_station_db import load_station_db, stations_in_bbox_swne_with
#   from sunshine_forecast import (
#       fetch_sunshine_forecast,
#       FORECAST_MODES,
#       HEAT_CLIP_HOURS,
#       TITLES as FORECAST_TITLES,
#   )


# ===========================================================================
# ENDRING 2 — Utvid Mode-typen (linje ~49)
# ===========================================================================

# Fra:
#   Mode = Literal["last24h", "day", "mtd", "ytd"]
#
# Til:
#   Mode = Literal["last24h", "day", "mtd", "ytd", "next24h", "next48h", "next7d"]


# ===========================================================================
# ENDRING 3 — Legg til forecast-gren i build_sunshine_map_html()
#             Sett inn RETT FØR linjen:  day_str = date_str or _date.today().isoformat()
# ===========================================================================

PATCH_TEXT = '''
    # ------------------------------------------------------------------
    # PROGNOSE-MODI  (next24h / next48h / next7d)
    # Bruker YR Locationforecast i stedet for Frost — Frost har ikke
    # et forecast-endepunkt, kun historisk arkiv.
    # ------------------------------------------------------------------
    if mode in FORECAST_MODES:
        title = FORECAST_TITLES[mode]
        heat_clip = HEAT_CLIP_HOURS[mode]

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

        merged = fetch_sunshine_forecast(st, mode=mode, timeout=timeout)

        if merged.empty:
            return make_info_map(
                title="Ingen prognosedata",
                message="Klarte ikke hente YR-prognose for noen stasjon. Sjekk User-Agent i sunshine_forecast.py.",
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
    # EKSISTERENDE KODE (Frost observasjoner) fortsetter uendret herfra
    # ------------------------------------------------------------------
'''


# ===========================================================================
# ENDRING 4 (valgfri) — Flask-ruten: legg til de nye modi i validering
# ===========================================================================

FLASK_ROUTE_CHANGE = '''
# I ruten som håndterer /ver/solskinn-kart, oppdater VALID_MODES:

VALID_MODES = {"last24h", "day", "mtd", "ytd", "next24h", "next48h", "next7d"}
'''


# ===========================================================================
# ENDRING 5 (valgfri) — Jinja2-template: knapper for prognose-valg
# ===========================================================================

TEMPLATE_BUTTONS = '''
{# Legg til i mode-velgeren i din HTML-template: #}

<div class="mode-group" style="margin-top:8px;">
  <span style="font-size:12px; color:#64748b; font-weight:600;">PROGNOSE</span>
  <div style="display:flex; gap:6px; margin-top:4px; flex-wrap:wrap;">
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
</div>
'''
