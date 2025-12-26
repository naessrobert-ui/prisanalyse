from datetime import date as _date

from flask import Blueprint, request, render_template

from snow_map import build_snow_map_html
from precip_map import build_precip_map_html

ver_bp = Blueprint("ver", __name__)


# ------------------
# HUB / MENY (INLINE)
# ------------------
@ver_bp.route("/")
def ver_hub():
    return """
<!doctype html>
<html lang="no">
  <head>
    <meta charset="utf-8" />
    <title>Vær – Væranalyse</title>
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <style>
      body { margin:0; font-family: system-ui, -apple-system, "Segoe UI", sans-serif; background:#f5f7fb; }
      .page { max-width: 1000px; margin: 32px auto; padding: 0 16px; }
      .grid { display:grid; grid-template-columns: repeat(auto-fit, minmax(260px, 1fr)); gap: 16px; }
      .card { background:white; border-radius:16px; padding:18px 22px; box-shadow:0 18px 45px rgba(15,23,42,.08); }
      a.btn { display:inline-block; margin-top:10px; padding:8px 14px; border-radius:999px; background:#2563eb; color:white; text-decoration:none; }
      p { margin: 8px 0 0; color:#334155; }
      h1 { margin: 0 0 12px; }
      h2 { margin: 0 0 6px; }
    </style>
  </head>
  <body>
    <div class="page">
      <h1>Vær</h1>
      <div class="grid">
        <div class="card">
          <h2>Snømengde</h2>
          <p>Snødybde fra Frost. Velg dato.</p>
          <a class="btn" href="/ver/sno">Åpne</a>
        </div>
        <div class="card">
          <h2>Nedbør</h2>
          <p>Zoom/pan og hent data for synlig område. Toppliste + legend.</p>
          <a class="btn" href="/ver/nedbor">Åpne</a>
        </div>
      </div>
    </div>
  </body>
</html>
"""


# ------------------
# SNØ
# ------------------
@ver_bp.route("/sno")
def sno_index():
    today_str = _date.today().isoformat()
    return render_template("ver/snow_index.html", default_date=today_str)


@ver_bp.route("/snomengde-kart")
def snomengde_kart():
    date_str = request.args.get("date")  # kan være None
    return build_snow_map_html(date_str=date_str, show_heatmap=True)


# ------------------
# NEDBØR (INLINE INDEX)
# ------------------
@ver_bp.route("/nedbor")
def nedbor_index():
    today_str = _date.today().isoformat()
    return f"""
<!doctype html>
<html lang="no">
  <head>
    <meta charset="utf-8" />
    <title>Nedbør i Norge – Væranalyse</title>
    <meta name="viewport" content="width=device-width, initial-scale=1" />

    <style>
      body {{
        margin: 0;
        padding: 0;
        font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
        background: #f5f7fb;
      }}
      .page {{
        max-width: 1200px;
        margin: 32px auto;
        padding: 0 16px 32px;
      }}
      .card {{
        background: white;
        border-radius: 16px;
        padding: 18px 22px;
        box-shadow: 0 18px 45px rgba(15, 23, 42, 0.08);
        margin-bottom: 16px;
      }}
      .card h1 {{ margin: 0 0 8px; }}
      .controls {{
        display: flex;
        flex-wrap: wrap;
        gap: 10px;
        margin-top: 10px;
        align-items: center;
      }}
      .controls input,
      .controls select {{
        padding: 6px 10px;
        border-radius: 10px;
        border: 1px solid #d1d5db;
      }}
      .controls button {{
        padding: 7px 14px;
        border-radius: 999px;
        border: none;
        background: #2563eb;
        color: white;
        cursor: pointer;
      }}
      #map-frame {{
        width: 100%;
        height: 80vh;
        border: none;
        border-radius: 16px;
        box-shadow: 0 18px 45px rgba(15, 23, 42, 0.1);
        background: #e5e7eb;
      }}
    </style>
  </head>

  <body>
    <div class="page">

      <div class="card">
        <h1>Nedbør i Norge</h1>
        <p>
          Zoom/pan til ønsket område og trykk <b>Hent data for synlig område</b> i kartet.
          Når data er hentet, kan du bytte periode uten å miste zoom (bbox + zoom/center beholdes).
        </p>

        <form id="controls" class="controls">
          <label for="date-input">Dato:</label>
          <input
            type="date"
            id="date-input"
            name="date"
            value="{today_str}"
            max="{today_str}"
          />

          <label for="mode">Periode:</label>
          <select id="mode" name="mode">
            <option value="last24h" selected>Siste 24 timer (rullerende)</option>
            <option value="day">Kalenderdøgn (valgt dato)</option>
            <option value="mtd">Hittil i måneden</option>
            <option value="ytd">Hittil i året</option>
          </select>

          <button type="submit">Vis</button>
        </form>
      </div>

      <iframe
        id="map-frame"
        src="/ver/nedbor-kart?mode=last24h"
        loading="lazy"
      ></iframe>

    </div>

    <script>
      const form = document.getElementById("controls");
      const dateInput = document.getElementById("date-input");
      const modeSelect = document.getElementById("mode");
      const frame = document.getElementById("map-frame");

      function updateFrame() {{
        const mode = modeSelect.value || "last24h";
        const baseUrl = "/ver/nedbor-kart";

        const qs = new URLSearchParams();
        qs.set("mode", mode);

        if (mode !== "last24h") {{
          const d = dateInput.value;
          if (d) qs.set("date", d);
        }}

        // ✅ behold bbox + zoom/center hvis kartet allerede er lastet for et område
        try {{
          const current = new URL(frame.src);
          const bbox = current.searchParams.get("bbox");
          const z = current.searchParams.get("z");
          const clat = current.searchParams.get("clat");
          const clon = current.searchParams.get("clon");

          if (bbox) qs.set("bbox", bbox);
          if (z) qs.set("z", z);
          if (clat) qs.set("clat", clat);
          if (clon) qs.set("clon", clon);
        }} catch (e) {{}}

        frame.src = `${{baseUrl}}?${{qs.toString()}}`;
      }}

      form.addEventListener("submit", function (e) {{
        e.preventDefault();
        updateFrame();
      }});

      modeSelect.addEventListener("change", updateFrame);
    </script>
  </body>
</html>
"""


# ------------------
# NEDBØR (KART)
# ------------------
@ver_bp.route("/nedbor-kart")
def nedbor_kart():
    date_str = request.args.get("date")
    mode = request.args.get("mode", "last24h")
    bbox = request.args.get("bbox")

    z = request.args.get("z")
    clat = request.args.get("clat")
    clon = request.args.get("clon")

    if mode not in {"last24h", "day", "mtd", "ytd"}:
        mode = "last24h"

    return build_precip_map_html(
        date_str=date_str,
        mode=mode,  # type: ignore[arg-type]
        bbox=bbox,
        z=z,
        clat=clat,
        clon=clon,
        show_heatmap=True,
    )
