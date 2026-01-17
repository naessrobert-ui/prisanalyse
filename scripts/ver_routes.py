from __future__ import annotations

from datetime import date as _date
from flask import Blueprint, request, render_template, Response

from snow_map import build_snow_map_html
from precip_map import build_precip_map_html
from sunshine_map import build_sunshine_map_html
from temp_map import build_min_temp_map_html

# ✅ Blueprint heter "ver" og URL-prefix er /ver
ver = Blueprint("ver", __name__, url_prefix="/ver")


# =========================
# HUB / MENY
# =========================
@ver.route("/")
def ver_hub() -> str:
    return """
<!doctype html>
<html lang="no">
  <head>
    <meta charset="utf-8" />
    <title>Vær – Væranalyse</title>
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <style>
      body { margin:0; font-family: system-ui, -apple-system, "Segoe UI", sans-serif; background:#f5f7fb; }
      .page { max-width: 1100px; margin: 32px auto; padding: 0 16px; }
      h1 { margin: 0 0 14px; }
      .grid { display:grid; grid-template-columns: 1fr 1fr; gap: 16px; }
      .card {
        background: white; border-radius: 18px; padding: 18px 20px;
        box-shadow: 0 18px 45px rgba(15,23,42,.08);
      }
      .card h2 { margin:0 0 6px; }
      .muted { color:#475569; margin: 0 0 12px; }
      .btn {
        display:inline-block; padding: 8px 14px; border-radius: 999px;
        background:#2563eb; color:#fff; text-decoration:none; font-weight:700;
      }
      .btn-green { background:#16a34a; }
      .btn-amber { background:#f59e0b; }
      .btn-red { background:#ef4444; }
      @media (max-width: 900px) { .grid { grid-template-columns: 1fr; } }
    </style>
  </head>
  <body>
    <div class="page">
      <h1>Vær</h1>
      <div class="grid">
        <div class="card">
          <h2>Snømengde</h2>
          <p class="muted">Snødybde fra Frost. Zoom/pan og hent for utsnitt.</p>
          <a class="btn" href="/ver/sno">Åpne</a>
        </div>

        <div class="card">
          <h2>Nedbør</h2>
          <p class="muted">Siste 24 timer (rullerende) + dag / MTD / YTD.</p>
          <a class="btn btn-green" href="/ver/nedbor">Åpne</a>
        </div>

        <div class="card">
          <h2>Solskinn</h2>
          <p class="muted">Siste 24 timer (rullerende) + dag / MTD / YTD.</p>
          <a class="btn btn-amber" href="/ver/solskinn">Åpne</a>
        </div>

        <div class="card">
          <h2>Min temperatur siste døgn</h2>
          <p class="muted">Velg fylke og se nyeste døgn-min (P1D) per stasjon.</p>
          <a class="btn btn-red" href="/ver/min-temp">Åpne</a>
        </div>
      </div>
    </div>
  </body>
</html>
"""


# =========================
# MIN TEMP (ny)
# =========================
@ver.get("/min-temp")
def min_temp_index():
    return render_template("ver/min_temp_index.html")


@ver.get("/min-temp-kart")
def min_temp_map():
    county = request.args.get("county") or None
    temp = request.args.get("temp", "min")
    period = request.args.get("period", "last")
    date_str = request.args.get("date")
    month_str = request.args.get("month")
    year_str = request.args.get("year")

    html = build_min_temp_map_html(
        county=county,
        temp=temp,  # "min" | "max" | "mean"
        period=period,  # "last" | "day" | "month" | "year"
        date_str=date_str,
        month_str=month_str,
        year_str=year_str,
        timeout=20,
        batch_size=80,
        limit=1000,
        qualities="0,1,2,3,4",
    )
    return Response(html, mimetype="text/html; charset=utf-8")


# =========================
# SNØ
# =========================
@ver.route("/sno")
def sno_index() -> str:
    today_str = _date.today().isoformat()

    # Default: sør-norge bbox
    default_bbox = "57.0,4.0,62.5,12.5"
    default_z = "5"
    default_clat = "60.5"
    default_clon = "8.5"

    return f"""
<!doctype html>
<html lang="no">
  <head>
    <meta charset="utf-8" />
    <title>Snømengde i Norge – Væranalyse</title>
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <style>
      :root {{
        --bg: #f5f7fb;
        --card: #ffffff;
        --ink: #0f172a;
        --muted: #475569;
        --border: #e2e8f0;
        --shadow: 0 18px 45px rgba(15,23,42,.10);
        --blue: #2563eb;
        --blue2: #1d4ed8;
        --green: #16a34a;
        --pill: #eef2ff;
      }}
      body {{
        margin:0;
        font-family: system-ui, -apple-system, "Segoe UI", sans-serif;
        background: var(--bg);
        color: var(--ink);
      }}
      .page {{
        max-width: 1240px;
        margin: 28px auto;
        padding: 0 16px 28px;
      }}
      .card {{
        background: var(--card);
        border-radius: 18px;
        padding: 16px 18px;
        box-shadow: var(--shadow);
        border: 1px solid rgba(226,232,240,.8);
        margin-bottom: 14px;
      }}
      .hdr {{
        display:flex; gap: 12px; align-items:flex-start; justify-content:space-between;
        flex-wrap: wrap;
      }}
      h1 {{
        margin:0;
        font-size: 22px;
        letter-spacing: -0.01em;
      }}
      .sub {{
        margin: 6px 0 0;
        color: var(--muted);
        font-size: 13px;
        line-height: 1.35;
      }}
      .controls {{
        display:flex;
        gap: 10px;
        align-items:center;
        flex-wrap:wrap;
        margin-top: 12px;
      }}
      .field {{
        display:flex;
        flex-direction:column;
        gap: 6px;
        min-width: 180px;
      }}
      .field label {{
        font-size: 12px;
        color: var(--muted);
        font-weight: 700;
      }}
      select, input[type="date"] {{
        padding: 9px 10px;
        border-radius: 12px;
        border: 1px solid var(--border);
        background: white;
        outline: none;
        font-size: 14px;
      }}
      .seg {{
        display:flex;
        gap: 6px;
        padding: 6px;
        border: 1px solid var(--border);
        border-radius: 14px;
        background: #fff;
      }}
      .seg button {{
        border: none;
        border-radius: 12px;
        padding: 8px 12px;
        font-weight: 800;
        cursor: pointer;
        background: transparent;
        color: var(--muted);
      }}
      .seg button.active {{
        background: var(--pill);
        color: var(--blue2);
      }}
      .actions {{
        display:flex;
        gap: 10px;
        align-items:center;
        margin-left:auto;
      }}
      .btn {{
        padding: 10px 14px;
        border-radius: 999px;
        border: none;
        background: var(--blue);
        color: white;
        font-weight: 900;
        cursor: pointer;
        box-shadow: 0 12px 24px rgba(37,99,235,.18);
      }}
      .btn:hover {{ background: var(--blue2); }}
      .ghost {{
        padding: 10px 14px;
        border-radius: 999px;
        border: 1px solid var(--border);
        background: white;
        color: var(--ink);
        font-weight: 900;
        cursor:pointer;
      }}
      .toggle {{
        display:flex;
        align-items:center;
        gap: 10px;
        padding: 10px 12px;
        border-radius: 14px;
        border: 1px solid var(--border);
        background: #fff;
      }}
      .toggle .tlabel {{
        font-weight: 900;
      }}
      .switch {{
        position: relative;
        width: 44px;
        height: 26px;
        background: #e2e8f0;
        border-radius: 999px;
        cursor: pointer;
        flex: 0 0 auto;
      }}
      .switch::after {{
        content:"";
        position:absolute;
        top: 3px;
        left: 3px;
        width: 20px;
        height: 20px;
        background: white;
        border-radius: 999px;
        box-shadow: 0 8px 16px rgba(15,23,42,.15);
        transition: transform .18s ease;
      }}
      .switch.on {{
        background: rgba(22,163,74,.25);
      }}
      .switch.on::after {{
        transform: translateX(18px);
        background: #16a34a;
      }}
      .panel {{
        margin-top: 10px;
        padding: 12px;
        border-radius: 16px;
        border: 1px solid var(--border);
        background: linear-gradient(180deg, #ffffff 0%, #fbfdff 100%);
        display:none;
      }}
      .panel.show {{ display:block; }}
      .panel .row {{
        display:flex;
        gap: 10px;
        align-items:flex-end;
        flex-wrap: wrap;
      }}
      .hint {{
        font-size: 12px;
        color: var(--muted);
        margin: 8px 0 0;
      }}
      #map-frame {{
        width:100%;
        height: 80vh;
        border: none;
        border-radius: 18px;
        background: #e5e7eb;
        box-shadow: var(--shadow);
      }}
      @media (max-width: 900px) {{
        .actions {{ width: 100%; justify-content:flex-start; margin-left:0; }}
        .field {{ min-width: 160px; }}
      }}
    </style>
  </head>
  <body>
    <div class="page">
      <div class="card">
        <div class="hdr">
          <div>
            <h1>Snømengde</h1>
            <p class="sub">
              Standard er <b>siste oppdaterte snødybde</b> for <b>zoom/pan-utsnittet</b>.
              Slå på <b>Endring</b> for “nå minus baseline”.
            </p>
          </div>

          <div class="actions">
            <div class="seg" aria-label="Modus">
              <button type="button" id="tab-latest" class="active">Latest</button>
              <button type="button" id="tab-day">Dato</button>
            </div>

            <button type="button" id="btn-reset" class="ghost" title="Tilbakestill utsnitt til region-default">
              Reset utsnitt
            </button>

            <button type="button" id="btn-go" class="btn">Vis</button>
          </div>
        </div>

        <div class="controls">
          <div class="field">
            <label for="region-select">Region</label>
            <select id="region-select" name="region">
              <option value="south" selected>Sør</option>
              <option value="mid">Midt</option>
              <option value="north">Nord</option>
              <option value="all">Hele landet (tregere)</option>
            </select>
          </div>

          <div class="field">
            <label for="date-input">Dato (kun Dato-modus)</label>
            <input type="date" id="date-input" name="date" value="{today_str}" max="{today_str}">
          </div>

          <div class="toggle" title="Slå på for å vise endring (nå - baseline)">
            <div class="switch" id="change-switch" role="switch" aria-checked="false" tabindex="0"></div>
            <div>
              <div class="tlabel">Endring</div>
              <div style="font-size:12px; color: var(--muted); font-weight:700;">Nå minus baseline</div>
            </div>
          </div>
        </div>

        <div class="panel" id="change-panel">
          <div class="row">
            <div class="field">
              <label for="since-select">Baseline</label>
              <select id="since-select">
                <option value="døgn" selected>Siste døgn</option>
                <option value="3døgn">Siste 3 døgn</option>
                <option value="year">I år</option>
                <option value="date">Valgt dato</option>
              </select>
            </div>

            <div class="field" id="since-date-field" style="display:none;">
              <label for="since-date">Siden dato</label>
              <input type="date" id="since-date" value="{today_str}" max="{today_str}">
            </div>
          </div>

          <p class="hint">
            Tips: Når <b>Endring</b> er på, beregnes endringen kun for stasjoner i nåværende utsnitt (bbox).
          </p>
        </div>
      </div>

      <iframe id="map-frame"
        src="/ver/snomengde-kart?mode=latest&region=south&bbox={default_bbox}&z={default_z}&clat={default_clat}&clon={default_clon}"
        loading="lazy"></iframe>
    </div>

    <script>
      const STORE_KEY = "snow_view_v2";

      const frame = document.getElementById("map-frame");

      const tabLatest = document.getElementById("tab-latest");
      const tabDay = document.getElementById("tab-day");

      const btnGo = document.getElementById("btn-go");
      const btnReset = document.getElementById("btn-reset");

      const regionSelect = document.getElementById("region-select");
      const dateInput = document.getElementById("date-input");

      const changeSwitch = document.getElementById("change-switch");
      const changePanel = document.getElementById("change-panel");
      const sinceSelect = document.getElementById("since-select");
      const sinceDateField = document.getElementById("since-date-field");
      const sinceDate = document.getElementById("since-date");

      let mode = "latest"; // "latest" | "day"
      let changeOn = false;

      const REGION_DEFAULTS = {{
        south: {{ bbox: "57.0,4.0,62.5,12.5", z: "5", clat: "60.5", clon: "8.5" }},
        mid:   {{ bbox: "62.0,4.0,66.7,16.5", z: "5", clat: "64.5", clon: "10.5" }},
        north: {{ bbox: "66.3,10.0,71.5,31.5", z: "4", clat: "68.8", clon: "19.0" }},
        all:   {{ bbox: "57.0,4.0,71.5,31.5", z: "4", clat: "64.0", clon: "14.0" }}
      }};

      function readSavedView() {{
        try {{
          const raw = sessionStorage.getItem(STORE_KEY);
          if (!raw) return null;
          const obj = JSON.parse(raw);
          if (!obj || !obj.bbox) return null;
          return obj;
        }} catch (e) {{
          return null;
        }}
      }}

      function saveViewFromFrameUrl() {{
        try {{
          const u = new URL(frame.contentWindow.location.href);
          const bbox = u.searchParams.get("bbox");
          if (!bbox) return;
          const z = u.searchParams.get("z") || "";
          const clat = u.searchParams.get("clat") || "";
          const clon = u.searchParams.get("clon") || "";
          sessionStorage.setItem(STORE_KEY, JSON.stringify({{ bbox, z, clat, clon }}));
        }} catch (e) {{}}
      }}

      frame.addEventListener("load", saveViewFromFrameUrl);

      function setMode(newMode) {{
        mode = newMode;
        tabLatest.classList.toggle("active", mode === "latest");
        tabDay.classList.toggle("active", mode === "day");

        const isDay = (mode === "day");
        dateInput.disabled = !isDay;
        dateInput.style.opacity = isDay ? "1" : "0.55";
      }}

      function setChange(on) {{
        changeOn = on;
        changeSwitch.classList.toggle("on", changeOn);
        changeSwitch.setAttribute("aria-checked", changeOn ? "true" : "false");
        changePanel.classList.toggle("show", changeOn);
      }}

      function updateSinceDateVisibility() {{
        const v = sinceSelect.value;
        sinceDateField.style.display = (v === "date") ? "block" : "none";
      }}

      sinceSelect.addEventListener("change", updateSinceDateVisibility);

      function buildFrameUrl(resetView=false) {{
        const region = regionSelect.value || "south";
        const d = dateInput.value || "{today_str}";

        const qs = new URLSearchParams();
        qs.set("region", region);

        // Når endring er på: vi kan likevel sette mode=latest (ryddigere)
        if (changeOn) {{
          qs.set("mode", "latest");
          qs.set("change", "1");

          if (sinceSelect.value === "date") {{
            qs.set("since", (sinceDate.value || "{today_str}"));
          }} else {{
            qs.set("since", sinceSelect.value);
          }}
        }} else {{
          qs.set("mode", mode);
          if (mode === "day") {{
            qs.set("date", d);
          }}
        }}

        const def = REGION_DEFAULTS[region] || REGION_DEFAULTS.south;

        if (resetView) {{
          qs.set("bbox", def.bbox);
          qs.set("z", def.z);
          qs.set("clat", def.clat);
          qs.set("clon", def.clon);
        }} else {{
          const saved = readSavedView();
          if (saved) {{
            if (saved.bbox) qs.set("bbox", saved.bbox);
            if (saved.z) qs.set("z", saved.z);
            if (saved.clat) qs.set("clat", saved.clat);
            if (saved.clon) qs.set("clon", saved.clon);
          }} else {{
            qs.set("bbox", def.bbox);
            qs.set("z", def.z);
            qs.set("clat", def.clat);
            qs.set("clon", def.clon);
          }}
        }}

        return "/ver/snomengde-kart?" + qs.toString();
      }}

      function go(resetView=false) {{
        frame.src = buildFrameUrl(resetView);
      }}

      tabLatest.addEventListener("click", () => {{ setMode("latest"); go(false); }});
      tabDay.addEventListener("click", () => {{ setMode("day"); go(false); }});

      btnGo.addEventListener("click", () => go(false));
      btnReset.addEventListener("click", () => {{
        try {{ sessionStorage.removeItem(STORE_KEY); }} catch (e) {{}}
        go(true);
      }});

      regionSelect.addEventListener("change", () => {{
        // Når region endres: reset view til region-default
        try {{ sessionStorage.removeItem(STORE_KEY); }} catch (e) {{}}
        go(true);
      }});

      changeSwitch.addEventListener("click", () => {{
        setChange(!changeOn);
        go(false);
      }});
      changeSwitch.addEventListener("keydown", (e) => {{
        if (e.key === "Enter" || e.key === " ") {{
          e.preventDefault();
          setChange(!changeOn);
          go(false);
        }}
      }});

      // init
      setMode("latest");
      setChange(false);
      updateSinceDateVisibility();
    </script>
  </body>
</html>
"""


@ver.route("/snomengde-kart")
def snomengde_kart():
    date_str = request.args.get("date")
    mode = request.args.get("mode", "latest")
    region = request.args.get("region")  # south|mid|north|all
    bbox = request.args.get("bbox")

    z = request.args.get("z")
    clat = request.args.get("clat")
    clon = request.args.get("clon")

    change = (request.args.get("change", "") or "").strip().lower() in {"1", "true", "yes", "on"}
    since = request.args.get("since") or ""

    if mode not in {"latest", "day"}:
        mode = "latest"

    html = build_snow_map_html(
        date_str=date_str,
        mode=mode,
        bbox=bbox,
        region=region,
        z=z,
        clat=clat,
        clon=clon,
        show_heatmap=True,
        # ✅ nytt:
        change=change,
        since=since,
        # tunables:
        timeout=20,
        qualities="0,1,2,3,4",
        window_days=2,
    )
    return Response(html, mimetype="text/html; charset=utf-8")


# =========================
# NEDBØR
# =========================
@ver.route("/nedbor")
def nedbor_index() -> str:
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
        box-shadow: 0 18px 45px rgba(15, 23, 42, 0.10);
        background: #e5e7eb;
      }}
    </style>
  </head>

  <body>
    <div class="page">
      <div class="card">
        <h1>Nedbør i Norge</h1>
        <p style="margin:0; color:#475569;">
          Standard er rullerende siste 24 timer. Zoom/pan til ønsket område og trykk hent i kartet.
          Når du bytter periode beholdes utsnittet.
        </p>

        <form id="controls-form" class="controls">
          <label for="date-input">Dato:</label>
          <input type="date" id="date-input" name="date" value="{today_str}" max="{today_str}">

          <label for="mode-select">Periode:</label>
          <select id="mode-select" name="mode">
            <option value="last24h" selected>Siste 24 timer</option>
            <option value="day">Kalenderdøgn</option>
            <option value="mtd">Hittil i måneden</option>
            <option value="ytd">Hittil i året</option>
          </select>

          <button type="submit">Vis</button>
        </form>
      </div>

      <iframe id="map-frame" src="/ver/nedbor-kart?mode=last24h" loading="lazy"></iframe>
    </div>

    <script>
      const STORE_KEY = "precip_view_v1";
      const form = document.getElementById("controls-form");
      const dateInput = document.getElementById("date-input");
      const modeSelect = document.getElementById("mode-select");
      const frame = document.getElementById("map-frame");

      function readSavedView() {{
        try {{
          const raw = sessionStorage.getItem(STORE_KEY);
          if (!raw) return null;
          const obj = JSON.parse(raw);
          if (!obj || !obj.bbox) return null;
          return obj;
        }} catch (e) {{
          return null;
        }}
      }}

      function buildFrameUrl() {{
        const mode = modeSelect.value || "last24h";
        const d = dateInput.value || "{today_str}";

        const qs = new URLSearchParams();
        qs.set("mode", mode);
        qs.set("date", d);

        const saved = readSavedView();
        if (saved) {{
          if (saved.bbox) qs.set("bbox", saved.bbox);
          if (saved.z) qs.set("z", saved.z);
          if (saved.clat) qs.set("clat", saved.clat);
          if (saved.clon) qs.set("clon", saved.clon);
        }}

        return "/ver/nedbor-kart?" + qs.toString();
      }}

      function saveViewFromFrameUrl() {{
        try {{
          const u = new URL(frame.contentWindow.location.href);
          const bbox = u.searchParams.get("bbox");
          if (!bbox) return;
          const z = u.searchParams.get("z") || "";
          const clat = u.searchParams.get("clat") || "";
          const clon = u.searchParams.get("clon") || "";
          sessionStorage.setItem(STORE_KEY, JSON.stringify({{ bbox, z, clat, clon }}));
        }} catch (e) {{}}
      }}

      frame.addEventListener("load", saveViewFromFrameUrl);

      form.addEventListener("submit", function(e) {{
        e.preventDefault();
        frame.src = buildFrameUrl();
      }});

      modeSelect.addEventListener("change", function() {{
        frame.src = buildFrameUrl();
      }});
    </script>
  </body>
</html>
"""


@ver.route("/nedbor-kart")
def nedbor_kart() -> str:
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


# =========================
# SOLSKINN
# =========================
@ver.route("/solskinn")
def solskinn_index() -> str:
    today_str = _date.today().isoformat()
    return f"""
<!doctype html>
<html lang="no">
  <head>
    <meta charset="utf-8" />
    <title>Solskinn i Norge – Væranalyse</title>
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
        box-shadow: 0 18px 45px rgba(15, 23, 42, 0.10);
        background: #e5e7eb;
      }}
    </style>
  </head>

  <body>
    <div class="page">
      <div class="card">
        <h1>Solskinn i Norge</h1>
        <p style="margin:0; color:#475569;">
          Standard er rullerende siste 24 timer. Zoom/pan til ønsket område og trykk hent i kartet.
          Når du bytter periode beholdes utsnittet.
        </p>

        <form id="controls-form" class="controls">
          <label for="date-input">Dato:</label>
          <input type="date" id="date-input" name="date" value="{today_str}" max="{today_str}">

          <label for="mode-select">Periode:</label>
          <select id="mode-select" name="mode">
            <option value="last24h" selected>Siste 24 timer</option>
            <option value="day">Kalenderdøgn</option>
            <option value="mtd">Hittil i måneden</option>
            <option value="ytd">Hittil i året</option>
          </select>

          <button type="submit">Vis</button>
        </form>
      </div>

      <iframe id="map-frame" src="/ver/solskinn-kart?mode=last24h" loading="lazy"></iframe>
    </div>

    <script>
      const STORE_KEY = "precip_view_v1";
      const form = document.getElementById("controls-form");
      const dateInput = document.getElementById("date-input");
      const modeSelect = document.getElementById("mode-select");
      const frame = document.getElementById("map-frame");

      function readSavedView() {{
        try {{
          const raw = sessionStorage.getItem(STORE_KEY);
          if (!raw) return null;
          const obj = JSON.parse(raw);
          if (!obj || !obj.bbox) return null;
          return obj;
        }} catch (e) {{
          return null;
        }}
      }}

      function buildFrameUrl() {{
        const mode = modeSelect.value || "last24h";
        const d = dateInput.value || "{today_str}";

        const qs = new URLSearchParams();
        qs.set("mode", mode);
        qs.set("date", d);

        const saved = readSavedView();
        if (saved) {{
          if (saved.bbox) qs.set("bbox", saved.bbox);
          if (saved.z) qs.set("z", saved.z);
          if (saved.clat) qs.set("clat", saved.clat);
          if (saved.clon) qs.set("clon", saved.clon);
        }}

        return "/ver/solskinn-kart?" + qs.toString();
      }}

      function saveViewFromFrameUrl() {{
        try {{
          const u = new URL(frame.contentWindow.location.href);
          const bbox = u.searchParams.get("bbox");
          if (!bbox) return;
          const z = u.searchParams.get("z") || "";
          const clat = u.searchParams.get("clat") || "";
          const clon = u.searchParams.get("clon") || "";
          sessionStorage.setItem(STORE_KEY, JSON.stringify({{ bbox, z, clat, clon }}));
        }} catch (e) {{}}
      }}

      frame.addEventListener("load", saveViewFromFrameUrl);

      form.addEventListener("submit", function(e) {{
        e.preventDefault();
        frame.src = buildFrameUrl();
      }});

      modeSelect.addEventListener("change", function() {{
        frame.src = buildFrameUrl();
      }});
    </script>
  </body>
</html>
"""


@ver.route("/solskinn-kart")
def solskinn_kart() -> str:
    date_str = request.args.get("date")
    mode = request.args.get("mode", "last24h")
    bbox = request.args.get("bbox")

    z = request.args.get("z")
    clat = request.args.get("clat")
    clon = request.args.get("clon")

    if mode not in {"last24h", "day", "mtd", "ytd"}:
        mode = "last24h"

    return build_sunshine_map_html(
        date_str=date_str,
        mode=mode,  # type: ignore[arg-type]
        bbox=bbox,
        z=z,
        clat=clat,
        clon=clon,
        show_heatmap=True,
    )
