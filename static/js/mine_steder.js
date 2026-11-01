/* "Mine steder": stedene brukeren sist har sett på, lagret i nettleseren.
 *
 * Ingenting lagres på serveren. Listen ligger i localStorage og følger derfor
 * nettleseren, ikke personen. Stjernemerkede steder ligger øverst og skyves
 * aldri ut av nye søk.
 *
 *   MineSteder.add({name, lat, lon, href})   - kalles når et sted vises
 *   MineSteder.render(element, iconFn)       - tegner listen (forsiden)
 */
(function () {
  "use strict";

  var KEY = "vaer_mine_steder_v1";
  var MAX = 8;
  var DEFAULTS = [
    { name: "Bergen", lat: 60.393, lon: 5.3242, href: "/ver/varsel/bergen" },
    { name: "Kvamskogen", lat: 60.3783, lon: 5.9796, href: "/ver/varsel/kvamskogen" },
  ];

  function read() {
    try {
      var list = JSON.parse(localStorage.getItem(KEY) || "[]");
      return Array.isArray(list)
        ? list.filter(function (p) { return p && isFinite(p.lat) && isFinite(p.lon) && p.name; })
        : [];
    } catch (_) {
      return [];
    }
  }

  function write(list) {
    try { localStorage.setItem(KEY, JSON.stringify(list)); } catch (_) { /* privat modus o.l. */ }
  }

  function same(a, b) {
    return Math.abs(a.lat - b.lat) < 0.01 && Math.abs(a.lon - b.lon) < 0.01;
  }

  function order(list) {
    var stars = list.filter(function (p) { return p.star; });
    var rest = list.filter(function (p) { return !p.star; });
    return stars.concat(rest.slice(0, Math.max(0, MAX - stars.length)));
  }

  function add(place) {
    var p = {
      name: String(place.name || "Valgt sted").trim(),
      lat: Number(place.lat),
      lon: Number(place.lon),
      href: place.href || null,
    };
    if (!isFinite(p.lat) || !isFinite(p.lon)) return;
    var list = read();
    var old = list.filter(function (q) { return same(q, p); })[0];
    if (old) { p.star = !!old.star; if (!place.href && old.href) p.href = old.href; }
    list = list.filter(function (q) { return !same(q, p); });
    list.unshift(p);
    write(order(list));
  }

  function punktUrl(name, lat, lon) {
    return "/ver/varsel/p" + Number(lat).toFixed(3) + "_" + Number(lon).toFixed(3) +
      "?navn=" + encodeURIComponent(name);
  }

  function link(p, norge) {
    if (p.href) return p.href;
    if (norge) return punktUrl(p.name, p.lat, p.lon);
    return "/ver/aktivt-varsel?lat=" + p.lat.toFixed(4) + "&lon=" + p.lon.toFixed(4) +
      "&sted=" + encodeURIComponent(p.name);
  }

  function esc(s) {
    return String(s).replace(/[&<>"']/g, function (c) {
      return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c];
    });
  }

  function dayLabel(iso, i) {
    if (i === 0) return "I dag";
    var d = new Date(iso + "T12:00:00");
    var n = d.toLocaleDateString("nb-NO", { weekday: "long" });
    return n.charAt(0).toUpperCase() + n.slice(1);
  }

  function fetchDays(p) {
    var url = "/ver/api/dagsvarsel?lat=" + p.lat.toFixed(2) + "&lon=" + p.lon.toFixed(2);
    return fetch(url).then(function (r) { return r.ok ? r.json() : { days: [] }; })
      .then(function (d) { var days = d.days || []; days.norge = !!d.norge; return days; })
      .catch(function () { return []; });
  }

  function render(el, iconFn) {
    if (!el) return;
    var stored = read();
    var places = stored.length ? stored : DEFAULTS;
    var editable = stored.length > 0;
    el.innerHTML = '<div class="ms-load">Henter …</div>';

    Promise.all(places.map(fetchDays)).then(function (all) {
      var head = (all.filter(function (d) { return d.length; })[0] || []).slice(0, 4);
      var html = '<div class="ms-row ms-head"><span></span>' +
        head.map(function (d, i) { return '<span class="ms-day">' + dayLabel(d.date, i) + "</span>"; }).join("") +
        "<span></span></div>";
      places.forEach(function (p, idx) {
        var days = all[idx];
        var cells = head.map(function (h) {
          var d = days.filter(function (x) { return x.date === h.date; })[0];
          if (!d) return '<span class="ms-day"></span>';
          var cls = d.tmax > 0 ? "ms-warm" : "ms-cold";
          return '<span class="ms-day" title="' + d.tmin + "° / " + d.tmax + "°, " +
            String(d.precip).replace(".", ",") + ' mm">' + (iconFn ? iconFn(d.symbol, 32) : "") +
            '<b class="' + cls + '">' + d.tmax + "°</b></span>";
        }).join("");
        var star = editable
          ? '<button type="button" class="ms-star' + (p.star ? " on" : "") + '" data-ms-star="' + idx +
            '" aria-label="Fest sted">' + (p.star ? "★" : "☆") + "</button>"
          : '<span class="ms-star">☆</span>';
        var del = editable
          ? '<button type="button" class="ms-del" data-ms-del="' + idx + '" aria-label="Fjern sted">✕</button>'
          : "<span></span>";
        html += '<div class="ms-row">' + '<span class="ms-name">' + star +
          '<a href="' + esc(link(p, days.norge)) + '">' + esc(p.name) + "</a></span>" + cells + del + "</div>";
      });
      if (!editable) {
        html += '<div class="ms-hint">Steder du søker opp, havner her automatisk. Bare i denne nettleseren.</div>';
      }
      el.innerHTML = html;
    });

    if (!el.dataset.msBound) {
      el.dataset.msBound = "1";
      el.addEventListener("click", function (e) {
        var s = e.target.closest("[data-ms-star]");
        var d = e.target.closest("[data-ms-del]");
        if (!s && !d) return;
        var list = read();
        if (s) { var i = +s.dataset.msStar; if (list[i]) list[i].star = !list[i].star; }
        if (d) { list.splice(+d.dataset.msDel, 1); }
        write(order(list));
        render(el, iconFn);
      });
    }
  }

  window.MineSteder = { add: add, render: render, list: read, punktUrl: punktUrl };
})();
