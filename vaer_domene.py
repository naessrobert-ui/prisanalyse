# -*- coding: utf-8 -*-
"""Serverer værseksjonen (/ver) på eget domene, f.eks. blirdetfint.no.

WSGI-mellomvare som ligger foran Flask:

* blirdetfint.no/<sti>      -> internt /ver/<sti>
* blirdetfint.no/            -> været i Bergen (/ver/varsel/bergen)
* blirdetfint.no/ver/<sti>  -> 301 til blirdetfint.no/<sti> ved sidenavigering,
  ellers sendes den rett gjennom. Da virker alle hardkodede "/ver/..."-lenker,
  fetch-kall og iframe-kart uendret.
* blirdetfint.no/snø, /kvamskogen osv. (ikke vær, krever innlogging)
  -> 302 til prisanalyse.no.
* prisanalyse.no/ver/<sti>  -> 301 til blirdetfint.no/<sti> ved sidenavigering
  (kan slås av med VAER_REDIRECT_FRA_PRISANALYSE=0). API-kall og iframes
  (f.eks. /ver/sammenlign innebygd på visitkvamskogen.no) røres ikke.
* robots.txt og sitemap.xml lages egne for værdomenet.
"""

from __future__ import annotations

import os
from html import escape
from typing import Iterable
from urllib.parse import quote

VAER_DOMENE = (os.environ.get("VAER_DOMENE", "blirdetfint.no") or "").strip().lower()
HOVED_DOMENE = (os.environ.get("HOVED_DOMENE", "prisanalyse.no") or "").strip().lower()
REDIRECT_FRA_HOVED = (
    os.environ.get("VAER_REDIRECT_FRA_PRISANALYSE", "1") or ""
).strip().lower() in {"1", "true", "ja", "yes", "on"}

FORSIDE = "/ver/varsel/bergen"

# Felles filer som ligger utenfor /ver og skal fungere likt på begge domener.
_DELTE_PREFIKSER = ("/static/", "/assets/", "/favicon.ico", "/stromdash/_dash")

# Sider som lenkes fra værsidene, men som hører til prisanalyse.no.
_TIL_HOVED_PREFIKSER = ("/snø", "/kvamskogen", "/login", "/logout")


def _verter(domene: str) -> set[str]:
    return {domene, f"www.{domene}"} if domene else set()


def _host(environ) -> str:
    host = environ.get("HTTP_X_FORWARDED_HOST") or environ.get("HTTP_HOST") or ""
    return host.split(",")[0].strip().split(":")[0].lower()


def _sti(environ) -> str:
    # PEP 3333: PATH_INFO er bytes dekodet som latin-1. Gjør om til ekte UTF-8
    # slik at "/snø" kan sammenlignes.
    raw = environ.get("PATH_INFO", "") or "/"
    try:
        return raw.encode("latin-1").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        return raw


def _sett_sti(environ, sti: str) -> None:
    environ["PATH_INFO"] = sti.encode("utf-8").decode("latin-1")


def _er_sidenavigering(environ) -> bool:
    if environ.get("REQUEST_METHOD", "GET") not in {"GET", "HEAD"}:
        return False
    dest = (environ.get("HTTP_SEC_FETCH_DEST") or "").lower()
    if dest:
        return dest == "document"
    # Crawlere og eldre klienter sender ikke Sec-Fetch-Dest.
    return "text/html" in (environ.get("HTTP_ACCEPT") or "")


def _uten_ver(sti: str) -> str:
    rest = sti[len("/ver"):]
    return rest if rest.startswith("/") else "/" + rest


def _med_query(sti: str, environ) -> str:
    qs = environ.get("QUERY_STRING", "")
    return quote(sti, safe="/-_.~") + (f"?{qs}" if qs else "")


def _svar(start_response, status: str, body: str = "", headers: Iterable = (), mimetype: str = "text/plain"):
    data = body.encode("utf-8")
    start_response(
        status,
        [("Content-Type", f"{mimetype}; charset=utf-8"), ("Content-Length", str(len(data))), *headers],
    )
    return [data]


def _redirect(start_response, url: str, permanent: bool = True):
    status = "301 Moved Permanently" if permanent else "302 Found"
    return _svar(start_response, status, headers=[("Location", url), ("Cache-Control", "no-cache")])


class VaerDomeneMiddleware:
    def __init__(self, wsgi_app, sitemap_sider: Iterable[str] = (), robots_disallow: Iterable[str] = ()):
        self.wsgi_app = wsgi_app
        self.sitemap_sider = [s for s in sitemap_sider if s == "/ver" or s.startswith("/ver/")]
        self.robots_disallow = list(robots_disallow)
        self.vaer_verter = _verter(VAER_DOMENE)
        self.hoved_verter = _verter(HOVED_DOMENE)

    # -- robots/sitemap for værdomenet --------------------------------------
    def _robots(self) -> str:
        disallow = []
        for linje in self.robots_disallow:
            navn, _, sti = linje.partition(":")
            sti = sti.strip()
            if sti.startswith("/ver/"):
                sti = _uten_ver(sti)
            disallow.append(f"{navn}: {sti}")
        linjer = ["User-agent: *", "Allow: /", *disallow, "", f"Sitemap: https://{VAER_DOMENE}/sitemap.xml"]
        return "\n".join(linjer) + "\n"

    def _sitemap(self) -> str:
        linjer = ['<?xml version="1.0" encoding="UTF-8"?>',
                  '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">']
        for sti in self.sitemap_sider:
            linjer.append(f"  <url><loc>{escape(f'https://{VAER_DOMENE}{_uten_ver(sti)}')}</loc></url>")
        linjer.append("</urlset>")
        return "\n".join(linjer) + "\n"

    # -- selve rutingen -----------------------------------------------------
    def __call__(self, environ, start_response):
        host = _host(environ)
        sti = _sti(environ)

        if host in self.vaer_verter:
            return self._vaer(environ, start_response, sti)

        if (
            REDIRECT_FRA_HOVED
            and host in self.hoved_verter
            and (sti == "/ver" or sti.startswith("/ver/"))
            and "/api/" not in sti
            and _er_sidenavigering(environ)
        ):
            return _redirect(start_response, f"https://{VAER_DOMENE}" + _med_query(_uten_ver(sti), environ))

        return self.wsgi_app(environ, start_response)

    def _vaer(self, environ, start_response, sti: str):
        if sti == "/robots.txt":
            return _svar(start_response, "200 OK", self._robots())
        if sti == "/sitemap.xml":
            return _svar(start_response, "200 OK", self._sitemap(), mimetype="application/xml")

        if sti.startswith(_DELTE_PREFIKSER):
            return self.wsgi_app(environ, start_response)

        if sti.startswith(_TIL_HOVED_PREFIKSER):
            return _redirect(start_response, f"https://{HOVED_DOMENE}" + _med_query(sti, environ), permanent=False)

        if sti == "/ver" or sti.startswith("/ver/"):
            if "/api/" not in sti and _er_sidenavigering(environ):
                return _redirect(start_response, _med_query(_uten_ver(sti), environ))
            return self.wsgi_app(environ, start_response)

        # Forsiden er været i Bergen; oversikten over alt ligger på /oversikt.
        _sett_sti(environ, FORSIDE if sti == "/" else "/ver" + sti)
        return self.wsgi_app(environ, start_response)
