# -*- coding: utf-8 -*-
from flask import Flask, request
from werkzeug.test import Client

from vaer_domene import VaerDomeneMiddleware

NAV = {"Sec-Fetch-Dest": "document", "Accept": "text/html"}


def _app():
    app = Flask(__name__)

    @app.route("/ver/")
    def hub():
        return "hub"

    @app.route("/ver/aktivt-varsel")
    def aktivt():
        return f"aktivt {request.args.get('sted', '')}"

    @app.route("/ver/api/varsel/<sted>")
    def api(sted):
        return f"api {sted}"

    @app.route("/static/x.css")
    def css():
        return "css"

    @app.route("/")
    def forside():
        return "prisanalyse-forside"

    app.wsgi_app = VaerDomeneMiddleware(
        app.wsgi_app,
        sitemap_sider=["/", "/ver/", "/ver/aktivt-varsel"],
        robots_disallow=["Disallow: /ver/vind-kart"],
    )
    return Client(app)


def get(c, path, host, headers=None):
    return c.get(path, headers={"Host": host, **(headers or {})})


def test_rot_pa_vaerdomenet_viser_huben():
    r = get(_app(), "/", "blirdetfint.no", NAV)
    assert r.status_code == 200 and r.get_data(as_text=True) == "hub"


def test_kort_sti_med_query():
    r = get(_app(), "/aktivt-varsel?sted=bergen", "www.blirdetfint.no", NAV)
    assert r.get_data(as_text=True) == "aktivt bergen"


def test_gammel_ver_lenke_pa_vaerdomenet_redirectes():
    r = get(_app(), "/ver/aktivt-varsel?sted=voss", "blirdetfint.no", NAV)
    assert r.status_code == 301 and r.headers["Location"].endswith("/aktivt-varsel?sted=voss")


def test_api_med_ver_prefiks_gar_rett_gjennom():
    r = get(_app(), "/ver/api/varsel/bergen", "blirdetfint.no", {"Sec-Fetch-Dest": "empty"})
    assert r.get_data(as_text=True) == "api bergen"


def test_statiske_filer_deles():
    assert get(_app(), "/static/x.css", "blirdetfint.no").get_data(as_text=True) == "css"


def test_ikke_vaer_sendes_til_prisanalyse():
    r = get(_app(), "/kvamskogen/", "blirdetfint.no", NAV)
    assert r.status_code == 302 and r.headers["Location"] == "https://prisanalyse.no/kvamskogen/"


def test_prisanalyse_ver_side_redirectes_til_nytt_domene():
    r = get(_app(), "/ver/aktivt-varsel?sted=bergen", "prisanalyse.no", NAV)
    assert r.status_code == 301
    assert r.headers["Location"] == "https://blirdetfint.no/aktivt-varsel?sted=bergen"


def test_prisanalyse_iframe_og_api_rores_ikke():
    c = _app()
    assert get(c, "/ver/aktivt-varsel", "prisanalyse.no", {"Sec-Fetch-Dest": "iframe"}).status_code == 200
    assert get(c, "/ver/api/varsel/bergen", "prisanalyse.no", NAV).status_code == 200


def test_prisanalyse_forside_uendret():
    assert get(_app(), "/", "prisanalyse.no", NAV).get_data(as_text=True) == "prisanalyse-forside"


def test_robots_og_sitemap_for_vaerdomenet():
    c = _app()
    robots = get(c, "/robots.txt", "blirdetfint.no").get_data(as_text=True)
    assert "Disallow: /vind-kart" in robots and "https://blirdetfint.no/sitemap.xml" in robots
    sitemap = get(c, "/sitemap.xml", "blirdetfint.no").get_data(as_text=True)
    assert "https://blirdetfint.no/aktivt-varsel" in sitemap
    assert "<loc>https://blirdetfint.no/</loc>" in sitemap
