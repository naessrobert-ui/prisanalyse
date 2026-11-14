from fastapi import FastAPI
from fastapi.testclient import TestClient

from meet_domene import MeetDomeneMiddleware
from meet_routes import router


def test_motes_forside_og_invitasjonslenker():
    app = FastAPI()
    app.add_middleware(MeetDomeneMiddleware)
    app.include_router(router)

    @app.get("/")
    def hovedside():
        return {"side": "prisanalyse"}

    with TestClient(app) as client:
        for host in ("xn--mtes-gra.no", "www.xn--mtes-gra.no", "XN--MTES-GRA.NO:443"):
            response = client.get("/?rom=test", headers={"host": host})
            assert response.status_code == 200
            assert "NAM Meet" in response.text
            assert response.headers["cache-control"] == "no-store"
        for host in ("prisanalyse.no", "blirdetfint.no", "xn--mtes-gra.no.example.org"):
            assert client.get("/", headers={"host": host}).json() == {"side": "prisanalyse"}
        assert client.get("/meet", headers={"host": "xn--mtes-gra.no"}).status_code == 200
        assert client.get("/meet/status", headers={"host": "xn--mtes-gra.no"}).status_code == 200
