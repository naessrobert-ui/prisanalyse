import importlib
import json

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect


@pytest.fixture()
def client(monkeypatch):
    monkeypatch.setenv("NAM_MEET_KODE", "hemmelig")
    import meet_routes
    importlib.reload(meet_routes)
    app = FastAPI()
    app.include_router(meet_routes.router)
    return TestClient(app)


def _msg(ws):
    return json.loads(ws.receive_text())


def test_side_lastes(client):
    r = client.get("/meet")
    assert r.status_code == 200
    assert "møtes.no" in r.text


def test_feil_kode_avvises(client):
    with pytest.raises(WebSocketDisconnect) as e:
        with client.websocket_connect("/meet/ws/test-rom?name=X&kode=feil") as ws:
            ws.receive_text()
    assert e.value.code == 4401


def test_to_deltakere_finner_hverandre_og_signalerer(client):
    with client.websocket_connect("/meet/ws/test-rom?name=Robert&kode=hemmelig") as a:
        wa = _msg(a)
        assert wa["t"] == "welcome" and wa["peers"] == []
        with client.websocket_connect("/meet/ws/test-rom?name=Kari&kode=hemmelig") as b:
            wb = _msg(b)
            assert [p["name"] for p in wb["peers"]] == ["Robert"]
            assert _msg(a)["t"] == "peer-joined"

            b.send_text(json.dumps({"t": "sig", "to": wa["id"], "data": {"k": "offer", "sid": "x", "sdp": "v=0"}}))
            m = _msg(a)
            assert m["t"] == "sig" and m["from"] == wb["id"] and m["data"]["k"] == "offer"

            a.send_text(json.dumps({"t": "state", "state": {"muted": True, "ukjent": 1}}))
            st = _msg(b)["state"]
            assert st["muted"] is True and "ukjent" not in st
        assert _msg(a)["t"] == "peer-left"


def test_status_viser_ingen_navn(client):
    r = client.get("/meet/status").json()
    assert set(r) == {"rom", "deltakere", "krever_kode", "turn"}


def test_samme_klient_id_gir_samme_deltaker_id(client):
    cid = "ab" * 16
    with client.websocket_connect(f"/meet/ws/test-rom?name=Robert&kode=hemmelig&cid={cid}") as a:
        id1 = _msg(a)["id"]
    with client.websocket_connect(f"/meet/ws/test-rom?name=Robert&kode=hemmelig&cid={cid}") as a:
        id2 = _msg(a)["id"]
    assert id1 == id2
    assert cid not in id1  # den hemmelige klient-id-en sendes aldri til andre



def test_peer_left_skiller_bye_fra_brudd(client):
    with client.websocket_connect("/meet/ws/test-rom?name=Robert&kode=hemmelig") as a:
        _msg(a)
        with client.websocket_connect("/meet/ws/test-rom?name=Kari&kode=hemmelig") as b:
            kari = _msg(b)["id"]
            _msg(a)  # peer-joined
        assert _msg(a) == {"t": "peer-left", "id": kari, "bye": False}

        with client.websocket_connect("/meet/ws/test-rom?name=Kari&kode=hemmelig") as b:
            kari = _msg(b)["id"]
            _msg(a)  # peer-joined
            b.send_text(json.dumps({"t": "bye"}))
            b.send_text(json.dumps({"t": "ping"}))
            assert _msg(b)["t"] == "pong"  # bye er behandlet
        assert _msg(a) == {"t": "peer-left", "id": kari, "bye": True}


def test_ny_forbindelse_fra_samme_fane_erstatter_den_gamle(client):
    cid = "cd" * 16
    with client.websocket_connect("/meet/ws/test-rom?name=Robert&kode=hemmelig") as a:
        _msg(a)
        with client.websocket_connect(f"/meet/ws/test-rom?name=Kari&kode=hemmelig&cid={cid}") as b1:
            kari = _msg(b1)["id"]
            assert _msg(a)["t"] == "peer-joined"
            with client.websocket_connect(f"/meet/ws/test-rom?name=Kari&kode=hemmelig&cid={cid}") as b2:
                velkommen = _msg(b2)
                assert velkommen["id"] == kari
                assert [p["name"] for p in velkommen["peers"]] == ["Robert"]  # ikke seg selv
                assert _msg(a) == {"t": "peer-joined", "peer": {"id": kari, "name": "Kari", "state": {}}}
                with pytest.raises(WebSocketDisconnect) as e:
                    b1.receive_text()
                assert e.value.code == 4000
                assert client.get("/meet/status").json()["deltakere"] == 2
                # Den gamle forbindelsen skal ikke ha meldt Kari ut.
                a.send_text(json.dumps({"t": "ping"}))
                assert _msg(a)["t"] == "pong"


def test_ice_krever_kode_og_gir_stun_uten_turn(client):
    assert client.get("/meet/ice?kode=feil").status_code == 401
    r = client.get("/meet/ice?kode=hemmelig").json()
    assert r["turn"] is False and r["iceServers"][0]["urls"]


def test_ice_henter_turn_fra_cloudflare_og_mellomlagrer(client, monkeypatch):
    import meet_routes

    kall = []

    async def falsk_hent():
        kall.append(1)
        return [{"urls": ["stun:stun.cloudflare.com:3478"]},
                {"urls": ["turn:turn.cloudflare.com:3478?transport=udp", "turn:turn.cloudflare.com:53?transport=udp"],
                 "username": "u", "credential": "c"}]

    monkeypatch.setattr(meet_routes, "TURN_KEY_ID", "id")
    monkeypatch.setattr(meet_routes, "TURN_TOKEN", "tok")
    monkeypatch.setattr(meet_routes, "_hent_turn", falsk_hent)
    a = client.get("/meet/ice?kode=hemmelig").json()
    b = client.get("/meet/ice?kode=hemmelig").json()
    assert a["turn"] is True and a == b
    assert len(kall) == 1  # mellomlagret
    assert client.get("/meet/status").json()["turn"] is True


def test_port_53_filtreres_bort(monkeypatch):
    import asyncio
    import meet_routes

    class Svar:
        def raise_for_status(self):
            pass

        def json(self):
            return {"iceServers": [{"urls": ["turn:t:3478?transport=udp", "turn:t:53?transport=udp"], "username": "u", "credential": "c"}]}

    class Klient:
        def __init__(self, **kw):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            pass

        async def post(self, url, headers, json):
            assert headers["Authorization"] == "Bearer tok" and json["ttl"] > 0
            return Svar()

    monkeypatch.setattr(meet_routes, "TURN_KEY_ID", "id")
    monkeypatch.setattr(meet_routes, "TURN_TOKEN", "tok")
    monkeypatch.setattr(meet_routes.httpx, "AsyncClient", Klient)
    servere = asyncio.run(meet_routes._hent_turn())
    assert servere[0]["urls"] == ["turn:t:3478?transport=udp"]


def test_invitasjon_gir_tilgang_til_ett_rom_uten_kode(client):
    assert client.get("/meet/invite?rom=test-rom&kode=feil").status_code == 401
    d = client.get("/meet/invite?rom=Test-Rom&kode=hemmelig").json()
    assert d["rom"] == "test-rom" and d["inv"] and d["utloper"] > 0
    inv = d["inv"]

    with client.websocket_connect(f"/meet/ws/test-rom?name=Kari&inv={inv}") as ws:
        assert _msg(ws)["t"] == "welcome"
    assert client.get(f"/meet/ice?rom=test-rom&inv={inv}").status_code == 200

    # Gjelder ikke andre rom
    with pytest.raises(WebSocketDisconnect) as e:
        with client.websocket_connect(f"/meet/ws/annet-rom?name=Kari&inv={inv}") as ws:
            ws.receive_text()
    assert e.value.code == 4401
    assert client.get(f"/meet/ice?rom=annet-rom&inv={inv}").status_code == 401

    # Den inviterte kan dele samme lenke videre, men den forlenges ikke
    assert client.get(f"/meet/invite?rom=test-rom&inv={inv}").json()["inv"] == inv


def test_utlopt_eller_forfalsket_invitasjon_avvises(client):
    import meet_routes

    gammel = meet_routes._lag_invitasjon("test-rom", 1000)
    ekte = client.get("/meet/invite?rom=test-rom&kode=hemmelig").json()["inv"]
    falsk = ekte[:-1] + ("1" if ekte[-1] == "0" else "0")
    for inv in (gammel, falsk, "tull", ""):
        with pytest.raises(WebSocketDisconnect) as e:
            with client.websocket_connect(f"/meet/ws/test-rom?name=X&inv={inv}") as ws:
                ws.receive_text()
        assert e.value.code == 4401


def test_public_create_room_scopes_invitation_and_preserves_old_access(client):
    import meet_routes

    before = client.get('/meet/status').json()
    headers = {'host': 'xn--mtes-gra.no'}
    response = client.post('/meet/create', headers=headers)
    assert response.status_code == 200
    assert response.headers['cache-control'] == 'no-store'
    first = response.json()
    second = client.post('/meet/create', headers=headers).json()
    assert first['rom'] != second['rom']
    assert meet_routes._gyldig_invitasjon(first['inv'], first['rom'])
    assert not meet_routes._gyldig_invitasjon(first['inv'], second['rom'])
    assert first['utloper'] > meet_routes.time.time()
    assert 'hemmelig' not in response.text
    assert client.get('/meet/status').json() == before
    with client.websocket_connect(f"/meet/ws/{first['rom']}?name=Vert&inv={first['inv']}") as host:
        assert _msg(host)['peers'] == []
        with client.websocket_connect(f"/meet/ws/{first['rom']}?name=Gjest&inv={first['inv']}") as guest:
            assert _msg(guest)['peers'][0]['name'] == 'Vert'
    assert client.get('/meet/ice?rom=test-rom&kode=feil').status_code == 401
    assert client.get('/meet/ice?rom=test-rom&kode=hemmelig').status_code == 200


def test_public_create_requires_meeting_domain(client):
    assert client.post('/meet/create', headers={'host': 'prisanalyse.no'}).status_code == 404
    assert client.post('/meet/create', headers={'host': 'xn--mtes-gra.no.example.com'}).status_code == 404
    assert client.get('/meet/create', headers={'host': 'xn--mtes-gra.no'}).status_code == 405
    assert client.post('/meet/create', headers={'host': 'www.xn--mtes-gra.no'}).status_code == 200
