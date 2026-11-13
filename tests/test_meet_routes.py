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
    assert "NAM Meet" in r.text


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
    assert set(r) == {"rom", "deltakere", "krever_kode"}
