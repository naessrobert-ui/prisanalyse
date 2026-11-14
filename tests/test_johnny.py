import asyncio
import importlib
import json

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient


@pytest.fixture
def meeting(monkeypatch):
    monkeypatch.setenv('NAM_MEET_KODE', 'meeting-test-code')
    monkeypatch.setenv('JOHNNY_ENABLED', 'true')
    monkeypatch.setenv('OPENAI_API_KEY', 'test-placeholder-not-real')
    import meet_routes
    importlib.reload(meet_routes)
    async def voice(text, on_audio):
        import struct
        await on_audio(struct.pack('<' + 'h'*2400, *([1000]*2400)))
        return {'model': 'gpt-live-1', 'voice': 'marin', 'finalized': True}
    monkeypatch.setattr(meet_routes.yvonne_voice, 'speak', voice)
    app = FastAPI()
    app.include_router(meet_routes.router)
    with TestClient(app) as client:
        yield client, meet_routes


def message(ws, kind):
    for _ in range(30):
        value = ws.receive_json()
        if value.get('t') == kind:
            return value
    raise AssertionError('Expected ' + kind)


def connect(client, cid):
    return client.websocket_connect('/meet/ws/test-rom?name=' + cid[:3] + '&kode=meeting-test-code&cid=' + cid)


def approve(ws):
    ws.send_json({'t': 'johnny-consent', 'accept': True})
    return message(ws, 'johnny')


def headers(cid):
    return {'Content-Type': 'audio/webm', 'X-Meet-Room': 'test-rom', 'X-Meet-Client': cid}


AUDIO = b'\x1aE\xdf\xa3test-audio'


def test_all_participants_must_approve_and_reply_is_shared(meeting, monkeypatch):
    client, routes = meeting
    calls = []
    async def answer(audio, mime):
        calls.append((audio, mime))
        return {'text': 'Et kontrollert svar.', 'question': 'Et spørsmål?', 'citations': []}
    monkeypatch.setattr(routes.johnny_ai, 'answer', answer)
    a_id, b_id = 'ab' * 16, 'cd' * 16
    with connect(client, a_id) as a, connect(client, b_id) as b:
        message(a, 'welcome');message(b, 'welcome')
        assert client.post('/meet/johnny/ask', headers=headers(a_id), content=AUDIO).status_code == 409
        a.send_json({'t': 'johnny-propose'})
        assert message(a, 'johnny')['active'] is False
        message(b, 'johnny')
        assert approve(a)['active'] is False
        message(b, 'johnny')
        assert client.post('/meet/johnny/ask', headers=headers(a_id), content=AUDIO).status_code == 409
        assert not calls
        assert approve(b)['active'] is True
        assert message(a, 'johnny')['active'] is True
        response = client.post('/meet/johnny/ask', headers=headers(a_id), content=AUDIO)
        assert response.status_code == 200
        assert message(a, 'johnny-answer')['text'] == 'Et kontrollert svar.'
        assert message(b, 'johnny-answer')['askedBy'] == 'aba'
        assert calls == [(AUDIO, 'audio/webm')]


def test_new_participant_pauses_until_fresh_approval_and_no_stops(meeting):
    client, routes = meeting
    a_id = 'ab' * 16
    with connect(client, a_id) as a:
        message(a, 'welcome');a.send_json({'t': 'johnny-propose'});message(a, 'johnny')
        assert approve(a)['active'] is True
        with connect(client, 'cd' * 16) as b:
            message(b, 'welcome')
            assert message(a, 'johnny')['active'] is False
            assert message(b, 'johnny')['approved'] == []
            assert client.post('/meet/johnny/ask', headers=headers(a_id), content=AUDIO).status_code == 409
            approve(a);message(b, 'johnny')
            assert approve(b)['active'] is True
            message(a, 'johnny')
            b.send_json({'t': 'johnny-consent', 'accept': False})
            assert message(b, 'johnny')['proposed'] is False
            assert message(a, 'johnny')['active'] is False
            assert client.post('/meet/johnny/ask', headers=headers(a_id), content=AUDIO).status_code == 409


def test_withdrawal_cancels_in_flight_provider(meeting, monkeypatch):
    import threading
    client, routes = meeting
    started, cancelled = threading.Event(), threading.Event()
    async def answer(audio, mime):
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise
    monkeypatch.setattr(routes.johnny_ai, 'answer', answer)
    cid = 'ab' * 16
    with connect(client, cid) as a:
        message(a, 'welcome');a.send_json({'t': 'johnny-propose'});message(a, 'johnny');approve(a)
        responses = []
        thread = threading.Thread(target=lambda: responses.append(client.post('/meet/johnny/ask', headers=headers(cid), content=AUDIO)))
        thread.start()
        assert started.wait(3)
        a.send_json({'t': 'johnny-consent', 'accept': False})
        assert cancelled.wait(3)
        thread.join(3)
        assert responses[0].status_code == 409
        assert routes._johnny_state('test-rom')['active'] is False


def test_identity_file_type_and_rate_limits(meeting, monkeypatch):
    client, routes = meeting
    calls = []
    async def answer(audio, mime):
        calls.append(1)
        return {'text': 'Svar', 'question': 'Spørsmål', 'citations': []}
    monkeypatch.setattr(routes.johnny_ai, 'answer', answer)
    cid = 'ab' * 16
    assert client.post('/meet/johnny/ask', headers=headers(cid), content=AUDIO).status_code == 403
    with connect(client, cid) as a:
        message(a, 'welcome');a.send_json({'t': 'johnny-propose'});message(a, 'johnny');approve(a)
        assert client.post('/meet/johnny/ask', headers={**headers(cid), 'Content-Type': 'text/html'}, content=b'html').status_code == 415
        assert client.post('/meet/johnny/ask', headers=headers(cid), content=b'not audio').status_code == 415
        assert not calls
        assert client.post('/meet/johnny/ask', headers=headers(cid), content=AUDIO).status_code == 200
        assert client.post('/meet/johnny/ask', headers=headers(cid), content=AUDIO).status_code == 429


def test_optional_feature_does_not_enable_without_server_configuration(monkeypatch):
    import johnny_ai
    monkeypatch.delenv('OPENAI_API_KEY', raising=False)
    monkeypatch.setenv('JOHNNY_ENABLED', 'true')
    assert johnny_ai.capability()['available'] is False
    monkeypatch.setenv('OPENAI_API_KEY', 'test-placeholder')
    monkeypatch.delenv('JOHNNY_ENABLED', raising=False)
    assert johnny_ai.capability()['available'] is False


def test_provider_payload_citations_and_no_stored_response(monkeypatch):
    import johnny_ai
    monkeypatch.setenv('OPENAI_API_KEY', 'test-placeholder')
    calls = []
    async def transport(request):
        body = await request.aread()
        calls.append((str(request.url), body))
        if str(request.url).endswith('/transcriptions'):
            assert b'gpt-transcribe' in body
            return httpx.Response(200, json={'text': 'Hvor mange bor i Norge?'})
        data = json.loads(body)
        assert data['store'] is False
        assert data['tools'] == [{'type': 'web_search'}]
        return httpx.Response(200, json={'output': [{'type': 'message', 'content': [{'type': 'output_text', 'text': 'Tall fra SSB.', 'annotations': [{'type': 'url_citation', 'start_index': 9, 'end_index': 12, 'title': 'SSB', 'url': 'https://www.ssb.no/'}]}]}]})
    real = httpx.AsyncClient
    monkeypatch.setattr(johnny_ai.httpx, 'AsyncClient', lambda **kwargs: real(transport=httpx.MockTransport(transport), **kwargs))
    result = asyncio.run(johnny_ai.answer(AUDIO, 'audio/webm'))
    assert result['question'] == 'Hvor mange bor i Norge?'
    assert result['citations'][0]['url'] == 'https://www.ssb.no/'
    assert len(calls) == 2


def test_new_participant_cancels_streaming_live_voice(meeting, monkeypatch):
    import threading
    client, routes = meeting
    started, cancelled = threading.Event(), threading.Event()
    async def answer(audio, mime):
        return {'text': 'Et svar.', 'question': 'Test?', 'citations': []}
    async def voice(text, on_audio):
        await on_audio(b'\x01\x00' * 24)
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise
    monkeypatch.setattr(routes.johnny_ai, 'answer', answer)
    monkeypatch.setattr(routes.yvonne_voice, 'speak', voice)
    cid = 'ab' * 16
    with connect(client, cid) as a:
        message(a, 'welcome');a.send_json({'t': 'johnny-propose'});message(a, 'johnny');approve(a)
        responses = []
        thread = threading.Thread(target=lambda: responses.append(client.post('/meet/johnny/ask', headers=headers(cid), content=AUDIO)))
        thread.start()
        assert started.wait(3)
        with connect(client, 'cd' * 16) as b:
            message(b, 'welcome')
            assert cancelled.wait(3)
            thread.join(3)
            assert responses[0].status_code == 409
            assert routes._johnny_state('test-rom')['active'] is False
