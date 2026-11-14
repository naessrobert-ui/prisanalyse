import asyncio
import base64
import json
import struct

import pytest

import yvonne_voice


class VoiceSocket:
    def __init__(self, *, error=False):
        self.sent = []
        voiced = struct.pack('<' + 'h' * 2400, *([1000] * 2400))
        self.events = [{'type': 'session.started', 'session': {'id': 'test'}}]
        if error:
            self.events.append({'type': 'error', 'error': {'code': 'test_error'}})
        else:
            self.events += [
                {'type': 'session.output_transcript.delta', 'delta': 'Hei, jeg heter Yvonne.'},
                {'type': 'session.output_audio.delta', 'delta': base64.b64encode(voiced).decode()},
                {'type': 'session.output_audio.delta', 'delta': base64.b64encode(b'\0' * 24000 * 2 * 4).decode()},
            ]

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass

    async def send(self, value):
        event = json.loads(value)
        self.sent.append(event)
        if event['type'] == 'session.close':
            self.events.append({'type': 'session.closed', 'usage': {'seconds': 5}})

    async def recv(self):
        await asyncio.sleep(0)
        if self.events:
            return json.dumps(self.events.pop(0))
        await asyncio.Event().wait()


def test_live_voice_stream_uses_silence_not_meeting_audio_and_finalizes(monkeypatch):
    monkeypatch.setenv('OPENAI_API_KEY', 'test-only-placeholder')
    ws = VoiceSocket()
    connection = []
    def connect(url, **kwargs):
        connection.append((url, kwargs))
        return ws
    monkeypatch.setattr(yvonne_voice, 'connect', connect)
    chunks = []
    async def chunk(data):
        chunks.append(data)
    result = asyncio.run(yvonne_voice.speak('Hei, jeg heter Yvonne.', chunk))
    assert result['model'] == 'gpt-live-1'
    assert result['voice'] == 'marin' and result['finalized']
    assert result['transcript'] == 'Hei, jeg heter Yvonne.'
    assert len(chunks) == 2
    assert connection[0][0] == 'wss://api.openai.com/v1/live/sessions'
    config = ws.sent[0]['session']
    assert config['store'] is False and config['delegation'] == {'type': 'client'}
    assert 'blid' in config['instructions'] and 'norsk' in config['instructions']
    for event in ws.sent:
        if event['type'] == 'session.input_audio.append':
            assert not any(base64.b64decode(event['audio']))
    assert any(e['type'] == 'session.close' for e in ws.sent)


def test_live_error_still_requests_close(monkeypatch):
    monkeypatch.setenv('OPENAI_API_KEY', 'test-only-placeholder')
    ws = VoiceSocket(error=True)
    monkeypatch.setattr(yvonne_voice, 'connect', lambda *args, **kwargs: ws)
    async def chunk(data):
        raise AssertionError('No audio expected')
    with pytest.raises(ValueError, match='rejected'):
        asyncio.run(yvonne_voice.speak('En test.', chunk))
    assert any(e['type'] == 'session.close' for e in ws.sent)


def test_cancelled_meeting_stops_voice_and_finalizes(monkeypatch):
    monkeypatch.setenv('OPENAI_API_KEY', 'test-only-placeholder')
    ws = VoiceSocket()
    monkeypatch.setattr(yvonne_voice, 'connect', lambda *args, **kwargs: ws)
    async def chunk(data):
        raise asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(yvonne_voice.speak('Et svar.', chunk))
    assert any(e['type'] == 'session.close' for e in ws.sent)


def test_spoken_text_strips_links_and_reference_tokens():
    assert yvonne_voice.spoken_text('Tall fra [SSB](https://www.ssb.no). \ue200citeABC\ue201') == 'Tall fra SSB.'
