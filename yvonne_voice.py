"""GPT-Live voice for a single grounded reply. No microphone is sent here."""
import asyncio
import base64
import contextlib
import json
import logging
import math
import os
import re
import struct
import time

from websockets.asyncio.client import connect

log = logging.getLogger(__name__)
RATE = 24000
MAX_SECONDS = 60


def spoken_text(text):
    text = re.sub(r'\[([^\]]+)\]\(https?://[^)]+\)', r'\1', text)
    text = re.sub(r'\ue200[^\ue201]*\ue201', '', text)
    text = re.sub(r'https?://\S+', '', text)
    return text.strip()[:2200]


async def speak(text, on_audio):
    """Stream bounded PCM16 to the already-consenting meeting.

    GPT-Live has no audio-done event. Close after a measured silence in PCM,
    not a pause in transcript delivery. A hard timeout bounds cost/latency.
    """
    text = spoken_text(text)
    if not text:
        raise ValueError('No text for voice')
    headers = {'Authorization': 'Bearer ' + os.environ['OPENAI_API_KEY']}
    model = os.environ.get('YVONNE_VOICE_MODEL', 'gpt-live-1')
    voice = os.environ.get('YVONNE_VOICE', 'marin')
    instructions = (
        'Du er Yvonne, en blid og behagelig norsk møteassistent med en varm, naturlig kvinnelig stemme. '
        'Snakk norsk bokmål, tydelig og vennlig, med et lite smil i stemmen og rolig samtaletempo. '
        'Ikke le overdrevent, ikke bruk reklamestemme. Du skal bare formidle den ferdige svarteksten. '
        'Ikke legg til nye fakta, ikke gjør egne oppslag, ikke still oppfølgingsspørsmål og ikke deleger. '
        'Les teksten med en gang og bli deretter stille. Gjenta den aldri.'
    )
    finished = False
    started = False
    sent_close = False
    voiced = False
    trailing_samples = 0
    output_samples = 0
    transcript = []
    created = time.monotonic()
    sender = None
    result = {'model': model, 'voice': voice, 'finalized': False}
    async with connect('wss://api.openai.com/v1/live/sessions', additional_headers=headers,
                       open_timeout=15, close_timeout=3, max_size=2 * 1024 * 1024) as ws:
        async def silence():
            # Keep Live's session timeline moving. Synthetic silence, never meeting audio.
            packet = base64.b64encode(b'\0' * 4800).decode('ascii')
            while True:
                await ws.send(json.dumps({'type': 'session.input_audio.append', 'audio': packet}))
                await asyncio.sleep(.1)

        async def close():
            nonlocal sent_close
            if not sent_close and started:
                sent_close = True
                if sender:
                    sender.cancel()
                await ws.send(json.dumps({'type': 'session.close'}))

        try:
            await ws.send(json.dumps({'type': 'session.start', 'event_id': 'yvonne_start', 'session': {
                'model': model, 'store': False, 'instructions': instructions,
                'audio': {'format': {'type': 'audio/pcm', 'rate': RATE}, 'output': {'voice': voice}},
                'delegation': {'type': 'client'},
                'input': [{'type': 'message', 'role': 'developer', 'content': [{'type': 'input_text', 'text':
                    'Les kun denne ferdige svarteksten høyt nå, på norsk. Ikke endre fakta eller legg til innhold:\n' + text}]}],
            }}))
            async with asyncio.timeout(MAX_SECONDS + 15):
                while not finished:
                    raw = await asyncio.wait_for(ws.recv(), timeout=8 if sent_close else 15)
                    event = json.loads(raw)
                    kind = event.get('type')
                    if kind == 'error':
                        error = event.get('error', {})
                        log.warning('Yvonne Live rejected a command: code=%s param=%s', str(error.get('code', 'unknown'))[:80], str(error.get('param', 'unknown'))[:120])
                        raise ValueError('Voice API rejected command')
                    if kind == 'session.started':
                        started = True
                        sender = asyncio.create_task(silence())
                        await ws.send(json.dumps({'type': 'session.commentary.append', 'event_id': 'yvonne_read',
                            'delegation_id': None, 'content': 'Les den ferdige svarteksten fra oppstarten nå, på norsk, og bli så stille.'}))
                    elif kind == 'session.output_transcript.delta':
                        transcript.append(str(event.get('delta', '')))
                    elif kind == 'session.output_audio.delta' and not sent_close:
                        pcm = base64.b64decode(event.get('delta', ''), validate=True)
                        if len(pcm) % 2 or len(pcm) > RATE * 2 * 5:
                            raise ValueError('Invalid voice audio')
                        samples = len(pcm) // 2
                        output_samples += samples
                        values = struct.unpack('<' + 'h' * samples, pcm) if samples else ()
                        # Quantify energy in the audio itself; caption timing is not used.
                        rms = math.sqrt(sum(v * v for v in values) / samples) if samples else 0
                        if rms >= 100:
                            voiced = True
                            trailing_samples = 0
                        elif voiced:
                            trailing_samples += samples
                        if output_samples > RATE * MAX_SECONDS:
                            raise ValueError('Voice duration exceeded limit')
                        await on_audio(pcm)
                        if voiced and trailing_samples >= RATE * 4:
                            await close()
                    elif kind == 'session.closed':
                        result['finalized'] = True
                        result['seconds'] = event.get('usage', {}).get('seconds')
                        finished = True
                    if time.monotonic() - created > MAX_SECONDS and not sent_close:
                        await close()
                        if not voiced:
                            raise ValueError('Voice never started')
            if not voiced:
                raise ValueError('No audible voice returned')
            result['transcript'] = ''.join(transcript)
            return result
        finally:
            if sender:
                sender.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await sender
            if started and not sent_close:
                # Also close on consent withdrawal, provider failure or request cancellation.
                with contextlib.suppress(Exception):
                    await asyncio.shield(close())
                    async with asyncio.timeout(3):
                        while not finished:
                            event = json.loads(await asyncio.shield(ws.recv()))
                            if event.get('type') == 'session.closed':
                                finished = True
            if started and not (finished or result['finalized']):
                log.warning('Yvonne Live session close was not confirmed')
