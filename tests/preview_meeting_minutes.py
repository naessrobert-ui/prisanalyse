"""Local browser-test server only. Fake transcription and summary, real signed Pi queue."""
import asyncio
import base64
import importlib
import json
import os
import tempfile
from pathlib import Path

import httpx
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from fastapi import FastAPI

import minutes_bridge
import minutes_protocol
from minutes_pi import Store, create_app

folder=Path(tempfile.mkdtemp(prefix='minutes-preview-'))
cli=folder/'fake-whisper'
cli.write_text('#!/usr/bin/env python3\nimport sys\nfrom pathlib import Path\nPath(sys.argv[sys.argv.index("-of")+1]+".txt").write_text("Vi vedtar å kjøpe en skriver. Ansvar og frist er ikke bestemt.")\n')
cli.chmod(0o700)
key=base64.b64encode(Ed25519PrivateKey.generate().private_bytes_raw()).decode()
os.environ.update(OPENAI_API_KEY='test-only',MINUTES_PI_ORIGIN='https://example.ts.net:10000',MINUTES_SIGNING_KEY=key)
os.environ.pop('NAM_MEET_KODE',None)
import meet_routes
importlib.reload(meet_routes)
store=Store(folder/'data',cli,folder/'model');pi=create_app(store,minutes_protocol.public_key(key),run_worker=False)
stats={'chunks':0,'summaries':0,'audio_bytes':0}
async def call(method,path,*,data=None,audio=None):
    body=audio if audio is not None else (json.dumps(data).encode() if data is not None else b'')
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=pi),base_url='http://pi') as c:
        r=await c.request(method,path,headers=minutes_protocol.sign(key,method,path,body),content=body)
    if r.status_code>=400:raise minutes_bridge.Unavailable('Pi rejected request',r.status_code)
    if audio is not None:
        stats['chunks']+=1;stats['audio_bytes']+=len(audio)
        await asyncio.to_thread(store.process_next)
    return r.json()
async def summarize(text,participants,partial):
    stats['summaries']+=1
    assert 'skriver' in text
    return 'REFERATUTKAST\nBeslutninger: Kjøp av skriver.\nAnsvar og frist: ikke avklart.\nMøtelederen må godkjenne.'
minutes_bridge.call=call;minutes_bridge.summarize=summarize
app=FastAPI();app.include_router(meet_routes.router)
@app.get('/')
async def root():return await meet_routes.meet_side()
@app.get('/testing/stats')
async def testing_stats():return stats
