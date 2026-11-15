import asyncio
import base64
import importlib
import io
import json
import struct
import threading
import time
import wave
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import httpx
import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

import minutes_bridge
import minutes_protocol
from minutes_pi import Store, create_app


def wav(seconds=.5):
    out=io.BytesIO()
    with wave.open(out,'wb') as f:
        f.setnchannels(1);f.setsampwidth(2);f.setframerate(16000)
        f.writeframes(struct.pack('<h',1000)*int(seconds*16000))
    return out.getvalue()


@pytest.fixture
def key():
    return base64.b64encode(Ed25519PrivateKey.generate().private_bytes_raw()).decode()


@pytest.fixture
def store(tmp_path):return Store(tmp_path/'data','/unused/whisper','/unused/model')


def sid():return 'ab'*16

def reader():return 'cd'*32


def test_signature_auth_and_replay(key):
    public=minutes_protocol.public_key(key);body=b'wave';nonces={}
    h={k.lower():v for k,v in minutes_protocol.sign(key,'POST','/v1/job',body).items()}
    minutes_protocol.verify(public,'POST','/v1/job',h,body,nonces)
    with pytest.raises(ValueError):minutes_protocol.verify(public,'POST','/v1/job',h,body,nonces)
    with pytest.raises(Exception):minutes_protocol.verify(public,'POST','/v1/other',h,body,{})
    with pytest.raises(Exception):minutes_protocol.verify(public,'POST','/v1/job',h,b'changed',{})


def test_durable_queue_idempotence_and_audio_removed_after_processing(store,monkeypatch):
    store.create(sid(),reader(),['Robert'])
    store.append(sid(),0,wav());assert store.append(sid(),0,wav())['duplicate']
    with pytest.raises(HTTPException):store.append(sid(),0,wav(.6))
    with pytest.raises(HTTPException):store.append(sid(),2,wav())
    def local_run(args,**kwargs):
        assert args[:3]==['nice','-n','10'] and args[args.index('-l')+1]=='no' and args[args.index('-t')+1]=='2'
        Path(args[args.index('-of')+1]+'.txt').write_text('Vi vedtar å kjøpe en ny skriver. Ansvarlig er ikke avklart.')
        return type('Result',(),{'returncode':0})()
    monkeypatch.setattr('minutes_pi.subprocess.run',local_run)
    # Recreate the store like a worker restart; queued data must survive.
    other=Store(store.directory,store.cli,store.model)
    assert other.process_next()
    with other.connect() as db:assert db.execute('SELECT audio FROM chunks').fetchone()[0] is None
    other.finish(sid(),reader(),0)
    with ThreadPoolExecutor(max_workers=2) as pool:
        claims=list(pool.map(lambda _:other.claim(sid(),reader()),range(2)))
    assert sum(c.get('claimed',False) for c in claims)==1
    assert 'ny skriver' in next(c['transcript'] for c in claims if c.get('claimed'))
    other.save_draft(sid(),reader(),'REFERATUTKAST: Kjøp av skriver.')
    assert other.status(sid(),reader())['status']=='done'
    other.delete(sid(),reader())
    with pytest.raises(HTTPException):other.status(sid(),reader())


def test_invalid_wav_and_owner_token(store):
    store.create(sid(),reader(),[])
    for audio in [b'bad',wav(21)]:
        with pytest.raises(HTTPException):store.append(sid(),0,audio)
    with pytest.raises(HTTPException):store.status(sid(),'ff'*32)
    with pytest.raises(HTTPException):store.create('ef'*16,'ab'*32,[])
    store.finish(sid(),reader(),-1)
    store.claim(sid(),reader())
    assert store.status(sid(),reader())['status']=='failed'


def test_monthly_quota_survives_content_deletion(store):
    store.monthly=1;store.create(sid(),reader(),[]);store.delete(sid(),reader())
    with pytest.raises(HTTPException) as e:store.create('ef'*16,reader(),[])
    assert e.value.status_code==429


def test_expired_and_idle_sessions(store):
    store.create(sid(),reader(),[])
    with store.connect() as db:db.execute('UPDATE sessions SET touched=?',(time.time()-130,))
    store.cleanup();assert store.status(sid(),reader())['partial']
    with store.connect() as db:db.execute('UPDATE sessions SET expires=?',(time.time()-1,))
    store.cleanup()
    with pytest.raises(HTTPException):store.status(sid(),reader())


def test_pi_rejects_unsigned_requests_and_protects_all_content(store,key):
    app=create_app(store,minutes_protocol.public_key(key),run_worker=False)
    with TestClient(app) as c:
        assert c.get('/health').status_code==200
        assert c.post('/v1/sessions',json={'id':sid(),'reader':reader(),'participants':[]}).status_code==403
        data=json.dumps({'id':sid(),'reader':reader(),'participants':[]}).encode()
        h=minutes_protocol.sign(key,'POST','/v1/sessions',data)
        assert c.post('/v1/sessions',headers=h,content=data).status_code==200
        assert c.post('/v1/sessions',headers=h,content=data).status_code==403
        assert c.get('/v1/sessions/'+sid()+'/status').status_code==405


@pytest.fixture
def integrated(monkeypatch,store,key):
    monkeypatch.setenv('OPENAI_API_KEY','test-only')
    monkeypatch.setenv('MINUTES_PI_ORIGIN','https://example.ts.net:10000')
    monkeypatch.setenv('MINUTES_SIGNING_KEY',key)
    monkeypatch.setenv('NAM_MEET_KODE','test')
    import meet_routes
    importlib.reload(meet_routes)
    pi=create_app(store,minutes_protocol.public_key(key),run_worker=False)
    async def call(method,path,*,data=None,audio=None):
        body=audio if audio is not None else (json.dumps(data).encode() if data is not None else b'')
        h=minutes_protocol.sign(key,method,path,body)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=pi),base_url='http://pi') as c:
            r=await c.request(method,path,headers=h,content=body)
        if r.status_code>=400:raise minutes_bridge.Unavailable('Pi rejected',r.status_code)
        return r.json()
    monkeypatch.setattr(minutes_bridge,'call',call)
    summaries=[]
    async def summary(text,participants,partial):
        summaries.append((text,participants,partial));return 'REFERATUTKAST: Vedtak om ny skriver. Ansvarlig ikke avklart.'
    monkeypatch.setattr(minutes_bridge,'summarize',summary)
    def local_run(args,**kwargs):
        Path(args[args.index('-of')+1]+'.txt').write_text('Vi vedtar å kjøpe en ny skriver.')
        return type('Result',(),{'returncode':0})()
    monkeypatch.setattr('minutes_pi.subprocess.run',local_run)
    app=FastAPI();app.include_router(meet_routes.router)
    with TestClient(app) as c:yield c,meet_routes,store,summaries


def message(ws,kind):
    for _ in range(30):
        m=ws.receive_json()
        if m.get('t')==kind:return m
    raise AssertionError(kind)


def headers(cid):return {'X-Meet-Room':'test-room','X-Meet-Client':cid}


def test_meeting_to_local_transcription_to_draft_without_vote(integrated):
    c,r,store,summaries=integrated;cid='ab'*16
    with c.websocket_connect('/meet/ws/test-room?name=Robert&kode=test&cid='+cid) as a:
        message(a,'welcome')
        start=c.post('/meet/minutes/start',headers=headers(cid));assert start.status_code==200
        data=start.json();state=message(a,'minutes');assert state['phase']=='recording' and state['start_at']>time.time()
        assert c.post('/meet/minutes/chunk/0',headers={**headers(cid),'X-Minutes-Epoch':str(state['epoch'])},content=wav()).status_code==409
        r.minutes_meet.states['test-room']['start_at']=0
        assert c.post('/meet/minutes/chunk/0',headers={**headers(cid),'X-Minutes-Epoch':str(state['epoch'])},content=wav()).status_code==200
        assert not summaries
        assert c.post('/meet/minutes/finish',headers=headers(cid)).status_code==200
        report_headers={'X-Minutes-Token':data['reader']}
        assert c.get('/meet/minutes/report/'+data['id'],headers=report_headers).json()['queued']==1
        assert not summaries;assert store.process_next()
        result=c.get('/meet/minutes/report/'+data['id'],headers=report_headers).json()
        assert 'ny skriver' in result['draft'];assert len(summaries)==1
        c.get('/meet/minutes/report/'+data['id'],headers=report_headers);assert len(summaries)==1
        assert message(a,'minutes-draft')['text']==result['draft']
    # Recovery does not require staying connected to the meeting.
    assert c.get('/meet/minutes/report/'+data['id'],headers=report_headers).json()['draft']
    assert c.get('/meet/minutes/report/'+data['id'],headers={'X-Minutes-Token':'ff'*32}).status_code==403


def test_new_joiner_gets_notice_and_everyone_can_stop(integrated):
    c,r,store,summaries=integrated;cid='ab'*16
    with c.websocket_connect('/meet/ws/test-room?name=Robert&kode=test&cid='+cid) as a:
        message(a,'welcome');d=c.post('/meet/minutes/start',headers=headers(cid)).json();first=message(a,'minutes')
        with c.websocket_connect('/meet/ws/test-room?name=Anna&kode=test&cid='+'cd'*16) as b:
            message(b,'welcome');notice=message(b,'minutes');assert notice['phase']=='recording' and notice['epoch']>first['epoch'] and notice['partial']
            assert c.post('/meet/minutes/chunk/0',headers={**headers(cid),'X-Minutes-Epoch':str(first['epoch'])},content=wav()).status_code==409
            # A wrong reader token must never stop/delete a live recording.
            assert c.delete('/meet/minutes/report/'+d['id'],headers={'X-Minutes-Token':'ff'*32}).status_code==403
            assert r.minutes_meet.states['test-room']['phase']=='recording'
            assert c.post('/meet/minutes/finish',headers=headers('cd'*16)).status_code==200
            assert r.minutes_meet.states['test-room']['partial']


def test_summary_payload_no_search_no_voice_no_stored_response(monkeypatch):
    monkeypatch.setenv('OPENAI_API_KEY','not-real');calls=[]
    async def transport(request):
        data=json.loads(request.content);calls.append(data)
        assert data['store'] is False and 'tools' not in data
        assert 'kildemateriale' in data['instructions'] and 'forslag fra vedtak' in data['instructions']
        return httpx.Response(200,json={'status':'completed','output':[{'type':'message','content':[{'type':'output_text','text':'Utkast'}]}]})
    real=httpx.AsyncClient
    monkeypatch.setattr(minutes_bridge.httpx,'AsyncClient',lambda **kw:real(transport=httpx.MockTransport(transport),**kw))
    assert asyncio.run(minutes_bridge.summarize('Vi foreslår en skriver.',['Robert'],False))=='Utkast'
    assert len(calls)==1
