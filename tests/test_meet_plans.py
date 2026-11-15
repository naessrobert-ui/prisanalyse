import asyncio
import importlib
from datetime import datetime,timedelta,timezone
import json
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

import meet_plans as plans

HEAD={'X-Meet-Request':'1'}


def payload(**changes):
    start=datetime.now(timezone.utc)+timedelta(days=15)
    d={'title':'Styremøte æøå','name':'Robert','local_start':start.strftime('%Y-%m-%dT12:30'),'timezone':'Europe/Oslo','duration':60,'agenda':'Sak 1: budsjett\nSak 2: fordeling'}
    return d|changes


@pytest.fixture
def app(monkeypatch):
    monkeypatch.setattr(plans,'_store',plans.MemoryStore());plans._rates.clear()
    monkeypatch.setenv('NAM_MEET_KODE','test-code')
    import meet_routes
    importlib.reload(meet_routes)
    a=FastAPI();a.include_router(meet_routes.router)
    with TestClient(a,base_url='http://localhost') as c:yield c,meet_routes


def create(c,**kw):
    response=c.post('/meet/plans',headers=HEAD,json=payload(**kw));assert response.status_code==200,response.text;return response.json()


def test_plan_is_durable_and_links_separate(app):
    c,r=app;p=create(c)
    assert len(p['room'])==25 and p['inv'].startswith('p.')
    assert p['owner'] not in p['join_url'] and p['owner'] in p['manage_url']
    assert 'owner_hash' not in p
    assert c.get('/meet/plans/'+p['room']).status_code==403
    assert c.get('/meet/plans/'+p['room'],headers={'X-Plan-Owner':'ab'*32}).status_code==403
    h={'X-Plan-Invite':p['inv']}
    assert c.get('/meet/plans/'+p['room'],headers=h).json()['title']==p['title']
    # Reload signaling module as on a deploy: durable store remains authoritative.
    importlib.reload(r)
    assert asyncio.run(r._meet_access('',p['inv'],p['room']))
    assert not asyncio.run(r._meet_access('test-code','',p['room']))
    with c.websocket_connect('/meet/ws/'+p['room']+'?name=Early&inv='+p['inv']+'&cid='+'ab'*16) as ws:
        assert ws.receive_json()['t']=='welcome'


def test_edit_preserves_guest_link_updates_calendar_and_owner_only(app):
    c,r=app;p=create(c);path='/meet/plans/'+p['room']
    body=payload(title='Flyttet møte',duration=90,version=p['version'])
    assert c.put(path,headers={**HEAD,'X-Plan-Invite':p['inv']},json=body).status_code==403
    edited=c.put(path,headers={**HEAD,'X-Plan-Owner':p['owner']},json=body)
    assert edited.status_code==200
    v=edited.json();assert v['join_url']==p['join_url'] and v['version']==1
    assert c.put(path,headers={**HEAD,'X-Plan-Owner':p['owner']},json=body).status_code==409
    assert c.get(path,headers={'X-Plan-Invite':p['inv']}).json()['title']=='Flyttet møte'


def test_cancel_revokes_new_entry_and_calendar_cancellation(app):
    c,r=app;p=create(c);path='/meet/plans/'+p['room']
    assert c.post(path+'/cancel',headers={**HEAD,'X-Plan-Invite':p['inv']},json={}).status_code==403
    assert c.post(path+'/cancel',headers={**HEAD,'X-Plan-Owner':p['owner']},json={}).json()['cancelled']
    assert not asyncio.run(r._meet_access('',p['inv'],p['room']))
    assert c.get('/meet/invite',params={'rom':p['room'],'inv':p['inv']}).status_code==401
    ics=c.post(path+'/calendar',headers={**HEAD,'X-Plan-Invite':p['inv']},json={}).content
    assert b'METHOD:CANCEL\r\n' in ics and b'STATUS:CANCELLED' in ics and b'BEGIN:VALARM' not in ics
    with pytest.raises(WebSocketDisconnect):
        with c.websocket_connect('/meet/ws/'+p['room']+'?inv='+p['inv']) as ws:ws.receive_json()
    assert c.request('DELETE',path,headers={**HEAD,'X-Plan-Owner':p['owner']},json={}).status_code==200
    assert c.get(path,headers={'X-Plan-Owner':p['owner']}).status_code==404


def test_calendar_rfc_datetime_folding_escaping_and_no_owner_secret(app):
    c,r=app;p=create(c,title='Æøå '*25,agenda='Første; sak, og \\ tall\nBEGIN:VEVENT\n'+('Økonomi og utvikling '*80))
    data=c.post('/meet/plans/'+p['room']+'/calendar',headers={**HEAD,'X-Plan-Invite':p['inv']},json={})
    assert data.status_code==200 and 'text/calendar' in data.headers['content-type']
    text=data.content.decode();assert '\r\n' in text
    assert all(len(line.encode())<=75 for line in text.split('\r\n'))
    unfolded=text.replace('\r\n ','')
    assert unfolded.count('\r\nBEGIN:VEVENT\r\n')==1
    assert 'Første\\; sak\\, og \\\\ tall\\nBEGIN:VEVENT' in unfolded
    assert 'DTSTART:'+datetime.fromisoformat(p['start']).strftime('%Y%m%dT%H%M%SZ') in unfolded
    assert p['owner'] not in unfolded and p['join_url'] in unfolded
    assert 'UID:'+p['room']+'@motes.no' in unfolded


@pytest.mark.parametrize('changes',[{'duration':True},{'duration':481},{'title':''},{'title':'a'*121},{'timezone':'not/a/zone'},{'local_start':'2020-01-01T10:00'},{'agenda':'x'*4001}])
def test_invalid_inputs_and_csrf(app,changes):
    c,r=app
    assert c.post('/meet/plans',json=payload()).status_code==403
    assert c.post('/meet/plans',headers=HEAD,json=payload(**changes)).status_code==400


@pytest.mark.parametrize('start,needle',[('2027-03-28T02:30','finnes ikke'),('2027-10-31T02:30','to ganger')])
def test_dst_errors_are_explicit(app,start,needle):
    c,r=app;v=c.post('/meet/plans',headers=HEAD,json=payload(local_start=start));assert v.status_code==400 and needle in v.json()['detail']


def test_store_compare_and_swap_prevents_concurrent_overwrite():
    s=plans.MemoryStore();s.put('a',{'version':0});_,tag=s.get('a')
    def write(_):
        try:s.put('a',{'version':1},tag);return True
        except plans.Conflict:return False
    with ThreadPoolExecutor(2) as pool:assert sum(pool.map(write,[1,2]))==1


def test_s3_refuses_public_storage_and_enforces_private_prefix(monkeypatch):
    import boto3
    monkeypatch.setenv('MEET_PLANS_BUCKET','test-private-bucket')
    class Body:
        def read(self,n):return b'{"version":0}'
        def close(self):pass
    class S3:
        private=False
        calls=[]
        def get_public_access_block(self,**kw):return {'PublicAccessBlockConfiguration':{k:self.private for k in ['BlockPublicAcls','IgnorePublicAcls','BlockPublicPolicy','RestrictPublicBuckets']}}
        def put_object(self,**kw):self.calls.append(kw)
        def get_object(self,**kw):self.calls.append(kw);return {'Body':Body(),'ETag':'"test-etag"'}
        def delete_object(self,**kw):self.calls.append(kw)
    client=S3();monkeypatch.setattr(boto3,'client',lambda *a,**kw:client)
    store=plans.S3Store()
    with pytest.raises(plans.Unavailable):store.ready()
    client.private=True;store.ready();store.put('plan-'+'a'*20,{'version':0})
    assert client.calls[-1]['IfNoneMatch']=='*' and client.calls[-1]['ServerSideEncryption']=='AES256'
    assert client.calls[-1]['Key'].startswith('motes/plans/')
    store.put('plan-'+'a'*20,{'version':1},'"test-etag"');assert client.calls[-1]['IfMatch']=='"test-etag"'


def test_invite_expires_after_end_not_after_creation(app):
    c,r=app;p=create(c)
    value,_=plans.storage().get(p['room']);assert datetime.fromisoformat(value['start'])>datetime.now(timezone.utc)+timedelta(days=1)
    assert asyncio.run(r._meet_access('',p['inv'],p['room']))
    value['end']=(datetime.now(timezone.utc)-timedelta(hours=25)).isoformat()
    plans.storage().put(p['room'],value,'0')
    assert not asyncio.run(r._meet_access('',p['inv'],p['room']))
