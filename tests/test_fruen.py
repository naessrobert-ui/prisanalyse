import hashlib
import io
import json
import secrets
import time

import httpx
import pytest
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient
from PIL import Image

from familie.server import create_app,code_hash
from familie.fruen import COOKIE,ADMIN

HEAD={'X-Fruen-Request':'1'}
CODE='test-fruen-administrator'


@pytest.fixture
def fruen(tmp_path):
    salt=secrets.token_hex(16)
    (tmp_path/'access.json').write_text(json.dumps({'salt':salt,'code_hash':code_hash('familie-test-kode',salt),'session_key':secrets.token_hex(32)}))
    salt=secrets.token_bytes(16)
    (tmp_path/'fruen-admin.json').write_text(json.dumps({'salt':salt.hex(),'hash':hashlib.scrypt(CODE.encode(),salt=salt,n=16384,r=8,p=1).hex()}))
    app=create_app(tmp_path,secure_cookie=False)
    with TestClient(app,base_url='http://localhost') as admin:
        response=admin.post('/fruen/api/admin/login',headers=HEAD,json={'code':CODE});assert response.status_code==200
        yield app,admin,tmp_path


def enrol(app,admin,name='Robert',kind='member'):
    invite=admin.post('/fruen/api/admin/invite',headers=HEAD,json={'name':name,'kind':kind});assert invite.status_code==200
    c=TestClient(app,base_url='http://localhost');r=c.post('/fruen/api/enrol',headers=HEAD,json={'token':invite.json()['token']});assert r.status_code==200
    return c,r.json(),invite.json()['token']


def ready(c):return c.post('/fruen/api/state',headers=HEAD,json={'ready':True,'after':0})


def post(c,path,data=None):return c.post('/fruen/api/'+path,headers=HEAD,json=data or {})


def test_separate_membership_single_use_and_privacy(fruen):
    app,admin,root=fruen;stranger=TestClient(app,base_url='http://localhost')
    assert stranger.get('/fruen').status_code==200
    assert stranger.get('/fruen/api/photos').status_code==401
    assert stranger.post('/familie/api/login',headers={'X-Family-Request':'1'},json={'code':'familie-test-kode','name':'Familie'}).status_code==200
    assert stranger.get('/fruen/api/me').status_code==401
    c,person,token=enrol(app,admin)
    assert post(stranger,'enrol',{'token':token}).status_code==401
    assert c.post('/fruen/api/state',json={'ready':True}).status_code==403
    assert c.get('/fruen/api/admin/accounts').status_code==401
    assert c.get('/familie/api/items').status_code==401
    result=c.get('/fruen/api/me');assert result.json()['name']=='Robert' and 'token' not in result.text
    assert result.headers['Cache-Control']=='private, no-store'
    assert 'frame-ancestors' in result.headers['Content-Security-Policy']
    c.close();stranger.close()


def test_only_photos_and_videos_shared_and_revocation_restricts_media(fruen):
    app,admin,root=fruen;c,a,_=enrol(app,admin)
    image=io.BytesIO();Image.new('RGB',(5,5),'red').save(image,format='PNG')
    admin.post('/familie/api/login',headers={'X-Family-Request':'1'},json={'code':'familie-test-kode','name':'Familie'})
    uploaded=admin.post('/familie/api/items',params={'days':1,'caption':'Hei'},headers={'X-Family-Request':'1','Content-Type':'image/png'},content=image.getvalue());ident=uploaded.json()['id']
    items=c.get('/fruen/api/photos').json()['items'];assert len(items)==1 and items[0]['id']==ident
    media=c.get('/fruen/media/'+ident);assert media.status_code==200 and 'image/png' in media.headers['content-type']
    assert c.get('/fruen/media/'+ident,headers={'Range':'bytes=0-9'}).status_code==206
    assert c.get('/fruen/media/'+ident,headers={'If-None-Match':media.headers['etag']}).status_code==304
    admin.post('/fruen/api/admin/revoke/'+a['id'],headers=HEAD,json={})
    assert c.get('/fruen/media/'+ident,headers={'If-None-Match':media.headers['etag']}).status_code==401
    c.close()


def test_automatic_answer_notice_authorization_and_end(fruen):
    app,admin,root=fruen;caller,a,_=enrol(app,admin);station,b,_=enrol(app,admin,'Fruen','auto');third,_,_=enrol(app,admin,'Annen')
    ready(caller);ready(station);ready(third)
    assert post(station,'call',{'to':a['id']}).status_code==403
    r=post(caller,'call',{'to':b['id']});assert r.status_code==200;ident=r.json()['id']
    assert post(station,'call/'+ident+'/accept').status_code==409
    assert post(third,'call/'+ident+'/accept').status_code==404
    assert post(third,'call',{'to':b['id']}).status_code==409
    import sqlite3
    with sqlite3.connect(root/'posts.sqlite3') as db:db.execute('UPDATE fruen_calls SET created=? WHERE id=?',(time.time()-6,ident))
    assert post(station,'call/'+ident+'/accept').status_code==200
    assert post(caller,'call/'+ident+'/signal',{'kind':'offer','data':{'type':'offer','sdp':'test-sdp'}}).status_code==200
    state=ready(station).json()['call'];assert state['peer_name']=='Robert' and state['signals'][0]['kind']=='offer'
    assert post(third,'call/'+ident+'/signal',{'kind':'ice','data':{'candidate':'test'}}).status_code==404
    assert post(station,'call/'+ident+'/signal',{'kind':'offer','data':{'type':'offer','sdp':'test'}}).status_code==403
    assert post(caller,'call/'+ident+'/end').status_code==200
    assert ready(station).json()['call']['status']=='ended'
    caller.close();station.close();third.close()


def test_manual_answer_both_directions_and_offline(fruen):
    app,admin,root=fruen;a,one,_=enrol(app,admin,'A');b,two,_=enrol(app,admin,'B')
    assert post(a,'call',{'to':two['id']}).status_code==409
    ready(a);ready(b)
    ident=post(a,'call',{'to':two['id']}).json()['id'];assert post(b,'call/'+ident+'/accept').status_code==200;post(b,'call/'+ident+'/end')
    ident=post(b,'call',{'to':one['id']}).json()['id'];assert post(a,'call/'+ident+'/accept').status_code==200
    post(a,'state',{'ready':False,'after':0});assert ready(b).json()['call']['status']=='ended'
    a.close();b.close()


def test_revoke_active_call_and_signal_expiry(fruen):
    app,admin,root=fruen;a,one,_=enrol(app,admin,'A');b,two,_=enrol(app,admin,'B');ready(a);ready(b)
    ident=post(a,'call',{'to':two['id']}).json()['id'];post(b,'call/'+ident+'/accept')
    admin.post('/fruen/api/admin/revoke/'+one['id'],headers=HEAD,json={})
    assert ready(a).status_code==401;assert ready(b).json()['call']['status']=='ended'
    a.close();b.close()


def test_proxy_forwards_only_fruen_cookies_and_authenticated_ice(monkeypatch):
    import familie_proxy,meet_routes
    monkeypatch.setenv('FAMILY_ORIGIN','https://example.ts.net:8443');requests=[]
    class Body(httpx.AsyncByteStream):
        async def __aiter__(self):yield b'{}'
    def transport(request):
        requests.append(request)
        assert 'motes_family_session' not in request.headers.get('cookie','') and 'irrelevant' not in request.headers.get('cookie','')
        if request.url.path.endswith('/me'):return httpx.Response(200,json={'kind':'member'})
        return httpx.Response(200,stream=Body())
    real=httpx.AsyncClient
    monkeypatch.setattr(familie_proxy.httpx,'AsyncClient',lambda **kw:real(transport=httpx.MockTransport(transport),**kw))
    async def ice(**kwargs):return JSONResponse({'iceServers':[],'turn':True})
    monkeypatch.setattr(meet_routes,'meet_ice',ice)
    app=FastAPI();app.include_router(familie_proxy.router)
    with TestClient(app) as c:
        assert c.get('/fruen/api/ice').status_code==401
        c.cookies.set(COOKIE,'ab'*32);c.cookies.set(ADMIN,'cd'*32);c.cookies.set('motes_family_session','private-family');c.cookies.set('irrelevant','secret')
        assert c.get('/fruen/api/ice').json()['turn'] is True
        assert c.post('/fruen/api/state',headers=HEAD,json={'ready':True}).status_code==200
        assert requests[-1].headers['x-fruen-request']=='1'
        assert 'motes_fruen_admin' in requests[-1].headers['cookie']


def test_expired_invitation_and_wrong_admin_code(fruen):
    app,admin,root=fruen
    token=admin.post('/fruen/api/admin/invite',headers=HEAD,json={'name':'Sen','kind':'member'}).json()['token']
    import sqlite3
    with sqlite3.connect(root/'posts.sqlite3') as db:db.execute('UPDATE fruen_invites SET expires=?',(time.time()-1,))
    c=TestClient(app,base_url='http://localhost')
    r=post(c,'enrol',{'token':token});assert r.status_code==401 and 'utløpt' in r.json()['detail']
    assert c.get('/fruen/api/me').status_code==401
    assert c.post('/fruen/api/admin/login',headers=HEAD,json={'code':'feil-kode-som-er-lang'}).status_code==401
    assert c.get('/fruen/api/admin/accounts').status_code==401
    c.close()


def test_night_mode_blocks_calls_to_frame_only(fruen,monkeypatch):
    import familie.fruen as fruen_module
    from datetime import datetime
    from zoneinfo import ZoneInfo
    oslo=ZoneInfo('Europe/Oslo')
    at=lambda h,m=0:datetime(2026,11,15,h,m,tzinfo=oslo).timestamp()
    assert fruen_module.is_night(at(22)) and fruen_module.is_night(at(3)) and fruen_module.is_night(at(6,59))
    assert not fruen_module.is_night(at(7)) and not fruen_module.is_night(at(21,59))
    app,admin,root=fruen;a,one,_=enrol(app,admin,'A');b,two,_=enrol(app,admin,'B');f,frame,_=enrol(app,admin,'Fruen','auto')
    ready(a);ready(b);ready(f)
    monkeypatch.setattr(fruen_module,'night_override',True)
    state=ready(f).json();assert state['night'] is True
    contact=next(x for x in ready(a).json()['contacts'] if x['id']==frame['id'])
    assert contact['sleeping'] is True and contact['online'] is False
    r=post(a,'call',{'to':frame['id']});assert r.status_code==409 and 'sover' in r.json()['detail']
    assert ready(a).json()['night'] is False
    assert post(a,'call',{'to':two['id']}).status_code==200  # family members can still call each other
    assert any(x['sleeping'] for x in admin.get('/fruen/api/admin/accounts').json()['accounts'])
    monkeypatch.setattr(fruen_module,'night_override',False)
    assert ready(f).json()['night'] is False
    a.close();b.close();f.close()


def test_end_reason_reaches_the_other_side(fruen):
    import sqlite3
    app,admin,root=fruen;a,one,_=enrol(app,admin,'A');f,frame,_=enrol(app,admin,'Fruen','auto')
    ready(a);ready(f)
    ident=post(a,'call',{'to':frame['id']}).json()['id']
    with sqlite3.connect(root/'posts.sqlite3') as db:db.execute('UPDATE fruen_calls SET created=created-6')
    assert post(f,'call/'+ident+'/accept').status_code==200
    assert post(f,'call/'+ident+'/end',{'reason':'camera'}).status_code==200
    call=ready(a).json()['call']
    assert call['status']=='ended' and call['reason']=='callee:camera' and call['peer_kind']=='auto' and call['outgoing'] is True
    assert post(a,'call/'+ident+'/end',{'reason':'hangup'}).status_code==200
    assert ready(a).json()['call']['reason']=='callee:camera'  # first reason is kept
    ident=post(a,'call',{'to':frame['id']}).json()['id']
    assert post(a,'call/'+ident+'/end',{'reason':'<script>'}).status_code==200
    assert ready(f).json()['call']['reason']=='caller:hangup'  # unknown codes are not stored
    ready(f);ident=post(a,'call',{'to':frame['id']}).json()['id']
    with sqlite3.connect(root/'posts.sqlite3') as db:db.execute('UPDATE fruen_calls SET created=created-100 WHERE id!=?',(ident,));db.execute('UPDATE fruen_calls SET created=created-50 WHERE id=?',(ident,))
    assert ready(a).json()['call']['reason']=='timeout:noanswer'
    a.close();f.close()
