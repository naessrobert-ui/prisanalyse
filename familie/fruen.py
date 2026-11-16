"""Private photo frames and explicitly enrolled family visitors. No media recording."""
import asyncio
import hashlib
import hmac
import json
import re
import secrets
import time
from datetime import datetime
from pathlib import Path

from fastapi import HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse

COOKIE='motes_fruen_session'
ADMIN='motes_fruen_admin'
NIGHT=(21,8)  # local hours in Norway: the automatic frame sleeps and takes no calls
night_override=None  # tests only


def is_night(now=None):
    if night_override is not None:return night_override
    try:
        from zoneinfo import ZoneInfo
        hour=datetime.fromtimestamp(now or time.time(),ZoneInfo('Europe/Oslo')).hour
    except Exception:
        hour=datetime.fromtimestamp(now or time.time()).hour
    start,end=NIGHT
    return (hour>=start or hour<end) if start>end else start<=hour<end


def register(app,root,db,media_response,secure_cookie):
    attempts=[]
    with db() as c:
        c.executescript('''
        CREATE TABLE IF NOT EXISTS fruen_accounts (
          id TEXT PRIMARY KEY,name TEXT NOT NULL,kind TEXT NOT NULL,token TEXT NOT NULL,
          expires REAL NOT NULL,active INTEGER NOT NULL DEFAULT 1,seen REAL NOT NULL DEFAULT 0,ready INTEGER NOT NULL DEFAULT 0);
        CREATE TABLE IF NOT EXISTS fruen_invites (
          token TEXT PRIMARY KEY,name TEXT NOT NULL,kind TEXT NOT NULL,expires REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS fruen_admin_sessions (token TEXT PRIMARY KEY,expires REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS fruen_calls (
          id TEXT PRIMARY KEY,caller TEXT NOT NULL,callee TEXT NOT NULL,status TEXT NOT NULL,
          created REAL NOT NULL,accepted REAL,ended REAL,reason TEXT NOT NULL DEFAULT '');
        CREATE TABLE IF NOT EXISTS fruen_signals (
          seq INTEGER PRIMARY KEY AUTOINCREMENT,call_id TEXT NOT NULL,sender TEXT NOT NULL,recipient TEXT NOT NULL,kind TEXT NOT NULL,data TEXT NOT NULL);
        CREATE INDEX IF NOT EXISTS fruen_call_state ON fruen_calls(status,caller,callee);
        ''')
        if 'reload' not in {r[1] for r in c.execute('PRAGMA table_info(fruen_accounts)')}:
            c.execute('ALTER TABLE fruen_accounts ADD COLUMN reload INTEGER NOT NULL DEFAULT 0')

    def hashed(value):return hashlib.sha256(value.encode()).hexdigest()

    def account(request):
        token=request.cookies.get(COOKIE,'')
        if not re.fullmatch('[0-9a-f]{64}',token):raise HTTPException(401,'Denne enheten må inviteres først.')
        with db() as c:r=c.execute('SELECT * FROM fruen_accounts WHERE token=? AND active=1 AND expires>?',(hashed(token),time.time())).fetchone()
        if not r:raise HTTPException(401,'Tilgangen er avsluttet. Be administrator om en ny invitasjon.')
        return dict(r)

    def admin(request):
        token=request.cookies.get(ADMIN,'')
        if not re.fullmatch('[0-9a-f]{64}',token):raise HTTPException(401,'Logg inn som administrator.')
        with db() as c:r=c.execute('SELECT 1 FROM fruen_admin_sessions WHERE token=? AND expires>?',(hashed(token),time.time())).fetchone()
        if not r:raise HTTPException(401,'Logg inn som administrator.')

    async def body(request,limit=70000):
        if request.headers.get('x-fruen-request')!='1':raise HTTPException(403,'Åpne besøksiden og prøv igjen.')
        data=bytearray()
        async for part in request.stream():
            data.extend(part)
            if len(data)>limit:raise HTTPException(413,'For stor forespørsel.')
        try:r=json.loads(data or b'{}')
        except ValueError:raise HTTPException(400,'Ugyldig innhold.')
        if not isinstance(r,dict):raise HTTPException(400,'Ugyldig innhold.')
        return r

    def cleanup():
        now=time.time()
        with db() as c:
            c.execute("UPDATE fruen_calls SET status='ended',ended=?,reason=CASE WHEN status='ringing' AND created<? THEN 'timeout:noanswer' ELSE 'timeout:connection' END WHERE status!='ended' AND (created<? OR (status='ringing' AND created<?) OR caller IN (SELECT id FROM fruen_accounts WHERE active=0 OR expires<? OR seen<? OR ready=0) OR callee IN (SELECT id FROM fruen_accounts WHERE active=0 OR expires<? OR seen<? OR ready=0))",(now,now-45,now-3600,now-45,now,now-15,now,now-15))
            c.execute('DELETE FROM fruen_signals WHERE call_id IN (SELECT id FROM fruen_calls WHERE ended<?)',(now-60,))
            c.execute('DELETE FROM fruen_calls WHERE ended<?',(now-60,))
            c.execute('DELETE FROM fruen_invites WHERE expires<?',(now,))
            c.execute('DELETE FROM fruen_admin_sessions WHERE expires<?',(now,))
    app.state.fruen_cleanup=cleanup

    @app.get('/fruen',response_class=HTMLResponse)
    @app.get('/fruen/',response_class=HTMLResponse)
    @app.get('/fruen/klargjor',response_class=HTMLResponse)
    async def page():return HTMLResponse(Path(__file__).with_name('fruen.html').read_text())

    @app.get('/fruen/admin',response_class=HTMLResponse)
    async def admin_page():return HTMLResponse(Path(__file__).with_name('fruen-admin.html').read_text())

    @app.get('/fruen/app.js')
    async def js():
        from fastapi.responses import Response
        return Response(Path(__file__).with_name('fruen.js').read_text(),media_type='text/javascript')

    @app.get('/fruen/api/health')
    async def health():return {'ok':True,'configured':(root/'fruen-admin.json').is_file()}

    @app.post('/fruen/api/admin/login')
    async def login(request:Request):
        d=await body(request,2048);now=time.time()
        attempts[:]=[t for t in attempts if t>now-900]
        if len(attempts)>=10:raise HTTPException(429,'Vent 15 minutter før nytt forsøk.')
        try:config=json.loads((root/'fruen-admin.json').read_text())
        except FileNotFoundError:raise HTTPException(503,'Administrator må settes opp på Pi-en først.')
        attempts.append(now)
        code=d.get('code','')
        if not isinstance(code,str) or not 12<=len(code)<=200:raise HTTPException(401,'Feil administratorkode.')
        actual=await asyncio.to_thread(hashlib.scrypt,code.encode(),salt=bytes.fromhex(config['salt']),n=16384,r=8,p=1)
        if not hmac.compare_digest(actual.hex(),config['hash']):raise HTTPException(401,'Feil administratorkode.')
        attempts.clear();token=secrets.token_hex(32)
        with db() as c:c.execute('INSERT INTO fruen_admin_sessions VALUES(?,?)',(hashed(token),now+28800))
        response=JSONResponse({'ok':True});response.set_cookie(ADMIN,token,max_age=28800,secure=secure_cookie,httponly=True,samesite='strict',path='/fruen');return response

    @app.post('/fruen/api/admin/logout')
    async def admin_logout(request:Request):
        await body(request);admin(request)
        with db() as c:c.execute('DELETE FROM fruen_admin_sessions WHERE token=?',(hashed(request.cookies[ADMIN]),))
        response=JSONResponse({'ok':True});response.delete_cookie(ADMIN,path='/fruen');return response

    @app.get('/fruen/api/admin/accounts')
    async def accounts(request:Request):
        admin(request);cleanup()
        with db() as c:rows=c.execute('SELECT id,name,kind,expires,active,seen,ready FROM fruen_accounts ORDER BY name').fetchall()
        night=is_night()
        return {'accounts':[dict(r,sleeping=r['kind']=='auto' and night) for r in rows]}

    @app.post('/fruen/api/admin/invite')
    async def invite(request:Request):
        admin(request);d=await body(request,2048)
        name=str(d.get('name','')).strip()[:50];kind=d.get('kind')
        if not name or kind not in {'member','auto'}:raise HTTPException(400,'Velg navn og enhetstype.')
        token=secrets.token_hex(32)
        with db() as c:
            c.execute('DELETE FROM fruen_invites WHERE expires<?',(time.time(),))
            if c.execute('SELECT COUNT(*) FROM fruen_accounts WHERE active=1').fetchone()[0]>=30:raise HTTPException(429,'Maks 30 enheter. Fjern gamle enheter først.')
            c.execute('INSERT INTO fruen_invites VALUES(?,?,?,?)',(hashed(token),name,kind,time.time()+1800))
        return {'token':token,'expires_in':1800}

    @app.post('/fruen/api/admin/reload/{ident}')
    async def request_reload(ident:str,request:Request):
        # Lets a relative reload the page on a device from afar, e.g. the frame at her home.
        admin(request);await body(request)
        with db() as c:
            if not c.execute('UPDATE fruen_accounts SET reload=1 WHERE id=? AND active=1',(ident,)).rowcount:raise HTTPException(404,'Fant ikke enheten.')
        return {'ok':True}

    @app.post('/fruen/api/admin/revoke/{ident}')
    async def revoke(ident:str,request:Request):
        admin(request);await body(request)
        with db() as c:
            c.execute('UPDATE fruen_accounts SET active=0,ready=0 WHERE id=?',(ident,))
            c.execute("UPDATE fruen_calls SET status='ended',ended=?,reason='Tilgang avsluttet' WHERE status!='ended' AND (caller=? OR callee=?)",(time.time(),ident,ident))
        return {'ok':True}

    @app.post('/fruen/api/enrol')
    async def enrol(request:Request):
        d=await body(request,2048);token=d.get('token','')
        if not isinstance(token,str) or not re.fullmatch('[0-9a-f]{64}',token):raise HTTPException(401,'Ugyldig invitasjon.')
        now=time.time();session=secrets.token_hex(32);ident=secrets.token_hex(16)
        with db() as c:
            c.execute('BEGIN IMMEDIATE')
            r=c.execute('SELECT * FROM fruen_invites WHERE token=? AND expires>?',(hashed(token),now)).fetchone()
            if not r:raise HTTPException(401,'Invitasjonen er brukt eller utløpt.')
            if c.execute('SELECT COUNT(*) FROM fruen_accounts WHERE active=1').fetchone()[0]>=30:raise HTTPException(429,'Maks 30 aktive enheter.')
            c.execute('DELETE FROM fruen_invites WHERE token=?',(hashed(token),))
            c.execute('INSERT INTO fruen_accounts(id,name,kind,token,expires) VALUES(?,?,?,?,?)',(ident,r['name'],r['kind'],hashed(session),now+365*86400))
        response=JSONResponse({'id':ident,'name':r['name'],'kind':r['kind']});response.set_cookie(COOKIE,session,max_age=365*86400,secure=secure_cookie,httponly=True,samesite='strict',path='/fruen');return response

    @app.get('/fruen/api/me')
    async def me(request:Request):
        a=account(request);return {k:a[k] for k in ('id','name','kind','expires')}

    @app.post('/fruen/api/logout')
    async def logout(request:Request):
        await body(request);a=account(request)
        with db() as c:
            c.execute('UPDATE fruen_accounts SET ready=0 WHERE id=?',(a['id'],))
            c.execute("UPDATE fruen_calls SET status='ended',ended=?,reason='Logget ut' WHERE status!='ended' AND (caller=? OR callee=?)",(time.time(),a['id'],a['id']))
        response=JSONResponse({'ok':True});response.delete_cookie(COOKIE,path='/fruen');return response

    @app.get('/fruen/api/photos')
    async def photos(request:Request,offset:int=0):
        account(request)
        if offset<0 or offset>100000:raise HTTPException(400,'Ugyldig side.')
        with db() as c:rows=c.execute("SELECT id,mime,caption,created,expires FROM posts WHERE (expires=0 OR expires>?) AND (mime LIKE 'image/%' OR mime LIKE 'video/%') ORDER BY created DESC,id LIMIT 201 OFFSET ?",(time.time(),offset)).fetchall()
        return {'items':[dict(r) for r in rows[:200]],'more':len(rows)>200}

    @app.api_route('/fruen/media/{ident}',methods=['GET','HEAD'])
    async def media(ident:str,request:Request):
        account(request)
        with db() as c:r=c.execute('SELECT mime FROM posts WHERE id=?',(ident,)).fetchone()
        if not r or not r['mime'].startswith(('image/','video/')):raise HTTPException(404)
        return await media_response(ident,request)

    @app.post('/fruen/api/state')
    async def state(request:Request):
        a=account(request);d=await body(request,2048)
        now=time.time()
        with db() as c:
            c.execute('UPDATE fruen_accounts SET seen=?,ready=? WHERE id=?',(now,int(d.get('ready') is True),a['id']))
            reload=bool(c.execute('UPDATE fruen_accounts SET reload=0 WHERE id=? AND reload=1',(a['id'],)).rowcount)
        cleanup()
        with db() as c:
            contacts=c.execute('SELECT id,name,kind,seen,ready FROM fruen_accounts WHERE active=1 AND expires>? AND id!=? ORDER BY name',(now,a['id'])).fetchall()
            call=c.execute("SELECT * FROM fruen_calls WHERE (caller=? OR callee=?) ORDER BY created DESC LIMIT 1",(a['id'],a['id'])).fetchone()
            result=None
            if call and (call['status']!='ended' or call['ended']>now-30):
                r=dict(call);other=r['callee'] if r['caller']==a['id'] else r['caller'];peer=c.execute('SELECT name,kind FROM fruen_accounts WHERE id=?',(other,)).fetchone()
                r.update(peer_name=peer['name'] if peer else 'Familien',peer_kind=peer['kind'] if peer else 'member',outgoing=r['caller']==a['id'])
                after=d.get('after',0)
                if not isinstance(after,int) or after<0:raise HTTPException(400,'Ugyldig signalposisjon.')
                r['signals']=[{'seq':s['seq'],'kind':s['kind'],'data':json.loads(s['data'])} for s in c.execute('SELECT * FROM fruen_signals WHERE call_id=? AND recipient=? AND seq>? ORDER BY seq',(r['id'],a['id'],after))]
                result=r
        night=is_night(now)
        return {'contacts':[{'id':r['id'],'name':r['name'],'kind':r['kind'],'sleeping':r['kind']=='auto' and night,'online':bool(r['ready'] and r['seen']>now-10) and not (r['kind']=='auto' and night)} for r in contacts],'call':result,'night':a['kind']=='auto' and night,'reload':reload}

    @app.post('/fruen/api/call')
    async def ring(request:Request):
        a=account(request);d=await body(request,2048);cleanup();now=time.time()
        if a['kind']!='member':raise HTTPException(403,'Denne bildeskjermen starter ikke anrop.')
        target=d.get('to')
        if not isinstance(target,str) or not re.fullmatch('[0-9a-f]{32}',target):raise HTTPException(400,'Velg en godkjent enhet.')
        with db() as c:
            c.execute('BEGIN IMMEDIATE')
            peer=c.execute('SELECT * FROM fruen_accounts WHERE id=? AND active=1 AND expires>?',(target,now)).fetchone()
            if peer and peer['kind']=='auto' and is_night(now):raise HTTPException(409,peer['name']+' sover nå (nattmodus 21–08).')
            if not peer or peer['id']==a['id'] or not peer['ready'] or peer['seen']<now-10 or not a['ready'] or a['seen']<now-10:raise HTTPException(409,'Enheten er ikke klar for besøk.')
            if c.execute("SELECT 1 FROM fruen_calls WHERE status!='ended' AND (caller IN (?,?) OR callee IN (?,?))",(a['id'],target,a['id'],target)).fetchone():raise HTTPException(409,'En av dere er allerede i en samtale.')
            ident=secrets.token_hex(16);c.execute("INSERT INTO fruen_calls(id,caller,callee,status,created) VALUES(?,?,?,'ringing',?)",(ident,a['id'],target,now))
        return {'id':ident}

    def active_call(c,ident,a):
        r=c.execute('SELECT * FROM fruen_calls WHERE id=? AND (caller=? OR callee=?)',(ident,a['id'],a['id'])).fetchone()
        if not r:raise HTTPException(404,'Fant ikke samtalen.')
        return r

    @app.post('/fruen/api/call/{ident}/accept')
    async def accept(ident:str,request:Request):
        a=account(request);await body(request,2048);cleanup()
        with db() as c:
            c.execute('BEGIN IMMEDIATE');r=active_call(c,ident,a)
            if r['callee']!=a['id'] or r['status']!='ringing':raise HTTPException(409,'Samtalen kan ikke besvares.')
            if a['kind']=='auto' and time.time()<r['created']+5:raise HTTPException(409,'Venter på lydvarselet.')
            c.execute("UPDATE fruen_calls SET status='accepted',accepted=? WHERE id=?",(time.time(),ident))
        return {'ok':True}

    @app.post('/fruen/api/call/{ident}/end')
    async def end(ident:str,request:Request):
        # The reason is shown to the other side, e.g. that the frame could not start its camera.
        a=account(request);d=await body(request,2048)
        code=d.get('reason') if d.get('reason') in {'hangup','declined','camera','connection','hidden'} else 'hangup'
        with db() as c:
            r=active_call(c,ident,a)
            if r['status']!='ended':
                c.execute("UPDATE fruen_calls SET status='ended',ended=?,reason=? WHERE id=?",(time.time(),('caller:' if r['caller']==a['id'] else 'callee:')+code,ident))
        return {'ok':True}

    @app.post('/fruen/api/call/{ident}/signal')
    async def signal(ident:str,request:Request):
        a=account(request);d=await body(request);cleanup();kind=d.get('kind');data=d.get('data')
        if kind not in {'offer','answer','ice'} or not isinstance(data,dict):raise HTTPException(400,'Ugyldig signal.')
        if kind=='ice':
            if not isinstance(data.get('candidate'),str) or len(data['candidate'])>4096:raise HTTPException(400,'Ugyldig ICE.')
        elif data.get('type')!=kind or not isinstance(data.get('sdp'),str) or len(data['sdp'])>64000:raise HTTPException(400,'Ugyldig SDP.')
        with db() as c:
            c.execute('BEGIN IMMEDIATE');r=active_call(c,ident,a)
            if r['status']!='accepted':raise HTTPException(409,'Samtalen er ikke aktiv.')
            if (kind=='offer' and r['caller']!=a['id']) or (kind=='answer' and r['callee']!=a['id']):raise HTTPException(403,'Feil signalretning.')
            if c.execute('SELECT COUNT(*) FROM fruen_signals WHERE call_id=?',(ident,)).fetchone()[0]>=150:raise HTTPException(429,'For mange signaler.')
            other=r['callee'] if r['caller']==a['id'] else r['caller']
            c.execute('INSERT INTO fruen_signals(call_id,sender,recipient,kind,data) VALUES(?,?,?,?,?)',(ident,a['id'],other,kind,json.dumps(data)))
        return {'ok':True}
