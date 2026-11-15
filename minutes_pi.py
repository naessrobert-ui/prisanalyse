"""Pi-only durable queue and local Whisper worker. Contains no OpenAI key."""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import io
import json
import os
import re
import secrets
import sqlite3
import subprocess
import tempfile
import time
import wave
from contextlib import asynccontextmanager, contextmanager
from pathlib import Path

from cryptography.exceptions import InvalidSignature
from fastapi import FastAPI, HTTPException, Request

from minutes_protocol import verify

MAX_BODY = 700000
MAX_CHUNKS = 270
RETENTION = 86400


class Store:
    def __init__(self, directory, cli, model, monthly=40):
        self.directory, self.cli, self.model = Path(directory), str(cli), str(model)
        self.monthly = monthly
        self.directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.db = self.directory / 'minutes.sqlite3'
        with self.connect() as db:
            db.executescript('''
              CREATE TABLE IF NOT EXISTS sessions (
                id TEXT PRIMARY KEY, reader TEXT NOT NULL, participants TEXT NOT NULL,
                created REAL NOT NULL, touched REAL NOT NULL, expires REAL NOT NULL,
                status TEXT NOT NULL DEFAULT 'open', final_seq INTEGER, partial INTEGER NOT NULL DEFAULT 0,
                summary TEXT NOT NULL DEFAULT '', claim_until REAL NOT NULL DEFAULT 0,
                attempts INTEGER NOT NULL DEFAULT 0);
              CREATE TABLE IF NOT EXISTS chunks (
                sid TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE, seq INTEGER NOT NULL,
                digest TEXT NOT NULL, audio BLOB, status TEXT NOT NULL DEFAULT 'queued',
                text TEXT NOT NULL DEFAULT '', attempts INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY(sid,seq));
            ''')
            db.execute("UPDATE chunks SET status=CASE WHEN attempts>=2 THEN 'error' ELSE 'queued' END,audio=CASE WHEN attempts>=2 THEN NULL ELSE audio END WHERE status='processing'")

    @contextmanager
    def connect(self):
        db = sqlite3.connect(self.db, timeout=10)
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA foreign_keys=ON')
        db.execute('PRAGMA secure_delete=ON')
        db.execute('BEGIN IMMEDIATE')
        try:
            with db:
                yield db
        finally:
            db.close()

    def cleanup(self):
        now = time.time()
        with self.connect() as db:
            db.execute('DELETE FROM sessions WHERE expires<?', (now,))
            db.execute("UPDATE sessions SET status='closing', final_seq=(SELECT COALESCE(MAX(seq),-1) FROM chunks WHERE sid=sessions.id),partial=1 WHERE status='open' AND touched<?", (now - 120,))

    def create(self, sid, reader, participants):
        self.cleanup()
        now = time.time()
        month = time.strftime('%Y-%m-01', time.gmtime(now))
        with self.connect() as db:
            if db.execute('SELECT 1 FROM sessions WHERE id=?', (sid,)).fetchone():
                self.session(db, sid, reader)
                return {'id':sid,'exists':True}
            # A durable month counter survives 24h content deletion.
            db.execute('CREATE TABLE IF NOT EXISTS quota (month TEXT PRIMARY KEY, used INTEGER NOT NULL)')
            used = db.execute('SELECT used FROM quota WHERE month=?', (month,)).fetchone()
            if used and used[0] >= self.monthly:
                raise HTTPException(429, 'Månedsgrensen er nådd.')
            if (db.execute("SELECT COUNT(*) FROM sessions WHERE status='open'").fetchone()[0] >= 1 or db.execute("SELECT COUNT(*) FROM chunks WHERE status IN ('queued','processing')").fetchone()[0]):
                raise HTTPException(429, 'Pi-en behandler allerede et møte.')
            db.execute('INSERT INTO sessions(id,reader,participants,created,touched,expires) VALUES(?,?,?,?,?,?)',
                       (sid, hashlib.sha256(reader.encode()).hexdigest(), json.dumps(participants), now, now, now+RETENTION))
            db.execute('INSERT INTO quota VALUES(?,1) ON CONFLICT(month) DO UPDATE SET used=used+1', (month,))
        return self.status(sid, reader)

    def session(self, db, sid, reader=None):
        s = db.execute('SELECT * FROM sessions WHERE id=? AND expires>?', (sid,time.time())).fetchone()
        if not s:
            raise HTTPException(410, 'Jobben er utløpt.')
        if reader is not None and not hmac.compare_digest(s['reader'], hashlib.sha256(reader.encode()).hexdigest()):
            raise HTTPException(403, 'Ugyldig hentekode.')
        return s

    def append(self, sid, seq, audio):
        if seq < 0 or seq >= MAX_CHUNKS:
            raise HTTPException(413, 'Maks 90 minutter.')
        try:
            with wave.open(io.BytesIO(audio)) as wav:
                if wav.getnchannels()!=1 or wav.getsampwidth()!=2 or wav.getframerate()!=16000 or wav.getcomptype()!='NONE' or not 1 <= wav.getnframes() <= 320000:
                    raise ValueError()
                if len(wav.readframes(wav.getnframes())) != wav.getnframes()*2:
                    raise ValueError()
        except (wave.Error, EOFError, ValueError):
            raise HTTPException(415, 'Forventet inntil 20 sekunder PCM16 WAV, 16 kHz mono.')
        digest = hashlib.sha256(audio).hexdigest()
        with self.connect() as db:
            s = self.session(db, sid)
            prior = db.execute('SELECT digest FROM chunks WHERE sid=? AND seq=?',(sid,seq)).fetchone()
            if prior:
                if prior[0] != digest:
                    raise HTTPException(409, 'Sekvensen har annet innhold.')
                return {'ok':True,'duplicate':True}
            if s['status']!='open' or time.time()-s['created']>5400:
                raise HTTPException(409, 'Møtet tar ikke imot flere lydbiter.')
            expected = db.execute('SELECT COALESCE(MAX(seq),-1)+1 FROM chunks WHERE sid=?',(sid,)).fetchone()[0]
            if expected!=seq:
                raise HTTPException(409, 'Mangler forrige lydbit.')
            if db.execute('SELECT COALESCE(SUM(length(audio)),0) FROM chunks').fetchone()[0]+len(audio)>32*1024*1024:
                raise HTTPException(429, 'Transkripsjonen har for stort etterslep.')
            db.execute('INSERT INTO chunks(sid,seq,digest,audio) VALUES(?,?,?,?)',(sid,seq,digest,audio))
            db.execute('UPDATE sessions SET touched=? WHERE id=?',(time.time(),sid))
        return {'ok':True,'duplicate':False}

    def finish(self, sid, reader, final_seq, partial=False):
        with self.connect() as db:
            s = self.session(db,sid,reader)
            if s['status']=='open':
                actual = db.execute('SELECT COALESCE(MAX(seq),-1) FROM chunks WHERE sid=?',(sid,)).fetchone()[0]
                db.execute("UPDATE sessions SET status='closing',final_seq=?,partial=?,touched=? WHERE id=?", (actual,int(partial or final_seq!=actual),time.time(),sid))
        return self.status(sid,reader)

    def status(self, sid, reader):
        with self.connect() as db:
            s = self.session(db,sid,reader)
            counts = {r[0]:r[1] for r in db.execute('SELECT status,COUNT(*) FROM chunks WHERE sid=? GROUP BY status',(sid,))}
            next_seq = db.execute('SELECT COALESCE(MAX(seq),-1)+1 FROM chunks WHERE sid=?',(sid,)).fetchone()[0]
            ready = s['status'] in {'closing','generating','failed'} and not (counts.get('queued',0)+counts.get('processing',0))
            return {'id':sid,'status':s['status'],'draft':s['summary'],'partial':bool(s['partial'] or counts.get('error')),
                    'queued':counts.get('queued',0)+counts.get('processing',0),'completed':counts.get('done',0),
                    'errors':counts.get('error',0),'next_seq':next_seq,'ready':ready,'attempts':s['attempts'],'expires':s['expires']}

    def claim(self, sid, reader):
        now=time.time()
        with self.connect() as db:
            s=self.session(db,sid,reader)
            if s['summary'] or s['status']=='open' or s['claim_until']>now or s['attempts']>=2:
                return {'claimed':False}
            if db.execute("SELECT COUNT(*) FROM chunks WHERE sid=? AND status IN ('queued','processing')",(sid,)).fetchone()[0]:
                return {'claimed':False}
            rows=db.execute('SELECT seq,text,status FROM chunks WHERE sid=? ORDER BY seq',(sid,)).fetchall()
            text='\n'.join(r['text'] if r['status']=='done' else '[Manglende lyd i del '+str(r['seq']+1)+']' for r in rows)
            if not text.strip():
                db.execute("UPDATE sessions SET status='failed',attempts=2 WHERE id=?",(sid,))
                return {'claimed':False,'empty':True}
            db.execute("UPDATE sessions SET status='generating',claim_until=?,attempts=attempts+1 WHERE id=?",(now+180,sid))
            return {'claimed':True,'transcript':text,'participants':json.loads(s['participants']), 'partial':bool(s['partial'] or any(r['status']=='error' for r in rows))}

    def save_draft(self,sid,reader,text):
        if not isinstance(text,str) or len(text)>20000:
            raise HTTPException(400,'Ugyldig utkast.')
        with self.connect() as db:
            self.session(db,sid,reader)
            db.execute("UPDATE sessions SET status=?,summary=?,claim_until=0 WHERE id=?",('done' if text else 'failed',text,sid))
        return self.status(sid,reader)

    def delete(self,sid,reader):
        with self.connect() as db:
            self.session(db,sid,reader)
            db.execute('DELETE FROM sessions WHERE id=?',(sid,))
        return {'ok':True}

    def process_next(self):
        self.cleanup()
        with self.connect() as db:
            row=db.execute("SELECT sid,seq,audio FROM chunks WHERE status='queued' ORDER BY rowid LIMIT 1").fetchone()
            if not row:return False
            db.execute("UPDATE chunks SET status='processing',attempts=attempts+1 WHERE sid=? AND seq=?",(row['sid'],row['seq']))
        text=None
        try:
            with tempfile.TemporaryDirectory(dir=self.directory) as folder:
                source=Path(folder)/'chunk.wav';source.write_bytes(row['audio']);out=Path(folder)/'result'
                result=subprocess.run(['nice','-n','10',self.cli,'-m',self.model,'-f',str(source),'-l','no','-t','2','-otxt','-of',str(out)],
                    stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=100,check=False)
                if result.returncode==0:
                    text=out.with_suffix('.txt').read_text().strip()
                    if len(text)>12000:text=None
        except (OSError, subprocess.TimeoutExpired):
            pass
        with self.connect() as db:
            r=db.execute('SELECT attempts FROM chunks WHERE sid=? AND seq=?',(row['sid'],row['seq'])).fetchone()
            if not r:return True
            status='done' if text is not None else ('error' if r['attempts']>=2 else 'queued')
            db.execute('UPDATE chunks SET status=?,text=?,audio=CASE WHEN ? THEN NULL ELSE audio END WHERE sid=? AND seq=?',
                       (status,text or '',int(status in {'done','error'}),row['sid'],row['seq']))
        return True


def create_app(store=None, public=None, run_worker=True):
    store=store or Store(os.environ['MINUTES_DATA_DIR'],os.environ['WHISPER_CLI'],os.environ['WHISPER_MODEL'],int(os.environ.get('MINUTES_MONTHLY_LIMIT','40')))
    public=public or Path(os.environ['MINUTES_PUBLIC_KEY_FILE']).read_text().strip()
    nonces={}
    @asynccontextmanager
    async def lifespan(app):
        stop=asyncio.Event()
        async def worker():
            while not stop.is_set():
                worked=await asyncio.to_thread(store.process_next)
                if not worked:
                    try:await asyncio.wait_for(stop.wait(),1)
                    except TimeoutError:pass
        task=asyncio.create_task(worker()) if run_worker else None
        try:yield
        finally:
            stop.set()
            if task:await task
    app=FastAPI(lifespan=lifespan,docs_url=None,redoc_url=None,openapi_url=None)
    @app.get('/health')
    async def health():return {'ok':True,'local_transcription':True}
    @app.api_route('/v1/{path:path}',methods=['POST','PUT','DELETE'])
    async def api(request:Request,path:str):
        body=bytearray()
        async for part in request.stream():
            body.extend(part)
            if len(body)>MAX_BODY:raise HTTPException(413,'For stor forespørsel.')
        try:verify(public,request.method,request.url.path,request.headers,body,nonces)
        except (ValueError,InvalidSignature):raise HTTPException(403,'Ugyldig signatur.')
        try:
            if path=='health':return {'ok':True}
            if path=='sessions' and request.method=='POST':
                d=json.loads(body)
                if not re.fullmatch('[0-9a-f]{32}',d['id']) or not re.fullmatch('[0-9a-f]{64}',d['reader']):raise ValueError()
                names=d.get('participants',[])
                if not isinstance(names,list) or len(names)>20 or any(not isinstance(n,str) or len(n)>60 for n in names):raise ValueError()
                return await asyncio.to_thread(store.create,d['id'],d['reader'],names)
            match=re.fullmatch(r'sessions/([0-9a-f]{32})/(status|finish|claim|draft|delete|chunks/\d{1,3})',path)
            if not match:raise HTTPException(404)
            sid,action=match.groups()
            if action.startswith('chunks/'):
                return await asyncio.to_thread(store.append,sid,int(action.split('/')[1]),bytes(body))
            d=json.loads(body);reader=d['reader']
            if not isinstance(reader,str) or not re.fullmatch('[0-9a-f]{64}',reader):raise ValueError()
            if action=='status':return await asyncio.to_thread(store.status,sid,reader)
            if action=='finish':return await asyncio.to_thread(store.finish,sid,reader,int(d.get('last_seq',-1)),bool(d.get('partial')))
            if action=='claim':return await asyncio.to_thread(store.claim,sid,reader)
            if action=='draft':return await asyncio.to_thread(store.save_draft,sid,reader,d.get('draft',''))
            if action=='delete':return await asyncio.to_thread(store.delete,sid,reader)
        except (KeyError,ValueError,TypeError,json.JSONDecodeError):raise HTTPException(400,'Ugyldig innhold.')
    return app
