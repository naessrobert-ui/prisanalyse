"""Private durable scheduled meetings, capability links and interoperable iCalendar."""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import os
import re
import secrets
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse, Response

router=APIRouter()
UTC=timezone.utc
ROOM=re.compile(r'plan-[0-9a-f]{20}')
TOKEN=re.compile(r'[0-9a-f]{64}')
PREFIX='motes/plans/'
MAX_OBJECT=32768
_store=None
_rates={}
_rate_lock=threading.Lock()


class Unavailable(Exception):pass
class Missing(Exception):pass
class Conflict(Exception):pass


class S3Store:
    def __init__(self):self.client=None;self.bucket='';self.checked=0;self.lock=threading.Lock()
    def connection(self):
        with self.lock:
            if self.client and time.monotonic()-self.checked<600:return self.client,self.bucket
            bucket=(os.environ.get('MEET_PLANS_BUCKET') or os.environ.get('NEWSWEB_CONSENSUS_BUCKET') or os.environ.get('S3_BUCKET_NAME') or 'prisanalyse-data').strip()
            try:
                import boto3
                from botocore.config import Config
                client=self.client or boto3.client('s3',region_name=os.environ.get('AWS_REGION','eu-north-1'),config=Config(connect_timeout=3,read_timeout=8,retries={'max_attempts':1,'mode':'standard'}))
                block=client.get_public_access_block(Bucket=bucket)['PublicAccessBlockConfiguration']
                if not all(block.get(k) is True for k in ['BlockPublicAcls','IgnorePublicAcls','BlockPublicPolicy','RestrictPublicBuckets']):raise ValueError('Not private')
            except Exception as exc:
                code=getattr(exc,'response',{}).get('Error',{}).get('Code',type(exc).__name__)
                logging.getLogger(__name__).warning('Meeting plans private storage unavailable: %s',code)
                raise Unavailable('Privat lagring for planlagte møter er ikke tilgjengelig.') from exc
            self.client,self.bucket,self.checked=client,bucket,time.monotonic()
            return client,bucket
    def ready(self):self.connection();return True
    def get(self,room):
        client,bucket=self.connection()
        try:
            result=client.get_object(Bucket=bucket,Key=PREFIX+room+'.json')
            try:body=result['Body'].read(MAX_OBJECT+1)
            finally:result['Body'].close()
            if len(body)>MAX_OBJECT:raise ValueError()
            return json.loads(body),result['ETag']
        except Exception as exc:
            if getattr(exc,'response',{}).get('Error',{}).get('Code') in {'NoSuchKey','404'}:raise Missing() from exc
            raise Unavailable('Møtet kunne ikke hentes fra privat lagring.') from exc
    def delete(self,room):
        client,bucket=self.connection()
        try:client.delete_object(Bucket=bucket,Key=PREFIX+room+'.json')
        except Exception as exc:raise Unavailable('Møtet kunne ikke slettes.') from exc
    def put(self,room,value,etag=None):
        client,bucket=self.connection();body=json.dumps(value,ensure_ascii=False,separators=(',',':')).encode()
        if len(body)>MAX_OBJECT:raise ValueError('Plan too large')
        args={'Bucket':bucket,'Key':PREFIX+room+'.json','Body':body,'ContentType':'application/json','ServerSideEncryption':'AES256','CacheControl':'private,no-store'}
        args['IfMatch' if etag else 'IfNoneMatch']=etag if etag else '*'
        try:client.put_object(**args)
        except Exception as exc:
            if getattr(exc,'response',{}).get('Error',{}).get('Code') in {'PreconditionFailed','ConditionalRequestConflict','412','409'}:raise Conflict() from exc
            raise Unavailable('Møtet kunne ikke lagres privat.') from exc


class MemoryStore:
    """Explicitly injected by tests; never selected as a production fallback."""
    def __init__(self):self.items={};self.lock=threading.Lock()
    def ready(self):return True
    def get(self,room):
        with self.lock:
            if room not in self.items:raise Missing()
            value,revision=self.items[room];return json.loads(json.dumps(value)),revision
    def delete(self,room):
        with self.lock:self.items.pop(room,None)
    def put(self,room,value,etag=None):
        with self.lock:
            previous=self.items.get(room)
            if (previous and etag!=previous[1]) or (not previous and etag):raise Conflict()
            self.items[room]=(json.loads(json.dumps(value)),str(value['version']))


def storage():
    global _store
    if _store is None:_store=S3Store()
    return _store


def public_base(request):
    host=(request.url.hostname or '').lower().rstrip('.')
    if host in {'xn--mtes-gra.no','www.xn--mtes-gra.no'}:return 'https://xn--mtes-gra.no/'
    if host in {'localhost','127.0.0.1'}:return str(request.base_url)
    raise HTTPException(404,'Planlegg møtet på møtes.no.')


def utc(value):return datetime.fromisoformat(value).astimezone(UTC)

def stamp(value):return value.astimezone(UTC).isoformat(timespec='seconds')


def validate(d):
    if not isinstance(d,dict):raise HTTPException(400,'Fyll ut møtedetaljene.')
    fields={}
    for field,limit in [('title',120),('name',80),('agenda',4000)]:
        value=d.get(field,'')
        if not isinstance(value,str) or len(value)>limit:raise HTTPException(400,'Møtetittel, navn eller agenda er for lang.')
        value=value.replace('\r\n','\n').replace('\r','\n')
        if any(ord(c)<32 and c not in '\n\t' for c in value):raise HTTPException(400,'Ugyldige tegn i møtedetaljene.')
        fields[field]=value.strip() if field=='agenda' else ' '.join(value.split())
    if not fields['title'] or not fields['name']:raise HTTPException(400,'Skriv møtetittel og navnet ditt.')
    duration=d.get('duration')
    if isinstance(duration,bool) or not isinstance(duration,int) or not 5<=duration<=480:raise HTTPException(400,'Velg varighet mellom 5 og 480 minutter.')
    tz=d.get('timezone','Europe/Oslo');local=d.get('local_start','')
    try:
        if not isinstance(tz,str) or len(tz)>80 or not isinstance(local,str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}',local):raise ValueError()
        zone=ZoneInfo(tz);naive=datetime.fromisoformat(local);start=naive.replace(tzinfo=zone,fold=0);alternative=naive.replace(tzinfo=zone,fold=1)
        if start.astimezone(UTC).astimezone(zone).replace(tzinfo=None)!=naive:raise HTTPException(400,'Klokkeslettet finnes ikke på grunn av sommertid. Velg et annet tidspunkt.')
        if start.utcoffset()!=alternative.utcoffset():raise HTTPException(400,'Klokkeslettet forekommer to ganger ved overgang fra sommertid. Velg et annet tidspunkt.')
    except (ValueError,TypeError,ZoneInfoNotFoundError):raise HTTPException(400,'Velg en gyldig dato og tidssone.')
    now=datetime.now(UTC);start=start.astimezone(UTC)
    if start<now-timedelta(seconds=30) or start>now+timedelta(days=365):raise HTTPException(400,'Velg et fremtidig tidspunkt innen ett år.')
    return {**fields,'timezone':tz,'start':stamp(start),'end':stamp(start+timedelta(minutes=duration)),'duration':duration}


def owner_ok(plan,token):return bool(TOKEN.fullmatch(token or '') and hmac.compare_digest(plan['owner_hash'],hashlib.sha256(token.encode()).hexdigest()))

def guest_ok(plan,token,*,joining=False):
    valid=bool(re.fullmatch(r'p\.[0-9a-f]{64}',token or '') and hmac.compare_digest(plan['guest'],token[2:]))
    if joining:return valid and not plan['cancelled'] and datetime.now(UTC)<utc(plan['end'])+timedelta(hours=24)
    return valid


def load(room):
    if not ROOM.fullmatch(room):raise Missing()
    return storage().get(room)


async def authorize(room,token):
    if not ROOM.fullmatch(room) or not re.fullmatch(r'p\.[0-9a-f]{64}',token or ''):return False
    try:plan,_=await asyncio.to_thread(load,room)
    except (Missing,Unavailable):return False
    return guest_ok(plan,token,joining=True)


def visible(plan,base):
    inv='p.'+plan['guest'];url=base+'#'+plan['room']+'?inv='+inv
    return {k:plan[k] for k in ['room','title','name','agenda','timezone','start','end','duration','version','cancelled']}|{'inv':inv,'join_url':url,'expires':stamp(utc(plan['end'])+timedelta(hours=24))}


def access(request,plan):
    own=request.headers.get('x-plan-owner','')
    if owner_ok(plan,own):return 'owner'
    if guest_ok(plan,request.headers.get('x-plan-invite','')):return 'guest'
    raise HTTPException(403,'Denne møtelenken gir ikke tilgang.')


async def body(request):
    if request.headers.get('x-meet-request')!='1':raise HTTPException(403,'Åpne møtes.no og prøv igjen.')
    raw=bytearray()
    async for part in request.stream():
        raw.extend(part)
        if len(raw)>20000:raise HTTPException(413,'Møtedetaljene er for store.')
    try:return json.loads(raw)
    except ValueError:raise HTTPException(400,'Fyll ut møtedetaljene.')


def limited(request):
    now=time.time();key=hashlib.sha256((request.client.host if request.client else 'unknown').encode()).hexdigest()
    with _rate_lock:
        for old in list(_rates):
            _rates[old]=[t for t in _rates[old] if t>now-3600]
            if not _rates[old]:del _rates[old]
        if len(_rates.get(key,[]))>=20 or sum(map(len,_rates.values()))>=200:raise HTTPException(429,'For mange nye møter. Prøv igjen senere.')
        _rates.setdefault(key,[]).append(now)


def fold(line):
    result=[];part=''
    for char in line:
        if len((part+char).encode('utf-8'))>75:result.append(part);part=' '
        part+=char
    result.append(part);return '\r\n'.join(result)


def escape(value):return value.replace('\\','\\\\').replace('\r\n','\n').replace('\r','\n').replace('\n','\\n').replace(';',r'\;').replace(',',r'\,')


def calendar(plan,base):
    fmt=lambda s:utc(s).strftime('%Y%m%dT%H%M%SZ')
    url=visible(plan,base)['join_url'];description='Arrangør: '+plan['name']+'\n'+plan['agenda']+'\n\nBli med: '+url+'\nVarighet er en kalenderavtale, ikke en tidsgrense for samtalen.'
    lines=['BEGIN:VCALENDAR','VERSION:2.0','PRODID:-//motes.no//Planlagte moter//NB','CALSCALE:GREGORIAN','METHOD:'+('CANCEL' if plan['cancelled'] else 'PUBLISH'),'BEGIN:VEVENT','UID:'+plan['room']+'@motes.no','SEQUENCE:'+str(plan['version']),'DTSTAMP:'+fmt(plan['updated']),'CREATED:'+fmt(plan['created']),'LAST-MODIFIED:'+fmt(plan['updated']),'DTSTART:'+fmt(plan['start']),'DTEND:'+fmt(plan['end']),'SUMMARY:'+escape(plan['title']),'DESCRIPTION:'+escape(description),'LOCATION:'+escape(url),'URL:'+url,'STATUS:'+('CANCELLED' if plan['cancelled'] else 'CONFIRMED')]
    if not plan['cancelled']:lines+=['BEGIN:VALARM','TRIGGER:-PT10M','ACTION:DISPLAY','DESCRIPTION:'+escape(plan['title']),'END:VALARM']
    return ('\r\n'.join(fold(line) for line in lines+['END:VEVENT','END:VCALENDAR'])+'\r\n').encode('utf-8')


@router.get('/meet/planner.js',include_in_schema=False)
async def script():return FileResponse(Path(__file__).with_name('meet-planner.js'),media_type='text/javascript',headers={'Cache-Control':'no-store'})


@router.get('/meet/plans/status',include_in_schema=False)
async def capability():
    try:await asyncio.to_thread(storage().ready);return {'available':True}
    except Unavailable:return {'available':False,'detail':'Privat lagring for planlagte møter er ikke tilgjengelig ennå.'}


@router.post('/meet/plans',include_in_schema=False)
async def create(request:Request):
    base=public_base(request);data=validate(await body(request));limited(request)
    room='plan-'+secrets.token_hex(10);owner=secrets.token_hex(32);now=stamp(datetime.now(UTC))
    plan={**data,'room':room,'guest':secrets.token_hex(32),'owner_hash':hashlib.sha256(owner.encode()).hexdigest(),'created':now,'updated':now,'version':0,'cancelled':False}
    try:await asyncio.to_thread(storage().put,room,plan)
    except (Unavailable,Conflict):raise HTTPException(503,'Møtet kunne ikke lagres. Prøv igjen senere.')
    return JSONResponse({**visible(plan,base),'owner':owner,'manage_url':base+'#plan-eier='+room+'.'+owner},headers={'Cache-Control':'no-store'})


async def authorized(room,request):
    if not TOKEN.fullmatch(request.headers.get('x-plan-owner','')) and not re.fullmatch(r'p\.[0-9a-f]{64}',request.headers.get('x-plan-invite','')):raise HTTPException(403,'Bruk en gyldig møtelenke.')
    try:plan,etag=await asyncio.to_thread(load,room)
    except Missing:raise HTTPException(404,'Møtet finnes ikke.')
    except Unavailable as exc:raise HTTPException(503,str(exc))
    role=access(request,plan)
    return plan,etag,role


@router.get('/meet/plans/{room}',include_in_schema=False)
async def get(room:str,request:Request):
    plan,_,role=await authorized(room,request)
    value=visible(plan,public_base(request))
    if role=='owner':value['manage_url']=public_base(request)+'#plan-eier='+room+'.'+request.headers['x-plan-owner']
    return JSONResponse(value,headers={'Cache-Control':'no-store','X-Robots-Tag':'noindex'})


@router.post('/meet/plans/{room}/calendar',include_in_schema=False)
async def download(room:str,request:Request):
    await body(request);plan,_,_=await authorized(room,request)
    return Response(calendar(plan,public_base(request)),media_type='text/calendar; charset=utf-8',headers={'Cache-Control':'no-store','Content-Disposition':'attachment; filename="'+room+'.ics"'})


@router.put('/meet/plans/{room}',include_in_schema=False)
async def edit(room:str,request:Request):
    data=await body(request);plan,etag,role=await authorized(room,request)
    if role!='owner':raise HTTPException(403,'Bruk den private arrangørlenken for å endre møtet.')
    if not isinstance(data,dict) or data.get('version')!=plan['version'] or plan['cancelled']:raise HTTPException(409,'Møtet er endret eller avlyst. Hent det på nytt.')
    plan.update(validate(data));plan.update(version=plan['version']+1,updated=stamp(datetime.now(UTC)))
    try:await asyncio.to_thread(storage().put,room,plan,etag)
    except Conflict:raise HTTPException(409,'Møtet er endret i en annen fane. Hent det på nytt.')
    except Unavailable as exc:raise HTTPException(503,str(exc))
    return JSONResponse(visible(plan,public_base(request)),headers={'Cache-Control':'no-store'})


@router.post('/meet/plans/{room}/cancel',include_in_schema=False)
async def cancel(room:str,request:Request):
    await body(request);plan,etag,role=await authorized(room,request)
    if role!='owner':raise HTTPException(403,'Bare arrangørlenken kan avlyse møtet.')
    if not plan['cancelled']:
        plan.update(cancelled=True,version=plan['version']+1,updated=stamp(datetime.now(UTC)))
        try:await asyncio.to_thread(storage().put,room,plan,etag)
        except Conflict:raise HTTPException(409,'Møtet ble endret. Hent det på nytt.')
        except Unavailable as exc:raise HTTPException(503,str(exc))
    return JSONResponse(visible(plan,public_base(request)),headers={'Cache-Control':'no-store'})


@router.delete('/meet/plans/{room}',include_in_schema=False)
async def delete(room:str,request:Request):
    await body(request);plan,_,role=await authorized(room,request)
    if role!='owner':raise HTTPException(403,'Bare arrangørlenken kan slette møtet.')
    try:await asyncio.to_thread(storage().delete,room)
    except Unavailable as exc:raise HTTPException(503,str(exc))
    return JSONResponse({'ok':True},headers={'Cache-Control':'no-store'})
