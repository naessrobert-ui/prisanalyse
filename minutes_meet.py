"""Meeting authorization and Pi relay. In-memory signaling, durable jobs on Pi."""
import asyncio
import base64
import os
import re
import secrets
import time

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse
from pathlib import Path

import minutes_bridge as bridge
import minutes_protocol

router=APIRouter()
states={}
rooms={}


def configure(live_rooms, participant_id, room_name, send, broadcast):
    global rooms, pid_for, clean_room, send_to, publish_to
    rooms=live_rooms;pid_for=participant_id;clean_room=room_name;send_to=send;publish_to=broadcast
    states.clear()


def identity(request):
    cid=request.headers.get('x-meet-client','')
    room=clean_room(request.headers.get('x-meet-room',''))
    if not re.fullmatch('[0-9a-f]{32}',cid):raise HTTPException(403,'Bli med i møtet først.')
    pid=pid_for(cid)
    if pid not in rooms.get(room,{}):raise HTTPException(403,'Bli med i møtet først.')
    return room,pid


def snapshot(room):
    s=states.get(room)
    if not s:return {'t':'minutes','phase':'off','available':bridge.configured()}
    return {'t':'minutes','available':bridge.configured(),**{k:s[k] for k in ('id','owner','phase','start_at','epoch','last_seq','partial')}}


async def publish(room):
    await publish_to(room,snapshot(room))


async def joined(room,pid,reconnect=False):
    s=states.get(room)
    if not s:return
    if s['phase']=='recording':
        # Everyone sees the notice before incoming new participant audio is captured.
        s['partial']=True  # The unsent tail is discarded during the new participant notice.
        s['epoch']+=1;s['start_at']=time.time()+8
    await publish(room)


async def left(room,pid):
    s=states.get(room)
    if s and s['owner']==pid and s['phase']=='recording':
        await finish(room,partial=True)
    if not rooms.get(room):states.pop(room,None)


async def finish(room,partial=False):
    s=states.get(room)
    if not s:return
    if s['phase']!='recording':return
    s['phase']='processing';s['epoch']+=1;s['partial']|=partial
    await publish(room)
    try:
        await bridge.call('POST',f"/v1/sessions/{s['id']}/finish",data={'reader':s['reader'],'last_seq':s['last_seq'],'partial':s['partial']})
    except bridge.Unavailable:
        s['partial']=True
        # Pi automatically closes idle sessions; the owner retains a recovery token.
    await publish(room)


@router.get('/meet/minutes/status',include_in_schema=False)
async def capability():
    if not bridge.configured():return {'available':False}
    try:
        await bridge.call('POST','/v1/health')
        return {'available':True,'max_minutes':90,'retention_hours':24}
    except bridge.Unavailable:return {'available':False}


@router.get('/meet/minutes/public-key',include_in_schema=False)
async def worker_key():
    try:
        origin,private=bridge.config()
        value=minutes_protocol.public_key(private)
    except (bridge.Unavailable,ValueError):raise HTTPException(503,'Referat er ikke konfigurert.')
    return JSONResponse({'public_key':value,'origin':origin},headers={'Cache-Control':'no-store'})


@router.get('/meet/minutes-capture.js',include_in_schema=False)
async def capture_source():
    return FileResponse(Path(__file__).with_name('minutes-capture.js'),media_type='text/javascript',headers={'Cache-Control':'no-store'})


@router.post('/meet/minutes/start',include_in_schema=False)
async def start(request:Request):
    room,pid=identity(request)
    if not bridge.configured():raise HTTPException(503,'Pi-tjenesten er ikke klar ennå.')
    prior=states.get(room)
    if prior and prior['phase'] in {'preparing','processing'}:raise HTTPException(409,'Et referat er allerede under behandling.')
    if prior and prior['phase']=='recording':
        if prior['owner']!=pid:raise HTTPException(409,'En annen deltaker lager allerede referat.')
        return {**snapshot(room),'reader':prior['reader']}
    # Reserve before awaiting Pi so concurrent starts cannot open two sessions.
    s={'id':secrets.token_hex(16),'reader':secrets.token_hex(32),'owner':pid,'phase':'preparing','start_at':0,'epoch':1,'last_seq':-1,'partial':False,'uploading':False}
    states[room]=s
    try:
        await bridge.call('POST','/v1/sessions',data={'id':s['id'],'reader':s['reader'],'participants':[p['name'] for p in rooms[room].values()]})
    except bridge.Unavailable as exc:
        if states.get(room) is s:states.pop(room,None)
        raise HTTPException(exc.status_code,str(exc))
    if pid not in rooms.get(room,{}) or states.get(room) is not s:
        await bridge.call('POST',f"/v1/sessions/{s['id']}/finish",data={'reader':s['reader'],'last_seq':-1,'partial':True})
        raise HTTPException(409,'Møtet ble avsluttet under oppstart.')
    s['phase']='recording';s['start_at']=time.time()+8
    await publish(room)
    return {**snapshot(room),'reader':s['reader']}


@router.post('/meet/minutes/chunk/{seq}',include_in_schema=False)
async def chunk(seq:int,request:Request):
    room,pid=identity(request);s=states.get(room)
    try:epoch=int(request.headers.get('x-minutes-epoch','-1'))
    except ValueError:raise HTTPException(400,'Ugyldig opptaksperiode.')
    if (not s or s['owner']!=pid or s['phase']!='recording' or epoch!=s['epoch'] or time.time()<s['start_at']):
        raise HTTPException(409,'Opptaket er stoppet eller pauset for nytt varsel.')
    if seq<0 or seq>=270:raise HTTPException(413,'Referatet nådde grensen på 90 minutter.')
    if s['uploading']:raise HTTPException(429,'Forrige lydbit sendes fortsatt.')
    s['uploading']=True
    try:
        body=bytearray()
        async for part in request.stream():
            body.extend(part)
            if len(body)>700000:raise HTTPException(413,'Lydbiten er for stor.')
        if s['phase']!='recording' or epoch!=s['epoch']:raise HTTPException(409,'Opptaket er pauset.')
        result=await bridge.call('POST',f"/v1/sessions/{s['id']}/chunks/{seq}",audio=bytes(body))
        s['last_seq']=max(s['last_seq'],seq)
        return result
    except bridge.Unavailable as exc:raise HTTPException(exc.status_code,str(exc))
    finally:s['uploading']=False


@router.post('/meet/minutes/finish',include_in_schema=False)
async def finish_api(request:Request):
    room,pid=identity(request)
    # Any participant can stop further capture. No vote or all-approve gate.
    s=states.get(room)
    await finish(room,partial=bool(s and s['owner']!=pid) or request.headers.get('x-minutes-partial')=='1')
    return {'ok':True}


def report_auth(request,sid):
    token=request.headers.get('x-minutes-token','')
    if not re.fullmatch('[0-9a-f]{32}',sid) or not re.fullmatch('[0-9a-f]{64}',token):raise HTTPException(403,'Ugyldig hentelenke.')
    return token


async def report(sid,token):
    status=await bridge.call('POST',f'/v1/sessions/{sid}/status',data={'reader':token})
    if status['ready'] and status['attempts']<2:
        claim=await bridge.call('POST',f'/v1/sessions/{sid}/claim',data={'reader':token})
        if claim.get('claimed'):
            text=''
            try:text=await bridge.summarize(claim['transcript'],claim['participants'],claim['partial'])
            except asyncio.CancelledError:
                raise
            except Exception:
                # A cancelled request/deploy must not publish a fabricated draft.
                # Pi's lease expires; bounded retry remains possible from the recovery link.
                pass
            status=await bridge.call('POST',f'/v1/sessions/{sid}/draft',data={'reader':token,'draft':text})
    if status.get('draft'):
        for room,s in list(states.items()):
            if s['id']==sid and not s.get('published'):
                s['published']=True;s['phase']='done'
                await publish_to(room,{'t':'minutes-draft','id':sid,'text':status['draft'],'partial':status['partial']})
                await publish(room)
    return status


@router.get('/meet/minutes/report/{sid}',include_in_schema=False)
async def report_api(sid:str,request:Request):
    token=report_auth(request,sid)
    try:value=await report(sid,token)
    except bridge.Unavailable as exc:raise HTTPException(exc.status_code,str(exc))
    return JSONResponse(value,headers={'Cache-Control':'no-store','X-Robots-Tag':'noindex'})


@router.post('/meet/minutes/report/{sid}/finish',include_in_schema=False)
async def recover_finish(sid:str,request:Request):
    token=report_auth(request,sid)
    try:return await bridge.call('POST',f'/v1/sessions/{sid}/finish',data={'reader':token,'last_seq':-2,'partial':True})
    except bridge.Unavailable as exc:raise HTTPException(exc.status_code,str(exc))


@router.delete('/meet/minutes/report/{sid}',include_in_schema=False)
async def delete_report(sid:str,request:Request):
    token=report_auth(request,sid)
    try:result=await bridge.call('POST',f'/v1/sessions/{sid}/delete',data={'reader':token})
    except bridge.Unavailable as exc:raise HTTPException(exc.status_code,str(exc))
    for room,s in list(states.items()):
        if s['id']==sid:
            s['phase']='deleted';s['epoch']+=1;await publish(room)
    return result
