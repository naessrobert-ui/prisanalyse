"""
møtes.no – møterom med lyd, kamera, skjermdeling og chat.

Denne modulen gir:
  GET  /meet                 -> selve møtesiden (nam_meet.html)
  WS   /meet/ws/{rom}        -> oppkobling (signalering) mellom deltakerne

Lyd og bilde går kryptert direkte mellom nettleserne (WebRTC).
Serveren formidler bare oppkoblingen, chat og hvem som er i rommet.
Møterommene ligger i minnet og forsvinner når rommet tømmes.
Valgfri Johnny: bare et aktivt innsendt spørsmål sendes til OpenAI etter alles samtykke.
Ingen kontinuerlig AI-lytting eller opptak av hele møtet. Svaret deles i møtet.

Miljøvariabler:
  NAM_MEET_KODE   Tilgangskode deltakerne må skrive inn. Sterkt anbefalt.
                  Står den tom, kan alle som kjenner adressen bli med.
  NAM_MEET_MAKS   Maks deltakere per rom (standard 10).

Invitasjoner: den som har tilgangskoden, kan lage en lenke som gir tilgang til ett rom i
24 timer uten koden (GET /meet/invite). Lenken er signert med en nøkkel avledet fra
NAM_MEET_KODE, så alle invitasjoner slutter å virke hvis koden byttes.
  CLOUDFLARE_TURN_KEY_ID, CLOUDFLARE_TURN_API_TOKEN
                  TURN-nøkkel fra Cloudflare (Realtime > TURN). Uten TURN får deltakere
                  på strenge nett (bedriftsnett, en del mobilnett) ikke lyd og bilde.

NB: Rommene ligger i minnet til prosessen. Kjør med én worker
(uvicorn uten --workers, eller --workers 1), ellers havner deltakere
i samme rom på hver sin prosess og finner ikke hverandre.
"""
import asyncio
import hashlib
import hmac
import json
import os
import re
import secrets
from pathlib import Path

import logging
import time

import httpx
from fastapi import APIRouter, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse

import johnny_ai

router = APIRouter()

HTML_FIL = Path(__file__).with_name("nam_meet.html")
KODE = os.environ.get("NAM_MEET_KODE", "")
MAKS = int(os.environ.get("NAM_MEET_MAKS", "10"))
MAKS_MELDING = 64_000  # bytes; SDP-tilbud er typisk 3–8 kB
TURN_KEY_ID = os.environ.get("CLOUDFLARE_TURN_KEY_ID", "")
TURN_TOKEN = os.environ.get("CLOUDFLARE_TURN_API_TOKEN", "")
TURN_TTL = 12 * 3600  # sekunder nøklene gjelder; lengre enn et møte
TURN_FORNY = 3600  # hent nye nøkler fra Cloudflare minst så ofte
STUN = [{"urls": ["stun:stun.cloudflare.com:3478", "stun:stun.l.google.com:19302"]}]

log = logging.getLogger(__name__)
_ice_cache: dict = {"til": 0.0, "servere": None}

# rom -> {deltaker_id -> {"ws": WebSocket, "name": str, "state": dict}}
_rom: dict[str, dict[str, dict]] = {}
_las = asyncio.Lock()


# Consent is tied to live WebSocket participants, not a reusable meeting invite.
_johnny: dict[str, dict] = {}
_johnny_requests: list[float] = []


def _johnny_state(rom):
    state = _johnny.get(rom, {})
    participants = set(_rom.get(rom, {}))
    active = bool(participants and state.get('proposed') and state.get('consents') == participants and johnny_ai.capability()['available'])
    return {'t': 'johnny', **johnny_ai.capability(), 'proposed': bool(state.get('proposed')), 'active': active,
            'approved': sorted(state.get('consents', set()) & participants), 'participants': len(participants),
            'busy': bool(state.get('busy')), 'listening': state.get('listening', ''), 'listener': state.get('listener', '')}


async def _johnny_publish(rom, *, force=False):
    if rom not in _johnny and not force:
        return
    await _kringkast(rom, _johnny_state(rom))


def _johnny_pause(rom, *, remove=False):
    state = _johnny.get(rom)
    if state:
        task = state.get('task')
        if task and not task.done():
            task.cancel()
        state.update(consents=set(), busy=False, listening='', task=None, epoch=state.get('epoch', 0) + 1)
        if remove:
            _johnny.pop(rom, None)


async def _johnny_event(rom, pid, msg):
    kind = msg.get('t')
    if kind == 'johnny-propose':
        if not johnny_ai.capability()['available']:
            await _send(_rom[rom][pid]['ws'], {'t': 'johnny-error', 'text': 'Johnny må aktiveres av eieren av møtes.no først.'})
            return
        # Repeated proposals must never reset other participants' decisions.
        if not _johnny.get(rom, {}).get('proposed'):
            _johnny[rom] = {'proposed': True, 'consents': set(), 'epoch': 0, 'busy': False, 'requests': []}
    elif kind == 'johnny-consent':
        state = _johnny.get(rom)
        if state and msg.get('accept') is True:
            state['consents'].add(pid)
        elif state:
            _johnny_pause(rom, remove=True)
    elif kind == 'johnny-listening':
        if _johnny_state(rom)['active']:
            state = _johnny[rom]
            if msg.get('value') is True and not state.get('busy') and (not state.get('listening') or state.get('listener') == pid):
                state['listening'] = _rom[rom][pid]['name']
                state['listener'] = pid
            elif state.get('listener') == pid:
                state['listening'] = ''
    await _johnny_publish(rom, force=True)


@router.get('/meet/johnny/status', include_in_schema=False)
async def johnny_status():
    return johnny_ai.capability()


@router.post('/meet/johnny/ask', include_in_schema=False)
async def johnny_ask(request: Request):
    rom = _rens_rom(request.headers.get('x-meet-room', ''))
    cid = request.headers.get('x-meet-client', '')
    if not re.fullmatch(r'[0-9a-f]{32}', cid):
        raise HTTPException(403, 'Bli med i møtet først.')
    pid = _deltaker_id(cid)
    peer = _rom.get(rom, {}).get(pid)
    if not peer:
        raise HTTPException(403, 'Bli med i møtet først.')
    if not _johnny_state(rom)['active']:
        raise HTTPException(409, 'Alle deltakere må godkjenne Johnny først.')
    state = _johnny[rom]
    if state.get('busy') or (state.get('listening') and state.get('listener') != pid):
        raise HTTPException(429, 'Johnny svarer på et annet spørsmål. Vent litt.')
    now = time.time()
    state['requests'] = [t for t in state['requests'] if t > now - 60]
    _johnny_requests[:] = [t for t in _johnny_requests if t > now - 3600]
    if len(state['requests']) >= 2 or len(_johnny_requests) >= 20:
        raise HTTPException(429, 'Grensen for AI-spørsmål er nådd. Prøv igjen senere.')
    mime = request.headers.get('content-type', '').split(';', 1)[0].strip().lower()
    if mime not in {'audio/webm', 'audio/mp4', 'audio/ogg'}:
        raise HTTPException(415, 'Nettleseren leverte ikke et støttet lydformat.')
    epoch = state['epoch']
    state.update(busy=True, task=asyncio.current_task(), listening='')
    state['requests'].append(now)
    _johnny_requests.append(now)
    await _johnny_publish(rom)
    try:
        body = bytearray()
        async for chunk in request.stream():
            body.extend(chunk)
            if len(body) > 512 * 1024:
                raise HTTPException(413, 'Spørsmålet er for langt. Maks 30 sekunder.')
        if not body:
            raise HTTPException(400, 'Fant ingen lyd. Prøv igjen.')
        if ((mime == 'audio/webm' and not body.startswith(b'\x1aE\xdf\xa3'))
                or (mime == 'audio/ogg' and not body.startswith(b'OggS'))
                or (mime == 'audio/mp4' and body[4:8] != b'ftyp')):
            raise HTTPException(415, 'Lydfilen kunne ikke leses.')
        if not _johnny_state(rom)['active'] or state['epoch'] != epoch:
            raise HTTPException(409, 'Samtykket er endret. Spørsmålet er avbrutt.')
        result = await johnny_ai.answer(bytes(body), mime)
        if _johnny.get(rom) is not state or not _johnny_state(rom)['active'] or state['epoch'] != epoch:
            raise HTTPException(409, 'Samtykket er endret. Svaret deles ikke.')
        await _kringkast(rom, {'t': 'johnny-answer', **result, 'askedBy': peer['name']})
        return {'ok': True}
    except asyncio.CancelledError:
        raise HTTPException(409, 'Johnny er stoppet fordi deltakerne eller samtykket er endret.')
    except (httpx.HTTPError, ValueError, KeyError):
        raise HTTPException(502, 'Johnny fikk ikke svart nå. Sjekk AI-oppsettet eller prøv senere.')
    finally:
        if _johnny.get(rom) is state:
            state.update(busy=False, task=None, listening='')
            await _johnny_publish(rom)


def _rens_rom(r: str) -> str:
    r = (r or "").lower()
    r = r.replace("æ", "ae").replace("ø", "o").replace("å", "a")
    r = re.sub(r"[^a-z0-9-]+", "-", r).strip("-")
    return r[:32]


def _rens_navn(n: str) -> str:
    n = re.sub(r"[\x00-\x1f\x7f]", "", n or "").strip()
    return n[:40] or "Deltaker"


def _rens_state(s) -> dict:
    if not isinstance(s, dict):
        return {}
    return {
        "muted": bool(s.get("muted")),
        "sharing": bool(s.get("sharing")),
        "cam": bool(s.get("cam")),
        "shareAt": s.get("shareAt") if isinstance(s.get("shareAt"), (int, float)) else 0,
    }


INVITASJON_GYLDIG = 24 * 3600  # sekunder


def _inv_nokkel() -> bytes:
    return hashlib.sha256(b"nam-meet-invitasjon|" + KODE.encode()).digest()


def _lag_invitasjon(rom: str, utloper: int) -> str:
    sig = hmac.new(_inv_nokkel(), f"{rom}|{utloper}".encode(), hashlib.sha256).hexdigest()[:32]
    return f"{utloper}.{sig}"


def _gyldig_invitasjon(inv: str, rom: str) -> bool:
    """Sjekker at lenken er laget av oss, gjelder akkurat dette rommet og ikke har gått ut."""
    try:
        utloper_txt, sig = (inv or "").split(".", 1)
        utloper = int(utloper_txt)
    except ValueError:
        return False
    if utloper < time.time():
        return False
    return secrets.compare_digest(sig.encode(), _lag_invitasjon(rom, utloper).split(".", 1)[1].encode())


def _har_tilgang(kode: str, inv: str, rom: str) -> bool:
    if not KODE:
        return True
    if kode and secrets.compare_digest(kode.encode(), KODE.encode()):
        return True
    return _gyldig_invitasjon(inv, rom)


def _deltaker_id(cid: str) -> str:
    """Fast id for en nettleserfane, avledet fra en hemmelig klient-id.

    Fanen beholder da samme id når den kobler til igjen (f.eks. etter en deploy), slik at
    lyd og bilde kan fortsette. De andre ser bare den avledede id-en og kan ikke utgi seg
    for å være fanen.
    """
    if re.fullmatch(r"[0-9a-f]{32}", cid or ""):
        return hashlib.sha256(cid.encode()).hexdigest()[:12]
    return secrets.token_hex(6)


async def _send(ws: WebSocket, msg: dict) -> None:
    try:
        await ws.send_text(json.dumps(msg, ensure_ascii=False))
    except Exception:
        pass


async def _kringkast(rom: str, msg: dict, unntatt: str | None = None) -> None:
    for pid, d in list(_rom.get(rom, {}).items()):
        if pid != unntatt:
            await _send(d["ws"], msg)


@router.get("/meet", response_class=HTMLResponse, include_in_schema=False)
@router.get("/meet/", response_class=HTMLResponse, include_in_schema=False)
async def meet_side():
    return HTMLResponse(
        HTML_FIL.read_text(encoding="utf-8"),
        headers={
            "Cache-Control": "no-store",
            "Permissions-Policy": "microphone=(self), camera=(self), display-capture=(self)",
        },
    )


@router.post("/meet/create", include_in_schema=False)
async def meet_create(request: Request):
    """Et nytt tilfeldig rom med en invitasjon som bare gir tilgang til dette rommet.

    Den gamle felleskoden beholdes for eksisterende rom og sendes aldri til klienten.
    Oppretting reserverer ikke serverressurser: rommet lages ved første oppkobling.
    """
    host = (request.url.hostname or "").lower().rstrip(".")
    if host not in {"xn--mtes-gra.no", "www.xn--mtes-gra.no", "localhost", "127.0.0.1"}:
        return JSONResponse({"feil": "Opprett møtet på møtes.no."}, status_code=404)
    rom = "mote-" + secrets.token_hex(10)
    utloper = int(time.time()) + INVITASJON_GYLDIG
    return JSONResponse(
        {"rom": rom, "inv": _lag_invitasjon(rom, utloper), "utloper": utloper},
        headers={"Cache-Control": "no-store"},
    )


@router.get("/meet/status", include_in_schema=False)
async def meet_status():
    """Lite helsesjekk: antall aktive rom og deltakere (ingen navn)."""
    return {
        "rom": len(_rom),
        "deltakere": sum(len(m) for m in _rom.values()),
        "krever_kode": bool(KODE),
        "turn": bool(TURN_KEY_ID and TURN_TOKEN),
    }


async def _hent_turn() -> list | None:
    """Midlertidige TURN-nøkler fra Cloudflare. Den faste nøkkelen forlater aldri serveren."""
    url = f"https://rtc.live.cloudflare.com/v1/turn/keys/{TURN_KEY_ID}/credentials/generate-ice-servers"
    async with httpx.AsyncClient(timeout=5) as klient:
        r = await klient.post(url, headers={"Authorization": f"Bearer {TURN_TOKEN}"}, json={"ttl": TURN_TTL})
    r.raise_for_status()
    servere = r.json().get("iceServers")
    if isinstance(servere, dict):
        servere = [servere]
    if not isinstance(servere, list) or not servere:
        raise ValueError("uventet svar fra Cloudflare")
    for s in servere:  # port 53 blokkeres av nettlesere og gir bare tidsavbrudd
        if isinstance(s.get("urls"), list):
            s["urls"] = [u for u in s["urls"] if ":53?" not in u and not u.endswith(":53")]
    return servere


@router.get("/meet/invite", include_in_schema=False)
async def meet_invite(rom: str = Query(""), kode: str = Query(""), inv: str = Query("")):
    """Invitasjonslenke til ett rom. Med tilgangskoden får du en ny lenke som gjelder i 24 timer.
    Er du selv invitert, får du samme lenke tilbake (den forlenges ikke)."""
    rom = _rens_rom(rom)
    if len(rom) < 3:
        return JSONResponse({"feil": "ugyldig rom"}, status_code=400)
    if not KODE:
        return JSONResponse({"rom": rom, "inv": "", "utloper": None}, headers={"Cache-Control": "no-store"})
    if kode and secrets.compare_digest(kode.encode(), KODE.encode()):
        utloper = int(time.time()) + INVITASJON_GYLDIG
        return JSONResponse({"rom": rom, "inv": _lag_invitasjon(rom, utloper), "utloper": utloper}, headers={"Cache-Control": "no-store"})
    if _gyldig_invitasjon(inv, rom):
        return JSONResponse({"rom": rom, "inv": inv, "utloper": int(inv.split(".", 1)[0])}, headers={"Cache-Control": "no-store"})
    return JSONResponse({"feil": "ingen tilgang"}, status_code=401)


@router.get("/meet/ice", include_in_schema=False)
async def meet_ice(kode: str = Query(""), inv: str = Query(""), rom: str = Query("")):
    """ICE-servere til nettleseren: STUN alltid, TURN når det er satt opp."""
    if not _har_tilgang(kode, inv, _rens_rom(rom)):
        return JSONResponse({"feil": "feil tilgangskode"}, status_code=401)
    if not (TURN_KEY_ID and TURN_TOKEN):
        return JSONResponse({"iceServers": STUN, "turn": False}, headers={"Cache-Control": "no-store"})
    if _ice_cache["servere"] is None or time.time() > _ice_cache["til"]:
        try:
            _ice_cache["servere"] = await _hent_turn()
            _ice_cache["til"] = time.time() + TURN_FORNY
        except Exception as exc:
            log.warning("møtes.no: fikk ikke TURN-nøkler fra Cloudflare: %r", exc)
            if _ice_cache["servere"] is None or time.time() > _ice_cache["til"] + TURN_TTL - 2 * TURN_FORNY:
                return JSONResponse({"iceServers": STUN, "turn": False}, headers={"Cache-Control": "no-store"})
    return JSONResponse({"iceServers": _ice_cache["servere"], "turn": True}, headers={"Cache-Control": "no-store"})


@router.websocket("/meet/ws/{rom}")
async def meet_ws(
    ws: WebSocket,
    rom: str,
    name: str = Query("Deltaker"),
    kode: str = Query(""),
    cid: str = Query(""),
    inv: str = Query(""),
):
    await ws.accept()
    rom = _rens_rom(rom)
    if not _har_tilgang(kode, inv, rom):
        await ws.close(code=4401)
        return
    if len(rom) < 3:
        await ws.close(code=4400)
        return

    async with _las:
        medlemmer = _rom.setdefault(rom, {})
        pid = _deltaker_id(cid)
        gammel = medlemmer.get(pid)
        if gammel is None and len(medlemmer) >= MAKS:
            await ws.close(code=4409)
            return
        meg = {"ws": ws, "name": _rens_navn(name), "state": {}}
        andre = [{"id": k, "name": v["name"], "state": v["state"]} for k, v in medlemmer.items() if k != pid]
        medlemmer[pid] = meg

    _johnny_pause(rom)
    if gammel is not None:
        # Samme fane har koblet til på nytt før den gamle forbindelsen ble ryddet bort.
        try:
            await gammel["ws"].close(code=4000)
        except Exception:
            pass

    await _send(ws, {"t": "welcome", "id": pid, "peers": andre})
    await _kringkast(rom, {"t": "peer-joined", "peer": {"id": pid, "name": meg["name"], "state": {}}}, unntatt=pid)

    await _johnny_publish(rom)
    try:
        while True:
            raw = await ws.receive_text()
            if len(raw) > MAKS_MELDING:
                continue
            try:
                msg = json.loads(raw)
            except ValueError:
                continue
            if not isinstance(msg, dict):
                continue
            t = msg.get("t")
            if isinstance(t, str) and t.startswith('johnny-'):
                await _johnny_event(rom, pid, msg)
            elif t == "sig":
                mal = _rom.get(rom, {}).get(msg.get("to"))
                if mal:
                    await _send(mal["ws"], {"t": "sig", "from": pid, "data": msg.get("data")})
            elif t == "state":
                meg["state"] = _rens_state(msg.get("state"))
                await _kringkast(rom, {"t": "state", "id": pid, "state": meg["state"]}, unntatt=pid)
            elif t == "chat":
                tekst = str(msg.get("text", "")).strip()[:500]
                if tekst:
                    await _kringkast(rom, {"t": "chat", "from": pid, "name": meg["name"], "text": tekst})
            elif t == "bye":
                meg["bye"] = True
            elif t == "ping":
                await _send(ws, {"t": "pong"})
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        async with _las:
            medlemmer = _rom.get(rom, {})
            fjernet = medlemmer.get(pid) is meg
            if fjernet:
                medlemmer.pop(pid)
            if not medlemmer:
                _rom.pop(rom, None)
        if fjernet:
            _johnny_pause(rom, remove=not bool(_rom.get(rom)))
            await _johnny_publish(rom)
            # bye=True: deltakeren gikk selv. Ellers kan det være et kort brudd (f.eks. ny deploy),
            # og klientene beholder lyd og bilde en stund i tilfelle deltakeren kommer tilbake.
            await _kringkast(rom, {"t": "peer-left", "id": pid, "bye": bool(meg.get("bye"))})
