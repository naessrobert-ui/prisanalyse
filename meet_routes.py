"""
NAM Meet – møterom med lyd, kamera, skjermdeling og chat.

Denne modulen gir:
  GET  /meet                 -> selve møtesiden (nam_meet.html)
  WS   /meet/ws/{rom}        -> oppkobling (signalering) mellom deltakerne

Lyd og bilde går kryptert direkte mellom nettleserne (WebRTC).
Serveren formidler bare oppkoblingen, chat og hvem som er i rommet.
Ingenting lagres: alt ligger i minnet og forsvinner når rommet tømmes.

Miljøvariabler:
  NAM_MEET_KODE   Tilgangskode deltakerne må skrive inn. Sterkt anbefalt.
                  Står den tom, kan alle som kjenner adressen bli med.
  NAM_MEET_MAKS   Maks deltakere per rom (standard 10).

NB: Rommene ligger i minnet til prosessen. Kjør med én worker
(uvicorn uten --workers, eller --workers 1), ellers havner deltakere
i samme rom på hver sin prosess og finner ikke hverandre.
"""
import asyncio
import hashlib
import json
import os
import re
import secrets
from pathlib import Path

from fastapi import APIRouter, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse

router = APIRouter()

HTML_FIL = Path(__file__).with_name("nam_meet.html")
KODE = os.environ.get("NAM_MEET_KODE", "")
MAKS = int(os.environ.get("NAM_MEET_MAKS", "10"))
MAKS_MELDING = 64_000  # bytes; SDP-tilbud er typisk 3–8 kB

# rom -> {deltaker_id -> {"ws": WebSocket, "name": str, "state": dict}}
_rom: dict[str, dict[str, dict]] = {}
_las = asyncio.Lock()


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


@router.get("/meet/status", include_in_schema=False)
async def meet_status():
    """Lite helsesjekk: antall aktive rom og deltakere (ingen navn)."""
    return {"rom": len(_rom), "deltakere": sum(len(m) for m in _rom.values()), "krever_kode": bool(KODE)}


@router.websocket("/meet/ws/{rom}")
async def meet_ws(
    ws: WebSocket, rom: str, name: str = Query("Deltaker"), kode: str = Query(""), cid: str = Query("")
):
    await ws.accept()
    rom = _rens_rom(rom)
    if KODE and not secrets.compare_digest(kode.encode(), KODE.encode()):
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

    if gammel is not None:
        # Samme fane har koblet til på nytt før den gamle forbindelsen ble ryddet bort.
        try:
            await gammel["ws"].close(code=4000)
        except Exception:
            pass

    await _send(ws, {"t": "welcome", "id": pid, "peers": andre})
    await _kringkast(rom, {"t": "peer-joined", "peer": {"id": pid, "name": meg["name"], "state": {}}}, unntatt=pid)

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
            if t == "sig":
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
            # bye=True: deltakeren gikk selv. Ellers kan det være et kort brudd (f.eks. ny deploy),
            # og klientene beholder lyd og bilde en stund i tilfelle deltakeren kommer tilbake.
            await _kringkast(rom, {"t": "peer-left", "id": pid, "bye": bool(meg.get("bye"))})
