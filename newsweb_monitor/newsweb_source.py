"""Authorized Oslo Børs NewsWeb newsreader integration.

Uses observed official API endpoints; no browser scraping. Only Oslo Børs (XOSL).
"""
from __future__ import annotations
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode
from urllib.request import Request, urlopen
import json
import re
import os

API = "https://api3.oslo.oslobors.no/v1/newsreader"
MAX_BYTES = 5_000_000
HEADERS = {"Accept": "application/json", "User-Agent": "Prisanalyse-NewsMonitor/1.0"}

def get_json(path: str, params: dict | None = None) -> dict:
    url = API + "/" + path + ("?" + urlencode(params) if params else "")
    with urlopen(Request(url, headers=HEADERS), timeout=35) as response:
        payload = response.read(MAX_BYTES + 1)
    if len(payload) > MAX_BYTES:
        raise ValueError("NewsWeb JSON exceeded 5 MB")
    data = json.loads(payload)
    head = data.get("header", {})
    if head.get("result.val", 0) != 0:
        raise ValueError(f"NewsWeb API error: {head.get('result.text')}")
    return data.get("data", {})

def iso(value: str) -> str:
    # NewsWeb ISO timestamps already carry Z.
    when = datetime.fromisoformat(value.replace("Z","+00:00"))
    if not when.tzinfo:
        raise ValueError("Naive timestamp")
    return when.isoformat()

def index(hours: int = 48) -> list[dict]:
    """Read last two days to tolerate delayed GitHub jobs, with ID-based dedup downstream."""
    now = datetime.now(timezone.utc)
    start = (now-timedelta(hours=hours)).date().isoformat()
    end = (now+timedelta(days=1)).date().isoformat()
    data = get_json("list", {"market":"XOSL","fromDate":start,"toDate":end})
    messages = data.get("messages", [])
    if not isinstance(messages,list):
        raise ValueError("NewsWeb messages has unexpected shape")
    if data.get("overflow"):
        raise RuntimeError("NewsWeb list overflowed; widen pagination or narrow dates")
    output=[]
    for x in messages:
        if x.get("test") or "XOSL" not in x.get("markets",[]):
            continue
        mid = x.get("messageId") or x.get("id")
        if not str(mid).isdigit():
            continue
        categories = x.get("category") or []
        categories = categories if isinstance(categories,list) else [categories]
        cat = " ".join(str(c.get("category_en") or c.get("category_no") or "") for c in categories if isinstance(c,dict)).strip()
        output.append({
            "id":str(mid),"market":"XOSL","instrument_type":"equity",
            # NewsWeb lists all issuer disclosures, incl bonds; classify bond/rates in filter.
            "ticker":str(x.get("issuerSign") or "").upper(),
            "issuer":str(x.get("issuerName") or ""),
            "issuer_id":str(x.get("issuerId") or ""),
            "title":str(x.get("title") or ""),
            "body":"","url":f"https://newsweb.oslobors.no/message/{mid}",
            "published_at":iso(str(x.get("publishedTime"))),
            "category":cat,"num_attachments":int(x.get("numbAttachments") or 0),
            "correction_for":str(x.get("correctionForMessageId") or ""),
        })
    # De-duplicate multi-market / language repeats by ID only; do not drop corrections.
    return list({x["id"]:x for x in output}.values())

def details(item: dict) -> dict:
    data = get_json("message", {"messageId":item["id"]})
    msg = data.get("message", {})
    if not isinstance(msg,dict):
        raise ValueError("NewsWeb message missing")
    item=dict(item)
    item["body"]=str(msg.get("body") or msg.get("text") or "")[:70_000]
    raw=msg.get("attachments") or msg.get("attachment") or []
    item["attachments"]=raw if isinstance(raw,list) else []
    return item

def is_financial_report(item: dict) -> bool:
    title=(item.get("title") or "").lower()
    cat=(item.get("category") or "").lower()
    return bool(re.search(
        r"\b(?:q[1-4]|[1-4]q)\b|kvartal|quarter|interim report|half.year|halvår|annual report|"
        r"årsrapport|full.year|årsresultat|year.end|financial results|resultater",
        title)) or ("financial report" in cat)

def is_bond_notice(item:dict) -> bool:
    title=(item.get("title") or "").lower()
    cat=(item.get("category") or "").lower()
    if "adjustment of interest rate" in cat or "renteregulering" in cat:
        return True
    return bool(re.search(r"\b(rentefastsettelse|ny rente|interest adjustment|bond listing|"
                          r"obligasjonslån|tier [12] bond|covered bond)\b",title))

def download_attachment(message_id: str, attachment_id: str, max_bytes: int=22_000_000) -> bytes:
    if not (str(message_id).isdigit() and str(attachment_id).isdigit()):
        raise ValueError("Invalid attachment IDs")
    url=API+"/attachment?"+urlencode({"messageId":message_id,"attachmentId":attachment_id})
    with urlopen(Request(url,headers={"User-Agent":HEADERS["User-Agent"]}),timeout=60) as resp:
        data=resp.read(max_bytes+1)
    if len(data)>max_bytes:
        raise ValueError("Report attachment exceeds 22 MB")
    if not (data.startswith(b"%PDF-") or data[:2]==b"PK"):
        raise ValueError("Unsupported report attachment (expected PDF/ZIP)")
    return data
