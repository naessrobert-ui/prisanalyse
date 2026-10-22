"""Collapse multilingual NewsWeb disclosures about the SAME event.

A pair must have the same ticker, near-identical publication time, and a
specific matching topic. Timestamp/ticker alone are NEVER sufficient: a
company can publish earnings, dividends and buy-backs in the same minute.
This operates solely on already-public, sanitized fields.
"""
from __future__ import annotations

import copy
import re
from datetime import datetime
from urllib.parse import urlparse

PAIR_WINDOW_SECONDS = 12

def _stamp(raw:object):
    try:
        value=datetime.fromisoformat(str(raw or "").replace("Z","+00:00"))
        return value.timestamp() if value.tzinfo else None
    except (TypeError, ValueError, OverflowError):
        return None

def _title(row:dict)->str:
    return str(row.get("title") or "").casefold()

def _language(title:str)->str:
    """Best-effort label for a link, never decisive for grouping."""
    s=title.casefold()
    norwegian=len(re.findall(
        r"\b(?:resultater|kvartal|regnskapsrapport|kvartalsrapport|"
        r"kvartalsberetning|utstedelse|vellykket|vurderer|ansvarlig|"
        r"soliditet|underlag|passasjertall|entreprenørmarkedet|"
        r"utbytte|tilbakekjøp|søknad|vedtak|innkalling|påminnelse|"
        r"mottatt|invitasjon|oppdatering|egne aksjer|aksjeprogram|ansatte|kjøp)\b",s))
    english=len(re.findall(
        r"\b(?:results|quarter|quarterly|interim|issuance|successful|"
        r"contemplating|subordinated|capital adequacy|passengers|"
        r"contracting|dividend|buy.back|repurchase|application|"
        r"decision|invitation|reminder|shares|financial|trading)\b",s))
    if norwegian>english:return "Norsk"
    if english>norwegian:return "Engelsk"
    return "Original"

def _topic(row:dict)->str:
    title=_title(row)
    # Distinguish securities series: DNBA77 ≠ DNBA78 even if identical wording.
    ids=re.findall(r"\b(?:dnba\d+[a-z]*|no001\d{9})\b",title,re.I)
    if ids:return "security:"+":".join(sorted(ids))
    # Report and invitation must not be collapsed even if published together.
    if re.search(r"invitasjon|invitation|påminnelse|reminder",title):
        return "result_presentation_invite"
    if re.search(r"generalforsamling|general meeting",title):
        return "general_meeting"
    if re.search(r"aksjeprogram|share programmes|share programs|employees?|ansatte",title) and re.search(
        r"kjøp av aksjer|tilbakekjøp|buy.back|buyback|shares",title):
        return "employee_buyback"
    if re.search(r"tilbakekjøp|buy.back|buyback|repurchase",title):
        return "share_buyback"
    if re.search(r"\b(?:aksjeutbytte|utbytte|dividend)\b",title):
        return "dividend"
    if re.search(r"rentefastsettelse|ny rente|interest rate adjustment",title):
        return ""  # These can be distinct securities with identical titles.
    if re.search(r"considering|contemplating|vurderer",title) and re.search(
        r"lån|obligasjon|bond|utstedelse|issuance|issue",title):
        return "contemplated_bond"
    if re.search(r"vellykket|successful",title) and re.search(
        r"lån|obligasjon|bond|utstedelse|issuance|issue",title):
        return "completed_bond"
    if re.search(r"soliditet|capital adequacy",title):
        return "capital_adequacy"
    if re.search(r"passasjertall|passengers?",title):
        return "passenger_statistics"
    if re.search(r"entreprenørmarked|contracting markets?",title):
        return "contracting_market_outlook"
    if re.search(r"mottatt søknad|received application|søknad om opptak",title):
        return "trading_admission_application"
    if re.search(r"vedtak om opptak|decision on admission",title):
        return "trading_admission_decision"
    if re.search(r"\b(?:q[1-4]|[1-4]q)\b",title) and _doc_name(row):
        return "financial_results"
    if re.search(r"resultat|results?|financial report|interim report|quarterly report|"
                 r"(?:first|second|third|fourth|[1-4](?:st|nd|rd|th)?)\s+quarter\s+report|"
                 r"kvartalsrapport|kvartalsberetning|regnskapsrapport",title):
        return "financial_results"
    return ""

def _period(row:dict)->str:
    report=row.get("financial_report") or {}
    if not isinstance(report,dict):report={}
    p=str(report.get("period") or "").upper()
    if re.fullmatch(r"20\d{2}(?:Q[1-4]|FY)",p):return p
    text=_title(row)+" "+str((report.get("document") or {}).get("name") or "").casefold()
    m=re.search(r"\b(?:20(\d{2})\s*[-_ ]?q([1-4])|q([1-4])\s*[-_ ]?20(\d{2})|"
                r"([1-4])q(\d{2})|(\d{2})q([1-4]))\b",text)
    if m:
        a=m.groups()
        if a[0]:return "20"+a[0]+"Q"+a[1]
        if a[2]:return "20"+a[3]+"Q"+a[2]
        if a[4]:return "20"+a[5]+"Q"+a[4]
        return "20"+a[6]+"Q"+a[7]
    m=re.search(r"(?:(?:third|tredje|3\.)\s+(?:quarter|kvartal)\s+(20\d{2})|"
                r"(20\d{2})\s+(?:third|tredje|3\.)\s+(?:quarter|kvartal))",text)
    if m:return (m.group(1) or m.group(2))+"Q3"
    return ""

def _doc_name(row:dict)->str:
    f=row.get("financial_report") or {}
    return str(((f.get("document") or {}).get("name") or "")).casefold() if isinstance(f,dict) else ""

def _strong_translation_pair(left:dict,right:dict)->bool:
    topic=_topic(left)
    if not topic or topic!=_topic(right):return False
    # Distinct named loan series never merged with one another.
    if topic.startswith("security:"):return False
    if topic=="financial_results":
        lp,rp=_period(left),_period(right)
        if lp and rp and lp!=rp:return False
        # Full reports often have separate NO/EN PDFs, or the same English PDF.
        # Never combine unrelated result announcements without period or docs.
        if lp and rp and lp==rp:return True
        if _doc_name(left) and _doc_name(left)==_doc_name(right):return True
        return False
    # Other categories are tightly scoped and require complementary language.
    la,lb=_language(str(left.get("title") or "")),_language(str(right.get("title") or ""))
    if la!=lb and "Original" not in (la,lb):
        return True
    # A duplicate title can be valid, but only with a meaningful topic.
    return _title(left).strip()==_title(right).strip() and len(_title(left).split())>=5

def _versions(row:dict)->list[dict]:
    existing=row.get("language_versions")
    if isinstance(existing,list) and existing:
        return [v for v in existing if isinstance(v,dict) and v.get("id")]
    return [{"id":str(row.get("id") or ""),
             "title":str(row.get("title") or "")[:260],
             "url":str(row.get("url") or ""),
             "published_at":str(row.get("published_at") or ""),
             "language":_language(str(row.get("title") or ""))}]

def _rank(row:dict):
    fin=row.get("financial_report") or {}
    metrics=fin.get("metrics") or [] if isinstance(fin,dict) else []
    count=sum(1 for m in metrics if isinstance(m,dict) and m.get("current") is not None)
    label=_language(str(row.get("title") or ""))
    return (not row.get("skipped"), label=="Norsk",count,int(row.get("score") or 0))

def _merge(members:list[dict])->dict:
    selected=max(members,key=_rank)
    result=copy.deepcopy(selected)
    # Retain all official disclosures; user may need both PDF/language links.
    versions={}
    for member in members:
        for v in _versions(member):
            ident=str(v.get("id") or "")
            if ident and ident not in versions:
                versions[ident]={
                    "id":ident[:35],
                    "title":str(v.get("title") or "")[:260],
                    "url":str(v.get("url") or "")[:400],
                    "published_at":str(v.get("published_at") or "")[:48],
                    "language":str(v.get("language") or "Original")[:20],
                }
    result["language_versions"]=sorted(versions.values(),key=lambda x:x["published_at"],reverse=True)[:6]
    result["score"]=max(int(m.get("score") or 0) for m in members)
    result["skipped"]=all(bool(m.get("skipped")) for m in members)
    # Use latest source time for freshness even if Norwegian text is older.
    result["published_at"]=max(str(m.get("published_at") or "") for m in members)
    # A well-extracted report takes precedence over empty AI PDF attempts.
    reports=[m.get("financial_report") for m in members
             if isinstance(m.get("financial_report"),dict)]
    if reports:
        def richness(report):
            count=sum(1 for m in report.get("metrics",[])
                      if isinstance(m,dict) and m.get("current") is not None)
            return (count,report.get("status")=="analyzed",bool(report.get("period")))
        result["financial_report"]=copy.deepcopy(max(reports,key=richness))
    # Keep private comparisons of any language member; only used inside
    # authenticated Flask render, never in publisher/public JSON.
    private=[m for m in members if m.get("consensus_comparison")]
    if private:
        chosen=max(private,key=lambda m:(
            m.get("consensus_kind")=="actual",len(m["consensus_comparison"])))
        for key in ("consensus_comparison","consensus_kind","consensus_simulated_asof"):
            if key in chosen:result[key]=copy.deepcopy(chosen[key])
        differences=[abs(float(m.get("surprise_pct"))) for m in
                     result["consensus_comparison"]
                     if isinstance(m.get("surprise_pct"),(int,float))]
        result["largest_surprise_abs"]=max(differences,default=-1)
    return result

def collapse_stories(items:list[dict])->list[dict]:
    """Stable, order-independent dedup, keeping dissimilar same-company news.

    One batch might contain already-merged stories from the previous run; those
    retain their source links when another language version arrives.
    """
    ordered=sorted((item for item in items if isinstance(item,dict)),
                   key=lambda x:(str(x.get("published_at") or ""),str(x.get("id") or "")),reverse=True)
    groups=[]
    for item in ordered:
        time=_stamp(item.get("published_at"))
        matched=None
        for group in groups:
            first=group[0]
            if str(first.get("ticker") or "").upper()!=str(item.get("ticker") or "").upper():
                continue
            previous=_stamp(first.get("published_at"))
            if time is None or previous is None or abs(time-previous)>PAIR_WINDOW_SECONDS:
                continue
            if _strong_translation_pair(first,item):
                matched=group
                break
        if matched is None:
            groups.append([item])
        else:
            matched.append(item)
    result=[_merge(g) if len(g)>1 or "language_versions" in g[0] else g[0] for g in groups]
    return sorted(result,key=lambda x:(str(x.get("published_at") or ""),str(x.get("id") or "")),reverse=True)
