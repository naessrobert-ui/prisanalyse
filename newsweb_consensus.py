"""Private, manually uploaded Bloomberg BEST snapshots for NewsWeb comparisons.

Upload files never enter GitHub or the public NewsWeb JSON. The S3 bucket/prefix
must be private. Store only spreadsheet cached values, with server-generated asof.
No Excel formula is evaluated here.
"""
from __future__ import annotations

import io
import math
import os
import re
import uuid
import zipfile
from datetime import datetime, timezone
from xml.etree import ElementTree as ET

_NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
MAX_XLSX_BYTES = 3_000_000
MAX_UNCOMPRESSED = 17_000_000
MAX_COMPANIES = 3500
S3_PREFIX = "newsweb/consensus/snapshots/"
METRIC_FIELDS = {
    "BEST_NET_INCOME": "net_income",
    "BEST_EBIT": "ebit",
    "BEST_EPS": "eps",
    "BEST_EBITDA": "ebitda",
    # A historical/accounting NET_INT_INC field is NOT consensus; require BEST.
    "BEST_NET_INT_INC": "net_interest_income",
}
METRIC_DISPLAY = {
    "net_income": "Resultat etter skatt",
    "ebit": "EBIT",
    "eps": "Resultat per aksje (EPS)",
    "ebitda": "EBITDA",
    "net_interest_income": "Netto renteinntekter",
}
_QUARTER = re.compile(r"^(\d{2})Q([1-4])$", re.I)
_TICKER = re.compile(r"^([A-Z0-9._-]{1,25})\s+NO\s+EQUITY$", re.I)
_COL = re.compile(r"^([A-Z]{1,3})(\d+)$")

def _colno(ref: str) -> int:
    letters = _COL.match(ref)
    if not letters: return -1
    value=0
    for ch in letters.group(1):
        value=value*26+ord(ch)-ord("A")+1
    return value-1

def _sheet_data(payload: bytes) -> list[list[object]]:
    if len(payload)>MAX_XLSX_BYTES or len(payload)<100:
        raise ValueError("Excel-filen er for stor eller ugyldig (maks 3 MB).")
    try:
        z=zipfile.ZipFile(io.BytesIO(payload))
    except (zipfile.BadZipFile,OSError) as ex:
        raise ValueError("Ugyldig Excel-fil.") from ex
    with z:
        info=z.infolist()
        if len(info)>300 or sum(i.file_size for i in info)>MAX_UNCOMPRESSED:
            raise ValueError("Excel-filen overskrider sikker størrelsesgrense.")
        files=set(z.namelist())
        if "xl/workbook.xml" not in files:
            raise ValueError("Mangler arbeidsbok.")
        # This specific export has a 'Data' worksheet. Locate via the actual
        # workbook relationship, rather than assuming any file named sheet1.
        wb=ET.fromstring(z.read("xl/workbook.xml"))
        sheets=wb.find(_NS+"sheets")
        data_sheet=next((x for x in sheets if x.attrib.get("name","").strip().lower()=="data"),None) if sheets is not None else None
        if data_sheet is None:
            raise ValueError('Excel-arket må hete "Data".')
        rel_id=data_sheet.attrib.get("{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id")
        rel_root=ET.fromstring(z.read("xl/_rels/workbook.xml.rels"))
        link=next((x for x in rel_root if x.attrib.get("Id")==rel_id),None)
        if link is None:
            raise ValueError("Finner ikke data-arket.")
        target=link.attrib.get("Target","")
        sheetfile=target.lstrip("/") if target.startswith("/") else "xl/"+target
        import posixpath
        sheetfile=posixpath.normpath(sheetfile)
        if not sheetfile.startswith("xl/worksheets/") or sheetfile not in files:
            raise ValueError("Ugyldig Excel-arkreferanse.")
        strings=[]
        if "xl/sharedStrings.xml" in files:
            strings_xml=ET.fromstring(z.read("xl/sharedStrings.xml"))
            for si in strings_xml.findall(_NS+"si"):
                strings.append("".join((t.text or "") for t in si.iter(_NS+"t")))
        root=ET.fromstring(z.read(sheetfile))
        sheet=root.find(_NS+"sheetData")
        rows=[]
        if sheet is None: raise ValueError("Ingen rader i Excel-arket.")
        for row in sheet:
            if len(rows)>MAX_COMPANIES+5:
                raise ValueError("For mange rader.")
            number=int(row.attrib.get("r",len(rows)+1))
            if number<1 or number>MAX_COMPANIES+10:
                raise ValueError("Ugyldige radnumre.")
            while len(rows)<number: rows.append([])
            output=rows[number-1]
            for c in row.findall(_NS+"c"):
                col=_colno(c.attrib.get("r",""))
                if col<0 or col>50: continue
                v=c.find(_NS+"v")
                typ=c.attrib.get("t","")
                raw=v.text if v is not None else None
                if typ=="inlineStr":
                    inline=c.find(_NS+"is")
                    value="".join((e.text or "") for e in inline.iter(_NS+"t")) if inline is not None else ""
                elif typ=="s" and raw is not None:
                    value=strings[int(raw)] if 0<=int(raw)<len(strings) else ""
                elif typ in ("str","e"):
                    value=raw or ""
                elif raw is None:
                    value=None  # Formula without a cached value is NOT an estimate
                else:
                    try: value=float(raw)
                    except ValueError: value=raw
                while len(output)<=col: output.append(None)
                output[col]=value
    return rows

def period(raw: object, annual_year: int|None) -> str|None:
    s=str(raw or "").strip().upper()
    match=_QUARTER.fullmatch(s)
    if match:return "20"+match.group(1)+"Q"+match.group(2)
    if s=="YEAR" and annual_year:
        return f"{annual_year}FY"
    return None

def parse_excel(payload: bytes, *, annual_year: int|None, amount_units_confirmed: bool) -> dict:
    if annual_year is not None and annual_year not in range(2025,2033):
        raise ValueError("Regnskapsår må være fra 2025 til 2032.")
    rows=_sheet_data(payload)
    if len(rows)<3: raise ValueError("Excel-filen mangler datarader.")
    header1=rows[0]; header2=rows[1]
    cols=[]
    for j in range(2,max(len(header1),len(header2))):
        year=header1[j] if j<len(header1) else ""
        metric=(str(header2[j]) if j<len(header2) else "").strip().upper()
        p=period(year,annual_year)
        key=METRIC_FIELDS.get(metric)
        if p and key:
            cols.append((j,p,key))
    if not cols:raise ValueError("Fant ingen gjenkjente kvartaler og estimatfelt.")
    records={}
    companies=set()
    errors=0
    for row in rows[2:]:
        raw=str(row[0] or "").strip() if row else ""
        m=_TICKER.fullmatch(raw)
        if not m: continue
        symbol=m.group(1).upper()
        name=str(row[1] or "")[:120] if len(row)>1 else ""
        companies.add(symbol)
        for col,p,key in cols:
            val=row[col] if len(row)>col else None
            if not isinstance(val,(int,float)) or isinstance(val,bool) or not math.isfinite(val):
                errors+=1
                continue
            # Statement facts are not reported in this sheet: these are estimates.
            # Very large values may indicate scaling errors; leave them unavailable.
            if abs(val)>1e13:
                errors+=1
                continue
            rec={"ticker":symbol,"bloomberg_symbol":raw,"name":name,
                 "period":p,"metric":key,"value":round(float(val),8),
                 "unit_type":"per_share" if key=="eps" else "millions",
                 "currency":"reporting_currency_unverified",
                 "source_field":str(header2[col])}
            records[f"{symbol}|{p}|{key}"]=rec
    if not records:
        raise ValueError("Ingen gyldige, bufrede Bloomberg-tall funnet. Oppdater filen i Excel før opplasting.")
    return {"format":"newsweb_best_snapshot_v1",
            "annual_year":annual_year,
            "amount_units_confirmed":bool(amount_units_confirmed),
            "records":records,
            "company_count":len(companies),
            "value_count":len(records),
            "invalid_or_missing_cells":errors,
            "periods":sorted({x["period"] for x in records.values()})}

def _s3():
    import boto3
    bucket=os.getenv("NEWSWEB_CONSENSUS_BUCKET","").strip()
    if not bucket:
        raise RuntimeError("NEWSWEB_CONSENSUS_BUCKET må peke til en privat S3-bøtte.")
    client=boto3.client("s3",region_name=os.getenv("AWS_REGION") or "eu-north-1")
    # Existing S3 buckets are fine, but all four public-access blocks are
    # mandatory. Store only under the newsweb/consensus/snapshots/ prefix.
    try:
        block=client.get_public_access_block(Bucket=bucket)["PublicAccessBlockConfiguration"]
    except Exception as exc:
        raise RuntimeError("Kan ikke bekrefte S3 Public Access Block. Opplasting stoppet.") from exc
    if not all(block.get(x) is True for x in
            ("BlockPublicAcls","IgnorePublicAcls","BlockPublicPolicy","RestrictPublicBuckets")):
        raise RuntimeError("S3-bøtten må blokkere all offentlig tilgang.")
    return client,bucket

def store_snapshot(snapshot:dict, *, original_name:str) -> str:
    import json
    client,bucket=_s3()
    dt=datetime.now(timezone.utc)
    snapshot["uploaded_at"]=dt.isoformat(timespec="seconds")
    snapshot["original_name"]=str(original_name)[:120]
    stamp=dt.strftime("%Y%m%dT%H%M%SZ")
    key=S3_PREFIX+stamp+"-"+uuid.uuid4().hex[:10]+".json"
    client.put_object(Bucket=bucket,Key=key,
                      Body=json.dumps(snapshot,ensure_ascii=False,separators=(",",":")).encode("utf-8"),
                      ContentType="application/json",ServerSideEncryption="AES256",
                      CacheControl="private,no-store")
    return key

def list_snapshots(limit:int=200)->list[dict]:
    client,bucket=_s3()
    pages=client.get_paginator("list_objects_v2").paginate(Bucket=bucket,Prefix=S3_PREFIX)
    result=[]
    for page in pages:
        for x in page.get("Contents",[]):
            key=x.get("Key","")
            if key.endswith(".json"):
                result.append({"key":key,"modified":x.get("LastModified")})
        # Allow at most 10k snapshots to prevent pathological long requests
        if len(result)>10000: break
    return sorted(result,key=lambda x:x["key"],reverse=True)[:limit]

def load_snapshot(key:str)->dict:
    if not key.startswith(S3_PREFIX) or not key.endswith(".json") or "/" in key[len(S3_PREFIX):]:
        raise ValueError("Ugyldig snapshot ID")
    import json
    client,bucket=_s3()
    obj=client.get_object(Bucket=bucket,Key=key)
    blob=obj["Body"].read(2_000_001)
    if len(blob)>2_000_000:
        raise ValueError("Estimatsnapshotet er uventet stort.")
    return json.loads(blob)

def _dt(value:str)->datetime|None:
    try:
        d=datetime.fromisoformat(value.replace("Z","+00:00"))
        return d.astimezone(timezone.utc) if d.tzinfo else None
    except (ValueError,TypeError,AttributeError):
        return None

def period_from_estimates(item:dict,snapshot:dict)->str:
    """Infer missing period only from this ticker's uploaded quarterly estimates."""
    from newsweb_monitor.report_analysis import inferred_reporting_quarter
    ticker=str(item.get("ticker") or "").upper()
    records=snapshot.get("records") or {}
    available={str(row.get("period") or "") for row in records.values()
               if isinstance(row,dict) and str(row.get("ticker") or "").upper()==ticker}
    try:
        expected=inferred_reporting_quarter(str(item.get("published_at") or ""))
    except (ValueError,TypeError):
        return ""
    return expected if expected in available else ""

def _actual_in_estimate_units(metric:dict)->tuple[float,str]|None:
    """Convert PDF currency units to millions; leave EPS per share."""
    actual=metric.get("current")
    if isinstance(actual,bool) or not isinstance(actual,(int,float)) or not math.isfinite(actual):
        return None
    unit=str(metric.get("unit") or "").lower().strip()
    name=str(metric.get("name") or "")
    if name=="eps":
        if any(x in unit for x in ("/share","/aksje","per share","per aksje")):
            return float(actual),str(metric.get("unit"))
        return None
    if any(x in unit for x in ("million","mill.","mio","mill "," mn")):
        return float(actual),str(metric.get("unit"))
    if any(x in unit for x in ("thousand","tusen","'000"," 000s")):
        return float(actual)/1000,str(metric.get("unit"))+" → millioner"
    if re.fullmatch(r"[a-z]{3}",unit) and unit.upper() in (
        "USD","NOK","SEK","DKK","EUR","GBP","CHF","CAD","AUD","JPY","CNY","KRW","PLN"):
        return float(actual)/1_000_000,str(metric.get("unit"))+" → millioner"
    return None

def safe_comparisons(item:dict, snapshots:list[dict])->list[dict]:
    """Private on-request comparison, never added to public NewsWeb JSON."""
    report=item.get("financial_report")
    if not isinstance(report,dict):return []
    period_=report.get("period")
    if not report.get("metrics"):return []
    published=_dt(item.get("published_at",""))
    if published is None:return []
    symbol=str(item.get("ticker","")).upper()
    # From all uploaded versions, pick newest snapshot strictly *before* news.
    selected=None
    for snap in snapshots:
        stamp=_dt(snap.get("uploaded_at",""))
        if stamp and stamp<published:
            selected=snap
            break
    if not selected:return []
    if not period_:
        period_=period_from_estimates(item,selected)
        if not period_:return []
    if not selected.get("amount_units_confirmed"):
        return []
    recs=selected.get("records") or {}
    results=[]
    for m in report.get("metrics",[]):
        k=str(m.get("name") or "")
        rec=recs.get(f"{symbol}|{period_}|{k}")
        if not rec:continue
        if rec.get("unit_type")==("per_share" if k=="eps" else "millions"):
            normalized=_actual_in_estimate_units(m)
            if normalized is None:continue
            actual,display_unit=normalized
            estimate=rec.get("value")
            # Without an explicit currency column, Bloomberg quote currency
            # cannot be safely matched to PDF report currency. We present
            # raw indicative values and flag this uncertainty.
            if not isinstance(actual,(int,float)) or not isinstance(estimate,(int,float)):
                continue
            if not math.isfinite(actual) or not math.isfinite(estimate):
                continue
            results.append({"metric":k,"label":METRIC_DISPLAY.get(k,k),
                "actual":round(actual,3),"consensus":round(estimate,3),
                "difference":round(actual-estimate,3),
                "surprise_pct":round(100*(actual-estimate)/abs(estimate),1) if estimate else None,
                "asof":selected["uploaded_at"],
                "source_field":rec.get("source_field"),
                "unit":display_unit,
                "note":"Indikativt: kontrollér valuta, justert/rapportert definisjon og periodisering"})
    return results


def simulate_today(snapshot:dict, items:list[dict], *, now:datetime|None=None)->dict:
    """One-time upload rehearsal: compare all today's Oslo disclosures against
    values treated as if uploaded 24 hours earlier. Pure: NEVER mutate snapshot,
    its genuine uploaded_at, or the source site reports.

    Data is stored only inside the PRIVATE S3 snapshot; never in public GitHub.
    """
    from datetime import timedelta
    from zoneinfo import ZoneInfo
    now=now or datetime.now(timezone.utc)
    oslo=ZoneInfo("Europe/Oslo")
    target_day=now.astimezone(oslo).date()
    assumed_asof=now-timedelta(days=1)
    rehearsal=dict(snapshot)
    rehearsal["uploaded_at"]=assumed_asof.isoformat(timespec="seconds")
    todays=[]
    for raw in items:
        if not isinstance(raw,dict): continue
        published=_dt(raw.get("published_at",""))
        if published and published.astimezone(oslo).date()==target_day:
            todays.append(raw)
    todays.sort(key=lambda i:i.get("published_at",""),reverse=True)
    found=0
    matched_metrics=0
    result_items=[]
    from collections import Counter
    reasons=Counter()
    for item in todays:
        label=str(item.get("headline") or item.get("title") or "")[:210]
        ticker=str(item.get("ticker") or "")[:25]
        report=item.get("financial_report")
        rows=[]
        if not isinstance(report,dict) or not report.get("metrics"):
            status="Ingen ekstraherte regnskapstall"
            reasons["no_financial_metrics"]+=1
        elif not report.get("period"):
            status="Rapportperiode mangler"
            reasons["missing_period"]+=1
        elif not snapshot.get("amount_units_confirmed"):
            status="Beløpsenheter ikke bekreftet"
            reasons["units_unconfirmed"]+=1
        else:
            rows=safe_comparisons(item,[rehearsal])
            if rows:
                status="Simulert sammenligning tilgjengelig"
            else:
                status="Ikke sammenlignbare estimater/tall"
                reasons["no_comparable_values"]+=1
        if rows:
            found+=1
            matched_metrics+=len(rows)
        result_items.append({
            "ticker":ticker, "title":label,
            "published_at":str(item.get("published_at") or "")[:45],
            "message_url":str(item.get("url") or "")[:260],
            "period":str((report or {}).get("period") or "")[:15] if isinstance(report,dict) else "",
            "status":status,
            "comparisons":rows
        })
    return {
        "simulation_only":True,
        "run_at":now.isoformat(timespec="seconds"),
        "target_day":target_day.isoformat(),
        "effective_asof":assumed_asof.isoformat(timespec="seconds"),
        "messages_scanned":len(todays),
        "messages_with_matches":found,
        "metrics_matched":matched_metrics,
        "unmatched_reasons":dict(reasons),
        "items":result_items[:500]
    }


def overlay_private_consensus(items:list[dict], snapshots:list[dict],
                              *,now:datetime|None=None)->tuple[int,int]:
    """Attach actual PIT and, separately, same-day simulated consensus to PRIVATE copies.

    The simulation is display-only, explicitly labelled, and cannot alter
    actual as-of timestamps, archival snapshots, or public NewsWeb JSON.
    """
    from zoneinfo import ZoneInfo
    from datetime import timedelta
    now=now or datetime.now(timezone.utc)
    today=now.astimezone(ZoneInfo("Europe/Oslo")).date().isoformat()
    actual_count=0
    simulation_count=0
    latest_sim=None
    for snap in snapshots:
        sim=snap.get("simulation")
        if (isinstance(sim,dict) and sim.get("simulation_only") is True
                and sim.get("target_day")==today
                and isinstance(sim.get("items"),list)):
            latest_sim=sim
            break
    simulation_map={}
    if latest_sim:
        for row in latest_sim["items"]:
            if not isinstance(row,dict) or not row.get("comparisons"):
                continue
            identifier=(str(row.get("ticker") or "").upper(),
                        str(row.get("published_at") or ""))
            simulation_map[identifier]=row["comparisons"]
    for item in items:
        actual=safe_comparisons(item,snapshots)
        if actual:
            item["consensus_comparison"]=actual
            item["consensus_kind"]="actual"
            actual_count+=1
            continue
        identifier=(str(item.get("ticker") or "").upper(),
                    str(item.get("published_at") or "")[:45])
        simulated=simulation_map.get(identifier)
        if simulated:
            item["consensus_comparison"]=simulated
            item["consensus_kind"]="simulation"
            item["consensus_simulated_asof"]=latest_sim.get("effective_asof")
            simulation_count+=1
    return actual_count,simulation_count
