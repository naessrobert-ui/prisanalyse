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
        raise RuntimeError("NEWSWEB_CONSENSUS_BUCKET må være en separat privat S3-bøtte.")
    client=boto3.client("s3",region_name=os.getenv("AWS_REGION") or "eu-north-1")
    # Mandatory: never write proprietary estimates to an S3 bucket that may
    # allow anonymous/public access, including legacy 'prisanalyse-data'.
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

def safe_comparisons(item:dict, snapshots:list[dict])->list[dict]:
    """Private on-request comparison, never added to public NewsWeb JSON."""
    report=item.get("financial_report")
    if not isinstance(report,dict):return []
    period_=report.get("period")
    if not period_ or not report.get("metrics"):return []
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
    if not selected.get("amount_units_confirmed"):
        return []
    recs=selected.get("records") or {}
    results=[]
    for m in report.get("metrics",[]):
        k=str(m.get("name") or "")
        rec=recs.get(f"{symbol}|{period_}|{k}")
        if not rec:continue
        if rec.get("unit_type")==("per_share" if k=="eps" else "millions"):
            actual=m.get("current")
            estimate=rec.get("value")
            # Without an explicit currency column, Bloomberg quote currency
            # cannot be safely matched to PDF report currency. We present
            # raw indicative values and flag this uncertainty.
            if not isinstance(actual,(int,float)) or not isinstance(estimate,(int,float)):
                continue
            unit=str(m.get("unit","")).lower().strip()
            if k=="eps":
                if not ("/share" in unit or "/aksje" in unit or "per share" in unit):
                    continue
            elif not any(w in unit for w in ("million","mill.","mio","mill ", " mn")):
                continue
            if not math.isfinite(actual) or not math.isfinite(estimate):
                continue
            results.append({"metric":k,"label":METRIC_DISPLAY.get(k,k),
                "actual":round(actual,3),"consensus":round(estimate,3),
                "difference":round(actual-estimate,3),
                "surprise_pct":round(100*(actual-estimate)/abs(estimate),1) if estimate else None,
                "asof":selected["uploaded_at"],
                "source_field":rec.get("source_field"),
                "unit":m.get("unit",""),
                "note":"Indikativt: kontrollér valuta, justert/rapportert definisjon og periodisering"})
    return results
