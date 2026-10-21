"""Brief NewsWeb API smoke check: inspect only schema and first returned item."""
import json
from urllib.request import Request,urlopen
base="https://api3.oslo.oslobors.no/v1/newsreader/"
queries=[
  "list",
  "list?market=XOSL",
  "list?category=1001",
  "list?fromDate=2026-10-20&toDate=2026-10-21",
  "message?messageId=684456",
  "get?messageId=684456",
]
for query in queries:
 try:
  with urlopen(Request(base+query,headers={"Accept":"application/json","User-Agent":"Mozilla/5.0"}),timeout=12) as r:
   raw=r.read(120000)
  obj=json.loads(raw)
  data=obj.get("data") if isinstance(obj,dict) else {}
  print("ENDPOINT",query,"keys",list(data)[:15] if isinstance(data,dict) else type(data).__name__,
     "lengths",{k:len(v) for k,v in data.items() if isinstance(v,(list,dict))} if isinstance(data,dict) else {},
     "header",obj.get("header",{}).get("result.text") if isinstance(obj,dict) else "")
  if isinstance(data,dict):
   for k,v in data.items():
    if isinstance(v,list) and v:
     print("FIRST",k,json.dumps(v[0],ensure_ascii=False)[:2500]); break
    if k in ("message","messages") and isinstance(v,dict):
     print("FIRST",k,json.dumps(v,ensure_ascii=False)[:2500])
 except Exception as e:
  print("FAILED",query,type(e).__name__,str(e)[:160])
