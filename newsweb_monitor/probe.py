"""One-shot NewsWeb API schema check; never outputs non-public report content."""
import json
from urllib.request import urlopen, Request
u="https://api3.oslo.oslobors.no/v1/newsreader/list?category=1001"
try:
    with urlopen(Request(u,headers={"User-Agent":"Mozilla/5.0 (NewsMonitor/1.0)","Accept":"application/json"}),timeout=15) as resp:
        blob=resp.read(500_001)
        print("status",resp.status,"content-type",resp.headers.get("Content-Type"),"bytes",len(blob))
    data=json.loads(blob)
    print("root",type(data).__name__)
    if isinstance(data,dict):
        print("fields",[(k,type(v).__name__) for k,v in data.items()])
        for key,v in data.items():
            if isinstance(v,list) and v and isinstance(v[0],dict):
                print("array_field",key,"keys",list(v[0].keys()))
                print("example",json.dumps(v[0],ensure_ascii=False)[:2800])
                break
    elif isinstance(data,list):
        print("count",len(data))
        if data and isinstance(data[0],dict):
            print("keys",list(data[0]))
            print("example",json.dumps(data[0],ensure_ascii=False)[:2800])
except Exception as exc:
    print("PROBE_FAILED",type(exc).__name__,str(exc)[:400])
