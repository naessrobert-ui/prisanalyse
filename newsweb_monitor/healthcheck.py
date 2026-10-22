"""Public, non-sensitive NewsWeb run diagnostics and freshness checks."""
from __future__ import annotations
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT=Path(__file__).resolve().parent
OSLO=ZoneInfo("Europe/Oslo")

def summarize(stats:dict,now:datetime|None=None)->dict:
    now=now or datetime.now(timezone.utc)
    local=now.astimezone(OSLO)
    active=local.weekday()<5 and 8<=local.hour<17
    source_time=stats.get("source_latest_at")
    source_age=None
    if source_time:
        try:
            parsed=datetime.fromisoformat(source_time.replace("Z","+00:00"))
            source_age=round(max(0,(now-parsed).total_seconds()/3600),2)
        except (TypeError,ValueError):
            pass
    failures=int(stats.get("ai_failed",0))+int(stats.get("details_failed",0))
    fetched=int(stats.get("fetched",0))
    analyzed=int(stats.get("analyzed",0))
    eligible=int(stats.get("eligible",0))
    problems=[]
    warnings=[]
    if active and fetched==0:
        problems.append("NewsWeb API returned zero XOSL messages in the 48-hour index")
    if active and eligible>0 and analyzed==0 and failures>0:
        problems.append(f"{eligible} eligible announcements, zero analyzed; {failures} processing failures")
    if active and source_age is not None and source_age>=4:
        warnings.append(f"Latest NewsWeb source message is {source_age:.1f} hours old; check source/feed")
    if active and fetched>0 and analyzed==0 and not problems:
        warnings.append("No new analyzed messages; check already_seen/filtered counters")
    return {"status":"error" if problems else "warning" if warnings else "ok",
            "problems":problems,"warnings":warnings,"source_age_hours":source_age,
            "market_hours":active}

def main()->int:
    path=ROOT/"reports"/"latest.json"
    report=json.loads(path.read_text(encoding="utf-8"))
    stats=report.get("diagnostics") or {}
    if not stats:
        print("::error::NewsWeb run did not write diagnostics")
        return 1
    check=summarize(stats)
    print("NewsWeb source: fetched={fetched}, recent={recent}, already_seen={already_seen}, "
          "eligible={eligible}, filtered={filtered}, analyzed={analyzed}, "
          "ai_failed={ai_failed}, details_failed={details_failed}, "
          "capped={capped}, recovered_unpublished={recovered_unpublished}".format(**stats))
    for w in check["warnings"]:
        print("::warning::"+w)
    for e in check["problems"]:
        print("::error::"+e)
    with open(os.environ.get("GITHUB_STEP_SUMMARY",os.devnull),"a",encoding="utf-8") as out:
        out.write("\n## NewsWeb healthcheck\n\n")
        out.write(f"**Status:** {check['status']}\n\n")
        out.write("| Hentet fra kilde | Innenfor 36 t | Tidligere sett | Nye kandidater | Filtrert | Analysert | AI-feil | Detaljfeil | Over grense |\n")
        out.write("|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n")
        out.write("| "+" | ".join(str(stats.get(k,0)) for k in
            ("fetched","recent","already_seen","eligible","filtered","analyzed","ai_failed","details_failed","capped"))+" |\n\n")
        out.write("Gjenopprettet fra upublisert cache: "+str(stats.get("recovered_unpublished",0))+"\\n\\n")
        out.write("Siste melding i kilden: "+str(stats.get("source_latest_at") or "ukjent")+"\n\n")
        for message in check["warnings"]+check["problems"]:
            out.write("- "+message+"\n")
    return 1 if check["problems"] else 0

if __name__=="__main__":
    sys.exit(main())
