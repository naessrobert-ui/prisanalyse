"""Non-invasive integration smoke test for GitHub Actions. No AI or notifications."""
from .newsweb_source import index,details,is_financial_report,download_attachment
from .report_analysis import pick_attachment,text_from_pdf

def main():
    items=index(hours=48)
    print("NewsWeb XOSL messages:",len(items))
    if not items:raise RuntimeError("No recent XOSL messages found in 48h")
    latest=details(items[0])
    print("Latest message details:",bool(latest.get("body")), "attachments:",len(latest.get("attachments") or []))
    if not latest.get("body") and not latest.get("attachments"):
        raise RuntimeError("Message endpoint returned neither text nor attachments")
    for candidate in items[:100]:
        if not is_financial_report(candidate) or not candidate.get("num_attachments"):
            continue
        full=details(candidate)
        pdf=pick_attachment(full.get("attachments") or [])
        if pdf:
            b=download_attachment(full["id"],pdf["id"])
            print("Downloaded PDF bytes:",len(b))
            # Actual PDF extraction is verified by pypdf; scans may contain no text.
            if b.startswith(b"%PDF-"):
                text=text_from_pdf(b,max_pages=2,limit=8000)
                print("PDF extraction characters from first 2 pages:",len(text))
            return
    print("No financial PDF in first 100 announcements; PDF extraction not tested")

if __name__=="__main__":
    main()
