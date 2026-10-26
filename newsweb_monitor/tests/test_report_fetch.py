import json
import os
import unittest
from importlib.util import find_spec
from unittest.mock import patch
from urllib.error import HTTPError

if find_spec("flask") is not None:
    import newsweb_routes
else:
    newsweb_routes = None

SNAPSHOT={"mode":"live","generated_at":"2026-10-26T07:16:40+00:00","items":[{"id":"1"}]}

class _Response:
    def __init__(self,payload,etag=""):
        self._data=json.dumps(payload).encode()
        self.headers={"ETag":etag}
    def read(self,_limit):
        return self._data
    def __enter__(self):
        return self
    def __exit__(self,*_):
        return False

@unittest.skipIf(newsweb_routes is None, "Flask only installed in web-app test job")
class ReportFetchTests(unittest.TestCase):
    def setUp(self):
        newsweb_routes._cache.update(checked_at=0.0,report=None,etag="",fresh_until=0.0)

    def test_token_reads_api_without_raw_cdn(self):
        seen=[]
        def fake(req,timeout):
            seen.append(req.full_url)
            return _Response(SNAPSHOT,'"abc"')
        with patch.dict(os.environ,{"NEWSWEB_GITHUB_TOKEN":"t"}),patch.object(newsweb_routes,"urlopen",fake):
            report,_=newsweb_routes.read_report()
        self.assertEqual(report["generated_at"],SNAPSHOT["generated_at"])
        self.assertEqual(seen,[newsweb_routes.PUBLIC_REPORT_API_URL])
        self.assertEqual(newsweb_routes._cache["etag"],'"abc"')

    def test_not_modified_keeps_cached_report(self):
        newsweb_routes._cache.update(report=SNAPSHOT,etag='"abc"')
        def fake(req,timeout):
            self.assertEqual(req.get_header("If-none-match"),'"abc"')
            raise HTTPError(req.full_url,304,"Not Modified",{},None)
        with patch.dict(os.environ,{"NEWSWEB_GITHUB_TOKEN":"t"}),patch.object(newsweb_routes,"urlopen",fake):
            report,_=newsweb_routes.read_report()
        self.assertIs(report,SNAPSHOT)

    def test_api_failure_falls_back_to_raw(self):
        def fake(req,timeout):
            if req.full_url==newsweb_routes.PUBLIC_REPORT_API_URL:
                raise HTTPError(req.full_url,401,"Bad credentials",{},None)
            return _Response(SNAPSHOT)
        with patch.dict(os.environ,{"NEWSWEB_GITHUB_TOKEN":"t"}),patch.object(newsweb_routes,"urlopen",fake):
            report,_=newsweb_routes.read_report()
        self.assertEqual(report["generated_at"],SNAPSHOT["generated_at"])

    def test_without_token_uses_raw(self):
        seen=[]
        def fake(req,timeout):
            seen.append(req.full_url)
            return _Response(SNAPSHOT)
        with patch.dict(os.environ,{"NEWSWEB_GITHUB_TOKEN":""}),patch.object(newsweb_routes,"urlopen",fake):
            newsweb_routes.read_report()
        self.assertEqual(seen,[newsweb_routes.PUBLIC_REPORT_URL])

    def test_manual_run_shortens_cache_window(self):
        calls=[]
        def fake(req,timeout):
            calls.append(1)
            return _Response(SNAPSHOT)
        with patch.dict(os.environ,{"NEWSWEB_GITHUB_TOKEN":""}),patch.object(newsweb_routes,"urlopen",fake):
            newsweb_routes.read_report()
            newsweb_routes._cache["checked_at"]-=15
            newsweb_routes.read_report()
            self.assertEqual(len(calls),1)
            newsweb_routes.expect_fresh_report()
            newsweb_routes.read_report()
        self.assertEqual(len(calls),2)

if __name__=="__main__":
    unittest.main()
