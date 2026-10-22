from datetime import timedelta
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from flask import Flask, redirect, request, session
from flask_session import Session

from site_remember_access import (
    REMEMBER_COOKIE, REMEMBER_DAYS, REMEMBER_SECONDS, RememberAccess,
)


class RememberAccessTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.app = self.make_app("first")
        self.client = self.app.test_client()

    def make_app(self, name, code="test-access-code", secret="test-server-secret"):
        app = Flask(__name__)
        app.config.update(
            TESTING=True, SECRET_KEY=secret, SESSION_TYPE="filesystem",
            SESSION_FILE_DIR=str(Path(self.directory.name) / name),
            SESSION_PERMANENT=False,
            PERMANENT_SESSION_LIFETIME=timedelta(days=REMEMBER_DAYS),
        )
        Session(app)
        access = RememberAccess(app, code)

        @app.post("/grant")
        def grant():
            return access.logged_in(redirect("/private"), remember=request.form.get("remember") == "1")

        @app.get("/private")
        def private():
            return ("allowed", 200) if session.get("access_granted") else ("denied", 401)

        @app.get("/logout")
        def logout():
            return access.logged_out(redirect("/"))

        return app

    def login(self, remember=True):
        return self.client.post("/grant", data={"remember": "1"} if remember else {})

    def token(self):
        return self.client.get_cookie(REMEMBER_COOKIE).value

    def test_cookie_is_persistent_and_protected(self):
        response = self.login()
        header = next(h for h in response.headers.getlist("Set-Cookie") if h.startswith(REMEMBER_COOKIE + "="))
        for flag in ("Max-Age=" + str(REMEMBER_SECONDS), "Secure", "HttpOnly", "SameSite=Lax", "Path=/"):
            self.assertIn(flag, header)
        self.assertNotIn("test-access-code", header)

    def test_survives_browser_restart_with_only_remember_cookie(self):
        self.login()
        restarted = self.app.test_client()
        restarted.set_cookie(REMEMBER_COOKIE, self.token())
        self.assertEqual(restarted.get("/private").status_code, 200)

    def test_survives_deploy_with_empty_session_directory(self):
        self.login()
        deployed = self.make_app("new-deploy").test_client()
        deployed.set_cookie(REMEMBER_COOKIE, self.token())
        self.assertEqual(deployed.get("/private").status_code, 200)

    def test_deleted_session_files_are_recreated(self):
        self.login()
        for path in Path(self.app.config["SESSION_FILE_DIR"]).iterdir():
            path.unlink()
        self.assertEqual(self.client.get("/private").status_code, 200)

    def test_logout_removes_remembered_access(self):
        self.login()
        self.client.get("/private")
        self.client.get("/logout")
        self.assertIsNone(self.client.get_cookie(REMEMBER_COOKIE))
        self.assertEqual(self.client.get("/private").status_code, 401)

    def test_unchecked_remember_uses_browser_session_only(self):
        response = self.login(remember=False)
        self.assertIsNone(self.client.get_cookie(REMEMBER_COOKIE))
        self.assertEqual(self.client.get("/private").status_code, 200)
        session_header = next(h for h in response.headers.getlist("Set-Cookie") if h.startswith("session="))
        self.assertNotIn("Expires=", session_header)
        self.assertEqual(self.app.test_client().get("/private").status_code, 401)

    def test_forged_token_cannot_grant_access(self):
        self.login()
        forged = self.token() + "forged"
        fresh = self.app.test_client()
        fresh.set_cookie(REMEMBER_COOKIE, forged)
        self.assertEqual(fresh.get("/private").status_code, 401)

    def test_expiry_revokes_access_even_with_surviving_session(self):
        with patch("itsdangerous.timed.time.time", return_value=1800000000):
            self.login()
        with patch("itsdangerous.timed.time.time", return_value=1800000000 + REMEMBER_SECONDS + 1):
            self.assertEqual(self.client.get("/private").status_code, 401)

    def test_missing_remember_cookie_revokes_remembered_session(self):
        self.login()
        self.client.delete_cookie(REMEMBER_COOKIE)
        self.assertEqual(self.client.get("/private").status_code, 401)

    def test_code_or_secret_rotation_revokes_remembered_access(self):
        self.login()
        for changes in ({"code": "new-code"}, {"secret": "new-secret"}, {"code": ""}):
            with self.subTest(changes=changes):
                deployed = self.make_app("first", **changes).test_client()
                deployed.set_cookie(REMEMBER_COOKIE, self.token())
                # Retain the old server session too, as during a rolling deploy.
                deployed.set_cookie("session", self.client.get_cookie("session").value)
                self.assertEqual(deployed.get("/private").status_code, 401)


if __name__ == "__main__":
    unittest.main()
