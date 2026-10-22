"""Persistent access independent of Flask's disposable filesystem sessions."""

import hashlib
import hmac

from flask import request, session
from itsdangerous import BadData, URLSafeTimedSerializer


REMEMBER_DAYS = 90
REMEMBER_SECONDS = REMEMBER_DAYS * 24 * 60 * 60
REMEMBER_COOKIE = "prisanalyse_remember_access"


class RememberAccess:
    def __init__(self, app, access_code, *, secure=True):
        self.enabled = bool(access_code)
        self.secure = secure
        # Bind to both server secret and access code: rotating either revokes
        # remembered access. No password or session data is put in the cookie.
        key = hmac.new(
            access_code.encode("utf-8"),
            str(app.secret_key).encode("utf-8"),
            hashlib.sha256,
        ).digest()
        self.serializer = URLSafeTimedSerializer(key, salt="site-remember-access-v1")
        app.before_request(self.restore)

    def valid(self, token):
        if not self.enabled or not token:
            return False
        try:
            return self.serializer.loads(token, max_age=REMEMBER_SECONDS) == "access"
        except BadData:
            return False

    def restore(self):
        if self.valid(request.cookies.get(REMEMBER_COOKIE)):
            session["access_granted"] = True
            session["access_remembered"] = True
            session.permanent = True
        elif session.pop("access_remembered", False):
            # A surviving/renewed Flask session must not outlive the signed
            # access token or keep granting access after code rotation.
            session.pop("access_granted", None)

    def logged_in(self, response, *, remember):
        session["access_granted"] = True
        session["access_remembered"] = remember
        session.permanent = remember
        if remember:
            response.set_cookie(
                REMEMBER_COOKIE,
                self.serializer.dumps("access"),
                max_age=REMEMBER_SECONDS,
                secure=self.secure or request.is_secure,
                httponly=True,
                samesite="Lax",
                path="/",
            )
        else:
            self.clear_cookie(response)
        return response

    def logged_out(self, response):
        session.pop("access_granted", None)
        session.pop("access_remembered", None)
        self.clear_cookie(response)
        return response

    def clear_cookie(self, response):
        response.delete_cookie(
            REMEMBER_COOKIE,
            path="/",
            secure=self.secure or request.is_secure,
            httponly=True,
            samesite="Lax",
        )
