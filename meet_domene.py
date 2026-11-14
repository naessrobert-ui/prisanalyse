"""Vis møtesiden på forsiden av møtes.no, i samme ASGI-prosess."""


class MeetDomeneMiddleware:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http" and scope.get("path") == "/":
            host = next(
                (value.decode("latin-1") for key, value in scope.get("headers", [])
                 if key.lower() == b"host"), ""
            ).lower().split(":", 1)[0].rstrip(".")
            if host in {"xn--mtes-gra.no", "www.xn--mtes-gra.no"}:
                scope = dict(scope, path="/meet", raw_path=b"/meet")
        await self.app(scope, receive, send)
