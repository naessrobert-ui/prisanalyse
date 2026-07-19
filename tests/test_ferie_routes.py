from __future__ import annotations

import io
from pathlib import Path

from flask import Flask
from PIL import Image

import ferie_routes


def _app(monkeypatch) -> Flask:
    monkeypatch.delenv("FERIE_S3_BUCKET", raising=False)
    monkeypatch.delenv("S3_BUCKET_NAME", raising=False)
    monkeypatch.delenv("FERIE_UPLOAD_CODE", raising=False)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)

    root = Path(__file__).resolve().parents[1]
    app = Flask(__name__, template_folder=str(root / "templates"))
    app.secret_key = "test-secret"
    app.config["TESTING"] = True
    app.register_blueprint(ferie_routes.ferie_bp)
    return app


def test_trip_page_renders_without_s3(monkeypatch):
    app = _app(monkeypatch)

    response = app.test_client().get("/ferie/sor-england-2026/")

    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert "Sør-England" in body
    assert "The Hope Anchor" in body
    assert "Mercure Brighton Seafront Hotel" in body
    assert "Seven Sisters" in body
    assert "Dollar, Brighton" in body
    assert "FERIE_UPLOAD_CODE" in body
    assert "Reiseassistenten aktiveres" in body


def test_upload_rejects_missing_code(monkeypatch):
    app = _app(monkeypatch)

    response = app.test_client().post(
        "/ferie/sor-england-2026/bilder",
        data={},
        follow_redirects=False,
    )

    assert response.status_code == 302
    assert response.headers["Location"].endswith("/ferie/sor-england-2026/#bilder")


def test_question_endpoint_is_disabled_without_api_key(monkeypatch):
    app = _app(monkeypatch)

    response = app.test_client().post(
        "/ferie/sor-england-2026/sporsmal",
        json={"question": "Hva gjør vi i Rye?"},
    )

    assert response.status_code == 503
    assert "ikke aktivert" in response.get_json()["error"]


def test_prepare_jpeg_resizes_and_converts():
    original = Image.new("RGBA", (3000, 1800), (10, 20, 30, 128))
    source = io.BytesIO()
    original.save(source, format="PNG")

    result = ferie_routes._prepare_jpeg(source.getvalue())

    with Image.open(io.BytesIO(result)) as image:
        assert image.format == "JPEG"
        assert image.mode == "RGB"
        assert max(image.size) <= ferie_routes.MAX_IMAGE_EDGE
