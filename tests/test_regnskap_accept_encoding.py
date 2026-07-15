# -*- coding: utf-8 -*-
"""Regresjonstest for Proff-modulens Accept-Encoding-håndtering.

Bakgrunn: Proff serverer br-komprimert (brotli) innhold når "br" annonseres.
Uten en brotli-dekoder installert klarer ikke requests å pakke ut svaret, og
__NEXT_DATA__/HTML-tabellene forsvinner – som ga feilen
«Ingen __NEXT_DATA__ på siden og ingen orgnr funnet i URL.».
"""

import importlib.util

import regnskap_routes


def _brotli_decoder_available() -> bool:
    return (
        importlib.util.find_spec("brotli") is not None
        or importlib.util.find_spec("brotlicffi") is not None
    )


def test_accept_encoding_always_supports_gzip_and_deflate():
    encoding = regnskap_routes._detect_accept_encoding()
    tokens = {t.strip() for t in encoding.split(",")}
    assert "gzip" in tokens
    assert "deflate" in tokens


def test_accept_encoding_only_advertises_brotli_when_decodable():
    encoding = regnskap_routes._detect_accept_encoding()
    tokens = {t.strip() for t in encoding.split(",")}
    # "br" skal kun annonseres når vi faktisk kan dekode det.
    assert ("br" in tokens) == _brotli_decoder_available()


def test_session_uses_detected_accept_encoding():
    sess = regnskap_routes.make_session()
    assert sess.headers.get("Accept-Encoding") == regnskap_routes._ACCEPT_ENCODING
