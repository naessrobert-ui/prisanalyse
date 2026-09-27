"""Parsing av antall favoritter fra FINN-annonsesiden (ingen nettverk)."""
from scripts import kupp_vakt as k


def test_span_pl4_ved_favorittknapp():
    html = """
    <div class="flex items-center">
      <button aria-label="Legg til i favoritter"><svg><title>Hjerte</title></svg></button>
      <span class="pl-4 text-s">37</span>
    </div>"""
    assert k.parse_favoritter(html) == 37


def test_span_pl4_uten_favoritthint_ignoreres():
    html = '<div><span class="pl-4">2019</span></div><p>Ingen favoritter her?</p>'
    # "favoritter" i fritekst uten tall foran skal ikke gi treff
    assert k.parse_favoritter(html) is None


def test_tusenskille_i_tallet():
    html = ('<div data-testid="favorite-button"><svg></svg>'
            '<span class="pl-4">1\xa0204</span></div>')
    assert k.parse_favoritter(html) == 1204


def test_aria_label_tekst():
    html = '<button aria-label="12 personer har lagret annonsen"></button>'
    assert k.parse_favoritter(html) == 12


def test_synlig_tekst():
    assert k.parse_favoritter("<p>5 har lagret denne annonsen</p>") == 5


def test_json_fallback():
    html = '<script>{"adId":1,"favoriteCount":9}</script>'
    assert k.parse_favoritter(html) == 9


def test_tom_side():
    assert k.parse_favoritter("") is None
    assert k.parse_favoritter("<html><body>Ingenting</body></html>") is None


def test_berik_favoritter_feil_gir_none(monkeypatch):
    monkeypatch.setattr(k, "_fetch", lambda *a, **kw: None)
    biler = [{"FinnKode": "1"}, {"FinnKode": "2"}]
    k.berik_favoritter(biler)
    assert [b["favoritter"] for b in biler] == [None, None]


def test_meldinger_viser_favoritter():
    b = {"Merke": "Tesla", "Modell": "Model 3", "Årstall": 2021, "Kjørelengde": 50000,
         "Pris": 250000, "forventet_pris": 300000, "rabatt_pct": 16.7,
         "rabatt_kr": 50000, "url": "u", "favoritter": 14}
    assert "Favoritter: 14" in k._formater_bil(b)
    assert "❤ 14" in k._pushover_melding([b])
    b["favoritter"] = None
    assert "Favoritter" not in k._formater_bil(b)
    assert "❤" not in k._pushover_melding([b])
