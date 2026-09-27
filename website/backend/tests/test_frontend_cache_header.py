"""Das Frontend muss jede Auslieferung revalidieren lassen.

Anlass (26.9.2026, beim Nachmessen von PR #209). Der Deploy war durch, die
neue style.css lag im Container — und der Browser zeigte weiter das alte
Layout. Gemessen an den Antwort-Headern von evidora.eu:

    style.css   ETag, Last-Modified — KEIN Cache-Control
    index.html  ETag, Last-Modified — KEIN Cache-Control

Ohne ``Cache-Control`` darf ein Browser heuristische Frische annehmen
(ueblich: ein Zehntel des Alters seit ``Last-Modified``) und die Datei
ausliefern, ohne beim Server nachzufragen. Fuer einen Dienst, dessen
Dateinamen keinen Hash tragen, heisst das: Ein Frontend-Fix erreicht die
Nutzer erst irgendwann.

``no-cache`` verbietet nicht das Speichern, sondern die Verwendung OHNE
Rueckfrage. Mit dem ETag, das nginx ohnehin setzt, kostet die Rueckfrage
ein 304 ohne Body.

Diese Suite prueft die Konfiguration, nicht den laufenden Container — sie
soll verhindern, dass die Zeile beim naechsten Umbau still verschwindet.
Kein Netz, kein Docker.
"""

import re
from pathlib import Path

import pytest

NGINX = Path(__file__).resolve().parents[2] / "frontend" / "nginx.conf"
DOCKERFILE = Path(__file__).resolve().parents[2] / "frontend" / "Dockerfile"
CONF = NGINX.read_text(encoding="utf-8")


def test_nginx_conf_existiert():
    assert NGINX.is_file(), NGINX


def test_statische_dateien_werden_revalidiert():
    assert re.search(r'add_header\s+Cache-Control\s+"no-cache"', CONF), CONF


def test_der_header_gilt_auch_fuer_fehlerantworten():
    """Ohne `always` setzt nginx den Header bei 304/404 nicht — und genau
    die 304-Antwort ist der Normalfall dieser Regel."""
    m = re.search(r'add_header\s+Cache-Control\s+"no-cache"\s+always\s*;', CONF)
    assert m, "add_header … always fehlt"


def test_der_header_steht_im_statischen_location_block():
    """Er darf nicht versehentlich im /api/-Proxy landen."""
    static_block = re.search(r"location\s+/\s*\{(.*?)\n    \}", CONF, re.S)
    assert static_block, "location / nicht gefunden"
    assert "Cache-Control" in static_block.group(1)
    api_block = re.search(r"location\s+/api/\s*\{(.*?)\n    \}", CONF, re.S)
    assert api_block, "location /api/ nicht gefunden"
    assert "Cache-Control" not in api_block.group(1)


def test_die_konfiguration_landet_im_image():
    assert "COPY nginx.conf /etc/nginx/conf.d/default.conf" in DOCKERFILE.read_text(encoding="utf-8")


@pytest.mark.parametrize("datei", ["index.html", "style.css", "app.js", "i18n.js"])
def test_die_ausgelieferten_dateien_tragen_keinen_hash(datei):
    """Der Grund fuer `no-cache`: Ein Dateiname ohne Hash kann sich nicht
    selbst als neue Datei ausweisen. Kaeme je ein Build mit gehashten Namen,
    waere diese Regel durch eine lange max-age zu ersetzen."""
    assert f"COPY {datei} /usr/share/nginx/html/" in DOCKERFILE.read_text(encoding="utf-8")
