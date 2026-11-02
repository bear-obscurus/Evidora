"""Ein toter Link war gar nicht tot — er war ausgesperrt.

Gemessen am 2.11.2026 beim Abarbeiten der 14 toten Beleg-Links. Einer
davon, die Inflationsausgleichsgesetz-FAQ des BMF, stand im Bericht als
``TOT``. Der Abruf von Hand ergab aber:

    HTTP 503  30840B
    <title>Security Check</title>
    "Unsere Systeme haben ungewöhnlichen Datenverkehr … nicht von einem
     Roboter gesendet werden."

Das ist eine Myra-Cloud-Bot-Sperre, keine fehlende Seite. #235 hat
gelernt, dass HTTP 200 nicht heisst, dass die Seite da ist. Hier gilt die
Gegenrichtung: **404 heisst nicht, dass die Seite weg ist.**

## Warum der Status allein nicht reicht

``ist_tot`` stuft nur 404/410 und erloschene Domains als tot ein — ein 503
hätte also gar nicht in der Liste landen dürfen. Dreimal nachgefragt kam
dreimal 503. Beim Lauf des Werkzeugs muss die Sperre demnach **404**
geantwortet haben: dieselbe Wand, zwei Status-Codes. Genau deshalb kann
der Status die Frage nicht entscheiden; der Rumpf muss mitgelesen werden.

## Die Marker sind an echten Rümpfen gemessen, nicht geraten

Fünf echte Antworten, 0 Fehler:

| Rumpf | erkannt |
|---|---|
| BMF/Myra-Sperre (30.835 Zeichen) | Sperre |
| ages.at 404 | keine |
| acsm.org 404 (339.050 Zeichen) | keine |
| eff.org 404 | keine |
| ssd.eff.org, lebende Seite | keine |

Auf der Myra-Seite greifen sechs Marker, der früheste (``security check``)
an Position 34 — im ``<title>``, also weit innerhalb des Fensters von
20.000 Zeichen, das ``ist_bot_wall`` liest.

## Was dieser Test nicht prüft

Dass der Beleg inhaltlich noch stimmt. Die Seite ist für jeden maschinellen
Abruf unzugänglich; der Klick auf "Bitte klicken Sie hier, wenn Sie kein
Roboter sind" ist eine Bot-Prüfung und wird nicht umgangen. Das Ergebnis
ist deshalb nur: dieser Link gehört nicht in die Tot-Liste.
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "tools"))

from check_urls import _BOT_WALL_MARKER, ist_bot_wall, ist_tot  # noqa: E402

# Wörtlich aus der Antwort vom 2.11.2026 (gekürzt, nicht umformuliert).
MYRA_RUMPF = (
    '<html><head><title>Security Check</title></head><body>'
    '<div class="heading"><h1>Security</h1><div class="row clearfix">'
    '<div class="security-check security-check-text">'
    '<h2 translate="no">Sicherheitsüberprüfung</h2>'
    '<p translate="no">Unsere Systeme haben ungewöhnlichen Datenverkehr aus '
    'Ihrem Computernetzwerk festgestellt. Diese Seite überprüft, ob die '
    'Anfragen wirklich von Ihnen und nicht von einem Roboter gesendet '
    'werden.<br><br><strong>Bitte klicken Sie '
    '<a href="/myracloud-captcha/?q=%2FContent%2FDE%2FStandardartikel">hier'
    '</a></strong></p></div></div></body></html>'
)

# Ebenfalls echt: der 404-Rumpf von eff.org/issues/vpn.
ECHTER_404_RUMPF = (
    '<html><head>'
    '<title>Page not found | Electronic Frontier Foundation</title>'
    '</head><body><h1>Page not found</h1>'
    '<p>The requested page could not be found.</p></body></html>'
)


# --------------------------------------------------------------------------
# Der Anlass
# --------------------------------------------------------------------------

def test_die_myra_sperre_wird_als_sperre_erkannt():
    assert ist_bot_wall(MYRA_RUMPF) is True


def test_der_bmf_link_zaehlt_mit_sperre_nicht_mehr_als_tot():
    """Der Fall, der den Bericht verfälscht hat: 404 PLUS Sperre."""
    befund = {"url": "https://www.bundesfinanzministerium.de/x.html",
              "status": 404, "bot_wall": True}
    assert ist_tot(befund) is False


def test_dieselbe_wand_mit_503_zaehlt_auch_nicht():
    befund = {"status": 503, "bot_wall": True}
    assert ist_tot(befund) is False


# --------------------------------------------------------------------------
# Giftprobe: der Erkenner darf echte Totmeldungen nicht verschlucken
# --------------------------------------------------------------------------

def test_ein_echter_404_rumpf_ist_keine_sperre():
    assert ist_bot_wall(ECHTER_404_RUMPF) is False


def test_404_ohne_sperre_bleibt_tot():
    """Ohne diese Zeile wäre der Fix eine Amnestie für alle 404."""
    assert ist_tot({"status": 404}) is True


def test_410_ohne_sperre_bleibt_tot():
    assert ist_tot({"status": 410}) is True


def test_erloschene_domain_bleibt_tot():
    befund = {"status": None,
              "error": "ConnectError: [Errno 8] nodename nor servname provided"}
    assert ist_tot(befund) is True


def test_erloschene_domain_bleibt_tot_auch_wenn_das_feld_dabei_steht():
    befund = {"status": None, "bot_wall": False,
              "error": "ConnectError: [Errno 8] nodename nor servname provided"}
    assert ist_tot(befund) is True


# --------------------------------------------------------------------------
# Grenzen des Erkenners
# --------------------------------------------------------------------------

def test_leerer_rumpf_ist_keine_sperre():
    assert ist_bot_wall("") is False
    assert ist_bot_wall(None) is False


def test_gross_kleinschreibung_ist_gleichgueltig():
    assert ist_bot_wall("<TITLE>SECURITY CHECK</TITLE>") is True


def test_cloudflare_abweisung_wird_erkannt():
    assert ist_bot_wall('<div id="cf_chl_opt">...</div>') is True
    assert ist_bot_wall("Attention Required! | Cloudflare") is True


def test_nur_der_vordere_rumpf_wird_gelesen():
    """Dokumentiert die Fenstergrenze — eine Sperre steht oben, nicht unten.

    Auf der echten Myra-Seite liegt der erste Marker an Position 34.
    """
    weit_hinten = "x" * 20000 + "security check"
    assert ist_bot_wall(weit_hinten) is False
    knapp_davor = "x" * 19980 + "security check"
    assert ist_bot_wall(knapp_davor) is True


def test_ein_beleg_ueber_sicherheitsluecken_ist_keine_sperre():
    """Das Wort "Sicherheit" allein darf nicht reichen.

    Sonst wäre jeder Beleg zur BSI-Lage oder zu einer CVE eine Bot-Sperre.
    """
    text = ("<title>BSI: Lage der IT-Sicherheit in Deutschland</title>"
            "<p>Die Sicherheitslage bleibt angespannt; eine Überprüfung der "
            "Systeme wird empfohlen.</p>")
    assert ist_bot_wall(text) is False


def test_marker_sind_klein_geschrieben():
    """Sonst greift der Vergleich gegen den kleingeschriebenen Rumpf nie."""
    for m in _BOT_WALL_MARKER:
        assert m == m.lower(), m


def test_kein_marker_ist_so_kurz_dass_er_ueberall_steckt():
    for m in _BOT_WALL_MARKER:
        assert len(m) >= 7, m
