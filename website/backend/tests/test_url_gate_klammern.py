"""Das URL-Gate schnitt Links an der ersten Klammer ab.

Befund (2026-09-05, beim Onkologie-Refresh #136 aufgelaufen): Das Gate
`check-new-urls` blockierte den PR mit

    [404] https://eur-lex.europa.eu/legal-content/DE/TXT/?uri=CELEX:32022H1213(01

Die schliessende Klammer fehlt — der Link selbst liefert 200. Ursache war die
Zeichenklasse in `URL_RE`, die ``)`` ausschloss: die Erfassung endete an der
ersten Klammer. Zusaetzlich strich `clean_url` nachgestellte Klammern
bedingungslos.

Der Ausschluss hatte einen legitimen Grund — Links in Fliesstext-Klammern wie
``(siehe https://example.com)`` sollen die Klammer nicht mitnehmen. Die
Unterscheidung ist aber nicht „Klammer ja/nein", sondern „balanciert oder
nicht": eine schliessende Klammer ohne oeffnende gehoert zum Text, eine mit
gehoert zur URL.

**Vorbestehend**, nicht durch #136 verursacht: in `data/*.json` stehen laengst
Cell- und Current-Biology-DOIs mit Klammern
(``…/fulltext/S2405-4712(20)30203-0``). Sie fielen nur nie auf, weil das Gate
ausschliesslich NEU hinzugekommene URLs prueft — und sie waren schon da.

Fuer `tools/check_new_urls.py` gab es bis hierher keinen Test.
"""

import re
import sys
from pathlib import Path

BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))

from tools.check_new_urls import URL_RE, clean_url  # noqa: E402

# Der alte Ausdruck — zur Kalibrierung: der Test muss gegen ihn rot werden.
ALT_RE = re.compile(r'https?://[^\s"<>\)]+')


def _erste(regex, text):
    treffer = regex.findall(text)
    return clean_url(treffer[0]) if treffer else ""


# --------------------------------------------------------------------------
# Die konkreten Faelle, an denen es aufgeschlagen ist
# --------------------------------------------------------------------------

EUR_LEX = "https://eur-lex.europa.eu/legal-content/DE/TXT/?uri=CELEX:32022H1213(01)"
CELL = "https://www.cell.com/cell-systems/fulltext/S2405-4712(20)30203-0"
CURRBIO = "https://www.cell.com/current-biology/fulltext/S0960-9822(14)00770-9"


def test_eur_lex_url_bleibt_vollstaendig():
    assert _erste(URL_RE, f'"{EUR_LEX}"') == EUR_LEX


def test_cell_dois_bleiben_vollstaendig():
    """Diese stehen seit Langem in den Daten — das Gate haette sie ebenso
    zerschnitten, wenn sie je als 'neu' aufgetaucht waeren."""
    for u in (CELL, CURRBIO):
        assert _erste(URL_RE, f'"{u}"') == u


def test_der_alte_ausdruck_zerschneidet_sie():
    """Kalibrierung an einem echten Positiv: ohne den Fix ist der Test rot."""
    assert _erste(ALT_RE, f'"{EUR_LEX}"') != EUR_LEX
    assert _erste(ALT_RE, f'"{EUR_LEX}"').endswith("(01")
    assert _erste(ALT_RE, f'"{CELL}"').endswith("(20")


# --------------------------------------------------------------------------
# Die Gegenrichtung: Fliesstext-Klammern gehoeren NICHT zur URL
# --------------------------------------------------------------------------

def test_fliesstext_klammer_wird_weiterhin_abgeschnitten():
    for text, erwartet in (
        ("(siehe https://example.com)", "https://example.com"),
        ("Quelle (https://example.com/a/b).", "https://example.com/a/b"),
        ("[https://example.com/x]", "https://example.com/x"),
    ):
        assert _erste(URL_RE, text) == erwartet, text


def test_balancierte_klammer_am_ende_bleibt():
    u = "https://de.wikipedia.org/wiki/Merkur_(Planet)"
    assert _erste(URL_RE, f"Siehe {u}") == u


def test_gemischter_fall_klammer_in_der_url_und_im_satz():
    """URL mit eigener Klammer, zusaetzlich in Satzklammern gesetzt."""
    u = "https://de.wikipedia.org/wiki/Merkur_(Planet)"
    assert _erste(URL_RE, f"(siehe {u})") == u


def test_nachgestellte_satzzeichen_weiterhin_weg():
    for text, erwartet in (
        ("https://example.com.", "https://example.com"),
        ("https://example.com,", "https://example.com"),
        ("https://example.com;", "https://example.com"),
        ('"https://example.com"', "https://example.com"),
    ):
        assert _erste(URL_RE, text) == erwartet, text


def test_url_ohne_klammern_unveraendert():
    u = "https://www.uspreventiveservicestaskforce.org/uspstf/recommendation/breast-cancer-screening"
    assert _erste(URL_RE, f'"{u}"') == u


def test_clean_url_terminiert_auch_bei_nur_satzzeichen():
    assert clean_url("...") == ""
    assert clean_url("") == ""


# --------------------------------------------------------------------------
# Der Bestand muss den geprueften Ausdruck ueberstehen
# --------------------------------------------------------------------------

def test_keine_url_im_bestand_wird_zerschnitten():
    """Jede URL in data/*.json, die eine Klammer enthaelt, muss nach der
    Extraktion wieder genau so dastehen wie in der Datei."""
    import json
    kaputt = []
    for pfad in sorted((BACKEND / "data").glob("*.json")):
        roh = pfad.read_text(encoding="utf-8")
        for feld in ("source_url", "secondary_url", "url"):
            for treffer in re.finditer(rf'"{feld}": "(https?://[^"]+)"', roh):
                original = treffer.group(1)
                if "(" not in original and ")" not in original:
                    continue
                if _erste(URL_RE, f'"{original}"') != original:
                    kaputt.append((pfad.name, original))
        json.loads(roh)  # nebenbei: die Datei muss valide bleiben
    assert not kaputt, f"vom Extraktor zerschnitten: {kaputt}"
