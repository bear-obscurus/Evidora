"""OpenAQ lieferte sieben Wochen 401 — wegen EINEM Zeichen in der .env.

Gemessen am 31.10.2026, nachdem der ntfy-Alarm die bekannt defekten
Quellen wieder in den Blick brachte.

## Die Diagnose, die sieben Wochen gefehlt hat

Drei Abfragen vom Prod-Host trennen die Faelle:

    ohne Key                "Unauthorized. A valid API key must be
                             provided in the X-API-Key header"
                            -> unser Header-Name waere falsch
    Authorization: Bearer   dieselbe Meldung
                            -> das Schema ist unveraendert
    X-API-Key: <unser Key>  {"detail":"Invalid credentials"}
                            -> die API nimmt den Header an und weist
                               den SCHLUESSEL ab

Damit war klar: Unser Code ist richtig, der Schluessel nicht. Und die
FORM zeigte warum — 65 Zeichen, davon eines ausserhalb hex, an Position
0. Mit entferntem ersten Zeichen antwortete `/v3/parameters` sofort mit
**HTTP 200**.

⚠️ Der Eintrag im Waechter-Register nannte „65 Zeichen" seit dem
2026-09-07 und riet daneben „vermutlich abgelaufen oder widerrufen".
Die Zahl stand also sieben Wochen da — nur hat niemand 65 gegen 64
gerechnet. Die Vermutung ist jetzt durch die Messung ersetzt.

## Was der Code daraus lernt

``schluessel_form`` nennt beim ersten Gebrauch die GESTALT des
Schluessels im Log — Laenge und Zeichenklassen, **nie den Wert**. Mit
dieser einen Zeile waere die Diagnose eine Minute gewesen statt sieben
Wochen.

Die Grenze ist streng: Der Wert darf NICHT ins Log (#153
Log-Redaktion, und #237, wo ein Alarm-Topic im Log stand und damit das
Zugangsmittel verriet). Auch nicht gekuerzt, auch nicht die ersten
Zeichen — die Funktion gibt Zaehlungen zurueck, keine Zeichen.

Dependency-light: nur `_ausfall`, kein Netz.
"""
import os
import sys

import pytest

BACKEND = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BACKEND)

from services._ausfall import schluessel_form  # noqa: E402


def test_der_echte_fall_wird_benannt():
    """65 Zeichen, eines ausserhalb hex, an Position 0 — genau die
    Gestalt des OpenAQ-Schluessels in der .env."""
    form = schluessel_form("%" + "a1b2c3d4" * 8)
    assert "65" in form
    assert "Position 0" in form
    assert "ausserhalb hex" in form


def test_der_reparierte_schluessel_sieht_unauffaellig_aus():
    assert schluessel_form("a1b2c3d4" * 8) == "Laenge 64, hex"


@pytest.mark.parametrize("wert,erwartet_in", [
    ("", "nicht gesetzt"),
    ("abcdef", "hex"),
    ("sk-live-ABCdef", "ausserhalb hex"),
    ("A" * 40, "hex"),                      # Hex ist case-insensitive
    ("xyz123", "alphanumerisch"),           # kein Hex, aber kein Satzzeichen
])
def test_die_gestalt_wird_richtig_benannt(wert, erwartet_in):
    assert erwartet_in in schluessel_form(wert), wert


def test_ein_leerzeichen_in_der_mitte_faellt_auf():
    """Der zweite plausible .env-Unfall: ein Zeilenumbruch oder
    Leerzeichen mitten im Wert."""
    form = schluessel_form("a" * 32 + " " + "b" * 32)
    assert "Position 32" in form


# ------------------------------------------------------- die harte Grenze

GEHEIM = "deadbeef" * 8          # 64 hex, sieht aus wie ein echter Key


def test_der_wert_kommt_NIE_vor():
    """Die wichtigste Eigenschaft. #153 redigiert Keys aus dem Log, #237
    fand ein Alarm-Topic darin — diese Funktion darf das nicht wieder
    aufreissen."""
    form = schluessel_form(GEHEIM)
    assert GEHEIM not in form
    # Auch kein Praefix und kein Suffix:
    for n in (4, 6, 8, 12):
        assert GEHEIM[:n] not in form, f"Praefix {n}"
        assert GEHEIM[-n:] not in form, f"Suffix {n}"


def test_auch_bei_kaputter_form_kein_wert_im_log():
    kaputt = "%" + GEHEIM
    form = schluessel_form(kaputt)
    assert GEHEIM not in form
    assert "deadbeef" not in form
    # Das fremde Zeichen selbst wird NICHT ausgegeben, nur seine Position.
    assert "%" not in form


def test_die_ausgabe_ist_kurz_und_zaehlt_nur():
    """Eine Zeile Log, keine Daten — sonst wird aus der Hilfe ein Leck."""
    for wert in ("", "a" * 64, "%" + "a" * 64, "sk-" + "x" * 50):
        form = schluessel_form(wert)
        assert len(form) < 90, (wert[:3], form)
        assert not any(c in form for c in "%$&="), form


# ------------------------------------------------ der Dienst nutzt es auch

def test_openaq_loggt_die_form_genau_einmal():
    """Einmal je Prozess, nicht je Abfrage — sonst steht der Hinweis bei
    jedem Faktencheck im Log."""
    import services.openaq as oq
    quelle = open(oq.__file__, encoding="utf-8").read()
    assert "schluessel_form(api_key)" in quelle
    assert "_FORM_GEMELDET" in quelle
    assert quelle.count("logger.info(f\"OpenAQ: API-Key-Form") == 1


def test_die_geschichte_des_befundes_bleibt_lesbar():
    """Der Eintrag ist raus, weil die Quelle wieder laeuft — aber die
    Ursache bleibt als Spur im Werkzeug. Sonst beginnt die naechste
    401-Diagnose wieder bei einer Vermutung (#295).

    Die alte Vermutung „vermutlich abgelaufen oder widerrufen" stand
    sieben Wochen und war falsch; sie darf nicht als Erklaerung
    stehenbleiben.
    """
    sys.path.insert(0, os.path.join(BACKEND, "tools"))
    import quellen_lebenszeichen as QM
    assert "OpenAQ" not in QM.BEKANNT_DEFEKT
    import re
    roh = open(QM.__file__, encoding="utf-8").read()
    # Kommentarzeichen UND Umbrueche raus — sonst sind sie tragend.
    quelle = re.sub(r"\s+", " ", re.sub(r"(?m)^\s*#\s?", "", roh))
    assert "2026-10-31" in quelle
    assert "versprengtes Satzzeichen" in quelle
    assert "Invalid credentials" in quelle, (
        "die Meldung, die Header-Fehler von Key-Fehler trennt")
    i = quelle.index("versprengtes Satzzeichen")
    assert "FALSCH" in quelle[max(0, i - 400):i], (
        "die alte Vermutung muss als falsch markiert sein")
