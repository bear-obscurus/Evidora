"""Der Waechter fuer das Modell, mit dem wir antworten.

Anlass: Mistral-Mail vom 30.10.2026 („Action Required: Update Your
Mistral AI Models"). Nachgemessen gegen den Katalog unseres Keys:

    Prod faehrt MISTRAL_MODEL=mistral-medium-latest
    -> Katalog: mistral-medium-latest und mistral-medium-3-5 fuehren
       EINANDER als Alias, datierte ID mistral-medium-2604
    -> das ist das Modell, das am 30.4.2027 abgeschaltet wird

Drei Luecken, die beim Nachmessen dazukamen:

1. **Die API warnt nicht.** ``deprecation`` und
   ``deprecation_replacement_model`` sind bei **allen 64** Modellen
   ``None`` — auch bei ``zai-glm-5-2``, das am Tag NACH der Messung
   abgeschaltet wurde. Ein Waechter auf dieses Feld waere wertlos; das
   ist gemessen, nicht vermutet.
2. ``_mistral_health`` ruft genau ``/v1/models`` auf und wirft die Liste
   weg — sie prueft nur den HTTP-Status.
3. Ein 404 fuer ein abgeschaltetes Modell ist in ``chat_completion``
   nicht eigens behandelt: drei Retries, dann scheitert der Check.

## Woran die Identitaet haengt — und wo das nicht geht

43 der 64 Modelle fuehren unter ihren Aliasen eine **datierte ID** im
Format JJMM (``mistral-medium-2604`` = April 2026). Wechselt die, wurde
das Modell unter uns getauscht — das Risiko jedes ``-latest``-Alias
(#285).

⚠️ **21 Modelle haben keine**, darunter ``mistral-large-4`` — genau das
Modell, das Mistral als Ersatz empfiehlt. Dort ist die Identitaet nicht
pinnbar, und der Waechter sagt das, statt eine Sicherheit zu behaupten.
``mistral-large-4-0`` darf dabei nicht als Datierung durchgehen: ``-4-0``
sind keine vier Ziffern.

Die Alias-MENGE waere empfindlicher, churnt aber harmlos
(``mistral-vibe-cli-latest`` und ``magistral-medium-latest`` haengen
heute am Medium-Eintrag). Nur die datierte ID loest einen Push aus —
ein Waechter, der bei jedem Schluckauf schreit, wird abgeschaltet (#128).

Dependency-light: Katalog-Auszug woertlich aus der echten API, kein Netz.
"""
import os
import sys

import pytest

BACKEND = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BACKEND)
sys.path.insert(0, os.path.join(BACKEND, "tools"))

from services._modell_katalog import (  # noqa: E402
    ABKUENDIGUNGEN,
    MODELL_PINS,
    datierte_id,
    finde_eintrag,
    nachricht,
    pruefe,
)
import modell_wachhund as wh  # noqa: E402

#: Woertlich aus dem Katalog unseres Keys (30.10.2026), auf die drei
#: Eintraege verkuerzt, auf die es ankommt. Die gegenseitigen Aliase sind
#: unveraendert — sie sind der eigentliche Befund.
KATALOG = [
    {"id": "mistral-medium-latest",
     "aliases": ["mistral-medium", "mistral-medium-3-5",
                 "mistral-medium-3.5", "mistral-medium-3",
                 "mistral-medium-2604", "mistral-vibe-cli-latest",
                 "mistral-vibe-cli-with-tools", "magistral-medium-latest"],
     "deprecation": None, "deprecation_replacement_model": None,
     "max_context_length": 262144},
    {"id": "mistral-large-4", "aliases": ["mistral-large-4-0"],
     "deprecation": None, "deprecation_replacement_model": None,
     "max_context_length": 1048576},
    {"id": "mistral-large-latest", "aliases": ["mistral-large-2512"],
     "deprecation": None, "deprecation_replacement_model": None,
     "max_context_length": 262144},
]


# ------------------------------------------------------- der Katalog lesen

def test_der_alias_wird_gefunden_nicht_nur_die_id():
    """Der Kern: Unser konfigurierter Name IST bei diesem Eintrag nur ein
    Alias — eine Suche ueber ``id`` allein findet ihn nicht."""
    assert finde_eintrag(KATALOG, "mistral-medium-3-5") is KATALOG[0]
    assert finde_eintrag(KATALOG, "mistral-medium-2604") is KATALOG[0]
    assert finde_eintrag(KATALOG, "gibt-es-nicht") is None


def test_die_datierte_id_wird_erkannt():
    assert datierte_id(KATALOG[0]) == "mistral-medium-2604"
    assert datierte_id(KATALOG[2]) == "mistral-large-2512"


def test_vier_null_ist_keine_datierung():
    """``mistral-large-4-0`` darf nicht als JJMM durchgehen — sonst
    behauptet der Waechter fuer large-4 eine Identitaet, die es im
    Katalog nicht gibt."""
    assert datierte_id(KATALOG[1]) == ""


def test_die_api_meldet_keine_abkuendigung():
    """Gemessen ueber alle 64 Modelle: ueberall None. Dieser Test pinnt
    die BEGRUENDUNG dafuer, dass der Waechter das Feld nicht liest —
    faellt er, war die Messung ueberholt und man kann es einfacher."""
    for eintrag in KATALOG:
        assert eintrag["deprecation"] is None
        assert eintrag["deprecation_replacement_model"] is None


# ------------------------------------------------------------- die Lagen

@pytest.mark.parametrize("modell,lage", [
    ("mistral-medium-latest", "ok"),
    ("mistral-medium-3-5", "ok"),
    ("mistral-large-latest", "ok"),
    ("mistral-large-4", "nicht_pinnbar"),
    ("gibt-es-nicht-mehr", "verschwunden"),
])
def test_jede_lage_wird_erkannt(modell, lage):
    assert pruefe(KATALOG, modell)["lage"] == lage


def test_ein_tausch_unter_dem_alias_faellt_auf():
    """Das Szenario, fuer das der Waechter da ist: Der Name bleibt, das
    Modell dahinter wechselt."""
    getauscht = [dict(KATALOG[0], aliases=["mistral-medium-3-6",
                                           "mistral-medium-2711"])]
    befund = pruefe(getauscht, "mistral-medium-latest")
    assert befund["lage"] == "getauscht"
    assert befund["datiert"] == "mistral-medium-2711"
    assert befund["erwartet"] == "mistral-medium-2604"


def test_ohne_pin_wird_es_gemeldet():
    """Ein Produktionsmodell ohne Pin ist selbst ein Befund — ohne Pin
    kann ein Tausch nicht auffallen."""
    fremd = [{"id": "mistral-tiny-latest",
              "aliases": ["mistral-tiny-2512"]}]
    assert pruefe(fremd, "mistral-tiny-latest")["lage"] == "ohne_pin"


def test_der_pin_fuer_das_prod_modell_steht_fest():
    """Gemessen am 30.10.2026. Faellt dieser Test, hat jemand den Pin
    geaendert — das soll ein sichtbarer Commit sein."""
    assert MODELL_PINS["mistral-medium-latest"] == "mistral-medium-2604"
    assert ABKUENDIGUNGEN["mistral-medium-2604"] == "2027-04-30"
    # Der naheliegende Ausweg ist keiner: large-latest steht auf
    # derselben Abschaltliste.
    assert ABKUENDIGUNGEN["mistral-large-2512"] == "2027-04-30"


# --------------------------------------- je Alarmweg eine Nachricht (#236)

@pytest.mark.parametrize("lage", sorted(wh.AUSGANG))
def test_jede_lage_baut_eine_nachricht(lage):
    text = nachricht({"lage": lage, "modell": "mistral-medium-latest",
                      "datiert": "mistral-medium-2604",
                      "erwartet": "mistral-medium-2604"})
    assert text and "mistral-medium-latest" in text, lage


def test_die_tausch_nachricht_nennt_BEIDE_ids():
    """Ohne beide Zahlen ist die Meldung nicht handlungsfaehig."""
    text = nachricht(pruefe(
        [dict(KATALOG[0], aliases=["mistral-medium-2711"])],
        "mistral-medium-latest"))
    assert "mistral-medium-2711" in text
    assert "mistral-medium-2604" in text


def test_die_ok_nachricht_nennt_das_abschaltdatum():
    text = nachricht(pruefe(KATALOG, "mistral-medium-latest"))
    assert "2027-04-30" in text


def test_nachricht_ohne_modell_stuerzt_nicht():
    assert nachricht({}) 
    assert nachricht({"lage": "verschwunden"})


# ------------------------------------------------- die Ausgangs-Tabelle

def test_jede_lage_hat_einen_ausgang():
    """Sonst fiele eine Lage in den Default und wuerde als
    „voruebergehend" gemeldet."""
    for lage in ("ok", "nicht_pinnbar", "ohne_pin", "getauscht",
                 "verschwunden"):
        assert lage in wh.AUSGANG, lage


def test_nur_befunde_pushen():
    """``nicht_pinnbar`` bleibt eine Log-Zeile (#128)."""
    assert wh.AUSGANG["ok"] == (0, False)
    assert wh.AUSGANG["nicht_pinnbar"] == (0, False)
    assert wh.AUSGANG["getauscht"][1] is True
    assert wh.AUSGANG["verschwunden"][1] is True


def test_der_exit_vertrag_haelt():
    """#236: 1 und 2 heissen „lief, hier ist ein Befund", alles andere
    ungleich 0 heisst „kam nicht zum Laufen". Die Cron-Zeile
    unterscheidet genau daran."""
    befund_codes = {c for c, push in wh.AUSGANG.values() if push}
    assert befund_codes <= {1, 2}
    for code in (wh.EX_USAGE, wh.EX_TEMPFAIL):
        assert code not in (0, 1, 2)


# ------------------------------------------------------ main() Ende zu Ende

@pytest.fixture
def _kein_push(monkeypatch):
    gesendet = []
    monkeypatch.setattr(wh, "_push",
                        lambda w, t, x, dringend=False:
                        gesendet.append((t, x, dringend)))
    return gesendet


def _umgebung(monkeypatch, key="k", modell="mistral-medium-latest"):
    for name, wert in (("MISTRAL_API_KEY", key), ("MISTRAL_MODEL", modell),
                       ("EVIDORA_ALERT_WEBHOOK", "")):
        if wert:
            monkeypatch.setenv(name, wert)
        else:
            monkeypatch.delenv(name, raising=False)


def test_ohne_key_ex_usage(monkeypatch, _kein_push):
    _umgebung(monkeypatch, key="")
    monkeypatch.setattr(sys, "argv", ["modell_wachhund.py"])
    assert wh.main() == wh.EX_USAGE
    assert not _kein_push


def test_ohne_modell_ex_usage(monkeypatch, _kein_push):
    """Der stille Rueckfall auf mistral-small-latest ist ein
    Konfigurationsfehler, kein Befund — ein anderes Modell beantwortet
    dann JEDEN Claim."""
    _umgebung(monkeypatch, modell="")
    monkeypatch.setattr(sys, "argv", ["modell_wachhund.py"])
    assert wh.main() == wh.EX_USAGE


@pytest.mark.parametrize("fehler", [
    __import__("urllib.error", fromlist=["x"]).HTTPError(
        "u", 401, "nope", {}, None),
    __import__("urllib.error", fromlist=["x"]).HTTPError(
        "u", 503, "busy", {}, None),
    TimeoutError("langsam"),
    ValueError("kaputtes json"),
])
def test_jeder_abrufsfehler_ist_voruebergehend(monkeypatch, _kein_push,
                                               fehler):
    """Ein 401 heisst bei Mistral nicht zwingend „Key ungueltig" —
    derselbe Key lieferte nach einem Kontingent-Zustand zehn Tage spaeter
    wieder 200 (QA50D). Kein Befund, kein Push."""
    _umgebung(monkeypatch)
    monkeypatch.setattr(sys, "argv", ["modell_wachhund.py"])

    def _wirf(key, timeout):
        raise fehler
    monkeypatch.setattr(wh, "hole_katalog", _wirf)
    assert wh.main() == wh.EX_TEMPFAIL
    assert not _kein_push


def test_leerer_katalog_ist_voruebergehend(monkeypatch, _kein_push):
    _umgebung(monkeypatch)
    monkeypatch.setattr(sys, "argv", ["modell_wachhund.py"])
    monkeypatch.setattr(wh, "hole_katalog", lambda key, timeout: [])
    assert wh.main() == wh.EX_TEMPFAIL
    assert not _kein_push


def test_der_gute_fall_pusht_nicht(monkeypatch, _kein_push):
    _umgebung(monkeypatch)
    monkeypatch.setattr(sys, "argv", ["modell_wachhund.py"])
    monkeypatch.setattr(wh, "hole_katalog", lambda key, timeout: KATALOG)
    assert wh.main() == wh.EX_OK
    assert not _kein_push


def test_verschwundenes_modell_alarmiert_dringend(monkeypatch, _kein_push):
    _umgebung(monkeypatch)
    monkeypatch.setattr(sys, "argv", ["modell_wachhund.py"])
    monkeypatch.setattr(wh, "hole_katalog",
                        lambda key, timeout: [KATALOG[1]])
    assert wh.main() == wh.EX_ALARM
    assert len(_kein_push) == 1
    titel, text, dringend = _kein_push[0]
    assert dringend is True
    assert "verschwunden" in titel
    assert "MISTRAL_MODEL" in text, text


def test_tausch_alarmiert_als_befund(monkeypatch, _kein_push):
    _umgebung(monkeypatch)
    monkeypatch.setattr(sys, "argv", ["modell_wachhund.py"])
    monkeypatch.setattr(
        wh, "hole_katalog",
        lambda key, timeout: [dict(KATALOG[0],
                                   aliases=["mistral-medium-2711"])])
    assert wh.main() == wh.EX_BEFUND
    assert len(_kein_push) == 1
    _, text, dringend = _kein_push[0]
    assert dringend is False
    assert "mistral-medium-2711" in text


def test_nicht_pinnbares_modell_pusht_nicht(monkeypatch, _kein_push):
    """large-4 ist der empfohlene Ersatz — es darf keinen taeglichen
    Push erzeugen, nur eine Log-Zeile."""
    _umgebung(monkeypatch, modell="mistral-large-4")
    monkeypatch.setattr(sys, "argv", ["modell_wachhund.py"])
    monkeypatch.setattr(wh, "hole_katalog", lambda key, timeout: KATALOG)
    assert wh.main() == wh.EX_OK
    assert not _kein_push
