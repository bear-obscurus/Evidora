"""Wer zuerst fragt, legte die Sprache fuer alle anderen fest.

Punkt 14, gemessen am 13.10.2026 gegen prod. Dreimal dieselbe Behauptung,
nur das Cache-Bust-Suffix unterschied sich, und das Sprachfeld wechselte:

    {"claim": "Austria has the highest …", "lang": "en"}      -> 10,3 s
    {"claim": "Austria has the highest …", "lang": "de"}      ->  0,6 s
    {"claim": "Austria has the highest …", "language": "en"}  ->  0,9 s

Der zweite Lauf wollte Deutsch und bekam Wort fuer Wort den englischen
Text des ersten. Im Log: ``SEMANTIC HIT cos=0.999``.

Die Ursache war nicht, dass ``lang`` unausgewertet bleibt — das Feld ist
ueberall verdrahtet, vom Frontend (``app.js`` sendet ``lang``) bis zu
``SYSTEM_PROMPTS[lang]``, und mit ``lang: "en"`` antwortet der Dienst
auf Englisch. Die Ursache war der Cache: Sein Schluessel war nur der
normalisierte Claim.

Dieselbe Fehlerfamilie wie PR #203 (die Richtung fehlte im Schluessel,
"ueber 300" traf "unter 300") und #118 (die Messgroesse fehlte). Was die
Antwort bestimmt, gehoert in den Schluessel.

## Was hier NICHT behauptet wird

Dass eine echte UEBERSETZUNG die Sprachgrenze springt. Gemessen:
"Oesterreich gibt mehr fuer Bildung aus als Deutschland" blieb gegen die
englische Fassung unter ``SEMANTIC_THRESHOLD`` (0,92), lief frisch durch
die Pipeline und bekam eigene Zahlen. Der Sprachfilter in der
semantischen Schleife deckt den Fall trotzdem ab — er kostet einen
Vergleich, und ein Schwellenwert ist nichts, worauf man sich verlassen
sollte.

Dependency-light: Der semantische Pfad braucht ein Embedding-Modell, das
in CI fehlt. Die Tests, die ihn betreffen, legen die Embeddings deshalb
selbst — sonst pruefen sie nur den Exact-Pfad und sehen gruen aus
([[lessons_learned]]: eine gruene Suite beweist nichts ueber
ungetesteten Code).
"""
import time

import pytest

from services import verdict_cache as vc

ERGEBNIS_EN = {"verdict": "false", "confidence": 0.85,
               "summary": "Austria's gap is the third-highest in the EU."}
ERGEBNIS_DE = {"verdict": "false", "confidence": 0.85,
               "summary": "Oesterreich liegt auf Platz 3 in der EU."}
CLAIM = "Austria has the highest gender pay gap in the EU"


@pytest.fixture(autouse=True)
def leerer_cache():
    vc.clear()
    yield
    vc.clear()


# --- Der gemessene Fall, ueber den Exact-Pfad ---------------------------

def test_deutsch_bekommt_nicht_die_englische_antwort():
    """Der Kern. Ueber den Exact-Pfad, weil der kein Modell braucht."""
    vc.put(CLAIM, ERGEBNIS_EN, "en")
    assert vc.get(CLAIM, "en") is not None
    assert vc.get(CLAIM, "de") is None


def test_englisch_bekommt_nicht_die_deutsche_antwort():
    """Die Gegenrichtung — sonst pinnt der Test nur einen Sonderfall."""
    vc.put(CLAIM, ERGEBNIS_DE, "de")
    assert vc.get(CLAIM, "de") is not None
    assert vc.get(CLAIM, "en") is None


def test_beide_sprachen_leben_nebeneinander():
    """Trennen heisst nicht verdraengen: zwei Eintraege, nicht einer."""
    vc.put(CLAIM, ERGEBNIS_EN, "en")
    vc.put(CLAIM, ERGEBNIS_DE, "de")
    assert vc.get(CLAIM, "en")["summary"] == ERGEBNIS_EN["summary"]
    assert vc.get(CLAIM, "de")["summary"] == ERGEBNIS_DE["summary"]
    assert vc.stats()["total"] == 2


def test_die_voreinstellung_ist_deutsch():
    """Alte Aufrufer ohne ``lang`` landen im deutschen Eimer, nicht in
    einem dritten, der nie wieder getroffen wird."""
    vc.put(CLAIM, ERGEBNIS_DE)
    assert vc.get(CLAIM, "de") is not None
    assert vc.get(CLAIM) is not None


def test_unbekannte_sprache_faellt_auf_deutsch_zurueck():
    """``synthesize_results`` tut dasselbe (``if lang not in
    SYSTEM_PROMPTS: lang = "de"``). Taete der Cache es nicht, legte ein
    Tippfehler im Feld einen eigenen Eimer an, der nie wieder getroffen
    wird — ein stilles Leck im Store-Limit."""
    vc.put(CLAIM, ERGEBNIS_DE, "de")
    assert vc.get(CLAIM, "fr") is not None
    assert vc.get(CLAIM, "") is not None
    vc.clear()
    vc.put(CLAIM, ERGEBNIS_DE, "klingonisch")
    assert vc.get(CLAIM, "de") is not None
    assert vc.stats()["total"] == 1


# --- Der semantische Pfad, mit selbst gelegten Embeddings --------------

def _lege_eintrag(claim, result, lang, emb):
    """Schreibt direkt in den Store, mit einem gesetzten Embedding.

    ``put`` wuerde ``_embed`` rufen, und das braucht das
    sentence-transformers-Modell, das in CI nicht da ist. Dann waere
    ``emb is None``, die semantische Schleife uebersprang den Eintrag,
    und der Test waere gruen, ohne den Pfad je betreten zu haben.
    """
    vc._verdict_store[vc._schluessel(lang, vc._normalize(claim))] = (
        time.time(), emb, result, vc.get_data_version())


@pytest.fixture
def numpy_oder_skip():
    return pytest.importorskip("numpy")


def test_semantischer_treffer_bleibt_in_seiner_sprache(numpy_oder_skip,
                                                       monkeypatch):
    """Zwei nahezu identische Vektoren, zwei Sprachen.

    cos = 1,0 liegt weit ueber SEMANTIC_THRESHOLD — ohne den Sprachfilter
    in der Schleife wuerde der deutsche Abruf hier treffen. Genau dieser
    Weg war es live, bei cos = 0,999.

    ``monkeypatch``, nicht ``vc._embed = …`` mit ``del`` danach: Eine
    erste Fassung tat das, und ``del`` LOESCHT die Funktion, statt sie
    zurueckzulegen — jeder spaetere ``put`` im selben Prozess waere an
    ``NameError: name '_embed' is not defined`` gestorben. Die Suite blieb
    gruen, weil zufaellig kein spaeterer Test ``put`` rief.
    """
    np = numpy_oder_skip
    emb = np.ones(8, dtype="float32") / np.sqrt(8)
    _lege_eintrag(CLAIM, ERGEBNIS_EN, "en", emb)

    # Ein anderer Claim-Wortlaut, damit der Exact-Pfad NICHT greift und
    # wirklich die semantische Schleife antwortet.
    anders = CLAIM + " in 2024"
    monkeypatch.setattr(vc, "_embed", lambda _t: emb)
    assert vc.get(anders, "en") is not None, (
        "Der semantische Pfad trifft nicht einmal in derselben "
        "Sprache — dann prueft dieser Test gar nichts")
    assert vc.get(anders, "de") is None


def test_der_sprachfilter_ist_der_grund(numpy_oder_skip, monkeypatch):
    """Giftprobe am Instrument: ohne den Filter trifft es.

    Ohne diesen Test koennte der Treffer oben aus einem ganz anderen
    Grund ausbleiben — etwa weil der Polaritaets-Guard anschlaegt — und
    der Test sagte trotzdem "der Sprachfilter wirkt".
    """
    np = numpy_oder_skip
    emb = np.ones(8, dtype="float32") / np.sqrt(8)
    anders = CLAIM + " in 2024"
    monkeypatch.setattr(vc, "_embed", lambda _t: emb)
    # Derselbe Eintrag, aber als deutscher gelegt: jetzt MUSS der
    # deutsche Abruf treffen. Bleibt er leer, blockt etwas anderes.
    _lege_eintrag(CLAIM, ERGEBNIS_DE, "de", emb)
    assert vc.get(anders, "de") is not None, (
        "Auch in derselben Sprache kein Treffer — der Test oben "
        "beweist dann nichts ueber die Sprache")


# --- Die Verdrahtung ---------------------------------------------------

def test_main_reicht_lang_an_beide_aufrufstellen():
    """Der Schluessel kann die Sprache nur trennen, wenn sie ankommt.

    Ein Gate auf die Quelle, weil der Pfad in CI nicht laeuft: ``main.py``
    liest ``lang`` aus dem Body und muss es an get UND put weitergeben.
    Fehlt es bei put, landet alles im deutschen Eimer; fehlt es bei get,
    liest jeder aus dem deutschen.
    """
    import os
    pfad = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "main.py")
    with open(pfad, encoding="utf-8") as fh:
        quelle = fh.read()
    assert "_vc.get(claim, lang)" in quelle, "get bekommt die Sprache nicht"
    assert "_vc.put(claim, synthesis, lang)" in quelle, (
        "put bekommt die Sprache nicht")


def test_frontend_und_backend_benutzen_denselben_feldnamen():
    """``app.js`` sendet ``lang``, ``main.py`` liest ``lang``.

    Die Notiz zu Punkt 14 lautete, ``language`` werde nicht ausgewertet.
    Das Feld heisst nicht so — und eine Sonde, die ``language`` sendet,
    bekommt deshalb stillschweigend Deutsch. Dieses Gate haelt die
    beiden Seiten zusammen, damit eine Umbenennung auffaellt.
    """
    import os
    basis = os.path.dirname(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__))))
    with open(os.path.join(basis, "frontend", "app.js"), encoding="utf-8") as fh:
        app = fh.read()
    with open(os.path.join(basis, "backend", "main.py"), encoding="utf-8") as fh:
        haupt = fh.read()
    assert "lang: currentLang" in app, "app.js sendet das Feld nicht mehr"
    assert 'body.get("lang"' in haupt, "main.py liest das Feld nicht mehr"


def test_die_bekannten_sprachen_deckel_sich_mit_den_prompts():
    """Ein Eimer fuer eine Sprache, die der Synthesizer nicht kann, wird
    nie getroffen — und ein Prompt ohne Eimer teilt sich einen."""
    from services.synthesizer import SYSTEM_PROMPTS
    assert set(vc.BEKANNTE_SPRACHEN) == set(SYSTEM_PROMPTS)


def test_embed_ist_nach_der_suite_noch_da():
    """Faengt den Fehler, den die erste Fassung dieser Datei hatte.

    Laeuft als letzter Test der Datei und prueft, dass die Monkeypatches
    oben ``_embed`` zurueckgelegt haben. Ohne dieses Gate faellt ein
    geloeschtes Modul-Attribut erst in einer ANDEREN Datei auf, und dort
    sucht niemand.
    """
    assert callable(getattr(vc, "_embed", None)), (
        "_embed fehlt im Modul — ein Test hat es nicht zurueckgelegt")
    vc.put("Gate-Probe", {"verdict": "true", "confidence": 0.9}, "de")
    assert vc.get("Gate-Probe", "de") is not None
