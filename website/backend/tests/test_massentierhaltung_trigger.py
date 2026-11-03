"""Der korrigierte Fakt erreichte seine eigene Kernfrage nicht (3.11.2026).

#300 hat den Massentierhaltungs-Fakt korrigiert: Statt „85 % Vollspaltboden
(AGES)" sagt er jetzt, dass es keine amtliche Quote gibt, und nennt die
Schätzungen 58–80 % mit Erheber. Der Live-Check danach:

    "In Österreich leben 85 Prozent der Schweine auf Vollspaltboden"
    -> unverifiable @ 0,1, kein einziger Beleg

## Was im Prod-Log stand

Die Landwirtschaft lieferte **einen** Treffer — aber den Schwester-Fakt
``viehbestand_at_2026`` mit den Tierzahlen. Der Synthesizer bekam Rinder-
und Schweinezahlen und nichts zum Vollspaltboden und sagte zu Recht
„unverifiable". Die ehrliche Auskunft stand im richtigen Fakt, und der
wurde nie gezeigt.

Der Cosine-Rückfall ist für dieses Pack abgeschaltet (#41, er zog
themenfremde Claims). Die Trigger sind der einzige Weg zum Fakt.

## Gemessen, bevor etwas gebaut wurde

Eine Batterie aus 23 Formulierungen, Erwartung je Zeile vorab festgelegt:

    vorher    8 von 16 Ziel-Formulierungen
    nachher  16 von 16
    Kontrollen  0 von 5 getroffen, vorher wie nachher

Drei getrennte Ursachen:

* **Gruppe 2 kennt weder „Prozent" noch „leben" noch „meisten".** Die
  Composite-Regel verlangt ein Wort aus beiden Gruppen; „2,5 Millionen
  Rinder" kam nur zufällig über „millionen" an.
* **Die Schreibweise.** Der Fakt schreibt „Vollspaltboden", der VGT
  „Vollspaltenboden". ``vollspaltboden`` steckt nicht in
  ``vollspaltenboden`` — Gruppe 1 war damit bei der anderen Schreibweise
  leer. Beide Formen sind in österreichischen Quellen belegt.
* **Henne im Käfig.** „AT-Hühner leben in Käfigen" traf über eine eigene
  Regel, „Legehennen in Österreich leben in Käfigen" nicht.

Der Fix ist deshalb **kein** breiteres Gruppe-2-Vokabular — „Prozent" in
Gruppe 2 hätte jede Schweinepest-Statistik an diesen Fakt gezogen —,
sondern drei enge Ergänzungen:

    K1  trigger_keywords  vollspaltboden, spaltenboden
    K2  trigger_all       [Henne/Huhn]  UND  [Käfig]
    K3  Gruppe 2          " stroh "  (gebunden)

Je Kandidat einzeln gemessen: 0 neue Treffer auf 11.032 Korpus-Claims,
0 auf den Phrasings aller anderen Fakten, 0 neue tolerante Treffer
(Tippfehler-Pass), 0 Muss-Treffer verloren.

## Zwei meiner Erwartungen waren falsch

„In Österreich gibt es mehr Rinder als Schweine" und „Die Zahl der
Schweine in Österreich sinkt" hatte ich als Ziel eingetragen. Sie gehören
zum Schwester-Fakt ``viehbestand_at_2026``, dessen Kernsatz genau diese
Fragen beantwortet — und der sie schon heute fängt.

## Nebenbei zwei vorbestehende Fehltreffer

Der Fakt traf zwei der 11.032 Korpus-Claims, die nichts mit Tierhaltung
zu tun haben:

* **„Weihnachtsmarkt auf dem Berliner Breitscheidplatz in einem Käfig"** —
  Gruppe 1 ``käfig``, Gruppe 2 ``platz`` aus Breitscheid-PLATZ. Dieselbe
  Klasse wie #293. ``platz`` ist jetzt gebunden; „mehr Platz pro Tier"
  trifft weiter.
* **Ein Corona-Claim** über Schweinegrippe und saisonale Grippe —
  ``schwein`` aus SCHWEINEgrippe, ``vergleich`` als ganzes Wort. **Nicht
  behoben:** Binden hilft hier nicht, ``schwein`` muss für „Mastschweine"
  ungebunden bleiben, und der Matcher hat keinen Ausschluss-Mechanismus.
  Für 1 von 11.032 Claims einen zu bauen wäre unverhältnismäßig. Der
  Schaden ist Kontextrauschen: ein Schweinehaltungs-Fakt im Prompt eines
  Corona-Checks.

## Die Batterie ist konstruiert, nicht beobachtet

Der ClaimReview-Korpus enthält **null** Claims zu diesem Thema — kein
Vollspaltboden, keine Massentierhaltung, keine Henne im Käfig. Er taugt
hier zur Über-Trigger-Prüfung, nicht zur Abdeckungsmessung. Die
Formulierungen sind meine; das Thema ist eine reale österreichische
Debatte (Vollspaltenboden-Verbot), eine Häufigkeit aus echten Claims gibt
es dafür nicht.

Keine Netzabfrage in diesen Tests.
"""

import json
import sys
from pathlib import Path

import pytest

BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))

from services._topic_match import substring_or_composite_match  # noqa: E402

PACK = BACKEND / "data" / "landwirtschaft_pack.json"


def _fakten():
    return json.loads(PACK.read_text(encoding="utf-8"))["facts"]


def _fakt(kennung):
    t = [f for f in _fakten() if (f.get("id") or f.get("topic")) == kennung]
    assert len(t) == 1, (kennung, len(t))
    return t[0]


ZIEL = "massentierhaltung_empirie_2026"
SCHWESTER = "viehbestand_at_2026"


def _trifft(kennung, claim):
    return substring_or_composite_match(_fakt(kennung), claim.lower())


# --------------------------------------------------------------------------
# Die Batterie
# --------------------------------------------------------------------------

SOLL = [
    # die Kernfrage aus dem Live-Test, in mehreren Formen
    "In Österreich leben 85 Prozent der Schweine auf Vollspaltboden",
    "Die meisten Schweine in Österreich leben auf Vollspaltboden",
    "Die meisten Schweine in Österreich stehen auf Vollspaltenboden",
    "Vollspaltenboden ist in Österreich die Regel",
    "90 Prozent der Mastschweine in Österreich haben keinen Zugang zu Stroh",
    "Österreichische Schweine leben fast alle auf Spaltenböden",
    "Der Vollspaltenboden wird in Österreich verboten",
    "Vollspaltboden ist Standard",
    # Bestand / Massentierhaltung
    "Österreich hat 2,5 Millionen Rinder",
    "In Österreich gibt es keine Massentierhaltung",
    "Österreich hat industrielle Massentierhaltung wie die USA",
    # Hühner / Käfig
    "Käfighaltung ist in Österreich verboten",
    "Legehennen in Österreich leben in Käfigen",
    "AT-Hühner leben in Käfigen",
    # Tierwohl
    "Tierwohl ist in Österreich besser als in Deutschland",
    "Schweine in AT haben es gut",
]

#: Duerfen diesen Fakt NICHT ziehen — Schwein, Prozent, Österreich in
#: anderem Zusammenhang. Gegen genau diese Gefahr ist Gruppe 2 NICHT um
#: "prozent" erweitert worden.
NICHT = [
    "Die Schweinepest hat 30 Prozent der Betriebe getroffen",
    "Schweinefleisch ist um 10 Prozent teurer geworden",
    "Die meisten Österreicher essen gern Schweinsbraten",
    "Rinderwahnsinn ist in Österreich ausgerottet",
    "Hühnereier sind seit 2022 um 40 Prozent teurer",
    "Der Weihnachtsmarkt auf dem Breitscheidplatz wird in einem Käfig gefeiert",
    "Er hat einen Strohmann vorgeschoben, um den Tierschutz zu umgehen",
]

#: Fragen, die der Schwester-Fakt beantwortet — nicht dieser.
FUER_DIE_SCHWESTER = [
    "In Österreich gibt es mehr Rinder als Schweine",
    "Die Zahl der Schweine in Österreich sinkt",
]


@pytest.mark.parametrize("claim", SOLL)
def test_formulierung_erreicht_den_fakt(claim):
    assert _trifft(ZIEL, claim), claim


@pytest.mark.parametrize("claim", NICHT)
def test_kontrolle_erreicht_den_fakt_nicht(claim):
    assert not _trifft(ZIEL, claim), claim


@pytest.mark.parametrize("claim", FUER_DIE_SCHWESTER)
def test_bestandsfragen_gehen_an_den_schwester_fakt(claim):
    """Meine Erwartung war falsch — und der richtige Fakt faengt sie."""
    assert _trifft(SCHWESTER, claim), claim


# --------------------------------------------------------------------------
# Die drei Ursachen einzeln
# --------------------------------------------------------------------------

@pytest.mark.parametrize("schreibweise", [
    "Vollspaltboden", "Vollspaltenboden", "Vollspaltböden", "Vollspaltenböden",
    "Spaltenboden", "Spaltenböden", "Teilspaltenboden",
])
def test_jede_schreibweise_reicht_allein(schreibweise):
    """K1: das Wort traegt den Fakt, auch ohne zweite Gruppe."""
    assert _trifft(ZIEL, f"Der {schreibweise} wird diskutiert"), schreibweise


def test_beide_schreibweisen_stehen_als_schluesselwort():
    """``vollspaltboden`` steckt NICHT in ``vollspaltenboden`` — beide noetig."""
    kw = _fakt(ZIEL)["trigger_keywords"]
    assert "vollspaltboden" in kw
    assert "spaltenboden" in kw
    assert "vollspaltboden" not in "vollspaltenboden"


def test_henne_und_kaefig_braucht_beide():
    """K2: Kaefig allein darf die neue Regel nicht ausloesen."""
    assert _trifft(ZIEL, "Hühner werden in Käfigen gehalten")
    assert _trifft(ZIEL, "Legehennen sitzen im Käfig")
    regel = _fakt(ZIEL)["trigger_all"][-1]
    nur_regel = {"trigger_all": [regel]}
    assert not substring_or_composite_match(nur_regel, "ein faradayscher käfig")
    assert not substring_or_composite_match(nur_regel, "hühner legen eier")


def test_stroh_ist_gebunden():
    """K3: sonst Strohballen, Strohtuerme, Strohmann."""
    g2 = _fakt(ZIEL)["trigger_composite"][1]
    assert " stroh " in g2
    assert "stroh" not in g2
    assert _trifft(ZIEL, "Mastschweine haben keinen Zugang zu Stroh")
    assert not _trifft(ZIEL, "Schweine fressen Strohballen")


def test_platz_ist_gebunden_und_traegt_weiter():
    """Der Breitscheidplatz-Fehltreffer — die #293-Klasse."""
    g2 = _fakt(ZIEL)["trigger_composite"][1]
    assert " platz " in g2
    assert "platz" not in g2
    assert _trifft(ZIEL, "Schweine haben zu wenig Platz pro Tier")
    assert not _trifft(ZIEL, "Auf dem Breitscheidplatz steht ein Käfig")


def test_gruppe_zwei_kennt_kein_prozent():
    """Absichtlich: "prozent" haette jede Schweinepest-Statistik gezogen."""
    g2 = _fakt(ZIEL)["trigger_composite"][1]
    assert not any("prozent" in t for t in g2)


# --------------------------------------------------------------------------
# Über-Trigger: keine fremden Phrasings
# --------------------------------------------------------------------------

def _alle_fakten():
    for p in sorted((BACKEND / "data").glob("*.json")):
        try:
            d = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            continue
        if isinstance(d, dict):
            for f in d.get("facts") or []:
                if isinstance(f, dict):
                    yield p.name, (f.get("id") or f.get("topic")), f


#: Die einzige gewollte Ueberschneidung: der Schwester-Fakt, ebenfalls
#: oesterreichische Schweine.
ERLAUBT_FREMD = {("landwirtschaft_pack.json", SCHWESTER,
                  "Viehbestand Österreich Schweine")}


def test_kein_fremdes_phrasing_zieht_diesen_fakt():
    ziel = _fakt(ZIEL)
    fremd = {(d, k, ph) for d, k, f in _alle_fakten() if k != ZIEL
             for ph in (f.get("claim_phrasings_handled") or [])
             if isinstance(ph, str)
             and substring_or_composite_match(ziel, ph.lower())}
    assert fremd <= ERLAUBT_FREMD, sorted(fremd - ERLAUBT_FREMD)


def test_die_eigenen_phrasings_treffen():
    """Muss-Treffer, inklusive der fuenf neu aufgenommenen."""
    f = _fakt(ZIEL)
    fehlt = [ph for ph in f["claim_phrasings_handled"]
             if not substring_or_composite_match(f, ph.lower())]
    assert not fehlt, fehlt
    assert len(f["claim_phrasings_handled"]) >= 11


# --------------------------------------------------------------------------
# Korpus — nur wo er liegt (gitignoriert, in der CI nicht vorhanden)
# --------------------------------------------------------------------------

_INDEX = BACKEND / "data" / "claimreview_index.json"
_ohne_index = pytest.mark.skipif(
    not _INDEX.exists(),
    reason=f"{_INDEX.name} ist gitignoriert — die Kontrollen oben pruefen "
           f"die Breitscheidplatz-Klasse synthetisch")


@_ohne_index
def test_korpus_hoechstens_der_bekannte_rest():
    """Gemessen am 3.11.2026: 2 Treffer vorher, 1 nachher.

    Der verbleibende ist der Schweinegrippe-Claim (siehe Docstring). Eine
    Obergrenze statt einer festen Zahl: der Korpus darf wachsen, und ein
    echter Tierhaltungs-Claim soll den Test nicht brechen.
    """
    f = _fakt(ZIEL)
    claims = [r["claim"] for r in json.loads(_INDEX.read_text(encoding="utf-8"))
              if r.get("claim")]
    treffer = [c for c in claims if substring_or_composite_match(f, c.lower())]
    assert not any("breitscheidplatz" in c.lower() for c in treffer)
    assert len(treffer) <= 3, [c[:80] for c in treffer]


# --------------------------------------------------------------------------
# Die zweite Kopie der falschen Zahl
# --------------------------------------------------------------------------

def test_der_dienst_docstring_fuehrt_die_alte_zahl_nicht_mehr_als_stand():
    """#134: eine Zahl in zwei Kopien driftet. Der Docstring von
    landwirtschaft_pack.py fuehrte nach #300 noch "2.5 Mio Rinder"."""
    quelle = (BACKEND / "services" / "landwirtschaft_pack.py").read_text(
        encoding="utf-8")
    kopf = quelle[:quelle.index('"""', 3)]
    assert "1,80 Mio Rinder" in kopf
    assert "2.6 Mio\n    Schweine, 2.5 Mio Rinder" not in kopf
