"""Vier Buchstaben zogen dreizehn fremde Claims in den Ungleichheits-Dienst.

Punkt 4 der offenen Liste hieß "`language` wird im Backend nicht
ausgewertet — Verdicts antworten immer auf Deutsch". Auch das war
**veraltet**: Gemessen gegen die Live-API mit ``lang: "en"`` kommen vier von
vier englischen Claims auf Englisch zurück, samt Nuance und Disclaimer, und
alle vier mit richtigem Verdict. Der Parameter wird in ``main.py`` gelesen,
an den Synthesizer übergeben (Zeile 1698) und das Frontend sendet ihn als
``lang``. Der Eintrag stammt aus der #206-Zeit.

Auf dem Weg dorthin kam ein echter Befund heraus — und der war messbar.

## Der Befund aus #280, bis zum Token verfolgt

#280 hatte EINEN neuen Fehltreffer hinterlassen: Nach der Normalisierung am
Dienst-Eingang erreichte ein Claim über Dschihadisten und den Vatikan den
WID-Dienst. Das auslösende Token ist ``"dina"`` —
*Distributional National Accounts*, eine echte WID-Methodik. Es steckt in

    Kar**dina**ele    (normalisiert aus "Kardinäle")

und, wie die Messung über 12.155 echte Claims zeigt, in acht weiteren
Wortfamilien über drei Sprachen:

    15 Claims erreichten das wid-Gate
     2 davon zu Recht
    13 über "dina" in einem fremden Wort:

        or**dina**teurs (französisch, 3×)   Fer**dina**nd
        Kar**dina**l / Kardinäle             Zielkoor**dina**ten
        skan**dina**vische / Skan**dina**vien / Scan**dina**vie
        or**dina**ire (fr.) / or**dina**ry (en.)
        hasilatın**da** mü... **dina**mika (aserbaidschanisch)

## Binden allein hätte den Satzanfang gekostet

``" dina "`` mit Rand-Leerzeichen entfernt die dreizehn — und verliert
"DINA zeigt eine andere Verteilung", weil vor dem ersten Wort kein
Leerzeichen steht. Genau die Falle aus #207: *"wer `"ki"` zu `" ki "` band,
verlor jeden Claim, der mit „KI" beginnt."*

Die Abhilfe gibt es seit dem 26.09.2026 — ``trifft_mit_wortgrenze`` zählt
Claim-Rand und Satzzeichen als Wortgrenze. Sie war aber nur bei den FAKTEN
angeschlossen (über ``_topic_match``), nicht bei den Diensten: **65 von 73
Diensten mit eigenem Vokabular riefen das nackte ``trifft``**. Damit war
ein kurzes Token in einem Dienst-Vokabular überhaupt nicht sicher bindbar.

wid ist jetzt der vierte Dienst mit dem wortgrenzen-bewussten Matcher.

## Gemessen, 12.155 Claims

    vorher   15 Treffer
    nachher   2 Treffer
    entfallen 13 — alle dreizehn Fehltreffer, einzeln unten aufgeführt
    neu        0
    verloren   0 echte

Und "DINA" trifft weiter: am Satzanfang, mitten im Satz und mit Bindestrich.

## Was NICHT in diesem PR steht

Ein Zensus über kurze ungebundene Tokens in allen Dienst-Vokabularen meldete
**256 Treffer** — und ist als Instrument **verworfen**: Die Liste besteht
überwiegend aus Länder-Codes ("de", "ch", "at", "CHE", "DEU"), die auf der
DATEN-Seite verglichen werden, nicht gegen den Claim. Genau die
Unterscheidung, an der schon #152 hing. Der brauchbare Maßstab ist das
GATE über den Korpus, und das zeigt pro Dienst zweistellige Zahlen
(nvd 317, mitre 52, noaa 31, era5 15, wid 15, eba 11, der Rest 0) — kein
flächiges Problem.

Die 65 Dienste ohne wortgrenzen-bewussten Matcher bleiben als gemessener
Rückstand stehen, nicht als Baustelle dieses PRs: Jeder Umbau braucht
seinen eigenen Sweep, und ohne Anlass pro Dienst ist er nicht zu bewerten.

Dependency-light: nur wid und das Korpus, kein Netz.
"""
import json
import os
import pathlib
import re
import sys

import pytest

BACKEND = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BACKEND))

from services._schreibweise import normalisiere  # noqa: E402
from services.wid import _claim_mentions_wid  # noqa: E402


def _gate(claim: str) -> bool:
    """Das Gate so, wie der Dienst es seit #280 aufruft."""
    return bool(_claim_mentions_wid(normalisiere(claim.lower())))


#: Die dreizehn Claims, die am 26.10.2026 ueber "dina" im Gate landeten.
#: Woertlich aus claimreview_index / den Stress-Korpora.
FEHLTREFFER = [
    '"Islamistische Dschihadisten kreuzigen Christen in Syrien, während '
    'der Vatikan und bestimmte Kardinäle Ramadan feiern"',
    "Amazon offre des ordinateurs portables aux Belges de plus de 30 ans.",
    "CNN did a show with a chyron about Philippines President Ferdinand "
    "Marcos alleged cocaine use",
    "Ein Kardinal, der AfD-Wählern angeblich mit der Hölle droht",
    "Für die Eingabe der Taurus-Zielkoordinaten bräuchte es deutsche Soldaten",
    "Hunderte skandinavische Urlauberinnen und Urlauber sind mit einer "
    "Corona-Infektion aus Österreich zurückgekehrt",
    "L'OMS admet que le Covid-19 est comme la grippe ordinaire",
    "Lidl offre des ordinateurs portables Apple.",
    "Microsoft a annoncé son intention de désactiver les ordinateurs des "
    "personnes qui partagent du contenu",
    "Obama said, he wants ordinary people to 'surrender to powerful sovereign'",
    "Staatsmedien berichten nicht über neue Impfempfehlungen in Skandinavien.",
    "« Pas de 2ème vague, ni en Asie, ni en Afrique, ni en Scandinavie, "
    "ni en Russie. Curieux, non ? »",
    "“Qaz hasilatında müsbət dinamika davamlı xarakter daşıyır”",
]


@pytest.mark.parametrize("claim", FEHLTREFFER)
def test_kein_fremdes_wort_zieht_den_wid_dienst(claim):
    """Jeder einzelne war am 26.10.2026 ein Treffer."""
    assert not _gate(claim), claim


@pytest.mark.parametrize("claim", [
    "DINA zeigt eine andere Verteilung beim Top-1-Prozent",
    "Die DINA-Methodik von WID weist das Top-1-Prozent anders aus",
    "Laut DINA besitzt das oberste Prozent mehr als die Hälfte",
])
def test_der_fachbegriff_trifft_weiter(claim):
    """Binden darf nicht loeschen (#207) — auch nicht am Satzanfang."""
    assert _gate(claim), claim


@pytest.mark.parametrize("claim", [
    "Das Top-1-Prozent besitzt mehr als die Hälfte des Vermögens",
    "Die reichsten 10 Prozent halten laut World Inequality Lab den "
    "größten Anteil",
])
def test_die_echten_wid_claims_bleiben(claim):
    assert _gate(claim), claim


# ------------------------------------------------------- der Code dahinter

def _quelle() -> str:
    return (BACKEND / "services" / "wid.py").read_text(encoding="utf-8")


def _nur_code() -> str:
    """Die Quelle OHNE Kommentare.

    Zum fuenften Mal in dieser Sitzung dieselbe Falle: Die Pruefung
    "ungebundene Fassung nicht mehr da" schlug an der eigenen Begruendung
    fehl — der Kommentar zitiert `"dina"` absichtlich. Geprueft wird
    Code, nicht Prosa.
    """
    zeilen = []
    for zeile in _quelle().splitlines():
        ohne = zeile.split("#", 1)[0]
        if ohne.strip():
            zeilen.append(ohne)
    return "\n".join(zeilen)


def test_das_token_ist_gebunden():
    q = _nur_code()
    assert '" dina "' in q, "Rand-Leerzeichen fehlen"
    assert '"dina"' not in q, "ungebundene Fassung noch da"


def test_wid_nutzt_den_wortgrenzen_matcher():
    """Ohne ihn waere das gebundene Token am Satzanfang blind.

    Das ist die Voraussetzung dafuer, dass Binden hier UEBERHAUPT geht —
    siehe _flexion.WORTGRENZEN.
    """
    q = _nur_code()
    assert "trifft_mit_wortgrenze" in q
    assert "import trifft as _flexion_trifft" not in q


def test_die_begruendung_nennt_das_fremdwort():
    """Wer das Leerzeichen spaeter entfernt, soll den Grund lesen."""
    q = _quelle()
    assert "Kardina" in q or "KarDINAele" in q or "Kardinaele" in q


# ------------------------------------------------------ Sweep ueber Korpus

def _korpus() -> list[str]:
    raus: list[str] = []
    quellen = sorted((BACKEND / "tools" / "stress_tests").glob("*.json"))
    quellen += [BACKEND / "tools" / "stress_test_100_claims.json",
                BACKEND / "data" / "claimreview_index.json"]
    for pfad in quellen:
        try:
            with open(pfad, encoding="utf-8") as fh:
                daten = json.load(fh)
        except Exception:                        # pragma: no cover
            continue
        stapel = [daten]
        while stapel:
            o = stapel.pop()
            if isinstance(o, dict):
                c = o.get("claim")
                if isinstance(c, str) and len(c) > 10:
                    raus.append(c)
                stapel.extend(o.values())
            elif isinstance(o, list):
                stapel.extend(o)
    return sorted(set(raus))


#: Die neun Wortfamilien, in denen "dina" gemessen steckte. Synthetisch,
#: damit die Pruefung ohne den (gitignorierten) ClaimReview-Index laeuft.
WORTFAMILIEN = [
    "Ein Kardinal hat sich dazu geäußert",
    "Die Kardinäle wählen einen neuen Papst",
    "Lidl verkauft günstige Ordinateurs in Belgien",
    "Das war eine ganz ordinaire Grippe",
    "He wants ordinary people to decide",
    "Ferdinand Marcos war Präsident der Philippinen",
    "Die Zielkoordinaten wurden falsch eingegeben",
    "Skandinavische Länder haben andere Regeln",
    "In Skandinavien gab es keine zweite Welle",
    "Müsbət dinamika davamlı xarakter daşıyır",
]


@pytest.mark.parametrize("claim", WORTFAMILIEN)
def test_keine_wortfamilie_zieht_den_wid_dienst(claim):
    """Die neun Familien synthetisch — laeuft auch ohne den Index.

    Der Index `data/claimreview_index.json` ist gitignoriert (ein
    erzeugter Cache), in der CI also nicht vorhanden. Die erste Fassung
    dieser Suite hing daran: Zwei Korpus-Sweeps schlugen dort fehl, weil
    der Korpus statt 12.155 nur die 1.123 Stress-Claims umfasste — und die
    enthalten KEINE dieser Woerter. Der Vertrag gehoert deshalb in die
    Suite, nicht in fremde Daten.
    """
    assert not _gate(claim), claim


# Die Korpus-Sweeps bleiben als ZUSATZ, wo die Daten liegen (lokal, prod).
_INDEX = BACKEND / "data" / "claimreview_index.json"
_ohne_index = pytest.mark.skipif(
    not _INDEX.exists(),
    reason=f"{_INDEX.name} ist gitignoriert und hier nicht vorhanden — "
           f"die Wortfamilien pruefen WORTFAMILIEN synthetisch")


@_ohne_index
def test_das_gate_bleibt_eng():
    """Obergrenze statt fester Zahl: der Korpus darf wachsen.

    Gemessen am 26.10.2026: 2 von 12.155 Claims, vorher 15. Die Schwelle
    liegt bei 5 — jeder weitere Treffer soll auffallen, ohne dass ein
    zusaetzlicher ECHTER wid-Claim im Korpus den Test bricht.
    """
    claims = _korpus()
    assert len(claims) > 10000, f"Korpus unerwartet klein: {len(claims)}"
    treffer = [c for c in claims if _gate(c)]
    assert len(treffer) <= 5, [c[:70] for c in treffer]


@_ohne_index
def test_kein_claim_mit_ordina_oder_kardina_trifft():
    """Dieselben Wortfamilien, aber ueber die 12.155 ECHTEN Claims.

    Breiter als die dreizehn Einzelfaelle und als die synthetischen oben:
    Hier steht, was Menschen wirklich geschrieben haben.
    """
    muster = re.compile(r"ordina|kardina|koordina|skandinav|scandinav|ferdinand",
                        re.IGNORECASE)
    verdaechtig = [c for c in _korpus() if muster.search(c)]
    assert verdaechtig, "Das Korpus sollte solche Woerter enthalten"
    treffer = [c for c in verdaechtig if _gate(c)]
    assert not treffer, [c[:70] for c in treffer]


# ---------------------------------------------- der Rueckstand, dokumentiert

def test_der_rueckstand_ist_benannt_nicht_versteckt():
    """65 von 73 Diensten rufen das nackte ``trifft``.

    Kein Gate darauf — das waere ein Marker, der beim ersten Umbau bricht.
    Geprueft wird nur, dass der Mechanismus existiert und wid ihn nutzt;
    die Zahl steht im Docstring dieser Suite.
    """
    flexion = (BACKEND / "services" / "_flexion.py").read_text(encoding="utf-8")
    assert "def trifft_mit_wortgrenze" in flexion
    assert "WORTGRENZEN" in flexion
    nutzer = [p.name for p in sorted((BACKEND / "services").glob("*.py"))
              if "trifft_mit_wortgrenze" in p.read_text(encoding="utf-8")]
    assert "wid.py" in nutzer, nutzer
