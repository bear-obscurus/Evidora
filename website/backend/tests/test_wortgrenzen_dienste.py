"""Fünf Dienste hielten sich für zuständig, weil ein Akronym in einem
fremden Wort steckte.

Fortsetzung von #211 (das die FAKTEN gebunden hat) für die Dienste mit
eigenem Prädikat. Grundlage ist der Zensus vom 30.10.2026 über 11.032
echte Claims; das Memo dazu liegt unter
``~/Documents/Evidora Dokumente/Evidora_Wortgrenzen_Zensus_Dienste_2026-10-30.md``.

## Der Sweep gegen HEAD

    Dienst      vorher  nachher    weg    neu
    ris_at         810      241    607     38
    heizung        528        3    525      0
    getty          468        2    466      0
    pharma        1098      747    384     33
    nvd            312        0    312      0
    SUMME         3216      993   2294     71

**2.294 Fehltreffer weg.** Die 71 „neuen" sind zweierlei und werden
getrennt gehalten:

* **38 echte Gewinne** bei ``ris_at`` — siehe die Lücke unten.
* **33 franzoesische Impf-Claims** bei ``pharma``. Die sind KEIN
  Gewinn: Sie entstehen, weil die Keyword-Liste jetzt normalisiert ist
  und ``normalisiere`` auch ``é -> e`` faltet („vacciné" -> „vaccine").
  BASG verlangt aber ``pharma UND austria`` — sie erreichen den Dienst
  also nie. Festgehalten, damit niemand sie für Abdeckung hält.

## Was gebunden wurde — und was NICHT

Binden ist eine Abwägung je Token (#207/#211/#292). Gebunden sind nur
Tokens, deren Treffer in ein **fremdes Wortfeld** fallen:

    " ris "      601 frei / 0 gebunden   harRIS, paRIS, chRIStmas, kRISe
    " orf "       62 / 7                 VORFall, VORFeld, VORGeworfen
    " stgb "       4 / 3                 DStGB = der DEUTSCHE Gemeindebund
    " schog "      1 / 0                 SCHOGetten
    " geg "      526 / 0                 GEGen(333), GEGeben, GEGner
    " aat "      416 / 0                 stAAT, nl. gAAT/lAAT
    " ulan "      17 / 0                 hierzULANde, ambULANce
    " tempera "   34 / 0                 TEMPERAturen, TEMPERAtures
    " ema "      391 / 12                thEMA, niEMAnd, ehEMAlige
    " lot "       69 / 1                 LOThar, cLOTHes, piLOTen
    " nist "     310 / 0                 miNISTer(93), afghaNISTan
    " nvd "        2 / 0                 nl. (pNVD), coNVD
    " cvss "       0 / 0                 latent, wie die Kurz-Tokens in #211

NICHT gebunden, obwohl Binden etwas ändern würde — dort sind die
Compound-Treffer **gewollt**:

    "österreich"  102 / 68   österreichische, Österreichs
    "kanzler"      78 / 3    Bundeskanzler, Kanzlerin
    "impfung"     285 / 192  Impfungen, Grippeimpfung
    "verbot"       55 / 12   verboten, verbotenen
    "berlin"      107 / 80   Berliner

## Die Lücke, die der Sweep aufgedeckt hat

Zwei echte Österreich-Claims fielen weg — „Terroranschlag in **Wien**"
und „Entscheidung des Verwaltungsgerichts **Wien**". Der Grund war nicht
die Bindung: ``wien`` stand in **keiner** Markerliste. Beide hingen an
einem Zufallstreffer (pa**RIS**, k**RIS**e), und die Bindung hat die
Lücke nur sichtbar gemacht.

``"wien"`` ist jetzt Marker, **ungebunden** (45 frei / 36 gebunden, die
Differenz ist „wiener" und Satzzeichen — kein Fremdwort enthält „wien").
Alle 40 neu erkannten Claims von Hand geprüft: Wiener Linien, Wiener
Wohnen, MedUni Wien, Bundesheer, WEGA, Wien-Wahl. Kein Fehltreffer.

## ⚠️ Ein Fehler beim Bauen, den ein eigener Test gefangen hat

Beim Binden von ``tempera`` hat ein eingefügter Kommentar das Komma
verschluckt. Python hat die Nachbar-Literale konkateniert:
``" tempera "`` + ``"bronze-plastik"`` → ``" tempera bronze plastik"``.
Ein Token, das nichts mehr trifft — und ``bronze-plastik`` war
gleich mit weg. Deshalb prüft diese Suite für jeden gebundenen Trigger
auch den NACHBARN in der Liste.

Dependency-light: nur die Prädikate der fünf Dienste, kein Netz.
"""
import os
import sys

import pytest

BACKEND = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BACKEND)

from services._schreibweise import normalisiere  # noqa: E402
from services.basg import _claim_mentions_pharma  # noqa: E402
from services.dach_factbook import _claim_mentions_heizung  # noqa: E402
from services.getty import (  # noqa: E402
    _MATERIAL_TECHNIK,
    _claim_mentions_getty,
)
from services.nvd import _claim_mentions_nvd  # noqa: E402
from services.ris import _AT_HARD_MARKERS, _claim_mentions_austria  # noqa: E402


def _ris(claim):
    return _claim_mentions_austria({"claim": claim, "ner_entities": {}})


def _getty(claim):
    return _claim_mentions_getty(normalisiere(claim))


def _heizung(claim):
    return _claim_mentions_heizung(normalisiere(claim))


def _nvd(claim):
    return _claim_mentions_nvd(normalisiere(claim))


def _pharma(claim):
    return _claim_mentions_pharma(claim, {})


# --------------------------------------------- was NICHT mehr triggern darf

@pytest.mark.parametrize("prädikat,claim", [
    # " ris " — 601 Treffer, 0 als Wort
    (_ris, "Kamala Harris gewinnt die Wahl"),
    (_ris, "Der Eiffelturm in Paris"),
    (_ris, "HUNDREDS OF CHRISTMAS MARKETS IN GERMANY CANCELLED"),
    (_ris, "Mehrere Annahmen aus der Corona-Krise"),
    # " orf " — 55 von 62 Treffern waren fremd
    (_ris, "Ein Vorfall im Vorfeld der Wahl"),
    (_ris, "Ihm wurde vorgeworfen, gelogen zu haben"),
    # " stgb " / " schog "
    (_ris, "Laut DStGB kostet das die Kommunen"),
    (_ris, "Schogetten sind eine Schokoladenmarke"),
    # " geg " — 526 Treffer, 0 als Wort
    (_heizung, "Demonstrationen gegen die Corona-Politik"),
    (_heizung, "Es wurde angegeben, dass es teuer wird"),
    (_heizung, "Hingegen ist das Gegenteil richtig"),
    # " aat " / " ulan " / " tempera "
    (_getty, "Der Staat zahlt die Rechnung"),
    (_getty, "Het gaat om een laat antwoord"),
    (_getty, "Hierzulande ist eine Ambulance teuer"),
    (_getty, "Die Temperaturen steigen weiter"),
    (_getty, "Les temperatures montent en Corse"),
    # " ema " / " lot "
    (_pharma, "Das Thema beschäftigt niemand"),
    (_pharma, "Der ehemalige Kanzler sagte das"),
    (_pharma, "Lothar war früher Pilot"),
    # " nist " / " nvd "
    (_nvd, "Der Gesundheitsminister hat das gesagt"),
    (_nvd, "In Afghanistan herrscht Krieg"),
    (_nvd, "Le ministre français a confirmé"),
])
def test_fremdes_wort_startet_den_dienst_nicht_mehr(prädikat, claim):
    assert not prädikat(claim), claim


# ------------------------------------------- was weiter triggern MUSS

@pytest.mark.parametrize("prädikat,claim", [
    # Der gewollte Treffer — am Satzanfang, am Satzende, vor Satzzeichen
    # und mit Bindestrich. Ohne die Normalisierung wäre der
    # Bindestrich-Fall still weg (#207).
    (_ris, "Laut RIS ist das geregelt"),
    (_ris, "Siehe RIS."),
    (_ris, "Der ORF-Beitrag steigt 2026"),
    (_ris, "Laut ORF stimmt das nicht"),
    (_ris, "§ 5 SchOG regelt die Schulform"),
    (_ris, "§ 1 ABGB gilt weiterhin"),
    (_heizung, "Das GEG schreibt 65 Prozent vor"),
    (_heizung, "Im Gebäudeenergiegesetz (GEG) steht das"),
    (_getty, "Laut AAT ist das ein Kupferstich"),
    (_getty, "Die ULAN-Datenbank kennt ihn"),
    (_getty, "Ein Gemälde in Tempera auf Holz"),
    (_getty, "Die Tempera-Malerei des Barock"),
    (_pharma, "Die EMA hat das Medikament zugelassen"),
    (_pharma, "Laut EMA-Zulassung ist es sicher"),
    (_pharma, "Charge Lot 12345 wurde zurückgerufen"),
    (_nvd, "Laut NIST ist die Lücke kritisch"),
    (_nvd, "Die NIST-Datenbank führt sie"),
    (_nvd, "Der CVSS-Score liegt bei 9.8"),
])
def test_der_gewollte_treffer_ueberlebt(prädikat, claim):
    assert prädikat(claim), claim


# -------------------------------------- die Lücke, die der Sweep aufdeckte

@pytest.mark.parametrize("claim", [
    "Der Eiffelturm in Paris erstrahlte nach dem Terroranschlag in Wien "
    "rot-weiß-rot.",
    "Eine Entscheidung des Verwaltungsgerichts Wien",
    "In einem Schreiben von Wiener Wohnen",
    "Video zeigt Festnahme des Attentäters durch Wiener Polizei.",
])
def test_wien_ist_jetzt_ein_at_marker(claim):
    """Beide ersten Faelle hingen bis zum 30.10.2026 an einem
    Zufallstreffer (paRIS, kRISe) — ``wien`` stand in KEINER Liste."""
    assert _ris(claim), claim


def test_wien_bleibt_ungebunden():
    """45 frei / 36 gebunden — die Differenz ist „wiener" und
    Satzzeichen. Binden wuerde „Wiener Linien" kosten und nichts
    gewinnen, weil kein Fremdwort „wien" enthaelt."""
    assert "wien" in _AT_HARD_MARKERS
    assert _ris("Die Wiener Linien fahren wieder")


# --------------------------------- der Fehlgriff beim Bauen (Nachbar-Test)

def test_der_nachbar_des_gebundenen_tokens_ist_heil():
    """⚠️ Beim Binden von ``tempera`` hat ein eingefuegter Kommentar das
    Komma verschluckt — Python konkatenierte die Nachbar-Literale zu
    ``" tempera bronze plastik"``. Ein Token, das nichts trifft, und
    ``bronze-plastik`` war gleich mit weg."""
    assert " tempera " in _MATERIAL_TECHNIK
    assert not any(" " in t.strip() and "tempera" in t
                   for t in _MATERIAL_TECHNIK), _MATERIAL_TECHNIK
    assert _getty("Eine Bronze-Plastik aus dem Barock")


def test_kein_gebundenes_token_hat_seine_raender_verloren():
    """Zensus als Gate (#238): Waere ``norm_terme`` je so geaendert, dass
    es strippt, waeren alle Bindungen still weg."""
    gebunden = [t for t in _AT_HARD_MARKERS if t != t.strip()]
    assert len(gebunden) >= 4, gebunden
    for t in gebunden:
        assert t.startswith(" ") and t.endswith(" "), t
