"""Die Zahl stand im Fakt — die Frage danach kam nie dort an.

Gemessen am 18.10.2026, direkt nach dem Deploy von #260:

    Claim:  "Die EU hat 14 Sanktionspakete gegen Russland verhaengt"
    Antwort: unverifiable @ 0,1, 0 Belege

PR #260 hatte gerade die korrigierte Zahl eingetragen (21 nummerierte
Pakete, Stand Juli 2026). Sie war nicht erreichbar.

## Nicht die Ursache, die ich zuerst vermutete

Mein erster Gedanke war eine Regression aus #260. Gegen ``HEAD~1``
geprueft: nein. Kein Trigger-Token des Fakts enthielt je "paket", vorher
wie nachher. Die erste Trigger-Gruppe fuehrt ``"sanktionen"`` — und das
steckt NICHT in "Sanktion**s**pakete". Eine Buchstabengrenze, die seit
der Anlage des Fakts offen war.

## Und auch nicht "niemand antwortet"

Beim Nachsehen antwortete sehr wohl ein Fakt: ``eu_einstimmigkeit_gasp_2026``
fuehrt ``"sanktionspaket"`` als Token — zu Recht, denn sein Thema ist "Kann
ein einzelnes EU-Land ein Sanktionspaket blockieren?". Er hat nur keine
Paketzahl. Also antwortete der Fakt ueber das VERFAHREN auf eine Frage
nach der ANZAHL, und das Ergebnis war ``unverifiable``.

Die beiden widersprechen sich nicht, sie ergaenzen sich — anders als das
Paar aus #256, wo zwei Fakten dieselbe Groesse mit verschiedenen Zahlen
belegten. Deshalb duerfen hier beide antworten, und der Synthesizer nimmt
den, der die Frage traegt.

## Was ich NICHT messen konnte

Fuer die allgemeine Klasse — "Fakten mit einer praezisen Zahl, die ueber
die naheliegende Frage nicht erreichbar ist" — habe ich einen Zensus
versucht: das Substantiv vor der Kernzahl der Headline als Stichwortfrage.
Ergebnis: 221 von 231 Fakten "gefunden", und die Substantive waren
Zitationsjahre und Einheiten ("'Lancet' (1993)", "'Prozentpunkte'",
"'Original' (2013)"). Die Messung taugt nicht, und ein Gate darauf waere
Rauschen. Dieselbe Lehre wie PR #253: synthetische Sonden aus rohen
Tokens fuehren in die Irre.

Geblieben ist deshalb der EINE beobachtete Fall — plus seine Fragen in
``claim_phrasings_handled``, damit das Gate aus #230 sie festhaelt. Ein
Trigger ohne dokumentierte Phrasing ist eine Zusage, die niemand prueft.

Dependency-light: nur Trigger-Vergleiche, kein Netz, kein Modell.
"""
import glob
import json
import os

import pytest

from services._topic_match import substring_or_composite_match

DATA = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                    "data")
TF = ("trigger_keywords", "trigger_composite", "trigger_all")


def _laden():
    alle = []
    for p in sorted(glob.glob(os.path.join(DATA, "*.json"))):
        with open(p, encoding="utf-8") as fh:
            d = json.load(fh)
        if not isinstance(d, dict):
            continue
        for _k, v in d.items():
            if not isinstance(v, list):
                continue
            for it in v:
                if isinstance(it, dict) and any(it.get(t) for t in TF):
                    alle.append((os.path.basename(p), it))
    return alle


ALLE = _laden()


def _fakt(kennung):
    for _d, it in ALLE:
        if (it.get("id") or it.get("topic")) == kennung:
            return it
    raise AssertionError(f"{kennung} nicht gefunden")


def _wer_antwortet(frage):
    return {it.get("id") or it.get("topic") for _d, it in ALLE
            if substring_or_composite_match(it, frage.lower())}


WIRKUNG = "russland_sanktionen_wirkung_2026"
VERFAHREN = "eu_einstimmigkeit_gasp_2026"


# --- Der gemessene Fall -----------------------------------------------

@pytest.mark.parametrize("frage", [
    "Die EU hat 14 Sanktionspakete gegen Russland verhaengt",
    "Wie viele Sanktionspakete hat die EU gegen Russland verhaengt?",
    "Die EU hat 21 Sanktionspakete gegen Russland beschlossen",
])
def test_die_frage_nach_der_anzahl_erreicht_den_fakt_mit_der_zahl(frage):
    assert WIRKUNG in _wer_antwortet(frage), frage


def test_sanktionspakete_enthaelt_nicht_sanktionen():
    """Die Buchstabengrenze, die den Fall verursacht hat.

    Ohne diesen Test liest sich das neue Token wie Geschmackssache.
    """
    assert "sanktionen" not in "sanktionspakete"
    assert "sanktionspaket" in "sanktionspakete"


def test_das_neue_token_steht_im_fakt():
    toks = [t for g in (_fakt(WIRKUNG).get("trigger_composite") or []) for t in g]
    assert "sanktionspaket" in toks


# --- Was unberuehrt bleiben musste ------------------------------------

@pytest.mark.parametrize("frage", [
    "Ein einzelnes EU-Land kann ein Sanktionspaket alleine blockieren",
    "Veto EU Sanktionspaket",
    "EU Sanktionen Einstimmigkeit",
])
def test_die_verfahrensfrage_bleibt_beim_verfahrens_fakt(frage):
    """Der Fakt ueber Einstimmigkeit und Veto besitzt "Sanktionspaket" zu
    Recht. Die Frage nach dem Verfahren darf NICHT beim Wirkungs-Fakt
    landen — sonst waere der Fix ein Tausch, keine Ergaenzung."""
    antwort = _wer_antwortet(frage)
    assert VERFAHREN in antwort, frage
    assert WIRKUNG not in antwort, (
        f"{frage!r} landet jetzt auch beim Wirkungs-Fakt")


def test_die_beiden_fakten_beantworten_verschiedene_fragen():
    """Begruendung dafuer, dass beide antworten DUERFEN.

    Anders als das Paar aus #256 (zwei Fakten, dieselbe Groesse, zwei
    Zahlen) haben diese zwei verschiedene Gegenstaende: Verfahren gegen
    Wirkung. Waeren sie inhaltsgleich, waere das Ergaenzen der falsche
    Fix gewesen.
    """
    a, b = _fakt(WIRKUNG), _fakt(VERFAHREN)
    assert a.get("scope") != b.get("scope")
    assert a.get("source_url") != b.get("source_url")
    a_text = json.dumps(a, ensure_ascii=False)
    b_text = json.dumps(b, ensure_ascii=False)
    assert "21 nummerierte Pakete" in a_text, "die Zahl steht im Wirkungs-Fakt"
    assert "21 nummerierte Pakete" not in b_text, (
        "der Verfahrens-Fakt traegt jetzt auch eine Paketzahl — dann "
        "stehen zwei Fakten mit derselben Groesse da, und das ist der "
        "Fall aus #256")


# --- Die Zusage wird geprueft ----------------------------------------

@pytest.mark.parametrize("phrasing", [
    "Wie viele Sanktionspakete hat die EU gegen Russland verhaengt",
    "Die EU hat 14 Sanktionspakete gegen Russland verhaengt",
])
def test_die_neuen_fragen_stehen_in_den_phrasings(phrasing):
    """Ein Trigger ohne dokumentierte Phrasing ist eine Zusage, die
    niemand prueft. So stehen sie unter dem Gate aus #230."""
    assert phrasing in (_fakt(WIRKUNG).get("claim_phrasings_handled") or [])


def test_die_alte_zahl_bleibt_als_einstieg():
    """Wer "14 Pakete" behauptet, soll den Fakt finden und die aktuelle
    Zahl bekommen — dieselbe Ueberlegung wie bei den SIPRI-Triggern in
    #260. Die Phrasing nennt bewusst 14, nicht 21."""
    phr = _fakt(WIRKUNG).get("claim_phrasings_handled") or []
    assert any("14 Sanktionspakete" in p for p in phr)


# --- Die Gegenproben ueber alle Fakten -------------------------------

def test_kein_muss_treffer_verloren():
    """Basislinie aus #230: 0."""
    verloren = [(it.get("id") or it.get("topic"), ph) for _d, it in ALLE
                for ph in it.get("claim_phrasings_handled") or []
                if not substring_or_composite_match(it, ph.lower())]
    assert not verloren, f"{len(verloren)} verloren: {verloren[:5]}"


def test_der_wirkungs_fakt_zieht_keine_fremden_phrasings():
    """Ueber-Trigger-Sweep: Das neue Token darf keine Formulierung
    anziehen, die einem anderen Fakt gehoert."""
    ziel = _fakt(WIRKUNG)
    eigen = set(ziel.get("claim_phrasings_handled") or [])
    fremd = [(it.get("id") or it.get("topic"), ph) for _d, it in ALLE
             for ph in it.get("claim_phrasings_handled") or []
             if ph not in eigen and substring_or_composite_match(ziel, ph.lower())]
    assert not fremd, f"{len(fremd)} fremde Treffer: {fremd[:5]}"


def test_alle_fakten_sind_erfasst():
    assert len(ALLE) >= 660, len(ALLE)
