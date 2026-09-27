"""Dieselbe Sache, anderes Wort: „weibliche" statt „Frauen".

Anlass (27.9.2026, Live-Messung zu PR #227). Der Claim

    "Österreich 2024: mehr männliche als weibliche Mordopfer"

bekam live `unverifiable @ 0.1` mit der Begründung, es gebe „keine
geschlechtsspezifische Aufschlüsselung" — während drei andere
Formulierungen derselben Frage den Femizid-Fakt trafen. Die Trigger-Gruppen
kennen die Personen-Substantive („frauen", „männer"), nicht die Adjektive.

Zwei Dinge wurden vorab gemessen und ausgeschlossen:

* KOMPOSITA sind nicht das Problem. „mord" trifft weiterhin in „mordopfer",
  auch nach der Wortgrenzen-Bindung aus #207.
* STEMMING hilft nicht. „weiblich" kommt von „Weib", „Frau" ist
  etymologisch unverwandt — es gibt keinen gemeinsamen Stamm zu prüfen.

## Der Sweep über alle Fakten

Über alle 664 Trigger-Fakten wurden Paare AUS DEN DATEN abgeleitet: Ein Paar
gilt als attestiert, wenn irgendein Fakt beide Wörter in derselben
Trigger-Gruppe führt. 29 Paare mit gemeinsamem Stamm, davon 18 ohne
Teilzeichenketten-Beziehung, 33 mechanisch als „bezahlt" gemessen.

Eine erste Fassung setzte alle 14 Klassen symmetrisch um — und der
Über-Trigger-Sweep wies sie ab: 332 → 350 Fremdtreffer-Paare. Die 18 neuen
waren überwiegend Stichwort-Degeneration, ein Claim-Wort erfüllte zwei
AND-Gruppen desselben Fakts:

    women_bulky_muscles_2026 (Fitness-Mythos) beantwortete
      "Frauen verdienen 18 Prozent weniger als Männer"
    psychotherapie_wirkt_nicht_mythos beantwortete
      "Semaglutid ist wirksamste pharmakologische Adipositas-Therapie"

Geblieben ist deshalb nur die Klasse mit echter Messung dahinter, und nur
in der Richtung NOMEN → ADJEKTIV. Gemessen nach der Verengung:

    Fremdtreffer-Paare       332 -> 332   (kein einziger neuer)
    verlorene Muss-Treffer    58 ->  58   (die 58 sind vorbestehend)
    Adjektiv-Batterie       3/10 -> 6/10

Die vier offenen Batterie-Fälle scheitern NICHT mehr am Geschlecht — das
wurde Gruppe für Gruppe nachgesehen: „Aufsichtsräte" scheitert am
Umlaut-Plural des Tokens „aufsichtsrat", „unbezahlte Arbeit" fehlt dem
Care-Gap-Fakt als Themenwort, und zwei Erwartungen waren zu streng (die
Claims nennen ihr Thema nicht: „mehrere Dinge gleichzeitig" statt
„Multitasking", „Belästigung" ohne „Sexismus"). In allen drei Fällen, in
denen das Adjektiv das EINZIGE Hindernis war, trifft der Fakt jetzt.

Keine Netzabfrage, kein Modell.
"""

import copy
import glob
import json
import os
import sys

import pytest

BACKEND = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(BACKEND, "data")
sys.path.insert(0, BACKEND)

from services._tippfehler import tippfehler_match  # noqa: E402
from services._topic_match import (  # noqa: E402
    find_matching_items,
    substring_or_composite_match,
)
from services._wortformen import REGELN, mit_wortformen  # noqa: E402

TF = ("trigger_keywords", "trigger_composite", "trigger_all")


def _fid(it):
    return it.get("id") or it.get("topic") or ""


def _laden():
    """Ortskundig: EINE Fakt-ID kann in mehreren Dateien liegen.

    ``gender_pay_gap_2026`` liegt in arbeitsmarkt_pack.json UND
    gleichstellung_pack.json — zwei verschiedene Fakten (anderer Scope,
    andere Quellen, andere Trigger) mit derselben ID. Vorbestehend, hier
    nicht behoben, aber ein Instrument, das pro ID nur eine Datei kennt,
    prueft fuer diesen Fakt die falsche und meldet vier Phantom-Verluste.
    """
    orte, alle = {}, []
    for p in sorted(glob.glob(os.path.join(DATA, "*.json"))):
        with open(p, encoding="utf-8") as fh:
            d = json.load(fh)
        if not isinstance(d, dict):
            continue
        for k, v in d.items():
            if not isinstance(v, list):
                continue
            for it in v:
                if isinstance(it, dict) and any(it.get(t) for t in TF):
                    orte.setdefault(_fid(it), []).append((os.path.basename(p), k))
                    alle.append((os.path.basename(p), k, it))
    return orte, alle


ORTE, ALLE = _laden()
KOPIEN: dict = {}
for _d, _k, _it in ALLE:
    KOPIEN.setdefault(_fid(_it), []).append((_d, _it))
FAKT = {f: kopien[-1][1] for f, kopien in KOPIEN.items()}
PHRASINGS = [(d, ph) for d, _k, it in ALLE
             for ph in it.get("claim_phrasings_handled") or []]


def _trifft_echt(fid: str, claim: str) -> bool:
    """Die echte Pipeline: exakter Pass, dann tippfehler-tolerant.

    Bei mehreren Orten genuegt einer — der Claim erreicht den Fakt dann.
    """
    for datei, key in ORTE[fid]:
        treffer = find_matching_items(os.path.join(DATA, datei), key,
                                      claim_lc=claim.lower(), full_claim=claim,
                                      descriptor_fn=None)
        if any(fid in (x.get("id"), x.get("topic")) for x in treffer):
            return True
    return False


def _gruppen(it):
    raus = list(it.get("trigger_composite") or [])
    for regel in it.get("trigger_all") or []:
        raus += list(regel)
    return [g for g in raus if isinstance(g, (list, tuple))]


# ---------------------------------------------------------------------------
# 1. Die Regel selbst
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("gruppe,erwartet", [
    (("frauen", "gender"), ("frauen", "gender", "weiblich")),
    (("maenner",), ("maenner", "weiblich" and "maennlich")),
    (("frauen", "maenner"), ("frauen", "maenner", "weiblich", "maennlich")),
    # Schon abgedeckt: „weibliche" enthaelt den Stamm.
    (("frau", "weibliche"), ("frau", "weibliche")),
    (("leerstand", "abgabe"), ("leerstand", "abgabe")),
])
def test_regel_ergaenzt_genau_das(gruppe, erwartet):
    assert mit_wortformen(gruppe) == erwartet


def test_richtung_ist_absicht():
    """Nur Nomen -> Adjektiv. Die Rueckrichtung war die Ursache des
    women_bulky-Fehltreffers: Gruppe c/1 fuehrt „maennlich" im Sinne
    „maennlich wirkende Muskeln", nicht als Personengruppe."""
    assert mit_wortformen(("maennlich", "muskel")) == ("maennlich", "muskel")
    assert mit_wortformen(("weibliche", "ladies")) == ("weibliche", "ladies")


def test_unbeteiligte_gruppe_bleibt_dasselbe_objekt():
    """Kein Objekt-Neubau fuer die 99 %, die nichts angeht."""
    g = ("leerstand", "abgabe")
    assert mit_wortformen(g) is g


def test_nur_zwei_regeln():
    """Wer eine Klasse ergaenzt, muss den Sweep neu fahren — die Verengung
    auf Geschlecht ist gemessen, nicht geraten."""
    assert len(REGELN) == 2
    ausloeser = set().union(*(a for a, _f in REGELN))
    assert ausloeser == {"frau", "frauen", "mann", "maenner"}


def test_trigger_keywords_werden_nicht_geweitet():
    """Keywords sind der bedingungslose Eingang eines Fakts."""
    nur_keyword = {"id": "probe", "headline": "x", "trigger_keywords": ["frauen"]}
    assert substring_or_composite_match(nur_keyword, "frauen verdienen weniger")
    assert not substring_or_composite_match(nur_keyword, "weibliche angestellte verdienen weniger")


# ---------------------------------------------------------------------------
# 2. Der gemessene Fall und die drei, die er mitnahm
# ---------------------------------------------------------------------------

GESCHLOSSEN = [
    ("femizide_at_de_2026", "Österreich 2024: mehr männliche als weibliche Mordopfer"),
    ("femizide_at_de_2026", "In Österreich wurden mehr weibliche als männliche Personen ermordet"),
    ("sexualisierte_gewalt_dunkelfeld_2026",
     "Weibliche Opfer sexualisierter Gewalt zeigen die Tat selten an"),
]


@pytest.mark.parametrize("fid,claim", GESCHLOSSEN,
                         ids=[f"{f}-{i}" for i, (f, _c) in enumerate(GESCHLOSSEN)])
def test_adjektiv_formulierung_trifft(fid, claim):
    assert _trifft_echt(fid, claim), claim


@pytest.mark.parametrize("fid,claim", GESCHLOSSEN,
                         ids=[f"{f}-{i}" for i, (f, _c) in enumerate(GESCHLOSSEN)])
def test_der_treffer_kommt_wirklich_von_der_regel(fid, claim):
    """Selbstkontrolle: Ohne das Nomen in der Gruppe faellt der Treffer weg.
    Sonst koennte er von irgendeinem anderen Token stammen, und die Tests
    oben bewiesen nichts."""
    ohne = copy.deepcopy(FAKT[fid])
    for feld in ("trigger_composite",):
        ohne[feld] = [[t for t in g if t.lower() not in ("frau", "frauen", "mann", "männer", "maenner")]
                      for g in ohne.get(feld) or []]
    for r, regel in enumerate(ohne.get("trigger_all") or []):
        ohne["trigger_all"][r] = [
            [t for t in g if t.lower() not in ("frau", "frauen", "mann", "männer", "maenner")]
            for g in regel]
    assert not substring_or_composite_match(ohne, claim.lower()), (
        "Der Treffer kam nicht vom Nomen-Trigger — dieser Test prueft nichts")


def test_bekannte_reste_haben_eine_andere_ursache():
    """Vier Batterie-Faelle bleiben offen, aber an keinem ist das Geschlecht
    schuld. Wer einen davon schliesst, dreht die Zeile um."""
    reste = [
        # (Fakt, Claim, warum)
        ("frauenquote_wirksamkeit_2026",
         "Männliche Führungskräfte dominieren die Aufsichtsräte",
         "Token 'aufsichtsrat' trifft nicht in 'Aufsichtsräte' — Umlaut-Plural"),
        ("gender_care_gap_2026",
         "Weibliche Erwerbstätige leisten mehr unbezahlte Arbeit",
         "'unbezahlte Arbeit' fehlt als Themenwort"),
        ("multitasking_2026",
         "Weibliche Gehirne können besser mehrere Dinge gleichzeitig",
         "Claim nennt das Thema nicht — Erwartung war zu streng"),
        ("sexismus_erfahrungen_2026",
         "Weibliche Beschäftigte erleben häufiger Belästigung am Arbeitsplatz",
         "'Belästigung' ohne 'Sexismus' — Erwartung war zu streng"),
    ]
    for fid, claim, _warum in reste:
        assert not _trifft_echt(fid, claim), (
            f"{fid} trifft jetzt doch — Zeile umdrehen: {claim}")
        # Die Geschlechts-Gruppe trifft dabei sehr wohl; es scheitert an einer
        # ANDEREN Gruppe. Das ist der Unterschied zum Ausgangsbefund.
        treffer = [g for g in _gruppen(FAKT[fid])
                   if substring_or_composite_match(
                       {"id": "x", "headline": "",
                        "trigger_keywords": list(mit_wortformen(tuple(g)))},
                       claim.lower())]
        assert treffer, f"{fid}: keine einzige Gruppe trifft — andere Ursache als gemessen"


# ---------------------------------------------------------------------------
# 3. Ueber-Trigger-Sweep auf den betroffenen Fakten — und er schlaegt an
# ---------------------------------------------------------------------------

# Die 22 Fakten, deren Gruppen durch die Regel eine Form dazubekommen.
BETROFFEN = (
    "gender_pay_gap_2026", "multitasking_2026", "frauenquote_wirksamkeit_2026",
    "mint_frauen_anteil_2026", "femizide_at_de_2026",
    "sexualisierte_gewalt_dunkelfeld_2026", "gender_care_gap_2026",
    "frauen_in_politik_2026", "sexismus_erfahrungen_2026",
    "vereinbarkeit_familie_beruf_2026", "gender_equality_index_2026",
    "borderline_klischee_mythos", "oecd_lebenserwartung_2024",
    "mammographie_screening", "psa_prostata_screening",
    "eltern_geschlecht_vererbung_mythos", "antibabypille_gewicht_mythos",
    "menstruation_synchronisation_mythos", "wechseljahre_alter_mythos",
    "kinderbetreuungsgeld_2026", "pflegende_angehoerige_2026",
)

# Fremdtreffer, die es VOR der Regel schon gab — Messung 27.9.2026, vorher
# und nachher identisch. Gesichtet: alle am Thema (Karenz <-> Vereinbarkeit,
# Screening <-> Onkologie, Lebenserwartung <-> Trisomie), kein Leck.
ERLAUBT_FREMD = {
    # Zwei verschiedene Fakten mit DERSELBEN ID in zwei Dateien: Jede
    # Kopie sieht die Phrasings der anderen als "fremd". Vorbestehend.
    "gender_pay_gap_2026": {
        "Frauen verdienen 18 Prozent weniger als Männer",
        "Frauen verdienen 18 Prozent weniger fuer gleiche Arbeit",
        "Frauen verdienen weniger weil sie weniger arbeiten",
        "Gender Pay Gap ist 20 Prozent",
        "Gender Pay Gap existiert nicht",
        "Equal Pay Day ist Mythos",
        "Lohn-Diskriminierung von Frauen",
        "Frauen verhandeln einfach schlechter",
        "Gender Pay Gap ist nur Lifestyle-Choice",
        "Lohnlücke gibt es eigentlich gar nicht",
        "Wenn man bereinigt ist Pay Gap nur 6 Prozent",
    },
    "gender_care_gap_2026": {"Kinderbetreuungsgeld bekommen nur Frauen"},
    "frauen_in_politik_2026": {
        "In Österreich vertrauen die Menschen dem Parlament mehr als im EU-Schnitt",
    },
    "vereinbarkeit_familie_beruf_2026": {
        "Karenz hat keinen Lohn-Effekt",
        "Karenz schadet der Karriere nicht",
        "Karenz vernichtet Karriere komplett",
        "Kinderbetreuungsgeld bekommen nur Frauen",
        "Lange Karenz ist gut fuer Mutter und Karriere",
    },
    "oecd_lebenserwartung_2024": {
        "Down-Syndrom-Lebenserwartung ist nur 30 Jahre",
        "Lebenserwartung Deutschland sinkt",
        "Linkshänder haben 9 Jahre kürzere Lebenserwartung",
        "Trisomie 21 verkürzt Lebenserwartung dramatisch",
    },
    "mammographie_screening": {
        "Mammographie-Screening senkt die Brustkrebs-Sterblichkeit",
        "Tamoxifen ist Standard bei HR-positivem Brustkrebs",
    },
    "psa_prostata_screening": {
        "Active Surveillance ist bei Niedrigrisiko-Prostatakrebs Standard",
        "PSA-Screening ist nicht generell empfohlen",
    },
    "kinderbetreuungsgeld_2026": {"Karenz hat keinen Lohn-Effekt"},
}


def _fremde_treffer(datei: str, it: dict) -> set:
    return {ph for d, ph in PHRASINGS if d != datei
            and (substring_or_composite_match(it, ph.lower())
                 or tippfehler_match(it, ph.lower()))}


@pytest.mark.parametrize("fid", BETROFFEN)
def test_keine_neuen_fremdtreffer(fid):
    for datei, it in KOPIEN[fid]:
        neu = _fremde_treffer(datei, it) - ERLAUBT_FREMD.get(fid, set())
        assert not neu, f"{fid} ({datei}): neue Fremdtreffer {sorted(neu)}"


WO_BULKY = ORTE["women_bulky_muscles_2026"][0][0]


def test_sweep_schlaegt_an():
    """Gift-Probe: die verworfene Rueckrichtung (Adjektiv -> Nomen) von Hand
    in den Fitness-Mythos. Der Sweep MUSS die Gender-Pay- und Femizid-Claims
    melden — sonst ist seine Null oben kein Beweis."""
    it = copy.deepcopy(FAKT["women_bulky_muscles_2026"])
    for g in it["trigger_composite"]:
        if any("maennlich" in str(t).lower() or "männlich" in str(t).lower() for t in g):
            g.append("männer")
    neu = _fremde_treffer(WO_BULKY, it)
    assert any("verdienen" in ph for ph in neu), (
        f"Gift-Probe blieb unentdeckt — der Sweep ist blind. Gefunden: {sorted(neu)}")


@pytest.mark.parametrize("fid", BETROFFEN)
def test_eigene_phrasings_treffen_weiter(fid):
    """Muss-Kontrolle auf den betroffenen Fakten: Die Regel nimmt keinem
    dokumentierten Phrasing seinen Fakt."""
    verloren = [ph for ph in FAKT[fid].get("claim_phrasings_handled") or []
                if not _trifft_echt(fid, ph)]
    assert not verloren, f"{fid}: verlorene Phrasings {verloren}"


# ---------------------------------------------------------------------------
# 4. Was der Sweep nebenbei fand
# ---------------------------------------------------------------------------

def test_doppelte_fakt_id_ist_bekannt():
    """``gender_pay_gap_2026`` liegt zweimal in den Daten — zwei
    VERSCHIEDENE Fakten (anderer Scope, andere Quellen, andere Trigger) mit
    derselben ID. Vorbestehend, hier bewusst nicht behoben: Eine ID zu
    aendern beruehrt Daten, Tests und moeglicherweise Cache-Schluessel und
    braucht ihre eigene Messung.

    Solange das so ist, muss jedes Instrument, das Fakten ueber ihre ID
    einer Datei zuordnet, MEHRERE Orte zulassen. Ein Instrument mit
    ``setdefault`` prueft sonst die falsche Datei und meldete hier vier
    Phantom-Verluste (54 echte statt 58 gemeldeter).

    Wer die ID aufloest, dreht diesen Test um."""
    doppelt = {f: [d for d, _it in kopien] for f, kopien in KOPIEN.items()
               if len(kopien) > 1}
    assert doppelt == {"gender_pay_gap_2026": ["arbeitsmarkt_pack.json",
                                              "gleichstellung_pack.json"]}, doppelt
