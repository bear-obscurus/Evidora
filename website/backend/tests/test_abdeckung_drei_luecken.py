"""Drei geschlossene Abdeckungsluecken (2026-08-29, Retrieval-Zensus Schritt 1).

Herkunft: Der Retrieval-Zensus (PR #120) zeigte, dass 75-77 % aller
Fehlverdicts im Retrieval entstehen. Von den 38 historischen Klasse-B-Claims
(keine Evidenz + falsches Verdict) waren nach den Cluster-Sprints aber nur
noch VIER offen — und einer davon (#317 Arztbesuche) beantwortet sich
inzwischen von selbst korrekt. Geblieben sind drei:

  #71  „Die Haelfte aller Lebensmittel in Oesterreich wird weggeworfen"
       live `unverifiable@0.1`, soll `false`
  #65  „Wegen dem DMA muss Apple in der EU alternative App-Stores erlauben"
       live `unverifiable@0.1`, soll `true`
  #58  „Das EU-Parlament hat seinen Sitz in Bruessel"
       live `false@0.85`, soll `mixed`

Der Lebensmittel-Fakt ist zugleich ein Anwendungsfall der #321-Lehre: Die
„Haelfte" im Mythos ist eine REALE Zahl — sie bezeichnet nur eine andere
Bezugsgroesse (Haushalts-Anteil an den vermeidbaren ABFAELLEN, nicht Anteil
der weggeworfenen Lebensmittel an ALLEN Lebensmitteln). Deshalb bindet die
Headline die Bezugsgroesse ausdruecklich, statt die Zahl bloss zu bestreiten.

Gepinnt wird, was die Fakten WIRKEN laesst — nicht ihr Wortlaut:
  * Headline unter dem 400-Zeichen-Prompt-Cap (sonst erreicht die
    entscheidende Aussage den Synthesizer nicht garantiert),
  * kein STRUKTURELL-Override-Token (die Fakten differenzieren; ein Marker
    wuerde die jeweilige Gegenrichtung mit-kippen),
  * die eigene Phrasings-Battery (Trigger-Fix ohne Muss-Treffer-Kontrolle
    laesst „Fakt komplett stumm" als Erfolg durchgehen),
  * kein Ueber-Triggern auf themenfremde Claims.
"""

import json
from pathlib import Path

import pytest

from services._struct_marker import has_false_verdict_override
from services._topic_match import find_matching_items
from services.demokratie_pack import claim_mentions_demokratie_cached
from services.landwirtschaft_pack import claim_mentions_landwirtschaft_cached
from services.tech_ki_pack import claim_mentions_tech_ki_cached

DATA = Path(__file__).resolve().parents[1] / "data"

FAKTEN = {
    "lebensmittelabfall_at_2026": ("landwirtschaft_pack.json", claim_mentions_landwirtschaft_cached),
    "dma_app_marktplaetze_eu_2026": ("tech_ki_pack.json", claim_mentions_tech_ki_cached),
    "ep_sitz_strassburg_2026": ("demokratie_pack.json", claim_mentions_demokratie_cached),
}

ZIEL_CLAIMS = {
    "lebensmittelabfall_at_2026": "Die Hälfte aller Lebensmittel in Österreich wird weggeworfen.",
    "dma_app_marktplaetze_eu_2026": "Wegen dem Digital Markets Act muss Apple in der EU alternative App-Stores erlauben.",
    "ep_sitz_strassburg_2026": "Das EU-Parlament hat seinen Sitz in Brüssel.",
}

# Themenfremde Kontroll-Claims quer durch die QA-Korpora.
FREMD = [
    "In Österreich sterben mehr Menschen an Herz-Kreislauf-Erkrankungen als an Krebs.",
    "Die Krone bekommt die meisten Inserate der Bundesregierung.",
    "Viktor Orbán ist Ungarns Ministerpräsident.",
    "Der Fleischverzehr in Österreich liegt bei fast 65 Kilo pro Kopf im Jahr.",
    "Österreich hat mehr als 2.000 Kilometer Autobahnen und Schnellstraßen.",
    "Windkraft liefert in Österreich mehr Strom als Photovoltaik.",
    "Die Jugendarbeitslosigkeit in Österreich liegt über 20 Prozent.",
    "Bei der Feuerwehr in Österreich gibt es viel mehr Freiwillige als Berufsfeuerwehrleute.",
]


def _fakt(fid):
    pack, _ = FAKTEN[fid]
    facts = json.loads((DATA / pack).read_text(encoding="utf-8"))["facts"]
    treffer = [f for f in facts if f.get("id") == fid]
    assert treffer, f"Fakt {fid} fehlt in {pack}"
    return treffer[0]


@pytest.mark.parametrize("fid", sorted(FAKTEN))
def test_headline_unter_prompt_cap(fid):
    """> 400 Zeichen => claim-zentrierte Trunkierung, und die entscheidende
    Aussage ist nicht mehr garantiert im Prompt (Lehrgeld #321/#117)."""
    head = _fakt(fid)["headline"]
    assert len(head) <= 400, f"{fid}: Headline {len(head)} Zeichen"


@pytest.mark.parametrize("fid", sorted(FAKTEN))
def test_kein_struktur_override(fid):
    """Alle drei Fakten differenzieren, sie widerlegen nicht pauschal — ein
    STRUKTURELL-Marker wuerde die Gegenrichtung mit-kippen."""
    assert not has_false_verdict_override(
        _fakt(fid)["data"].get("kernsatz_fuer_synthesizer", "")
    )


@pytest.mark.parametrize("fid", sorted(FAKTEN))
def test_eigene_phrasings_treffen(fid):
    _, gate = FAKTEN[fid]
    for phrasing in _fakt(fid)["claim_phrasings_handled"]:
        assert gate(phrasing), f"{fid}: Phrasing trifft nicht — {phrasing!r}"


@pytest.mark.parametrize("fid", sorted(ZIEL_CLAIMS))
def test_zielclaim_erreicht_genau_diesen_fakt(fid):
    """Nicht nur „das Pack feuert", sondern „DIESER Fakt matcht" — sonst
    koennte ein Nachbar-Fakt den Treffer vortaeuschen."""
    pack, _ = FAKTEN[fid]
    claim = ZIEL_CLAIMS[fid]
    hits = find_matching_items(str(DATA / pack), "facts", claim_lc=claim.lower(),
                               full_claim=claim, descriptor_fn=None)
    assert fid in [h.get("id") for h in hits]


@pytest.mark.parametrize("fid", sorted(FAKTEN))
def test_kein_ueber_triggern_auf_fremde_claims(fid):
    """Beidseitige Pinnung: Leck zu UND eigene Themen treffen weiter."""
    pack, _ = FAKTEN[fid]
    for claim in FREMD:
        hits = find_matching_items(str(DATA / pack), "facts", claim_lc=claim.lower(),
                                   full_claim=claim, descriptor_fn=None)
        assert fid not in [h.get("id") for h in hits], \
            f"{fid} feuert auf themenfremden Claim: {claim!r}"


# --------------------------------------------------------------------------
# Inhaltliche Pins — die Zahlen, wegen derer der Fakt existiert
# --------------------------------------------------------------------------

def test_lebensmittel_bindet_die_bezugsgroesse():
    """Die #321-Lehre angewandt: Die „Haelfte" ist eine echte Zahl fuer eine
    ANDERE Bezugsgroesse. Der Fakt muss beide benennen und zuordnen."""
    f = _fakt("lebensmittelabfall_at_2026")
    head = f["headline"].lower()
    assert "hälfte" in head
    assert "haushalt" in head, "Die Bezugsgröße der 'Hälfte' muss in der Headline stehen"
    assert "bezugsgröße" in head or "bezugsgroesse" in head
    d = f["data"]
    assert d["haushalte_t"] == 685000 and d["vermeidbare_abfaelle_at_t"] == 1200000


def test_dma_nennt_rechtsgrundlage_und_eu_beschraenkung():
    f = _fakt("dma_app_marktplaetze_eu_2026")
    head = f["headline"]
    assert "2022/1925" in head, "Rechtsgrundlage gehört in die Headline"
    assert "17.4" in head
    assert "EU" in head and ("NUR" in head or "nur" in head), \
        "Die EU-Beschränkung ist der entscheidende Vorbehalt"


def test_ep_sitz_nennt_alle_drei_orte():
    f = _fakt("ep_sitz_strassburg_2026")
    head = f["headline"].lower()
    # ß/ss: die Headline schreibt STRASSBURG in Versalien — beide
    # Schreibweisen sind korrekt, der Trigger kennt ohnehin beide.
    assert "straßburg" in head or "strassburg" in head
    for ort in ("brüssel", "luxemburg"):
        assert ort in head, f"{ort} fehlt in der Headline"
    assert "protokoll nr. 6" in head, "Rechtsgrundlage fehlt"
