"""Themen-Packs ohne Cosine-Backup (QA100-Wurzel-Fix, 2026-07-27).

Der QA100-Lauf zeigte 15 deterministische `unverifiable@0.1`-Verdicts,
deren gemeinsame Wurzel NICHT in der Verdict-Logik lag: sechs Themen-
Packs lieferten Treffer auf 24-37 % ALLER 100 breit gestreuten Claims —
MedienTransparenz auf „Olympia-Medaillen", Bildung auf „Österreich ist
NATO-Mitglied", Energy-Charts auf „BIP pro Kopf". Der Prompt wurde von
Fremdthemen geflutet; bei Per-Source-Cap [:3] und 400-Zeichen-Truncation
blieb für den passenden Inhalt kein Platz, und das LLM antwortete
korrekt „keine der Quellen enthält das".

Messung im Container: von 176 Treffern kamen **172 aus dem Cosine-
Backup**, nur 4 aus exakten Triggern. Exakt das #41-Muster (Multi-Topic-
Packs sind am Cosine-Level nicht trennbar). Fix analog: erst die eigene
claim_phrasings-Abdeckung von 80 % auf 100 % geschlossen (16 trigger_all-
Regeln), dann descriptor_fn=None. Ergebnis auf denselben 100 Claims:
176 -> 5 Treffer, davon alle fünf thematisch korrekt.

Diese Tests pinnen alle drei Seiten:
  1. Phrasings-Battery bleibt 100 % — wer eine Phrasing einträgt, muss
     den Trigger dafür liefern (sonst fällt sie still auf 0 zurück,
     weil es keinen Backup mehr gibt).
  2. Der Cosine-Backup bleibt aus.
  3. Off-Topic-Claims matchen nichts — UND On-Topic-Claims matchen
     weiterhin. Ohne (3) hätte man die Packs einfach nur abgeschaltet.

Kein Modell, kein Netzwerk — reine Substring-/Composite-Logik.
"""

import importlib
import json
import os

import pytest

from services._topic_match import substring_or_composite_match

DATA_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")

# (Service-Modul, Daten-JSON)
TOPIC_PACKS = [
    ("medientransparenz", "medientransparenz.json"),
    ("energy_charts", "energy_charts.json"),
    ("education_dach", "education_dach.json"),
    ("transport_at", "transport_at.json"),
    ("housing_at", "housing_at.json"),
    ("eu_crime", "eu_crime.json"),
]

# Claims aus dem QA100-Lauf, die KEINES dieser Packs etwas angehen.
# Vor dem Fix lieferten alle sechs Packs hierauf Cosine-Treffer.
OFF_TOPIC_CLAIMS = [
    "Bei Olympia 2024 in Paris hat Österreich mehr Medaillen geholt als in Tokio",
    "Österreich ist NATO-Mitglied",
    "Laut Weltbank ist Österreich ein Hocheinkommensland",
    "Österreich hat flächendeckend Glasfaser",
    # "Die Studiengebühren in Österreich sind abgeschafft" stand hier, bis
    # education_dach am 2026-07-28 den Fakt `studienbeitrag_at` bekam
    # (Cluster-A-Lücke #55). Der Claim ist für diesen Service seither
    # ON-topic — siehe test_studienbeitrag_ist_jetzt_on_topic.
    "Das BIP pro Kopf ist in Österreich höher als in Deutschland",
    "Die Lebenserwartung in Österreich liegt über 82 Jahren",
    "Wer gewinnt die nächste Nationalratswahl in Österreich?",
    "In Österreich gibt es mehr Rinder als Schweine",
    "Wien ist die zweitgrößte deutschsprachige Stadt",
]

# Je Pack ein Claim, der es SEHR WOHL angeht — der Gegenbeweis dazu,
# dass hier nicht einfach sechs Quellen stillgelegt wurden.
ON_TOPIC_CLAIMS = {
    "medientransparenz": "Wie viel gibt die Bundesregierung für Inserate aus?",
    "energy_charts": "Wie hoch ist der Anteil der Erneuerbaren am österreichischen Strom?",
    "education_dach": "Wie hat Österreich bei PISA abgeschnitten?",
    "transport_at": "Wie pünktlich ist die ÖBB wirklich?",
    "housing_at": "Explodieren die Mieten in Österreich?",
    "eu_crime": "Wie hoch ist die Mordrate in Österreich?",
}


def _facts(json_name):
    with open(os.path.join(DATA_DIR, json_name), encoding="utf-8") as fh:
        return json.load(fh)["facts"]


@pytest.mark.parametrize("pack,json_name", TOPIC_PACKS)
def test_phrasings_battery_full_exact_coverage(pack, json_name):
    """Jede selbst deklarierte claim_phrasings_handled muss von einem
    exakten Trigger des Packs getroffen werden. Ohne Cosine-Backup ist
    eine ungetriggerte Phrasing eine STILL tote Zusage."""
    facts = _facts(json_name)
    missing = [
        (f.get("topic"), ph)
        for f in facts
        for ph in (f.get("claim_phrasings_handled") or [])
        if not any(substring_or_composite_match(g, ph.lower()) for g in facts)
    ]
    assert not missing, f"{pack}: ungetriggerte Phrasings {missing}"


@pytest.mark.parametrize("pack,json_name", TOPIC_PACKS)
def test_cosine_backup_disabled(pack, json_name):
    """descriptor_fn=None ist der Kontrakt dieses Umbaus — der Backup
    darf nicht unbemerkt zurückkehren."""
    mod = importlib.import_module(f"services.{pack}")
    src = open(mod.__file__, encoding="utf-8").read()
    assert "descriptor_fn=None" in src, (
        f"{pack}: Cosine-Backup wieder aktiv? descriptor_fn=None fehlt.")
    assert "descriptor_fn=_descriptor" not in src


@pytest.mark.parametrize("pack,json_name", TOPIC_PACKS)
@pytest.mark.parametrize("claim", OFF_TOPIC_CLAIMS)
def test_off_topic_claims_match_nothing(pack, json_name, claim):
    facts = _facts(json_name)
    hits = [f.get("topic") for f in facts
            if substring_or_composite_match(f, claim.lower())]
    assert not hits, f"{pack} feuert auf themenfremden Claim {claim!r}: {hits}"


@pytest.mark.parametrize("pack,json_name", TOPIC_PACKS)
def test_on_topic_claim_still_matches(pack, json_name):
    """Gegenrichtung: die Packs müssen ihre eigenen Themen weiterhin
    treffen — sonst wäre der Fix nur eine Abschaltung."""
    claim = ON_TOPIC_CLAIMS[pack]
    facts = _facts(json_name)
    hits = [f.get("topic") for f in facts
            if substring_or_composite_match(f, claim.lower())]
    assert hits, f"{pack}: eigener Themen-Claim {claim!r} trifft nichts mehr"


def test_erzeugungsvergleich_trifft_at_strom_eckdaten():
    """Live-Regression aus der Verifikation (QA100 #44): Nach dem
    Abschalten des Backups fiel 'Windkraft liefert in Österreich mehr
    Strom als Photovoltaik' durch — der Aspekt-Gruppe des Facts fehlten
    die ERZEUGUNGS-Verben. Folge live: nur noch IRENA lieferte Daten,
    und zwar installierte KAPAZITÄT (PV 10.295 MW > Wind 4.292 MW)
    statt Erzeugung — also die falsche Messgröße, Verdict kippte auf
    false. Der Fact muss auf Erzeugungs-Vergleiche triggern."""
    facts = _facts("energy_charts.json")
    claim = "Windkraft liefert in Österreich mehr Strom als Photovoltaik"
    hits = [f.get("topic") for f in facts
            if substring_or_composite_match(f, claim.lower())]
    assert "at_strom_eckdaten" in hits, hits


def test_gesamt_trefferquote_bleibt_niedrig():
    """Aggregat-Wächter: über die zehn Off-Topic-Claims dürfen alle sechs
    Packs zusammen NULL Treffer haben. Vor dem Fix waren es 176 Treffer
    auf 100 Claims (24-37 % pro Pack)."""
    total = 0
    for pack, json_name in TOPIC_PACKS:
        facts = _facts(json_name)
        for claim in OFF_TOPIC_CLAIMS:
            if any(substring_or_composite_match(f, claim.lower())
                   for f in facts):
                total += 1
    assert total == 0, f"{total} Off-Topic-Treffer über alle Packs"


def test_studienbeitrag_ist_jetzt_on_topic():
    """Cluster-A-Lücke #55: education_dach besitzt seit 2026-07-28 den
    Fakt `studienbeitrag_at`. Der Claim war vorher bewusst als
    Off-Topic-Referenz gelistet — die Erwartung hat sich mit dem neuen
    Fakt geändert, nicht das Verhalten des Guards."""
    facts = _facts("education_dach.json")
    hits = [f.get("topic") for f in facts
            if substring_or_composite_match(
                f, "die studiengebühren in österreich sind abgeschafft")]
    assert hits == ["studienbeitrag_at"], hits
