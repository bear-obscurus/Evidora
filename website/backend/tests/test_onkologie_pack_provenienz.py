"""Onkologie-Pack: Provenienz und die Mistel-Evidenz von 2008 auf 2024.

Befund (2026-09-05, letzter Eintrag aus dem Freshness-Rückstand): Der Pack hatte
**überhaupt keine Metadaten** — kein `schema_version`, kein `fetched_at_iso`,
kein `source_label`. Der Freshness-Check konnte deshalb nur „—/124d" melden: er
sah kein Feld, nur die Datei-mtime. Eine Datei ohne Stichtag kann nicht altern,
weil niemand weiss, wann sie zuletzt geprüft wurde.

Inhaltlich waren neun der zehn Fakten in Ordnung. Ein Europe-PMC-Sweep 2024-2026
je Thema (Mikrowelle, Zucker/Ketogen, Säure-Basen, Laetril, Deo/Aluminium, BH,
Vitamin C) fand nichts, was die Aussagen widerlegt — die Treffer betrafen
Mikrowellen-ABLATION als Therapieverfahren, Amygdalin-Nanopartikel in vitro und
eine Retraction. Das ist erwartbar: Mythos-Widerlegungen altern langsam.

Geändert wurde die **Misteltherapie**. Der Fakt ruhte allein auf dem Cochrane-
Review von 2008. Seither gibt es MISTRAL — ein doppelblindes, placebo-
kontrolliertes Phase-III-RCT (n=290, fortgeschrittenes Pankreaskarzinom,
Dtsch Arztebl Int 2024, Lebensqualitäts-Auswertung Palliat Med 2026). Es
BESTÄTIGT die Cochrane-Einschätzung: kein Überlebensvorteil (HR 1,13), kein
Lebensqualitäts-Unterschied (p=0,86). Cochrane 2008 bleibt deshalb zitiert —
eine neue Studie, die dasselbe zeigt, ersetzt die alte nicht, sie stützt sie.
"""

import asyncio
import json
from pathlib import Path

BACKEND = Path(__file__).resolve().parents[1]
PFAD = BACKEND / "data" / "onkologie_pack.json"
DATEN = json.loads(PFAD.read_text(encoding="utf-8"))
FAKTEN = {f["topic"]: f for f in DATEN["facts"]}

MAX_STR = 400  # synthesizer.MAX_STR

from services.onkologie_pack import search_onkologie  # noqa: E402


def _ergebnis(claim, topic):
    r = asyncio.run(search_onkologie({"claim": claim, "original_claim": claim}))
    for x in r["results"]:
        if x["topic"] == topic:
            return x
    raise AssertionError(f"{claim!r} liefert nichts zu {topic!r}")


# --------------------------------------------------------------------------
# Provenienz: der Pack hatte keine
# --------------------------------------------------------------------------

def test_pack_hat_ueberhaupt_metadaten():
    for feld in ("schema_version", "fetched_at_iso", "source_label", "letzte_pruefung"):
        assert feld in DATEN, f"{feld} fehlt — der Pack hatte gar keine Metadaten"


def test_stichtag_ist_ein_datum():
    assert len(DATEN["fetched_at_iso"]) == 10
    assert DATEN["fetched_at_iso"].count("-") == 2


def test_pruefvermerk_sagt_was_geprueft_wurde_und_was_nicht():
    """Ein Vermerk 'geprüft' ohne Angabe, WAS geprüft wurde, ist wertlos."""
    v = DATEN["letzte_pruefung"]
    assert "2026-09-05" in v
    assert "Mistel" in v and "Früherkennung" in v


def test_kein_fakt_verlorengegangen():
    """Der Früherkennungs-Fakt wurde in vier fokussierte Einträge geteilt
    (eigener PR) — die neun Mythos-Fakten müssen unberührt bleiben."""
    assert len(DATEN["facts"]) == 13
    assert "mistel_krebs_therapie_mythos" in FAKTEN


# --------------------------------------------------------------------------
# Mistel: MISTRAL ergänzt, Cochrane bleibt
# --------------------------------------------------------------------------

def test_mistral_studie_ist_im_fakt():
    kern = FAKTEN["mistel_krebs_therapie_mythos"]["data"]["kernsatz_fuer_synthesizer"]
    assert "MISTRAL" in kern
    for zahl in ("290", "1,13", "7,8", "8,3", "0,86"):
        assert zahl in kern, f"{zahl} fehlt im Kernsatz"
    assert "38915151" in kern, "PMID fehlt — ohne sie ist die Studie nicht auffindbar"


def test_mistral_steht_auch_in_der_headline():
    """Die Headline ist der Kanal, den die Prompt-Kürzung nachweislich
    ungekürzt passiert (≤400 Zeichen)."""
    kopf = FAKTEN["mistel_krebs_therapie_mythos"]["headline"]
    assert "MISTRAL" in kopf and "1,13" in kopf and "0,86" in kopf
    assert len(kopf) <= MAX_STR


def test_cochrane_2008_bleibt_zitiert():
    """MISTRAL bestätigt Cochrane, es ersetzt es nicht. Eine neue Studie mit
    demselben Ergebnis macht die alte nicht ungültig."""
    f = FAKTEN["mistel_krebs_therapie_mythos"]
    assert "Horneber" in f["data"]["kernsatz_fuer_synthesizer"]
    assert "Cochrane" in f["headline"]
    assert "Cochrane" in f["source_label"] and "MISTRAL" in f["source_label"]


def test_mistel_richtung_stimmt():
    """Kein Vorteil heisst kein Vorteil — die Zahlen dürfen nicht als Erfolg
    gelesen werden können."""
    kopf = FAKTEN["mistel_krebs_therapie_mythos"]["headline"]
    assert "weder Überlebens- noch Lebensqualitäts-Vorteil" in kopf
    kern = FAKTEN["mistel_krebs_therapie_mythos"]["data"]["kernsatz_fuer_synthesizer"]
    assert "KEIN Überlebensvorteil" in kern and "KEIN Unterschied" in kern


def test_mistel_erreicht_den_prompt_ungekuerzt():
    """Der Fakt trägt einen STRUKTURELL-Marker und ist damit von der
    400-Zeichen-Kürzung ausgenommen."""
    x = _ergebnis("Mistel heilt Krebs", "mistel_krebs_therapie_mythos")
    assert "STRUKTURELL FALSCH:" in x["display_value"]
    assert "MISTRAL" in x["display_value"]


# --------------------------------------------------------------------------
# Was NICHT geändert wurde, soll auch nicht kaputtgehen
# --------------------------------------------------------------------------

def test_alle_headlines_unter_dem_prompt_budget():
    zu_lang = {f["topic"]: len(f["headline"]) for f in DATEN["facts"]
               if len(f["headline"]) > MAX_STR}
    assert not zu_lang, f"Headlines über {MAX_STR} Zeichen: {zu_lang}"


def test_alle_dokumentierten_phrasings_treffen():
    from services.onkologie_pack import claim_mentions_onkologie_cached
    fehl = [p for f in DATEN["facts"]
            for p in f.get("claim_phrasings_handled", [])
            if not claim_mentions_onkologie_cached(p)]
    assert not fehl, f"Phrasings ohne Treffer: {fehl}"


def test_die_sieben_stabilen_mythen_sind_unberuehrt():
    """Der Sweep hat sie bestätigt — sie stehen hier, damit ein späterer
    Refresh nicht versehentlich an ihnen dreht."""
    for topic, marker in (
        ("mikrowelle_krebs_mythos", "nicht-ionisierend"),
        ("zucker_fuettert_krebs_mythos", "Warburg"),
        ("saeure_basen_krebs_mythos", "7,35"),
        ("aprikosenkerne_laetril_mythos", "Amygdalin"),
        ("alkalisches_wasser_krebs_mythos", "Magensäure"),
        ("deo_brustkrebs_mythos", "Aluminium"),
        ("bh_brustkrebs_mythos", "Singer"),
    ):
        f = FAKTEN[topic]
        text = f["headline"] + json.dumps(f["data"], ensure_ascii=False)
        assert marker in text, f"{topic}: {marker!r} fehlt"


def test_kein_ueber_trigger_bei_fachfremden_claims():
    for claim in ("Wie hoch ist die Inflation in Österreich?",
                  "Die Mikrowelle in der Kantine ist kaputt",
                  "Wie viel Zucker ist in Cola?"):
        r = asyncio.run(search_onkologie({"claim": claim, "original_claim": claim}))
        assert not r["results"], f"Über-Trigger: {claim!r}"
