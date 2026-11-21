"""Geringfügigkeitsgrenze und Reallöhne: drei neue Fakten im Arbeitsmarkt-Pack.

Beide Themen standen in ``current_status.md`` als Abdeckungslücken. Die
Varianten-Batterie vom 5.11.2026 hat gezeigt, dass die Zuordnung gerade bei
alltagsnahen Formulierungen Fakten verliert (67 %). Darum stehen hier neben
den Fachbegriffen Sätze, wie Menschen sie tippen: „Wie viel darf ich
geringfügig dazuverdienen?", „beim Stempeln", „geringfügig hackeln",
„Verdienen wir heute weniger als vor der Teuerung?".

Was der Test zusagt:

1. Jede dokumentierte Phrasing trifft ihren eigenen Fakt, ausdrücklich im
   exakten Pass (``substring_or_composite_match``) — das Pack läuft mit
   ``descriptor_fn=None``, ohne Cosine-Rückfall.
2. Die neuen Trigger ziehen keine fremden Claims an: keine der rund 2.600
   dokumentierten Phrasings ANDERER Fakten (alle ``data/*.json``) trifft
   einen der drei neuen Fakten, und eine Liste naher, aber fremder Claims
   (geringfügig als Adverb, deutsche Reallöhne, Inflation ohne Lohn,
   Mindestlohn, AMS-Quote, Kalte Progression …) bleibt draußen.
3. Umgekehrt treffen die neuen Phrasings nur die drei neuen Fakten. Die
   einzige erlaubte Überschneidung liegt zwischen den beiden
   Geringfügigkeits-Fakten: Wer als Arbeitsloser fragt, ob er geringfügig
   dazuverdienen darf, bekommt auch die Höhe der Grenze. Diese Paare sind
   unten einzeln aufgezählt.
4. Kurze Tokens: „geringfügig" steht nie allein (als Adverb traefe es
   „stieg geringfügig"), „AMS" nur gebunden (`` ams ``, sonst ⊂ „langsam").

Modellfrei: kein Netz, kein SpaCy, kein sentence-transformers.
"""

import glob
import json
import os
import sys

import pytest

BACKEND = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(BACKEND, "data")
sys.path.insert(0, BACKEND)

from services._topic_match import substring_or_composite_match  # noqa: E402

PACK = os.path.join(DATA, "arbeitsmarkt_pack.json")
NEU = (
    "geringfuegigkeitsgrenze_at_2026",
    "geringfuegig_arbeitslos_zuverdienst_2026",
    "realloehne_at_2020_2025",
)
TF = ("trigger_keywords", "trigger_composite", "trigger_all")


def _pack_fakten() -> dict:
    with open(PACK, encoding="utf-8") as fh:
        return {f["id"]: f for f in json.load(fh)["facts"]}


def _alle_fakten() -> list[tuple[str, dict]]:
    alle = []
    for p in sorted(glob.glob(os.path.join(DATA, "*.json"))):
        with open(p, encoding="utf-8") as fh:
            d = json.load(fh)
        if not isinstance(d, dict):
            continue
        for v in d.values():
            if isinstance(v, list):
                for it in v:
                    if isinstance(it, dict) and any(it.get(t) for t in TF):
                        alle.append((os.path.basename(p), it))
    return alle


FAKTEN = _pack_fakten()
ALLE = _alle_fakten()


def _fid(it: dict) -> str:
    return it.get("id") or it.get("topic") or ""


def _trifft(fid: str, claim: str) -> bool:
    return substring_or_composite_match(FAKTEN[fid], claim.lower())


# ---------------------------------------------------------------------------
# 1. Muss-Treffer: jede Phrasing ihr eigener Fakt
# ---------------------------------------------------------------------------

# (Fakt, Phrasing) — ausgeschrieben, nicht aus der Datei gelesen: Sonst
# bestünde der Test auch dann, wenn jemand eine Phrasing still entfernt.
MUSS = [
    ("geringfuegigkeitsgrenze_at_2026", "Wie viel darf ich geringfügig dazuverdienen?"),
    ("geringfuegigkeitsgrenze_at_2026", "Wie hoch ist die Geringfügigkeitsgrenze 2026?"),
    ("geringfuegigkeitsgrenze_at_2026", "Die Geringfügigkeitsgrenze wurde 2026 nicht erhöht"),
    ("geringfuegigkeitsgrenze_at_2026", "Die Geringfügigkeitsgrenze liegt bei 551 Euro im Monat"),
    ("geringfuegigkeitsgrenze_at_2026", "Bin ich als Geringfügige krankenversichert?"),
    ("geringfuegigkeitsgrenze_at_2026", "Geringfügig Beschäftigte sind nur unfallversichert"),
    ("geringfuegigkeitsgrenze_at_2026", "Mit zwei geringfügigen Jobs bleibt man sozialversicherungsfrei"),
    ("geringfuegigkeitsgrenze_at_2026", "Darf ich neben dem Studium geringfügig arbeiten?"),
    ("geringfuegigkeitsgrenze_at_2026", "Wie viel darf man geringfügig hackeln?"),
    ("geringfuegig_arbeitslos_zuverdienst_2026", "Darf ich als Arbeitsloser geringfügig dazuverdienen?"),
    ("geringfuegig_arbeitslos_zuverdienst_2026", "Arbeitslose dürfen seit 2026 nichts mehr dazuverdienen"),
    ("geringfuegig_arbeitslos_zuverdienst_2026", "Ich bin beim AMS und will geringfügig arbeiten"),
    ("geringfuegig_arbeitslos_zuverdienst_2026", "Kann ich beim Stempeln einen Nebenjob haben?"),
    ("geringfuegig_arbeitslos_zuverdienst_2026", "Neben der Notstandshilfe ist ein Zuverdienst verboten"),
    ("geringfuegig_arbeitslos_zuverdienst_2026", "Ab 50 dürfen Arbeitslose weiter geringfügig arbeiten"),
    ("geringfuegig_arbeitslos_zuverdienst_2026",
     "Wer Arbeitslosengeld bekommt, darf bis zur Geringfügigkeitsgrenze dazuverdienen"),
    ("realloehne_at_2020_2025", "Verdienen wir heute weniger als vor der Teuerung?"),
    ("realloehne_at_2020_2025", "Die Reallöhne in Österreich sind seit 2020 gesunken"),
    ("realloehne_at_2020_2025", "2022 gab es in Österreich einen historischen Reallohnverlust"),
    ("realloehne_at_2020_2025", "Die Teuerung hat unsere Löhne aufgefressen"),
    ("realloehne_at_2020_2025", "Die Lohnabschlüsse haben die Inflation nicht ausgeglichen"),
    ("realloehne_at_2020_2025", "Seit der Inflation kann man sich mit dem Gehalt weniger leisten"),
    ("realloehne_at_2020_2025", "Kriegen wir real weniger Lohn als vor Corona?"),
    ("realloehne_at_2020_2025", "Sind unsere Reallöhne besser als im Rest der EU?"),
]


@pytest.mark.parametrize("fid,claim", MUSS, ids=[f"{f[:12]}-{c[:30]}" for f, c in MUSS])
def test_phrasing_trifft_ihren_fakt(fid, claim):
    assert fid in FAKTEN, f"{fid} fehlt in arbeitsmarkt_pack.json"
    assert claim in (FAKTEN[fid].get("claim_phrasings_handled") or []), (
        f"{fid}: Phrasing nicht dokumentiert: {claim!r}")
    assert _trifft(fid, claim), f"{fid} trifft {claim!r} nicht"


def test_dokumentierte_phrasings_sind_genau_die_gepruften():
    """Wer eine Phrasing in der Datei ergänzt, ergänzt sie auch hier."""
    for fid in NEU:
        assert fid in FAKTEN, fid
        doku = set(FAKTEN[fid].get("claim_phrasings_handled") or [])
        hier = {c for f, c in MUSS if f == fid}
        assert doku == hier, (fid, doku ^ hier)


def test_alltagsformulierungen_sind_dabei():
    """Erfolgskriterium: ein Teil ist so formuliert, wie Menschen tippen."""
    alltag = [
        "Wie viel darf ich geringfügig dazuverdienen?",
        "Wie viel darf man geringfügig hackeln?",            # österreichisch
        "Kann ich beim Stempeln einen Nebenjob haben?",        # österreichisch
        "Verdienen wir heute weniger als vor der Teuerung?",   # Sorgefrage
        "Die Teuerung hat unsere Löhne aufgefressen",
        "Kriegen wir real weniger Lohn als vor Corona?",
    ]
    gesamt = {c for _f, c in MUSS}
    assert set(alltag) <= gesamt


# ---------------------------------------------------------------------------
# 2. Keine fremden Claims: dokumentierte Phrasings aller anderen Fakten
# ---------------------------------------------------------------------------

def test_keine_fremde_phrasing_trifft_einen_neuen_fakt():
    fremd = []
    for datei, it in ALLE:
        if _fid(it) in NEU:
            continue
        for ph in it.get("claim_phrasings_handled") or []:
            for fid in NEU:
                if _trifft(fid, ph):
                    fremd.append((fid, datei, _fid(it), ph))
    assert not fremd, fremd


def test_fremde_phrasings_wurden_wirklich_geprueft():
    """Sonst meldet der Test oben auf einer leeren Menge grün."""
    n = sum(len(it.get("claim_phrasings_handled") or [])
            for _d, it in ALLE if _fid(it) not in NEU)
    assert n >= 2600, n


# Nahe, aber fremde Claims. Jeder Eintrag benennt das Token, das ihn
# anziehen könnte.
FREMD = [
    # "geringfügig" als Adverb
    "Die Arbeitslosigkeit ist im Oktober nur geringfügig gestiegen",
    "ABS verkürzt den Bremsweg nur geringfügig",
    "Die Löhne sind 2025 geringfügig stärker gestiegen als die Preise",
    "Die Inflation sank im Mai geringfügig",
    # AMS als Teilwort
    "In Amsterdam sind die Mieten am höchsten in Europa",
    "Die Wirtschaft wächst langsam, die Arbeitslosigkeit steigt",
    # Arbeitslosigkeit ohne Zuverdienst
    "Die AMS-Arbeitslosenquote ist höher als die Eurostat-Quote",
    "Arbeitslose in Österreich sind im Schnitt über ein Jahr arbeitslos",
    "Die Notstandshilfe soll abgeschafft werden",
    # Lohn/Mindestlohn ohne Realbezug
    "Mindestlohn vernichtet Jobs",
    "Gewerkschaften erhöhen die Löhne um 10 bis 15 Prozent",
    "Frauen verdienen in Österreich 18 Prozent weniger als Männer",
    "Migranten drücken die Löhne",
    # Inflation ohne Lohn
    "Die EZB ist schuld an der Inflation",
    "Die offizielle Inflation ist geschönt",
    "Die Mieten sind stärker gestiegen als die Inflation",
    "Die Pensionen werden 2026 unter der Inflation erhöht",
    # Kalte Progression: Lohn + Inflation, aber Steuerthema
    "Durch die kalte Progression frisst der Staat die Lohnerhöhung",
    # Reallohn ohne Österreich/EU/wir-Bezug: deutscher Claim
    "Die Reallöhne in Deutschland sind 2024 um 3,1 Prozent gestiegen",
    "Reallohnindex Destatis zeigt Plus",
    # Geringfügig, aber Deutschland
    "Die Minijob-Grenze in Deutschland liegt bei 556 Euro",
]


@pytest.mark.parametrize("claim", FREMD)
def test_fremder_claim_bleibt_draussen(claim):
    treffer = [fid for fid in NEU if fid in FAKTEN and _trifft(fid, claim)]
    assert not treffer, f"{claim!r} zieht {treffer}"


# ---------------------------------------------------------------------------
# 3. Neue Phrasings ziehen keine alten Fakten
# ---------------------------------------------------------------------------

# Die einzige erlaubte Überschneidung: Zuverdienst-Fragen bekommen auch die
# Grenze. Ausgeschrieben, damit eine neue Überschneidung auffällt.
GESICHTET = {
    ("geringfuegigkeitsgrenze_at_2026", "Darf ich als Arbeitsloser geringfügig dazuverdienen?"),
    ("geringfuegigkeitsgrenze_at_2026", "Ich bin beim AMS und will geringfügig arbeiten"),
    ("geringfuegigkeitsgrenze_at_2026", "Ab 50 dürfen Arbeitslose weiter geringfügig arbeiten"),
    ("geringfuegigkeitsgrenze_at_2026",
     "Wer Arbeitslosengeld bekommt, darf bis zur Geringfügigkeitsgrenze dazuverdienen"),
}


def test_neue_phrasings_treffen_nur_gesichtete_fakten():
    ueber = set()
    for _f, claim in MUSS:
        for _datei, it in ALLE:
            fid = _fid(it)
            if fid == _f:
                continue
            if substring_or_composite_match(it, claim.lower()):
                ueber.add((fid, claim))
    assert ueber == GESICHTET, {"neu": ueber - GESICHTET, "weg": GESICHTET - ueber}


# ---------------------------------------------------------------------------
# 4. Kurze Tokens
# ---------------------------------------------------------------------------

def _alle_tokens(it: dict) -> list[str]:
    toks = list(it.get("trigger_keywords") or [])
    for grp in it.get("trigger_composite") or []:
        toks += grp
    for regel in it.get("trigger_all") or []:
        for grp in regel:
            toks += grp
    return toks


@pytest.mark.parametrize("fid", NEU)
def test_geringfuegig_und_ams_nie_ungebunden(fid):
    toks = _alle_tokens(FAKTEN[fid])
    assert "geringfuegig" not in toks and "geringfügig" not in toks
    assert "ams" not in toks, "ungebunden steckt 'ams' in 'langsam'"
    # Ungebunden mindestens drei Zeichen; ein Kürzel wie " eu " nur mit
    # Leerzeichen an beiden Seiten (sonst ⊂ „LEUte", siehe Über-Trigger-Sweep).
    kurz = [t for t in toks if len(t.strip()) < 3 and not (t.startswith(" ") and t.endswith(" "))]
    assert not kurz, kurz


# ---------------------------------------------------------------------------
# 5. Belege: jede Zahl mit Quelle und Stand
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("fid", NEU)
def test_fakt_hat_quelle_und_stand(fid):
    f = FAKTEN[fid]
    assert f["source_url"].startswith("https://")
    assert f["secondary_url"].startswith("https://")
    assert "Stand" in f["headline"], "Stand gehört in die Headline, die wird nicht gekürzt"
    assert f["year"] == 2026


def test_kernzahlen_stehen_im_fakt():
    """Die Zahlen, auf die eine Alltagsfrage zielt, stehen in der Headline."""
    h = FAKTEN["geringfuegigkeitsgrenze_at_2026"]["headline"]
    assert "551,10" in h and "nicht erhöht" in h
    h = FAKTEN["geringfuegig_arbeitslos_zuverdienst_2026"]["headline"]
    assert "1.1.2026" in h and "31.1.2026" in h
    h = FAKTEN["realloehne_at_2020_2025"]["headline"]
    for z in ("−3,4 %", "+4,2 %", "8,6 %", "7,8 %", "+2,7 %"):
        assert z in h, z


def test_pack_bleibt_exakt():
    """Kein Cosine-Rückfall: Das Pack übergibt descriptor_fn=None."""
    with open(os.path.join(BACKEND, "services", "arbeitsmarkt_pack.py"), encoding="utf-8") as fh:
        src = fh.read()
    assert "descriptor_fn=None" in src
