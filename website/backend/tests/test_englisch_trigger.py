"""Englische Claims treffen die deutschen Pack-Trigger (2026-09-26).

Anlass, live gegen Produktion gemessen:

    "Is it true that the vacancy tax in Vorarlberg is 3000 euros a year?"
    -> unverifiable@0.1 nach 7 s

Der deutsche Claim zur selben Sache liefert true@0.9 — es fehlte das
Retrieval, nicht der Fakt. Neu ist ein dritter, englischer Pass in
``find_matching_items`` (services/_englisch.py): ein kuratiertes Glossar
schreibt die deutschen Trigger-Begriffe an Ort und Stelle in den Claim.

Gemessen mit tools/englisch_messung.py gegen einen Worktree von main:

    deutsche Phrasings, eigener Fakt exakt     2.542/2.603 -> 2.542/2.603
    englischer Korpus (624), verlorene Treffer zurueck    82,9 %
    Paraphrasen-Probe (100, anderer Wortlaut)             46,4 %
    neue dienst-fremde Treffer, 2.603 deutsche Phrasings  0
    neue dienst-fremde Treffer, 624 englische Claims      80
        davon trifft das deutsche Quell-Phrasing sie auch 67
        echt neu                                          13

Diese Suite pinnt die Mechanik und die Schranken, nicht die Prozente:
Sprach-Gate, Muss-Treffer, exakt vor schwach, Wortgrenzen in englischen
Restwoertern, Politik-Guard, Glossar-Hygiene, Determinismus — und als
Regressions-Gate eine Untergrenze fuer die Rueckgewinnung und eine
Obergrenze fuer die echt neuen Fremdtreffer.

Keine Netzabfrage, kein Modell.
"""

import functools
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))
sys.path.insert(0, str(BACKEND / "tools"))

from englisch_messung import fakt_id, population  # noqa: E402
from services import _englisch  # noqa: E402
from services._englisch import (  # noqa: E402
    GLOSSAR,
    _woerter,
    englisch_gate,
    englisch_match,
    englische_fassung,
    ist_englisch,
)
from services._schreibweise import normalisiere  # noqa: E402
from services._tippfehler import tippfehler_match  # noqa: E402
from services._topic_match import (  # noqa: E402
    _CORRUPTION_TOKENS,
    _PARTY_TOKENS,
    _SUPERLATIVE_TOKENS,
    find_matching_items,
    politik_guard_action,
    substring_or_composite_match,
)

DATA = BACKEND / "data"
KORPUS = json.loads((BACKEND / "tools" / "englisch_korpus.json").read_text(encoding="utf-8"))
PARA = json.loads((BACKEND / "tools" / "englisch_paraphrasen.json").read_text(encoding="utf-8"))
TRIGGER_FELDER = ("trigger_keywords", "trigger_composite", "trigger_all")

LIVE = "Is it true that the vacancy tax in Vorarlberg is 3000 euros a year?"


@functools.lru_cache(maxsize=1)
def _population():
    """[(Pfad, items_key, Dateiname, items)] — nur Fakten MIT Trigger-Feldern."""
    out = []
    for p, k, datei in population(str(BACKEND)):
        items = json.loads(Path(p).read_text(encoding="utf-8"))[k]
        out.append((p, k, datei, [it for it in items if isinstance(it, dict)
                                  and any(it.get(t) for t in TRIGGER_FELDER)]))
    return out


def _datei(datei):
    return next(x for x in _population() if x[2] == datei)


def _finde(datei, claim):
    p, k, _, _ = _datei(datei)
    return find_matching_items(p, k, claim_lc=claim.lower(), full_claim=claim,
                               descriptor_fn=None)


def _status_quo(items, claim):
    """Was find_matching_items auf main lieferte: exakt, sonst tolerant."""
    cl = claim.lower()
    m = [it for it in items if substring_or_composite_match(it, cl)]
    return m or [it for it in items if tippfehler_match(it, cl)]


def _fakt(datei, fid):
    return next(it for it in _datei(datei)[3] if fakt_id(it) == fid)


# --------------------------------------------------------------------------
# Der Live-Fall
# --------------------------------------------------------------------------

def test_live_claim_trifft_jetzt_die_leerstandsabgabe():
    fakt = _fakt("wohnen_pack.json", "leerstandsabgabe_wirkung_2026")
    assert not substring_or_composite_match(fakt, LIVE.lower())
    assert not tippfehler_match(fakt, LIVE.lower())
    treffer = _finde("wohnen_pack.json", LIVE)
    assert "leerstandsabgabe_wirkung_2026" in [fakt_id(t) for t in treffer]


def test_live_claim_ist_als_schwach_markiert():
    """Ein Glossar-Treffer darf kein 'strukturell falsch' behaupten."""
    treffer = _finde("wohnen_pack.json", LIVE)
    assert treffer
    assert all(t["data"]["_matched_exact"] is False for t in treffer)


def test_deutsches_pendant_bleibt_exakt():
    claim = "Stimmt es, dass die Leerstandsabgabe in Vorarlberg 3000 Euro im Jahr beträgt?"
    treffer = _finde("wohnen_pack.json", claim)
    assert any(fakt_id(t) == "leerstandsabgabe_wirkung_2026"
               and t["data"]["_matched_exact"] is True for t in treffer)


# --------------------------------------------------------------------------
# Das Sprach-Gate
# --------------------------------------------------------------------------

@pytest.mark.parametrize("claim", [
    "Österreich Stromimport",                    # kein Funktionswort
    "Hitler war Sozialist",                      # 'war' ist auch englisch
    "Impfungen verursachen Autismus",
    "Lesen im Dunkeln macht die Augen kaputt",   # 'die' ist auch englisch
    "AT-Arbeitslosigkeit ist 8 Prozent",
    "WHO empfiehlt max. 80 dB",                  # 'who' ist die WHO
    "Was bekommt Karl-Renner-Institut?",         # 'was' ist deutsch
])
def test_gate_bleibt_fuer_deutsche_claims_zu(claim):
    assert not englisch_gate(claim)
    assert englische_fassung(claim.lower()) is None


@pytest.mark.parametrize("claim", [
    LIVE,
    "Vaccines cause autism",                     # Stichwort-Gate: 2 Begriffe
    "Austrian unemployment rate 2024",
    "The minimum wage destroys jobs",
])
def test_gate_oeffnet_fuer_englische_claims(claim):
    assert englisch_gate(claim)


def test_detect_language_haette_als_gate_nicht_getaugt():
    """Der Grund fuer ein eigenes Signal, als Zahl gepinnt: das vorhandene
    services.ner._detect_language haelt die Mehrheit der deutschen Phrasings
    fuer Englisch. Kippt dieser Test, taugt es vielleicht doch — dann das
    Gate neu messen, nicht einfach austauschen."""
    from services.ner import _detect_language
    de = [ph for *_, items in _population() for it in items
          for ph in it.get("claim_phrasings_handled") or []]
    falsch = sum(_detect_language(ph) == "en" for ph in de)
    assert falsch / len(de) > 0.5
    offen = sum(englisch_gate(ph) for ph in de)
    assert offen / len(de) < 0.005


# Die deutschen Phrasings, fuer die das Gate offen ist. Sieben davon sind in
# den Fakten dokumentierte ENGLISCHE Phrasings; echter Fehlalarm ist nur der
# Kampagnen-Name. Ein neues Phrasing in dieser Liste heisst: ansehen.
GATE_OFFEN_BEKANNT = {
    "GDPR fines kill small businesses",
    "data retention is mandatory in Germany",
    "Real-name policy stops online hate",
    "Subscriber data requests are illegal in Germany",
    "Browser fingerprinting is not personal data",
    "Mozilla blocks all fingerprinting",
    "Abs are made in the kitchen",
    "End the Cage Age war erfolgreich",
}


def test_gate_offen_nur_fuer_bekannte_phrasings():
    offen = {ph for *_, items in _population() for it in items
             for ph in it.get("claim_phrasings_handled") or []
             if englisch_gate(ph)}
    assert offen <= GATE_OFFEN_BEKANNT, offen - GATE_OFFEN_BEKANNT


def test_gate_erkennt_den_englischen_korpus_weitgehend():
    claims = [c["en"] for c in KORPUS["claims"]]
    assert sum(englisch_gate(c) for c in claims) / len(claims) >= 0.90


# --------------------------------------------------------------------------
# Muss-Treffer: exakt bleibt exakt, exakt gewinnt
# --------------------------------------------------------------------------

def test_alle_exakten_phrasings_treffen_weiter_exakt():
    """Alle 2.542 Phrasings, die ihren Fakt exakt treffen, tun das auch nach
    find_matching_items — mit _matched_exact=True."""
    verloren = []
    geprueft = 0
    for p, k, datei, items in _population():
        for it in items:
            for ph in it.get("claim_phrasings_handled") or []:
                if not substring_or_composite_match(it, ph.lower()):
                    continue
                geprueft += 1
                treffer = find_matching_items(p, k, claim_lc=ph.lower(),
                                              full_claim=ph, descriptor_fn=None)
                if not any(fakt_id(t) == fakt_id(it) and t["data"]["_matched_exact"]
                           for t in treffer):
                    verloren.append((datei, fakt_id(it), ph))
    assert geprueft > 2500
    assert not verloren, verloren[:5]


def test_exakter_treffer_verdraengt_den_englischen_pass():
    """Trifft ein englischer Claim in einer Datei exakt, liefert diese Datei
    NUR exakte Treffer — der Glossar-Pass laeuft gar nicht erst."""
    for c in KORPUS["claims"]:
        for p, k, datei, items in _population():
            if not any(substring_or_composite_match(it, c["en"].lower()) for it in items):
                continue
            treffer = find_matching_items(p, k, claim_lc=c["en"].lower(),
                                          full_claim=c["en"], descriptor_fn=None)
            assert all(t["data"]["_matched_exact"] for t in treffer), (datei, c["en"])


# --------------------------------------------------------------------------
# Die Mechanik
# --------------------------------------------------------------------------

def test_ersetzung_an_ort_und_stelle_haelt_die_nachbarschaft():
    """Mehrwort-Trigger brauchen die Nachbarschaft: 'arbeitslosenquote wien'."""
    f = englische_fassung("unemployment rate in vienna 2024")
    assert "arbeitslosenquote wien 2024" in f
    assert "10 prozent" in englische_fassung("we only use 10 percent of our brain")


def test_extras_nur_fuer_dieselbe_wortspanne():
    """'destroys' hat zwei deutsche Entsprechungen, beide stehen im Text.
    'student grants' ist Studienbeihilfe — das Teilwort 'student' kommt nicht
    zusaetzlich dazu, ein deutscher Nutzer schriebe es auch nicht."""
    f = englische_fassung("the minimum wage destroys jobs")
    assert "zerstoert" in f and "vernichtet" in f
    f = englische_fassung("student grants in austria are too low")
    assert "studienbeihilfe" in f
    assert "studenten" not in f and "studierende" not in f


@pytest.mark.parametrize("claim,soll", [
    ("3,000 euros", ["3.000", "euros"]),
    ("0.5 per mille", ["0,5", "per", "mille"]),
    ("15,000 drone strikes", ["15.000", "drone", "strikes"]),
    ("1.4 mmol", ["1,4", "mmol"]),
    ("100,000 dollars", ["100.000", "dollars"]),
])
def test_englische_zahlen_werden_deutsch_geschrieben(claim, soll):
    assert _woerter(claim) == soll


def test_trigger_trifft_englisches_restwort_nur_als_ganzes_wort():
    """'ass' (Acetylsalicylsaeure) steckt in 'glass' — im deutschen Claim
    'Glas' nie. In englischen Restwoertern gilt deshalb die Wortgrenze."""
    ass = {"trigger_keywords": ["ass"]}
    assert not englisch_match(ass, "a glass of milk every day is healthy")
    assert englisch_match(ass, "taking ass every day is healthy")
    ai = {"trigger_composite": [["ai"], ["vernichtet"]]}
    assert not englisch_match(ai, "screen time destroys the brains of teenagers")
    assert englisch_match(ai, "ai destroys millions of jobs")


def test_in_den_deutschen_glossen_bleibt_es_ein_substring():
    """Wie im Deutschen: 'mangel' steckt in 'fachkraeftemangel'."""
    assert englisch_match({"trigger_keywords": ["mangel"]},
                          "there is a skilled labour shortage in austria")


def test_aspirin_fall_bleibt_draussen_der_echte_bleibt_drin():
    wein = _finde("gesundheits_autoritaeten_pack.json",
                  "A glass of red wine a day is good for your heart")
    assert "aspirin_primaerpraevention_konsens_2026" not in [fakt_id(t) for t in wein]
    aspirin = _finde("gesundheits_autoritaeten_pack.json",
                     "Taking aspirin daily protects against heart attacks")
    assert "aspirin_primaerpraevention_konsens_2026" in [fakt_id(t) for t in aspirin]


def test_ergebnis_haengt_nicht_am_hash_seed():
    """Welcher Begriff an Ort und Stelle steht, darf nicht von der
    set-Reihenfolge abhaengen (PYTHONHASHSEED)."""
    code = ("import sys; sys.path.insert(0, %r)\n"
            "from services._englisch import englische_fassung as f\n"
            "for c in %r: print(f(c))\n") % (str(BACKEND), [
                LIVE.lower(), "pensions will soon collapse",
                "vaccines cause autism", "the minimum wage destroys jobs"])
    ausgaben = set()
    for seed in ("1", "2", "3"):
        env = dict(os.environ, PYTHONHASHSEED=seed)
        ausgaben.add(subprocess.run([sys.executable, "-c", code], env=env,
                                    capture_output=True, text=True, check=True).stdout)
    assert len(ausgaben) == 1


# --------------------------------------------------------------------------
# Politik-Guard
# --------------------------------------------------------------------------

@pytest.mark.parametrize("claim,soll", [
    ("The FPÖ is the most corrupt party in Austria", "block_country_sources"),
    ("The ÖVP has the highest corruption of all parties", "block_country_sources"),
    ("The SPÖ is the worst party when it comes to corruption", "block_country_sources"),
    ("Kickl is the most corrupt politician", "block_country_sources"),
    ("The Greens have the biggest corruption problem", "block_country_sources"),
    # Wie im Deutschen: konkreter Anker oder keine Partei -> kein Block
    ("The FPÖ was involved in the Ibiza corruption scandal", "pass"),
    ("Austria is a corrupt country", "pass"),
    ("Every party in Austria is corrupt", "pass"),
    ("Die FPÖ ist die korrupteste Partei Österreichs", "block_country_sources"),
])
def test_guard_sieht_die_englische_fassung(claim, soll):
    assert politik_guard_action(claim.lower()) == soll


def test_englischer_parteisuperlativ_erreicht_keine_laenderquelle():
    """Auf main feuerte transparency (CPI) auf den englischen Satz, der
    deutsche wurde blockiert. Genau der Kategorienfehler, den der Guard
    verhindern soll."""
    from services.transparency import _claim_mentions_cpi
    assert not _claim_mentions_cpi("The FPÖ is the most corrupt party in Austria")
    assert _claim_mentions_cpi("Austria is a corrupt country")


def test_guard_greift_auch_im_englischen_pass_zentral():
    """Der Glossar-Pass allein wuerde den CPI-Fakt treffen; find_matching_items
    haelt ihn an, auch fuer Packs, die den Guard selbst nicht rufen."""
    claim = "The FPÖ has the highest corruption of all parties"
    fakt = _fakt("demokratie_pack.json", "korruption_index_2026")
    assert englisch_match(fakt, claim.lower())
    assert _finde("demokratie_pack.json", claim) == []


# --------------------------------------------------------------------------
# Glossar-Hygiene
# --------------------------------------------------------------------------

VERBOTEN = {"gift", "handy", "also", "fast", "kind", "art", "rat", "bad",
            "brief", "true", "really", "the", "is", "a", "an", "in", "at",
            "was", "will", "so"}


def test_keine_falschen_freunde_und_kein_true():
    for de, varianten in GLOSSAR:
        for v in varianten:
            assert normalisiere(v).strip() not in VERBOTEN, (de, v)


def test_wortanfang_varianten_sind_einwortig_und_lang_genug():
    for de, varianten in GLOSSAR:
        for v in varianten:
            if v.endswith("*"):
                assert " " not in v and len(v) - 1 >= 5, (de, v)


def test_jeder_deutsche_begriff_steht_genau_einmal():
    schluessel = [normalisiere(de).strip() for de, _ in GLOSSAR]
    doppelt = {s for s in schluessel if schluessel.count(s) > 1}
    assert not doppelt, doppelt


def test_jede_glosse_kann_einen_trigger_treffen():
    """Die deutsche Seite muss einen Trigger (ab 3 Zeichen) enthalten — oder
    fuer den Politik-Guard da sein. Sonst ist der Eintrag tot."""
    tokens = set()
    for *_, items in _population():
        for it in items:
            for t in it.get("trigger_keywords") or []:
                tokens.add(normalisiere(t).strip())
            for gruppen in [it.get("trigger_composite") or []] + list(it.get("trigger_all") or []):
                for g in gruppen:
                    if isinstance(g, (list, tuple)):
                        tokens |= {normalisiere(x).strip() for x in g if isinstance(x, str)}
    tokens = {t for t in tokens if len(t) >= 3}
    guard = {t.strip() for t in _PARTY_TOKENS + _CORRUPTION_TOKENS + _SUPERLATIVE_TOKENS}
    tot = [de for de, _ in GLOSSAR
           if normalisiere(de).strip() not in guard
           and not any(t in normalisiere(de) for t in tokens)]
    assert not tot, tot


def test_modul_liefert_fuer_deutsche_claims_nichts():
    """Der Pass ist fuer deutsche Claims unsichtbar — auch fuer die kurzen,
    die _detect_language fuer Englisch haelt."""
    assert englische_fassung("wohnungs-mangel ist lüge") is None
    assert englische_fassung("") is None
    assert not ist_englisch("Die Mindestsicherung in Österreich beträgt rund 1000 Euro")


# --------------------------------------------------------------------------
# Regressions-Gate auf den Mess-Korpora
# --------------------------------------------------------------------------

def _rueckgewinnung(claims):
    verloren = zurueck = 0
    for c in claims:
        _, _, _, items = _datei(c["datei"])
        vorher = any(fakt_id(t) == c["id"] for t in _status_quo(items, c["en"]))
        if vorher:
            continue
        verloren += 1
        zurueck += any(fakt_id(t) == c["id"] for t in _finde(c["datei"], c["en"]))
    return zurueck, verloren


@pytest.mark.parametrize("split,mind", [("dev", 0.80), ("test", 0.80)])
def test_rueckgewinnung_hauptkorpus(split, mind):
    """Obergrenze-Messung (Korpus und Glossar aus einer Hand), gemessen 82,3 /
    83,6 %. Faellt sie unter 80 %, hat eine Aenderung Glossar oder Gate
    beschaedigt."""
    zurueck, verloren = _rueckgewinnung(
        [c for c in KORPUS["claims"] if c["split"] == split])
    assert verloren > 200
    assert zurueck / verloren >= mind, (zurueck, verloren)


def test_rueckgewinnung_paraphrasen_probe():
    """Die ehrlichere Zahl (anderer deutscher Wortlaut, nach dem Einfrieren
    des Glossars geschrieben): gemessen 46,4 %."""
    zurueck, verloren = _rueckgewinnung(PARA["claims"])
    assert zurueck / verloren >= 0.44, (zurueck, verloren)


def _neue_fremdtreffer(claim, eigene_datei):
    """Dienst-fremde Fakten, die NUR der englische Pass liefert.

    Schnell statt 61 x find_matching_items je Claim: Der englische Pass kann
    in einer Datei nur etwas beitragen, wenn englisch_match dort anschlaegt —
    erst dann wird geprueft, ob exakt oder tolerant ihm zuvorkommen. Fuer
    jede Datei mit Beitrag wird gegen find_matching_items gegengeprueft, damit
    die Abkuerzung nicht vom echten Matcher weglaeuft."""
    cl = claim.lower()
    if englische_fassung(cl) is None or politik_guard_action(cl) != "pass":
        return set()
    neu = set()
    for p, k, datei, items in _population():
        if datei == eigene_datei:
            continue
        en = [it for it in items if englisch_match(it, cl)]
        if not en or _status_quo(items, claim):
            continue
        echt = find_matching_items(p, k, claim_lc=cl, full_claim=claim,
                                   descriptor_fn=None)
        assert {fakt_id(t) for t in echt} == {fakt_id(t) for t in en}, (datei, claim)
        neu |= {(datei, fakt_id(t)) for t in en}
    return neu


def _deutsch_trifft(de_claim, datei, fid):
    _, _, _, items = _datei(datei)
    return fid in {fakt_id(t) for t in _status_quo(items, de_claim)}


def test_echt_neue_fremdtreffer_bleiben_begrenzt():
    """Ueber-Trigger-Sweep als Gate. Ein neuer dienst-fremder Treffer eines
    englischen Claims zaehlt nur, wenn das deutsche Quell-Phrasing denselben
    fremden Fakt NICHT auch trifft (sonst ist es derselbe Querbezug zweier
    Packs). Gemessen: 80 neu, 67 davon deckungsgleich, 13 echt neu — 11
    davon thematisch plausibel."""
    alle, echt_neu = [], []
    for c in KORPUS["claims"]:
        for datei, fid in sorted(_neue_fremdtreffer(c["en"], c["datei"])):
            alle.append((c["en"], datei, fid))
            if not _deutsch_trifft(c["de"], datei, fid):
                echt_neu.append((c["en"], datei, fid))
    assert len(alle) <= 80, len(alle)
    assert len(echt_neu) <= 13, echt_neu


def test_sweep_ist_nicht_blind():
    """Gegenprobe des Instruments: ein vergifteter Glossar-Eintrag ('the' ->
    'oesterreich', 'is' -> 'gefaehrlich') MUSS neue Fremdtreffer erzeugen."""
    alt_glossar = _englisch.GLOSSAR
    try:
        _englisch.GLOSSAR = alt_glossar + (("österreich", ("the",)),
                                           ("gefährlich", ("is",)))
        _englisch._index.cache_clear()
        _englisch._fassung.cache_clear()
        n = sum(len(_neue_fremdtreffer(c["en"], c["datei"]))
                for c in KORPUS["claims"][:200])
    finally:
        _englisch.GLOSSAR = alt_glossar
        _englisch._index.cache_clear()
        _englisch._fassung.cache_clear()
    sauber = sum(len(_neue_fremdtreffer(c["en"], c["datei"]))
                 for c in KORPUS["claims"][:200])
    assert n > sauber + 10, (n, sauber)


def test_themenfremde_englische_claims_ziehen_kaum_etwas():
    neu_gesamt = [(claim, z) for claim in KORPUS["themenfremd"]
                  for z in sorted(_neue_fremdtreffer(claim, None))]
    # Einziger Fall: "highest mountain" -> Everest-Fakt (thematisch nah)
    assert len(neu_gesamt) <= 1, neu_gesamt


# --------------------------------------------------------------------------
# Was im Prompt ankommt (tools/prompt_zensus.py)
# --------------------------------------------------------------------------
#
# Retrieval allein reicht nicht: Die claim-zentrierte Kuerzung rankt die
# deutschen Fakt-Saetze nach Claim-Termen. Ein englischer Claim teilt mit
# ihnen kaum ein Wort — vor der Korrektur kamen bei "How high is the vacancy
# tax in Tyrol?" 226 von 6.235 Zeichen an, der Tiroler Betrag fehlte. Die
# deutschen Glossen fliessen jetzt in die Claim-Terme ein
# (synthesizer._prompt_claim_terms). Die deutsche 30er-Batterie bleibt
# unveraendert 30/30 und wird in test_prompt_zensus.py gepinnt.

@pytest.mark.parametrize("claim,datei,fakt,muss", [
    (LIVE, "wohnen_pack.json", "leerstandsabgabe_wirkung_2026", ["8,20 bis 18,50"]),
    ("How high is the vacancy tax in Tyrol?", "wohnen_pack.json",
     "leerstandsabgabe_wirkung_2026", ["10 bis 215"]),
    ("Does Styria have a vacancy tax?", "wohnen_pack.json",
     "leerstandsabgabe_wirkung_2026", ["1.10.2022"]),
    ("How many women were murdered in Austria in 2024?", "gleichstellung_pack.json",
     "femizide_at_de_2026", [["40 vollendete Morde", "40 auf vollendete Morde"]]),
    ("How much CO2 does a 130 km/h speed limit on motorways save?",
     "mobilitaet_pack.json", "mobilitaet-tempolimit_130_2026",
     [["1,9 Mio. t", "1,9 Millionen"]]),
    ("Our food is full of pesticides", "landwirtschaft_pack.json",
     "pestizid_rueckstaende_2026", [["96,7 %", "58,4 %"]]),
])
def test_englischer_claim_bringt_die_deutsche_zahl_in_den_prompt(claim, datei, fakt, muss):
    import prompt_zensus
    r = prompt_zensus.pruefe({"claim": claim, "datei": datei, "fakt": fakt, "muss": muss})
    assert not r["fehlt"], (claim, r["fehlt"], r["an"])


def test_claim_terme_deutscher_claims_bleiben_unveraendert():
    from services.synthesizer import _prompt_claim_terms
    assert _prompt_claim_terms({}, "Wie hoch ist die Leerstandsabgabe in Tirol?") == [
        "hoch", "leerstandsabgabe", "tirol"]
    terme = _prompt_claim_terms({}, "How high is the vacancy tax in Tyrol?")
    assert "leerstandsabgabe" in terme and "tirol" in terme
