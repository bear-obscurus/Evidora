"""Der tolerante Pass haelt keine echten Woerter mehr fuer Tippfehler.

Gefunden bei #207: „Kuenstliche Intelligenz fuehrt zu Massenarbeitslosigkeit"
(arbeitsmarkt_pack, ki_job_verdraengung_2026) traf tolerant den Fakt
``ki_bewusstsein_2026`` — „fuehrt" liegt Abstand 1 neben dem Token „fuehlt",
bei gleichem Anlaut. #205 misst Schreibweisen-Naehe, nicht Tippfehler.

Gemessen am 2026-09-26 mit der echten ``find_matching_items``
(``descriptor_fn=None``) ueber die 2.603 dokumentierten
``claim_phrasings_handled`` von Fakten MIT Trigger-Feldern plus 1.163
Stress-Claims: der tolerante Pass lieferte 20 Treffer, keinen im eigenen Pack,
und in ALLEN 20 war das tragende Claim-Wort ein echtes Wort.

    anderes Wort      fuehrt, aktion, schweiz (2x), dieser           5  -> weg
    Form des Tokens   schadet, vernichtet, entgiftet, normal, ...   15  -> bleiben

Rueckgewinnung (#205-Sonde): 2.884 -> 2.884. Erschoepfend ueber jede Position
in jedem Wort >= 6 (160.223 Tippfehler-Claims): -19 von 110.759 — Tippfehler,
die selbst ein echtes Wort ergeben („Welpen" -> „Wellen").

Diese Suite pinnt beide Richtungen am echten Pipeline-Pfad, die Regel als
Praedikat, das Vokabular als Contract und den Korpus-Sweep als Gate — mit
Gift-Probe: ohne Vokabular MUSS der Sweep die fuenf alten Lecks melden.

Dependency-light: reine Trigger-Tests, kein Netz/LLM.
"""
import glob
import json
import os

import pytest

import services._tippfehler as TF
from services._schreibweise import normalisiere
from services._tippfehler import (
    _fugen_s,
    _kandidaten,
    _prosa,
    _t_z_vor_i,
    abstand_hoechstens_eins,
    echtwoerter,
    ist_anderes_wort,
    ist_formvariante,
    tippfehler_match,
    token_ist_schreibnah,
)
from services._topic_match import find_matching_items, substring_or_composite_match

BACKEND = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(BACKEND, "data")
TRIGGER_FELDER = ("trigger_keywords", "trigger_composite", "trigger_all")


def _treffer(datei: str, claim: str) -> list[dict]:
    return find_matching_items(os.path.join(DATA, datei), "facts",
                               claim_lc=claim.lower(), full_claim=claim,
                               descriptor_fn=None)


def _fid(it: dict) -> str:
    return it.get("id") or it.get("topic") or ""


# ---------------------------------------------------------------------------
# 1. Beide Richtungen am echten Pipeline-Pfad
# ---------------------------------------------------------------------------
# Jede Zeile traf auf main (#205) tolerant — gemessen, nicht konstruiert.

LECKS = [
    ("tech_ki_pack.json", "ki_bewusstsein_2026",
     "Kuenstliche Intelligenz fuehrt zu Massenarbeitslosigkeit"),      # fuehlt
    ("finanzen_pack.json", "aktien_gluecksspiel_2026",
     "EU-Bürger wollen mehr Klima-Aktion von der EU."),                # aktien
    ("landwirtschaft_pack.json", "viehbestand_at_2026",
     "Österreich hat eine höhere Mordrate als die Schweiz."),          # schwein
    ("landwirtschaft_pack.json", "viehbestand_at_2026",
     "Die Schweiz investiert weniger pro Kopf in die Schiene als Deutschland."),
    ("landwirtschaft_pack.json", "regionale_versorgung_mythen_2026",
     "Mit dieser Methode aktivieren Sie 100 Prozent des Gehirns"),     # diesel
]

BLEIBT_TOLERANT = [
    # echte Tippfehler — der Zweck von #205
    ("wohnen_pack.json", "leerstandsabgabe_wirkung_2026",
     "Wie hoc ist die Leertsandsabgabe in Tirol?"),
    ("tech_ki_pack.json", "ki_bewusstsein_2026", "ChatGPT fuelht sich traurig"),
    ("tech_ki_pack.json", "ki_bewusstsein_2026", "Hat ChatGPT Gefuehel?"),
    # Formen des Tokens: echte Woerter, aber dasselbe Wort
    ("wirtschaftspolitik_pack.json", "mindestlohn_beschaeftigung_2026",
     "12 Euro Mindestlohn vernichtet Stellen"),                        # Endung
    ("substanzen_pack.json", "entgiftungs_diaeten_drogen_mythos",
     "Sauna entgiftet den Körper"),
    ("finanzen_pack.json", "aktien_gluecksspiel_2026",
     "Mit Aktien 20 Prozent jährliche Rendite ist normal"),
    ("eter.json", "eter_overview_at_2021", "Tertiär-Quote Österreich"),
    ("oecd_health.json", "oecd_spitalsbetten_2024",
     "In Österreich fehlen bis 2030 rund 100.000 Pflegekräfte."),      # Partizip
    ("who_hearing.json", "phl_jugend_risiko_2026",
     "Laute Musik über Kopfhörer verursacht bleibende Schäden"),       # Umlaut
    ("wirtschaftspolitik_pack.json", "vermoegenssteuer_kapitalflucht_2026",
     "Die Vermögensteuer vertreibt die Reichen aus dem Land"),         # Fugen-s
]


@pytest.mark.parametrize("datei,fid,claim", LECKS)
def test_anderes_echtes_wort_traegt_nicht(datei, fid, claim):
    assert fid not in {_fid(t) for t in _treffer(datei, claim)}, (
        f"{fid} trifft {claim!r} ueber ein anderes echtes Wort")


@pytest.mark.parametrize("datei,fid,claim", LECKS)
def test_lecks_trafen_ohne_vokabular(monkeypatch, datei, fid, claim):
    """Gift-Probe je Leck: ohne Vokabular (== #205) trifft es wieder. Sonst
    waere der Test oben gruen, ohne dass die Regel etwas tut."""
    monkeypatch.setattr(TF, "echtwoerter", lambda: frozenset())
    assert fid in {_fid(t) for t in _treffer(datei, claim)}


@pytest.mark.parametrize("datei,fid,claim", BLEIBT_TOLERANT)
def test_tippfehler_und_formen_tragen_weiter(datei, fid, claim):
    treffer = {_fid(t): t["data"]["_matched_exact"] for t in _treffer(datei, claim)}
    assert fid in treffer, f"{fid} verliert {claim!r}"
    assert treffer[fid] is False, "sollte tolerant tragen — sonst misst der Fall nichts"


def test_marijuana_ist_eine_schreibung_kein_tippfehler():
    """„Marijuana" ist ein anderes echtes Wort als der Token „marihuana" —
    und zugleich eine uebliche deutsche Schreibung. Bisher trug es nur der
    tolerante Pass; jetzt steht es als Token neben „marihuana" (wie schon in
    gesundheits_autoritaeten_pack) und trifft exakt."""
    treffer = {_fid(t): t["data"]["_matched_exact"]
               for t in _treffer("substanzen_pack.json",
                                 "Marijuana schädigt das Gehirn von Jugendlichen")}
    assert treffer.get("cannabis_jugend_hirnschaden_mythos") is True
    assert treffer.get("cannabis_legalisierung_konsum_mythos") is True


# ---------------------------------------------------------------------------
# 2. Die Regel: anderes Wort vs. Form vs. Tippfehler
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("tok,wort", [
    ("fuehlt", "fuehrt"),
    ("aktien", "aktion"),
    ("schwein", "schweiz"),
    ("diesel", "dieser"),
    ("verkehr", "verzehr"),
    ("bewerbung", "bewertung"),
    ("moderna", "moderne"),
    ("uebertrieben", "uebertreiben"),   # ie/ei bewusst nicht: partei/partie
])
def test_anderes_wort(tok, wort):
    assert abstand_hoechstens_eins(tok, wort)
    assert not ist_formvariante(tok, wort)
    assert ist_anderes_wort(tok, wort)
    assert not token_ist_schreibnah(tok, [wort])


@pytest.mark.parametrize("tok,wort", [
    ("schaden", "schadet"),             # Flexionsendung
    ("vernichten", "vernichtet"),
    ("normale", "normal"),
    ("tertiaere", "tertiaer"),
    ("bezieher", "beziehen"),
    ("fehlend", "fehlen"),              # Partizip
    ("ablehnen", "ablehnten"),          # Praeteritum
    ("schaden", "schaeden"),            # Umlaut-Umschrift
    ("auslaender", "auslander"),
    ("vermoegenssteuer", "vermoegensteuer"),   # Fugen-s
    ("potenzial", "potential"),         # t/z vor i
    ("essentiell", "essenziell"),
])
def test_form_desselben_worts(tok, wort):
    assert abstand_hoechstens_eins(tok, wort)
    assert ist_formvariante(tok, wort)
    assert not ist_anderes_wort(tok, wort)
    assert token_ist_schreibnah(tok, [wort])


@pytest.mark.parametrize("tok,wort", [
    ("leerstandsabgabe", "leertsandsabgabe"),
    ("fuehlt", "fuelht"),
    ("strommix", "stromix"),
    ("karotten", "karoten"),
])
def test_tippfehler_bleibt_tippfehler(tok, wort):
    assert wort not in echtwoerter()
    assert token_ist_schreibnah(tok, [wort])


@pytest.mark.parametrize("a,b", [
    ("bremen", "bremsen"),              # s nach 4 Zeichen: keine Fuge
    ("transparenz", "transparent"),     # t/z am Ende, nicht vor i
    ("insolvenz", "insolvent"),
])
def test_schreibvarianten_bleiben_eng(a, b):
    assert not _fugen_s(a, b)
    assert not _t_z_vor_i(a, b)
    assert not ist_formvariante(a, b)


# ---------------------------------------------------------------------------
# 3. Das Vokabular
# ---------------------------------------------------------------------------

def test_vokabular_kennt_die_alltagswoerter():
    v = echtwoerter()
    assert len(v) > 10_000, "Vokabular leer — die Regel liefe ins Leere"
    for w in ("fuehrt", "aktion", "schweiz", "dieser", "vermoegensteuer"):
        assert w in v, w


def test_vokabular_nur_aus_trigger_packs():
    """euvsdisinfo_db.json ist zwei Drittel aller Woerter in data/ —
    englisch und abgeschnitten. Es hat keine Trigger-Fakten und zaehlt
    nicht."""
    v = echtwoerter()
    for w in ("manipulati", "ukrainie", "coronovirus"):
        assert w not in v, w


def test_prosa_ohne_trigger_phrasings_und_urls():
    fakt = {"trigger_keywords": ["fuehlt"], "claim_phrasings_handled": ["fuehrt"],
            "trigger_composite": [["aktien"]], "headline": "Kopf",
            "context_notes": ["Notiz"], "source_url": "https://example.org/x"}
    assert sorted(_prosa(fakt)) == ["Kopf", "Notiz"]


def test_einzelstuecke_zaehlen_nicht():
    """Schwelle 2: ein Tippfehler in der Prosa soll kein echtes Wort werden.
    „Gepant", „Karoten" stehen je einmal in den Packs."""
    for w in ("gepant", "karoten"):
        assert w not in echtwoerter(), w


def test_strommix_in_der_prosa_richtig_geschrieben():
    """„Stromix" stand dreimal im energie_klima_pack — und waere damit ein
    echtes Wort, der Tippfehler „Stromix" also nie mehr zu „strommix"
    zurueckgeholt worden. Die Prosa ist korrigiert; dieser Test haelt es."""
    assert "stromix" not in echtwoerter()


def test_vokabular_nur_bei_bedarf(monkeypatch):
    """Formvarianten entscheiden ohne Vokabular — im Normalfall wird es nie
    gebaut. Erst ein schreibweisen-nahes Wort, das keine Form ist, braucht es."""
    gebaut = []
    monkeypatch.setattr(TF, "echtwoerter",
                        lambda: gebaut.append(1) or frozenset())
    assert token_ist_schreibnah("schaden", ["schadet"])
    assert not gebaut
    token_ist_schreibnah("fuehlt", ["fuehrt"])
    assert gebaut


# ---------------------------------------------------------------------------
# 4. Korpus-Sweep als Gate — mit Gift-Probe
# ---------------------------------------------------------------------------

def _gruppen() -> list[tuple[str, list[dict]]]:
    out = []
    for p in sorted(glob.glob(os.path.join(DATA, "*.json"))):
        with open(p, encoding="utf-8") as fh:
            d = json.load(fh)
        if not isinstance(d, dict):
            continue
        for v in d.values():
            if isinstance(v, list):
                items = [it for it in v if isinstance(it, dict)
                         and any(it.get(f) for f in TRIGGER_FELDER)]
                if items:
                    out.append((os.path.basename(p), items))
    return out


def _claims() -> list[str]:
    out = []
    for _d, items in GRUPPEN:
        for it in items:
            out.extend(ph for ph in it.get("claim_phrasings_handled") or []
                       if isinstance(ph, str))
    for p in sorted(glob.glob(os.path.join(BACKEND, "tools", "stress_tests", "*.json"))):
        with open(p, encoding="utf-8") as fh:
            d = json.load(fh)
        for c in (d.get("claims") if isinstance(d, dict) else d) or []:
            if isinstance(c, dict) and isinstance(c.get("claim"), str):
                out.append(c["claim"])
    with open(os.path.join(BACKEND, "tools", "stress_test_100_claims.json"),
              encoding="utf-8") as fh:
        out.extend(c["claim"] for c in json.load(fh))
    return out


def _einwort_tokens(it: dict):
    toks = list(it.get("trigger_keywords") or [])
    for grp in it.get("trigger_composite") or []:
        toks.extend(grp)
    for regel in it.get("trigger_all") or []:
        for grp in regel:
            toks.extend(grp)
    for t in toks:
        if isinstance(t, str):
            n = normalisiere(t)
            if len(n) >= TF.MIND_LAENGE and " " not in n and "-" not in n:
                yield n


GRUPPEN = _gruppen()
# Vorfilter: Praefix -> (Token, Gruppe). Ein toleranter Treffer braucht ein
# Claim-Wort neben einem Einwort-Token; nur diese Gruppen laufen durch den
# echten exakten + toleranten Pass. So bleibt der Sweep bei Sekunden.
_INDEX: dict[str, set] = {}
for _g, (_d, _items) in enumerate(GRUPPEN):
    for _it in _items:
        for _t in _einwort_tokens(_it):
            _INDEX.setdefault(_t[:TF.PRAEFIX_LAENGE], set()).add((_t, _g))


def _tolerante_treffer() -> set[tuple[str, str]]:
    """(Fakt, Claim) aller toleranten Treffer im Korpus — wie
    find_matching_items: der tolerante Pass nur bei leerem exaktem Pass."""
    out = set()
    for claim in CLAIMS:
        lc = claim.lower()
        woerter = _kandidaten(normalisiere(lc))
        gruppen = {g for w in woerter for t, g in _INDEX.get(w[:TF.PRAEFIX_LAENGE], ())
                   if t != w and abstand_hoechstens_eins(t, w)}
        for g in gruppen:
            items = GRUPPEN[g][1]
            if any(substring_or_composite_match(it, lc) for it in items):
                continue
            out.update((_fid(it), claim) for it in items if tippfehler_match(it, lc))
    return out


CLAIMS = _claims()

# Was nach dem Fix tolerant bleibt: jeder Treffer von einer FORM des Tokens
# getragen. Am Thema oder so gut wie ein exakter Treffer auf die Grundform
# („Hund … Schaden" traefe hund_haftung genauso) — das ist Trigger-Semantik,
# keine Tippfehler-Toleranz. Kommt etwas dazu: ansehen. Form -> hier
# eintragen; anderes Wort -> Vokabular/Regel pruefen.
ERLAUBT = {
    "mindestlohn_beschaeftigung_2026": {
        "Mindestlohn schadet kleinen Betrieben",              # schaden
        "12 Euro Mindestlohn vernichtet Stellen",             # vernichten
    },
    "bauern_sterben_empirie_2026": {
        "Mindestlohn schadet kleinen Betrieben",              # kleiner
    },
    "phl_jugend_risiko_2026": {                               # schaden
        "Eine Stunde Smartphone schadet der Hirnentwicklung",
        "Eine Stunde Smartphone schadet der Hirnentwicklung.",
        "Es ist wissenschaftlich erwiesen dass Smartphone vor 13 schadet",
        "Es ist wissenschaftlich erwiesen dass Smartphone vor 13 schadet.",
    },
    "entgiftungs_diaeten_drogen_mythos": {"Sauna entgiftet den Körper"},
    "aktien_gluecksspiel_2026": {                             # normale
        "Mit Aktien 20 Prozent jährliche Rendite ist normal",
        "Mit Aktien 20 Prozent jährliche Rendite ist normal.",
    },
    "eter_overview_at_2021": {"Tertiär-Quote Österreich"},    # tertiaere
    "hartz_iv_faulenzer_2026": {
        "Drittstaatsangehörige beziehen häufiger Sozialhilfe",  # bezieher
    },
    "hund_haftung_2026": {"Vegane Hund-Ernährung schadet immer dem Hund"},
    "oecd_spitalsbetten_2024": {
        "In Österreich fehlen bis 2030 rund 100.000 Pflegekräfte.",  # fehlend
    },
    "bfr_acrylamid_2026": {                                   # verbrennen
        "Elektroautos sind über ihren Lebenszyklus klimaschädlicher als "
        "Verbrenner.",
    },
}


def _unerlaubt(treffer: set[tuple[str, str]]) -> list[tuple[str, str]]:
    return sorted((f, c) for f, c in treffer if c not in ERLAUBT.get(f, set()))


def test_sweep_korpus_ist_vollstaendig():
    """Sonst misst der Sweep ins Leere — gemessen: 2.603 + 1.163 Claims."""
    assert len(CLAIMS) >= 3700


def test_sweep_keine_unbekannten_toleranten_treffer():
    neu = _unerlaubt(_tolerante_treffer())
    assert not neu, f"neue tolerante Treffer im Korpus: {neu}"


def test_sweep_schlaegt_an(monkeypatch):
    """Gift-Probe: ohne Vokabular meldet der Sweep genau die fuenf Lecks —
    sonst ist er blind, und seine Null oben beweist nichts."""
    monkeypatch.setattr(TF, "echtwoerter", lambda: frozenset())
    assert set(_unerlaubt(_tolerante_treffer())) == {(f, c) for _d, f, c in LECKS}
