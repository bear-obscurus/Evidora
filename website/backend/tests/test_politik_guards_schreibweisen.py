"""Die beiden Politik-Guards trugen dieselbe Schreibweisen-Lücke.

Gefunden beim Bauen von #150: `freedom_house` reicht den Claim an den
Politik-Tabu-Guard weiter, und dessen Token-Liste führt Personennamen mit
Bindestrich — `meinl-reisinger`, `rendi-wagner`, `van der bellen`,
`die grünen`. **4 von 7 Mehrwort-Tokens rutschen in einer üblichen
Schreibweise durch:**

    block_country_sources | Meinl-Reisinger ist die korrupteste Politikerin
    pass                  | Meinl Reisinger ist die korrupteste Politikerin

Und dann feuern `vdem`, `wgi` und `demokratie_pack` — Länder-Quellen auf eine
Partei-Korruptions-Aussage, genau der Kategorienfehler, gegen den der Guard am
2026-05-17 gebaut wurde. Es braucht dafür keine Normalisierung irgendwo: es
reicht, dass jemand den Namen ohne Bindestrich tippt.

Der zweite Guard hatte dieselbe Lücke, schlimmer: der Wahlprognose-Guard in
`verdict_postprocess.py` (Politik-Tabu #2) trug Umlaute **in den
Regex-Mustern** — `dürfte`, `könnte`, `stärkste`, `nächste` — und lief gegen
ein blosses `.lower()`. **4 von 5 realistischen Prognose-Claims** in
ASCII-Umschrift passierten ihn ungehindert. Dieser Guard hat die letzte
Autorität über das Verdict.

Ehrlich zur Schwere: live liess sich daraus **kein falsches Verdict** zeigen.
Der Prognose-Guard überschreibt nur, wenn das Modell nicht ohnehin schon
`unverifiable` sagt — und in den geprüften Fällen sagte es das von selbst. Das
Loch ist ein Riss im Sicherheitsnetz, nicht ein laufender Schaden. Ein Netz
prüft man aber, bevor jemand hineinfällt.

Fix in beiden Fällen: die Token-Listen durch `norm_terme`, die Muster in
gefalteter Schreibweise, und der Guard normalisiert den Claim **selbst**,
statt sich auf 16 Aufrufstellen zu verlassen. Bewusst NICHT die Varianten
aufzählen — dieselbe Falle wie die Frontex-Flexionsformen (#141).

Richtung des Ausfalls, je Liste verschieden, beide falsch:

    PARTY / CORRUPTION / SUPERLATIVE  verfehlt -> „pass"  (Loch)
    SPECIFIC_ANCHOR / AFFAIR_PERSON   verfehlt -> „block" (Über-Block)
"""

import re
import sys
import textwrap
import warnings
from pathlib import Path

import pytest

warnings.filterwarnings("ignore")
BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))

from services._schreibweise import norm_terme, normalisiere  # noqa: E402
from services._topic_match import (  # noqa: E402
    _AFFAIR_PERSON_TOKENS,
    _CORRUPTION_TOKENS,
    _PARTY_TOKENS,
    _SPECIFIC_ANCHOR_TOKENS,
    _SUPERLATIVE_TOKENS,
    politik_guard_action,
)

UMLAUT = str.maketrans({"ä": "ae", "ö": "oe", "ü": "ue", "ß": "ss"})


# --------------------------------------------------------------------------
# Guard 1 — Partei-Korruptions-Superlativ
# --------------------------------------------------------------------------

MUSS_BLOCKEN = (
    "Die FPÖ ist die korrupteste Partei Österreichs",
    "Die FPOE ist die korrupteste Partei Oesterreichs",
    "Meinl-Reisinger ist die korrupteste Politikerin",
    "Meinl Reisinger ist die korrupteste Politikerin",
    "Rendi-Wagner ist am korruptesten",
    "Rendi Wagner ist am korruptesten",
    "Van der Bellen hat die höchste Korruption zugelassen",
    "Van-der-Bellen hat die hoechste Korruption zugelassen",
    "Die ÖVP hat die größte Korruption",
    "Die OEVP hat die groesste Korruption",
    "Die Grünen sind die schlimmste Partei bei Geldwäsche",
    "Die Gruenen sind die schlimmste Partei bei Geldwaesche",
)

MUSS_DURCHLASSEN = (
    # konkreter Anker -> der Claim ist prüfbar, Länder-Quellen sind erlaubt
    "Die FPÖ war in die Ibiza-Affäre verwickelt",
    "Die ÖVP-Chats zeigen Absprachen",
    "Die OEVP-Chats zeigen Absprachen",
    "Bei der Casinos-Affäre gab es 12 Verfahren",
    "Bei der Casinos-Affaere gab es 12 Verfahren",
    "Grasser wurde im BUWOG-Prozess verurteilt",
    "Die BVT-Affäre hatte Konsequenzen",
    "Die BVT-Affaere hatte Konsequenzen",
    "Cum-Ex war der größte Steuerskandal",
    "Cum Ex war der groesste Steuerskandal",
    # gar keine Partei-Korruptions-Aussage
    "Österreich liegt im Korruptionsindex auf Platz 20",
    "Oesterreich liegt im Korruptionsindex auf Platz 20",
    "Die Korruption in Ungarn ist laut CPI hoch",
    "Wie hoch ist die Inflation in Österreich?",
    "Die SPÖ hat 2024 die Wien-Wahl gewonnen",
)


@pytest.mark.parametrize("claim", MUSS_BLOCKEN)
def test_partei_korruptions_superlativ_blockt(claim):
    """Vor dem Fix: 10 von 14 — vier rutschten durch."""
    assert politik_guard_action(claim.lower()) == "block_country_sources"


@pytest.mark.parametrize("claim", MUSS_DURCHLASSEN)
def test_legitimer_claim_behaelt_seine_quellen(claim):
    """Die Muss-Treffer-Kontrolle. Ein Guard, der zu viel blockt, schaltet
    Fakten still ab — das ist der teurere Fehler von beiden."""
    assert politik_guard_action(claim.lower()) == "pass"


def test_guard_ist_unabhaengig_von_der_schreibweise_des_aufrufers():
    """16 Aufrufstellen reichen mal `claim.lower()`, mal `claim_lc`, mal eine
    bereits gefaltete Variable herein. Der Guard normalisiert deshalb SELBST;
    `normalisiere` ist idempotent, also ist das überall richtig."""
    for claim in MUSS_BLOCKEN:
        assert politik_guard_action(claim.lower()) == \
            politik_guard_action(normalisiere(claim)), claim
    for claim in MUSS_DURCHLASSEN:
        assert politik_guard_action(claim.lower()) == \
            politik_guard_action(normalisiere(claim)), claim


def test_leerer_claim_passiert():
    assert politik_guard_action("") == "pass"
    assert politik_guard_action(None or "") == "pass"


@pytest.mark.parametrize("liste,name", [
    (_PARTY_TOKENS, "_PARTY_TOKENS"),
    (_CORRUPTION_TOKENS, "_CORRUPTION_TOKENS"),
    (_SUPERLATIVE_TOKENS, "_SUPERLATIVE_TOKENS"),
    (_SPECIFIC_ANCHOR_TOKENS, "_SPECIFIC_ANCHOR_TOKENS"),
    (_AFFAIR_PERSON_TOKENS, "_AFFAIR_PERSON_TOKENS"),
])
def test_alle_fuenf_listen_sind_normalisiert(liste, name):
    """Alle fünf, nicht nur die auffälligen: bei zweien kippt der Ausfall in
    die andere Richtung (Über-Block), was genauso falsch ist."""
    for tok in liste:
        assert normalisiere(tok) == tok, f"{name}: {tok!r} ist nicht gefaltet"


def test_keine_ascii_zwillinge_mehr():
    """`fpoe` neben `fpö` war Handarbeit — und Handarbeit vergisst welche."""
    for liste in (_PARTY_TOKENS, _CORRUPTION_TOKENS, _SUPERLATIVE_TOKENS,
                  _SPECIFIC_ANCHOR_TOKENS, _AFFAIR_PERSON_TOKENS):
        assert len(set(liste)) == len(liste), f"Dublette in {liste}"


def test_wortgrenzen_schutz_ueberlebt_die_normalisierung():
    """`"grüne "` und `"alle "` tragen ein Rand-Leerzeichen als Wortgrenze.

    Ein `strip()` in `normalisiere` würde daraus `"alle"` machen — genau der
    Fehler, der beim Bauen von #143 passiert ist (`" eter "` traf in
    „KilomETER"). Hier wird geprüft, dass die Faltung ihn nicht wieder
    einführt.
    """
    gruene = [t for t in _PARTY_TOKENS if t.startswith("gruene")]
    assert gruene == ["gruene "], gruene
    assert "alle " in _SUPERLATIVE_TOKENS


def test_bekannte_grenze_alle_trifft_auch_als_wortende():
    """VORBESTEHEND, hier nur festgehalten — nicht in diesem PR behoben.

    Das Rand-Leerzeichen in `"alle "` schützt nur die RECHTE Seite. Links
    steckt das Token weiterhin in „Krist**alle** ", „Interv**alle** ",
    „Met**alle** ". Ein `" alle "` mit beidseitigem Leerzeichen wäre keine
    Lösung: der gefaltete Claim trägt kein führendes Leerzeichen, „Alle
    Politiker sind korrupt" würde dann nicht mehr greifen — und das ist der
    Fall, für den das Token da ist.

    Die Fehlrichtung ist die sichere: ein Über-Block kostet Quellen, kein
    falsches Verdict. Sauber wäre eine Wortgrenzen-Prüfung für dieses eine
    Token — eine Änderung an der Vergleichsart des Guards, die eine eigene
    Messung verdient und nicht nebenbei in einen Schreibweisen-PR gehört.
    """
    assert politik_guard_action(
        "die kristalle der övp-kronleuchter sind korrupt"
    ) == "block_country_sources"


def test_guard_vertraegt_auch_ungefalteten_originaltext():
    """Nebengewinn der internen Normalisierung: der Guard braucht kein
    `.lower()` mehr vom Aufrufer. Vorher gab „Die ÖVP ist die korrupteste
    Partei" ein `pass`, wenn jemand den Originaltext hereinreichte — das
    Token heisst `övp`, der Claim trug `ÖVP`."""
    roh = "Die ÖVP ist die korrupteste Partei Österreichs"
    assert politik_guard_action(roh) == "block_country_sources"


# --------------------------------------------------------------------------
# Guard 2 — Wahlprognose (Politik-Tabu #2)
# --------------------------------------------------------------------------

def _prognose_guard():
    """Muster und Tokens aus dem Quelltext ziehen und die Logik nachbilden.

    Der Guard steckt mitten in einer langen Funktion; ihn dort aufzurufen
    hiesse, ein ganzes Verdict-Dict zu bauen. Die Logik ist zwei Zeilen —
    die Zusicherung gilt den Mustern, und die kommen aus der echten Datei.
    """
    quelle = (BACKEND / "services" / "verdict_postprocess.py").read_text(
        encoding="utf-8")
    blk = textwrap.dedent(quelle[quelle.index("    _PROGNOSE_PATTERNS = ("):
                                 quelle.index("    is_prognose = (")])
    ns = {"norm_terme": norm_terme}
    exec(blk, ns)  # noqa: S102 — eigener Quelltext, kein Fremdinput
    muster, tokens = ns["_PROGNOSE_PATTERNS"], ns["_PARTY_TOKENS_SHORT"]

    def guard(claim):
        n = normalisiere(claim)
        return (any(re.search(p, n) for p in muster)
                and any(t in n for t in tokens))
    return guard


PROGNOSEN = (
    ("Die SPÖ dürfte die nächste Wahl gewinnen",
     "Die SPOE duerfte die naechste Wahl gewinnen"),
    ("Die ÖVP könnte stärkste Partei werden",
     "Die OEVP koennte staerkste Partei werden"),
    ("Bei der nächsten Nationalratswahl wird die FPÖ siegen",
     "Bei der naechsten Nationalratswahl wird die FPOE siegen"),
    ("Die Grünen dürften die nächste Wahl verlieren",
     "Die Gruenen duerften die naechste Wahl verlieren"),
    ("Die AfD wird die nächste Wahl gewinnen",
     "Die AfD wird die naechste Wahl gewinnen"),
)


@pytest.mark.parametrize("umlaut,ascii_form", PROGNOSEN)
def test_wahlprognose_greift_in_beiden_schreibweisen(umlaut, ascii_form):
    """Vor dem Fix griff die ASCII-Form in 4 von 5 Fällen nicht."""
    guard = _prognose_guard()
    assert guard(umlaut), umlaut
    assert guard(ascii_form), ascii_form


@pytest.mark.parametrize("claim", (
    "Die SPÖ hat die Wien-Wahl 2025 gewonnen",
    "Die SPOE hat die Wien-Wahl 2025 gewonnen",
    "Bei der Nationalratswahl 2024 wurde die FPÖ stärkste Partei",
    "Bei der Nationalratswahl 2024 wurde die FPOE staerkste Partei",
    "Wie hoch ist die Inflation in Österreich?",
    "Die nächste Volkszählung findet 2031 statt",
))
def test_abgeschlossenes_wahlergebnis_ist_keine_prognose(claim):
    """Evidora bewertet abgeschlossene Wahlergebnisse — der Guard darf sie
    nicht mit einer Prognose verwechseln."""
    assert not _prognose_guard()(claim), claim


def test_prognose_muster_tragen_keine_umlaute():
    """Die Muster laufen gegen den gefalteten Claim. Ein Umlaut im Muster
    kann dort nie mehr treffen — er wäre toter Code, der aussieht, als würde
    er schützen."""
    quelle = (BACKEND / "services" / "verdict_postprocess.py").read_text(
        encoding="utf-8")
    # Nur die MUSTER — die Token-Liste daneben darf ihre Umlaute behalten,
    # sie läuft durch `norm_terme` und bleibt so lesbar.
    blk = quelle[quelle.index("    _PROGNOSE_PATTERNS = ("):
                 quelle.index("    _PARTY_TOKENS_SHORT = ")]
    assert not re.search(r"[äöüßÄÖÜ]", blk), (
        "Umlaut im Muster des Wahlprognose-Guards — gegen den gefalteten "
        "Claim kann er nie treffen: " + blk)
