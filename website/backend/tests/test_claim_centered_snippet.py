"""Golden-Tests für die claim-zentrierte Feld-Trunkierung (Audit 2026-07-07).

Befund (Kickl/„Volkskanzler"): Der DE-Wikipedia-Artikel „Volkskanzler" WAR
im Retrieval, aber die alte Trunkierung nahm stumpf die ersten 400 Zeichen
(NS-Begriffsgeschichte) — die claim-entscheidende Passage („Herbert Kickl
bezeichnet sich selbst so") stand weiter hinten und fiel weg → „nicht
überprüfbar" trotz vorhandener Quelle.

Fix: _claim_centered_truncate() extrahiert das Fenster um das erste
Vorkommen eines Claim-Terms, wenn dieses nach dem Anfangsfenster liegt.
Head-Verhalten bleibt identisch, wenn kein Term hinten steht (keine
Regression für alle Nicht-Treffer-Felder).

Dependency-light: rein string-basiert, kein Netz, kein Modell.
"""

from services.synthesizer import (
    _claim_centered_truncate,
    _prompt_claim_terms,
)

MAX = 400


def test_terms_extracted_from_entities_and_claim():
    analysis = {"entities": ["Herbert Kickl", "Volkskanzler"]}
    terms = _prompt_claim_terms(analysis, "Herbert Kickl hat sich als Volkskanzler bezeichnen lassen")
    assert "kickl" in terms
    assert "volkskanzler" in terms
    # Stoppwörter dürfen NICHT als Terme auftauchen
    assert "sich" not in terms and "hat" not in terms and "als" not in terms


def test_short_string_unchanged():
    s = "Kurzer Text unter dem Limit."
    assert _claim_centered_truncate(s, ["kickl"], MAX) == s


def test_head_behavior_when_no_term_present():
    """Kein Claim-Term im Feld → altes Head-Verhalten (s[:MAX] + Ellipse)."""
    s = "A" * 100 + " " + "wort " * 200  # >400, kein Term
    out = _claim_centered_truncate(s, ["kickl", "volkskanzler"], MAX)
    assert out.endswith("[…]")
    assert not out.startswith("[…]")  # Head, kein zentriertes Fenster
    assert len(out) <= MAX + 8


def test_head_behavior_when_term_in_opening_window():
    """Term steht schon vorne → weiterhin Head (kein unnötiges Verschieben)."""
    s = "Kickl ist Parteiobmann. " + "fülltext " * 100
    out = _claim_centered_truncate(s, ["kickl"], MAX)
    assert out.startswith("Kickl")
    assert not out.startswith("[…]")


def test_kickl_volkskanzler_passage_survives():
    """Kernfall: claim-relevante Passage steht HINTER dem 400-Zeichen-Kopf
    und muss nach dem Fix im getrimmten Feld enthalten sein."""
    prefix = (
        "Der Begriff Volkskanzler wurde in der Zeit des Nationalsozialismus "
        "propagandistisch fuer Adolf Hitler verwendet und bezeichnete den "
        "Fuehrer als Kanzler des gesamten Volkes. Der Ausdruck Volkskanzler "
        "hat damit eine historisch stark belastete Konnotation und wird in "
        "der modernen politischen Kommunikation nur selten und meist bewusst "
        "provokativ eingesetzt, sowohl direkt als auch indirekt auf namhafte "
        "Politiker der jeweiligen Gegenwart bezogen und diskutiert. "
    )
    payload = (
        "Der Parteiobmann der FPOe Herbert Kickl bezeichnet sich selbst so und "
        "benutzte den Begriff umfangreich in der Kampagne fuer die "
        "Nationalratswahl 2024."
    )
    s = prefix + payload
    assert len(prefix) > MAX  # die entscheidende Stelle liegt garantiert hinten

    out = _claim_centered_truncate(s, ["kickl", "volkskanzler"], MAX)
    # Vor dem Fix (Head s[:400]) wäre "Kickl bezeichnet sich selbst so" WEG.
    assert "Kickl" in out
    assert "bezeichnet sich selbst" in out
    assert len(out) <= MAX + 60


def test_rare_term_beats_frequent_term():
    """IDF-Kern: ein Satz mit dem SELTENEN Claim-Term schlägt einen Satz mit
    dem HÄUFIGEN Term, auch wenn letzterer früher steht und öfter vorkommt."""
    frequent_sentences = " ".join(
        f"Volkskanzler Aspekt Nummer {i} wird hier ausfuehrlich erlaeutert." for i in range(8)
    )  # "volkskanzler" 8× in frühen Sätzen
    rare = "Herbert Kickl bezeichnet sich selbst als solchen im Wahlkampf 2024."
    s = frequent_sentences + " " + rare
    assert len(s) > MAX
    out = _claim_centered_truncate(s, ["kickl", "volkskanzler"], MAX)
    assert "Kickl" in out, "seltener, spezifischer Term muss ins Snippet"


def test_window_cuts_on_word_boundaries():
    s = "vorspann " * 80 + "ZIELWORT hier steht der relevante inhalt " + "nachspann " * 40
    out = _claim_centered_truncate(s, ["zielwort"], MAX)
    assert "ZIELWORT" in out
    # keine halben Wörter am Rand (Anfang/Ende sind ganze Tokens oder Ellipse)
    core = out.strip("[…] ").strip()
    assert not core.startswith("pann")  # kein abgeschnittenes "vorspann"
