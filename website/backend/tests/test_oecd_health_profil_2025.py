"""OECD-Gesundheitsdaten: Vintage 2025 und ein Renderer, der Feldwechsel überlebt.

Befund (2026-09-05, aus dem Freshness-Rückstand): `data/oecd_health.json` trug
den Stand *OECD Health Statistics 2024 / Health at a Glance 2023*. Ersetzt durch
das **Country Health Profile 2025** (OECD + European Observatory + Europäische
Kommission, „State of Health in the EU").

Drei Zahlen waren nicht nur alt, sondern in einer anderen MESSGRÖSSE angegeben —
dieselbe Falle wie beim Fleisch-Befund (#321):

  Lebenserwartung   alt 81,7 (2024)      ->  **82,3** (2024, Profil 2025)
  Spitalsbetten     alt 7,2 AKUTbetten   ->  **6,6 ALLE Betten** (2023)
  Ausgaben/Kopf     alt 5.780 € nominal  ->  **4.901 € kaufkraftbereinigt** (2023)

Wer 7,2 gegen 6,6 oder 5.780 gegen 4.901 hält, misst zwei verschiedene Dinge und
liest einen dramatischen Rückgang heraus, den es nicht gibt. Die Warnung steht
deshalb in der HEADLINE — dem einzigen Kanal, der die 400-Zeichen-Kürzung im
Prompt nachweislich ungekürzt passiert (Lehre aus #117).

Der Renderer hatte dieselbe Krankheit wie `services/wifo_ihs.py`: Feldnamen fest
verdrahtet, inklusive Arithmetik `at_2024 - at_2000`. Beim Vintage-Wechsel warf
das `TypeError: unsupported operand type(s) for -: 'float' and 'NoneType'` — und
`search_*` hat kein try/except, die Fan-out-Schleife verwirft die GANZE Quelle
mit einer blossen WARNING. Ein Feld weniger darf höchstens einen Satz kosten,
nicht die Quelle.

Deutschland- und Schweiz-Werte sind bewusst ENTFERNT statt fortgeschrieben: Das
Länderprofil stellt Österreich nur dem EU-Schnitt gegenüber, und zwei Vintages
in einem Datensatz zu mischen ist die V-Dem-Lehre.
"""

import asyncio
import json
from pathlib import Path

BACKEND = Path(__file__).resolve().parents[1]
PFAD = BACKEND / "data" / "oecd_health.json"
DATEN = json.loads(PFAD.read_text(encoding="utf-8"))
FAKTEN = {f["topic"]: f for f in DATEN["facts"]}
QUELLE = (BACKEND / "services" / "oecd_health.py").read_text(encoding="utf-8")

MAX_STR = 400  # synthesizer.MAX_STR — ab hier kürzt der Prompt

from services._fmt import de_num  # noqa: E402
from services.oecd_health import search_oecd_health  # noqa: E402

MUSS_TREFFER = {
    "Die Lebenserwartung in Österreich sinkt": "lebenserwartung",
    "Österreich hat zu wenig Spitalsbetten": "spitalsbetten",
    "Das Gesundheitssystem in Österreich kollabiert": "spitalsbetten",
    "Das österreichische Gesundheitssystem ist unterfinanziert": "gesundheitsausgaben",
    "BIP Anteil Gesundheit": "gesundheitsausgaben",
    "Schulkinder werden immer dicker": "kinder_adipositas",
}


def _treffer(claim):
    return asyncio.run(search_oecd_health({"claim": claim,
                                           "original_claim": claim}))["results"]


def _ergebnis(claim, topic):
    for r in _treffer(claim):
        if r["topic"] == topic:
            return r
    raise AssertionError(f"{claim!r} liefert kein Ergebnis zu {topic!r}")


# --------------------------------------------------------------------------
# Der Renderer darf einen Feldwechsel überleben
# --------------------------------------------------------------------------

def test_renderer_stuerzt_bei_keinem_fakt_ab():
    """Der eigentliche Regressionstest: mit dem alten Renderer wirft dieser
    Aufruf TypeError, weil `lebenserwartung_at_2000` nicht mehr existiert."""
    for claim, topic in MUSS_TREFFER.items():
        assert _ergebnis(claim, topic)["display_value"], f"leer bei {claim!r}"


def test_fehlendes_feld_kostet_nur_einen_satz():
    """Ein Datensatz ohne ein Feld darf den Baustein verlieren — nicht mehr."""
    from services.oecd_health import _fuege
    voll = _fuege(("A.", ["x"]), ("B.", ["y"]), d={"x": 1, "y": 2})
    teil = _fuege(("A.", ["x"]), ("B.", ["y"]), d={"x": 1})
    assert voll == "A. B." and teil == "A."


def test_keine_platzhalter_in_der_ausgabe():
    """`de_int`/`de_num` geben bei None ein '?' zurück — das darf nie in den
    Prompt gelangen, weil der Synthesizer es als Zahl lesen könnte."""
    for claim, topic in MUSS_TREFFER.items():
        text = _ergebnis(claim, topic)["display_value"]
        for platzhalter in ("None", "?", "{", "}"):
            assert platzhalter not in text, f"{platzhalter!r} in {topic}: {text}"


def test_keine_feldnamen_mehr_der_alten_vintage_im_code():
    code = "\n".join(z for z in QUELLE.splitlines()
                     if not z.lstrip().startswith("#"))
    for verdrahtet in ("lebenserwartung_at_2000", "lebenserwartung_de_2024",
                       "lebenserwartung_ch_2024", "akutbetten_pro_1000",
                       "anzahl_pflegekraefte_pro_1000"):
        assert verdrahtet not in code, f"{verdrahtet} steht noch im Renderer"


# --------------------------------------------------------------------------
# Die drei Messgrößen-Fallen
# --------------------------------------------------------------------------

def test_lebenserwartung_ist_profil_2025():
    d = FAKTEN["lebenserwartung"]["data"]
    assert d["lebenserwartung_at_2024"] == 82.3
    assert d["lebenserwartung_at_frauen_2024"] == 84.5
    assert d["geschlechter_luecke_jahre_2024"] == 4.5
    assert "82,3" in _ergebnis("Die Lebenserwartung in Österreich sinkt",
                               "lebenserwartung")["display_value"]


def test_spitalsbetten_sind_alle_betten_nicht_akutbetten():
    d = FAKTEN["spitalsbetten"]["data"]
    assert d["spitalsbetten_pro_1000_at_2023"] == 6.6
    assert d["spitalsbetten_rueckgang_2017_2023_pct"] == 10
    assert d["stationaere_aufenthalte_rueckgang_2017_2023_pct"] == 15
    kopf = FAKTEN["spitalsbetten"]["headline"]
    assert "MESSGRÖSSE" in kopf and "AKUTBETTEN" in kopf and "7,2" in kopf
    assert "NICHT" in kopf, "die Warnung muss ein Verbot sein, kein Hinweis"


def test_ausgaben_pro_kopf_sind_kaufkraftbereinigt():
    d = FAKTEN["gesundheitsausgaben"]["data"]
    assert d["gesundheitsausgaben_at_pct_bip_2023"] == 11.2
    assert d["pro_kopf_kkp_eur_at_2023"] == 4901
    assert d["pro_kopf_kkp_eur_eu_avg_2023"] == 3832
    kopf = FAKTEN["gesundheitsausgaben"]["headline"]
    assert "KAUFKRAFTBEREINIGT" in kopf and "5.780" in kopf and "NICHT" in kopf


def test_alte_zahlen_nur_noch_als_warnung():
    """7,2 und 5.780 dürfen vorkommen — aber nur in der Abgrenzung, nie als
    aktueller Wert."""
    for topic, alt in (("spitalsbetten", "7,2"), ("gesundheitsausgaben", "5.780")):
        kopf = FAKTEN[topic]["headline"]
        vor = kopf[:kopf.index(alt)]
        assert "MESSGRÖSSE" in vor, f"{alt} in {topic} steht ohne Abgrenzung"


# --------------------------------------------------------------------------
# Die Warnung muss die Prompt-Kürzung überleben (Lehre aus #117)
# --------------------------------------------------------------------------

def test_headline_und_display_bleiben_unter_dem_prompt_budget():
    for f in DATEN["facts"]:
        assert len(f["headline"]) <= MAX_STR, (
            f"{f['id']}: Headline {len(f['headline'])} > {MAX_STR} Zeichen — "
            "die Messgrößen-Warnung am Ende würde weggekürzt")
    for claim, topic in MUSS_TREFFER.items():
        text = _ergebnis(claim, topic)["display_value"]
        assert len(text) <= MAX_STR, f"{topic}: display_value {len(text)} > {MAX_STR}"


def test_messgroesse_steht_im_ungekuerzten_kanal():
    """Nicht nur im data-Feld: `messgroesse` landet am ENDE des display_value
    und wäre bei jeder künftigen Verlängerung das Erste, was wegfällt."""
    for topic in ("spitalsbetten", "gesundheitsausgaben"):
        assert "MESSGRÖSSE" in FAKTEN[topic]["headline"], (
            f"{topic}: Warnung nur im display_value — nicht kürzungsfest")


# --------------------------------------------------------------------------
# Keine gemischten Vintages
# --------------------------------------------------------------------------

def test_keine_de_ch_werte_aus_fremder_vintage():
    """Das Länderprofil kennt nur AT gegen EU-Schnitt."""
    for feld in ("lebenserwartung_de_2024", "lebenserwartung_ch_2024",
                 "lebenserwartung_eu_avg_2024", "lebenserwartung_at_2000",
                 "lebenserwartung_at_2010", "lebenserwartung_at_maenner_2024",
                 "akutbetten_pro_1000_at", "akutbetten_pro_1000_de",
                 "anzahl_pflegekraefte_pro_1000_at",
                 "gesundheitsausgaben_de_pct_bip_2024",
                 "gesundheitsausgaben_at_pro_kopf_eur_2024"):
        for topic in ("lebenserwartung", "spitalsbetten", "gesundheitsausgaben"):
            assert feld not in FAKTEN[topic]["data"], f"{feld} ist alte Vintage"


def test_notes_widersprechen_den_daten_nicht():
    """Die alte Note nannte 'Frauen 84,1 / Männer 79,3' — gegen 84,5 im
    neuen Datensatz. Ein Selbstwiderspruch im selben Fakt ist schlimmer als
    eine fehlende Note."""
    notes = " ".join(FAKTEN["lebenserwartung"]["context_notes"])
    assert "84,1" not in notes and "79,3" not in notes
    assert "84,5" in notes and "4,5" in notes


def test_quellenangabe_nennt_das_laenderprofil():
    for topic in ("lebenserwartung", "spitalsbetten", "gesundheitsausgaben"):
        f = FAKTEN[topic]
        assert "Country Health Profile 2025" in f["source_label"]
        assert "chp2025" in f["source_url"]
    assert FAKTEN["kinder_adipositas"]["source_label"].startswith("WHO-COSI"), (
        "der Kinder-Fakt hat eine andere Quelle und bleibt unangetastet")


def test_jahr_passt_zur_vintage_des_fakts():
    """2024 für die Lebenserwartung, 2023 für Betten und Ausgaben — der
    Renderer liest das Jahr aus dem Fakt, statt es zu verdrahten."""
    assert FAKTEN["lebenserwartung"]["year"] == 2024
    assert FAKTEN["spitalsbetten"]["year"] == 2023
    assert FAKTEN["gesundheitsausgaben"]["year"] == 2023
    code = "\n".join(z for z in QUELLE.splitlines()
                     if not z.lstrip().startswith("#"))
    assert "+ year" in code, "Feldnamen müssen aus dem Jahr des Fakts kommen"


# --------------------------------------------------------------------------
# Trigger: alle Phrasings, keine Über-Trigger
# --------------------------------------------------------------------------

def test_alle_dokumentierten_phrasings_treffen():
    from services.oecd_health import claim_mentions_oecd_health_cached
    fehl = [p for f in DATEN["facts"]
            for p in f.get("claim_phrasings_handled", [])
            if not claim_mentions_oecd_health_cached(p)]
    assert not fehl, f"Phrasings ohne Treffer: {fehl}"


def test_bip_anteil_ohne_bindestrich_triggert():
    """Vorbestehende Lücke: der Trigger kannte nur 'bip-anteil', die
    dokumentierte Phrasing schreibt 'BIP Anteil Gesundheit'."""
    assert _ergebnis("BIP Anteil Gesundheit", "gesundheitsausgaben")


def test_kein_ueber_trigger_bei_fachfremden_claims():
    for claim in ("Das BIP Österreichs wuchs 2025 um 0,8 Prozent",
                  "Die Lebenshaltungskosten in Wien steigen",
                  "Wie viele Betten hat das Hotel Sacher?",
                  "Der Bundeskanzler ist Gesundheitsminister"):
        assert not _treffer(claim), f"Über-Trigger: {claim!r}"


# --------------------------------------------------------------------------
# de_num (neu in services/_fmt.py)
# --------------------------------------------------------------------------

def test_de_num_schreibt_deutsch_und_stuerzt_nicht():
    assert de_num(82.3) == "82,3"
    assert de_num(6) == "6" and de_num(6.0) == "6"
    assert de_num(11.2) == "11,2"
    assert de_num(None) == "?" and de_num("kaputt") == "?"
