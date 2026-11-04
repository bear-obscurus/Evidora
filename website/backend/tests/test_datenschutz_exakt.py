"""Die Datenschutzerklaerung nennt Zahlen — jede muss zum Code passen.

Der Nutzer wollte beide Fassungen „moeglichst exakt" (4.11.2026). Bis dahin
stand dort u. a. „Logs werden beim Neustart des Servers automatisch
geloescht" (falsch: Docker behielt sie bis zum Neuerstellen oder 100 MB) und
„Deine eingegebenen Behauptungen werden nicht geloggt" (falsch bis #306).
Der Verlauf im Browser fehlte ganz, ebenso Google als Empfaenger von
Suchbegriffen.

Ein exakter Text bleibt nur exakt, wenn eine geaenderte Konstante ihn
mitzieht. Deshalb prueft dieser Test jede Zahl in BEIDEN Fassungen gegen die
Stelle im Code, aus der sie stammt. Aendert jemand die Aufbewahrung, das
Rate-Limit oder den Cache, schlaegt er an, bis der Text nachgezogen ist.

Nicht pruefbar, weil extern: Mistral bewahrt API-Ein- und -Ausgaben 30 Tage
zur Missbrauchserkennung auf (legal.mistral.ai/terms/privacy-policy, am
4.11.2026 nachgelesen), und der Trainings-Widerspruch ist eine Einstellung im
Mistral-Konto.

Keine Netzabfrage in diesen Tests.
"""

import re
from pathlib import Path

import pytest

from services import _log_datei as LD
from services import _log_redaktion as LR

BACKEND = Path(__file__).resolve().parents[1]
WEBSITE = BACKEND.parent
I18N = (WEBSITE / "frontend" / "i18n.js").read_text(encoding="utf-8")


def _privacy(n):
    """Die n-te ``privacy_html``-Vorlage: 0 = Deutsch, 1 = Englisch."""
    teile = I18N.split("privacy_html: `")
    assert len(teile) == 3, "zwei privacy_html-Vorlagen erwartet"
    return teile[n + 1].split("`", 1)[0]


DE, EN = _privacy(0), _privacy(1)


def _host_rotate(logdatei):
    s = (WEBSITE / "host_logs_ohne_ip.sh").read_text(encoding="utf-8")
    m = re.search(re.escape(f"/var/log/nginx/{logdatei} {{") + r"(.*?)\n\}", s, re.S)
    return int(re.search(r"\brotate (\d+)\b", m.group(1)).group(1))


def _konstante(datei, name):
    m = re.search(rf"^{name}\s*=\s*(?:int\(os\.getenv\(\"{name}\", \")?(\d+)",
                  (BACKEND / datei).read_text(encoding="utf-8"), re.M)
    assert m, (datei, name)
    return int(m.group(1))


# --------------------------------------------------------------------------
# Aufbewahrung: heute + N rotierte Dateien = hoechstens N + 1 Tage
# --------------------------------------------------------------------------

def test_zugriffs_und_dienstprotokoll_31_tage():
    tage = LD.AUFBEWAHRUNG_TAGE + 1
    assert _host_rotate("access.log") + 1 == tage
    assert DE.count(f"spätestens {tage} Tagen") == 2, "Zugriffs- UND Dienstprotokoll"
    assert EN.count(f"after {tage} days at the latest") == 2


def test_fehlerprotokoll_8_tage():
    tage = _host_rotate("error.log") + 1
    assert f"spätestens {tage} Tagen" in DE and f"höchstens {tage} Tage" in DE
    assert f"after {tage} days at the latest" in EN and f"at most {tage} days" in EN


def test_container_logs_100_mb():
    c = (WEBSITE / "docker-compose.yml").read_text(encoding="utf-8")
    for dienst in ("backend", "frontend"):
        start = c.index(f"\n  {dienst}:\n") + 1
        naechster = re.search(r"\n  [a-z]+:\n", c[start:])
        block = c[start:start + naechster.start()] if naechster else c[start:]
        groesse = int(re.search(r'max-size: "(\d+)m"', block).group(1))
        anzahl = int(re.search(r'max-file: "(\d+)"', block).group(1))
        assert groesse * anzahl == 100, (dienst, groesse, anzahl)
    assert "höchstens 100 MB je Dienst" in DE and "at most 100 MB per service" in EN


# --------------------------------------------------------------------------
# Arbeitsspeicher, Rate-Limit, Browser
# --------------------------------------------------------------------------

def test_cache_30_minuten():
    ttl = _konstante("services/verdict_cache.py", "DEFAULT_TTL")
    assert ttl % 60 == 0
    assert f"bis zu {ttl // 60} Minuten im Arbeitsspeicher" in DE
    assert f"in memory for up to {ttl // 60} minutes" in EN


def test_rate_limit_10_pro_minute():
    assert _konstante("main.py", "RATE_WINDOW") == 60, "Text sagt 'pro Minute'"
    n = _konstante("main.py", "RATE_LIMIT")
    assert f"höchstens {n} Prüfungen pro Minute" in DE
    assert f"at most {n} checks per minute" in EN


def test_verlauf_10_pruefungen():
    js = (WEBSITE / "frontend" / "app.js").read_text(encoding="utf-8")
    n = int(re.search(r"const MAX_HISTORY = (\d+);", js).group(1))
    assert f"letzten {n} Prüfungen (Behauptung, Ergebnis, Datum)" in DE
    assert f"last {n} checks (claim, result, date)" in EN
    assert 'localStorage.setItem("evidora-lang"' in I18N, "Sprache steht im localStorage"


def test_verlauf_button_heisst_wie_im_text():
    assert 'history_clear: "Verlauf löschen"' in I18N and "„Verlauf löschen“" in DE
    assert 'history_clear: "Clear history"' in I18N and "“Clear history”" in EN
    assert 'btn_report: "Ergebnis melden"' in I18N and "„Ergebnis melden“" in DE
    assert 'btn_report: "Report result"' in I18N and "“Report result”" in EN


def test_vier_buchstaben():
    wort = {4: ("vier", "four")}[LR.MIN_WORT]
    assert f"weniger als {wort[0]} Buchstaben" in DE
    assert f"shorter than {wort[1]} letters" in EN


def test_teilen_link_mit_fragment():
    js = (WEBSITE / "frontend" / "app.js").read_text(encoding="utf-8")
    assert "#claim=${encodeURIComponent(claim)}" in js
    assert "hinter dem #-Zeichen" in DE and "after the # sign" in EN


def test_nur_mistral_cloud_wird_beschrieben():
    """Auf evidora.eu ist MISTRAL_API_KEY gesetzt; ollama.py nimmt dann
    immer die Cloud (use_cloud = bool(MISTRAL_API_KEY)), ohne Rueckfall."""
    src = (BACKEND / "services" / "ollama.py").read_text(encoding="utf-8")
    assert "use_cloud = bool(MISTRAL_API_KEY)" in src
    assert "Mistral AI" in DE and "Mistral AI" in EN


def test_google_steht_in_der_empfaengerliste():
    """Gemessen 3.11.2026: 178 Anfragen mit Suchbegriffen an factchecktools."""
    src = (BACKEND / "services" / "claimreview.py").read_text(encoding="utf-8")
    assert "factchecktools.googleapis.com" in src
    assert "Google Fact Check Tools (Google, USA)" in DE
    assert "Google Fact Check Tools (Google, USA)" in EN


# --------------------------------------------------------------------------
# Was nicht mehr stimmt, darf nicht mehr dastehen
# --------------------------------------------------------------------------

@pytest.mark.parametrize("satz", [
    "beim Neustart des Servers automatisch gelöscht",
    "Deine eingegebenen Behauptungen werden nicht geloggt",
    "Da keine personenbezogenen Daten gespeichert werden",
    "Lokales Sprachmodell",
    "keine Trainingsnutzung laut Mistral-Richtlinien",
])
def test_alte_saetze_de_sind_weg(satz):
    assert satz not in DE


@pytest.mark.parametrize("satz", [
    "automatically deleted when the server restarts",
    "Your entered claims are not logged",
    "Since no personal data is stored",
    "Local language model",
    "no training use per Mistral",
])
def test_alte_saetze_en_sind_weg(satz):
    assert satz not in EN


def test_beide_fassungen_nennen_dieselben_zahlen():
    """Eine Zahl, die nur in einer Sprache steht, ist in der anderen falsch.

    Nur Abschnitt 2 bis 4 — die Quellenliste in Abschnitt 5 ist in der
    englischen Fassung ausfuehrlicher (ESA-2010) und hier nicht Gegenstand.
    """
    zahlen = lambda t: sorted(re.findall(r"\b\d+\b", re.sub(r"<[^>]+>", " ", t)))
    teil = lambda t: t[t.index("<h3>2."):t.index("<h3>5.")]
    assert zahlen(teil(DE)) == zahlen(teil(EN))
    assert zahlen(teil(DE)), "leer — Test misst nichts"
