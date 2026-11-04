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


def test_rate_limit_kennwert_hoechstens_90_sekunden():
    """Seit 4.11.2026: Kennwert statt IP, geraeumt im Hintergrund."""
    fenster = _konstante("main.py", "RATE_WINDOW")
    abstand = _konstante("services/ratelimit.py", "RAEUM_INTERVALL_S")
    grenze = _konstante("services/ratelimit.py", "HOECHSTDAUER_S")
    assert fenster + abstand + 10 <= grenze, "kein Puffer fuer eine belegte Event-Loop"
    assert f"spätestens {grenze} Sekunden nach deiner letzten Prüfung" in DE
    assert f"at the latest {grenze} seconds after your last check" in EN
    assert "nicht die IP-Adresse selbst" in DE and "not the IP address itself" in EN


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
    # bis 4.11.2026 abends — der Swap machte "nicht auf Datentraeger" unhaltbar
    "nicht auf Datenträger geschrieben",
])
def test_alte_saetze_de_sind_weg(satz):
    assert satz not in DE


@pytest.mark.parametrize("satz", [
    "automatically deleted when the server restarts",
    "Your entered claims are not logged",
    "Since no personal data is stored",
    "Local language model",
    "no training use per Mistral",
    "It is not written to disk",
])
def test_alte_saetze_en_sind_weg(satz):
    assert satz not in EN


def test_beide_fassungen_nennen_dieselben_zahlen():
    """Eine Zahl, die nur in einer Sprache steht, ist in der anderen falsch.

    Nur Abschnitt 2 bis 4 — die Quellenliste in Abschnitt 5 ist in der
    englischen Fassung ausfuehrlicher (ESA-2010) und hier nicht Gegenstand.
    """
    zahlen = lambda t: sorted(re.findall(r"\b\d+\b", re.sub(r"<[^>]+>", " ", t)))
    teil = lambda t: t[t.index("<h3>1."):t.index("<h3>5.")]
    assert zahlen(teil(DE)) == zahlen(teil(EN))
    assert zahlen(teil(DE)), "leer — Test misst nichts"


# --------------------------------------------------------------------------
# Verantwortlicher und Hoster (5.11.2026: "den transparenten Weg")
# --------------------------------------------------------------------------

def test_verantwortlicher_kommt_aus_dem_impressum():
    """Open Source: Name und Kontakt stehen in der Instanz-Konfiguration
    (/api/legal), nicht im Code — Impressum und Erklaerung lesen dieselbe Quelle."""
    assert "Verantwortlich für die Verarbeitung: {verantwortlich}" in DE
    assert "Controller responsible for the processing: {verantwortlich}" in EN
    js = (WEBSITE / "frontend" / "app.js").read_text(encoding="utf-8")
    block = js[js.index('if (type === "privacy")'):js.index('} else if (type === "disclaimer")')]
    assert "await ladeImpressum()" in block
    for feld in ("name", "location", "email"):
        assert f"escapeHtml(imp.{feld})" in block, feld
    assert 'replace(/\\{verantwortlich\\}/g' in block
    assert I18N.count("privacy_controller_unconfigured:") == 2, "DE und EN"


def test_hoster_wird_genannt():
    assert "Hetzner Online GmbH (Industriestraße 25, 91710 Gunzenhausen, Deutschland) in einem Rechenzentrum in Nürnberg" in DE
    assert "Hetzner Online GmbH (Industriestraße 25, 91710 Gunzenhausen, Germany) in a data centre in Nuremberg" in EN


def test_meldungs_mails_haben_ein_loeschkriterium():
    """Art. 13 Abs. 2 lit. a DSGVO: Dauer oder Kriterien der Speicherung."""
    assert "bis die Meldung bearbeitet ist, und löschen sie danach" in DE
    assert "until the report has been handled, and delete it afterwards" in EN


# --------------------------------------------------------------------------
# Abschnitt 5: Quellen und ihre Nutzungsbedingungen
# --------------------------------------------------------------------------

def _quellen(t):
    teil = t[t.index("<h3>5."):]
    return [re.sub(r"<[^>]+>", "", re.split(r" — ", li)[0]).strip()
            for li in re.findall(r"<li>(.*?)</li>", teil, re.S)]


#: Jede genannte Quelle hat einen Dienst — sonst nennt die Erklaerung etwas,
#: das Evidora nicht abfragt.
QUELLE_ZU_DIENST = {
    "PubMed / NCBI": "pubmed.py", "WHO GHO": "who.py", "EMA": "ema.py",
    "EFSA": "efsa.py", "Copernicus CDS": "copernicus.py", "Eurostat": "eurostat.py",
    "EZB": "ecb.py", "ECB": "ecb.py", "UNHCR": "unhcr.py", "EEA": "eea.py",
    "ECDC": "ecdc.py", "Statistik Austria": "statistik_austria.py",
    "GeoSphere Austria": "geosphere.py", "BASG": "basg.py", "RIS": "ris.py",
    "BMI Volksbegehren": "volksbegehren.py", "BMI Popular Initiatives": "volksbegehren.py",
    "BMI Wahlen / Bundeswahlbehörde": "wahlen.py", "BMI Elections / Federal Electoral Commission": "wahlen.py",
    "Parlament Abstimmungen": "abstimmungen.py", "Austrian Parliament Voting Records": "abstimmungen.py",
    "AT Factbook": "at_factbook.py", "BKA PKS": "pks.py", "DACH Factbook": "dach_factbook.py",
    "Cochrane Reviews": "cochrane.py", "GADMO Faktenchecks": "gadmo.py", "GADMO fact-checks": "gadmo.py",
    "EFCSN-Faktenchecker": "claimreview.py", "EFCSN fact-checkers": "claimreview.py",
}


def test_beide_sprachen_nennen_dieselben_quellen():
    """Bis 5.11.2026 fehlte EFSA im Englischen."""
    de, en = _quellen(DE), _quellen(EN)
    assert len(de) == len(en) == 23, (len(de), len(en))
    assert [QUELLE_ZU_DIENST[q] for q in de] == [QUELLE_ZU_DIENST[q] for q in en]


def test_jede_quelle_hat_einen_dienst():
    for q in _quellen(DE) + _quellen(EN):
        assert (BACKEND / "services" / QUELLE_ZU_DIENST[q]).exists(), q


def _eintrag(t, name):
    teil = t[t.index("<h3>5."):]
    (li,) = [x for x in re.findall(r"<li>(.*?)</li>", teil, re.S) if x.startswith(f"<strong>{name}</strong>")]
    return li


@pytest.mark.parametrize("name", ["EMA", "Eurostat"])
def test_ema_und_eurostat_sind_nicht_cc_by(name):
    """Beide erlauben Weiterverwendung mit Quellenangabe — keine CC-Lizenz
    (EMA Legal Notice; bei Eurostat gilt CC BY 4.0 nur fuer redaktionelle
    Inhalte). Nachgelesen am 5.11.2026."""
    assert "CC BY" not in _eintrag(DE, name) and "CC BY" not in _eintrag(EN, name)
    assert "Weiterverwendung mit Quellenangabe erlaubt" in _eintrag(DE, name)
    assert "reuse permitted with acknowledgement of the source" in _eintrag(EN, name)


def test_ris_ist_cc_by_mit_aktuellem_link():
    """RIS-OGD-FAQ: CC BY 4.0; data.bka.gv.at leitet seit 2026 um."""
    for t in (DE, EN):
        e = _eintrag(t, "RIS")
        assert "(CC BY 4.0)" in e and "https://www.ris.bka.gv.at/UI/Ogd.aspx" in e
        assert "data.bka.gv.at" not in e


@pytest.mark.parametrize("name", ["Statistik Austria", "GeoSphere Austria"])
def test_cc_by_wo_es_stimmt(name):
    assert "(CC BY 4.0)" in _eintrag(DE, name) and "(CC BY 4.0)" in _eintrag(EN, name)


def test_geosphere_nutzt_die_cc_by_jahresdaten():
    """HISTALP waere CC BY-NC 4.0 — genutzt wird klima-v2-1y (CC BY 4.0)."""
    src = (BACKEND / "services" / "geosphere.py").read_text(encoding="utf-8")
    assert "klima-v2-1y" in src and "histalp" not in src.lower()


def test_esvg_2010_in_beiden_sprachen():
    assert "ESVG-2010-Förderquote" in DE and "ESA-2010 subsidy ratio" in EN


def test_einleitung_sagt_auswahl():
    """23 Eintraege von rund 190 Quellen — "folgende Datenquellen" war falsch."""
    assert "Hier die wichtigsten mit ihren Nutzungsbedingungen; weitere stehen unter „Quellen“" in DE
    assert "These are the main ones with their terms of use; more are listed under “Sources”" in EN
    assert 'sources_link: "Quellen"' in I18N and 'sources_link: "Sources"' in I18N
