"""Was der Cron aufruft, muss es geben.

PR #249 hat die Wartung in einen eigenen Einmal-Container verlegt. Dabei
sind ZWEI Cron-Jobs gebrochen, und nichts prueffte die Aufrufer:

  * ``canary_check.py`` sprach weiter ``http://localhost:8000`` an. Im
    eigenen Container ist das sein eigener Netzwerk-Namensraum. Sechs Tage
    Fehlalarm (13.–18.10.2026), jeden Morgen zwei Pushes: einen vom
    Werkzeug ("Pipeline liefert kein Ergebnis") und einen vom Cron-Zweig
    ("Tool kam nicht zum Laufen — Backend-Container down?"). Beide falsch.
    Gemessen aus dem Wartungs-Container:

        http://backend:8000/api/health   -> 200
        http://localhost:8000/api/health -> Connection refused

  * Die CORDIS-Zeile rief den Compose-Service ``cordis-refresh``, den #249
    zu ``wartung`` umbenannt hat. ``no such service: cordis-refresh``, auf
    prod nachgeprueft. Quartalsweise — der letzte Lauf war der 1.10., also
    VOR der Umbenennung, der naechste waere der 1.1.2027 gewesen. Ein
    halbes Jahr stiller Ausfall, in dem nur der Cache gealtert waere.

Beide waren mechanisch zu finden: ein Name, der nirgends mehr existiert.
Dieses Gate rechnet es aus der kanonischen Crontab aus — deshalb liegt
sie seit dem 18.10.2026 im Repo (``website/crontab.prod``) und nicht nur
auf dem Server.

Dependency-light: nur Textdateien, kein Netz, kein Docker.
"""
import os
import re

import pytest

BACKEND = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WEBSITE = os.path.dirname(BACKEND)
CRONTAB = os.path.join(WEBSITE, "crontab.prod")
COMPOSE = os.path.join(WEBSITE, "docker-compose.yml")
TOOLS = os.path.join(BACKEND, "tools")


def _crontab() -> str:
    with open(CRONTAB, encoding="utf-8") as fh:
        return fh.read()


def _zeilen() -> list[str]:
    """Die wirksamen Zeilen — ohne Kommentare und Leerzeilen."""
    return [z for z in _crontab().splitlines()
            if z.strip() and not z.lstrip().startswith("#")]


def _compose_services() -> set[str]:
    """Service-Namen aus docker-compose.yml, ohne YAML-Abhaengigkeit.

    Services stehen auf Einrueckungstiefe 2 unter ``services:``. Ein
    Parser waere praeziser, aber PyYAML ist in den dependency-light
    Suiten nicht gesetzt, und das Muster ist stabil.
    """
    with open(COMPOSE, encoding="utf-8") as fh:
        text = fh.read()
    return set(re.findall(r"^  ([a-z][a-z0-9_-]*):$", text, re.M))


def _werkzeuge() -> set[str]:
    return {n for n in os.listdir(TOOLS) if n.endswith(".py")}


_LOOPBACK = re.compile(r"https?://(localhost|127\.0\.0\.1)")


def _loopback_defaults(pfad: str) -> list[str]:
    """``default=``-Werte eines CLI-Werkzeugs, die auf Loopback zeigen.

    Ueber den Syntaxbaum, nicht ueber Text — und mit aufgeloesten
    Modul-Konstanten. Eine erste Fassung suchte Text und wurde von einer
    Giftprobe am 18.10.2026 ueberlistet: Mit ``STANDARD_URL =
    "http://localhost:8000"`` stand in der ``default=``-Zeile nur noch
    der Name, kein Literal, und das Gate schwieg. Ein Gate, das sich
    durch eine Umbenennung abschalten laesst, ist keins.

    Docstrings bleiben aussen vor — dort stehen Aufruf-BEISPIELE. Eine
    Textsuche hat deshalb am 18.10. zwei unbeteiligte Werkzeuge gemeldet
    (``stress_test.py``, ``profile_latency.py``), deren echte Defaults
    auf ``https://evidora.eu`` zeigen.
    """
    import ast

    with open(pfad, encoding="utf-8") as fh:
        baum = ast.parse(fh.read())
    konstanten: dict[str, str] = {}
    for knoten in ast.walk(baum):
        if isinstance(knoten, ast.Assign) and isinstance(
                getattr(knoten, "value", None), ast.Constant):
            if isinstance(knoten.value.value, str):
                for ziel in knoten.targets:
                    if isinstance(ziel, ast.Name):
                        konstanten[ziel.id] = knoten.value.value

    def _texte(knoten):
        """Alle Zeichenketten, die aus diesem Ausdruck kommen koennen."""
        if isinstance(knoten, ast.Constant) and isinstance(knoten.value, str):
            return [knoten.value]
        if isinstance(knoten, ast.Name):
            return [konstanten[knoten.id]] if knoten.id in konstanten else []
        aus = []
        for kind in ast.iter_child_nodes(knoten):
            aus.extend(_texte(kind))
        return aus

    treffer = []
    for knoten in ast.walk(baum):
        if not isinstance(knoten, ast.keyword) or knoten.arg != "default":
            continue
        for text in _texte(knoten.value):
            if _LOOPBACK.search(text):
                treffer.append(text[:80])
    return treffer


# --- Die zwei gebrochenen Aufrufe --------------------------------------

def test_jedes_aufgerufene_werkzeug_existiert():
    """Haette die CORDIS-Zeile gefangen, waere sie ueber den Wrapper
    gelaufen — und faengt sie jetzt, weil sie es tut."""
    vorhanden = _werkzeuge()
    gerufen = set(re.findall(r"run_evidora_tool\.sh\s+([a-z_0-9]+\.py)",
                             _crontab()))
    assert gerufen, "Keine Werkzeug-Aufrufe gefunden — Gate liest ins Leere"
    fehlend = sorted(gerufen - vorhanden)
    assert not fehlend, f"Cron ruft nicht existierende Werkzeuge: {fehlend}"


def test_jeder_genannte_compose_service_existiert():
    """Der konkrete Bruch: ``cordis-refresh`` gab es nicht mehr."""
    vorhanden = _compose_services()
    assert vorhanden, "Keine Services gelesen — Gate liest ins Leere"
    gerufen = set(re.findall(
        r"docker\s+compose[^\n;|}]*?\brun\b[^\n;|}]*?\s"
        r"(?!--)([a-z][a-z0-9_-]*)", _crontab()))
    fehlend = sorted(gerufen - vorhanden)
    assert not fehlend, (
        f"Cron nennt nicht existierende Compose-Services: {fehlend}")


def test_die_crontab_ruft_compose_gar_nicht_mehr_direkt():
    """Ein roher ``docker compose run`` in der Crontab muss den
    Service-Namen kennen — und genau dieses Wissen ist in #249 veraltet.
    Alle Werkzeuge gehen jetzt ueber den Wrapper, der ihn als einziger
    kennt."""
    roh = [z for z in _zeilen() if "docker compose" in z and " run" in z]
    assert not roh, f"{len(roh)} Zeile(n) rufen compose direkt: {roh[:1]}"


def test_kein_werkzeug_im_cron_spricht_eine_loopback_adresse_an():
    """Der konkrete Bruch des Kanarienvogels, als Gate.

    Im Wartungs-Container zeigt ``localhost`` auf den Container selbst.
    Ein Werkzeug, das von dort das Backend sucht, muss den
    Compose-Servicenamen benutzen. Geprueft wird der DEFAULT im Code,
    nicht ein Beispiel im Docstring — die erste Fassung dieses Gates
    fiel genau darauf herein und meldete zwei unbeteiligte Werkzeuge
    (``stress_test.py``, ``profile_latency.py``), deren Defaults auf
    ``https://evidora.eu`` zeigen.
    """
    gerufen = sorted(set(re.findall(
        r"run_evidora_tool\.sh\s+([a-z_0-9]+\.py)", _crontab())))
    assert gerufen, "Keine Werkzeug-Aufrufe gefunden"
    schlecht = []
    for name in gerufen:
        for stelle in _loopback_defaults(os.path.join(TOOLS, name)):
            schlecht.append((name, stelle))
    assert not schlecht, (
        f"Werkzeug(e) im Cron mit Loopback-Default: {schlecht}")


# --- Der Exit-Code-Vertrag ---------------------------------------------

#: Werkzeuge, die einen BEFUND von "kam nicht zum Laufen" unterscheiden,
#: je mit den Exit-Codes, die einen Befund bedeuten. Das Werkzeug hat in
#: diesen Faellen den Push selbst geschickt — der Cron darf keinen
#: zweiten schicken, sonst klingelt es doppelt und keiner der beiden
#: Texte ist der genauere.
BEFUND_CODES = {
    "quellen_lebenszeichen.py": ("2",),
    "canary_check.py": ("1", "3"),
}


@pytest.mark.parametrize("werkzeug,codes", sorted(BEFUND_CODES.items()))
def test_cron_trennt_befund_von_nicht_gelaufen(werkzeug, codes):
    """Lehre aus #236, zweiter Fall.

    Der Kanarienvogel benutzt Exit 1 fuer "Pipeline defekt" — ein
    BEFUND. Der Cron-Zweig ``|| { … "kam nicht zum Laufen" … }`` zog
    daraus den Schluss, das Werkzeug sei nicht gestartet. Sechs Tage
    lang stand die falsche Ursache im Alarm.
    """
    zeile = next((z for z in _zeilen() if werkzeug in z), None)
    assert zeile, f"{werkzeug} steht nicht im Cron"
    # Verboten ist die KOMMANDO-Form ``… || { … }``: sie macht aus jedem
    # Exit != 0 denselben Zweig. Das ``||`` INNERHALB einer Bedingung
    # (``[ "$rc" = 1 ] || [ "$rc" = 3 ]``) ist dagegen genau das, was wir
    # wollen — eine erste Fassung dieses Tests verbot beides und schlug
    # auf der richtigen Zeile an.
    assert "|| {" not in zeile, (
        f"{werkzeug}: `|| {{` macht aus jedem Exit != 0 ein "
        f"'kam nicht zum Laufen' — Befunde brauchen einen eigenen Zweig")
    for code in codes:
        assert f'"$rc" = {code}' in zeile, (
            f"{werkzeug}: Exit {code} ist ein Befund, der Cron "
            f"behandelt ihn nicht gesondert")


def test_bei_einem_befund_pusht_nur_das_werkzeug():
    """Zwei Pushes fuer einen Vorfall sind kein doppelter Schutz.

    Am 13.-18.10. kamen jeden Morgen zwei: "Pipeline liefert kein
    Ergebnis" vom Werkzeug und "Backend-Container down?" vom Cron. Der
    zweite war falsch und lauter.
    """
    for werkzeug, codes in BEFUND_CODES.items():
        zeile = next(z for z in _zeilen() if werkzeug in z)
        # Der Befund-Zweig steht vor dem elif; alles ab "elif" gehoert
        # zum Nicht-Gelaufen-Fall und darf pushen.
        befund_teil = zeile.split("elif")[0]
        assert "curl" not in befund_teil, (
            f"{werkzeug}: der Cron pusht schon beim Befund — das "
            f"Werkzeug hat da bereits gepusht")


# --- Die Nachricht selbst ----------------------------------------------

def test_der_alarm_behauptet_keine_ursache_die_er_nicht_geprueft_hat():
    """Der eigentliche Fehler steckte im TEXT, nicht in der Logik.

    "Backend-Container down?" war eine Behauptung ueber die Ursache, und
    sie war falsch: der Container lief gesund. Je Alarmweg ein Test, der
    die Nachricht BAUT (#236).
    """
    import sys
    sys.path.insert(0, TOOLS)
    import canary_check as cc

    titel, msg = cc.alarm_text(
        cc.STANDARD_URL, "antwortet_nicht",
        "URLError: <urlopen error [Errno 111] Connection refused>",
        False, "nicht versucht")
    assert cc.STANDARD_URL in msg, "Die geprueffte Adresse fehlt"
    assert "nicht erreichbar" in titel.lower()
    # Beide Erklaerungen genannt, keine behauptet.
    assert "laeuft nicht" in msg and "Adresse" in msg
    assert "down?" not in msg

    titel2, msg2 = cc.alarm_text(
        cc.STANDARD_URL, "ok", "ready (ok)", False, "kein Verdict-Frame im SSE")
    assert "antwortet" in msg2, (
        "Der Pipeline-Alarm muss sagen, dass das Backend ERREICHBAR war "
        "— sonst ist er vom Erreichbarkeits-Alarm nicht zu unterscheiden")
    assert titel != titel2, "Zwei Lagen, zwei Titel"


def test_die_adresse_des_kanarienvogels_ist_der_compose_service():
    import sys
    sys.path.insert(0, TOOLS)
    import canary_check as cc
    assert cc.STANDARD_URL == "http://backend:8000"
    assert "backend" in _compose_services()


def test_erreichbarkeit_und_schlechte_antwort_sind_zwei_lagen():
    """``HTTPError`` heisst: der Dienst hat geantwortet. Die alte Fassung
    warf ihn mit ``URLError`` in ein gemeinsames ``False``."""
    import sys
    sys.path.insert(0, TOOLS)
    import canary_check as cc
    import urllib.error
    assert issubclass(urllib.error.HTTPError, urllib.error.URLError)
    assert urllib.error.URLError in cc.NICHT_ERREICHBAR
    quelle = open(os.path.join(TOOLS, "canary_check.py"),
                  encoding="utf-8").read()
    i_http = quelle.index("except urllib.error.HTTPError")
    i_nicht = quelle.index("except NICHT_ERREICHBAR")
    assert i_http < i_nicht, (
        "HTTPError muss VOR NICHT_ERREICHBAR gefangen werden, sonst "
        "gilt eine Antwort mit Fehlerstatus als 'niemand da'")
