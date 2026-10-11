"""Ein toter Konnektor muss im Log des Dienstes auffallen (#238, 11.10.2026).

DOAB war seit dem 24.9. tot. Gesehen hat das nur die tägliche Sonde — im
Backend-Log stand nichts, weil der Fehlerpfad auf `debug` loggte und eine
leere Liste zurückgab. Für den Aufrufer ist das von „diese Quelle hat zu
dem Claim nichts" nicht zu unterscheiden, und in Produktion läuft das Log
auf INFO: die Zeile existierte dort gar nicht.

Der Zensus über alle 100 Konnektoren mit Sonde fand 77 Handler um den
Abruf — 49 auf `debug`, 7 auf `info`, 20 auf `warning`, einen stumm. In
**37 Diensten** ging kein Abruf-Handler über `debug` hinaus.

Dieser Test führt den Zensus als Gate: er prüft nicht die 37 von damals,
sondern **jeden** Konnektor mit Sonde, auch künftige. Ohne Ausnahmeliste —
die Zusage im Datenfeld ist ungeprüft, bis ein Gate sie prüft (#230).

Was NICHT lauter wurde, und warum:
  * **Parse-Handler** (`json.JSONDecodeError`, `KeyError`, …). „Ein
    Datensatz war kaputt" ist kein toter Upstream.
  * **Handler, die schon auf `info` oder `warning` loggen.** Die sind in
    Produktion sichtbar; sie durch den Helfer zu ziehen wäre Churn.
  * **`oenb_sdmx._fetch_oenb_direct_ping`.** Der gibt `False` zurück, nicht
    leere Daten — und hat gar keinen Aufrufer. Toter Code, kein Log-Problem.
"""

import ast
import importlib.util
import re
import sys
from pathlib import Path

import pytest

BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))

from services._ausfall import (  # noqa: E402
    WIEDERHOLUNG_S, _zuruecksetzen, melde_ausfall,
)

_spec = importlib.util.spec_from_file_location(
    "quellen_lebenszeichen", BACKEND / "tools" / "quellen_lebenszeichen.py")
QM = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(QM)

DIENSTE_MIT_SONDE = {s[1] for s in QM.SONDEN}

# Ein Abruf steht im try-Block, wenn einer dieser Aufrufe darin vorkommt.
ABRUF = ("client.get", "client.post", "httpx.get", "httpx.post", "urlopen",
         "client.request", "_get_json", "polite_client", "client.stream",
         "session.get", "requests.get")
# Diese Ausnahmen beschreiben kaputte DATEN, nicht eine kaputte Quelle.
PARSE_TYPEN = {"JSONDecodeError", "KeyError", "ValueError", "TypeError",
               "IndexError", "AttributeError", "UnicodeDecodeError"}
# Leere DATEN-Container. `False`/`0` sind Entscheidungswerte, keine Daten —
# dort ist der Fehlschlag der Rückgabewert und der Aufrufer entscheidet.
LEERE_DATEN = ("[]", "None", "{}", "()")


def _leere_daten(k) -> bool:
    if not isinstance(k, ast.Return):
        return False
    return ast.unparse(k.value) in LEERE_DATEN if k.value else True


def _log_stufen(h) -> list[str]:
    return [x.func.attr for x in ast.walk(h)
            if isinstance(x, ast.Call) and isinstance(x.func, ast.Attribute)
            and isinstance(x.func.value, ast.Name)
            and x.func.value.id in ("logger", "log", "logging")]


def _ausfall_namen(h) -> list[str]:
    """Die Dienst-Namen, mit denen `melde_ausfall` hier gerufen wird."""
    raus = []
    for x in ast.walk(h):
        if (isinstance(x, ast.Call) and isinstance(x.func, ast.Name)
                and x.func.id == "melde_ausfall" and len(x.args) >= 2
                and isinstance(x.args[1], ast.Constant)):
            raus.append(x.args[1].value)
    return raus


def _typen(h) -> set[str]:
    if h.type is None:
        return {"<bare>"}
    teile = h.type.elts if isinstance(h.type, ast.Tuple) else [h.type]
    return {ast.unparse(t).split(".")[-1] for t in teile}


def abruf_handler(quelle: str):
    """Alle Handler, die einen ABRUF-Fehlschlag in leere Daten verwandeln."""
    baum = ast.parse(quelle)
    for versuch in ast.walk(baum):
        if not isinstance(versuch, ast.Try):
            continue
        rumpf = "\n".join(ast.unparse(x) for x in versuch.body)
        if not any(a in rumpf for a in ABRUF):
            continue
        for h in versuch.handlers:
            if not any(_leere_daten(x) for x in ast.walk(h)):
                continue
            t = _typen(h)
            if t and t <= PARSE_TYPEN:
                continue
            yield h


def _module_mit_sonde():
    for pfad in sorted((BACKEND / "services").glob("*.py")):
        if pfad.stem in DIENSTE_MIT_SONDE:
            yield pfad


# --------------------------------------------------------------------------
# Das Gate
# --------------------------------------------------------------------------

def test_kein_konnektor_verschluckt_einen_ausfall_auf_debug():
    """Über ALLE Konnektoren mit Sonde, ohne Ausnahmeliste."""
    stumm = []
    for pfad in _module_mit_sonde():
        quelle = pfad.read_text(encoding="utf-8")
        for h in abruf_handler(quelle):
            stufen = set(_log_stufen(h))
            if _ausfall_namen(h):
                continue                      # geht über den Ausfall-Kanal
            if stufen & {"info", "warning", "error", "exception"}:
                continue                      # in Produktion sichtbar
            stumm.append(f"{pfad.name}:{h.lineno} stufen={sorted(stufen)}")
    assert not stumm, (
        f"{len(stumm)} Abruf-Fehlschläge wären im Prod-Log unsichtbar: "
        + ", ".join(stumm))


def test_der_zensus_findet_den_anlass_noch():
    """Gift-Probe für den Detektor selbst.

    Ein Gate, das nichts findet, weil sein Detektor nichts erkennt, ist von
    einem Gate ohne Befunde nicht zu unterscheiden. Hier die Form, die den
    Sweep ausgelöst hat — der Detektor MUSS sie sehen.
    """
    anlass = (
        "import logging\n"
        "logger = logging.getLogger('evidora')\n"
        "async def hole(client, query):\n"
        "    try:\n"
        "        r = await client.get('https://example.test/' + query)\n"
        "        return r.json()\n"
        "    except Exception as e:\n"
        "        logger.debug(f'Dienst fetch failed: {e}')\n"
        "        return []\n"
    )
    treffer = list(abruf_handler(anlass))
    assert len(treffer) == 1
    assert set(_log_stufen(treffer[0])) == {"debug"}
    assert not _ausfall_namen(treffer[0])


def test_parse_handler_blieben_leise():
    """Gegenprobe zur Gift-Probe: der Detektor darf NICHT alles einsammeln.
    Ein kaputter Datensatz ist kein Quellenausfall und muss leise bleiben."""
    parse_fall = (
        "import json, logging\n"
        "logger = logging.getLogger('evidora')\n"
        "async def hole(client):\n"
        "    r = await client.get('https://example.test/')\n"
        "    try:\n"
        "        return json.loads(r.text)\n"
        "    except json.JSONDecodeError as e:\n"
        "        logger.debug(f'kaputter Datensatz: {e}')\n"
        "        return []\n"
    )
    assert list(abruf_handler(parse_fall)) == []


def test_ausfall_wird_mit_dem_eigenen_dienstnamen_gemeldet():
    """Ein kopierter Name zeigt im Log auf den falschen Dienst — und die
    Stunde Stille gilt dann für den falschen."""
    falsch = []
    for pfad in _module_mit_sonde():
        baum = ast.parse(pfad.read_text(encoding="utf-8"))
        for x in ast.walk(baum):
            if (isinstance(x, ast.Call) and isinstance(x.func, ast.Name)
                    and x.func.id == "melde_ausfall"):
                namen = [a.value for a in x.args[1:2]
                         if isinstance(a, ast.Constant)]
                if namen and namen[0] != pfad.stem:
                    falsch.append(f"{pfad.name}:{x.lineno} -> {namen[0]}")
    assert not falsch, falsch


def test_jeder_gemeldete_dienstname_hat_eine_sonde():
    """Der Name im Log soll den Bogen zur Sonden-Liste schlagen."""
    for pfad in _module_mit_sonde():
        baum = ast.parse(pfad.read_text(encoding="utf-8"))
        for x in ast.walk(baum):
            if (isinstance(x, ast.Call) and isinstance(x.func, ast.Name)
                    and x.func.id == "melde_ausfall" and len(x.args) >= 2
                    and isinstance(x.args[1], ast.Constant)):
                assert x.args[1].value in DIENSTE_MIT_SONDE, (
                    pfad.name, x.args[1].value)


# --------------------------------------------------------------------------
# Der Helfer
# --------------------------------------------------------------------------

class _Mitschrift:
    def __init__(self):
        self.warnungen, self.debugs = [], []

    def warning(self, m):
        self.warnungen.append(m)

    def debug(self, m):
        self.debugs.append(m)


@pytest.fixture(autouse=True)
def _frischer_stand():
    _zuruecksetzen()
    yield
    _zuruecksetzen()


def test_der_erste_ausfall_ist_laut():
    log = _Mitschrift()
    melde_ausfall(log, "doab", "DOAB fetch failed: 403")
    assert log.warnungen == ["DOAB fetch failed: 403"]
    assert log.debugs == []


def test_die_folgenden_bleiben_leise():
    """Sonst schreibt eine Quelle, die bei jedem Claim gefragt wird, bei
    jedem Claim eine Warnung — der Wecker, den man abstellt (#130, #236)."""
    log = _Mitschrift()
    for _ in range(50):
        melde_ausfall(log, "doab", "DOAB fetch failed: 403")
    assert len(log.warnungen) == 1
    assert len(log.debugs) == 49


def test_zwei_dienste_zaehlen_getrennt():
    log = _Mitschrift()
    melde_ausfall(log, "doab", "a")
    melde_ausfall(log, "getty", "b")
    assert log.warnungen == ["a", "b"]


def test_nach_der_stille_wird_wieder_gewarnt_mit_zaehler(monkeypatch):
    """„Einmal gestolpert" muss von „seit Stunden tot" unterscheidbar sein."""
    import services._ausfall as A
    uhr = {"t": 1000.0}
    monkeypatch.setattr(A.time, "monotonic", lambda: uhr["t"])
    log = _Mitschrift()
    melde_ausfall(log, "doab", "tot")
    for _ in range(4):
        melde_ausfall(log, "doab", "tot")
    uhr["t"] += WIEDERHOLUNG_S + 1
    melde_ausfall(log, "doab", "tot")
    assert len(log.warnungen) == 2
    assert "+4 weitere" in log.warnungen[1]


def test_der_text_des_konnektors_bleibt_unveraendert():
    """Der Sweep hat den Kanal gewechselt, nicht die Meldung. Der Kontext
    darin — Query, Land, URL — ist beim Nachsehen genau das Gesuchte."""
    log = _Mitschrift()
    melde_ausfall(log, "crossref", "Crossref DOI fetch failed for '10.x': E")
    assert log.warnungen[0] == "Crossref DOI fetch failed for '10.x': E"


# --------------------------------------------------------------------------
# 5. Die zweite Form — Statuscode-Zweige (#241, 11.10.2026)
# --------------------------------------------------------------------------
# Der erste Sweep sah nur `ast.ExceptHandler`. `httpx` wirft bei 4xx/5xx
# aber nur, wenn man `raise_for_status()` ruft — sonst prüft der Konnektor
# selbst:
#
#     if resp.status_code != 200:
#         logger.debug(...)
#         return []
#
# Genau diesen Zweig nimmt DOAB mit seinem 403. Aufgefallen ist die Lücke
# erst bei der Live-Probe nach dem Deploy von #238: ein Claim, der DOAB
# triggert, erzeugte KEINE Warnung — obwohl DOAB nachweislich tot ist und
# die Quelle laut `source_coverage` abgefragt wurde. Der Zensus hatte 77
# Handler gezählt und die 73 Statuscode-Zweige daneben nicht gesehen.
#
# Lehre für dieses Gate: ein Detektor, der nur eine Gestalt des
# Fehlerpfades kennt, ist genauso blind wie der Sweep, der ihn benutzt.

from services._ausfall import (  # noqa: E402
    NICHT_GEFUNDEN, melde_http_ausfall,
)

# "Gibt es nicht" ist kein Ausfall: diese Zweige dürfen leise bleiben.
_NUR_NICHT_GEFUNDEN = re.compile(
    r"^[\w.]+\.status_code\s*(?:==\s*(?:404|410)"
    r"|in\s*[({\[]\s*(?:404|410)\s*(?:,\s*(?:404|410)\s*)*,?\s*[)}\]])$")


def _status_ausdruck(test) -> str | None:
    """Der `<resp>.status_code`-Zugriff im Test, falls einer drin ist.

    Bewusst nur `.status_code`: `data.get("status") != "ok"` ist ein
    NUTZLAST-Status und eine andere Klasse (HTTP 200 mit Fehler-Objekt im
    Rumpf — der WGI-Fall aus #107). Der gehört eigens gemessen, nicht hier
    mitgeschleift.
    """
    for k in ast.walk(test):
        if isinstance(k, ast.Attribute) and k.attr == "status_code":
            return ast.unparse(k)
    return None


def status_zweige(quelle: str):
    """Alle `if`-Zweige, die einen HTTP-Status in leere Daten verwandeln."""
    for k in ast.walk(ast.parse(quelle)):
        if not isinstance(k, ast.If):
            continue
        if _status_ausdruck(k.test) is None:
            continue
        if _NUR_NICHT_GEFUNDEN.match(ast.unparse(k.test).strip()):
            continue
        if any(_leere_daten(x) for b in k.body for x in ast.walk(b)):
            yield k


def _zweig_stufen(k) -> set[str]:
    return {lvl for b in k.body for lvl in _log_stufen(b)}


def _zweig_meldet(k) -> bool:
    return any(isinstance(x, ast.Call) and isinstance(x.func, ast.Name)
               and x.func.id in ("melde_ausfall", "melde_http_ausfall")
               for b in k.body for x in ast.walk(b))


def test_kein_konnektor_verschluckt_einen_statuscode_auf_debug():
    """Das Gate für die Form, die #238 übersehen hat."""
    stumm = []
    for pfad in _module_mit_sonde():
        for k in status_zweige(pfad.read_text(encoding="utf-8")):
            if _zweig_meldet(k):
                continue
            if _zweig_stufen(k) & {"info", "warning", "error", "exception"}:
                continue
            stumm.append(f"{pfad.name}:{k.lineno}")
    assert not stumm, (
        f"{len(stumm)} Statuscode-Ausfälle wären im Prod-Log unsichtbar: "
        + ", ".join(stumm))


def test_der_detektor_sieht_den_doab_fall():
    """Gift-Probe mit genau der Form, die 18 Tage unsichtbar blieb."""
    doab_form = (
        "import logging\n"
        "logger = logging.getLogger('evidora')\n"
        "async def hole(client, query):\n"
        "    resp = await client.get('https://example.test/' + query)\n"
        "    if resp.status_code != 200:\n"
        "        logger.debug(f'DOAB HTTP {resp.status_code}')\n"
        "        return []\n"
        "    return resp.json()\n"
    )
    treffer = list(status_zweige(doab_form))
    assert len(treffer) == 1
    assert _zweig_stufen(treffer[0]) == {"debug"}
    assert not _zweig_meldet(treffer[0])


def test_der_detektor_laesst_nicht_gefunden_in_ruhe():
    """Gegenprobe: ein 404-Zweig ist Datenabwesenheit, kein Ausfall. Würde
    er warnen, wäre der Kanal nach einer Stunde unbrauchbar — Wikipedia
    antwortet auf jede unbekannte Seite mit 404."""
    nicht_gefunden = (
        "import logging\n"
        "logger = logging.getLogger('evidora')\n"
        "async def hole(client, doi):\n"
        "    resp = await client.get('https://example.test/' + doi)\n"
        "    if resp.status_code == 404:\n"
        "        logger.debug('unbekannte DOI')\n"
        "        return []\n"
        "    return resp.json()\n"
    )
    assert list(status_zweige(nicht_gefunden)) == []


def test_der_detektor_laesst_nutzlast_status_in_ruhe():
    """`data.get("status") != "ok"` ist HTTP 200 mit Fehler-Objekt im Rumpf —
    der WGI-Fall aus #107. Eigene Klasse, eigene Messung."""
    nutzlast = (
        "import logging\n"
        "logger = logging.getLogger('evidora')\n"
        "async def hole(client):\n"
        "    resp = await client.get('https://example.test/')\n"
        "    data = resp.json()\n"
        "    if data.get('status') != 'ok':\n"
        "        logger.debug('nicht ok')\n"
        "        return []\n"
        "    return data\n"
    )
    assert list(status_zweige(nutzlast)) == []


def test_nicht_gefunden_bleibt_leise():
    log = _Mitschrift()
    for code in sorted(NICHT_GEFUNDEN):
        melde_http_ausfall(log, "wikipedia", code, f"HTTP {code}")
    assert log.warnungen == []
    assert len(log.debugs) == len(NICHT_GEFUNDEN)


@pytest.mark.parametrize("code", [403, 401, 500, 503, 429])
def test_echte_ausfaelle_werden_laut(code):
    log = _Mitschrift()
    melde_http_ausfall(log, "doab", code, f"DOAB HTTP {code}")
    assert log.warnungen == [f"DOAB HTTP {code}"]


def test_unlesbarer_status_wird_wie_ein_ausfall_behandelt():
    """Lieber eine Warnung zu viel als ein verschluckter Ausfall, wenn der
    Statuscode nicht als Zahl ankommt."""
    log = _Mitschrift()
    melde_http_ausfall(log, "doab", None, "kein Status")
    melde_http_ausfall(log, "getty", "kaputt", "kein Status")
    assert len(log.warnungen) == 2


# --------------------------------------------------------------------------
# 6. Die dritte Form — HTTP 200 mit Fehler im Rumpf (#242, 11.10.2026)
# --------------------------------------------------------------------------
# Die WGI-Blindheit aus #107: Die Weltbank quittierte eine kaputte Abfrage
# mit HTTP 200 und einem Fehler-OBJEKT im Rumpf. `raise_for_status` greift
# nicht, der Statuscode-Zweig greift nicht — der Konnektor cachte still eine
# leere Liste. So lag WGI von Mai bis September 2026 tot.
#
# `services/wgi.py` ist seither das VORBILD, nicht der Befund: dort steht
# die Prüfung mit einer lauten, erklärenden Meldung („Das ist kein leeres
# Ergebnis, sondern eine kaputte Abfrage"), und erst DANACH der stille
# Zweig für das tatsächlich leere Ergebnis. Dieses Gate muss diese Reihenfolge
# zulassen — sonst zwingt es zu Warnungen auf leere Trefferlisten.

from services._ausfall import melde_kaputte_nutzlast  # noqa: E402

_NUTZLAST_MARKEN = ("isinstance", "error", "errors", "status", "success",
                    "fault", "detail")
_LAUT = {"info", "warning", "error", "exception"}


def _json_namen(fn) -> set[str]:
    """Namen, die in dieser Funktion aus `.json()` / `json.loads` stammen.

    Die Abgrenzung zur zweiten Form: eine Formprüfung auf EINEN Datensatz
    in einer Schleife (`not isinstance(eintrag, dict)`) ist Robustheit pro
    Datensatz — dieselbe Klasse wie die Parse-Handler, die #238 bewusst
    leise gelassen hat. Erst die Prüfung der GANZEN Antwort heißt: die
    Quelle liefert etwas, das wir nicht lesen können.
    """
    namen = set()
    for k in ast.walk(fn):
        if not isinstance(k, (ast.Assign, ast.AnnAssign)):
            continue
        if k.value is None:
            continue
        q = ast.unparse(k.value)
        if ".json()" not in q and "json.loads" not in q:
            continue
        ziele = k.targets if isinstance(k, ast.Assign) else [k.target]
        namen |= {z.id for z in ziele if isinstance(z, ast.Name)}
    return namen


def _laut(k) -> bool:
    if any(isinstance(x, ast.Call) and isinstance(x.func, ast.Name)
           and x.func.id.startswith("melde_")
           for b in k.body for x in ast.walk(b)):
        return True
    return bool({lvl for b in k.body for lvl in _log_stufen(b)} & _LAUT)


def nutzlast_zweige(quelle: str):
    """`if`-Zweige, die eine FEHLERHAFTE oberste Antwort in Leeres wandeln."""
    for fn in ast.walk(ast.parse(quelle)):
        if not isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        oberste = _json_namen(fn)
        if not oberste:
            continue
        schon_laut: set[str] = set()
        # NACH ZEILE sortiert, nicht in `ast.walk`-Reihenfolge: die Regel
        # "laute Prüfung zuerst" hängt an der Quellordnung, und `ast.walk`
        # geht in die Breite. Bei wgi.py liegen beide `if` auf derselben
        # Ebene und die Reihenfolge stimmte zufällig — verschachtelt wäre
        # sie falsch gewesen.
        ifs = sorted((k for k in ast.walk(fn) if isinstance(k, ast.If)),
                     key=lambda k: k.lineno)
        for k in ifs:
            test = ast.unparse(k.test)
            if "status_code" in test:
                continue            # zweite Form, siehe #241
            betroffen = {n.id for n in ast.walk(k.test)
                         if isinstance(n, ast.Name)} & oberste
            if not betroffen:
                continue
            if _laut(k):
                # Ab hier ist für diese Variable eine laute Prüfung
                # vorhanden — ein folgender stiller Zweig ist der
                # Leer-Fall (Vorbild wgi.py).
                schon_laut |= betroffen
                continue
            if betroffen & schon_laut:
                continue
            if not any(m in test.lower() for m in _NUTZLAST_MARKEN):
                continue            # reine Leer-Prüfung: `not data`
            if not any(_leere_daten(x) for b in k.body for x in ast.walk(b)):
                continue
            yield k


def test_kein_konnektor_verschluckt_einen_fehler_im_rumpf():
    """Das Gate für die Form, die WGI vier Monate lang unsichtbar machte."""
    stumm = []
    for pfad in _module_mit_sonde():
        for k in nutzlast_zweige(pfad.read_text(encoding="utf-8")):
            stumm.append(f"{pfad.name}:{k.lineno}")
    assert not stumm, (
        f"{len(stumm)} Fehler-Nutzlasten wären im Prod-Log unsichtbar: "
        + ", ".join(stumm))


def test_der_detektor_sieht_den_crossref_fall():
    """Gift-Probe: Crossref prüfte zweimal `status != "ok"` und loggte
    dabei GAR NICHTS."""
    crossref_form = (
        "import logging\n"
        "logger = logging.getLogger('evidora')\n"
        "async def hole(client, doi):\n"
        "    resp = await client.get('https://example.test/' + doi)\n"
        "    data = resp.json()\n"
        "    if data.get('status') != 'ok':\n"
        "        return None\n"
        "    return data.get('message')\n"
    )
    assert len(list(nutzlast_zweige(crossref_form))) == 1


def test_die_reihenfolge_entscheidet_nicht_die_baum_traversierung():
    """Die Ausnahme gilt nur für Zweige NACH der lauten Prüfung.

    Steht der stille Zweig VOR der lauten, ist er nicht gedeckt — sonst
    könnte ein Konnektor das Gate mit einer einzigen lauten Prüfung
    irgendwo in der Funktion stillstellen.
    """
    falsche_reihenfolge = (
        "import logging\n"
        "logger = logging.getLogger('evidora')\n"
        "async def hole(client):\n"
        "    resp = await client.get('https://example.test/')\n"
        "    data = resp.json()\n"
        "    if not isinstance(data, list):\n"
        "        return []\n"
        "    if 'error' in data:\n"
        "        logger.warning('API-Fehler')\n"
        "        return []\n"
        "    return data\n"
    )
    assert len(list(nutzlast_zweige(falsche_reihenfolge))) == 1


def test_der_detektor_laesst_die_wgi_reihenfolge_zu():
    """Vorbild wgi.py: laute Fehler-Prüfung zuerst, danach der stille
    Leer-Fall. Würde das Gate hier anschlagen, zwänge es zu Warnungen auf
    jede leere Trefferliste."""
    wgi_form = (
        "import logging\n"
        "logger = logging.getLogger('evidora')\n"
        "async def hole(client):\n"
        "    resp = await client.get('https://example.test/')\n"
        "    data = resp.json()\n"
        "    if isinstance(data, list) and data and 'message' in data[0]:\n"
        "        logger.warning('API-FEHLER statt Daten')\n"
        "        return []\n"
        "    if not isinstance(data, list) or len(data) < 2:\n"
        "        return []\n"
        "    return data[1]\n"
    )
    assert list(nutzlast_zweige(wgi_form)) == []


def test_der_detektor_laesst_einzelne_datensaetze_in_ruhe():
    """Eine Formprüfung auf EINEN Datensatz in der Schleife ist Robustheit
    pro Datensatz, kein Quellenbefund (Abgrenzung zu #238)."""
    pro_datensatz = (
        "import logging\n"
        "logger = logging.getLogger('evidora')\n"
        "def forme(eintraege):\n"
        "    out = []\n"
        "    for eintrag in eintraege:\n"
        "        if not isinstance(eintrag, dict):\n"
        "            return []\n"
        "        out.append(eintrag)\n"
        "    return out\n"
    )
    assert list(nutzlast_zweige(pro_datensatz)) == []


def test_der_detektor_laesst_leere_treffer_in_ruhe():
    """`not items` heißt "nichts gefunden" — der Normalfall."""
    leer = (
        "import logging\n"
        "logger = logging.getLogger('evidora')\n"
        "async def hole(client):\n"
        "    resp = await client.get('https://example.test/')\n"
        "    data = resp.json()\n"
        "    if not data:\n"
        "        return []\n"
        "    return data\n"
    )
    assert list(nutzlast_zweige(leer)) == []


def test_leere_nutzlast_bleibt_leise():
    """Viele APIs antworten auf "nichts gefunden" mit `null` oder `[]`. Eine
    Formprüfung schlägt dann an, ohne dass etwas kaputt ist."""
    log = _Mitschrift()
    for leer in (None, [], {}, ""):
        melde_kaputte_nutzlast(log, "doab", "Antwort ist kein list", leer)
    assert log.warnungen == []
    assert len(log.debugs) == 4


def test_unbrauchbare_nutzlast_wird_laut():
    log = _Mitschrift()
    melde_kaputte_nutzlast(log, "doab", "Antwort ist kein list", "<html>…")
    assert len(log.warnungen) == 1


def test_ohne_nutzlast_wird_immer_laut():
    """Wer die Nutzlast nicht mitgibt, meint einen echten Fehler im Rumpf —
    etwa GraphQL-`errors` oder Crossrefs `status != "ok"`."""
    log = _Mitschrift()
    melde_kaputte_nutzlast(log, "crossref", "status='error' statt 'ok'")
    assert len(log.warnungen) == 1


def test_formatwechsel_und_ausfall_zaehlen_getrennt():
    """Zwei verschiedene Probleme derselben Quelle dürfen sich nicht
    gegenseitig in der Stunde Stille verstecken."""
    log = _Mitschrift()
    melde_ausfall(log, "doab", "DOAB HTTP 403")
    melde_kaputte_nutzlast(log, "doab", "DOAB: Antwort ist kein list", "x")
    assert len(log.warnungen) == 2
