"""Die Belege waren nie kaputt — wir haben falsch angeklopft.

QA50F, Klasse E: sechs von fünfzig Claims bekamen ein bestimmtes Verdict ohne
einen einzigen Beleg. In #166 habe ich daraus geschlossen, „das Modell füllt
das Feld gar nicht" — **das war falsch**, und es liess sich damals nicht
prüfen, weil beide Filter nur loggen, wenn sie etwas wegwerfen. Ein leeres
Ergebnis vom Modell und ein vollständig weggefiltertes sehen im Log gleich
aus: still.

Die Beobachtbarkeit aus #168 hat es entschieden. Sechs Live-Claims:

    Modell 1 -> Halluzinations-Filter 1 -> URL-Prüfung 0   (dreimal)
    Modell 2 -> Halluzinations-Filter 2 -> URL-Prüfung 2   (dreimal)

**Das Modell liefert Evidenz.** Der Halluzinations-Filter lässt sie durch.
Die URL-Prüfung wirft sie weg — und zwar in genau den drei Fällen, die ohne
Beleg endeten.

ZWEI URSACHEN, BEIDE AUF UNSERER SEITE
======================================
    Wikipedia   HEAD 403  ->  mit höflichem User-Agent 200
    Frontex     HEAD 405  ->  GET 200 (der Server verweigert nur HEAD)

Der Prüfer nutzte einen nackten `httpx.AsyncClient` ohne den projektweiten
User-Agent aus `_http_polite` — obwohl genau dieser Header existiert, damit
Wikipedia & Co. uns nicht abweisen. Und er kannte nur HEAD, das viele Server
mit „Method Not Allowed" beantworten.
"""

import asyncio
import sys
import warnings
from pathlib import Path

import pytest

warnings.filterwarnings("ignore")
BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))

from services.synthesizer import _validate_urls  # noqa: E402


class _Antwort:
    def __init__(self, code):
        self.status_code = code


class _Klient:
    """Zeichnet auf, welche Methoden der Pruefer benutzt."""

    def __init__(self, head_codes, get_codes=None):
        self.head_codes = head_codes
        self.get_codes = get_codes or {}
        self.aufrufe = []

    async def head(self, url, **kw):
        self.aufrufe.append(("HEAD", url))
        return _Antwort(self.head_codes.get(url, 200))

    async def get(self, url, **kw):
        self.aufrufe.append(("GET", url, kw.get("headers", {})))
        return _Antwort(self.get_codes.get(url, 200))

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False


def _lauf(evidence, klient, monkeypatch):
    import services.synthesizer as syn
    monkeypatch.setattr(syn, "polite_client", lambda **kw: klient)
    return asyncio.run(_validate_urls(evidence))


# --------------------------------------------------------------------------
# Die beiden gemessenen Ursachen
# --------------------------------------------------------------------------

def test_server_der_head_verweigert_wird_per_get_geprueft(monkeypatch):
    """Frontex antwortet auf HEAD mit 405 und auf GET mit 200. Der Beleg ist
    einwandfrei — nur die Methode war falsch."""
    url = "https://www.frontex.europa.eu/media-centre/news/news-release/x"
    k = _Klient(head_codes={url: 405}, get_codes={url: 200})
    r = _lauf([{"source": "Frontex", "url": url}], k, monkeypatch)
    assert len(r) == 1
    assert ("HEAD", url) in k.aufrufe
    assert any(a[0] == "GET" for a in k.aufrufe), "GET-Nachpruefung fehlt"


@pytest.mark.parametrize("code", [403, 405, 501])
def test_alle_drei_verweigerungs_codes_werden_nachgeprueft(monkeypatch, code):
    """403 (gezielt geblockt), 405 (Method Not Allowed), 501 (nicht
    implementiert) — alle drei sagen etwas über die METHODE, nicht über den
    Link."""
    url = "https://beispiel.test/seite"
    k = _Klient(head_codes={url: code}, get_codes={url: 200})
    assert len(_lauf([{"source": "X", "url": url}], k, monkeypatch)) == 1


def test_get_nachpruefung_zieht_nicht_die_ganze_seite(monkeypatch):
    """Range-Header: ein GET auf eine grosse Seite fuer eine Statuszeile wäre
    Verschwendung — und bei 50 Belegen am Tag spürbar."""
    url = "https://beispiel.test/gross"
    k = _Klient(head_codes={url: 405}, get_codes={url: 206})
    _lauf([{"source": "X", "url": url}], k, monkeypatch)
    get = [a for a in k.aufrufe if a[0] == "GET"][0]
    assert get[2].get("Range") == "bytes=0-0"


def test_hoeflicher_client_wird_benutzt():
    """Wikipedia gibt einem nackten Client 403 und dem projektweiten
    User-Agent 200. Der Header existiert genau dafür."""
    quelle = (BACKEND / "services" / "synthesizer.py").read_text(encoding="utf-8")
    block = quelle[quelle.index("async def _validate_urls"):
                   quelle.index("def _synth_result_complete")]
    assert "polite_client(" in block
    assert "httpx.AsyncClient(" not in block, (
        "nackter Client im Validator — genau das war die Ursache")


# --------------------------------------------------------------------------
# Was weiterhin verworfen werden muss
# --------------------------------------------------------------------------

def test_echter_toter_link_fliegt_raus(monkeypatch):
    url = "https://beispiel.test/gibt-es-nicht"
    k = _Klient(head_codes={url: 404}, get_codes={url: 404})
    assert _lauf([{"source": "X", "url": url}], k, monkeypatch) == []


def test_bei_404_wird_nicht_nachgeprueft(monkeypatch):
    """Ein 404 sagt etwas über den LINK, nicht über die Methode — eine
    GET-Nachprüfung wäre ein zweiter Fehlschlag und kostet nur Zeit."""
    url = "https://beispiel.test/weg"
    k = _Klient(head_codes={url: 404})
    _lauf([{"source": "X", "url": url}], k, monkeypatch)
    assert not any(a[0] == "GET" for a in k.aufrufe)


def test_eintrag_ohne_url_bleibt(monkeypatch):
    """Manche Pack-Treffer haben keine URL. Ihr Befund ist trotzdem gültig."""
    k = _Klient(head_codes={})
    r = _lauf([{"source": "Pack", "finding": "X", "url": ""}], k, monkeypatch)
    assert len(r) == 1


def test_doi_bleibt_ungeprueft(monkeypatch):
    """DOI-Links lösen im Browser fast immer auf, auch wenn HEAD blockt —
    diese Ausnahme gab es schon vorher und bleibt."""
    url = "https://doi.org/10.1038/nature12373"
    k = _Klient(head_codes={url: 403})
    assert len(_lauf([{"source": "X", "url": url}], k, monkeypatch)) == 1
    assert not k.aufrufe, "DOI darf gar nicht abgefragt werden"


def test_leere_evidenz_bleibt_leer(monkeypatch):
    k = _Klient(head_codes={})
    assert _lauf([], k, monkeypatch) == []


# --------------------------------------------------------------------------
# Der Grund muss im Log stehen
# --------------------------------------------------------------------------

def test_grund_der_verwerfung_steht_im_log(monkeypatch, caplog):
    """Ein blosses „removed" ist nicht diagnostizierbar: Zeitüberschreitung,
    IP-Sperre und toter Link sehen im Log identisch aus. Nach dem Deploy von
    #169 blieben zwei Belege liegen, und das Log konnte nicht sagen warum —
    dieselbe Lücke wie #168, eine Ebene tiefer."""
    import logging
    url = "https://beispiel.test/weg"
    k = _Klient(head_codes={url: 404})
    with caplog.at_level(logging.INFO, logger="evidora"):
        _lauf([{"source": "X", "url": url}], k, monkeypatch)
    zeilen = [r.getMessage() for r in caplog.records if "Removed broken" in r.getMessage()]
    assert zeilen, "keine Zeile zur verworfenen URL"
    assert "404" in zeilen[0], f"Grund fehlt: {zeilen[0]}"
    assert url in zeilen[0]


def test_grund_nennt_beide_stufen(monkeypatch, caplog):
    """Bei einer Nachprüfung sind zwei Codes im Spiel — nur beide zusammen
    unterscheiden „Server mag HEAD nicht" von „Server sperrt uns aus"."""
    import logging
    url = "https://beispiel.test/gesperrt"
    k = _Klient(head_codes={url: 403}, get_codes={url: 403})
    with caplog.at_level(logging.INFO, logger="evidora"):
        _lauf([{"source": "X", "url": url}], k, monkeypatch)
    zeile = [r.getMessage() for r in caplog.records if "Removed broken" in r.getMessage()][0]
    assert "HEAD 403" in zeile and "GET 403" in zeile, zeile


def test_grund_nennt_die_ausnahme(monkeypatch, caplog):
    """Eine Zeitüberschreitung ist kein kaputter Link. Wenn wir das nicht
    unterscheiden, suchen wir den Fehler beim Server statt bei uns."""
    import logging

    class Kaputt(_Klient):
        async def head(self, url, **kw):
            raise TimeoutError("zu langsam")

    url = "https://beispiel.test/langsam"
    with caplog.at_level(logging.INFO, logger="evidora"):
        _lauf([{"source": "X", "url": url}], Kaputt(head_codes={}), monkeypatch)
    zeile = [r.getMessage() for r in caplog.records if "Removed broken" in r.getMessage()][0]
    assert "TimeoutError" in zeile, zeile
