"""Token-Verbrauch pro LLM-Call wird geloggt (2026-08-21).

Anlass: Die Frage „was kostet ein Faktencheck" war nur aus dem Mistral-
Abrechnungs-Chart RUECKRECHENBAR (August 2026: 16,55 EUR, daraus geschaetzt
~4,5 Cent/Claim). Im Code wurde `usage` aus der Mistral-Antwort nirgends
ausgewertet — obwohl beide LLM-Pfade (claim_analyzer, synthesizer) einen
Retry-Zweig haben, der den jeweiligen Call verdoppelt. Wie oft der feuert,
war unsichtbar.

Der eigentliche Befund steckte im STREAMING-Pfad: Mistral liefert den
`usage`-Block im LETZTEN Chunk, und der traegt `choices: []`. Die alte
Schleife hatte

    choices = chunk.get("choices") or []
    if not choices:
        continue

VOR jeder usage-Auswertung — sie hat also exakt den Chunk verworfen, der die
Zahlen traegt. Der Synthesizer ist der teure Call (bis zu 16 Quellen x 3
Treffer x 400 Zeichen), also genau der, der blind war.

Diese Suite faehrt beide Pfade gegen einen gemockten HTTP-Transport — kein
Netz, kein Modell, kein API-Key noetig. Getestet wird das VERHALTEN
(erscheint die Log-Zeile mit den richtigen Zahlen), nicht die Formatierung
einer Hilfsfunktion.
"""

import json
import logging
import re

import httpx
import pytest

from services import ollama


# ---------------------------------------------------------------------------
# Hilfen: httpx.AsyncClient so ersetzen, dass er auf einen MockTransport geht
# ---------------------------------------------------------------------------

def _patch_client(monkeypatch, handler):
    """Alle httpx.AsyncClient(...) in ollama.py auf MockTransport umbiegen."""
    real_client = httpx.AsyncClient

    def factory(*args, **kwargs):
        kwargs["transport"] = httpx.MockTransport(handler)
        return real_client(*args, **kwargs)

    monkeypatch.setattr(ollama.httpx, "AsyncClient", factory)
    monkeypatch.setattr(ollama, "MISTRAL_API_KEY", "test-key-not-a-secret")


def _usage_lines(caplog):
    return [r.getMessage() for r in caplog.records
            if r.getMessage().startswith(ollama.USAGE_LOG_PREFIX)]


def _sse(chunks: list[dict]) -> str:
    body = "".join(f"data: {json.dumps(c)}\n\n" for c in chunks)
    return body + "data: [DONE]\n\n"


# ---------------------------------------------------------------------------
# Nicht-streamender Pfad (Analyzer + Synthesizer-Fallback + beide Retries)
# ---------------------------------------------------------------------------

async def test_nicht_streamend_loggt_tokens(monkeypatch, caplog):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={
            "choices": [{"message": {"content": '{"ok": true}'}}],
            "usage": {"prompt_tokens": 12345,
                      "completion_tokens": 456,
                      "total_tokens": 12801},
        })

    _patch_client(monkeypatch, handler)
    with caplog.at_level(logging.INFO, logger="evidora"):
        out = await ollama.chat_completion([{"role": "user", "content": "x"}],
                                           kind="analysis")

    assert out == '{"ok": true}'
    lines = _usage_lines(caplog)
    assert len(lines) == 1, f"genau eine usage-Zeile erwartet: {lines}"
    assert "kind=analysis" in lines[0]
    assert "prompt_tokens=12345" in lines[0]
    assert "completion_tokens=456" in lines[0]
    assert "total_tokens=12801" in lines[0]
    assert "streamed=no" in lines[0]


async def test_retry_zaehlt_als_eigener_call(monkeypatch, caplog):
    """Der Kostenpunkt, der bisher unsichtbar war: ein Retry ist ein zweiter
    bezahlter Call und muss als eigene Zeile erscheinen."""
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={
            "choices": [{"message": {"content": "{}"}}],
            "usage": {"prompt_tokens": 10, "completion_tokens": 2,
                      "total_tokens": 12},
        })

    _patch_client(monkeypatch, handler)
    with caplog.at_level(logging.INFO, logger="evidora"):
        await ollama.chat_completion([{"role": "user", "content": "x"}],
                                     kind="synthesis")
        await ollama.chat_completion([{"role": "user", "content": "x"}],
                                     kind="synthesis_retry")

    lines = _usage_lines(caplog)
    assert len(lines) == 2
    assert "kind=synthesis " in lines[0]
    assert "kind=synthesis_retry " in lines[1]


# ---------------------------------------------------------------------------
# Streaming-Pfad — der eigentliche Befund
# ---------------------------------------------------------------------------

async def test_streaming_faengt_den_usage_chunk_mit_leeren_choices(
        monkeypatch, caplog):
    """DER Gegenbeweis: Mistrals letzter Stream-Chunk hat `choices: []` und
    traegt die Zahlen. Ohne den Fix wurde er uebersprungen."""
    stream = _sse([
        {"choices": [{"delta": {"content": "Hallo"}}]},
        {"choices": [{"delta": {"content": " Welt"}}]},
        # finaler Chunk: keine choices, dafuer usage
        {"choices": [], "usage": {"prompt_tokens": 21000,
                                  "completion_tokens": 480,
                                  "total_tokens": 21480}},
    ])

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text=stream,
                              headers={"Content-Type": "text/event-stream"})

    _patch_client(monkeypatch, handler)
    with caplog.at_level(logging.INFO, logger="evidora"):
        out = await ollama.chat_completion_streaming(
            [{"role": "user", "content": "x"}], kind="synthesis")

    assert out == "Hallo Welt", "Content-Akkumulation darf nicht brechen"
    lines = _usage_lines(caplog)
    assert len(lines) == 1, f"genau eine usage-Zeile erwartet: {lines}"
    assert "prompt_tokens=21000" in lines[0]
    assert "total_tokens=21480" in lines[0]
    assert "streamed=yes" in lines[0]
    assert "kind=synthesis" in lines[0]


async def test_streaming_ohne_usage_block_loggt_unbekannt(monkeypatch, caplog):
    """Wenn Mistral keinen usage-Block schickt, muss das SICHTBAR sein —
    nicht still fehlen. Sonst haelt man eine Luecke faelschlich fuer 0."""
    stream = _sse([{"choices": [{"delta": {"content": "Text"}}]}])

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text=stream,
                              headers={"Content-Type": "text/event-stream"})

    _patch_client(monkeypatch, handler)
    with caplog.at_level(logging.INFO, logger="evidora"):
        await ollama.chat_completion_streaming(
            [{"role": "user", "content": "x"}], kind="synthesis")

    lines = _usage_lines(caplog)
    assert len(lines) == 1
    assert "tokens=unbekannt" in lines[0]


# ---------------------------------------------------------------------------
# Robustheit + Sicherheit
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("payload", [
    None, {}, {"usage": None}, {"usage": "kaputt"}, {"usage": []},
])
def test_extract_usage_ist_defensiv(payload):
    assert ollama._extract_usage(payload) is None


def test_teilweise_usage_stuerzt_nicht_ab(caplog):
    """Fehlende/negative Felder duerfen die Pipeline nie brechen — das Log
    ist Diagnostik, kein Verdict-Pfad."""
    with caplog.at_level(logging.INFO, logger="evidora"):
        ollama._log_usage("synthesis", "m", {"prompt_tokens": 100})
    line = _usage_lines(caplog)[0]
    assert "prompt_tokens=100" in line
    assert "completion_tokens=0" in line
    assert "total_tokens=100" in line, "total faellt auf prompt+completion zurueck"


async def test_log_enthaelt_niemals_prompt_oder_key(monkeypatch, caplog):
    """Lehrgeld 2026-05-24 (CORDIS-Key-Leak): Diagnose-Logs ueberleben ihren
    Use-Case. Diese Zeile darf nur Zaehler tragen — keinen Prompt-Inhalt,
    keinen Key, keinen Auth-Header."""
    geheim = "streng-geheimer-prompt-inhalt"

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={
            "choices": [{"message": {"content": "ok"}}],
            "usage": {"prompt_tokens": 5, "completion_tokens": 1,
                      "total_tokens": 6},
        })

    _patch_client(monkeypatch, handler)
    with caplog.at_level(logging.INFO, logger="evidora"):
        await ollama.chat_completion(
            [{"role": "user", "content": geheim}], kind="analysis")

    line = _usage_lines(caplog)[0]
    assert geheim not in line
    assert "test-key-not-a-secret" not in line
    assert "Bearer" not in line
    # Positiv-Kontrolle: die Zahlen sind da (sonst prueft der Test nichts)
    assert "prompt_tokens=5" in line


# ---------------------------------------------------------------------------
# cached_tokens + prompt_cache_key (30.10.2026)
#
# Anlass: Die Modellwechsel-Abschaetzung. 97 % unserer Tokens sind
# EINGABE, und gecachte Eingabe kostet 10 % — aber das Log zeigte die
# Zahl nicht, also konnte niemand sehen, dass sie durchgehend 0 war.
#
# Gemessen gegen die echte API mit dem deutschen Synthese-System-Prompt
# (14.700 Tokens), vier Aufrufe hintereinander:
#
#     ohne prompt_cache_key   cached = 0, 0, 0  (auch identisch wiederholt)
#     mit  prompt_cache_key   cached = 0, 14.464, 14.592   (98-99 %)
#
# Mistrals Cache ist also OPT-IN. Kosten je Faktencheck: 3,18 -> 0,95
# Cent (-70 %), mehr als ein Modellwechsel braechte.

_USAGE_MIT_DETAILS = {
    "prompt_tokens": 14738,
    "prompt_tokens_details": {"cached_tokens": 14464},
    "completion_tokens": 211,
    "completion_tokens_details": {"reasoning_tokens": 0},
    "total_tokens": 14949,
}


async def test_cached_tokens_stehen_im_log(monkeypatch, caplog):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={
            "choices": [{"message": {"content": "ok"}}],
            "usage": _USAGE_MIT_DETAILS,
        })

    _patch_client(monkeypatch, handler)
    with caplog.at_level(logging.INFO, logger="evidora"):
        await ollama.chat_completion([{"role": "user", "content": "x"}],
                                     kind="synthesis")
    zeile = _usage_lines(caplog)[0]
    assert "cached_tokens=14464" in zeile, zeile
    assert "prompt_tokens=14738" in zeile


async def test_reasoning_tokens_stehen_im_log(monkeypatch, caplog):
    """Heute 0. Bei einem Wechsel auf ein Reasoning-Modell ist das der
    Posten, der sich vervielfacht — gemessen 1.441 statt 0 je
    Analyse-Call bei mistral-large-4."""
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={
            "choices": [{"message": {"content": "ok"}}],
            "usage": dict(_USAGE_MIT_DETAILS,
                          completion_tokens_details={
                              "reasoning_tokens": 1441}),
        })

    _patch_client(monkeypatch, handler)
    with caplog.at_level(logging.INFO, logger="evidora"):
        await ollama.chat_completion([{"role": "user", "content": "x"}],
                                     kind="analysis")
    assert "reasoning_tokens=1441" in _usage_lines(caplog)[0]


@pytest.mark.parametrize("usage", [
    {"prompt_tokens": 5, "completion_tokens": 1},            # keine Details
    {"prompt_tokens": 5, "prompt_tokens_details": None},     # None statt dict
    {"prompt_tokens": 5, "prompt_tokens_details": "kaputt"},  # falscher Typ
    {"prompt_tokens": 5, "prompt_tokens_details": {"cached_tokens": None}},
    {"prompt_tokens": 5, "prompt_tokens_details": {"cached_tokens": -3}},
])
def test_fehlende_oder_kaputte_details_stuerzen_nicht_ab(caplog, usage):
    """Ein Anbieter, der das Unterfeld weglaesst oder anders fuellt, darf
    das Log nicht sprengen — derselbe Vertrag wie beim ganzen
    usage-Block."""
    with caplog.at_level(logging.INFO, logger="evidora"):
        ollama._log_usage("analysis", "m", usage)
    zeile = _usage_lines(caplog)[0]
    assert "cached_tokens=0" in zeile, zeile
    assert "reasoning_tokens=0" in zeile


# ------------------------------------------------- der Cache-Schluessel

_SYS = {"role": "system", "content": "Du bist ein Faktencheck-Assistent."}


def test_der_schluessel_haengt_am_system_teil():
    """Zwei verschiedene Behauptungen, derselbe System-Prompt → derselbe
    Bucket. Genau das macht den Cache nutzbar."""
    a = ollama.prompt_cache_key([_SYS, {"role": "user", "content": "Claim A"}])
    b = ollama.prompt_cache_key([_SYS, {"role": "user", "content": "Claim B"}])
    assert a and a == b


def test_ein_anderer_system_prompt_bekommt_einen_anderen_bucket():
    """Deutsch und Englisch haben verschiedene System-Prompts — und jede
    kuenftige Aenderung bekommt automatisch ihren eigenen Schluessel.
    Deshalb der Hash und keine Handliste (kein Marker-Drift)."""
    a = ollama.prompt_cache_key([_SYS, {"role": "user", "content": "x"}])
    b = ollama.prompt_cache_key([{"role": "system", "content": "Other"},
                                 {"role": "user", "content": "x"}])
    assert a != b


def test_kein_nutzerinhalt_im_schluessel():
    """Der Schluessel darf nichts aus der Behauptung tragen — die Doku
    warnt ausdruecklich davor, Inhalte in dieses Feld zu schreiben.

    ⚠️ Die erste Fassung dieses Tests suchte jedes Wort der Behauptung im
    Schluessel — und fiel an der "1" aus einer Hausnummer, die in jedem
    Hex-Hash zufaellig vorkommt. Geprueft wird jetzt die EIGENSCHAFT:
    feste Form, und bei voellig verschiedenen Nutzerinhalten derselbe
    Schluessel. Damit kann gar nichts durchsickern.
    """
    geheim = "Max Mustermann wohnt in der Hauptstrasse 1"
    k1 = ollama.prompt_cache_key([_SYS, {"role": "user", "content": geheim}])
    k2 = ollama.prompt_cache_key([_SYS, {"role": "user",
                                         "content": "etwas voellig anderes"}])
    assert re.fullmatch(r"evidora-[0-9a-f]{16}", k1), k1
    assert k1 == k2
    assert "Mustermann" not in k1 and "Hauptstrasse" not in k1


def test_ohne_system_teil_kein_schluessel():
    """Ohne konstanten Anfang gibt es nichts zu cachen — dann bleibt das
    Feld weg, statt einen nutzlosen Bucket aufzumachen."""
    assert ollama.prompt_cache_key([{"role": "user", "content": "x"}]) == ""
    assert ollama.prompt_cache_key([]) == ""
    assert ollama.prompt_cache_key(None) == ""


async def test_der_schluessel_geht_mit_der_anfrage_raus(monkeypatch, caplog):
    gesehen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        gesehen.update(json.loads(request.content))
        return httpx.Response(200, json={
            "choices": [{"message": {"content": "ok"}}],
            "usage": _USAGE_MIT_DETAILS})

    _patch_client(monkeypatch, handler)
    with caplog.at_level(logging.INFO, logger="evidora"):
        await ollama.chat_completion([_SYS, {"role": "user", "content": "x"}],
                                     kind="analysis")
    assert gesehen.get("prompt_cache_key") == ollama.prompt_cache_key(
        [_SYS, {"role": "user", "content": "x"}])
    # Und der Schluessel traegt nichts aus der Nutzeranfrage.
    assert "x" not in gesehen["prompt_cache_key"].replace("evidora-", "")


async def test_auch_der_streaming_pfad_schickt_ihn(monkeypatch, caplog):
    """Die Synthese streamt — ohne den Schluessel dort bliebe der GROESSTE
    Prompt ungecacht."""
    gesehen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        gesehen.update(json.loads(request.content))
        return httpx.Response(200, text=_sse([
            {"choices": [{"delta": {"content": "A"}}]},
            {"choices": [], "usage": _USAGE_MIT_DETAILS},
        ]), headers={"content-type": "text/event-stream"})

    _patch_client(monkeypatch, handler)
    with caplog.at_level(logging.INFO, logger="evidora"):
        out = await ollama.chat_completion_streaming(
            [_SYS, {"role": "user", "content": "x"}], kind="synthesis")
    assert out == "A"
    assert gesehen.get("stream") is True
    assert gesehen.get("prompt_cache_key") == ollama.prompt_cache_key(
        [_SYS, {"role": "user", "content": "x"}]), sorted(gesehen)
    # Und die Zahlen des Streams landen vollstaendig im Log.
    assert "cached_tokens=14464" in _usage_lines(caplog)[0]
