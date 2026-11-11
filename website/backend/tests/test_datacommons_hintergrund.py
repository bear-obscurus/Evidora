"""DataCommons baut im Hintergrund neu — der Dienst antwortet weiter.

Bis 11.11.2026 rechnete services/datacommons.update_index die Embeddings
aller 11.071 Eintraege synchron in der Event-Loop: beim Start (uvicorn nahm
erst danach Verbindungen an) und taeglich in der Stundenschleife. Gemessen
an 8 von 8 Neuaufbauten stand der Dienst 4,0–8,1 min still, nicht einmal
der Healthcheck kam durch.

Diese Tests pinnen die vier Teile des Umbaus:
1. Zwischenspeicher: Unveraenderte Eintraege werden nicht neu gerechnet;
   ein Modellwechsel oder ein kaputter Speicher fuehrt zu Neuberechnung.
2. Gerechnet wird im Thread und stueckweise; die Event-Loop laeuft weiter.
   Die Encode-Sperre in _st_model haelt parallele Tokenizer-Zugriffe fern.
3. update_index kehrt sofort zurueck und startet hoechstens eine
   Hintergrund-Aufgabe.
4. Ohne Embeddings wird NIE pro Anfrage alles eingebettet.

Kein Modell, kein Netz: ein gefaelschtes Modell zaehlt die encode-Aufrufe.
"""

import asyncio
import json
import os
import threading
import time

import numpy as np
import pytest

from services import _st_model
from services import datacommons as dc


class _FalschesModell:
    """Liefert je Text einen festen Vektor (aus dem Text abgeleitet) und zaehlt."""

    def __init__(self, pause=0.0):
        self.texte = []
        self.stuecke = []
        self.pause = pause

    @staticmethod
    def vektor(text):
        h = abs(hash(text)) % 10_000
        return np.array([h, h + 1, h + 2, h + 3], dtype=np.float32)

    def encode(self, texte, batch_size=32, convert_to_numpy=True, show_progress_bar=False, **_):
        if isinstance(texte, str):
            texte = [texte]
        if self.pause:
            time.sleep(self.pause)
        self.texte.extend(texte)
        self.stuecke.append(len(texte))
        return np.stack([self.vektor(t) for t in texte])


def _eintraege(n, start=0):
    return [{"claim": f"Behauptung {i}", "rating": "falsch", "url": f"u{i}", "source": "s", "date": ""}
            for i in range(start, start + n)]


@pytest.fixture
def frisch(monkeypatch, tmp_path):
    """Wie nach einem Neustart: nichts im Speicher, Dateien in tmp_path."""
    monkeypatch.setattr(dc, "DATA_DIR", tmp_path)
    monkeypatch.setattr(dc, "LOCAL_INDEX", tmp_path / "claimreview_index.json")
    monkeypatch.setattr(dc, "EMB_SPEICHER", tmp_path / "claimreview_embeddings.npz")
    monkeypatch.setattr(dc, "_index", None)
    monkeypatch.setattr(dc, "_embeddings", None)
    monkeypatch.setattr(dc, "_index_ts", 0)
    monkeypatch.setattr(dc, "_neuaufbau_task", None)
    monkeypatch.setattr(dc, "_modell_kennung", lambda: "modell-A@rev1")
    return tmp_path


# --- 1. Zwischenspeicher -----------------------------------------------------

def test_unveraenderte_eintraege_werden_nicht_neu_gerechnet(frisch):
    m = _FalschesModell()
    e = _eintraege(10)
    asyncio.run(dc._embeddings_fuer(e, m))
    assert len(m.texte) == 10

    m.texte.clear()
    matrix = asyncio.run(dc._embeddings_fuer(e, m))
    assert m.texte == [], "der zweite Lauf hat unveraenderte Eintraege neu gerechnet"
    for i, eintrag in enumerate(e):
        assert np.array_equal(matrix[i], m.vektor(f"{eintrag['claim']} {eintrag['rating']}"))


def test_nur_neue_eintraege_werden_gerechnet_und_die_reihenfolge_stimmt(frisch):
    m = _FalschesModell()
    asyncio.run(dc._embeddings_fuer(_eintraege(10), m))
    m.texte.clear()
    neu = _eintraege(1, start=99) + _eintraege(10)       # neuer Eintrag VORNE
    matrix = asyncio.run(dc._embeddings_fuer(neu, m))
    assert m.texte == ["Behauptung 99 falsch"]
    for i, eintrag in enumerate(neu):                    # Zeile i gehoert zu Eintrag i
        assert np.array_equal(matrix[i], m.vektor(f"{eintrag['claim']} {eintrag['rating']}"))


def test_ein_modellwechsel_macht_den_speicher_ungueltig(frisch, monkeypatch):
    m = _FalschesModell()
    asyncio.run(dc._embeddings_fuer(_eintraege(5), m))
    monkeypatch.setattr(dc, "_modell_kennung", lambda: "modell-B@rev2")
    m.texte.clear()
    asyncio.run(dc._embeddings_fuer(_eintraege(5), m))
    assert len(m.texte) == 5


def test_ein_kaputter_speicher_wird_neu_gerechnet_statt_zu_scheitern(frisch):
    dc.EMB_SPEICHER.write_bytes(b"kein npz")
    m = _FalschesModell()
    matrix = asyncio.run(dc._embeddings_fuer(_eintraege(4), m))
    assert len(m.texte) == 4 and matrix.shape == (4, 4)


def test_der_speicher_ist_kein_json(frisch):
    """data_freshness_check und stichtag_check laufen ueber data/*.json."""
    asyncio.run(dc._embeddings_fuer(_eintraege(3), _FalschesModell()))
    assert dc.EMB_SPEICHER.suffix == ".npz"
    assert not list(frisch.glob("*.json"))
    assert not list(frisch.glob("*.tmp")), "Temp-Datei blieb liegen"


# --- 2. Thread, Stuecke, Sperre ----------------------------------------------

def test_gerechnet_wird_stueckweise(frisch, monkeypatch):
    monkeypatch.setattr(dc, "EMB_STUECK", 4)
    m = _FalschesModell()
    asyncio.run(dc._embeddings_fuer(_eintraege(10), m))
    assert m.stuecke == [4, 4, 2]


def test_die_event_loop_laeuft_waehrend_des_rechnens_weiter(frisch, monkeypatch):
    """Das war der Stillstand: vorher kam waehrend des Rechnens NICHTS durch."""
    monkeypatch.setattr(dc, "EMB_STUECK", 2)
    m = _FalschesModell(pause=0.05)                       # 5 Stuecke x 50 ms

    async def lauf():
        ticks = 0
        fertig = asyncio.Event()

        async def ticker():
            nonlocal ticks
            while not fertig.is_set():
                ticks += 1
                await asyncio.sleep(0.01)

        t = asyncio.create_task(ticker())
        await dc._embeddings_fuer(_eintraege(10), m)
        fertig.set()
        await t
        return ticks

    assert asyncio.run(lauf()) >= 10


def test_die_encode_sperre_laesst_nur_einen_thread_gleichzeitig_rechnen():
    gleichzeitig, hoechstens = 0, 0
    zaehler = threading.Lock()

    class Modell:
        def encode(self, *a, **k):
            nonlocal gleichzeitig, hoechstens
            with zaehler:
                gleichzeitig += 1
                hoechstens = max(hoechstens, gleichzeitig)
            time.sleep(0.02)
            with zaehler:
                gleichzeitig -= 1
            return "ok"

    m = _st_model._mit_sperre(Modell())
    threads = [threading.Thread(target=m.encode, args=(["x"],)) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert hoechstens == 1


def test_das_geteilte_modell_bekommt_die_sperre():
    q = open(_st_model.__file__, encoding="utf-8").read()
    assert "_model = _mit_sperre(SentenceTransformer(" in q


# --- 3. update_index kehrt sofort zurueck ------------------------------------

def test_update_index_wartet_nicht_auf_den_neuaufbau(frisch, monkeypatch):
    aufrufe = []

    async def langsam(download):
        aufrufe.append(download)
        await asyncio.sleep(10)

    monkeypatch.setattr(dc, "_neuaufbau", langsam)

    async def lauf():
        t0 = time.monotonic()
        await dc.update_index()
        dauer = time.monotonic() - t0
        erste = dc._neuaufbau_task
        await asyncio.sleep(0)
        await dc.update_index()                           # zweiter Aufruf, Aufgabe laeuft noch
        assert dc._neuaufbau_task is erste, "zweite Hintergrund-Aufgabe gestartet"
        erste.cancel()
        return dauer

    assert asyncio.run(lauf()) < 0.5
    assert aufrufe == [True], "ohne lokalen Index muss heruntergeladen werden"


def test_frischer_index_ohne_embeddings_rechnet_ohne_download(frisch, monkeypatch):
    dc.LOCAL_INDEX.write_text(json.dumps(_eintraege(3)), encoding="utf-8")
    aufrufe = []

    async def merken(download):
        aufrufe.append(download)

    monkeypatch.setattr(dc, "_neuaufbau", merken)

    async def lauf():
        await dc.update_index()
        await dc._neuaufbau_task

    asyncio.run(lauf())
    assert aufrufe == [False]


def test_frischer_index_mit_speicher_startet_nichts(frisch, monkeypatch):
    e = _eintraege(6)
    dc.LOCAL_INDEX.write_text(json.dumps(e), encoding="utf-8")
    asyncio.run(dc._embeddings_fuer(e, _FalschesModell()))   # Speicher anlegen

    async def darf_nicht(download):
        raise AssertionError("Neuaufbau trotz frischem Index und vollem Speicher")

    monkeypatch.setattr(dc, "_neuaufbau", darf_nicht)
    asyncio.run(dc.update_index())
    assert dc._neuaufbau_task is None
    assert dc._embeddings is not None and len(dc._embeddings) == len(dc._index) == 6


def test_der_neuaufbau_tauscht_index_und_embeddings_gemeinsam(frisch, monkeypatch):
    dc.LOCAL_INDEX.write_text(json.dumps(_eintraege(7)), encoding="utf-8")
    monkeypatch.setattr(dc, "_geteiltes_modell", lambda: _FalschesModell())

    async def lauf():
        await dc.update_index()
        await dc._neuaufbau_task

    asyncio.run(lauf())
    assert len(dc._embeddings) == len(dc._index) == 7


# --- 4. Nie mehr alles pro Anfrage -------------------------------------------

def test_ohne_embeddings_wird_pro_anfrage_nichts_eingebettet(frisch, monkeypatch):
    e = _eintraege(50)
    m = _FalschesModell()
    monkeypatch.setattr(dc, "_index", e)
    monkeypatch.setattr(dc, "_embeddings", None)
    monkeypatch.setattr(dc, "_geteiltes_modell", lambda: m)
    assert dc._semantic_search("Behauptung 3", e, entities=["Behauptung"]) is None
    assert m.texte == [], "pro Anfrage wurde eingebettet"


def test_die_suche_faellt_dann_auf_schlagworte_zurueck(frisch, monkeypatch):
    e = _eintraege(5)
    m = _FalschesModell()
    monkeypatch.setattr(dc, "_index", e)
    monkeypatch.setattr(dc, "_geteiltes_modell", lambda: m)
    erg = asyncio.run(dc.search_datacommons(
        {"claim": "Behauptung 2", "entities": ["Behauptung 2"], "factcheck_queries": []}))
    assert [r["url"] for r in erg["results"]] == ["u2"]
    assert m.texte == []


def test_passt_die_laenge_nicht_wird_nicht_gesucht(frisch, monkeypatch):
    e = _eintraege(5)
    m = _FalschesModell()
    monkeypatch.setattr(dc, "_index", e)
    monkeypatch.setattr(dc, "_embeddings", np.zeros((4, 4), dtype=np.float32))
    monkeypatch.setattr(dc, "_geteiltes_modell", lambda: m)
    assert dc._semantic_search("x", e, entities=["x"]) is None
    assert m.texte == []
