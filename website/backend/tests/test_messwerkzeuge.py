"""Die Messwerkzeuge in tools/messung/ tun, was sie zusagen.

Bis 20.11.2026 schrieb Claude Prod-Pruefung, Analyse-Lauf und
Latenz-Auswertung fuer jede Messung neu — und machte dabei Fehler (falsch
zugeschnittene Spalten, ein Filter auf die falsche Spalte, ein Formatfehler
bei None). Diese Tests pinnen die festen Fassungen. Kein Netz, kein ssh,
kein Modell: subprocess.run wird ersetzt.
"""

import json
import os
import subprocess
import sys

import pytest

TOOLS = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "tools", "messung")
sys.path.insert(0, TOOLS)

import analyse_lauf as al          # noqa: E402
import latenz_auswerten as la      # noqa: E402
import prod_pruefen as pp          # noqa: E402

SSE = "\n".join([
    'event: step', 'data: {"step": "analyze"}', '',
    'data: {"step": "cache_hit"}',
    'data: kaputt{',
    'event: result',
    'data: {"verdict": "false", "confidence": 0.9, "summary": "S", "evidence": '
    '[{"url": "u1", "source": "Q1", "title": "T"}], "source_coverage": [{"source": "Eurostat"}]}',
    'data: {}',
])


# --- prod_pruefen ------------------------------------------------------------

def test_die_sse_antwort_wird_ausgewertet():
    r = pp.werte_sse_aus(SSE)
    assert r["verdict"] == "false" and r["confidence"] == 0.9
    assert r["cache"] is True
    assert r["evidence"] == [{"url": "u1", "source": "Q1", "titel": "T"}]
    assert r["quellen"] == ["Eurostat"]
    assert r["schritte"] == ["analyze", "cache_hit"]


def test_ein_fehler_wird_festgehalten_und_leere_antwort_ist_kein_absturz():
    r = pp.werte_sse_aus('data: {"detail": "MISTRAL_CREDITS_EXHAUSTED"}')
    assert r["fehler"] == "MISTRAL_CREDITS_EXHAUSTED" and r["verdict"] is None
    assert pp.werte_sse_aus("")["verdict"] is None


def test_der_key_bleibt_auf_dem_server_und_der_claim_reist_per_stdin():
    gesehen = {}

    def falsch(args, **kw):
        gesehen["args"], gesehen["input"] = args, kw.get("input")
        return subprocess.CompletedProcess(args, 0, stdout=SSE, stderr="")

    pp.pruefe("u@h", "Geheime Behauptung über Österreich", lauf=falsch)
    befehl = " ".join(gesehen["args"])
    assert "grep ^EVIDORA_TEST_API_KEY" in befehl, "Key muss serverseitig gelesen werden"
    assert "-d @-" in befehl, "Claim muss per stdin an curl"
    assert "Geheime Behauptung" not in befehl, "Claim steht in der Kommandozeile"
    assert json.loads(gesehen["input"]) == {"claim": "Geheime Behauptung über Österreich"}


def test_eingaben_werden_vereinheitlicht_und_doppelte_ids_abgelehnt():
    assert pp.normalisiere([{"nr": 7, "text": "a"}, {"id": "x", "claim": "b"}, "c"]) == [
        {"id": "7", "claim": "a"}, {"id": "x", "claim": "b"}, {"id": "3", "claim": "c"}]
    with pytest.raises(ValueError):                       # Zeichenkette an Stelle 3 kollidiert mit nr 3
        pp.normalisiere([{"nr": 3, "text": "a"}, "b", "c"])
    with pytest.raises(ValueError):
        pp.normalisiere([{"id": 1, "claim": "a"}, {"id": 1, "claim": "b"}])
    with pytest.raises(ValueError):
        pp.normalisiere([{"id": 1}])


def test_ein_abgebrochener_lauf_setzt_fort_und_notiert_das_zeitfenster(tmp_path):
    ein = tmp_path / "e.json"
    ein.write_text(json.dumps([{"id": 1, "claim": "a"}, {"id": 2, "claim": "b"}]), encoding="utf-8")
    aus = tmp_path / "r.jsonl"
    aus.write_text(json.dumps({"id": "1", "verdict": "true"}) + "\n", encoding="utf-8")
    aufrufe = []

    def falsch(args, **kw):
        aufrufe.append(json.loads(kw["input"])["claim"])
        return subprocess.CompletedProcess(args, 0, stdout=SSE, stderr="")

    assert pp.main([str(ein), str(aus), "--ssh", "u@h"], lauf=falsch) == 0
    assert aufrufe == ["b"], "der erledigte Eintrag wurde noch einmal geprueft"
    zeilen = [json.loads(z) for z in aus.read_text(encoding="utf-8").splitlines()]
    assert [z["id"] for z in zeilen] == ["1", "2"]
    fenster = (tmp_path / "r.jsonl.zeitfenster").read_text(encoding="utf-8")
    assert "UTC" in fenster and "1 Pruefungen" in fenster

    # Ein dritter Aufruf hat nichts mehr zu tun: kein Aufruf, kein neues Zeitfenster.
    aufrufe.clear()
    assert pp.main([str(ein), str(aus), "--ssh", "u@h"], lauf=falsch) == 0
    assert aufrufe == []
    assert (tmp_path / "r.jsonl.zeitfenster").read_text(encoding="utf-8") == fenster


def test_ohne_ziel_bricht_es_ab_statt_zu_raten(tmp_path, monkeypatch):
    monkeypatch.delenv("EVIDORA_SSH", raising=False)
    ein = tmp_path / "e.json"
    ein.write_text("[]", encoding="utf-8")
    assert pp.main([str(ein), str(tmp_path / "r.jsonl")]) == 2


# --- analyse_lauf ------------------------------------------------------------

def test_analyse_lauf_laedt_beim_import_kein_backend():
    """Der Import darf weder /app noch services anfassen — erst main() tut das."""
    q = open(al.__file__, encoding="utf-8").read()
    kopf = q.split("def main", 1)[0]
    assert "from services" not in kopf


def test_analyse_lauf_wiederholt_einmal_und_schreibt_fehler(monkeypatch):
    monkeypatch.setattr(al.asyncio, "sleep", lambda s: _sofort())
    zeilen, versuche = [], {"n": 0}

    async def wackelig(claim):
        versuche["n"] += 1
        if claim == "kaputt":
            raise RuntimeError("geht nicht")
        if versuche["n"] == 1:
            raise RuntimeError("einmal")
        return {"claim": claim.upper()}

    import asyncio
    asyncio.run(al.lauf([{"id": "1", "claim": "a"}, {"id": "2", "claim": "kaputt"}], wackelig, zeilen.append))
    erg = {json.loads(z)["id"]: json.loads(z) for z in zeilen}
    assert erg["1"]["analysis"] == {"claim": "A"} and erg["1"]["fehler"] is None
    assert erg["2"]["analysis"] is None and "geht nicht" in erg["2"]["fehler"]


async def _sofort():
    return None


# --- latenz_auswerten --------------------------------------------------------

PROBE = """18:39:38 legal 200 0.003231 health 200 0.003459
18:39:40 legal 000 0.000821 health 000 0.001688
18:40:33 legal 000 0.000700 health 000 0.001500
18:40:42 legal 200 0.005148 health 200 6.467325
18:40:44 legal 200 0.004322 health 200 0.003784
Muell
"""


def test_die_spalten_sind_richtig_zugeordnet():
    z = la.lies(PROBE.splitlines())
    assert len(z) == 5
    s = la.statistik(la.im_fenster(z, "18:40:35", "18:50:34"), "health")
    assert s["ok"] == 2 and s["max"] == pytest.approx(6.467325) and s["ueber"] == 1
    s = la.statistik(la.im_fenster(z, "18:40:35", "18:50:34"), "legal")
    assert s["ueber"] == 0 and s["max"] == pytest.approx(0.005148)


def test_das_ausfallfenster_wird_benannt():
    s = la.statistik(la.lies(PROBE.splitlines()), "legal")
    assert (s["fehl"], s["ausfall_von"], s["ausfall_bis"]) == (2, "18:39:40", "18:40:33")


def test_auswertung_ohne_daten_meldet_das(tmp_path, capsys):
    p = tmp_path / "leer.log"
    p.write_text("nichts\n", encoding="utf-8")
    assert la.main([str(p)]) == 1
    assert "keine auswertbaren Zeilen" in capsys.readouterr().out


def test_die_probe_schreibt_weder_ip_noch_claim():
    q = open(os.path.join(TOOLS, "latenz_probe.sh"), encoding="utf-8").read()
    befehle = [z for z in q.splitlines() if z.strip() and not z.lstrip().startswith("#")]
    text = "\n".join(befehle)
    assert "/api/check" not in text and "remote_addr" not in text
    assert text.count("curl ") == 2 and "%{http_code} %{time_total}" in text
