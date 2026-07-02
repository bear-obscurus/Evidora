"""Pinnt beide CORDIS-Bulk-ZIP-Formate für _records_from_zip.

Hintergrund (2026-07-02): CORDIS hat das Bulk-Format umgestellt — statt
einer ``project.json`` (Array) enthält das ZIP jetzt eine Datei pro
Projekt (``project-rcn-<rcn>_en.json``). Der Quartals-Refresh lief
dadurch still auf 0 Records. Diese Tests stellen sicher, dass beide
Formate geparst werden und ``frameworkProgramme`` im neuen Format aus
der ZIP-Herkunft gesetzt wird. Kein Netzwerk.
"""

import io
import json
import zipfile

from services.cordis import _records_from_zip


def _zip_with(files: dict) -> zipfile.ZipFile:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        for name, obj in files.items():
            z.writestr(name, json.dumps(obj))
    return zipfile.ZipFile(io.BytesIO(buf.getvalue()))


_REC = {
    "id": "101069359", "acronym": "SolDAC",
    "title": "Full spectrum SOLar Direct Air Capture",
    "objective": "Ethylene is ...", "keywords": "solar;DAC",
    "ecMaxContribution": "2073781.25", "totalCost": "2073781.25",
    "startDate": "2022-09-01", "endDate": "2025-08-31",
    "status": "SIGNED",
}


def test_legacy_single_project_json():
    zf = _zip_with({"project.json": [_REC, dict(_REC, id="2")]})
    recs = _records_from_zip(zf, "HORIZON")
    assert len(recs) == 2
    assert recs[0]["acronym"] == "SolDAC"


def test_new_per_project_files():
    zf = _zip_with({
        "project-rcn-237915_en.json": _REC,
        "project-rcn-237916_en.json": dict(_REC, id="2", acronym="X2"),
    })
    recs = _records_from_zip(zf, "HORIZON")
    assert len(recs) == 2
    assert {r["acronym"] for r in recs} == {"SolDAC", "X2"}
    # frameworkProgramme fehlt im neuen Format → aus ZIP-Herkunft gesetzt
    assert all(r["frameworkProgramme"] == "HORIZON" for r in recs)


def test_new_format_skips_broken_member():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("project-rcn-1_en.json", json.dumps(_REC))
        z.writestr("project-rcn-2_en.json", "{kaputt")
    zf = zipfile.ZipFile(io.BytesIO(buf.getvalue()))
    recs = _records_from_zip(zf, "H2020")
    assert len(recs) == 1
    assert recs[0]["frameworkProgramme"] == "H2020"


def test_empty_zip_returns_empty():
    zf = _zip_with({"readme.txt": "x"})
    assert _records_from_zip(zf, "HORIZON") == []
