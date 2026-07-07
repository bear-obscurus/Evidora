"""Regressions-Netz für die Eurostat-Fertilitätsrate (Bug 2026-07-07).

Der Connector mappte "Fertilität/Geburtenrate" auf das Dataset demo_frate
(altersspezifische Rate pro einzelnem Altersjahr, winzige ~0,00X-Werte);
ohne Alters-Filter überschrieb der Parser pro (geo,time) mit einem
willkürlichen Altersband → Österreich wurde als "0,0001 Kinder/Frau"
ausgegeben und das Länder-Ranking war invertiert.

Fix: demo_find + indic_de=TOTFERRT (Total Fertility Rate) → der bekannte
~1,3-"Kinder je Frau"-Wert.

Dependency-light: importiert nur services.eurostat (httpx-only), kein Netz
(die Fixture ist eine gekürzte, echte JSON-stat-2.0-Antwort-Struktur).
"""
from services.eurostat import DATASET_MAP, _parse_multi_country


def _fertility_entries():
    return [v for k, v in DATASET_MAP.items()
            if v.get("unit") == "Kinder/Frau"]


def test_no_known_broken_dataset_params():
    """Dataset-Audit 2026-07-07: guard gegen Rückfall auf Param-Codes, die
    entweder falsche Daten (ungefilterte Dimension → Zufallszeile) oder
    HTTP 400 / leere Antworten lieferten. Jeder Wert hier war ein realer
    Bug; alle 32 Dataset-Konfigurationen sind jetzt gefiltert + befüllt."""
    # (bad_code, warum) — darf in KEINEM params-Dict mehr vorkommen
    forbidden = {
        "demo_frate": "altersspezifisch statt TFR → 0,0001 Kinder/Frau",
        "E7000": "siec=E7000 mit GEP → leere Stromproduktion",
        "CP_PPS_HAB": "ungültige Unit für BIP/Kopf → leer",
        "LI_R_MD60": "indic_il existiert nicht in ilc_li02 → HTTP 400",
        "BAL": "flow=BAL existiert nicht in ext_lt_maineu → HTTP 400",
        "PC_EMP_TOT": "ungültige htec-Unit → HTTP 400",
        "T_M_NMT_KIS": "sectex existiert nicht in htec_emp_nat → HTTP 400",
        "MED_E_HR": "indic_se existiert nicht in earn_ses_pub2s → HTTP 400",
    }
    for kw, ds in DATASET_MAP.items():
        blob = f"{ds['dataset']} {ds['params']}"
        for bad, why in forbidden.items():
            assert bad not in blob, f"'{kw}' nutzt wieder '{bad}' ({why})"


def test_migration_and_gini_and_students_are_filtered():
    """Die 3 strukturell kaputten Datasets (ungefilterte Dim → Zufallszeile)
    haben jetzt den TOTAL/Aggregat-Filter."""
    def params_for(dataset):
        return [v["params"] for v in DATASET_MAP.values()
                if v["dataset"] == dataset]
    for p in params_for("migr_imm1ctz"):
        assert p.get("citizen") == "TOTAL"        # sonst: Zufalls-Staatsangehörigkeit
    for p in params_for("ilc_di12"):
        assert p.get("age") == "TOTAL"            # sonst: Zufalls-Altersgruppe (Gini)
    for p in params_for("educ_uoe_enrt01"):
        assert p.get("worktime") == "TOTAL" and p.get("sector") == "TOT_SEC"


def test_fertility_uses_total_fertility_rate_not_age_specific():
    """Config-Contract: alle 'Kinder/Frau'-Einträge nutzen demo_find/TOTFERRT,
    NICHT das altersspezifische demo_frate (das den Bug verursachte)."""
    entries = _fertility_entries()
    assert entries, "keine Fertilitäts-Einträge in DATASET_MAP gefunden"
    for e in entries:
        assert e["dataset"] == "demo_find", f"falsches Dataset: {e['dataset']}"
        assert e["dataset"] != "demo_frate"
        assert e["params"].get("indic_de") == "TOTFERRT", \
            "indic_de=TOTFERRT fehlt — sonst mischt demo_find alle Indikatoren"


def _jsonstat_demo_find(values_by_geo: dict[str, float], year: str = "2024"):
    """Minimale JSON-stat-2.0-Antwort wie demo_find/TOTFERRT sie liefert:
    Dimensionen freq(1) x indic_de(1) x geo(N) x time(1)."""
    geos = list(values_by_geo)
    size = [1, 1, len(geos), 1]
    # strides: product der nachfolgenden sizes
    value = {}
    for gi, g in enumerate(geos):
        flat = gi * size[3]          # freq=0, indic=0, geo=gi, time=0
        value[str(flat)] = values_by_geo[g]
    return {
        "id": ["freq", "indic_de", "geo", "time"],
        "size": size,
        "value": value,
        "dimension": {
            "freq": {"category": {"index": {"A": 0}, "label": {"A": "Annual"}}},
            "indic_de": {"category": {"index": {"TOTFERRT": 0},
                                      "label": {"TOTFERRT": "Total fertility rate"}}},
            "geo": {"category": {"index": {g: i for i, g in enumerate(geos)},
                                 "label": {g: g for g in geos}}},
            "time": {"category": {"index": {year: 0}, "label": {year: year}}},
        },
    }


def test_parsed_fertility_values_are_realistic_and_ranked():
    ds = _fertility_entries()[0]
    # Echte TFR-Größenordnung 2024: AT 1,31 / DE 1,35 / FR 1,61 / IT 1,18
    data = _jsonstat_demo_find({"AT": 1.31, "DE": 1.35, "FR": 1.61, "IT": 1.18})
    results = _parse_multi_country(data, ds)
    assert len(results) == 4
    # Alle Werte im plausiblen "Kinder je Frau"-Bereich (nicht 0,0001!)
    for r in results:
        assert r["value"].endswith("Kinder/Frau")
        num = float(r["value"].split()[0])
        assert 0.5 <= num <= 3.0, f"unplausibler TFR-Wert: {num}"
    # Ranking absteigend nach Wert: FR (1,61) vorne, IT (1,18) hinten
    assert results[0]["geo"] == "FR" and results[0]["rank"] == 1
    assert results[-1]["geo"] == "IT"
    # Der ursprüngliche Bug-Wert (0,0001) darf nirgends auftauchen
    assert all("0.0001" not in r["value"] for r in results)
