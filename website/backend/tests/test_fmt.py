"""Regressions-Netz für services._fmt.de_int (Audit-Befund R2-2).

Das Idiom f"{d.get(k):,}".replace(",", ".") crasht mit TypeError, sobald
der Wert None ist (fehlender/null Key) → die ganze kuratierte Quelle fällt
still aus dem Fan-out. de_int fängt das ab.

Dependency-light: pure stdlib, läuft in der GitHub-CI.
"""
from services._fmt import de_int


def test_thousands_formatting_de():
    assert de_int(1234567) == "1.234.567"
    assert de_int(1000) == "1.000"
    assert de_int(999) == "999"


def test_none_does_not_crash():
    # Der Kern des Bugs: None statt Zahl -> vorher TypeError-Crash.
    assert de_int(None) == "?"


def test_non_numeric_does_not_crash():
    assert de_int("keine zahl") == "?"
    assert de_int([]) == "?"


def test_zero_and_floats():
    assert de_int(0) == "0"
    assert de_int(0.0) == "0"
    assert de_int(1234.7) == "1.235"   # rundet kaufmännisch
    assert de_int("1500") == "1.500"   # numerischer String ok


def test_negative():
    assert de_int(-1234) == "-1.234"
