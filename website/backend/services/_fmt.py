"""Crash-sichere deutsche Tausender-Formatierung (Audit-Befund R2-2).

Ersetzt das Idiom ``f"{d.get(k):,}".replace(",", ".")``, das mit
``TypeError: unsupported format string passed to NoneType.__format__``
crasht, sobald der Wert ``None`` ist (fehlender/``null`` Key im kuratierten
JSON). Da ``search_*`` kein try/except hat, propagiert der Fehler bis in
die Fan-out-Schleife (main.py), die die GANZE Quelle nur mit einer WARNING
verwirft — die kuratierte Einordnung fehlt dann still im Verdict.

Nebeneffekt: das bisherige ``.replace(",", ".")`` lief über den GESAMTEN
f-String und wandelte auch legitime Text-Kommata (z. B. ", ") in Punkte
um. ``de_int`` formatiert nur die Zahl und lässt den umgebenden Text
unangetastet.

Pure stdlib -> dependency-light unit-testbar.
"""
from __future__ import annotations


def de_int(v) -> str:
    """``1234567`` -> ``'1.234.567'``. Bei ``None``/nicht-numerisch ``'?'``
    statt Crash (graceful degradation: die Quelle bleibt im Fan-out)."""
    try:
        return f"{int(round(float(v))):,}".replace(",", ".")
    except (TypeError, ValueError):
        return "?"
