"""Export-Sanitization: interne Pipeline-Marker aus den user-sichtbaren
Quellen entfernen (Audit 2026-07-07).

Befund (Juden/Weltverschwörung-Export): Im PDF/SSE-Export stand wörtlich
„Kernsatz fuer synthesizer: WICHTIG: …" — ein interner Synthesizer-Hinweis.
Ursache: `render_data_with_marker` baut EINEN display_value, der sowohl in
den Synthesizer-Prompt (braucht die Direktive) als auch in den User-Export
(darf sie nicht zeigen) geht.

Trennung: Der Prompt wird in synthesize_results VOR dem Export gebaut. Diese
Funktion säubert eine KOPIE der Quellen für synthesis['raw_sources'] — der
Prompt bleibt unangetastet.

Entfernt werden:
- STRUKTURELL-FALSCH-/COSINE-Prefixe (interne Verdict-Override-Direktiven),
- „Kernsatz fuer/für synthesizer: …"-Segmente,
- reine Synthesizer-Direktiv-Segmente („WICHTIG für die Bewertung: …",
  „VERDICT-DIREKTIVE: …"),
- rohe GDELT-GKG-Character-Offsets („ARMEDCONFLICT,710" → „ARMEDCONFLICT"),
- unaufgelöste HTML-Entities.
"""

import copy
import html
import re

from services._struct_marker import STRUCT_COSINE_PREFIX, STRUCT_EXACT_PREFIX

# Feld-Labels (aus render_data_with_marker: "Label: wert", " | "-getrennt),
# die interne Synthesizer-Hinweise sind und nie im Export erscheinen dürfen.
_INTERNAL_LABEL_RE = re.compile(
    r"^\s*(?:kernsatz f(?:ue|ü)r synthesizer|verdict-direktive)\s*:",
    re.IGNORECASE,
)
# Segmente, die als reine Bewertungs-Direktive beginnen.
_INTERNAL_SEGMENT_RE = re.compile(
    r"^\s*(?:WICHTIG f(?:ue|ü)r die Bewertung|VERDICT-DIREKTIVE)\s*:",
    re.IGNORECASE,
)
# GDELT-GKG-Tag mit Character-Offset: THEMA_NAME,12345 → THEMA_NAME
_GKG_OFFSET_RE = re.compile(r"\b([A-Z][A-Z0-9_]{2,}),\d{1,6}\b")

_STRIP_PREFIXES = (STRUCT_EXACT_PREFIX, STRUCT_COSINE_PREFIX)

# Felder eines Result-Dicts, deren freier Text user-sichtbar ist.
_TEXT_FIELDS = ("display_value", "description", "finding", "rating")


def _sanitize_text(s: str) -> str:
    if not isinstance(s, str) or not s:
        return s

    # 1) führende STRUKTURELL-Prefixe kappen (samt folgendem kernsatz-Text bis
    #    zum ersten " | " — das ist genau das Override-Segment).
    for pref in _STRIP_PREFIXES:
        if s.lstrip().startswith(pref):
            after = s.split(pref, 1)[1]
            # Rest bis zum ersten Segmenttrenner ist die interne Direktive.
            s = after.split(" | ", 1)[1] if " | " in after else ""
            break

    # 2) " | "-Segmente filtern, die interne Labels/Direktiven tragen.
    if " | " in s:
        segments = s.split(" | ")
        kept = [
            seg for seg in segments
            if not _INTERNAL_LABEL_RE.match(seg)
            and not _INTERNAL_SEGMENT_RE.match(seg)
        ]
        s = " | ".join(kept)
    else:
        if _INTERNAL_LABEL_RE.match(s) or _INTERNAL_SEGMENT_RE.match(s):
            s = ""

    # 3) GKG-Character-Offsets entfernen (nur die ,NNN-Suffixe).
    s = _GKG_OFFSET_RE.sub(r"\1", s)

    # 4) HTML-Entities auflösen.
    s = html.unescape(s)

    return s.strip(" |").strip()


def sanitize_sources_for_export(sources: list) -> list:
    """Gibt eine TIEFE KOPIE von ``sources`` zurück, deren user-sichtbare
    Textfelder von internen Pipeline-Markern befreit sind. Mutiert das
    Original nicht (der Synthesizer-Prompt nutzt die Originale).

    Robust: unbekannte Strukturen bleiben unverändert; wird ein Feld leer,
    bleibt es weg statt einen leeren Marker-Rest zu zeigen."""
    if not isinstance(sources, list):
        return sources
    out = copy.deepcopy(sources)
    for src in out:
        if not isinstance(src, dict):
            continue
        for r in src.get("results", []) or []:
            if not isinstance(r, dict):
                continue
            for field in _TEXT_FIELDS:
                if field in r and isinstance(r[field], str):
                    cleaned = _sanitize_text(r[field])
                    r[field] = cleaned
    return out
