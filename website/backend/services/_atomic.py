"""Atomarer Datei-Write (Audit R2-5/R2-12).

``open(path, "w")`` + ``json.dump`` kann bei einem Abbruch mitten im
Schreiben (SIGKILL/OOM, Container-Stop, Netzwerk-Timeout beim Streamen)
eine truncierte/ungültige Datei hinterlassen. Beim nächsten Cold-Start
lädt der Service dann kaputte Daten (JSONDecodeError -> Quelle fällt aus)
bzw. der deploy.sh-Dirty-Guard sieht eine halb-geschriebene getrackte
Datei.

``atomic_write_*`` schreibt in eine temporäre Datei im selben Verzeichnis
und benennt sie per ``os.replace`` um — auf demselben Filesystem ein
atomarer Rename: Leser sehen entweder die alte oder die vollständige neue
Datei, nie ein Fragment. Bei einem Fehler wird die Temp-Datei entfernt.
"""
from __future__ import annotations

import json as _json
import os
import tempfile


def atomic_write_text(path, text: str, encoding: str = "utf-8") -> None:
    path = os.fspath(path)
    directory = os.path.dirname(path) or "."
    fd, tmp = tempfile.mkstemp(dir=directory, prefix=".tmp-", suffix=".part")
    try:
        with os.fdopen(fd, "w", encoding=encoding) as f:
            f.write(text)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def atomic_write_json_array(path, elemente, *, ensure_ascii: bool = False) -> int:
    """Schreibt ein JSON-Array aus einem Iterator, ohne es im Speicher zu halten.

    `atomic_write_json` serialisiert erst das ganze Objekt in einen String.
    Beim CORDIS-Cache sind das 114 MB String NEBEN der Liste aus 59.002
    Dicts — gemessen 1.363 MiB Spitzen-RSS, auf einem Host mit 3.814 MB,
    von dem das Backend 2,5 GB haelt. Hier wird Element fuer Element
    geschrieben.

    Returns: Anzahl geschriebener Elemente. **Bei 0 wird NICHT umbenannt**
    und die Zieldatei bleibt unangetastet — ein fehlgeschlagener Refresh
    darf einen guten Cache nie zerstoeren (Lehrgeld 2026-07-02).
    """
    path = os.fspath(path)
    directory = os.path.dirname(path) or "."
    fd, tmp = tempfile.mkstemp(dir=directory, prefix=".tmp-", suffix=".part")
    n = 0
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write("[")
            for element in elemente:
                if n:
                    # ", " und nicht ",": genau das schreibt `json.dumps`
                    # fuer eine Liste. Damit ist die Datei byteweise
                    # identisch zu der des alten, speicherhungrigen Pfads —
                    # ein Umbau, dessen Ergebnis man vergleichen kann, statt
                    # ihm glauben zu muessen. Kostet 59 KB bei 59.002
                    # Records.
                    f.write(", ")
                f.write(_json.dumps(element, ensure_ascii=ensure_ascii))
                n += 1
            f.write("]")
            f.flush()
            os.fsync(f.fileno())
        if n == 0:
            os.unlink(tmp)
            return 0
        os.replace(tmp, path)
        return n
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def atomic_write_json(path, obj, *, ensure_ascii: bool = False,
                      indent=None, trailing_newline: bool = False) -> None:
    text = _json.dumps(obj, ensure_ascii=ensure_ascii, indent=indent)
    if trailing_newline:
        text += "\n"
    atomic_write_text(path, text)
