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


def atomic_write_json(path, obj, *, ensure_ascii: bool = False,
                      indent=None, trailing_newline: bool = False) -> None:
    text = _json.dumps(obj, ensure_ascii=ensure_ascii, indent=indent)
    if trailing_newline:
        text += "\n"
    atomic_write_text(path, text)
