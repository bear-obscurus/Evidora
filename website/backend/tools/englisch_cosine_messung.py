"""Messung zu Loesungsrichtung (b): traegt der Cosine-Backup englische Claims?

Braucht das Modell (``paraphrase-multilingual-MiniLM-L12-v2`` in der
gepinnten Revision aus services/_st_model.py) — lokal ohne HuggingFace-
Zugang nicht lauffaehig, darum im CI gemessen.

Deskriptor je Fakt wie in den Services mit ``descriptor_fn``:
``f"{headline}. {context_notes[:2]}"[:300]``.

Gemessen wird, was der Backup in ``find_matching_items`` taete: je Dienst
(= data-Datei) die Top-3 Fakten mit Cosinus >= Schwelle.

  Qualitaet   Liegt der eigene Fakt im eigenen Dienst auf Platz 1 / unter
              den Top 3? Cosinus des eigenen Fakts.
  #41-Effekt  Wie viele FREMDE Dienste haetten je Claim mindestens einen
              Fakt ueber der Schwelle — also gefeuert?

Fuer englische Claims, fuer die 2.603 deutschen Phrasings (Referenz: das
hat #41 gesehen) und fuer 40 themenfremde englische Claims (jeder Treffer
ist dort ein falscher).
"""

from __future__ import annotations

import json
import os
import statistics
import sys

HIER = os.path.dirname(os.path.abspath(__file__))
BACKEND = os.path.dirname(HIER)
sys.path.insert(0, BACKEND)
sys.path.insert(0, HIER)

from englisch_messung import KORPUS_DEFAULT, claims, fakt_id, population  # noqa: E402

SCHWELLEN = (0.45, 0.50, 0.55, 0.60, 0.65, 0.70, 0.75)


def deskriptor(f: dict) -> str:
    head = f.get("headline", "")
    notes = " ".join((f.get("context_notes") or [])[:2])
    return f"{head}. {notes}"[:300]


def main() -> None:
    from sentence_transformers import SentenceTransformer, util
    from services._st_model import _MODEL_NAME, _MODEL_REVISION
    from services._englisch import ist_englisch
    from services.ner import _detect_language

    model = SentenceTransformer(_MODEL_NAME, revision=_MODEL_REVISION)

    fakten = []            # (datei, id, deskriptor)
    for p, k, datei in population(BACKEND):
        with open(p, encoding="utf-8") as fh:
            for it in json.load(fh)[k]:
                if isinstance(it, dict) and any(
                        it.get(t) for t in ("trigger_keywords", "trigger_composite",
                                            "trigger_all")):
                    fakten.append((datei, fakt_id(it), deskriptor(it)))
    dateien = sorted({d for d, _, _ in fakten})
    idx_je_datei = {d: [i for i, f in enumerate(fakten) if f[0] == d] for d in dateien}

    alle = claims(BACKEND, KORPUS_DEFAULT)
    emb_f = model.encode([f[2] for f in fakten], convert_to_tensor=True,
                         batch_size=64, show_progress_bar=False)
    emb_c = model.encode([c["text"] for c in alle], convert_to_tensor=True,
                         batch_size=64, show_progress_bar=False)
    sim = util.cos_sim(emb_c, emb_f).cpu().numpy()

    def auswerten(auswahl: list[int], name: str, mit_soll: bool) -> dict:
        r = {"name": name, "claims": len(auswahl)}
        if mit_soll:
            top1 = top3 = 0
            cos_eigen = []
            for ci in auswahl:
                c = alle[ci]
                idx = idx_je_datei.get(c["datei"], [])
                geordnet = sorted(idx, key=lambda i: -sim[ci, i])
                ids = [fakten[i][1] for i in geordnet]
                if ids and ids[0] == c["id"]:
                    top1 += 1
                if c["id"] in ids[:3]:
                    top3 += 1
                eig = [sim[ci, i] for i in idx if fakten[i][1] == c["id"]]
                if eig:
                    cos_eigen.append(float(eig[0]))
            r["top1_im_eigenen_dienst"] = round(top1 / len(auswahl), 3)
            r["top3_im_eigenen_dienst"] = round(top3 / len(auswahl), 3)
            q = (statistics.quantiles(cos_eigen, n=4) if len(cos_eigen) > 1
                 else cos_eigen)
            r["cos_eigener_fakt_q1_median_q3"] = [round(float(x), 3) for x in q]
        for s in SCHWELLEN:
            fremd_je_claim = []
            recall = 0
            for ci in auswahl:
                c = alle[ci]
                n_fremd = 0
                for d in dateien:
                    idx = idx_je_datei[d]
                    oben = sorted(idx, key=lambda i: -sim[ci, i])[:3]
                    gefeuert = [i for i in oben if sim[ci, i] >= s]
                    if d == c.get("datei"):
                        if any(fakten[i][1] == c.get("id") for i in gefeuert):
                            recall += 1
                    elif gefeuert:
                        n_fremd += 1
                fremd_je_claim.append(n_fremd)
            r[f"s{s:.2f}"] = {
                "fremde_dienste_je_claim_mittel": round(statistics.mean(fremd_je_claim), 2),
                "fremde_dienste_je_claim_median": statistics.median(fremd_je_claim),
                "claims_ohne_fremden_dienst": round(
                    sum(x == 0 for x in fremd_je_claim) / len(auswahl), 3),
            }
            if mit_soll:
                r[f"s{s:.2f}"]["eigener_fakt_zurueck"] = round(recall / len(auswahl), 3)
        return r

    en = [i for i, c in enumerate(alle) if c["art"] == "en"]
    en_gate = [i for i in en if ist_englisch(alle[i]["text"])]
    de = [i for i, c in enumerate(alle) if c["art"] == "de"]
    de_detect_en = [i for i in de if _detect_language(alle[i]["text"]) == "en"]
    fremd = [i for i, c in enumerate(alle) if c["art"] == "themenfremd"]
    live = [i for i, c in enumerate(alle) if c["art"] == "live"]

    berichte = [
        auswerten(en, "en_alle", True),
        auswerten(en_gate, "en_mit_neuem_gate", True),
        auswerten(de, "de_phrasings", True),
        auswerten(de_detect_en, "de_die_detect_language_fuer_en_haelt", True),
        auswerten(fremd, "themenfremd_en", False),
        auswerten(live, "live", True),
    ]
    # Live-Claim im Detail: die Top-5 global
    ci = live[0]
    top = sorted(range(len(fakten)), key=lambda i: -sim[ci, i])[:5]
    berichte.append({"live_top5_global": [
        [fakten[i][0], fakten[i][1], round(float(sim[ci, i]), 3)] for i in top]})
    # Globale Top-1-Genauigkeit (ueber alle Dienste)
    for name, auswahl in (("en", en), ("de", de)):
        ok = sum(fakten[int(sim[ci].argmax())][1] == alle[ci]["id"] for ci in auswahl)
        berichte.append({f"global_top1_{name}": round(ok / len(auswahl), 3)})
    print("COSINE-MESSUNG-JSON-BEGINN")
    print(json.dumps(berichte, ensure_ascii=False, indent=1))
    print("COSINE-MESSUNG-JSON-ENDE")


if __name__ == "__main__":
    main()
