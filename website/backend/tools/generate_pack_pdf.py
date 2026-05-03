#!/usr/bin/env python3
"""Generate a one-page Evidora-Stress-Test PDF from results.json + meta.json.

Usage:
  python3 tools/generate_pack_pdf.py \
      --results /tmp/results_<pack>.json \
      --meta tools/pdf_meta/<pack>.json \
      --out "/Users/.../Documents/Evidora Dokumente/Evidora_<Topic>_20_Claims.pdf"

Format reproduces the existing 13 PDFs in the same folder:
  - Title 'Evidora' (large blue) + subtitle 'X-Stress-Test - 20 Claims'
  - Description paragraph(s)
  - Optional 'Probe-Strategie' / 'Methodische Disziplin' paragraph
  - 'System-Performance' key-value table
  - Optional 'Politische Guardrails' / 'Distanzierung legitimer Debatten'
  - Footer 'evidora.eu - X-Stress-Test, YYYY-MM-DD'

The meta.json contains per-pack narrative + custom kv-fields:
  {
    "title": "Tech-/KI-Mythen",            # for header + footer
    "topic_count": "9 Topics",              # short summary
    "description": "20 Claims aus...",
    "methodology": "Methodische Disziplin: ..."  // optional
    "guardrails": "..."                     // optional
    "performance_extras": [                 // optional rows
      {"label": "Pack-Coverage", "value": "9/9 Topics"}
    ]
  }
"""
from __future__ import annotations
import argparse, json, os, sys
from datetime import date
from collections import Counter

from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.colors import HexColor, black
from reportlab.lib.units import cm
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak
)


def _styles():
    s = getSampleStyleSheet()
    title = ParagraphStyle(
        "EvidoraTitle", parent=s["Title"],
        fontName="Helvetica-Bold", fontSize=42, leading=44,
        textColor=HexColor("#1A4F8B"),
        spaceAfter=2, alignment=0,  # left
    )
    subtitle = ParagraphStyle(
        "EvidoraSubtitle", parent=s["Heading2"],
        fontName="Helvetica-Bold", fontSize=18, leading=22,
        textColor=HexColor("#1A4F8B"),
        spaceAfter=10, alignment=0,
    )
    body = ParagraphStyle(
        "Body", parent=s["BodyText"],
        fontName="Helvetica", fontSize=10, leading=13,
        textColor=black, spaceAfter=8,
    )
    section = ParagraphStyle(
        "Section", parent=s["Heading3"],
        fontName="Helvetica-Bold", fontSize=12, leading=14,
        textColor=HexColor("#1A4F8B"),
        spaceAfter=6, spaceBefore=8,
    )
    footer = ParagraphStyle(
        "Footer", parent=s["Normal"],
        fontName="Helvetica", fontSize=8, leading=9,
        textColor=HexColor("#777777"),
        alignment=1,  # center
    )
    return {"title": title, "subtitle": subtitle, "body": body,
            "section": section, "footer": footer}


def _load_results(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _summarize_results(results: dict) -> dict:
    """Compute pass/fail counts."""
    items = results.get("results", [])
    n = len(items)
    v_match = 0
    v_strict = 0
    s_match = 0
    s_total_eval = 0
    fp = 0
    fn = 0
    for r in items:
        verdict = r.get("verdict")
        expected = set(r.get("expected_verdicts") or [])
        if expected and verdict in expected:
            v_match += 1
            v_strict += 1
        elif expected:
            # tolerance pair
            if verdict in {"false", "mostly_false"} and (expected & {"false", "mostly_false"}):
                v_match += 1
            elif verdict in {"true", "mostly_true"} and (expected & {"true", "mostly_true"}):
                v_match += 1
        # source-match
        exp_src = r.get("expected_source")
        if exp_src:
            s_total_eval += 1
            if any(exp_src in s for s in (r.get("sources_with_results") or [])):
                s_match += 1
    return {
        "n": n,
        "v_match": v_match,
        "v_strict": v_strict,
        "s_match": s_match,
        "s_total_eval": s_total_eval,
        "false_positives": fp,
        "false_negatives": fn,
    }


def build_pdf(meta: dict, results: dict, out_path: str) -> None:
    styles = _styles()
    doc = SimpleDocTemplate(
        out_path, pagesize=A4,
        leftMargin=2.0*cm, rightMargin=2.0*cm,
        topMargin=1.5*cm, bottomMargin=1.5*cm,
        title=f"Evidora — {meta.get('title','')}-Stress-Test",
    )
    story = []
    story.append(Paragraph("Evidora", styles["title"]))
    story.append(Paragraph(
        f"{meta.get('title','')}-Stress-Test - 20 Claims",
        styles["subtitle"],
    ))

    # Description
    if meta.get("description"):
        story.append(Paragraph(meta["description"], styles["body"]))

    # Methodology / Probe-Strategie
    if meta.get("methodology"):
        story.append(Paragraph(meta["methodology"], styles["body"]))

    # System-Performance table
    summary = _summarize_results(results)
    n = summary["n"]
    v_match = summary["v_match"]
    v_strict = summary["v_strict"]
    pct = f"{(v_match/n*100):.0f} %" if n else "—"
    pct_strict = f"{(v_strict/n*100):.0f} %" if n else "—"
    rows = [
        ["Verdict-Match (Toleranz):",
         f"{v_match} / {n} ({pct})"],
        ["Verdict-Match (strict):",
         f"{v_strict} / {n} ({pct_strict})"],
    ]
    if summary["s_total_eval"] > 0:
        rows.append([
            "Source-Match:",
            f"{summary['s_match']} / {summary['s_total_eval']} (Erwartete Quelle)",
        ])
    rows.append(["False Positives:", str(summary['false_positives'])])
    rows.append(["False Negatives:", str(summary['false_negatives'])])
    rows.append(["Topic-Anzahl im Pack:", meta.get("topic_count", "—")])
    for extra in (meta.get("performance_extras") or []):
        rows.append([extra.get("label","?"), extra.get("value","—")])

    story.append(Paragraph("System-Performance", styles["section"]))
    tbl = Table(rows, colWidths=[5.5*cm, 11.0*cm])
    tbl.setStyle(TableStyle([
        ("FONTNAME", (0,0), (-1,-1), "Helvetica"),
        ("FONTSIZE", (0,0), (-1,-1), 10),
        ("LEADING", (0,0), (-1,-1), 12),
        ("TEXTCOLOR", (0,0), (0,-1), HexColor("#1A4F8B")),
        ("VALIGN", (0,0), (-1,-1), "TOP"),
        ("BOTTOMPADDING", (0,0), (-1,-1), 4),
        ("TOPPADDING", (0,0), (-1,-1), 2),
    ]))
    story.append(tbl)
    story.append(Spacer(1, 0.4*cm))

    # Politische Guardrails (optional)
    if meta.get("guardrails"):
        story.append(Paragraph("Politische Guardrails", styles["section"]))
        story.append(Paragraph(meta["guardrails"], styles["body"]))

    # Distanzierung legitimer Debatten (optional)
    if meta.get("distanzierung"):
        story.append(Paragraph("Distanzierung legitimer Debatten", styles["section"]))
        story.append(Paragraph(meta["distanzierung"], styles["body"]))

    # Build with footer
    today = date.today().isoformat()
    footer_txt = f"evidora.eu - {meta.get('title','')}-Stress-Test, {today}"

    def _draw_footer(canvas, doc_):
        canvas.saveState()
        from reportlab.lib.units import cm as _cm
        canvas.setFont("Helvetica", 8)
        canvas.setFillColor(HexColor("#777777"))
        canvas.drawCentredString(
            A4[0]/2, 1.0*_cm, footer_txt,
        )
        canvas.restoreState()

    doc.build(story, onFirstPage=_draw_footer, onLaterPages=_draw_footer)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--results", required=True, help="Path to stress_test results JSON")
    ap.add_argument("--meta", required=True, help="Path to meta JSON (title, description, etc.)")
    ap.add_argument("--out", required=True, help="Output PDF path")
    args = ap.parse_args()

    with open(args.meta, "r", encoding="utf-8") as f:
        meta = json.load(f)
    results = _load_results(args.results)

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    build_pdf(meta, results, args.out)
    print(f"Wrote {args.out}")


if __name__ == "__main__":
    main()
