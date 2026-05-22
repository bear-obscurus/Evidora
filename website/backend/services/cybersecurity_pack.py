"""Cybersecurity-Pack — kuratierte Konsens-Daten zu IT-Sicherheits-
Mythen für Endnutzer:innen.

Static-First-Topic-Service nach dem Pattern aus ARCHITECTURE.md §3.5.

Inhaltlicher Fokus: technisch-statistische IT-Sicherheits-Konsens-
Aussagen (NIST/BSI/ENISA/EFF/Mozilla-gestützt). Komplementär zum
existierenden tech_ki_pack (das eher Tech-/KI-Mythen behandelt).

Topics (10):
  - passwort_wechsel_intervall_mythos (FALSE — NIST 800-63B 2017
    nahm 90-Tage-Empfehlung zurück)
  - mac_keine_viren_mythos (FALSE — Mac-Malware seit 2010er stark
    gewachsen, Apple XProtect-Updates, MalwareBytes 2024)
  - vpn_anonymitaet_mythos (FALSE — VPN verlagert Vertrauen, kein
    Anonymitäts-Garant; EFF + Citizen Lab)
  - public_wifi_banking_konsens (NUANCED — TLS 1.3 + HSTS schützen
    moderne Banking-Apps weitgehend; VPN als zusätzliche Schicht)
  - cookie_banner_sinn_kontroverse (NUANCED — GDPR-konform-implementiert
    schützt; Dark-Pattern-Banner GDPR-widrig)
  - inkognito_modus_anonymitaet_mythos (FALSE — nur lokale Browser-
    Daten, ISP/Webseiten/Tracker sehen weiterhin)
  - 2fa_unnoetig_mythos (FALSE — Microsoft + Google: 99,9% Risiko-
    Reduktion durch 2FA)
  - passwort_manager_unsicher_mythos (FALSE — Zero-Knowledge-
    Verschlüsselung, NIST/BSI/EFF empfehlen)
  - phishing_email_eindeutig_mythos (FALSE — KI-generierte Phishing
    seit 2023 sprachlich perfekt; URL/Kontext-Prüfung effektiver)
  - antivirus_linux_unnoetig_konsens (TRUE für Endnutzer-Desktops,
    DIFFERENZIERT für Server)

Quellen-Mix: NIST Special Publications (800-63B Digital Identity
Guidelines), BSI Bundesamt für Sicherheit in der Informationstechnik,
ENISA European Union Agency for Cybersecurity, EFF Electronic Frontier
Foundation, Mozilla, Apple Security Documentation, Microsoft Identity
Security Reports, Google Security Blog, MalwareBytes State of Malware
Reports, Sentinel One Threat Reports, NCSC UK, GDPR + EDPB Guidelines,
KnowBe4 Phishing Reports, FIDO Alliance, Yubico, peer-reviewed Studien
(Zhang 2010 ACM CCS, Komanduri 2011, Mathur 2019 ACM CSCW).

Politische Sensibilität: niedrig — alle technisch-statistisch
unkontrovers.
"""

import logging
import os

from services._topic_match import find_matching_items, load_items

logger = logging.getLogger("evidora")

STATIC_JSON_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "cybersecurity_pack.json",
)


def _descriptor(f: dict) -> tuple[dict, str]:
    head = f.get("headline", "")
    notes = " ".join((f.get("context_notes") or [])[:2])
    return (f, f"{head}. {notes}"[:300])


def _claim_matches_facts(claim_lc: str, full_claim: str | None = None) -> list[dict]:
    return find_matching_items(
        STATIC_JSON_PATH, "facts",
        claim_lc=claim_lc, full_claim=full_claim,
        descriptor_fn=_descriptor,
    )


def claim_mentions_cybersecurity_cached(claim: str) -> bool:
    if not claim:
        return False
    return bool(_claim_matches_facts(claim.lower(), full_claim=claim))


async def fetch_cybersecurity(client=None):
    return load_items(STATIC_JSON_PATH, "facts")


def _data_lines(d: dict) -> str:
    """Render data-dict via geteilten STRUKTURELL-Marker-Helper.

    Aktiviert STRUKTURELL-FALSCH-Prefix bei kernsatz_fuer_synthesizer
    mit Override-Token (siehe services/_struct_marker.py).
    """
    from services._struct_marker import render_data_with_marker
    return render_data_with_marker(d)


async def search_cybersecurity(analysis: dict) -> dict:
    empty = {
        "source": "Cybersecurity-Konsens (NIST + BSI + ENISA + EFF + Mozilla + NCSC)",
        "type": "cybersecurity_consensus",
        "results": [],
    }

    claim = (analysis or {}).get("original_claim") or (analysis or {}).get("claim", "") or ""
    matches = _claim_matches_facts(claim.lower(), full_claim=claim)
    if not matches:
        return empty

    results: list[dict] = []
    for fact in matches:
        topic = fact.get("topic", "")
        d = fact.get("data") or {}
        url = fact.get("source_url", "")
        secondary = fact.get("secondary_url", "")
        label = fact.get("source_label", "NIST / BSI / ENISA / EFF / Mozilla / NCSC")
        notes = fact.get("context_notes") or []
        notes_joined = " | ".join(notes)
        year = str(fact.get("year", ""))

        display = f"{fact.get('headline', '?')}. {_data_lines(d)}"
        description = (
            (d.get("context", "") + " " + notes_joined).strip()
        )

        results.append({
            "indicator_name": fact.get("headline", "?"),
            "indicator": "cybersecurity_konsens_fact",
            "country": "—",
            "year": year,
            "topic": topic,
            "display_value": display,
            "description": description,
            "url": url,
            "secondary_url": secondary,
            "source": label,
        })

    return {
        "source": "Cybersecurity-Konsens (NIST + BSI + ENISA + EFF + Mozilla + NCSC)",
        "type": "cybersecurity_consensus",
        "results": results,
    }
