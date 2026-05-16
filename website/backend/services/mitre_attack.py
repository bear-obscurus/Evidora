"""MITRE ATT&CK Framework — Adversary-TTP Lookup (Hybrid Static-Pack).

Datenquelle: https://github.com/mitre-attack/attack-stix-data — STIX-2.x-Dumps
des ATT&CK-Frameworks (Enterprise + Mobile + ICS). Wir laden primär das
Enterprise-Dataset (~47 MB Vollformat) im Prefetch und filtern es auf
4 relevante Object-Typen (intrusion-set + attack-pattern + malware + tool)
herunter (~5 MB). Spätere Calls lesen aus dieser reduzierten Datei.

Lizenz: MITRE Open License (royalty-free, kommerziell erlaubt) — Evidora-tauglich.

Komplementär zu OSV/NVD: OSV+NVD liefern Vulnerability-Daten (CVE-Level),
MITRE ATT&CK liefert TTP-Daten (wer/wie wird angegriffen — APT-Gruppen,
Techniken-IDs T1059 etc., Malware-Familien).

Trigger-Strategie:
  1. Technique-ID-Regex (T\\d{4}(\\.\\d{3})?)   → attack-pattern-Lookup
  2. Group-ID-Regex (G\\d{4})                  → intrusion-set-Lookup
  3. Software-ID-Regex (S\\d{4})               → malware/tool-Lookup
  4. APT-Name oder Alias-Match                 → intrusion-set
  5. Generische Threat-Keywords + Composite-Trigger

Politische Guardrails: Attribution ("staatlich gesteuert" etc.) wird AUSSCHLIESSLICH
aus der MITRE-Description übernommen und mit "laut MITRE-Attribution" gekennzeichnet.
NIE eigene Schuldzuweisung an Staaten oder Akteure.
"""

# WIRING für main.py:
# from services.mitre_attack import search_mitre_attack, claim_mentions_mitre_cached
# if claim_mentions_mitre_cached(claim):
#     tasks.append(cached("MITRE ATT&CK", search_mitre_attack, analysis))
#     queried_names.append("MITRE ATT&CK")
# data_updater.py:
# from services.mitre_attack import fetch_mitre_attack
# in prefetch_all(): fetch_mitre_attack(client)

from __future__ import annotations

import json
import logging
import os
import re
import time
from pathlib import Path

from services._http_polite import polite_client

logger = logging.getLogger("evidora")

STIX_URL = (
    "https://raw.githubusercontent.com/mitre/cti/master/"
    "enterprise-attack/enterprise-attack.json"
)

DATA_DIR = Path(__file__).parent.parent / "data"
LOCAL_PATH = DATA_DIR / "mitre_attack.json"

TIMEOUT_S = 60.0  # 47 MB STIX-Download
CACHE_TTL_S = 24 * 60 * 60  # 24 h
MAX_RESULTS = 5
KEEP_TYPES = ("intrusion-set", "attack-pattern", "malware", "tool")

# Modul-Level Cache: gefiltertes STIX-Dataset
_dataset: dict | None = None
_dataset_ts: float = 0.0


# ---------------------------------------------------------------------------
# Regex + Trigger-Whitelists
# ---------------------------------------------------------------------------
# Technique-ID T1059, T1059.001 (Sub-Technique)
_TECH_ID_REGEX = re.compile(r"\bT\d{4}(?:\.\d{3})?\b", re.IGNORECASE)
# Group-ID G0007
_GROUP_ID_REGEX = re.compile(r"\bG\d{4}\b", re.IGNORECASE)
# Software-ID S0061
_SOFT_ID_REGEX = re.compile(r"\bS\d{4}\b", re.IGNORECASE)

# Bekannte APT-Namen + Aliase, die ohne weiteren Kontext triggern sollen.
# Konservativ: nur prominente Gruppen mit eindeutigen Namen (sonst False-Positives).
_APT_NAMES = (
    "apt28", "apt29", "apt32", "apt33", "apt38", "apt40", "apt41",
    "lazarus", "fancy bear", "cozy bear", "sandworm", "turla",
    "equation group", "carbanak", "fin7", "fin8",
    "wicked panda", "double dragon", "winnti",
    "kimsuky", "andariel", "guardians of peace", "hidden cobra",
    "mustang panda", "stone panda", "leviathan", "deep panda",
    "menupass", "wizard spider", "ryuk gang",
    "darkside", "conti gang", "revil", "blackcat",
    "indrik spider", "evil corp",
)

# Threat-Keywords (generisch — triggern nur in Composite mit weiteren Hints)
_THREAT_KW = (
    "apt-gruppe", "apt gruppe", "apt group", "advanced persistent threat",
    "ttps", "ttp", "mitre att&ck", "mitre attck", "mitre att ck",
    "att&ck-framework", "attack framework",
    "spear-phishing", "spear phishing", "phishing-kampagne",
    "phishing-technik",
    "privilege escalation", "privilegien-eskalation", "rechte-ausweitung",
    "lateral movement", "laterale bewegung",
    "credential access", "credential dumping", "credential harvesting",
    "command and control", "c2 server", "c&c server",
    "initial access", "exfiltration",
    "threat actor", "threat-actor", "bedrohungsakteur",
    "staatlich gesteuerte hacker", "staatlich gesteuerter hacker",
    "staatlich gesteuert", "state-sponsored",
)

# Composite-Trigger: Nation-Keywords + Hacker/Cyber-Kontext (zusätzlich)
_NATION_HACKER = (
    "russland-hacker", "russische hacker", "russia-linked",
    "nordkorea-hacker", "nordkoreanische hacker",
    "china-hacker", "chinesische hacker", "china-linked",
    "iran-hacker", "iranische hacker", "iran-linked",
)


def _claim_mentions_mitre(claim_lc: str) -> bool:
    """Public-Trigger-Pre-Check.

    True wenn:
      - Technique-/Group-/Software-ID erkannt, ODER
      - APT-Name oder Alias direkt vorhanden, ODER
      - "MITRE ATT&CK" / "TTPs" explizit erwähnt, ODER
      - Nation+Hacker-Composite (russische hacker etc.).
    """
    if not claim_lc:
        return False
    if (_TECH_ID_REGEX.search(claim_lc)
            or _GROUP_ID_REGEX.search(claim_lc)
            or _SOFT_ID_REGEX.search(claim_lc)):
        return True
    if any(n in claim_lc for n in _APT_NAMES):
        return True
    if any(n in claim_lc for n in _NATION_HACKER):
        return True
    # MITRE/TTPs explizit
    if "mitre att" in claim_lc or "att&ck" in claim_lc:
        return True
    if "ttps" in claim_lc or "ttp-mapping" in claim_lc:
        return True
    # Composite: Threat-KW + Cyber/Hack-Kontext
    has_threat = any(k in claim_lc for k in _THREAT_KW)
    has_cyber = any(c in claim_lc for c in (
        "hack", "cyber", "angriff", "attack", "intrusion",
        "malware", "ransomware", "trojaner", "spionage",
    ))
    if has_threat and has_cyber:
        return True
    return False


def claim_mentions_mitre_cached(claim: str) -> bool:
    return _claim_mentions_mitre((claim or "").lower())


# ---------------------------------------------------------------------------
# Country-Attribution-Extraction (aus MITRE-Description)
# ---------------------------------------------------------------------------
# Stichwörter → (ISO3, deutscher Name). Gewichtet nach Spezifität.
_COUNTRY_MAP: list[tuple[re.Pattern, str, str]] = [
    (re.compile(r"\bnorth korea(n)?\b|\bdprk\b", re.IGNORECASE),
     "PRK", "Nordkorea"),
    (re.compile(r"\bnord-korea\b|\bnordkorea\b", re.IGNORECASE),
     "PRK", "Nordkorea"),
    (re.compile(r"\brussia(n)?\b|\bgru\b|\bsvr\b|\brussian (?:federation|government)\b",
                re.IGNORECASE),
     "RUS", "Russland"),
    (re.compile(r"\brussland\b|\brussisch", re.IGNORECASE),
     "RUS", "Russland"),
    (re.compile(r"\bchina|\bchinese\b|\bprc\b|\bpla\b", re.IGNORECASE),
     "CHN", "China"),
    (re.compile(r"\bchinesisch", re.IGNORECASE),
     "CHN", "China"),
    (re.compile(r"\biran(ian)?\b", re.IGNORECASE),
     "IRN", "Iran"),
    (re.compile(r"\biranisch", re.IGNORECASE),
     "IRN", "Iran"),
    (re.compile(r"\bvietnam(ese)?\b", re.IGNORECASE),
     "VNM", "Vietnam"),
    (re.compile(r"\bbelarus(ian)?\b", re.IGNORECASE),
     "BLR", "Belarus"),
    (re.compile(r"\bpakistan(i)?\b", re.IGNORECASE),
     "PAK", "Pakistan"),
    (re.compile(r"\bturk(ey|ish)\b", re.IGNORECASE),
     "TUR", "Türkei"),
]


def _country_from_description(desc: str) -> tuple[str, str]:
    """Erkenne Land aus MITRE-Description. Default = GLOBAL/unbekannt."""
    if not desc:
        return ("GLOBAL", "Global / nicht zugeordnet")
    # Cut to first 500 chars (state-attribution kommt fast immer früh)
    head = desc[:500]
    for pat, iso3, de_name in _COUNTRY_MAP:
        if pat.search(head):
            return (iso3, f"{de_name} (laut MITRE-Attribution)")
    return ("GLOBAL", "Keine staatliche Attribution durch MITRE")


_YEAR_REGEX = re.compile(r"(?:since|seit|active since|operating since)\s+(?:at least\s+)?(\d{4})", re.IGNORECASE)


def _year_from_description(desc: str, created: str = "") -> str:
    """Extrahiere First-Seen-Jahr aus Description; Fallback = created-Jahr."""
    if desc:
        m = _YEAR_REGEX.search(desc)
        if m:
            return m.group(1)
    if created and len(created) >= 4 and created[:4].isdigit():
        return created[:4]
    return "—"


# ---------------------------------------------------------------------------
# STIX-Loading + Filtering
# ---------------------------------------------------------------------------
async def _download_and_filter(client) -> list[dict] | None:
    """Download enterprise-attack.json (~47 MB) und filter auf KEEP_TYPES."""
    try:
        logger.info(f"MITRE ATT&CK: downloading full STIX from {STIX_URL}")
        resp = await client.get(STIX_URL, follow_redirects=True)
        if resp.status_code != 200:
            logger.warning(f"MITRE ATT&CK download HTTP {resp.status_code}")
            return None
        data = resp.json()
        if not isinstance(data, dict) or "objects" not in data:
            logger.warning("MITRE ATT&CK download: malformed STIX (no 'objects')")
            return None
        objects = data.get("objects") or []
        filtered = [
            o for o in objects
            if isinstance(o, dict)
            and o.get("type") in KEEP_TYPES
            and not o.get("revoked")
            and not o.get("x_mitre_deprecated")
        ]
        logger.info(
            f"MITRE ATT&CK: filtered {len(filtered)} objects from "
            f"{len(objects)} (kept types: {KEEP_TYPES})"
        )
        return filtered
    except Exception as e:
        logger.warning(f"MITRE ATT&CK download failed: {e}")
        return None


def _save_local(filtered: list[dict]) -> None:
    try:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        payload = {
            "source": "MITRE ATT&CK Framework (Enterprise)",
            "source_url": "https://attack.mitre.org/",
            "stix_origin": STIX_URL,
            "license": "MITRE Open License (royalty-free)",
            "fetched_at": int(time.time()),
            "object_count": len(filtered),
            "objects": filtered,
        }
        with open(LOCAL_PATH, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False)
        logger.info(
            f"MITRE ATT&CK: saved {len(filtered)} objects to {LOCAL_PATH} "
            f"({os.path.getsize(LOCAL_PATH)//1024} KB)"
        )
    except Exception as e:
        logger.warning(f"MITRE ATT&CK: failed to save local index: {e}")


def _load_local() -> dict | None:
    global _dataset, _dataset_ts
    if _dataset is not None:
        return _dataset
    if not LOCAL_PATH.exists():
        logger.info(f"MITRE ATT&CK: local file not found at {LOCAL_PATH}")
        return None
    try:
        with open(LOCAL_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict) or "objects" not in data:
            logger.warning("MITRE ATT&CK: local file malformed")
            return None
        _dataset = data
        _dataset_ts = float(data.get("fetched_at") or time.time())
        logger.info(
            f"MITRE ATT&CK: loaded {len(data.get('objects', []))} "
            f"objects from local cache"
        )
        return _dataset
    except Exception as e:
        logger.warning(f"MITRE ATT&CK: failed to load local file: {e}")
        return None


async def fetch_mitre_attack(client=None) -> list:
    """Prefetch-Hook für data_updater.

    Strategie:
      - Wenn lokal vorhanden + jünger als CACHE_TTL_S → kein Download.
      - Sonst: Download + Filter + Save.
    Return-Wert: list mit dem (gefilterten) Dataset-Dict für API-Kompatibilität
    mit anderen fetch_*-Hooks; bei Fehler [].
    """
    global _dataset, _dataset_ts, _indices

    # Schon im Modul gecached und frisch?
    if _dataset is not None and (time.time() - _dataset_ts) < CACHE_TTL_S:
        return [_dataset]

    # Lokale Datei jung genug?
    if LOCAL_PATH.exists():
        try:
            mtime = LOCAL_PATH.stat().st_mtime
            if (time.time() - mtime) < CACHE_TTL_S:
                loaded = _load_local()
                if loaded:
                    return [loaded]
        except Exception:
            pass

    # Sonst: Download + Filter + Save
    own_client = False
    if client is None:
        client = polite_client(timeout=TIMEOUT_S)
        own_client = True
    try:
        filtered = await _download_and_filter(client)
        if not filtered:
            # Fallback: lokale (evtl. veraltete) Datei
            return [_load_local()] if _load_local() else []
        _save_local(filtered)
        # Cache aktualisieren
        _dataset = {
            "source": "MITRE ATT&CK Framework (Enterprise)",
            "source_url": "https://attack.mitre.org/",
            "stix_origin": STIX_URL,
            "license": "MITRE Open License (royalty-free)",
            "fetched_at": int(time.time()),
            "object_count": len(filtered),
            "objects": filtered,
        }
        _dataset_ts = time.time()
        # Invalidate indices so they get rebuilt on next search
        _indices = None
        return [_dataset]
    finally:
        if own_client:
            await client.aclose()


# ---------------------------------------------------------------------------
# Search-Indizes (lazy)
# ---------------------------------------------------------------------------
_indices: dict | None = None


def _build_indices(dataset: dict) -> dict:
    """Baue Schlüssel→Object-Maps für O(1)-Lookup."""
    by_attack_id: dict[str, dict] = {}     # T1059, G0007, S0061
    by_name_lc: dict[str, dict] = {}        # Name + Aliase, lower-case
    intrusion_sets: list[dict] = []
    attack_patterns: list[dict] = []
    software: list[dict] = []

    for obj in dataset.get("objects") or []:
        t = obj.get("type")
        # Attack-ID aus external_references
        attack_id = None
        for ref in obj.get("external_references") or []:
            if ref.get("source_name") == "mitre-attack":
                attack_id = ref.get("external_id")
                break
        if attack_id:
            by_attack_id[attack_id.upper()] = obj
            obj["_attack_id"] = attack_id

        # Namen + Aliase (case-insensitive)
        name = obj.get("name") or ""
        if name:
            by_name_lc[name.lower()] = obj
        aliases = obj.get("aliases") or obj.get("x_mitre_aliases") or []
        for a in aliases:
            if isinstance(a, str) and a:
                by_name_lc.setdefault(a.lower(), obj)

        if t == "intrusion-set":
            intrusion_sets.append(obj)
        elif t == "attack-pattern":
            attack_patterns.append(obj)
        elif t in ("malware", "tool"):
            software.append(obj)

    return {
        "by_attack_id": by_attack_id,
        "by_name_lc": by_name_lc,
        "intrusion_sets": intrusion_sets,
        "attack_patterns": attack_patterns,
        "software": software,
    }


def _get_indices() -> dict | None:
    global _indices
    if _indices is not None:
        return _indices
    data = _load_local()
    if not data:
        return None
    _indices = _build_indices(data)
    logger.info(
        f"MITRE ATT&CK indices: {len(_indices['intrusion_sets'])} groups, "
        f"{len(_indices['attack_patterns'])} techniques, "
        f"{len(_indices['software'])} software"
    )
    return _indices


# ---------------------------------------------------------------------------
# Result-Formatters
# ---------------------------------------------------------------------------
_MD_LINK_REGEX = re.compile(r"\[([^\]]+)\]\([^\)]+\)")
_MD_CITATION_REGEX = re.compile(r"\(Citation:[^\)]+\)")


def _clean_description(desc: str) -> str:
    """Strip markdown-Links und (Citation: ...) — STIX nutzt beides."""
    if not desc:
        return ""
    out = _MD_CITATION_REGEX.sub("", desc)
    out = _MD_LINK_REGEX.sub(r"\1", out)
    return re.sub(r"\s+", " ", out).strip()


def _format_group(obj: dict) -> dict | None:
    aid = obj.get("_attack_id") or ""
    if not aid:
        return None
    name = obj.get("name") or aid
    aliases = obj.get("aliases") or []
    desc_full = _clean_description(obj.get("description") or "")
    iso3, country_name = _country_from_description(desc_full)
    year = _year_from_description(desc_full, obj.get("created") or "")

    primary_aliases = [a for a in aliases if a.lower() != name.lower()][:4]
    alias_str = (
        f" (auch {', '.join(primary_aliases)})" if primary_aliases else ""
    )

    # display_value: kompakte Headline
    if iso3 != "GLOBAL":
        display = (
            f"{aid} {name}{alias_str} — {country_name}, "
            f"aktive Operations seit {year}"
        )
    else:
        display = (
            f"{aid} {name}{alias_str} — {country_name}, "
            f"erste MITRE-Erfassung {year}"
        )

    description = desc_full[:900] + (
        " Quelle: MITRE ATT&CK Framework. Attributions-Aussagen sind "
        "ATT&CK-Eigenangaben (laut MITRE-Attribution); Evidora trifft "
        "keine eigene staatliche Schuldzuweisung."
    )

    return {
        "indicator_name": f"{aid} {name}{alias_str}"[:280],
        "indicator": f"mitre_{aid.lower()}",
        "country": iso3,
        "country_name": country_name,
        "year": year,
        "value": None,
        "display_value": display[:500],
        "description": description[:1200],
        "url": f"https://attack.mitre.org/groups/{aid}/",
        "source": "MITRE ATT&CK Framework (royalty-free)",
    }


def _format_technique(obj: dict) -> dict | None:
    aid = obj.get("_attack_id") or ""
    if not aid:
        return None
    name = obj.get("name") or aid
    desc_full = _clean_description(obj.get("description") or "")
    year = (obj.get("created") or "—")[:4]
    if not year.isdigit():
        year = "—"
    platforms = obj.get("x_mitre_platforms") or []
    is_sub = "." in aid

    display = (
        f"{aid} {name} — {'Sub-Technik' if is_sub else 'Technik'}, "
        f"Plattformen: {', '.join(platforms[:4]) or 'unspez.'}"
    )

    description = desc_full[:900] + (
        " Quelle: MITRE ATT&CK Framework, Enterprise Matrix. Technik-IDs "
        "kategorisieren beobachtete Adversary-Verhaltensweisen — sie sind "
        "deskriptiv, NICHT eine Schwere-Bewertung (laut MITRE-Attribution)."
    )

    # Sub-Technique-URL: /techniques/T1059/001/
    if is_sub:
        parent, sub = aid.split(".", 1)
        url = f"https://attack.mitre.org/techniques/{parent}/{sub}/"
    else:
        url = f"https://attack.mitre.org/techniques/{aid}/"

    return {
        "indicator_name": f"{aid} {name}"[:280],
        "indicator": f"mitre_{aid.lower().replace('.', '_')}",
        "country": "GLOBAL",
        "country_name": "ATT&CK-Technik (Global)",
        "year": year,
        "value": None,
        "display_value": display[:500],
        "description": description[:1200],
        "url": url,
        "source": "MITRE ATT&CK Framework (royalty-free)",
    }


def _format_software(obj: dict) -> dict | None:
    aid = obj.get("_attack_id") or ""
    if not aid:
        return None
    name = obj.get("name") or aid
    aliases = obj.get("x_mitre_aliases") or []
    labels = obj.get("labels") or []
    kind = "Malware" if "malware" in labels else "Tool"
    desc_full = _clean_description(obj.get("description") or "")
    iso3, country_name = _country_from_description(desc_full)
    year = _year_from_description(desc_full, obj.get("created") or "")

    primary_aliases = [a for a in aliases if a.lower() != name.lower()][:3]
    alias_str = (
        f" (auch {', '.join(primary_aliases)})" if primary_aliases else ""
    )

    display = f"{aid} {name}{alias_str} — {kind} ({country_name})"

    description = desc_full[:900] + (
        f" Quelle: MITRE ATT&CK Framework, Software-Datenbank. "
        f"Klassifizierung als {kind} entspricht MITRE-Einschätzung "
        f"(laut MITRE-Attribution)."
    )

    return {
        "indicator_name": f"{aid} {name}{alias_str}"[:280],
        "indicator": f"mitre_{aid.lower()}",
        "country": iso3,
        "country_name": country_name,
        "year": year,
        "value": None,
        "display_value": display[:500],
        "description": description[:1200],
        "url": f"https://attack.mitre.org/software/{aid}/",
        "source": "MITRE ATT&CK Framework (royalty-free)",
    }


def _format_object(obj: dict) -> dict | None:
    t = obj.get("type")
    if t == "intrusion-set":
        return _format_group(obj)
    if t == "attack-pattern":
        return _format_technique(obj)
    if t in ("malware", "tool"):
        return _format_software(obj)
    return None


# ---------------------------------------------------------------------------
# Public search
# ---------------------------------------------------------------------------
async def search_mitre_attack(analysis: dict) -> dict:
    """Lookup in geladenem (gefiltertem) ATT&CK-Dataset.

    Strategie:
      1. Technique-/Group-/Software-ID-Regex → direkter Lookup.
      2. Name/Alias-Match in by_name_lc.
      3. Fallback: erste Top-3 intrusion-sets, deren Aliase im Claim
         vorkommen (für Composite-Trigger).
    """
    empty = {
        "source": "MITRE ATT&CK",
        "type": "threat_intelligence",
        "results": [],
    }

    analysis = analysis or {}
    claim = analysis.get("claim") or ""
    original = analysis.get("original_claim") or claim
    if not isinstance(claim, str):
        claim = str(claim or "")
    if not isinstance(original, str):
        original = str(original or "")
    combined = f"{original} {claim}"
    combined_lc = combined.lower()

    if not _claim_mentions_mitre(combined_lc):
        return empty

    # Wenn Dataset noch nicht da → einmaliger Sync-Versuch
    indices = _get_indices()
    if indices is None:
        # Cold-Start: versuche Live-Download falls Datei fehlt
        try:
            await fetch_mitre_attack()
        except Exception as e:
            logger.debug(f"MITRE ATT&CK cold-start fetch failed: {e}")
        indices = _get_indices()
    if indices is None:
        logger.info("MITRE ATT&CK: dataset nicht verfügbar — leeres Ergebnis")
        return empty

    by_attack_id = indices["by_attack_id"]
    by_name_lc = indices["by_name_lc"]
    intrusion_sets = indices["intrusion_sets"]

    results: list[dict] = []
    seen_ids: set[str] = set()

    def _add(obj: dict | None):
        if obj is None:
            return
        formatted = _format_object(obj)
        if formatted is None:
            return
        ind = formatted["indicator"]
        if ind in seen_ids:
            return
        seen_ids.add(ind)
        results.append(formatted)

    # 1) ID-Regex-Treffer
    for rx in (_TECH_ID_REGEX, _GROUP_ID_REGEX, _SOFT_ID_REGEX):
        for m in rx.findall(combined):
            if len(results) >= MAX_RESULTS:
                break
            key = m.upper()
            obj = by_attack_id.get(key)
            if obj is not None:
                _add(obj)

    # 2) Name/Alias-Direkt-Lookup
    if len(results) < MAX_RESULTS:
        # Längere Tokens zuerst (vermeidet "apt28" trifft auch "apt2")
        candidates = sorted(by_name_lc.keys(), key=len, reverse=True)
        for token in candidates:
            if len(results) >= MAX_RESULTS:
                break
            if len(token) < 5:  # Schutz vor sehr kurzen Aliases
                continue
            # Wortgrenzen erzwingen
            pat = r"(?<!\w)" + re.escape(token) + r"(?!\w)"
            if re.search(pat, combined_lc):
                _add(by_name_lc[token])

    # 3) Composite-Fallback: bei "russische hacker" o.ä. Top-3
    if not results and any(n in combined_lc for n in _NATION_HACKER):
        # Country-Filter aus dem Claim ableiten
        target_iso = None
        if any(s in combined_lc for s in ("russland", "russisch", "russia")):
            target_iso = "RUS"
        elif any(s in combined_lc for s in ("nordkorea", "north korea")):
            target_iso = "PRK"
        elif any(s in combined_lc for s in ("china", "chinesisch")):
            target_iso = "CHN"
        elif any(s in combined_lc for s in ("iran", "iranisch")):
            target_iso = "IRN"

        if target_iso:
            matches = []
            for g in intrusion_sets:
                desc = _clean_description(g.get("description") or "")
                iso, _ = _country_from_description(desc)
                if iso == target_iso:
                    matches.append(g)
            for g in matches[:3]:
                _add(g)

    if not results:
        logger.info(
            f"MITRE ATT&CK: 0 Treffer (claim_lc head: '{combined_lc[:80]}')"
        )
        return empty

    logger.info(
        f"MITRE ATT&CK: {len(results)} Treffer geliefert "
        f"(IDs: {[r['indicator'] for r in results]})"
    )
    return {
        "source": "MITRE ATT&CK",
        "type": "threat_intelligence",
        "results": results,
    }
