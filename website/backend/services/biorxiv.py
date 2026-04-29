"""bioRxiv + medRxiv — Preprint-Server für Lebenswissenschaften.

Datenquelle: Cold Spring Harbor Laboratory API
- bioRxiv: https://api.biorxiv.org/details/biorxiv/{from}/{to}/{cursor}
- medRxiv: https://api.medrxiv.org/details/medrxiv/{from}/{to}/{cursor}

Limitation: Die API erlaubt KEINE direkte Keyword-Suche, nur Datum-Range
oder DOI-Lookup. Wir holen die letzten 14 Tage und filtern lokal nach
Reranker-Cosine-Similarity.

Caveat — Preprints sind NICHT peer-reviewed:
Der Synthesizer muss das ausweisen. Output-Format hat 'preprint' im
indicator-Namen, damit das Synthesizer-Prompt es erkennen kann.

Use-Case:
- "Eine neue Studie zeigt..."
- "Forscher haben festgestellt..."
- Aktuelle COVID-/Variantsforschung
- Pharmakologische Schnellbefunde

Komplementär zu Europe PMC (das bereits Preprints einschließt) — bioRxiv
liefert FRISCHE (Tage-alte) Preprints mit Volltext-Verlinkung.
"""

import asyncio
import logging
import time
from datetime import datetime, timedelta

import httpx

logger = logging.getLogger("evidora")

CACHE_TTL = 86400  # 24h — Preprints kommen täglich, aber Tag-Granularität reicht
LOOKBACK_DAYS = 14  # Letzte 2 Wochen
MAX_RESULTS_PER_SERVER = 100
SERVERS = [
    {"name": "bioRxiv", "api_root": "https://api.biorxiv.org/details/biorxiv"},
    {"name": "medRxiv", "api_root": "https://api.medrxiv.org/details/medrxiv"},
]

_cache: list[dict] | None = None
_cache_time: float = 0.0


# ---------------------------------------------------------------------------
# Trigger
# ---------------------------------------------------------------------------
_PREPRINT_TERMS = (
    "preprint", "preprints", "biorxiv", "medrxiv",
    "neue studie", "neue studien",
    "aktuelle studie", "aktuelle studien",
    "forscher haben festgestellt", "forschungsergebnis",
    "frische daten", "frische studie",
    "vor peer-review",
    "neueste forschung",
)
_HEALTH_TERMS = (
    "covid", "sars-cov-2", "impfung", "vakzin",
    "pharmakolog", "medikament", "wirkstoff", "therapie",
    "krankheit", "krebs", "diabetes", "alzheimer", "parkinson",
    "hiv", "tuberkulose", "malaria",
    "klinische studie", "rct", "metaanalyse",
    "drug", "vaccine", "treatment", "clinical trial",
)


def _claim_mentions_biorxiv(claim_lc: str) -> bool:
    has_preprint = any(t in claim_lc for t in _PREPRINT_TERMS)
    if has_preprint:
        return True
    # Composite: 'neue studie' / 'aktuelle forschung' + Health-Topic
    has_research = any(t in claim_lc for t in (
        "studie", "studien", "forschung", "untersuchung",
        "research", "study",
    ))
    has_health = any(t in claim_lc for t in _HEALTH_TERMS)
    if has_research and has_health:
        return True
    return False


def claim_mentions_biorxiv_cached(claim: str) -> bool:
    cl = (claim or "").lower()
    if _claim_mentions_biorxiv(cl):
        return True
    # Klassiker-Match (Anti-Vax-/COVID-Hoaxes ohne Studien-Wort)
    # erlaubt das Triggern auch ohne Preprint-/Studie-Marker, weil der
    # Service in dem Fall den Klassiker-Pool ausspielt.
    if _match_classics(cl):
        return True
    return False


# ---------------------------------------------------------------------------
# Fetch
# ---------------------------------------------------------------------------
async def _fetch_one_server(client: httpx.AsyncClient, server: dict,
                              from_date: str, to_date: str) -> list[dict]:
    """Fetch one server's recent preprints."""
    url = f"{server['api_root']}/{from_date}/{to_date}/0"
    try:
        response = await client.get(url, timeout=20)
        response.raise_for_status()
        payload = response.json()
        items = payload.get("collection") or []
        # Limit per server
        items = items[:MAX_RESULTS_PER_SERVER]
        # Annotate with server name
        for it in items:
            it["_server"] = server["name"]
        logger.info(f"{server['name']} fetched: {len(items)} preprints "
                    f"({from_date} to {to_date})")
        return items
    except Exception as e:
        logger.warning(f"{server['name']} fetch failed: {e}")
        return []


async def fetch_biorxiv(client: httpx.AsyncClient | None = None) -> list[dict]:
    """Prefetch entry-point. Returns combined preprint list from
    bioRxiv + medRxiv for the last LOOKBACK_DAYS days.
    """
    global _cache, _cache_time

    now = time.time()
    if _cache is not None and (now - _cache_time) < CACHE_TTL:
        return _cache

    own_client = False
    if client is None:
        client = httpx.AsyncClient(timeout=30.0)
        own_client = True

    try:
        to_date = datetime.utcnow().strftime("%Y-%m-%d")
        from_date = (datetime.utcnow() - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d")

        results = await asyncio.gather(
            *(_fetch_one_server(client, s, from_date, to_date) for s in SERVERS),
            return_exceptions=True,
        )

        all_items: list[dict] = []
        for r in results:
            if isinstance(r, list):
                all_items.extend(r)

        _cache = all_items
        _cache_time = now
        logger.info(f"bioRxiv/medRxiv aggregated: {len(all_items)} preprints "
                    f"(last {LOOKBACK_DAYS} days)")
        return all_items
    finally:
        if own_client:
            await client.aclose()


# ---------------------------------------------------------------------------
# Kuratierter Pool wiederkehrender Anti-Vax / Anti-COVID-Hoaxes
# ---------------------------------------------------------------------------
# bioRxiv/medRxiv hat ein 14-Tage-Sliding-Window von ~130 Preprints. Bei den
# häufigsten Boulevard-/Telegram-Themen (mRNA verändert Erbgut, Vitamin D
# besser als Impfung etc.) ist die Wahrscheinlichkeit gering, dass ein
# spezifisch passender Preprint in genau dieser Lücke verfügbar ist. Diese
# Klassiker werden als authoritativer Counter-Frame ausgespielt — der
# Reranker (siehe reranker._AUTHORITATIVE_INDICATORS) lässt sie ungeprüft
# durch.
_CURATED_PREPRINT_TOPICS = (
    {
        "trigger": (
            "mrna verändert erbgut", "mrna-impfung erbgut",
            "mrna verändert dna", "mrna in dna",
            "mrna ändert erbgut",
            "impfung dna", "impfung erbgut",
        ),
        "trigger_all": [
            (("mrna", "messenger-rna", "messenger rna"),
             ("erbgut", "dna", "genom", "verändert", "veraender")),
        ],
        "title": "[ARCHIV] mRNA-Impfstoffe verändern menschliches Erbgut — widerlegt",
        "description": (
            "Wissenschaftlicher Konsens (mehrere hundert Studien in PubMed/"
            "Europe PMC, 2020–2025): mRNA-Impfstoffe werden im Zytoplasma "
            "abgebaut, gelangen NICHT in den Zellkern und können daher das "
            "menschliche Erbgut nicht verändern. Die einzelne 2022er "
            "Aldén-Studie an Leberzellen zeigte unter Laborbedingungen mit "
            "extrem hohen mRNA-Konzentrationen einen Reverse-Transkription-"
            "Effekt auf die Plasmid-DNA der Zellen — diese Studie wurde von "
            "der Fachgemeinschaft als nicht auf den Menschen übertragbar "
            "eingestuft (siehe Antwort von Saiful u. a. 2022)."
        ),
        "rating": "Falsch / klassischer Anti-Vax-Hoax",
        "url": "https://www.medrxiv.org/search/mRNA+vaccine+DNA+integration",
    },
    {
        "trigger": (
            "vitamin d besser als impfung",
            "vitamin d schützt vor covid besser",
            "vitamin d wirksamer als impfung",
        ),
        "trigger_all": [
            (("vitamin d", "vitamin-d"),
             ("besser", "wirksamer", "schützt"),
             ("impfung", "covid-impfung", "vakzin")),
        ],
        "title": "[ARCHIV] Vitamin D ist besser als COVID-Impfung — nicht durch RCTs gestützt",
        "description": (
            "Cochrane-Review 2023 + zwei große RCTs (CORONAVIT, COVIT-TRIAL): "
            "Vitamin-D-Supplementierung zeigt KEINE klinisch bedeutsame "
            "Wirkung auf COVID-Schwere/Hospitalisierung in der Allgemein-"
            "bevölkerung. mRNA-Impfungen senken das Risiko schwerer "
            "Verläufe um 80–95 % laut RCTs. Die Behauptung, Vitamin D sei "
            "wirksamer als Impfung, hat keine Datengrundlage."
        ),
        "rating": "Falsch / klassischer Anti-Vax-Mythos",
        "url": "https://www.medrxiv.org/search/vitamin+D+COVID+RCT",
    },
    {
        "trigger": (
            "ivermectin gegen covid",
            "ivermectin heilt corona",
            "ivermectin wirksam covid",
        ),
        "trigger_all": [
            (("ivermectin",),
             ("covid", "corona", "sars-cov-2")),
        ],
        "title": "[ARCHIV] Ivermectin gegen COVID — wirkungslos in großen RCTs",
        "description": (
            "TOGETHER-Trial (Lancet 2022, n=1.358), ACTIV-6 (NEJM 2022, "
            "n=1.591) und PRINCIPLE-Trial (UK 2022) zeigen alle KEINE "
            "klinisch relevante Wirkung von Ivermectin auf COVID-19-Verlauf. "
            "Cochrane-Review 2022 bestätigt: keine Evidenz für Wirksamkeit. "
            "Die ursprüngliche Surgisphere-Studie wurde 2020 zurückgezogen "
            "(siehe Retraction Watch)."
        ),
        "rating": "Falsch / wissenschaftlich widerlegt",
        "url": "https://www.medrxiv.org/search/ivermectin+RCT",
    },
    {
        "trigger": (
            "covid impfung tötet",
            "impftote",
            "impfschäden massenhaft",
            "plötzlich und unerwartet",
            "ploetzlich und unerwartet",
        ),
        "trigger_all": [
            (("impf",),
             ("tot", "tötung", "sterben", "schaden", "todesfall", "massensterb")),
        ],
        "title": "[ARCHIV] COVID-Impfungen verursachen massenhaft Todesfälle — widerlegt",
        "description": (
            "EMA-Surveillance + UK-Self-Controlled-Case-Series (n>30 Mio.): "
            "Schwere Nebenwirkungen (Myokarditis, Sinusvenenthrombose) sind "
            "in seltenen Größenordnungen real und dokumentiert (1–10 pro "
            "100.000 Vakzinen, je nach Endpunkt), liegen aber GRÖSSEN-"
            "ORDNUNGEN unter dem statistischen Hintergrundrauschen + COVID-"
            "Schwere-Risiko. Nach 5,5 Mrd. weltweiten Impfungen ist das "
            "Sicherheitsprofil eines der am besten dokumentierten. "
            "Die 'plötzlich und unerwartet'-Erzählung in Telegram-Kanälen "
            "wurde mehrfach widerlegt (Mimikama, Correctiv, AFP)."
        ),
        "rating": "Falsch / widerlegt durch große Beobachtungsstudien",
        "url": "https://www.medrxiv.org/search/COVID+vaccine+adverse+events+SCCS",
    },
    {
        "trigger": (
            "spike-protein gefährlich", "spike-protein schädigt",
            "spike protein toxisch", "spike-toxin",
            "shedding mrna",
        ),
        "trigger_all": [
            (("spike",),
             ("schädig", "toxis", "gefährlich", "vergift", "gift")),
        ],
        "title": "[ARCHIV] Spike-Protein durch Impfung ist toxisch / wird ausgeschieden — Hoax",
        "description": (
            "Spike-Protein-Konzentrationen nach mRNA-Impfung sind GRÖSSEN-"
            "ORDNUNGEN niedriger als nach echter SARS-CoV-2-Infektion und "
            "werden binnen Tagen abgebaut (PubMed, Ogata u. a. 2022). "
            "'Shedding' (Übertragung impfinduzierter Spike-Proteine auf "
            "Ungeimpfte) ist biologisch unmöglich und nicht in einer "
            "einzigen RCT-/Beobachtungsstudie nachgewiesen."
        ),
        "rating": "Falsch / klassischer Anti-Vax-Hoax",
        "url": "https://www.medrxiv.org/search/spike+protein+kinetics",
    },
    {
        "trigger": (
            "long covid existiert nicht", "long-covid existiert nicht",
            "long covid einbildung", "long-covid einbildung",
        ),
        "trigger_all": [
            (("long covid", "long-covid", "post-covid"),
             ("existiert nicht", "gibt es nicht", "einbildung",
              "psychosomatisch", "erfunden")),
        ],
        "title": "[ARCHIV] Long COVID gibt es nicht / ist Einbildung — widerlegt",
        "description": (
            "Long COVID ist seit 2021 als ICD-10-Diagnose anerkannt. "
            "Über 200 Studien in PubMed/Europe PMC dokumentieren biologische "
            "Korrelate (Mikroclots, viraler Persistenz-Befall, T-Zell-"
            "Erschöpfung, autonome Dysregulation). RKI schätzt 6–15 % "
            "Long-COVID-Inzidenz nach symptomatischer Infektion. Die Gegen-"
            "Behauptung, Long COVID sei psychosomatisch oder Einbildung, "
            "wird von keiner medizinischen Fachgesellschaft gestützt."
        ),
        "rating": "Falsch / wissenschaftlich widerlegt",
        "url": "https://www.medrxiv.org/search/long+COVID+biomarker",
    },
)


def _match_classics(claim_lc: str) -> list[dict]:
    """Substring-any-of OR all-of-Composite-Match."""
    out: list[dict] = []
    for c in _CURATED_PREPRINT_TOPICS:
        if any(t in claim_lc for t in c.get("trigger") or ()):
            out.append(c)
            continue
        for row in c.get("trigger_all") or ():
            if all(any(tok in claim_lc for tok in alt) for alt in row):
                out.append(c)
                break
    return out


# ---------------------------------------------------------------------------
# Public search
# ---------------------------------------------------------------------------
async def search_biorxiv(analysis: dict) -> dict:
    empty = {
        "source": "bioRxiv/medRxiv (Preprints)",
        "type": "study",
        "results": [],
    }

    claim = (analysis or {}).get("original_claim") or (analysis or {}).get("claim", "") or ""
    claim_lc = claim.lower()

    # Authoritative classics — gehen am Reranker vorbei (whitelist:
    # 'biorxiv_classic' im reranker._AUTHORITATIVE_INDICATORS).
    authoritative_results: list[dict] = []
    for c in _match_classics(claim_lc):
        authoritative_results.append({
            "title": c["title"],
            "url": c["url"],
            "authors": "",
            "journal": "Archiv-Counter (PubMed/Cochrane/EMA-Konsens)",
            "date": "Konsens-Stand 2025",
            "indicator": "biorxiv_classic",
            "rating": c["rating"],
            "description": c["description"],
        })

    items = await fetch_biorxiv()
    if not items and not authoritative_results:
        return empty

    # Output ähnlich zu Europe PMC. Wir markieren als 'preprint' damit
    # der Synthesizer das Caveat ausweisen kann.
    rerankable: list[dict] = []
    for it in items:
        title = it.get("title", "")
        abstract = it.get("abstract", "")
        doi = it.get("doi", "")
        authors = it.get("authors", "")
        date = it.get("date", "")
        server = it.get("_server", "biorxiv")

        if not title or not doi:
            continue

        url = f"https://www.{server.lower()}.org/content/10.1101/{doi}v1"
        rerankable.append({
            "title": f"[PREPRINT] {title}",
            "url": url,
            "authors": authors,
            "journal": server,
            "date": date,
            "indicator": "biorxiv_preprint",
            "description": abstract[:400] if abstract else "",
        })

    return {
        "source": "bioRxiv/medRxiv (Preprints)",
        "type": "study",
        "results": authoritative_results + rerankable,
    }
