"""DeFiLlama — DeFi/Crypto TVL-Aggregator (350+ Chains, 5.000+ Protokolle).

DeFiLlama (https://defillama.com/) ist der grösste offene Aggregator für
Total Value Locked (TVL) in DeFi-Protokollen, Stablecoin-Marktdaten und
Cross-Chain-Volumen.

Für Evidora ergänzt der Service die bestehenden Finanz-/Wirtschafts-
Quellen um den DeFi-/Crypto-Daten-Layer:
- TVL pro Protokoll (Aave, Uniswap, Lido, MakerDAO etc.)
- Stablecoin-Marktkapitalisierungen (USDT, USDC, DAI etc.)
- Chain-TVL (Ethereum, Solana, Tron etc.)

API: https://api.llama.fi/ (Hauptdaten) und https://stablecoins.llama.fi/
(Stablecoins). Kein Auth, kein dokumentiertes Rate-Limit — wir bleiben
zurückhaltend (polite_client + 24h-Cache + Limit 3 Treffer).

Lizenz: MIT (Code), Open Source. Daten frei verwendbar.

Trigger-Strategie (CONSERVATIVE — Crypto ist nicht Evidora-Kern):
1. Direkt-Trigger: "defillama" / "defi llama"
2. Whitelist-Trigger: 30 bekannte DeFi-Protokoll-Namen + DeFi-Term
3. Composite: "tvl" / "total value locked" + Protokoll/Chain
4. Stablecoin-Term + bekannter Stablecoin-Name
5. NIEMALS Bitcoin-Preis-Claims triggern (kein DeFi → andere Domain)

WICHTIG — Politische / Markt-Guardrails:
- NUR deskriptive Markt-Zahlen, KEINE Investment-Bewertung
- KEINE Bewertung "Krypto ist Betrug" / "DeFi ist die Zukunft"
- Bei TVL-Schwankungen neutral berichten — Märkte sind volatil
"""

# WIRING für main.py:
# from services.defillama import search_defillama, claim_mentions_defillama_cached
# if claim_mentions_defillama_cached(claim):
#     tasks.append(cached("DeFiLlama", search_defillama, analysis))
#     queried_names.append("DeFiLlama")

from __future__ import annotations

import logging
import time
from urllib.parse import quote

from services._http_polite import polite_client

logger = logging.getLogger("evidora")

DEFILLAMA_API = "https://api.llama.fi"
STABLECOINS_API = "https://stablecoins.llama.fi"
TIMEOUT_S = 15.0
MAX_RESULTS = 3
CACHE_TTL_S = 24 * 3600  # 24h

# ---------------------------------------------------------------------------
# Hardcoded Whitelist: 30 bekannte DeFi-Protokoll-Slugs (lowercase)
# Slug = der Wert, der gegen /protocol/{slug} der API gestellt wird.
# Mehrere User-Visible-Namen können auf denselben Slug zeigen.
# ---------------------------------------------------------------------------
_DEFI_PROTOCOLS: dict[str, dict] = {
    # Lending / Borrowing
    "aave": {"slug": "aave", "name": "Aave",
             "desc": "Top DeFi Lending Protocol. Multi-chain (Eth/Avalanche/Polygon/Optimism/Arbitrum)"},
    "compound": {"slug": "compound-finance", "name": "Compound",
                 "desc": "Algorithmic Lending Protocol auf Ethereum (cTokens)"},
    "morpho": {"slug": "morpho", "name": "Morpho",
               "desc": "Optimierte Lending-Schicht auf Aave/Compound"},
    "spark": {"slug": "spark", "name": "Spark",
              "desc": "MakerDAO-Lending-Sub-DAO mit DAI-Fokus"},
    # DEX / AMM
    "uniswap": {"slug": "uniswap", "name": "Uniswap",
                "desc": "Grösste DEX (Decentralized Exchange) — AMM-Pionier auf Ethereum"},
    "curve": {"slug": "curve-finance", "name": "Curve",
              "desc": "DEX spezialisiert auf Stablecoin-Swaps"},
    "balancer": {"slug": "balancer", "name": "Balancer",
                 "desc": "Multi-Token AMM mit gewichteten Pools"},
    "pancakeswap": {"slug": "pancakeswap-amm", "name": "PancakeSwap",
                    "desc": "Grösste DEX auf BNB-Chain"},
    "sushiswap": {"slug": "sushi", "name": "SushiSwap",
                  "desc": "Multi-Chain-DEX, Uniswap-V2-Fork"},
    # Liquid Staking
    "lido": {"slug": "lido", "name": "Lido",
             "desc": "Liquid-Staking-Protokoll, stETH — grösster ETH-Staker"},
    "rocket pool": {"slug": "rocket-pool", "name": "Rocket Pool",
                    "desc": "Dezentrales Ethereum-Liquid-Staking (rETH)"},
    "rocketpool": {"slug": "rocket-pool", "name": "Rocket Pool",
                   "desc": "Dezentrales Ethereum-Liquid-Staking (rETH)"},
    "jito": {"slug": "jito", "name": "Jito",
             "desc": "Solana-Liquid-Staking + MEV-Distribution"},
    # CDP / Stablecoins
    "makerdao": {"slug": "makerdao", "name": "MakerDAO",
                 "desc": "Erstes DeFi-CDP-Protokoll — emittiert DAI-Stablecoin"},
    "maker": {"slug": "makerdao", "name": "MakerDAO",
              "desc": "Erstes DeFi-CDP-Protokoll — emittiert DAI-Stablecoin"},
    "sky": {"slug": "sky-lending", "name": "Sky (Maker)",
            "desc": "Rebranding von MakerDAO 2024 — USDS-Stablecoin"},
    "liquity": {"slug": "liquity-v1", "name": "Liquity",
                "desc": "ETH-besichertes CDP-Protokoll, LUSD-Stablecoin"},
    "frax": {"slug": "frax-finance", "name": "Frax",
             "desc": "Fractional-Algorithmic-Stablecoin + DeFi-Stack"},
    # Perps / Derivatives
    "gmx": {"slug": "gmx", "name": "GMX",
            "desc": "On-Chain Perpetuals-DEX (Arbitrum/Avalanche)"},
    "dydx": {"slug": "dydx", "name": "dYdX",
             "desc": "Perpetual Futures DEX (eigene App-Chain)"},
    "hyperliquid": {"slug": "hyperliquid", "name": "Hyperliquid",
                    "desc": "Order-Book-Perpetuals auf eigener L1"},
    # Bridges / Cross-Chain
    "stargate": {"slug": "stargate", "name": "Stargate",
                 "desc": "Cross-Chain-Liquiditäts-Bridge auf LayerZero"},
    "wormhole": {"slug": "wormhole", "name": "Wormhole",
                 "desc": "Multi-Chain Message-Bridge (Solana ↔ Eth ↔ etc.)"},
    # Yield / Vaults
    "yearn": {"slug": "yearn-finance", "name": "Yearn",
              "desc": "Yield-Aggregator mit Vaults"},
    "pendle": {"slug": "pendle", "name": "Pendle",
               "desc": "Yield-Tokenization (PT/YT) — DeFi-Bond-Markt"},
    "convex": {"slug": "convex-finance", "name": "Convex",
               "desc": "Curve-Booster-Protokoll (CRV-Locking)"},
    # LST / Restaking
    "eigenlayer": {"slug": "eigenlayer", "name": "EigenLayer",
                   "desc": "Ethereum-Restaking-Protokoll für Active Validation"},
    "ether.fi": {"slug": "ether.fi", "name": "ether.fi",
                 "desc": "Native Liquid-Restaking auf EigenLayer (eETH)"},
    "etherfi": {"slug": "ether.fi", "name": "ether.fi",
                "desc": "Native Liquid-Restaking auf EigenLayer (eETH)"},
    # Andere TOP-10-by-TVL (2026-Stand)
    "ondo": {"slug": "ondo-finance", "name": "Ondo Finance",
             "desc": "Tokenisierte US-Treasuries (RWA-Protokoll)"},
    "ethena": {"slug": "ethena", "name": "Ethena",
               "desc": "Synthetic-Dollar (USDe) — Delta-neutrales Staking"},
}

# Bekannte Stablecoin-Namen → DeFiLlama-Stablecoin-Symbol
_STABLECOIN_NAMES: dict[str, str] = {
    "usdt": "USDT", "tether": "USDT",
    "usdc": "USDC", "usd coin": "USDC",
    "dai": "DAI",
    "usds": "USDS", "sky usds": "USDS",
    "frax": "FRAX",
    "lusd": "LUSD",
    "tusd": "TUSD",
    "fdusd": "FDUSD",
    "pyusd": "PYUSD", "paypal usd": "PYUSD",
    "rai": "RAI",
    "usde": "USDe", "ethena usd": "USDe",
}

# Bekannte Chain-Namen → DeFiLlama-Chain-Name (für /v2/chains)
_CHAIN_NAMES: dict[str, str] = {
    "ethereum": "Ethereum", "eth": "Ethereum",
    "solana": "Solana",
    "tron": "Tron",
    "bnb chain": "BSC", "binance smart chain": "BSC", "bsc": "BSC",
    "polygon": "Polygon",
    "avalanche": "Avalanche",
    "arbitrum": "Arbitrum",
    "optimism": "Optimism",
    "base": "Base",
    "fantom": "Fantom",
    "sui": "Sui",
    "aptos": "Aptos",
    "near": "Near",
    "starknet": "Starknet",
    "zksync": "zkSync Era", "zksync era": "zkSync Era",
    "scroll": "Scroll",
    "linea": "Linea",
    "blast": "Blast",
    "celo": "Celo",
    "mantle": "Mantle",
    "monad": "Monad",
}

# Direkt-Trigger
_DIRECT_TERMS = (
    "defillama", "defi llama", "defi-llama",
)

# DeFi-Generelle-Terms (Composite-Part)
_DEFI_TERMS = (
    "tvl", "total value locked",
    "defi", "decentralized finance",
    "dezentrale finanzen", "dezentralen finanzen",
    "stablecoin", "stable-coin", "stable coin",
    "marktkapitalisierung stablecoin",
    "liquid staking", "liquid-staking",
    "yield farming", "yield-farming",
    "smart contract tvl", "protokoll tvl", "protocol tvl",
)

# EXCLUSION-Terms: NICHT triggern bei reinen Bitcoin-Preis-Claims
# (BTC ist nicht DeFi-spezifisch — andere Crypto-Service-Domain)
_BITCOIN_ONLY_TERMS = (
    "bitcoin-preis", "btc-preis", "bitcoin preis", "btc preis",
    "bitcoin kurs", "btc kurs", "bitcoin-kurs", "btc-kurs",
    "bitcoin halving", "btc halving",
    "bitcoin etf", "btc etf", "bitcoin-etf",
)


# ---------------------------------------------------------------------------
# Trigger
# ---------------------------------------------------------------------------
def _claim_mentions_defillama(claim_lc: str) -> bool:
    """Conservative Trigger:
    1. Direkter Term ('defillama') → True
    2. Whitelist-Protokoll + DeFi-Kontext → True
    3. 'TVL' / 'Total Value Locked' + Protokoll/Chain → True
    4. Stablecoin-Name + Marktkapitalisierungs-Term → True
    5. Chain-Name + 'TVL'/'DeFi' → True
    EXCLUSION: Bitcoin-Preis-only-Claims → False
    """
    if not claim_lc:
        return False

    # Bitcoin-only-Exclusion: wenn nur Bitcoin-Preis erwähnt wird und KEIN
    # DeFi-/TVL-/Protokoll-Term, NICHT triggern.
    has_btc_only = any(t in claim_lc for t in _BITCOIN_ONLY_TERMS)
    has_defi_anchor = any(t in claim_lc for t in (
        "tvl", "defi", "stablecoin", "uniswap", "aave", "lido", "maker",
        "ethereum", "solana", "defillama",
    ))
    if has_btc_only and not has_defi_anchor:
        return False

    # 1. Direkt
    if any(t in claim_lc for t in _DIRECT_TERMS):
        return True

    has_defi_term = any(t in claim_lc for t in _DEFI_TERMS)
    has_protocol = any(p in claim_lc for p in _DEFI_PROTOCOLS.keys())
    has_stablecoin = any(s in claim_lc for s in _STABLECOIN_NAMES.keys())
    has_chain = any(c in claim_lc for c in _CHAIN_NAMES.keys())
    has_tvl = "tvl" in claim_lc or "total value locked" in claim_lc

    # 2. Whitelist-Protokoll + DeFi-Term/TVL
    if has_protocol and (has_defi_term or has_tvl):
        return True

    # 3. TVL allein + Chain
    if has_tvl and has_chain:
        return True

    # 4. Stablecoin-Name + Markt-Term
    market_terms = (
        "marktkapitalisierung", "market cap", "marktkap",
        "circulating", "umlaufmenge", "umlauf",
        "stablecoin",
    )
    if has_stablecoin and any(t in claim_lc for t in market_terms):
        return True

    # 5. Chain + DeFi-Term (z.B. "Solana DeFi", "Ethereum DeFi-Volumen")
    if has_chain and has_defi_term:
        return True

    # 6. Reine Protokoll-Erwähnung NICHT triggern (zu breit).
    # Aber: Mehrere Protokolle ODER Protokoll+Stablecoin → vermutlich DeFi-Kontext.
    if has_protocol and has_stablecoin:
        return True

    return False


# Modul-Level-Trigger-Cache: 24h
_trigger_cache: dict[str, tuple[float, bool]] = {}


def claim_mentions_defillama_cached(claim: str) -> bool:
    """24h-Cache-Wrapper für den Trigger-Check."""
    claim_lc = (claim or "").lower().strip()
    if not claim_lc:
        return False
    now = time.time()
    cached = _trigger_cache.get(claim_lc)
    if cached and (now - cached[0]) < CACHE_TTL_S:
        return cached[1]
    result = _claim_mentions_defillama(claim_lc)
    _trigger_cache[claim_lc] = (now, result)
    # Cache-Hygiene
    if len(_trigger_cache) > 500:
        oldest = sorted(_trigger_cache.items(), key=lambda kv: kv[1][0])[:100]
        for k, _ in oldest:
            _trigger_cache.pop(k, None)
    return result


# ---------------------------------------------------------------------------
# Result-Cache (24h pro Query-Key)
# ---------------------------------------------------------------------------
_result_cache: dict[str, tuple[float, dict | list]] = {}


def _cache_get(key: str):
    now = time.time()
    hit = _result_cache.get(key)
    if hit and (now - hit[0]) < CACHE_TTL_S:
        return hit[1]
    return None


def _cache_put(key: str, value) -> None:
    _result_cache[key] = (time.time(), value)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _extract_protocols(claim_lc: str) -> list[dict]:
    """Erkenne genannte Protokoll-Slugs im Claim. Max 3, Reihenfolge stabil."""
    found: list[dict] = []
    seen_slugs: set[str] = set()
    for term, meta in _DEFI_PROTOCOLS.items():
        if term in claim_lc:
            slug = meta["slug"]
            if slug in seen_slugs:
                continue
            seen_slugs.add(slug)
            found.append(meta)
            if len(found) >= MAX_RESULTS:
                break
    return found


def _extract_stablecoin_symbols(claim_lc: str) -> list[str]:
    """Erkenne genannte Stablecoin-Symbole im Claim."""
    found: list[str] = []
    seen: set[str] = set()
    for term, symbol in _STABLECOIN_NAMES.items():
        if term in claim_lc and symbol not in seen:
            seen.add(symbol)
            found.append(symbol)
            if len(found) >= MAX_RESULTS:
                break
    return found


def _extract_chain(claim_lc: str) -> str | None:
    """Erkenne genannten Chain-Namen im Claim. Erstes Match gewinnt."""
    # Längste Matches zuerst (z.B. "zksync era" vor "zksync")
    for term, name in sorted(
        _CHAIN_NAMES.items(), key=lambda kv: -len(kv[0])
    ):
        if term in claim_lc:
            return name
    return None


def _format_usd(v) -> str:
    """Formatiere USD-Wert deutsch: 12345678901 → '12,3 Mrd. USD'."""
    if v is None:
        return "?"
    try:
        n = float(v)
    except (TypeError, ValueError):
        return "?"
    if n >= 1e9:
        num = f"{n/1e9:.1f}".replace(".", ",")
        return f"{num} Mrd. USD"
    if n >= 1e6:
        num = f"{n/1e6:.1f}".replace(".", ",")
        return f"{num} Mio. USD"
    if n >= 1e3:
        num = f"{n/1e3:.1f}".replace(".", ",")
        return f"{num} Tsd. USD"
    return f"{n:,.0f} USD".replace(",", ".")


def _current_year() -> str:
    return time.strftime("%Y", time.gmtime())


# ---------------------------------------------------------------------------
# HTTP-Calls
# ---------------------------------------------------------------------------
async def _fetch_protocol(client, slug: str) -> dict | None:
    """GET /protocol/{slug} — Protokoll-Detail inkl. currentChainTvls."""
    cache_key = f"protocol::{slug}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached or None

    url = f"{DEFILLAMA_API}/protocol/{quote(slug)}"
    try:
        resp = await client.get(url, follow_redirects=True)
        if resp.status_code != 200:
            logger.debug(
                f"DeFiLlama protocol HTTP {resp.status_code} for slug='{slug}'"
            )
            _cache_put(cache_key, {})
            return None
        data = resp.json()
    except Exception as e:
        logger.debug(f"DeFiLlama protocol fetch failed for '{slug}': {e}")
        return None

    if not isinstance(data, dict) or "name" not in data:
        _cache_put(cache_key, {})
        return None
    _cache_put(cache_key, data)
    return data


async def _fetch_chains(client) -> list[dict]:
    """GET /chains — Chain-Liste mit TVL."""
    cache_key = "chains::all"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached or []

    url = f"{DEFILLAMA_API}/chains"
    try:
        resp = await client.get(url, follow_redirects=True)
        if resp.status_code != 200:
            logger.debug(f"DeFiLlama chains HTTP {resp.status_code}")
            _cache_put(cache_key, [])
            return []
        data = resp.json()
    except Exception as e:
        logger.debug(f"DeFiLlama chains fetch failed: {e}")
        return []

    if not isinstance(data, list):
        _cache_put(cache_key, [])
        return []
    _cache_put(cache_key, data)
    return data


async def _fetch_stablecoins(client) -> list[dict]:
    """GET /stablecoins (stablecoins.llama.fi) — alle Stablecoins."""
    cache_key = "stablecoins::all"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached or []

    url = f"{STABLECOINS_API}/stablecoins"
    try:
        resp = await client.get(url, follow_redirects=True)
        if resp.status_code != 200:
            logger.debug(f"DeFiLlama stablecoins HTTP {resp.status_code}")
            _cache_put(cache_key, [])
            return []
        data = resp.json()
    except Exception as e:
        logger.debug(f"DeFiLlama stablecoins fetch failed: {e}")
        return []

    pegged = (data or {}).get("peggedAssets") if isinstance(data, dict) else None
    if not isinstance(pegged, list):
        _cache_put(cache_key, [])
        return []
    _cache_put(cache_key, pegged)
    return pegged


# ---------------------------------------------------------------------------
# Result-Builder
# ---------------------------------------------------------------------------
def _build_protocol_result(meta: dict, payload: dict, claim_lc: str) -> dict:
    """Baue Result für ein einzelnes Protokoll."""
    name = payload.get("name") or meta["name"]
    slug = meta["slug"]

    current_chain_tvls = payload.get("currentChainTvls") or {}
    # Aggregat-TVL ohne 'borrowed'/'staking'/'pool2' (sind Sub-Kategorien)
    # — wir nehmen die kanonische Chain-TVL-Summe.
    primary_chain = _extract_chain(claim_lc)
    chain_label = ""
    tvl_value = None

    if primary_chain and primary_chain in current_chain_tvls:
        tvl_value = current_chain_tvls[primary_chain]
        chain_label = primary_chain
    else:
        # Aggregat: Summe aller Chain-TVL (positive numerische Werte)
        # OHNE die '-borrowed' / '-staking' / '-pool2' Sub-Keys
        chain_tvls = {
            k: v for k, v in current_chain_tvls.items()
            if isinstance(v, (int, float)) and v > 0
            and "-" not in k
            and k not in ("borrowed", "staking", "pool2")
        }
        if chain_tvls:
            tvl_value = sum(chain_tvls.values())
            top_chain = max(chain_tvls.items(), key=lambda kv: kv[1])[0]
            chain_label = f"Multi-Chain (Top: {top_chain})"

    tvl_display = _format_usd(tvl_value)
    chain_tag = f" ({chain_label})" if chain_label else ""

    display = f"{name} TVL{chain_tag}: {tvl_display}"

    return {
        "indicator_name": f"TVL: {name}{chain_tag}",
        "indicator": f"defillama_{slug}",
        "country": "INT",
        "country_name": "—",
        "year": _current_year(),
        "value": tvl_value,
        "display_value": display,
        "description": (
            f"{meta.get('desc', '')}. "
            "Quelle: DeFiLlama (Open-Source DeFi-TVL-Aggregator). "
            "Nur deskriptive Markt-Daten — keine Investment-Bewertung. "
            "Krypto-Märkte sind volatil; TVL schwankt täglich."
        ),
        "url": f"https://defillama.com/protocol/{slug}",
        "source": "DeFiLlama (MIT)",
    }


def _build_stablecoin_result(asset: dict) -> dict:
    """Baue Result für einen Stablecoin."""
    name = asset.get("name") or "?"
    symbol = asset.get("symbol") or "?"
    pegtype = asset.get("pegType") or ""
    mechanism = asset.get("pegMechanism") or ""
    asset_id = asset.get("id") or ""

    circ = asset.get("circulating") or {}
    # circulating-Wert: nimm den ersten numerischen Wert in dem Dict
    circ_value = None
    for _k, v in circ.items():
        if isinstance(v, (int, float)) and v > 0:
            circ_value = v
            break

    circ_display = _format_usd(circ_value)
    mech_label_map = {
        "fiat-backed": "Fiat-besichert",
        "crypto-backed": "Crypto-besichert",
        "algorithmic": "algorithmisch",
    }
    mech_label = mech_label_map.get(mechanism, mechanism or "—")

    url = (
        f"https://defillama.com/stablecoin/{asset_id}"
        if asset_id else "https://defillama.com/stablecoins"
    )

    return {
        "indicator_name": f"Stablecoin: {name} ({symbol})",
        "indicator": f"defillama_stable_{symbol.lower()}",
        "country": "INT",
        "country_name": "—",
        "year": _current_year(),
        "value": circ_value,
        "display_value": f"{symbol} Marktkapitalisierung: {circ_display}",
        "description": (
            f"{name} ({symbol}) — {pegtype}, Mechanismus: {mech_label}. "
            "Marktkapitalisierung = Umlaufmenge in USD. "
            "Quelle: DeFiLlama-Stablecoin-Tracker (MIT). "
            "Nur deskriptive Markt-Daten — keine Investment-Bewertung."
        ),
        "url": url,
        "source": "DeFiLlama (MIT)",
    }


def _build_chain_result(chain: dict) -> dict:
    """Baue Result für eine Chain."""
    name = chain.get("name") or "?"
    tvl = chain.get("tvl")
    token = chain.get("tokenSymbol") or "—"

    slug_url = name.lower().replace(" ", "-")
    return {
        "indicator_name": f"Chain-TVL: {name}",
        "indicator": f"defillama_chain_{slug_url}",
        "country": "INT",
        "country_name": "—",
        "year": _current_year(),
        "value": tvl,
        "display_value": f"{name} DeFi-TVL: {_format_usd(tvl)}",
        "description": (
            f"Aggregierter DeFi-TVL aller Protokolle auf {name} "
            f"(Native-Token: {token}). "
            "Quelle: DeFiLlama (Open-Source). "
            "Nur deskriptive Markt-Daten — keine Investment-Bewertung."
        ),
        "url": f"https://defillama.com/chain/{quote(name)}",
        "source": "DeFiLlama (MIT)",
    }


# ---------------------------------------------------------------------------
# Public search
# ---------------------------------------------------------------------------
async def search_defillama(analysis: dict) -> dict:
    """Live-Lookup gegen DeFiLlama-API.

    Strategie:
    1. Erkannte Protokoll-Namen → /protocol/{slug} (max 3)
    2. Erkannte Stablecoin-Namen → /stablecoins, filter by symbol
    3. Erkannte Chain + TVL-Term → /chains, filter by name
    4. Fallback bei reinem 'TVL'+'DeFi' ohne Spezifikum: kein Result
    """
    empty = {
        "source": "DeFiLlama",
        "type": "defi_data",
        "results": [],
    }

    analysis = analysis or {}
    claim = analysis.get("claim") or analysis.get("original_claim") or ""
    original = analysis.get("original_claim") or claim
    matchable = f"{original} {claim}".lower()

    if not _claim_mentions_defillama(matchable):
        return empty

    protocols = _extract_protocols(matchable)
    stablecoin_symbols = _extract_stablecoin_symbols(matchable)
    chain_name = _extract_chain(matchable)
    has_tvl = "tvl" in matchable or "total value locked" in matchable

    results: list[dict] = []

    async with polite_client(timeout=TIMEOUT_S) as client:
        # 1. Protokoll-Lookups
        for meta in protocols:
            if len(results) >= MAX_RESULTS:
                break
            payload = await _fetch_protocol(client, meta["slug"])
            if not payload:
                continue
            try:
                r = _build_protocol_result(meta, payload, matchable)
            except Exception as e:
                logger.debug(
                    f"DeFiLlama build_protocol error '{meta['slug']}': {e}"
                )
                continue
            results.append(r)

        # 2. Stablecoin-Lookups
        if stablecoin_symbols and len(results) < MAX_RESULTS:
            assets = await _fetch_stablecoins(client)
            wanted = set(stablecoin_symbols)
            for asset in assets:
                if len(results) >= MAX_RESULTS:
                    break
                symbol = (asset.get("symbol") or "")
                if symbol in wanted:
                    try:
                        results.append(_build_stablecoin_result(asset))
                    except Exception as e:
                        logger.debug(f"DeFiLlama build_stablecoin error: {e}")

        # 3. Chain-Lookup (nur wenn Chain explizit + TVL/DeFi-Term, ohne
        # bereits gefundene Protokoll-Treffer für dieselbe Chain)
        if chain_name and has_tvl and len(results) < MAX_RESULTS:
            chains = await _fetch_chains(client)
            for c in chains:
                if (c.get("name") or "").lower() == chain_name.lower():
                    try:
                        results.append(_build_chain_result(c))
                    except Exception as e:
                        logger.debug(f"DeFiLlama build_chain error: {e}")
                    break

    results = results[:MAX_RESULTS]

    if not results:
        logger.info(
            f"DeFiLlama: 0 Treffer — proto={[p['slug'] for p in protocols]} "
            f"stable={stablecoin_symbols} chain={chain_name}"
        )
        return empty

    logger.info(
        f"DeFiLlama: {len(results)} Treffer — proto={[p['slug'] for p in protocols]} "
        f"stable={stablecoin_symbols} chain={chain_name}"
    )
    return {
        "source": "DeFiLlama",
        "type": "defi_data",
        "results": results,
    }
