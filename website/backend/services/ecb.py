import re

import httpx
import logging
from services._http_polite import polite_client

logger = logging.getLogger("evidora")

BASE_URL = "https://data-api.ecb.europa.eu/service/data"

# Warnung, die an den Einlagenzinsen haengt. Drei Saetze, die alle "Zinsen"
# heissen und regelmaessig verwechselt werden — der Anlass war genau diese
# Verwechslung (QA50E-Befund 4).
MESSWARNUNG = (
    "MESSGROESSE: das ist der Zinssatz, den oesterreichische Banken PRIVATEN "
    "HAUSHALTEN zahlen — nicht der EZB-Leitzins und nicht die "
    "EZB-Einlagefazilitaet (das ist der Satz, den BANKEN bei der EZB "
    "bekommen). Taeglich faellige Einlagen (Sparbuch, Girokonto) und "
    "gebundene Einlagen (Termin-/Festgeld) unterscheiden sich um ein "
    "Vielfaches und duerfen nicht gegeneinander eingesetzt werden."
)

# Map keywords (DE + EN) to ECB series keys
SERIES_MAP = {
    # Key interest rates
    "leitzins": {
        "series": "FM/B.U2.EUR.4F.KR.MRR_FR.LEV",
        "label": "EZB-Leitzins (Hauptrefinanzierungssatz)",
        "label_en": "ECB Key Interest Rate (Main Refinancing Rate)",
        "unit": "%",
    },
    "key interest rate": {
        "series": "FM/B.U2.EUR.4F.KR.MRR_FR.LEV",
        "label": "EZB-Leitzins (Hauptrefinanzierungssatz)",
        "label_en": "ECB Key Interest Rate (Main Refinancing Rate)",
        "unit": "%",
    },
    "zinsen": {
        "series": "FM/B.U2.EUR.4F.KR.MRR_FR.LEV",
        "label": "EZB-Leitzins (Hauptrefinanzierungssatz)",
        "label_en": "ECB Key Interest Rate (Main Refinancing Rate)",
        "unit": "%",
    },
    "interest rate": {
        "series": "FM/B.U2.EUR.4F.KR.MRR_FR.LEV",
        "label": "EZB-Leitzins (Hauptrefinanzierungssatz)",
        "label_en": "ECB Key Interest Rate (Main Refinancing Rate)",
        "unit": "%",
    },
    "einlagezins": {
        "series": "FM/B.U2.EUR.4F.KR.DFR.LEV",
        "label": "EZB-Einlagefazilität",
        "label_en": "ECB Deposit Facility Rate",
        "unit": "%",
    },
    "deposit rate": {
        "series": "FM/B.U2.EUR.4F.KR.DFR.LEV",
        "label": "EZB-Einlagefazilität",
        "label_en": "ECB Deposit Facility Rate",
        "unit": "%",
    },
    # --- Einlagenzinsen privater Haushalte (Oesterreich) -------------------
    # QA50E-Befund 4: "Die Zinsen fuer mein Sparbuch sind niedrig" bekam
    # true@0.85 — hergeleitet aus dem LEITZINS, weil es zu Sparzinsen gar
    # keine Reihe gab. Die Summary sagte woertlich "Sparbuchzinsen orientieren
    # sich typischerweise am Leitzins und sind daher aktuell ebenfalls
    # niedrig", die Nuance erfand dazu "typischerweise unter 2 % p.a.".
    #
    # Es gibt die echten Zahlen: EZB-MIR-Statistik, monatlich, nach Land und
    # Produkt. Am 2026-09-07 gegen die API geprueft (Stand Juli 2026):
    #   taeglich faellig  0,43 %   <- das ist das Sparbuch
    #   gebunden          2,10 %
    # Ein Faktor 5 zwischen zwei Zahlen, die beide "Sparzinsen" heissen —
    # dieselbe Falle wie Akut- gegen Gesamtbetten (#321). Deshalb stehen
    # BEIDE hier, mit Messgroessen-Warnung, statt einer allein.
    #
    # `praefix`: deutsche Komposita. "sparbuch" muss auch in
    # "Sparbuchzinsen" treffen, und dafuer darf die Wortgrenze nur VORNE
    # stehen. Bewusst nur fuer diese Stichwoerter — bei "euro" wuerde das
    # "europaeische" treffen, weswegen die Wortgrenze ueberhaupt da ist.
    "sparbuch": {
        "series": "MIR/M.AT.B.L21.A.R.A.2250.EUR.N",
        "label": "Sparzinsen Oesterreich — taeglich faellige Einlagen "
                 "privater Haushalte (Sparbuch, Girokonto)",
        "label_en": "Austria — overnight deposit rate, households",
        "unit": "%",
        "praefix": True,
        "vorrang": True,
        "hinweis": MESSWARNUNG,
    },
    "sparzins": {
        "series": "MIR/M.AT.B.L21.A.R.A.2250.EUR.N",
        "label": "Sparzinsen Oesterreich — taeglich faellige Einlagen "
                 "privater Haushalte (Sparbuch, Girokonto)",
        "label_en": "Austria — overnight deposit rate, households",
        "unit": "%",
        "praefix": True,
        "vorrang": True,
        "hinweis": MESSWARNUNG,
    },
    "sparkonto": {
        "series": "MIR/M.AT.B.L21.A.R.A.2250.EUR.N",
        "label": "Sparzinsen Oesterreich — taeglich faellige Einlagen "
                 "privater Haushalte (Sparbuch, Girokonto)",
        "label_en": "Austria — overnight deposit rate, households",
        "unit": "%",
        "praefix": True,
        "vorrang": True,
        "hinweis": MESSWARNUNG,
    },
    "spareinlagen": {
        "series": "MIR/M.AT.B.L21.A.R.A.2250.EUR.N",
        "label": "Sparzinsen Oesterreich — taeglich faellige Einlagen "
                 "privater Haushalte (Sparbuch, Girokonto)",
        "label_en": "Austria — overnight deposit rate, households",
        "unit": "%",
        "praefix": True,
        "vorrang": True,
        "hinweis": MESSWARNUNG,
    },
    "tagesgeld": {
        "series": "MIR/M.AT.B.L21.A.R.A.2250.EUR.N",
        "label": "Sparzinsen Oesterreich — taeglich faellige Einlagen "
                 "privater Haushalte (Sparbuch, Girokonto)",
        "label_en": "Austria — overnight deposit rate, households",
        "unit": "%",
        "praefix": True,
        "vorrang": True,
        "hinweis": MESSWARNUNG,
    },
    "festgeld": {
        "series": "MIR/M.AT.B.L22.A.R.A.2250.EUR.N",
        "label": "Zinsen Oesterreich — gebundene Einlagen privater "
                 "Haushalte (Termin-/Festgeld)",
        "label_en": "Austria — deposits with agreed maturity, households",
        "unit": "%",
        "praefix": True,
        "vorrang": True,
        "hinweis": MESSWARNUNG,
    },
    "termingeld": {
        "series": "MIR/M.AT.B.L22.A.R.A.2250.EUR.N",
        "label": "Zinsen Oesterreich — gebundene Einlagen privater "
                 "Haushalte (Termin-/Festgeld)",
        "label_en": "Austria — deposits with agreed maturity, households",
        "unit": "%",
        "praefix": True,
        "vorrang": True,
        "hinweis": MESSWARNUNG,
    },
    # Exchange rates
    "wechselkurs": {
        "series": "EXR/D.USD.EUR.SP00.A",
        "label": "EUR/USD Wechselkurs",
        "label_en": "EUR/USD Exchange Rate",
        "unit": "USD",
    },
    "exchange rate": {
        "series": "EXR/D.USD.EUR.SP00.A",
        "label": "EUR/USD Wechselkurs",
        "label_en": "EUR/USD Exchange Rate",
        "unit": "USD",
    },
    "dollar": {
        "series": "EXR/D.USD.EUR.SP00.A",
        "label": "EUR/USD Wechselkurs",
        "label_en": "EUR/USD Exchange Rate",
        "unit": "USD",
    },
    "euro": {
        "series": "EXR/D.USD.EUR.SP00.A",
        "label": "EUR/USD Wechselkurs",
        "label_en": "EUR/USD Exchange Rate",
        "unit": "USD",
    },
    "franken": {
        "series": "EXR/D.CHF.EUR.SP00.A",
        "label": "EUR/CHF Wechselkurs",
        "label_en": "EUR/CHF Exchange Rate",
        "unit": "CHF",
    },
    "pfund": {
        "series": "EXR/D.GBP.EUR.SP00.A",
        "label": "EUR/GBP Wechselkurs",
        "label_en": "EUR/GBP Exchange Rate",
        "unit": "GBP",
    },
    "yen": {
        "series": "EXR/D.JPY.EUR.SP00.A",
        "label": "EUR/JPY Wechselkurs",
        "label_en": "EUR/JPY Exchange Rate",
        "unit": "JPY",
    },
    # Money supply
    "geldmenge": {
        "series": "BSI/M.U2.Y.V.M30.X.1.U2.2300.Z01.E",
        "label": "Geldmenge M3 (Euroraum)",
        "label_en": "Money Supply M3 (Euro Area)",
        "unit": "EUR Mio.",
    },
    "money supply": {
        "series": "BSI/M.U2.Y.V.M30.X.1.U2.2300.Z01.E",
        "label": "Geldmenge M3 (Euroraum)",
        "label_en": "Money Supply M3 (Euro Area)",
        "unit": "EUR Mio.",
    },
    "geld drucken": {
        "series": "BSI/M.U2.Y.V.M30.X.1.U2.2300.Z01.E",
        "label": "Geldmenge M3 (Euroraum)",
        "label_en": "Money Supply M3 (Euro Area)",
        "unit": "EUR Mio.",
    },
    "money printing": {
        "series": "BSI/M.U2.Y.V.M30.X.1.U2.2300.Z01.E",
        "label": "Geldmenge M3 (Euroraum)",
        "label_en": "Money Supply M3 (Euro Area)",
        "unit": "EUR Mio.",
    },
    # HICP Inflation (ECB perspective)
    "inflation": {
        "series": "ICP/M.U2.N.000000.4.ANR",
        "label": "HVPI-Inflationsrate (Euroraum)",
        "label_en": "HICP Inflation Rate (Euro Area)",
        "unit": "%",
    },
    "teuerung": {
        "series": "ICP/M.U2.N.000000.4.ANR",
        "label": "HVPI-Inflationsrate (Euroraum)",
        "label_en": "HICP Inflation Rate (Euro Area)",
        "unit": "%",
    },
    "ezb inflation": {
        "series": "ICP/M.U2.N.000000.4.ANR",
        "label": "HVPI-Inflationsrate (Euroraum)",
        "label_en": "HICP Inflation Rate (Euro Area)",
        "unit": "%",
    },
    "ecb inflation": {
        "series": "ICP/M.U2.N.000000.4.ANR",
        "label": "HVPI-Inflationsrate (Euroraum)",
        "label_en": "HICP Inflation Rate (Euro Area)",
        "unit": "%",
    },
    "geldpolitik": {
        "series": "FM/B.U2.EUR.4F.KR.MRR_FR.LEV",
        "label": "EZB-Leitzins (Hauptrefinanzierungssatz)",
        "label_en": "ECB Key Interest Rate (Main Refinancing Rate)",
        "unit": "%",
    },
    "monetary policy": {
        "series": "FM/B.U2.EUR.4F.KR.MRR_FR.LEV",
        "label": "EZB-Leitzins (Hauptrefinanzierungssatz)",
        "label_en": "ECB Key Interest Rate (Main Refinancing Rate)",
        "unit": "%",
    },
}


HISTORICAL_KEYWORDS = [
    "rekord", "record", "höchst", "highest", "niedrigst", "lowest",
    "tiefst", "historisch", "historic", "jemals", "ever", "allzeit",
    "all-time", "noch nie", "never", "seit beginn", "since",
]


# Vage Wertungen im POSITIV. Sie verlangen keinen Superlativ und standen
# deshalb nicht in HISTORICAL_KEYWORDS — mit der Folge, dass "Der EZB-Leitzins
# ist niedrig" nur sechs Monatswerte bekam und gar nicht belegbar war. Die
# Antwort stuetzte sich dann auf ungestuetztes Modellwissen (">4 % in den
# 2000ern"), oder sie verweigerte.
#
# Mit der Spannweite der Reihe wird aus der Wertung eine pruefbare Aussage:
# 2,40 % laesst sich gegen das dokumentierte Minimum und Maximum einordnen.
#
# Wortgrenze auf BEIDEN Seiten, mit begrenztem Flexions-Schwanz. Ein offenes
# Praefix waere hier falsch: "hoch" traefe dann "Hochschule" und "Hochwasser"
# — genau der Fehler, gegen den die Wortgrenze im Reihen-Matching ueberhaupt
# existiert. Bis zu drei Buchstaben decken die deutschen Endungen ab
# ("niedrige", "niedrigen", "niedriger"), danach muss ein Nicht-Buchstabe
# stehen. Beim Bauen gemessen, nicht vermutet.
# Umlaute gehoeren in die Zeichenklasse UND in den Lookahead. Ohne sie wirkt
# ein "ü" als Wortgrenze, und "Geringfuegigkeitsgrenze" — mit echtem Umlaut
# geschrieben — matcht: nach "gering" steht "f" (in [a-z]), danach "ü" (nicht
# in [a-z]), der Lookahead ist erfuellt. Der eigene Test hat das gefangen,
# eine Ad-hoc-Sonde mit ASCII-Schreibweise vorher nicht.
_VAGE_WERTUNG = re.compile(
    r"\b(?:niedrig|hoh|hoch|teuer|billig|gering|guenstig|günstig|stark|"
    r"schwach)[a-zäöüß]{0,3}(?![a-zäöüß])",
    re.IGNORECASE)


def _needs_historical(claim: str) -> bool:
    """Check if the claim requires historical context."""
    claim_lower = claim.lower()
    if any(kw in claim_lower for kw in HISTORICAL_KEYWORDS):
        return True
    return bool(_VAGE_WERTUNG.search(claim_lower))


def _find_series(claim: str) -> list[dict]:
    """Find matching ECB series based on keywords in the claim.

    Uses word-boundary matching to avoid false positives like
    "euro" matching "europäische".
    """
    claim_lower = claim.lower()
    found = {}
    for keyword, series_info in SERIES_MAP.items():
        # Deutsche Komposita: "sparbuch" muss auch in "Sparbuchzinsen"
        # treffen — dort darf die Wortgrenze nur VORNE stehen. Bewusst nur
        # fuer markierte Stichwoerter: bei "euro" wuerde ein Praefix-Match
        # "europaeische" treffen, deswegen steht die Grenze ueberhaupt da.
        muster = (r'\b' + re.escape(keyword) if series_info.get("praefix")
                  else r'\b' + re.escape(keyword) + r'\b')
        if re.search(muster, claim_lower):
            series_key = series_info["series"]
            if series_key not in found:
                found[series_key] = series_info
    # Reihen mit `vorrang` zuerst: bei einem Sparbuch-Claim matcht auch
    # "zinsen" und damit der Leitzins. Ohne Vorrang schneidet `matching[:3]`
    # unter Umstaenden genau die Reihe weg, nach der gefragt wurde.
    return sorted(found.values(), key=lambda s: not s.get("vorrang", False))


def _parse_sdmx_json(data: dict, series_info: dict, historical: bool = False) -> list[dict]:
    """Parse SDMX-JSON response and extract observations."""
    results = []
    try:
        datasets = data.get("dataSets", [])
        if not datasets:
            return results

        # Get time dimension values
        dimensions = data.get("structure", {}).get("dimensions", {})
        obs_dimensions = dimensions.get("observation", [])
        time_dim = None
        for dim in obs_dimensions:
            if dim.get("id") == "TIME_PERIOD":
                time_dim = dim.get("values", [])
                break

        if not time_dim:
            return results

        # Get series observations
        series_data = datasets[0].get("series", {})
        for series_key, series_obj in series_data.items():
            observations = series_obj.get("observations", {})
            obs_indices = sorted(observations.keys(), key=lambda x: int(x))

            # Collect all values for historical analysis
            all_values = []
            for idx in obs_indices:
                obs_values = observations[idx]
                value = obs_values[0] if obs_values else None
                if value is not None:
                    time_idx = int(idx)
                    time_val = time_dim[time_idx]["id"] if time_idx < len(time_dim) else "?"
                    all_values.append((time_val, value))

            # Calculate historical context
            hist_prefix = ""
            if historical and len(all_values) > 6:
                values_only = [v for _, v in all_values]
                min_val = min(values_only)
                max_val = max(values_only)
                min_period = [t for t, v in all_values if v == min_val][0]
                max_period = [t for t, v in all_values if v == max_val][0]
                first_period = all_values[0][0]
                current_period = all_values[-1][0]
                unit = series_info["unit"]
                hist_prefix = (
                    f"WICHTIG — Historischer Kontext ({first_period} bis {current_period}): "
                    f"Das absolute Minimum lag bei {min_val:.2f} {unit} ({min_period}), "
                    f"das absolute Maximum bei {max_val:.2f} {unit} ({max_period}). "
                )
                logger.info(f"ECB historical context: min={min_val} ({min_period}), max={max_val} ({max_period})")

            # Add recent data points (last 6)
            recent = all_values[-6:] if len(all_values) > 6 else all_values
            for i, (time_val, value) in enumerate(recent):
                if series_info["unit"] in ("%",):
                    formatted = f"{value:.2f} {series_info['unit']}"
                elif series_info["unit"] == "EUR Mio.":
                    formatted = f"{value:,.0f} {series_info['unit']}"
                else:
                    formatted = f"{value:.4f} {series_info['unit']}"

                # Add historical context to first entry so LLM sees it prominently
                title = f"{series_info['label']}: {time_val} — {formatted}"
                if i == 0 and hist_prefix:
                    title = f"{hist_prefix}{title}"
                # Der JUENGSTE Wert wird als solcher gekennzeichnet. Ohne das
                # greift sich das Modell eine mittlere Zeile: im Lauf zum
                # QA50E-Befund 4 zitierte es "Leitzins aktuell (Juni 2025)
                # 2,15 %", obwohl 2026-06-17 mit 2,40 % in derselben Antwort
                # stand. Sechs Zeilen ohne Rangfolge laden dazu ein.
                if i == len(recent) - 1:
                    title = f"{title} [AKTUELLSTER WERT dieser Reihe]"
                    # Messgroessen-Warnung an denselben Datenpunkt: sechsmal
                    # derselbe Satz frisst das Prompt-Budget, das die Zahlen
                    # brauchen (Cap-Vertrag aus #131) — und wenn sie nur
                    # einmal steht, dann an der Zeile, die zitiert wird.
                    if series_info.get("hinweis"):
                        title = f"{title} {series_info['hinweis']}"

                results.append({
                    "title": title,
                    "indicator": series_info["label"],
                    "period": time_val,
                    "value": value,
                    "unit": series_info["unit"],
                    "url": f"https://data.ecb.europa.eu/data/datasets/{series_info['series'].split('/')[0]}",
                })
    except Exception as e:
        logger.error(f"ECB JSON parse error: {e}")

    return results


async def search_ecb(analysis: dict) -> dict:
    """Search ECB Statistical Data Warehouse for relevant economic data."""
    claim = analysis.get("claim", "")
    keywords = analysis.get("entities", [])
    search_text = f"{claim} {' '.join(keywords)}".lower()

    # Check if claim needs historical context
    historical = _needs_historical(claim)

    # Find matching series from keywords
    matching = _find_series(search_text)

    if not matching:
        # Default: try key interest rate for general economy claims
        matching = [SERIES_MAP["leitzins"]]

    results = []

    async with polite_client(timeout=20.0) as client:
        for series_info in matching[:3]:  # Max 3 series per request
            series_path = series_info["series"]
            url = f"{BASE_URL}/{series_path}"

            if historical:
                # Fetch 15 years of data for historical claims
                params = {
                    "startPeriod": "2010-01-01",
                    "format": "jsondata",
                    "detail": "dataonly",
                }
                logger.info(f"ECB historical query for: {series_path}")
            else:
                params = {
                    "lastNObservations": "6",
                    "format": "jsondata",
                    "detail": "dataonly",
                }

            try:
                resp = await client.get(url, params=params)
                if resp.status_code == 200:
                    data = resp.json()
                    parsed = _parse_sdmx_json(data, series_info, historical=historical)
                    results.extend(parsed)
                else:
                    logger.warning(f"ECB API error {resp.status_code} for {series_path}")
            except Exception as e:
                logger.error(f"ECB request failed for {series_path}: {e}")

    return {
        "source": "EZB (Europäische Zentralbank)",
        "type": "official_data",
        "results": results,
    }
