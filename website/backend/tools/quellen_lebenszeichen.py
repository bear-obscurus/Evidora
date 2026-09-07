#!/usr/bin/env python3
"""Lebenszeichen der Live-Quellen: liefert jede Quelle noch echte Daten?

WARUM ES DAS GIBT
=================
WGI (Weltbank-Governance) lieferte vom 17.05. bis 07.09.2026 — vier Monate —
fuer JEDES Land null Ergebnisse. Zwei Umstellungen auf Seiten der Quelle:
``source=75`` war nicht mehr die WGI-Datenbank, und ``RL.EST`` hiess dort
``GOV_WGI_RL.EST``. Die Weltbank quittiert so etwas mit **HTTP 200 und einem
Fehler-Objekt im Rumpf** — ``raise_for_status`` greift nicht, der Konnektor
cachte still eine leere Liste, und im Log stand:

    Source 13 (WGI (World Bank Governance)) returned 0 results

Von "diese Quelle hat zu diesem Land nichts" ist das nicht zu unterscheiden.
Gemeldet hat es nichts: der Kanarienvogel (#107) prueft die PIPELINE mit einem
Claim, die CI war gruen, und kein Test spricht mit einer echten API. Gefunden
wurde es nur, weil zufaellig eine Live-Kontrolle zu einem anderen PR anstand.

WAS DIESER JOB ANDERS MACHT
===========================
Er fragt die Quellen DIREKT — nicht ueber ``/api/check``. Das ist Absicht:

  * Kein LLM-Aufruf. 95 Live-Konnektoren taeglich durch die Pipeline zu
    schicken waere der teuerste denkbare Monitor.
  * Kein schwerer Python-Prozess neben dem Container (Lehrgeld 08.08.: das
    riss den Host mit). Der Job braucht nur die Standardbibliothek.
  * Er trennt "Quelle antwortet leer" von "Abfrage ist kaputt". Genau diese
    Unterscheidung fehlte bei WGI.

WAS ER NICHT KANN
=================
Er prueft, ob die QUELLE Daten liefert — nicht, ob der Konnektor sie richtig
verarbeitet. Eine Sonde kann gruen sein, waehrend das Parsing im Service
daneben liegt; WGI waere so ein Fall gewesen, wenn nur die IDs gestimmt
haetten. Das bleibt die offene Luecke.

Seit der dritten Welle deckt er einen TEIL der Live-Quellen ab — inzwischen
fast alle, aber "alle Sonden gruen" heisst weiterhin nicht "alle Quellen
liefern brauchbare Daten". Der Bericht nennt deshalb Zaehler und Nenner.

Damit die Sonden nicht von den Konnektoren wegdriften, prueft
``tests/test_quellen_lebenszeichen.py``, dass jede Sonden-URL noch als
Konstante im zugehoerigen Service steht.

Verwendung:
  python3 tools/quellen_lebenszeichen.py [--json] [--timeout 45]
                                         [--nur NAME] [--alert-webhook URL]

Cron (auf prod), ueber run_evidora_tool.sh:
  30 5 * * * /opt/Evidora/website/run_evidora_tool.sh quellen_lebenszeichen.py \
             >> /home/burrito/evidora-logs/quellen.log 2>&1

Exit-Codes: 0 = alle Sonden gruen, 1 = mindestens eine Quelle stumm oder
kaputt (Alert gesendet), 2 = Aufruf-/Konfigurationsfehler.
"""
import argparse
import datetime as _dt
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request

USER_AGENT = "Evidora-Quellenmonitor/1.0 (+https://evidora.eu; contact@evidora.eu)"

# Stand 2026-09-07, gezaehlt ueber services/*.py mit `async def search_/fetch_`
# und httpx-Nutzung, ohne Static-First-Pakete. Steht hier, damit der Bericht
# die Abdeckung nennt statt nur "alle Sonden gruen" — ein Test pinnt die Zahl
# gegen den echten Bestand.
ANZAHL_LIVE_KONNEKTOREN = 101


# --------------------------------------------------------------------------
# Pruefer: wie viele DATENZEILEN steckt in dieser Antwort?
#   > 0  Quelle lebt
#   0    Quelle antwortet, liefert aber nichts  -> stumm
#   -1   Fehler-Objekt statt Daten              -> kaputte Abfrage
# --------------------------------------------------------------------------

def _p_csv(text):
    return max(0, text.count("\n") - 1)


def _p_weltbank(text):
    """Der WGI-Fall: HTTP 200, aber ein Fehler-Objekt im Rumpf."""
    d = json.loads(text)
    if isinstance(d, list) and d and isinstance(d[0], dict) and "message" in d[0]:
        return -1
    return len(d[1] or []) if isinstance(d, list) and len(d) > 1 else 0


def _p_pfad(*pfad):
    """Zaehlt die Eintraege unter einem verschachtelten Schluessel."""
    def pruefer(text):
        d = json.loads(text)
        for schritt in pfad:
            if not isinstance(d, dict):
                return 0
            d = d.get(schritt)
            if d is None:
                return 0
        return len(d) if hasattr(d, "__len__") else 0
    return pruefer


def _p_zaehlt(marke):
    """Zaehlt ein Markup-Element — fuer RSS/Atom und XML-Listen."""
    return lambda text: text.count(marke)


def _p_liste(text):
    d = json.loads(text)
    return len(d) if isinstance(d, list) else 0


def _p_bytes(text):
    """Fuer Binaerdateien (XLSX, ZIP): nur die Groesse zaehlt. `_p_nicht_leer`
    versucht JSON zu lesen und scheitert dort — EMA liefert eine Excel-Datei,
    CORDIS ein ZIP."""
    return len(text)


def _p_nicht_leer(text):
    d = json.loads(text)
    return len(d) if hasattr(d, "__len__") else 0


# --------------------------------------------------------------------------
# Die Sonden. `service` verweist auf das Modul, aus dem die URL stammt —
# der Drift-Test in tests/ prueft, dass sie dort noch steht.
# --------------------------------------------------------------------------

def _ucdp_fenster() -> dict:
    """Dasselbe Zeitfenster, das ``services/ucdp.py`` rechnet: die letzten drei
    Kalenderjahre.

    Der erste Prod-Lauf meldete UCDP als „stumm" — und das war ein FEHLALARM
    meiner Sonde, nicht ein Ausfall: sie fragte nur 2025 ab, und der
    GED-Datensatz 25.1 reicht bis 2024. Der Konnektor fragt drei Jahre und
    bekommt Daten.

    Eine Sonde, die etwas anderes fragt als der Konnektor, misst nicht den
    Konnektor. Und ein Waechter, der grundlos schreit, wird abgeschaltet
    (#128) — deshalb rechnet die Sonde jetzt dieselbe Regel, und ein Test
    haelt fest, dass sie dieselbe bleibt.
    """
    heute = _dt.date.today()
    return {"StartDate": f"{heute.year - 3}-01-01",
            "EndDate": f"{heute.year}-12-31"}


# Die SDMX-Endpunkte antworten ohne passendes Accept mit HTTP 406.
_SDMX = {"headers": {"Accept": "application/vnd.sdmx.data+json"}}

SONDEN = [
    # (Anzeigename, service, URL, params, pruefer, env-var fuer Token-Header)
    ("WGI (Weltbank Governance)", "wgi",
     "https://api.worldbank.org/v2/country/AUT/indicator/GOV_WGI_RL.EST",
     {"source": "3", "format": "json", "per_page": "5", "date": "2020:2024"},
     _p_weltbank, None),
    ("Weltbank (Wirtschaftsdaten)", "worldbank",
     "https://api.worldbank.org/v2/country/AUT/indicator/NY.GDP.MKTP.CD",
     {"format": "json", "per_page": "5", "date": "2020:2024"},
     _p_weltbank, None),
    ("Eurostat", "eurostat",
     "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/prc_hicp_manr",
     {"format": "JSON", "geo": "AT", "coicop": "CP00", "unit": "RCH_A", "lang": "EN"},
     _p_pfad("value"), None),
    ("EZB", "ecb",
     "https://data-api.ecb.europa.eu/service/data/FM/D.U2.EUR.4F.KR.MRR_FR.LEV",
     {"format": "jsondata", "lastNObservations": "1"},
     lambda t: len((json.loads(t).get("dataSets") or [{}])[0].get("series") or {}), None),
    ("OpenAlex", "openalex", "https://api.openalex.org/works",
     {"search": "climate change", "per-page": "3", "mailto": "contact@evidora.eu"},
     _p_pfad("results"), None),
    ("Europe PMC", "europe_pmc",
     "https://www.ebi.ac.uk/europepmc/webservices/rest/search",
     {"query": "vaccine", "format": "json", "pageSize": "3"},
     _p_pfad("resultList", "result"), None),
    ("Crossref", "crossref", "https://api.crossref.org/works/10.1038/nature12373",
     {"mailto": "contact@evidora.eu"},
     lambda t: 1 if json.loads(t).get("message") else 0, None),
    ("DBnomics (ILO/IMF-Weg)", "ilostat",
     "https://api.db.nomics.world/v22/series/ILO/UNE_DEAP_SEX_AGE_RT",
     {"limit": "1"}, _p_pfad("series", "docs"), None),
    ("Statistik Austria (VPI)", "statistik_austria",
     "https://data.statistik.gv.at/data/OGD_vpi20c18_VPI_2020COICOP18_1.csv",
     {}, _p_csv, None),
    ("Statistik Austria (Arbeitslosenquote)", "statistik_austria",
     "https://data.statistik.gv.at/data/OGD_ake100_hvd_ogdonly_HVD_ALQUO_1.csv",
     {}, _p_csv, None),
    ("WHO GHO", "who", "https://ghoapi.azureedge.net/api/WHOSIS_000001",
     {"$top": "3"}, _p_pfad("value"), None),
    ("PubMed (E-utilities)", "pubmed",
     "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi",
     {"db": "pubmed", "term": "vaccine", "retmode": "json", "retmax": "3"},
     _p_pfad("esearchresult", "idlist"), None),
    ("OWID (CPI fuer Transparency)", "transparency",
     "https://ourworldindata.org/grapher/ti-corruption-perception-index.csv",
     {}, _p_csv, None),
    ("OWID (Wahlbeteiligung fuer IDEA)", "idea",
     "https://ourworldindata.org/grapher/voter-turnout-of-registered-voters.csv",
     {}, _p_csv, None),
    ("Klimadashboard (UBA)", "uba_klima",
     "https://api.klimadashboard.org/v0/data/emissions_data/records",
     {"limit": "2"}, _p_pfad("data"), None),
    ("UCDP", "ucdp", "https://ucdpapi.pcr.uu.se/api/gedevents/25.1",
     {"pagesize": "2", **_ucdp_fenster()},
     _p_pfad("Result"), "UCDP_TOKEN"),
    ("Wikidata (SPARQL)", "wikidata", "https://query.wikidata.org/sparql",
     {"query": "SELECT ?l WHERE { wd:Q40 rdfs:label ?l FILTER(lang(?l)='de') } LIMIT 1",
      "format": "json"}, _p_pfad("results", "bindings"), None),
    ("Google Fact Check", "claimreview",
     "https://factchecktools.googleapis.com/v1alpha1/claims:search",
     {"query": "Klimawandel", "languageCode": "de", "pageSize": "3"},
     _p_pfad("claims"), "GOOGLE_FACTCHECK_API_KEY"),

    # --- Zweite Welle (2026-09-07) ---------------------------------------
    # Jede dieser Sonden wurde einzeln gegen die echte API gefahren, bevor
    # sie hier steht. Die erste Fassung war zur Haelfte falsch: `eea` fragt
    # gar nicht eea.europa.eu ab, sondern Eurostat; `efsa` laeuft ueber
    # Crossref; `who_europe` haengt an dw.euro.who.int, nicht am Gateway.
    # Eine Sonde, die etwas anderes fragt als der Konnektor, misst nicht den
    # Konnektor (#158) — deshalb steht neben jeder der Service, dessen
    # Konstanten der Drift-Test dagegenhaelt.
    ("PubMed-Leitlinien (AHRQ-Weg)", "ahrq",
     "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi",
     {"db": "pubmed", "term": "guideline", "retmode": "json", "retmax": "3"},
     _p_pfad("esearchresult", "idlist"), None),
    ("arXiv", "arxiv", "http://export.arxiv.org/api/query",
     {"search_query": "all:climate", "max_results": "3"}, _p_zaehlt("<entry"), None),
    ("AT-Faktencheck (APA)", "at_faktencheck_rss",
     "https://apa.at/faktencheck/feed/", {}, _p_zaehlt("<item"), None),
    ("BASG", "basg", "https://www.basg.gv.at/whatsnew/rss", {},
     _p_zaehlt("<item"), None),
    ("bioRxiv", "biorxiv",
     "https://api.biorxiv.org/details/biorxiv/10.1101/2020.03.20.000141", {},
     _p_pfad("collection"), None),
    ("CDC Newsroom", "cdc_newsroom",
     "https://tools.cdc.gov/api/v2/resources/media/132608.rss", {},
     _p_zaehlt("<item"), None),
    ("CDC Open Data", "cdc_open_data", "https://api.us.socrata.com/api/catalog/v1",
     {"q": "covid", "limit": "3"}, _p_pfad("results"), None),
    ("ClinicalTrials.gov", "clinicaltrials",
     "https://clinicaltrials.gov/api/v2/studies",
     {"query.term": "diabetes", "pageSize": "3"}, _p_pfad("studies"), None),
    ("ClinVar", "clinvar",
     "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi",
     {"db": "clinvar", "term": "BRCA1", "retmode": "json", "retmax": "3"},
     _p_pfad("esearchresult", "idlist"), None),
    ("Cochrane (ueber PubMed)", "cochrane",
     "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi",
     {"db": "pubmed", "term": "cochrane database syst rev[jour]",
      "retmode": "json", "retmax": "3"},
     _p_pfad("esearchresult", "idlist"), None),
    ("DataCommons ClaimReview", "datacommons",
     "https://storage.googleapis.com/datacommons-feeds/claimreview/latest/data.json",
     {}, _p_pfad("dataFeedElement"), None),
    ("DOAB", "doab", "https://directory.doabooks.org/rest/search",
     {"query": "climate", "expand": "metadata"}, _p_liste, None),
    # Der Drift-Test hat hier zweimal zugeschlagen, und zu Recht: die erste
    # Fassung fragte opendata.ecdc.europa.eu ab (der Konnektor nimmt
    # OWID-Grapher-CSVs) und data.sec.gov — `edgar` ist aber die
    # JRC-EMISSIONSDATENBANK, nicht die US-Boersenaufsicht.
    ("ECDC (ueber OWID)", "ecdc",
     "https://ourworldindata.org/grapher/global-vaccination-coverage.csv", {},
     _p_csv, None),
    ("EEA (ueber Eurostat)", "eea",
     "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/env_air_emis",
     {"format": "JSON", "geo": "AT", "lang": "EN"}, _p_pfad("value"), None),
    ("EFSA (ueber Crossref)", "efsa",
     "https://api.crossref.org/journals/1831-4732/works",
     {"rows": "2", "mailto": "contact@evidora.eu"},
     _p_pfad("message", "items"), None),
    ("GBIF", "gbif", "https://api.gbif.org/v1/species/search",
     {"q": "Ursus", "limit": "3"}, _p_pfad("results"), None),
    ("IMF (ueber DBnomics)", "imf",
     "https://api.db.nomics.world/v22/series/IMF/WEO:2024-10/AUT.NGDP_RPCH", {},
     _p_pfad("series", "docs"), None),
    ("MedlinePlus", "medlineplus", "https://wsearch.nlm.nih.gov/ws/query",
     {"db": "healthTopics", "term": "diabetes", "retmax": "3"},
     _p_zaehlt("<document "), None),
    ("MITRE ATT&CK", "mitre_attack",
     "https://raw.githubusercontent.com/mitre/cti/master/enterprise-attack/"
     "enterprise-attack.json", {}, _p_pfad("objects"), None),
    ("Nominatim", "nominatim", "https://nominatim.openstreetmap.org/search",
     {"q": "Wien", "format": "json", "limit": "1"}, _p_liste, None),
    ("NVD", "nvd", "https://services.nvd.nist.gov/rest/json/cves/2.0",
     {"keywordSearch": "openssl", "resultsPerPage": "2"},
     _p_pfad("vulnerabilities"), None),
    ("openFDA", "openfda", "https://api.fda.gov/drug/event.json",
     {"limit": "2"}, _p_pfad("results"), None),
    ("OSV", "osv", "https://api.osv.dev/v1/vulns/GHSA-jfh8-c2jp-5v3q", {},
     _p_pfad("id"), None),
    ("Our World in Data", "owid",
     "https://ourworldindata.org/grapher/life-expectancy.csv", {}, _p_csv, None),
    ("USPSTF", "uspstf",
     "https://data.uspreventiveservicestaskforce.org/api/json", {},
     _p_nicht_leer, None),
    ("Wikipedia (REST)", "wikipedia",
     "https://de.wikipedia.org/api/rest_v1/page/summary/Wien", {},
     _p_pfad("extract"), None),
    ("UNESCO Welterbe", "world_heritage", "https://whc.unesco.org/en/list/xml/",
     {}, _p_zaehlt("<row"), None),
    ("UNHCR", "unhcr", "https://api.unhcr.org/population/v1/population/",
     {"limit": "2", "yearFrom": "2023"}, _p_pfad("items"), None),
    ("UNECE", "unece", "https://w3.unece.org/PXWeb2015/api/v1/en/STAT", {},
     _p_liste, None),
    ("DefiLlama", "defillama", "https://api.llama.fi/protocols", {},
     _p_liste, None),

    # --- Dritte Welle (2026-09-07) ----------------------------------------
    # Wieder gilt: jede Sonde einzeln gegen die echte API gefahren. Fuenf
    # brauchten Zusaetze, die es im Monitor vorher nicht gab — POST-Rumpf und
    # eigene Accept-Header (die SDMX-Endpunkte antworten sonst mit 406).
    ("Parlament AT (Nationalrat)", "parlament_at",
     "https://www.parlament.gv.at/Filter/api/json/post"
     "?jsMode=EVAL&FBEZ=WFW_002&listeId=10002&showAll=true&M=M&W=W", {},
     _p_nicht_leer, None, {"post": {}}),
    ("AI Incident Database", "aiid", "https://incidentdatabase.ai/api/graphql",
     {}, _p_pfad("data"), None,
     {"post": {"query": "{incidents(limit:1){incident_id}}"}}),
    ("BIS", "bis",
     "https://stats.bis.org/api/v2/data/dataflow/BIS/WS_CBPOL/1.0/D.AT",
     {"lastNObservations": "1", "format": "jsondata"},
     # Genau dieser Accept — mit "*/*" oder "application/json" antwortet
     # BIS mit HTTP 406.
     _p_zaehlt("dataSets"), None,
     {"headers": {"Accept": "application/vnd.sdmx.data+json;version=1.0.0"}}),
    ("OECD SDMX", "oecd_sdmx",
     "https://sdmx.oecd.org/public/rest/data/"
     "OECD.ELS.SPD,DSD_SOCX_AGG@DF_PUB_PRV,1.0/all",
     {"lastNObservations": "1"}, _p_zaehlt("dataSets"), None, _SDMX),
    ("ITF Verkehr (OECD SDMX)", "itf_transport",
     "https://sdmx.oecd.org/public/rest/data/"
     "OECD.ITF,DSD_TRENDS@DF_TRENDSSAFETY,1.0/all",
     {"lastNObservations": "1"}, _p_zaehlt("dataSets"), None, _SDMX),
    ("OECD (Fakten-Weg)", "oecd",
     "https://sdmx.oecd.org/public/rest/data/"
     "OECD.EDU.ECS,DSD_TALIS@DF_TALIS,1.0/all",
     {"lastNObservations": "1"}, _p_zaehlt("dataSets"), None, _SDMX),
    ("WITS", "wits",
     "https://wits.worldbank.org/API/V1/SDMX/V21/datasource/tradestats-tariff"
     "/reporter/aut/year/2020/partner/wld/product/all/indicator/AHS-SMPL-AVRG",
     {"format": "JSON"}, _p_nicht_leer, None),
    ("Parlament AT (Abstimmungen)", "abstimmungen",
     "https://www.parlament.gv.at/recherchieren/open-data/", {},
     _p_zaehlt("Open Data"), None),
    ("CEPII (ueber DBnomics)", "cepii",
     "https://api.db.nomics.world/v22/series/CEPII/CHELEM-TRADE-GTAP",
     {"limit": "1"}, _p_pfad("series", "docs"), None),
    ("DBnomics (Anbieterliste)", "dbnomics",
     "https://api.db.nomics.world/v22/providers", {},
     _p_pfad("providers", "docs"), None),
    ("Constitute", "constitute",
     "https://www.constituteproject.org/service/constitutions", {},
     _p_liste, None),
    ("NASA GISS (Copernicus-Weg)", "copernicus",
     "https://data.giss.nasa.gov/gistemp/tabledata_v4/GLB.Ts+dSST.csv", {},
     _p_csv, None),
    ("Copernicus CDS-Katalog", "era5",
     "https://cds.climate.copernicus.eu/api/catalogue/v1/collections", {},
     _p_pfad("collections"), None),
    ("EDGAR (JRC-Emissionen)", "edgar",
     "https://edgar.jrc.ec.europa.eu/dataset_ghg2024", {},
     _p_zaehlt("EDGAR"), None),
    ("EDPB", "edpb",
     "https://www.edpb.europa.eu/our-work-tools/our-documents_en", {},
     _p_zaehlt("EDPB"), None),
    ("EMA (Arzneimittel-Report)", "ema",
     "https://www.ema.europa.eu/en/documents/report/"
     "medicines-output-medicines-report_en.xlsx", {}, _p_bytes, None),
    ("ERIC", "eric", "https://api.ies.ed.gov/eric/",
     {"search": "reading", "format": "json", "rows": "2"},
     _p_pfad("response", "docs"), None),
    ("EUvsDisinfo (Feed)", "euvsdisinfo", "https://euvsdisinfo.eu/feed/", {},
     _p_zaehlt("<item"), None),
    ("GADMO (Correctiv-Feed)", "gadmo",
     "https://correctiv.org/faktencheck/feed/", {}, _p_zaehlt("<item"), None),
    ("GeoSphere Austria", "geosphere",
     "https://dataset.api.hub.geosphere.at/v1/datasets", {},
     _p_nicht_leer, None),
    ("SPARTACUS (GeoSphere)", "spartacus",
     "https://dataset.api.hub.geosphere.at/v1/datasets", {},
     _p_nicht_leer, None),
    ("Getty Vocabularies", "getty", "http://vocab.getty.edu/sparql.json",
     {"query": "SELECT ?s WHERE {?s a skos:Concept} LIMIT 1"},
     _p_pfad("results", "bindings"), None),
    ("Global Carbon Budget", "global_carbon_budget",
     "https://api.github.com/repos/openclimatedata/global-carbon-budget", {},
     _p_pfad("full_name"), None),
    ("IRENA", "irena", "https://pxweb.irena.org/api/v1/en/IRENASTAT", {},
     _p_liste, None),
    ("Mimikama", "mimikama", "https://www.mimikama.org/feed/", {},
     _p_zaehlt("<item"), None),
    ("NBER", "nber", "https://www.nber.org/rss/new.xml", {},
     _p_zaehlt("<item"), None),
    ("Mozilla Observatory", "observatory",
     "https://observatory-api.mdn.mozilla.net/api/v2/analyze",
     {"host": "evidora.eu"}, _p_pfad("scan"), None),
    ("OEAW ePub (OAI)", "oeaw_epub", "https://epub.oeaw.ac.at/oai",
     {"verb": "Identify"}, _p_zaehlt("<Identify"), None),
    ("OeNB (ueber EZB-MIR)", "oenb_sdmx",
     "https://data-api.ecb.europa.eu/service/data/MIR/M.AT.B.L21.A.R.A.2250.EUR.N",
     {"format": "jsondata", "lastNObservations": "1"},
     _p_zaehlt("dataSets"), None),
    ("ParlGov", "parlgov", "https://www.parlgov.org/data-info/", {},
     _p_zaehlt("ParlGov"), None),
    ("RIS (Bundesrecht)", "ris",
     "https://data.bka.gv.at/ris/api/v2.6/Bundesrecht",
     {"Applikation": "BrKons", "Suchworte": "Datenschutz",
      "DokumenteProSeite": "Ten"}, _p_zaehlt("OgdSearchResult"), None),
    ("SIPRI (ueber OWID)", "sipri",
     "https://ourworldindata.org/grapher/military-spending-sipri.csv", {},
     _p_csv, None),
    ("UNESCO UIS", "unesco_uis",
     "https://api.uis.unesco.org/api/public/data/indicators",
     {"indicator": "CR.1", "geoUnit": "AUT"}, _p_nicht_leer, None),
    ("BMI Volksbegehren", "volksbegehren", "https://www.bmi.gv.at/", {},
     _p_zaehlt("Bundesministerium"), None),
    ("BMI Wahlen", "wahlen", "https://www.bmi.gv.at/412/", {},
     _p_zaehlt("Wahl"), None),
    ("Wayback (CDX)", "wayback", "https://web.archive.org/cdx/search/cdx",
     {"url": "orf.at", "limit": "2", "output": "json"}, _p_liste, None),
    ("WCAG 2.2 (ACT-Mapping)", "wcag22",
     "https://raw.githubusercontent.com/w3c/wcag/main/guidelines/"
     "act-mapping.json", {}, _p_pfad("act-rules"), None),
    ("WebAIM Million", "webaim", "https://webaim.org/projects/million/", {},
     _p_zaehlt("WebAIM Million"), None),
    ("FRED", "fred", "https://fred.stlouisfed.org/graph/fredgraph.csv",
     {"id": "GDP"}, _p_csv, None),
    ("CORDIS", "cordis",
     "https://cordis.europa.eu/data/cordis-HORIZONprojects-json.zip", {},
     _p_bytes, None),
    ("Semantic Scholar", "semantic_scholar",
     "https://api.semanticscholar.org/graph/v1/paper/search",
     {"query": "vaccine", "limit": "2"}, _p_pfad("data"), None),

    # Token-Sonden: ohne gesetzte Variable werden sie uebersprungen, nicht
    # als Ausfall gemeldet (#128).
    ("CAMS (Copernicus ADS)", "cams",
     "https://ads.atmosphere.copernicus.eu/api/catalogue/v1/collections", {},
     _p_pfad("collections"), "ADS_API_KEY"),
    ("CITES (Species+)", "cites",
     "https://api.speciesplus.net/api/v1/taxon_concepts", {"per_page": "1"},
     _p_liste, "CITES_TOKEN"),
    ("DPLA", "dpla", "https://api.dp.la/v2/items",
     {"q": "vienna", "page_size": "2"}, _p_pfad("docs"), "DPLA_API_KEY"),
    ("Europeana", "europeana",
     "https://api.europeana.eu/record/v2/search.json",
     {"query": "wien", "rows": "2"}, _p_pfad("items"), "EUROPEANA_API_KEY"),
    ("GeoNames", "geonames", "https://secure.geonames.org/searchJSON",
     {"q": "Wien", "maxRows": "1"}, _p_pfad("geonames"), "GEONAMES_USERNAME"),
    ("IDMC", "idmc",
     "https://helix-tools-api.idmcdb.org/external-api/idus/last/5/", {},
     _p_liste, "IDMC_CLIENT_ID"),
    ("IUCN Red List", "iucn", "https://apiv3.iucnredlist.org/api/v3/version",
     {}, _p_pfad("version"), "IUCN_TOKEN"),
    ("NOAA CDO", "noaa", "https://www.ncei.noaa.gov/cdo-web/api/v2/datasets",
     {"limit": "2"}, _p_pfad("results"), "NOAA_API_TOKEN"),
    ("OpenAQ", "openaq", "https://api.openaq.org/v3/parameters",
     {"limit": "2"}, _p_pfad("results"), "OPENAQ_API_KEY"),
    ("UN Comtrade", "uncomtrade",
     "https://comtradeapi.un.org/data/v1/get/C/A/HS",
     {"reporterCode": "40", "period": "2022", "cmdCode": "TOTAL",
      "flowCode": "M"}, _p_pfad("data"), "COMTRADE_API_KEY"),
]

# --------------------------------------------------------------------------
# Quellen, die NACHWEISLICH kaputt sind — und zwar nicht durch uns.
#
# Dieselbe Ueberlegung wie bei der Waechter-Klasse BLOCKIERT (#142): ein
# Wecker, den man nicht abstellen kann, bringt einem bei, Wecker zu
# ignorieren. Diese Sonden laufen mit und stehen im Bericht, loesen aber
# KEINEN Push aus. Aufnahme nur mit geprueftem Grund UND einer Bedingung,
# unter der der Eintrag wieder verschwindet.
# --------------------------------------------------------------------------
BEKANNT_DEFEKT = {
    "WHO Europe (Health for All)": (
        "2026-09-07: TLS-Kette unvollstaendig. dw.euro.who.int sendet das "
        "Intermediate-Zertifikat nicht mit; aus dem Prod-Container schlaegt "
        "die Pruefung mit CERTIFICATE_VERIFY_FAILED fehl (vom Entwickler-Mac "
        "aus nicht, dort liegt das Intermediate im Store). Das Server-Zert "
        "selbst ist gueltig bis 2026-11-02. "
        "WIEDER AUFNEHMEN, wenn die Sonde ohne Anpassung durchlaeuft."),
    "Getty Vocabularies": (
        "2026-09-07: HTTP 403 vom PROD-HOST, mit und ohne Browser-UA — vom "
        "Entwickler-Mac aus HTTP 200. vocab.getty.edu blockt offenbar die "
        "Server-IP oder deren Bereich. Dieselbe Klasse wie WHO Europe: der "
        "Ausfall ist nur dort sichtbar, wo der Dienst laeuft. "
        "WIEDER AUFNEHMEN, sobald die Sonde aus dem Container 200 liefert."),
    "OpenAQ": (
        "2026-09-07: HTTP 401 mit dem in .env hinterlegten OPENAQ_API_KEY, "
        "auf /v3/parameters UND /v3/locations, vom Prod-Host geprueft. Der "
        "Key ist gesetzt (65 Zeichen), wird aber abgewiesen — vermutlich "
        "abgelaufen oder widerrufen. BEHEBBAR AUF UNSERER SEITE: neuen Key "
        "unter openaq.org anfordern und in .env eintragen. "
        "WIEDER AUFNEHMEN, sobald die Sonde 200 liefert."),
    "AI Incident Database": (
        "2026-09-07: HTTP 403 auf POST /api/graphql, reproduziert vom "
        "Entwickler-Mac und vom Prod-Host. Der Endpunkt lebt (GET antwortet "
        "mit 400 'GraphQL requires POST') und die Website liefert 200 — die "
        "Abfrage selbst wird abgewiesen, vermutlich Bot-Schutz oder eine neu "
        "eingefuehrte Authentifizierung. "
        "WIEDER AUFNEHMEN, sobald der POST wieder 200 liefert."),
    "FAOSTAT": (
        "2026-09-07: HTTP 521 (Cloudflare: Ursprungsserver nicht erreichbar), "
        "reproduziert vom Entwickler-Mac, vom Prod-Host und aus dem Container. "
        "Ausfall auf Seiten der FAO, nicht bei uns. "
        "WIEDER AUFNEHMEN, sobald die API wieder 200 liefert."),
}

SONDEN += [
    ("WHO Europe (Health for All)", "who_europe",
     "https://dw.euro.who.int/api/v5/measures", {}, _p_liste, None),
    ("FAOSTAT", "faostat",
     "https://fenixservices.fao.org/faostat/api/v1/en/data/QCL",
     {"area": "11", "item": "15", "element": "5510", "year": "2022"},
     _p_pfad("data"), None),
]

# Token, die als Query-Parameter statt als Header gehen.
TOKEN_ALS_PARAMETER = {
    "GOOGLE_FACTCHECK_API_KEY": "key",
    "DPLA_API_KEY": "api_key",
    "EUROPEANA_API_KEY": "wskey",
    "GEONAMES_USERNAME": "username",
    "COMTRADE_API_KEY": "subscription-key",
    "ADS_API_KEY": "key",
    "IUCN_TOKEN": "token",
}
TOKEN_ALS_HEADER = {
    "UCDP_TOKEN": "x-ucdp-access-token",
    "CITES_TOKEN": "X-Authentication-Token",
    "NOAA_API_TOKEN": "token",
    "OPENAQ_API_KEY": "X-API-Key",
    "IDMC_CLIENT_ID": "Authorization",
}


def _post_alert(webhook: str, title: str, message: str) -> None:
    """ntfy-Push — gleiche Mechanik wie canary_check und data_freshness_check."""
    if not webhook:
        print("WARN: kein EVIDORA_ALERT_WEBHOOK gesetzt — kein Push",
              file=sys.stderr)
        return
    try:
        req = urllib.request.Request(
            webhook, data=message.encode("utf-8"),
            headers={"Title": title, "Priority": "urgent",
                     "Tags": "rotating_light"})
        urllib.request.urlopen(req, timeout=10)
        print("Alert gesendet.")
    except Exception as e:  # noqa: BLE001
        print(f"WARN: Alert-Push fehlgeschlagen: {e}", file=sys.stderr)


# Fluechtige Fehler: ein einzelner Timeout oder ein 5xx sagt nichts ueber die
# Quelle. Im ersten Prod-Lauf der vollen Batterie lief genau das auf: Wayback
# lief einmal in einen Read-Timeout und war Sekunden spaeter wieder da — der
# Lauf schlug Alarm. Ein Waechter, der bei jedem Schluckauf schreit, wird
# abgeschaltet (#128), deshalb ein zweiter Versuch, bevor gemeldet wird.
FLUECHTIG = {"unerreichbar"}
FLUECHTIGE_CODES = {500, 502, 503, 504}


def sonde_laufen(sonde, timeout: float, versuche: int = 2) -> dict:
    """Eine Sonde fahren.

    Sonden sind 6- oder 7-Tupel. Das siebte Feld traegt Zusaetze, die einzelne
    Quellen brauchen:
      ``{"post": {...}}``    POST mit JSON-Rumpf statt GET — `parlament_at`
                             und `aiid` erwarten das, auch wenn die Filter im
                             Query-String stehen.
      ``{"headers": {...}}`` zusaetzliche Header — die SDMX-Endpunkte
                             antworten ohne passendes ``Accept`` mit 406.
    """
    ergebnis = _sonde_einmal(sonde, timeout)
    fluechtig = (ergebnis["status"] in FLUECHTIG
                 or (ergebnis["status"] == "http_fehler"
                     and any(f"HTTP {c}" == ergebnis.get("grund")
                             for c in FLUECHTIGE_CODES)))
    if fluechtig and versuche > 1:
        ergebnis = _sonde_einmal(sonde, timeout)
        ergebnis["wiederholt"] = True
    return ergebnis


def _sonde_einmal(sonde, timeout: float) -> dict:
    extra = sonde[6] if len(sonde) > 6 else {}
    name, service, url, params, pruefer, token_var = sonde[:6]
    params = dict(params)
    headers = {"User-Agent": USER_AGENT,
               "Accept": "application/json, text/csv, */*"}

    if token_var:
        wert = (os.getenv(token_var) or "").strip()
        if not wert:
            return {"name": name, "service": service, "status": "uebersprungen",
                    "grund": f"{token_var} nicht gesetzt"}
        if token_var in TOKEN_ALS_PARAMETER:
            params[TOKEN_ALS_PARAMETER[token_var]] = wert
        else:
            headers[TOKEN_ALS_HEADER[token_var]] = wert

    headers.update(extra.get("headers") or {})
    trenner = "&" if "?" in url else "?"
    voll = url + (trenner + urllib.parse.urlencode(params) if params else "")
    rumpf = None
    if extra.get("post") is not None:
        rumpf = json.dumps(extra["post"]).encode("utf-8")
        headers["Content-Type"] = "application/json"
    try:
        with urllib.request.urlopen(
                urllib.request.Request(voll, data=rumpf, headers=headers),
                timeout=timeout) as r:
            text = r.read().decode("utf-8", "replace")
            code = r.status
    except urllib.error.HTTPError as e:
        if e.code == 429:
            # "Zu oft gefragt" ist keine Aussage ueber die Quelle. Semantic
            # Scholar drosselt anonyme Zugriffe regelmaessig; ein Alarm daraus
            # waere der klassische Fehlalarm, an dem Waechter sterben (#128).
            return {"name": name, "service": service, "status": "gedrosselt",
                    "grund": "HTTP 429 — Rate-Limit, sagt nichts ueber die "
                             "Verfuegbarkeit der Quelle"}
        return {"name": name, "service": service, "status": "http_fehler",
                "grund": f"HTTP {e.code}"}
    except Exception as e:  # noqa: BLE001
        return {"name": name, "service": service, "status": "unerreichbar",
                "grund": f"{type(e).__name__}: {e}"}

    try:
        zeilen = pruefer(text)
    except Exception as e:  # noqa: BLE001
        return {"name": name, "service": service, "status": "unlesbar",
                "grund": f"{type(e).__name__}: {e}", "http": code}

    if zeilen == -1:
        return {"name": name, "service": service, "status": "kaputte_abfrage",
                "grund": "HTTP 200, aber Fehler-Objekt statt Daten",
                "http": code}
    if zeilen <= 0:
        return {"name": name, "service": service, "status": "stumm",
                "grund": "antwortet, liefert aber keine Datenzeile", "http": code}
    return {"name": name, "service": service, "status": "ok",
            "zeilen": zeilen, "http": code}


# Diese Zustaende sind ein Befund, nicht nur ein Hinweis.
ALARM = {"kaputte_abfrage", "stumm", "http_fehler", "unlesbar", "unerreichbar"}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--timeout", type=float, default=45.0)
    ap.add_argument("--json", action="store_true", help="Maschinenlesbar")
    ap.add_argument("--nur", help="nur Sonden, deren Name das enthaelt")
    ap.add_argument("--alert-webhook",
                    default=os.getenv("EVIDORA_ALERT_WEBHOOK", ""))
    a = ap.parse_args()

    sonden = [s for s in SONDEN if not a.nur or a.nur.lower() in s[0].lower()]
    if not sonden:
        print(f"Keine Sonde passt auf {a.nur!r}", file=sys.stderr)
        return 2

    ergebnisse = [sonde_laufen(s, a.timeout) for s in sonden]
    for e in ergebnisse:
        if e["name"] in BEKANNT_DEFEKT and e["status"] in ALARM:
            e["status"] = "bekannt_defekt"
            e["grund"] = BEKANNT_DEFEKT[e["name"]]
        elif e["name"] in BEKANNT_DEFEKT and e["status"] == "ok":
            # Der erfreuliche Fall: die Quelle ist zurueck. Das gehoert
            # gemeldet, damit der Eintrag verschwindet statt zu versteinern.
            e["grund"] = "wieder erreichbar — Eintrag in BEKANNT_DEFEKT entfernen"
    schlecht = [e for e in ergebnisse if e["status"] in ALARM]

    if a.json:
        print(json.dumps({"gesamt": len(ergebnisse),
                          "auffaellig": len(schlecht),
                          "ergebnisse": ergebnisse},
                         ensure_ascii=False, indent=1))
    else:
        for e in ergebnisse:
            marke = {"ok": "OK", "uebersprungen": "—"}.get(e["status"],
                                                           e["status"].upper())
            zusatz = (f"{e['zeilen']} Datenzeilen" if e["status"] == "ok"
                      else e.get("grund", ""))
            print(f"  {marke:16} {e['name']:38} {zusatz[:120]}")
        # Die Kategorien getrennt ausweisen. "51/51 gruen" waere unehrlich,
        # solange zwei Quellen nachweislich tot sind — sie loesen nur bewusst
        # keinen Push aus.
        gruen = [e for e in ergebnisse if e["status"] == "ok"]
        defekt = [e for e in ergebnisse if e["status"] == "bekannt_defekt"]
        uebersprungen = [e for e in ergebnisse if e["status"] == "uebersprungen"]
        print(f"\n{len(gruen)}/{len(ergebnisse)} Sonden liefern Daten. "
              f"{len(defekt)} bekannt defekt, {len(uebersprungen)} uebersprungen, "
              f"{len(schlecht)} auffaellig.")
        if defekt:
            print("  bekannt defekt: " + ", ".join(e["name"] for e in defekt))
        print(f"  Abdeckung: {len({s[1] for s in sonden})} von {ANZAHL_LIVE_KONNEKTOREN} "
              f"Live-Konnektoren.")

    if schlecht:
        text = "\n".join(f"{e['name']} ({e['service']}): {e['status']} — "
                         f"{e.get('grund','')}" for e in schlecht)
        _post_alert(a.alert_webhook,
                    f"Evidora: {len(schlecht)} Quelle(n) ohne Lebenszeichen",
                    text)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
