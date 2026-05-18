"""CITES Species+ — Internationaler Artenhandel-Schutz.

Datenquelle: CITES (Convention on International Trade in Endangered
Species of Wild Fauna and Flora). 184 Vertragsstaaten, drei Anhänge
(Anhang I = Handel verboten / II = streng reguliert / III = nationaler
Schutz mit internationaler Kooperation). Species+ ist der gemeinsame
Datenservice von UNEP-WCMC und dem CITES-Sekretariat. Trade Database
enthält ~20 Mio. dokumentierte Handels-Records seit 1975.

API-Endpoint:
  https://api.speciesplus.net/api/v1/taxon_concepts?name={sci_name}
  https://api.speciesplus.net/api/v1/taxon_concepts/{id}/distributions

Auth: Header ``X-Authentication-Token: {token}``. Token kostenlos via
UNEP-WCMC unter https://api.speciesplus.net/ (Account erforderlich).

Format: JSON.

Lizenz: UNEP-WCMC-Bedingungen — Faktenreferenz frei nutzbar mit
Zitation. Trade-Records sind Public Domain.

CITES-Anhänge:
  I   = Vom Aussterben bedroht, kommerzieller Handel verboten
        (z.B. Tiger, Nashorn, Großer Panda, alle Meeresschildkröten,
        afrikanischer Elefant in 30 von 37 Verbreitungsstaaten)
  II  = Nicht akut bedroht, aber Handel ohne strenge Kontrolle würde
        Aussterben bedrohen (z.B. afrikanischer Elefant in Botswana/
        Namibia/Simbabwe/Südafrika, Lebende Korallen, Bärenfelle)
  III = Auf Antrag eines einzelnen Staates gelistet (z.B. Walross
        durch Kanada, Honey Badger durch Botswana)

Komplementär zu IUCN:
  - IUCN Red List = wissenschaftliche Gefährdungsbewertung (EX/CR/EN/VU/NT/LC)
  - CITES = internationale Handelsregulierung (Anhang I/II/III)
  Eine Spezies kann z.B. IUCN-LC sein, aber CITES-II (vorsorglich).
  Umgekehrt: IUCN-CR + CITES-I für stark gehandelte Arten (Tiger).

Trigger: "CITES-Anhang I/II/III", "CITES-Listung", "Artenhandel [Spezies]",
"Wildlife Trade", "Elfenbein-Handel", "Nashorn-Handel", "Tropenholz CITES".

Politische Guardrails: Handelslisten sind völkerrechtliche Fakten ohne
politische Aufladung — keine Tabu-Berührung.
"""

# WIRING für main.py:
# from services.cites import search_cites, claim_mentions_cites_cached
# if claim_mentions_cites_cached(claim):
#     tasks.append(cached("CITES Species+", search_cites, analysis))
#     queried_names.append("CITES Species+")
#
# WIRING für data_updater.py (Prefetch — optional, Fallback ist statisch):
#   keine Aktion nötig; Live-Pfad cached selbst on-demand.
#
# WIRING für reranker (Whitelist):
#   "CITES Species+" zur Trusted-Source-Liste hinzufügen.

from __future__ import annotations

import logging
import os
import time
from functools import lru_cache
from urllib.parse import quote

from services._http_polite import polite_client

logger = logging.getLogger("evidora")

# ---------------------------------------------------------------------------
# Konstanten
# ---------------------------------------------------------------------------
CITES_API_BASE = "https://api.speciesplus.net/api/v1"
CITES_PORTAL = "https://speciesplus.net/species"

CACHE_TTL = 24 * 3600  # 24 h
TIMEOUT_S = 15.0
MAX_RESULTS = 5

# Cache: key=(kind, query) → (timestamp, list[result])
_cache: dict[tuple, tuple[float, list[dict]]] = {}

# ---------------------------------------------------------------------------
# Anhang-Labels (DE)
# ---------------------------------------------------------------------------
_APPENDIX_LABEL_DE = {
    "I": "Anhang I — Vom Aussterben bedroht; kommerzieller internationaler Handel verboten",
    "II": "Anhang II — Handel streng reguliert (Export-/Importgenehmigung erforderlich)",
    "III": "Anhang III — Auf Antrag eines Vertragsstaats gelistet; nationale Schutzmaßnahmen",
    "I/II": "Anhang I/II — Split-Listung (verschiedene Populationen unterschiedlich gelistet)",
    "I/II/III": "Anhang I/II/III — Mehrfach-Listung",
    "NC": "Nicht CITES-gelistet",
}

# ---------------------------------------------------------------------------
# Fallback-Liste: ~40 prominente CITES-gelistete Arten
# Quelle: CITES Appendices (Stand 25. November 2025, gültig seit COP19/2022)
# https://cites.org/eng/app/appendices.php
# ---------------------------------------------------------------------------
_FALLBACK_SPECIES: list[dict] = [
    # ---- Großkatzen (alle Anhang I oder II) ----
    {"de": "Tiger", "en": "Tiger", "sci": "Panthera tigris", "app": "I",
     "since": "1975",
     "trade_note": "Alle Tigerteile (Felle, Knochen, Krallen) verboten; "
                   "TCM-Druck weiter hoch."},
    {"de": "Löwe", "en": "Lion", "sci": "Panthera leo", "app": "II",
     "since": "1977",
     "trade_note": "Wildgefangene Knochen seit 2022 quotenfrei (de facto-Verbot); "
                   "Trophäenjagd nach Quote möglich."},
    {"de": "Leopard", "en": "Leopard", "sci": "Panthera pardus", "app": "I",
     "since": "1975",
     "trade_note": "Trophäenjagd nur in 12 Verbreitungsstaaten mit Quote zulässig."},
    {"de": "Schneeleopard", "en": "Snow Leopard",
     "sci": "Panthera uncia", "app": "I",
     "since": "1975",
     "trade_note": "Felle und Knochen weiter im Schwarzmarkt Zentralasien."},
    {"de": "Gepard", "en": "Cheetah", "sci": "Acinonyx jubatus", "app": "I",
     "since": "1975",
     "trade_note": "Lebende Cubs für Golf-Heimtierhaltung — illegaler Handel "
                   "Hauptbedrohung."},
    {"de": "Jaguar", "en": "Jaguar", "sci": "Panthera onca", "app": "I",
     "since": "1975",
     "trade_note": "Zähne als TCM-Substitut für Tiger-Zähne — wachsender "
                   "Schwarzmarkt Lateinamerika."},
    # ---- Bären ----
    {"de": "Eisbär", "en": "Polar Bear",
     "sci": "Ursus maritimus", "app": "II",
     "since": "1992",
     "trade_note": "Felle aus Kanada (Inuit-Quote) legal handelbar; "
                   "Heraufstufungs-Anträge bisher abgelehnt."},
    {"de": "Großer Panda", "en": "Giant Panda",
     "sci": "Ailuropoda melanoleuca", "app": "I",
     "since": "1984",
     "trade_note": "Kein kommerzieller Handel; nur Leih-Pandas China → "
                   "Zoos international."},
    {"de": "Braunbär", "en": "Brown Bear",
     "sci": "Ursus arctos", "app": "I/II",
     "since": "1975",
     "trade_note": "Bhutan/China/Mexiko/Mongolei in Anhang I; übrige "
                   "Populationen Anhang II."},
    # ---- Elefanten ----
    {"de": "Afrikanischer Elefant", "en": "African Elephant",
     "sci": "Loxodonta africana", "app": "I/II",
     "since": "1977/1997",
     "trade_note": "Botswana, Namibia, Simbabwe, Südafrika in Anhang II; "
                   "alle anderen Anhang I. Elfenbeinhandel international "
                   "seit 1989 verboten (mit Ausnahme der 1999/2008 "
                   "One-off-Verkäufe nach Japan/China)."},
    {"de": "Asiatischer Elefant", "en": "Asian Elephant",
     "sci": "Elephas maximus", "app": "I",
     "since": "1975",
     "trade_note": "Elfenbein-Handel verboten; Arbeitselefanten Thailand "
                   "regulierungstechnisch ausgenommen."},
    # ---- Nashörner (alle 5 Arten) ----
    {"de": "Sumatra-Nashorn", "en": "Sumatran Rhinoceros",
     "sci": "Dicerorhinus sumatrensis", "app": "I",
     "since": "1975",
     "trade_note": "Akut vom Aussterben bedroht (<80 Tiere); "
                   "Handel komplett verboten."},
    {"de": "Java-Nashorn", "en": "Javan Rhinoceros",
     "sci": "Rhinoceros sondaicus", "app": "I",
     "since": "1975",
     "trade_note": "~76 Tiere weltweit; kein Handel."},
    {"de": "Panzernashorn", "en": "Indian Rhinoceros",
     "sci": "Rhinoceros unicornis", "app": "I",
     "since": "1975",
     "trade_note": "Indien/Nepal; Horn-Handel verboten."},
    {"de": "Spitzmaulnashorn", "en": "Black Rhinoceros",
     "sci": "Diceros bicornis", "app": "I",
     "since": "1977",
     "trade_note": "Trophäenjagd-Quoten Namibia/Südafrika eng begrenzt."},
    {"de": "Breitmaulnashorn", "en": "White Rhinoceros",
     "sci": "Ceratotherium simum", "app": "I/II",
     "since": "1977",
     "trade_note": "Südafrika/Eswatini Anhang II (lebende Tiere + "
                   "Trophäen); übrige Anhang I. Horn-Handel international "
                   "verboten."},
    # ---- Primaten ----
    {"de": "Berggorilla", "en": "Mountain Gorilla",
     "sci": "Gorilla beringei beringei", "app": "I",
     "since": "1975",
     "trade_note": "Kommerzieller Handel verboten; Bushmeat + lebende "
                   "Cubs Hauptbedrohung."},
    {"de": "Orang-Utan (Borneo)", "en": "Bornean Orangutan",
     "sci": "Pongo pygmaeus", "app": "I",
     "since": "1975",
     "trade_note": "Heimtierhandel + Palmöl-Habitatverlust."},
    {"de": "Orang-Utan (Sumatra)", "en": "Sumatran Orangutan",
     "sci": "Pongo abelii", "app": "I",
     "since": "1975",
     "trade_note": "Anhang I seit Anfang an; <14.000 Tiere."},
    {"de": "Schimpanse", "en": "Chimpanzee",
     "sci": "Pan troglodytes", "app": "I",
     "since": "1977",
     "trade_note": "Kein kommerzieller Handel; Forschungs-/Zoo-Transfers "
                   "regulationspflichtig."},
    # ---- Meerestiere ----
    {"de": "Blauwal", "en": "Blue Whale",
     "sci": "Balaenoptera musculus", "app": "I",
     "since": "1975",
     "trade_note": "Alle Großwale Anhang I; Norwegen/Island/Japan "
                   "Vorbehalt eingelegt."},
    {"de": "Pottwal", "en": "Sperm Whale",
     "sci": "Physeter macrocephalus", "app": "I",
     "since": "1981",
     "trade_note": "Ambergris-Handel reguliert; Walfang von Japan/Norwegen "
                   "praktiziert (mit Vorbehalt)."},
    {"de": "Vaquita", "en": "Vaquita",
     "sci": "Phocoena sinus", "app": "I",
     "since": "1979",
     "trade_note": "~10 Tiere weltweit; Beifang in illegalen Totoaba-Netzen "
                   "Hauptursache."},
    {"de": "Weißer Hai", "en": "Great White Shark",
     "sci": "Carcharodon carcharias", "app": "II",
     "since": "2005",
     "trade_note": "Zähne, Kiefer, Flossen reguliert."},
    {"de": "Walhai", "en": "Whale Shark",
     "sci": "Rhincodon typus", "app": "II",
     "since": "2003",
     "trade_note": "Flossen-Handel reguliert."},
    {"de": "Riesenmanta", "en": "Giant Manta Ray",
     "sci": "Mobula birostris", "app": "II",
     "since": "2014",
     "trade_note": "Kiemen-Handel für TCM reguliert."},
    {"de": "Hammerhai (Bogenstirn-)", "en": "Scalloped Hammerhead",
     "sci": "Sphyrna lewini", "app": "II",
     "since": "2014",
     "trade_note": "Flossen-Quote pro Vertragsstaat."},
    # ---- Meeresschildkröten (alle 7 Arten Anhang I) ----
    {"de": "Lederschildkröte", "en": "Leatherback Turtle",
     "sci": "Dermochelys coriacea", "app": "I",
     "since": "1977",
     "trade_note": "Alle 7 Meeresschildkröten-Arten Anhang I."},
    {"de": "Echte Karettschildkröte", "en": "Hawksbill Turtle",
     "sci": "Eretmochelys imbricata", "app": "I",
     "since": "1975",
     "trade_note": "Tortoise-Shell-Handel ('bekko') international verboten; "
                   "Japan-Vorbehalt 1994 zurückgezogen."},
    # ---- Vögel ----
    {"de": "Kalifornischer Kondor", "en": "California Condor",
     "sci": "Gymnogyps californianus", "app": "I",
     "since": "1975",
     "trade_note": "Captive-Bred-Programm; Wildhandel verboten."},
    {"de": "Wanderfalke", "en": "Peregrine Falcon",
     "sci": "Falco peregrinus", "app": "I",
     "since": "1977",
     "trade_note": "Falknerei-Tiere mit CITES-Zertifikat handelbar; "
                   "Wildfang verboten."},
    {"de": "Graupapagei", "en": "African Grey Parrot",
     "sci": "Psittacus erithacus", "app": "I",
     "since": "2017",
     "trade_note": "2016 von Anhang II zu Anhang I heraufgestuft "
                   "(Heimtierhandel-Druck)."},
    # ---- Reptilien ----
    {"de": "Komodowaran", "en": "Komodo Dragon",
     "sci": "Varanus komodoensis", "app": "I",
     "since": "1977",
     "trade_note": "Lebend-Handel verboten; nur Zoo-Tauschprogramme."},
    {"de": "Grüner Leguan", "en": "Green Iguana",
     "sci": "Iguana iguana", "app": "II",
     "since": "1977",
     "trade_note": "Größtes Volumen im legalen Reptilien-Heimtierhandel."},
    # ---- Korallen / Wirbellose ----
    {"de": "Steinkorallen (alle Arten)", "en": "Stony Corals",
     "sci": "Scleractinia spp.", "app": "II",
     "since": "1985/1990",
     "trade_note": "Über 1.500 Arten in Anhang II; Aquarium-Handel "
                   "quotenpflichtig."},
    {"de": "Riesenmuschel", "en": "Giant Clam",
     "sci": "Tridacna gigas", "app": "II",
     "since": "1985",
     "trade_note": "Schalen + lebende Tiere für Aquaristik reguliert."},
    # ---- Pflanzen / Tropenholz ----
    {"de": "Brasilianisches Rosenholz", "en": "Brazilian Rosewood",
     "sci": "Dalbergia nigra", "app": "I",
     "since": "1992",
     "trade_note": "Gitarrenbau (Fretboards) — Lieferketten-Dokumentation "
                   "Pflicht."},
    {"de": "Palisander (alle Dalbergia-Arten)", "en": "Rosewood (all Dalbergia)",
     "sci": "Dalbergia spp.", "app": "II",
     "since": "2017",
     "trade_note": "Möbelimport aus Südostasien — viel illegaler Handel "
                   "über Vietnam/Laos."},
    {"de": "Afrikanisches Bubinga", "en": "Bubinga",
     "sci": "Guibourtia spp.", "app": "II",
     "since": "2017",
     "trade_note": "Schlagzeug-Kessel, Gitarrenkorpus."},
    {"de": "Mahagoni (Großblättriges)", "en": "Big-Leaf Mahogany",
     "sci": "Swietenia macrophylla", "app": "II",
     "since": "2003",
     "trade_note": "Möbel und Bootsbau; legales Volumen ~50.000 m3/Jahr."},
    {"de": "Agarholz / Adlerholz", "en": "Agarwood",
     "sci": "Aquilaria spp.", "app": "II",
     "since": "1995/2005",
     "trade_note": "Parfüm-Industrie ('Oud'); ~80 % illegaler Handel "
                   "(UNODC-Schätzung)."},
    # ---- Heimtierhandel-Schwerpunkte ----
    {"de": "Pangolin (Schuppentier)", "en": "Pangolin (all species)",
     "sci": "Manis spp. / Phataginus spp. / Smutsia spp.", "app": "I",
     "since": "2017",
     "trade_note": "Welthandel verbotenstes Säugetier; alle 8 Arten 2016 "
                   "von Anhang II zu Anhang I heraufgestuft. ~100.000 Tiere "
                   "pro Jahr illegal gewildert (UNODC)."},
    {"de": "Saiga-Antilope", "en": "Saiga Antelope",
     "sci": "Saiga tatarica", "app": "II",
     "since": "1995",
     "trade_note": "Hörner für TCM; 2022 Quote auf Null reduziert "
                   "(de facto-Verbot)."},
]

_DE_NAME_INDEX = {sp["de"].lower(): sp for sp in _FALLBACK_SPECIES}
_EN_NAME_INDEX = {sp["en"].lower(): sp for sp in _FALLBACK_SPECIES}
_SCI_NAME_INDEX = {sp["sci"].lower(): sp for sp in _FALLBACK_SPECIES}

# Spezies-Aliase: häufige Schreibvarianten / Plural-Formen / Trade-Items
_SPECIES_ALIASES: dict[str, str] = {
    "tiger": "Tiger",
    "tigern": "Tiger",
    "tigers": "Tiger",
    "tigerknochen": "Tiger",
    "löwen": "Löwe",
    "loewe": "Löwe",
    "loewen": "Löwe",
    "leoparden": "Leopard",
    "schneeleoparden": "Schneeleopard",
    "geparden": "Gepard",
    "jaguare": "Jaguar",
    "jaguaren": "Jaguar",
    "eisbären": "Eisbär",
    "eisbaer": "Eisbär",
    "eisbaeren": "Eisbär",
    "panda": "Großer Panda",
    "pandas": "Großer Panda",
    "grosser panda": "Großer Panda",
    "braunbären": "Braunbär",
    "braunbaer": "Braunbär",
    "braunbaeren": "Braunbär",
    "elefant": "Afrikanischer Elefant",
    "elefanten": "Afrikanischer Elefant",
    "afrikanische elefanten": "Afrikanischer Elefant",
    "asiatischer elefant": "Asiatischer Elefant",
    "asiatische elefanten": "Asiatischer Elefant",
    "elfenbein": "Afrikanischer Elefant",
    "elfenbeinhandel": "Afrikanischer Elefant",
    "elfenbein-handel": "Afrikanischer Elefant",
    "ivory": "Afrikanischer Elefant",
    "nashorn": "Breitmaulnashorn",
    "nashörner": "Breitmaulnashorn",
    "nashornhorn": "Breitmaulnashorn",
    "nashorn-handel": "Breitmaulnashorn",
    "rhinohorn": "Breitmaulnashorn",
    "rhino": "Breitmaulnashorn",
    "java-nashörner": "Java-Nashorn",
    "sumatra-nashörner": "Sumatra-Nashorn",
    "spitzmaulnashörner": "Spitzmaulnashorn",
    "schwarzes nashorn": "Spitzmaulnashorn",
    "weißes nashorn": "Breitmaulnashorn",
    "weisses nashorn": "Breitmaulnashorn",
    "panzernashörner": "Panzernashorn",
    "gorilla": "Berggorilla",
    "gorillas": "Berggorilla",
    "berggorillas": "Berggorilla",
    "orang-utan": "Orang-Utan (Borneo)",
    "orang-utans": "Orang-Utan (Borneo)",
    "orangutan": "Orang-Utan (Borneo)",
    "orangutans": "Orang-Utan (Borneo)",
    "sumatra-orang-utan": "Orang-Utan (Sumatra)",
    "schimpansen": "Schimpanse",
    "wal": "Blauwal",
    "wale": "Blauwal",
    "walen": "Blauwal",
    "walfang": "Blauwal",
    "blauwale": "Blauwal",
    "pottwale": "Pottwal",
    "vaquitas": "Vaquita",
    "kalifornischer schweinswal": "Vaquita",
    "weißer hai": "Weißer Hai",
    "weisser hai": "Weißer Hai",
    "weisshai": "Weißer Hai",
    "walhaie": "Walhai",
    "manta": "Riesenmanta",
    "mantarochen": "Riesenmanta",
    "hammerhai": "Hammerhai (Bogenstirn-)",
    "hammerhaie": "Hammerhai (Bogenstirn-)",
    "haifischflossen": "Hammerhai (Bogenstirn-)",
    "haiflossen": "Hammerhai (Bogenstirn-)",
    "shark fin": "Hammerhai (Bogenstirn-)",
    "shark fins": "Hammerhai (Bogenstirn-)",
    "lederschildkröten": "Lederschildkröte",
    "lederschildkroete": "Lederschildkröte",
    "meeresschildkröten": "Lederschildkröte",
    "meeresschildkroete": "Lederschildkröte",
    "karettschildkröte": "Echte Karettschildkröte",
    "schildpatt": "Echte Karettschildkröte",
    "bekko": "Echte Karettschildkröte",
    "kondor": "Kalifornischer Kondor",
    "kondore": "Kalifornischer Kondor",
    "wanderfalken": "Wanderfalke",
    "falknerei": "Wanderfalke",
    "graupapageien": "Graupapagei",
    "papagei": "Graupapagei",
    "papageien": "Graupapagei",
    "komodowaranen": "Komodowaran",
    "komodo-drache": "Komodowaran",
    "leguan": "Grüner Leguan",
    "leguane": "Grüner Leguan",
    "korallen": "Steinkorallen (alle Arten)",
    "steinkoralle": "Steinkorallen (alle Arten)",
    "riesenmuscheln": "Riesenmuschel",
    "tridacna": "Riesenmuschel",
    "rosenholz": "Brasilianisches Rosenholz",
    "palisander": "Palisander (alle Dalbergia-Arten)",
    "palisanderholz": "Palisander (alle Dalbergia-Arten)",
    "dalbergia": "Palisander (alle Dalbergia-Arten)",
    "tropenholz": "Palisander (alle Dalbergia-Arten)",
    "tropenhölzer": "Palisander (alle Dalbergia-Arten)",
    "bubinga": "Afrikanisches Bubinga",
    "mahagoni": "Mahagoni (Großblättriges)",
    "agarholz": "Agarholz / Adlerholz",
    "adlerholz": "Agarholz / Adlerholz",
    "oud": "Agarholz / Adlerholz",
    "agarwood": "Agarholz / Adlerholz",
    "pangolin": "Pangolin (Schuppentier)",
    "pangoline": "Pangolin (Schuppentier)",
    "schuppentier": "Pangolin (Schuppentier)",
    "schuppentiere": "Pangolin (Schuppentier)",
    "saiga": "Saiga-Antilope",
    "saiga-antilopen": "Saiga-Antilope",
}

# ---------------------------------------------------------------------------
# Trigger-Terms
# ---------------------------------------------------------------------------
_CITES_PRIMARY = (
    "cites", "cites-anhang", "cites anhang", "cites-listung",
    "cites listung", "cites-konvention", "cites konvention",
    "washingtoner artenschutzübereinkommen",
    "washingtoner artenschutzabkommen",
    "appendix i", "appendix ii", "appendix iii",
    "anhang i cites", "anhang ii cites", "anhang iii cites",
    "speciesplus", "species+",
)

_TRADE_TERMS = (
    "artenhandel", "wildtierhandel", "wildlife trade", "tierhandel",
    "wildtier-handel", "wildtier handel",
    "schmuggel", "gewildert", "wilderei", "wildern",
    "elfenbein", "elfenbeinhandel", "elfenbein-handel", "ivory",
    "nashornhorn", "nashorn-horn", "rhino horn", "rhinohorn",
    "tigerteile", "tigerknochen", "tiger bone",
    "haifischflossen", "haiflossen", "shark fin", "shark fins",
    "tropenholz", "tropenhölzer", "rosewood", "palisanderholz",
    "schildpatt", "tortoiseshell", "bekko",
    "walfang", "whaling",
    "trophäenjagd", "trophy hunting", "jagdtrophäe",
    "heimtierhandel", "exotic pet trade", "exotenhandel",
    "tcm", "traditional chinese medicine",
    "geschützte arten", "geschützte tierart", "geschützte tierarten",
    "internationaler artenschutz", "artenschutzabkommen",
    "handelsverbot", "exportverbot tier", "importverbot tier",
)


def _claim_mentions_cites(claim_lc: str) -> bool:
    """Trigger-Check für CITES-Handelsregulierungs-Claims.

    Strategie:
      1. Explizite CITES-Nennung → True.
      2. Handels-/Wilderei-Vokabular + bekannter Spezies-Name → True.
      3. Reine Spezies-Nennung ohne Handels-Kontext → False (das ist
         dann eher IUCN-Territorium).
    """
    if not claim_lc:
        return False

    # 1. Explizite CITES-Nennung
    if any(t in claim_lc for t in _CITES_PRIMARY):
        return True

    # 2. Handels-Vokabular vorhanden?
    has_trade = any(t in claim_lc for t in _TRADE_TERMS)
    if not has_trade:
        return False

    # 3. Spezies-Name (de/en/sci/Alias) im Claim?
    has_species = False
    for name in _DE_NAME_INDEX:
        if name in claim_lc:
            has_species = True
            break
    if not has_species:
        for alias in _SPECIES_ALIASES:
            if alias in claim_lc:
                has_species = True
                break
    if not has_species:
        for name in _EN_NAME_INDEX:
            if name in claim_lc:
                has_species = True
                break
    if not has_species:
        for sci in _SCI_NAME_INDEX:
            if sci in claim_lc:
                has_species = True
                break

    return has_species


@lru_cache(maxsize=512)
def claim_mentions_cites_cached(claim: str) -> bool:
    """LRU-gecachter Wrapper für Trigger-Check."""
    return _claim_mentions_cites((claim or "").lower())


# ---------------------------------------------------------------------------
# Resolver: Claim → Spezies-Liste
# ---------------------------------------------------------------------------
def _resolve_species_from_claim(claim_lc: str) -> list[dict]:
    """Identifiziere ein/mehrere Fallback-Spezies aus dem Claim-Text.

    Reihenfolge:
      1. Wissenschaftliche Namen (eindeutigster Match)
      2. DE-Namen (längere zuerst)
      3. EN-Namen (längere zuerst)
      4. Aliase (längere zuerst)
    Returns Liste eindeutiger Spezies-Dicts, max MAX_RESULTS.
    """
    found: list[dict] = []
    seen_sci: set[str] = set()

    def _push(sp: dict) -> None:
        sci = sp.get("sci", "")
        if sci and sci not in seen_sci:
            seen_sci.add(sci)
            found.append(sp)

    # 1. Wissenschaftliche Namen
    for sci, sp in _SCI_NAME_INDEX.items():
        if sci in claim_lc:
            _push(sp)

    # 2. DE-Namen — längere zuerst
    for name in sorted(_DE_NAME_INDEX.keys(), key=len, reverse=True):
        if name in claim_lc:
            _push(_DE_NAME_INDEX[name])

    # 3. EN-Namen
    for name in sorted(_EN_NAME_INDEX.keys(), key=len, reverse=True):
        if name in claim_lc:
            _push(_EN_NAME_INDEX[name])

    # 4. Aliase
    for alias in sorted(_SPECIES_ALIASES.keys(), key=len, reverse=True):
        if alias in claim_lc:
            canonical = _SPECIES_ALIASES[alias]
            sp = _DE_NAME_INDEX.get(canonical.lower())
            if sp:
                _push(sp)

    return found[:MAX_RESULTS]


# ---------------------------------------------------------------------------
# Live-API (best effort — Token via env CITES_TOKEN)
# ---------------------------------------------------------------------------
def _get_token() -> str | None:
    token = os.getenv("CITES_TOKEN") or os.getenv("CITES_API_KEY")
    return token or None


async def _fetch_cites_taxon(scientific_name: str) -> dict | None:
    """Live-Lookup gegen Species+-API. Returns dict mit
    cites_listing/full_name/author_year oder None bei Fehler/kein Token.
    """
    token = _get_token()
    if not token:
        logger.debug("cites: kein API-Token gesetzt — skip Live-Call")
        return None

    key = ("taxon", scientific_name.lower())
    now = time.time()
    cached = _cache.get(key)
    if cached and (now - cached[0]) < CACHE_TTL:
        records = cached[1]
        return records[0] if records else None

    url = f"{CITES_API_BASE}/taxon_concepts?name={quote(scientific_name)}"
    headers = {"X-Authentication-Token": token}
    try:
        async with polite_client(timeout=TIMEOUT_S) as client:
            resp = await client.get(
                url, headers=headers, follow_redirects=True,
            )
            if resp.status_code == 401:
                logger.info("cites: HTTP 401 — Token ungültig")
                _cache[key] = (now, [])
                return None
            if resp.status_code == 403:
                logger.info("cites: HTTP 403 — Token ohne Berechtigung")
                _cache[key] = (now, [])
                return None
            if resp.status_code != 200:
                logger.info(
                    f"cites: HTTP {resp.status_code} für {scientific_name}"
                )
                _cache[key] = (now, [])
                return None
            payload = resp.json()
    except Exception as e:
        logger.info(f"cites: fetch failed für {scientific_name}: {e}")
        return None

    if not isinstance(payload, dict):
        _cache[key] = (now, [])
        return None
    concepts = payload.get("taxon_concepts")
    if not isinstance(concepts, list) or not concepts:
        _cache[key] = (now, [])
        return None

    record = concepts[0] if isinstance(concepts[0], dict) else None
    _cache[key] = (now, [record] if record else [])
    return record


# ---------------------------------------------------------------------------
# Result-Builder
# ---------------------------------------------------------------------------
def _build_result_from_fallback(sp: dict) -> dict:
    """Konvertiere ein Fallback-Spezies-Dict in das Evidora-Result-Schema."""
    app = sp.get("app", "NC")
    app_label = _APPENDIX_LABEL_DE.get(app, f"Anhang {app}")
    de_name = sp.get("de", "—")
    en_name = sp.get("en", "")
    sci = sp.get("sci", "")
    since = sp.get("since", "")
    trade_note = sp.get("trade_note", "")

    display_value = (
        f"{de_name} ({sci}) — CITES-Anhang {app}. {app_label}."
    )
    if since:
        display_value += f" Gelistet seit {since}."

    description_parts = [
        f"Englischer Name: {en_name}." if en_name else "",
        f"Handelshinweis: {trade_note}" if trade_note else "",
        (
            "Quelle: CITES Appendices (Convention on International Trade "
            "in Endangered Species, 184 Vertragsstaaten). CITES regelt "
            "den internationalen Handel mit gefährdeten Arten — "
            "komplementär zur IUCN Red List (wissenschaftliche "
            "Gefährdungsbewertung)."
        ),
    ]

    portal_slug = sci.replace(" ", "+") if sci else quote(en_name)
    url = f"{CITES_PORTAL}/{portal_slug}" if portal_slug else CITES_PORTAL

    indicator_name = (
        f"CITES · {de_name} ({sci}) · Anhang {app}"
    )[:300]

    return {
        "indicator_name": indicator_name,
        "indicator": f"cites_{sci.lower().replace(' ', '_').replace('/', '_')}",
        "country": "GLOBAL",
        "country_name": "Welt",
        "year": since or "2025",
        "value": f"Anhang {app}",
        "display_value": display_value,
        "description": " ".join(p for p in description_parts if p),
        "url": url,
        "source": "CITES Species+",
    }


def _build_result_from_live(record: dict, fallback_sp: dict | None) -> dict:
    """Konvertiere einen Live-API-Record ins Evidora-Schema.

    Falls fallback_sp gesetzt, wird DE-Name / trade_note von dort genommen.
    """
    sci = record.get("full_name") or (fallback_sp or {}).get("sci", "")
    listing = record.get("cites_listing") or ""
    # Live-API liefert z.B. "I", "II", "I/II", "III"; gelegentlich leer
    app = listing if listing else (fallback_sp or {}).get("app", "NC")
    app_label = _APPENDIX_LABEL_DE.get(app, f"Anhang {app}")

    author_year = record.get("author_year") or ""
    common_names = record.get("common_names") or []
    en_name = ""
    if isinstance(common_names, list):
        for cn in common_names:
            if isinstance(cn, dict) and cn.get("language") == "EN":
                en_name = cn.get("name") or ""
                break

    de_name = (fallback_sp or {}).get("de") or en_name or sci
    trade_note = (fallback_sp or {}).get("trade_note", "")

    display_value = f"{de_name} ({sci}) — CITES-Anhang {app}. {app_label}."
    if author_year:
        display_value += f" Taxon-Referenz: {author_year}."

    description_parts = [
        f"Englischer Name: {en_name}." if en_name else "",
        f"Handelshinweis: {trade_note}" if trade_note else "",
        (
            "Quelle: CITES Species+ Live-API (UNEP-WCMC / "
            "CITES-Sekretariat). Datenstand entspricht aktuellem "
            "CITES-Appendix-Stand."
        ),
    ]

    portal_slug = sci.replace(" ", "+") if sci else ""
    url = f"{CITES_PORTAL}/{portal_slug}" if portal_slug else CITES_PORTAL

    indicator_name = (
        f"CITES · {de_name} ({sci}) · Anhang {app}"
    )[:300]

    return {
        "indicator_name": indicator_name,
        "indicator": f"cites_{sci.lower().replace(' ', '_').replace('/', '_')}",
        "country": "GLOBAL",
        "country_name": "Welt",
        "year": "2025",
        "value": f"Anhang {app}",
        "display_value": display_value,
        "description": " ".join(p for p in description_parts if p),
        "url": url,
        "source": "CITES Species+",
    }


# ---------------------------------------------------------------------------
# Public search
# ---------------------------------------------------------------------------
async def search_cites(analysis: dict) -> dict:
    """Lookup gegen CITES Species+ für Handelsregulierungs-Status.

    Strategie:
      1. Trigger-Match → frühes Empty-Return.
      2. Spezies aus Claim auflösen (Fallback-Index).
      3. Pro Spezies: Live-API versuchen (nur wenn Token gesetzt);
         sonst Fallback-Record.
      4. Top MAX_RESULTS Treffer ins Evidora-Format.

    Returns Dict mit ≤MAX_RESULTS Treffern oder Empty.
    """
    empty = {
        "source": "CITES Species+",
        "type": "wildlife_trade",
        "results": [],
    }

    claim = (analysis or {}).get("claim", "") or ""
    original = (analysis or {}).get("original_claim") or claim
    matchable = f"{original} {claim}".lower()

    if not _claim_mentions_cites(matchable):
        return empty

    species_list = _resolve_species_from_claim(matchable)
    if not species_list:
        logger.info("cites: Trigger matched, aber keine Spezies aufgelöst")
        return empty

    results: list[dict] = []
    token_present = _get_token() is not None

    for sp in species_list:
        sci = sp.get("sci", "")
        record = None
        if token_present and sci and "spp." not in sci.lower():
            # Live-API nicht zuverlässig für Gattungs-Wildcards
            try:
                record = await _fetch_cites_taxon(sci)
            except Exception as e:
                logger.debug(f"cites: Live-Call exception für {sci}: {e}")
                record = None

        if record:
            results.append(_build_result_from_live(record, sp))
        else:
            results.append(_build_result_from_fallback(sp))

        if len(results) >= MAX_RESULTS:
            break

    if not results:
        return empty

    logger.info(
        f"cites: {len(results)} Spezies aufgelöst "
        f"(live={'yes' if token_present else 'no'})"
    )
    return {
        "source": "CITES Species+",
        "type": "wildlife_trade",
        "results": results,
    }
