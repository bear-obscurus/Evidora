"""Ein Länder-Alias-Verzeichnis für alle Konnektoren, mit sicherem Matching.

Anlass, 2026-09-07: „Wie ist die Pressefreiheit in Suedafrika laut RSF?" kam
als `unverifiable@0.1` zurück. RSF **wurde abgefragt** und lieferte nichts —
nicht wegen der Schreibweise, sondern weil `südafrika` in `rsf.COUNTRY_MAP`
schlicht fehlte, obwohl `ZAF` im Datensatz steht. Gemessen:

    rsf: Karte 44 Länder, Datensatz 180  ->  136 unerreichbar

Das Muster ist älter als RSF. Im Projekt liegen **24 getrennte Länderkarten**,
jede von Hand für ihren Konnektor gepflegt, jede mit einem anderen Ausschnitt
— „eine Zahl in zwei Kopien driftet", nur mit 24 Kopien. Die Vereinigung der
zehn echten Namenskarten deckt 103 Länder ab; keine einzelne tut das.

DAS SCHWIERIGE IST NICHT DIE TABELLE, SONDERN DIE VERGLEICHSART
---------------------------------------------------------------
Von 44 auf 180 Länder heisst: Kollisionen. Die bisherige Suche prüft
`name in claim` in Einfüge-Reihenfolge, und damit gilt

    "mali"     steckt in  "so-MALI-a"
    "niger"    steckt in  "NIGER-ia"
    "oman"     steckt in  "r-OMAN-ia"
    "russland" steckt in  "weiss-RUSSLAND"
    "guinea"   steckt in  "papua-neu-GUINEA", "GUINEA-bissau", "äquatorial-GUINEA"

Drei Regeln zusammen lösen das, einzeln keine:

  1. **Wortgrenze auf beiden Seiten, mit einer Regel für die Flexion.**
     Nur den Wortanfang zu prüfen reicht nicht: „benin" träfe dann den
     „Beninger Weg", „guinea" das englische „guineafowl" — beides gemessen,
     nicht ausgedacht. Nur den Anfang UND das Ende zu prüfen reicht auch
     nicht: „Österreichs Pressefreiheit" verlöre den Genitiv, „russische
     Föderation" das Adjektiv.

     Erlaubt ist deshalb hinter dem Alias genau zweierlei, und beides ist
     eine grammatische Regel, keine Wortliste (Lehre aus #141):
     ein Genitiv-s, oder — wenn der Alias selbst schon auf „-isch" endet —
     bis zu drei weitere Buchstaben für die Adjektiv-Endung
     („russisch" → „russische", „russischen"). Danach muss ein Nicht-
     Buchstabe stehen.
  2. **Längste zuerst.** „weissrussland" wird vor „russland" geprüft.
  3. **Fundstelle verbrauchen.** Ohne das liefert „Nigeria" *beide* Codes:
     „nigeria" trifft, und „niger" trifft dieselbe Stelle gleich nochmal.
     Erst das Ausixen der Fundstelle macht die Zuordnung eindeutig.

Regel 3 ist die, die man vergisst. Ein Test hält alle drei fest.

Die Tabelle selbst ist die Vereinigung der zehn Namenskarten im Projekt plus
80 Länder, die dort nirgends vorkamen. Ausgeschlossen blieben Karten, die gar
keine Landesnamen abbilden: `parlgov.PERSON_TO_COUNTRY` (Politiker),
`nominatim`/`geonames` (ISO2-Codes), `ecdc.COUNTRY_CODES`. Eine erste,
unsaubere Vereinigung über *alle* 24 Karten hatte 853 Substring-Kollisionen —
fast alle davon aus solchen fremden Namensräumen.
"""

from __future__ import annotations

import re
from functools import lru_cache

from services._schreibweise import normalisiere

# ISO3 -> Aliasse (deutsch + englisch, klein, ungefaltet lesbar).
# Verglichen wird gegen den normalisierten Claim; `finde` faltet die Aliasse.
ALIASSE: dict[str, tuple[str, ...]] = {
    "AFG": ("afghan", "afghanisch", "afghanistan"),
    "AGO": ("angola", "angolanisch"),
    "ALB": ("albania", "albanian", "albanien", "albanisch"),
    "AND": ("andorra", "andorranisch"),
    "ARE": ("emirate", "united arab emirates", "vereinigte arabische emirate"),
    "ARG": ("argentina", "argentine", "argentinian", "argentinien", "argentinisch"),
    "ARM": ("armenia", "armenian", "armenien", "armenisch"),
    "AUS": ("australia", "australian", "australien", "australisch"),
    "AUT": ("austria", "austrian", "oesterreich", "oesterreichisch", "österreich", "österreichisch"),
    "AZE": ("aserbaidschan", "aserbaidschanisch", "azerbaijan"),
    "BDI": ("burundi", "burundisch"),
    "BEL": ("belgian", "belgien", "belgisch", "belgium"),
    "BEN": ("benin", "beninisch"),
    "BFA": ("burkina faso", "burkinisch"),
    "BGD": ("bangladesch", "bangladeschisch", "bangladesh", "bangladeshi"),
    "BGR": ("bulgaria", "bulgarian", "bulgarien", "bulgarisch"),
    "BHR": ("bahrain", "bahrainisch"),
    "BIH": ("bosnia", "bosnia and herzegovina", "bosnien", "bosnisch"),
    "BLR": ("belarus", "belarusian", "belorussian", "belorussisch", "weissrussisch", "weissrussland", "weißrussisch", "weißrussland"),
    "BLZ": ("belize", "belizisch"),
    "BOL": ("bolivia", "bolivianisch", "bolivien"),
    "BRA": ("brasilianisch", "brasilien", "brazil", "brazilian"),
    "BRN": ("brunei", "bruneiisch"),
    "BTN": ("bhutan", "bhutanisch"),
    "BWA": ("botsuana", "botswana", "botswanian"),
    "CAF": ("central african republic", "zentralafrika", "zentralafrikanische republik"),
    "CAN": ("canada", "canadian", "kanada", "kanadisch"),
    "CHE": ("eidgenossenschaft", "helvetia", "helvetisch", "schweiz", "schweizerisch", "swiss", "switzerland"),
    "CHL": ("chile", "chilean", "chilenisch"),
    "CHN": ("china", "chinese", "chinesisch", "volksrepublik china", "vr china"),
    # Der Apostroph ist kein Trennzeichen fuer `normalisiere` — „Cote d Ivoire"
    # ohne ihn ist aber die haeufigste Tippweise, deshalb beide Formen.
    "CIV": ("cote d'ivoire", "cote d ivoire", "côte d'ivoire",
            "elfenbeinküste", "ivorisch"),
    "CMR": ("cameroon", "kamerun", "kamerunisch"),
    "COD": ("demokratische republik kongo", "dr congo", "dr kongo", "kongo kinshasa", "kongo-kinshasa"),
    "COG": ("congo-brazzaville", "kongo brazzaville", "kongo-brazzaville", "republik kongo"),
    "COL": ("colombia", "colombian", "kolumbianisch", "kolumbien"),
    "COM": ("comoros", "komoren", "komorisch"),
    "CPV": ("cabo verde", "cape verde", "kap verde", "kapverden"),
    "CRI": ("costa rica", "costa-rica", "costaricanisch"),
    "CSS": ("oecs", "organisation of eastern caribbean states", "ostkaribische staaten"),
    "CTU": ("nordzypern", "northern cyprus"),
    "CUB": ("cuba", "cuban", "kuba", "kubanisch"),
    "CYP": ("cypriot", "cyprus", "zypern", "zypriotisch"),
    "CZE": ("czech", "czech republic", "czechia", "tschechien", "tschechisch"),
    "DEU": ("bundesrepublik", "deutsch", "deutschland", "german", "germany"),
    "DJI": ("djibouti", "dschibuti"),
    "DNK": ("daenemark", "daenisch", "danish", "denmark", "dänemark", "dänisch"),
    "DOM": ("dominican republic", "dominikanische republik"),
    "DZA": ("algeria", "algerien", "algerisch"),
    "ECU": ("ecuador", "ecuadorianisch", "ekuador"),
    "EGY": ("aegypten", "aegyptisch", "egypt", "egyptian", "ägypten", "ägyptisch"),
    "ERI": ("eritrea", "eritrean", "eritreisch"),
    "ESP": ("spain", "spanien", "spanisch", "spanish"),
    "EST": ("estland", "estnisch", "estonia", "estonian"),
    "ETH": ("aethiopien", "ethiopia", "ethiopian", "äthiopien"),
    "EUR": ("europa", "europe", "european union", "europäische union"),
    "FIN": ("finland", "finnisch", "finnish", "finnland"),
    "FJI": ("fidschi", "fiji"),
    "FRA": ("france", "frankreich", "franzoesisch", "französisch", "french"),
    "GAB": ("gabon", "gabun", "gabunisch"),
    "GBR": ("britain", "britisch", "british", "england", "grossbritannien", "großbritannien", "united kingdom", "vereinigtes koenigreich", "vereinigtes königreich"),
    "GEO": ("georgia", "georgian", "georgien", "georgisch"),
    "GHA": ("ghana", "ghanaian", "ghanaisch"),
    "GIN": ("guinea", "guineisch"),
    "GMB": ("gambia", "gambisch"),
    "GNB": ("guinea-bissau",),
    "GNQ": ("aequatorialguinea", "equatorial guinea", "äquatorialguinea"),
    "GRC": ("greece", "greek", "griechenland", "griechisch"),
    "GTM": ("guatemala", "guatemaltekisch"),
    "GUY": ("guyana", "guyanisch"),
    "HKG": ("hong kong", "hongkong"),
    "HND": ("honduranisch", "honduras"),
    "HRV": ("croatia", "croatian", "kroatien", "kroatisch"),
    "HTI": ("haiti", "haitianisch"),
    "HUN": ("hungarian", "hungary", "magyar", "ungarisch", "ungarn"),
    "IDN": ("indonesia", "indonesian", "indonesien", "indonesisch"),
    "IND": ("india", "indian", "indien", "indisch"),
    "IRL": ("ireland", "irisch", "irish", "irland"),
    "IRN": ("iran", "iranian", "iranisch", "persien"),
    "IRQ": ("irak", "iraq"),
    "ISL": ("iceland", "icelandic", "island", "isländisch"),
    "ISR": ("israel", "israeli", "israelisch"),
    "ITA": ("italian", "italien", "italienisch", "italy"),
    "JAM": ("jamaica", "jamaican", "jamaika"),
    "JOR": ("jordan", "jordanien", "jordanisch"),
    "JPN": ("japan", "japanese", "japanisch"),
    "KAZ": ("kasachisch", "kasachstan", "kazakhstan"),
    "KEN": ("kenia", "kenianisch", "kenya"),
    "KGZ": ("kirgisisch", "kirgisistan", "kirgistan", "kyrgyzstan"),
    "KHM": ("cambodia", "kambodscha", "kambodschanisch"),
    "KOR": ("korea", "republic of korea", "republik korea", "south korea", "suedkorea", "suedkoreanisch", "südkorea", "südkoreanisch"),
    "KWT": ("kuwait", "kuwaitisch"),
    "LAO": ("laos", "laotisch"),
    "LBN": ("lebanon", "libanesisch", "libanon"),
    "LBR": ("liberia", "liberianisch"),
    "LBY": ("libya", "libyen", "libysch"),
    "LIE": ("liechtenstein", "liechtensteinisch"),
    "LKA": ("sri lanka", "srilankisch"),
    "LSO": ("lesothisch", "lesotho"),
    "LTU": ("litauen", "litauisch", "lithuania", "lithuanian"),
    "LUX": ("luxembourg", "luxemburg", "luxemburgisch"),
    "LVA": ("latvia", "latvian", "lettisch", "lettland"),
    "MAR": ("marokkanisch", "marokko", "moroccan", "morocco"),
    "MDA": ("moldau", "moldawien", "moldova", "moldovan"),
    "MDG": ("madagascar", "madagaskar", "madagassisch"),
    "MDV": ("maldives", "malediven"),
    "MEX": ("mexican", "mexico", "mexikanisch", "mexiko"),
    "MKD": ("macedonia", "mazedonien", "nordmazedonien", "north macedonia"),
    "MLI": ("mali", "malisch"),
    "MLT": ("malta", "maltese", "maltesisch"),
    "MMR": ("birma", "burma", "myanmar", "myanmarisch"),
    "MNE": ("montenegrin", "montenegrinisch", "montenegro"),
    "MNG": ("mongolei", "mongolia", "mongolisch"),
    "MOZ": ("mosambik", "mosambikanisch", "mozambique"),
    "MRT": ("mauretanien", "mauretanisch", "mauritania"),
    "MUS": ("mauritisch", "mauritius"),
    "MWI": ("malawi", "malawisch"),
    "MYS": ("malaysia", "malaysisch"),
    "NAM": ("namibia", "namibisch"),
    "NER": ("niger", "nigrisch"),
    "NGA": ("nigeria", "nigerian", "nigerianisch"),
    "NIC": ("nicaragua", "nicaraguan", "nicaraguanisch"),
    "NLD": ("dutch", "holland", "holländisch", "netherlands", "niederlaendisch", "niederlande", "niederländisch"),
    "NOR": ("norway", "norwegen", "norwegian", "norwegisch"),
    "NPL": ("nepal", "nepalesisch"),
    "NZL": ("neuseeland", "neuseeländisch", "new zealand"),
    "OMN": ("oman", "omanisch"),
    "PAK": ("pakistan", "pakistani", "pakistanisch"),
    "PAN": ("panama", "panamaisch"),
    "PER": ("peru", "peruanisch", "peruvian"),
    "PHL": ("filipino", "philippinen", "philippines", "philippinisch"),
    "PNG": ("papua neuguinea", "papua new guinea", "papua-neuguinea"),
    "POL": ("poland", "polen", "polish", "polnisch"),
    "PRK": ("demokratische volksrepublik korea", "dpr korea", "dprk", "dvr korea", "nordkorea", "north korea"),
    "PRT": ("portugal", "portugiesisch", "portuguese"),
    "PRY": ("paraguay", "paraguayisch"),
    "PSE": ("palaestina", "palestine", "palästina", "palästinensisch"),
    "QAT": ("katar", "katarisch", "qatar"),
    "RKS": ("kosovan", "kosovarisch", "kosovo"),
    "ROU": ("romania", "romanian", "rumaenien", "rumaenisch", "rumänien", "rumänisch"),
    "RUS": ("russia", "russian", "russisch", "russische föderation", "russland", "rußland"),
    "RWA": ("ruanda", "ruandisch", "rwanda"),
    "SAU": ("saudi", "saudi arabia", "saudi arabien", "saudi-arabien", "saudi-arabisch"),
    "SDN": ("sudan", "sudanese", "sudanesisch"),
    "SEN": ("senegal", "senegalesisch"),
    "SGP": ("singapore", "singapur"),
    "SLE": ("sierra leone", "sierra-leonisch"),
    "SLV": ("el salvador", "salvadorianisch"),
    "SOM": ("somali", "somalia", "somalisch"),
    "SRB": ("serbia", "serbian", "serbien", "serbisch"),
    "SSD": ("south sudan", "suedsudan", "südsudan"),
    "SUR": ("surinam", "suriname"),
    "SVK": ("slovak", "slovakia", "slowakei", "slowakisch"),
    "SVN": ("slovenia", "slovenian", "slowenien", "slowenisch"),
    "SWE": ("schweden", "schwedisch", "sweden", "swedish"),
    "SWZ": ("eswatini", "swasiland", "swaziland"),
    "SYC": ("seychellen", "seychelles"),
    "SYR": ("syria", "syrian", "syrien", "syrisch"),
    "TCD": ("chad", "tschad", "tschadisch"),
    "TGO": ("togo", "togoisch"),
    "THA": ("thai", "thailaendisch", "thailand", "thailändisch"),
    "TJK": ("tadschikisch", "tadschikistan", "tajik", "tajikistan"),
    "TKM": ("turkmen", "turkmenisch", "turkmenistan", "turkmenistanisch"),
    "TLS": ("east timor", "osttimor", "timor-leste"),
    "TON": ("tonga", "tongaisch"),
    "TTO": ("trinidad", "trinidad and tobago", "trinidad und tobago"),
    "TUN": ("tunesien", "tunesisch", "tunisia", "tunisian"),
    "TUR": ("tuerkei", "tuerkisch", "turkey", "turkish", "türkei", "türkisch", "türkiye"),
    "TWN": ("republic of china", "republik china", "taiwan"),
    "TZA": ("tansania", "tansanisch", "tanzania"),
    "UGA": ("uganda", "ugandisch"),
    "UKR": ("ukraine", "ukrainian", "ukrainisch"),
    "URY": ("uruguay", "uruguayan", "uruguayisch"),
    "USA": ("america", "american", "amerika", "amerikanisch", "u.s.", "u.s.a.", "united states", "us-amerikanisch", "vereinigte staaten"),
    "UZB": ("usbekisch", "usbekistan", "uzbekistan"),
    "VEN": ("venezolanisch", "venezuela", "venezuelan"),
    "VNM": ("vietnam", "vietnamese", "vietnamesisch"),
    "WLD": ("global", "welt", "weltweit", "world"),
    "WSM": ("samoa", "samoanisch"),
    "XKX": ("kosovarisch", "kosovo"),
    "YEM": ("jemen", "jemenitisch", "yemen", "yemeni"),
    "ZAF": ("south africa", "south african", "south-african", "suedafrika", "suedafrikanisch", "südafrika", "südafrikanisch"),
    "ZMB": ("sambia", "sambisch", "zambia"),
    "ZWE": ("simbabwe", "simbabwisch", "zimbabwe"),}


@lru_cache(maxsize=4096)
def _muster(alias: str) -> re.Pattern[str]:
    """Wortgrenze vorn, Flexions-Spielraum je Wort — siehe Regel 1.

    Der Spielraum muss JEDES Wort betreffen, nicht nur das letzte: im
    Deutschen flektiert bei mehrteiligen Namen das vordere Adjektiv — „der
    europaeischEN Union", „der russischEN Foederation", „die vereinigtEN
    Staaten". Ein Muster, das nur hinten Luft laesst, verfehlt genau die
    Form, in der solche Namen im Satz vorkommen.

    Luft bekommt nur, was auf „-e" oder „-isch" endet — die attributive
    Grundform eines Adjektivs. Ein Wort auf „-er" bekommt sie NICHT, sonst
    traefe „niger" wieder „nigeria".
    """
    return re.compile(r"(?<![a-z0-9])"
                      + r"\s+".join(_wortmuster(w) for w in alias.split(" "))
                      + r"(?![a-z])")


# Attributive Adjektiv-Endungen. Abgeschnitten wird nur, wenn ein Stamm von
# mindestens fuenf Zeichen uebrig bleibt — sonst wuerde aus „niger" der Stamm
# „nig", und „nigeria" traefe wieder mit.
_ADJEKTIV_ENDUNGEN = ("es", "en", "er", "em", "e")
_MIN_STAMM = 5


def _wortmuster(wort: str) -> str:
    if wort.endswith("isch"):          # „russisch" ist selbst schon der Stamm
        return re.escape(wort) + r"[a-z]{0,3}"
    for endung in _ADJEKTIV_ENDUNGEN:
        if wort.endswith(endung) and len(wort) - len(endung) >= _MIN_STAMM:
            return re.escape(wort[:-len(endung)]) + r"[a-z]{0,3}"
    return re.escape(wort) + r"s?"     # blosser Genitiv


# Dasselbe Land unter zwei Codes. Das ist kein Versehen: die Datensaetze im
# Projekt verwenden unterschiedliche Kuerzel, und ein Konnektor muss den
# treffen, den SEINE Daten fuehren — deshalb bleiben beide stehen und
# ``erlaubt`` entscheidet. Nur wenn niemand einschraenkt, braucht es eine
# feste Rangfolge, sonst haengt das Ergebnis an der Dict-Reihenfolge.
# Vorne steht der ISO-3166-Code (XKX), hinten das Landeskuerzel (RKS, aus
# `easie.py`).
_CODE_RANG: dict[str, int] = {"XKX": 0, "RKS": 1}


@lru_cache(maxsize=1)
def _suchreihenfolge() -> tuple[tuple[str, str], ...]:
    """(normalisierter Alias, ISO3), längste zuerst — siehe Regel 2."""
    paare = [(normalisiere(a), iso) for iso, al in ALIASSE.items() for a in al]
    paare.sort(key=lambda p: (-len(p[0]), _CODE_RANG.get(p[1], 0), p[1]))
    return tuple(paare)


def finde(text: str, erlaubt: frozenset[str] | None = None,
          max_n: int = 3) -> list[str]:
    """ISO3-Codes der im Text genannten Länder, in Fundreihenfolge.

    ``erlaubt`` schränkt auf die Codes ein, die der aufrufende Datensatz
    überhaupt kennt — sonst meldet ein Konnektor ein Land, zu dem er nichts
    liefern kann, und der Nutzer bekommt „keine Daten" statt „nicht zuständig".
    """
    if not text:
        return []
    heu = normalisiere(text)
    # Erst alle Treffer sammeln, dann nach Position im Text sortieren. Die
    # Suchreihenfolge ist nach Alias-LAENGE sortiert (Regel 2) — ohne die
    # Umsortierung liefert „Niger und Nigeria" die Codes verkehrt herum, und
    # `max_n` schnitte das zuerst genannte Land weg.
    treffer: list[tuple[int, str]] = []
    gesehen: set[str] = set()
    for alias, iso in _suchreihenfolge():
        if iso in gesehen or (erlaubt is not None and iso not in erlaubt):
            continue
        fund = _muster(alias).search(heu)
        if not fund:
            continue
        gesehen.add(iso)
        treffer.append((fund.start(), iso))
        # Regel 3: Fundstelle verbrauchen, damit ein kuerzerer Alias nicht
        # dieselbe Stelle noch einmal trifft ("niger" in "nigeria").
        heu = (heu[:fund.start()] + " " * (fund.end() - fund.start())
               + heu[fund.end():])
    treffer.sort()
    return [iso for _, iso in treffer[:max_n]]


def aus_analyse(analysis: dict, erlaubt: frozenset[str] | None = None,
                max_n: int = 3) -> list[str]:
    """Bequemer Einstieg für Konnektoren: NER-Länder zuerst, dann der Claim."""
    ner = (analysis.get("ner_entities") or {}).get("countries") or []
    teile = [str(c) for c in ner] + [
        analysis.get("claim") or "", analysis.get("original_claim") or ""]
    gefunden: list[str] = []
    for t in teile:
        for iso in finde(t, erlaubt, max_n):
            if iso not in gefunden:
                gefunden.append(iso)
                if len(gefunden) >= max_n:
                    return gefunden
    return gefunden
