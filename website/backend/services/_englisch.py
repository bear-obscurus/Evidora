"""Englische Claims auf die deutschen Pack-Trigger abbilden.

Gemessen am 26.9.2026, live gegen Produktion:

    "Is it true that the vacancy tax in Vorarlberg is 3000 euros a year?"
    -> unverifiable@0.1 nach 7 s

Der deutsche Claim zur selben Sache liefert true@0.9. Es fehlt nicht der
Fakt, sondern das Retrieval: die Trigger in ``data/*.json`` sind deutsch.
Auf 624 englischen Claims (je Fakt einer, ``tools/englisch_korpus.json``)
trafen im Status quo 114 ihren Fakt, 500 verfehlten ihn, obwohl das deutsche
Quell-Phrasing traf.

WARUM EIN GLOSSAR UND KEIN COSINUS
==================================
Der Cosine-Backup ist in 49 von 63 Trigger-Services seit #41 abgeschaltet,
weil er bei Multi-Topic-Packs themenfremde Claims zog. Ihn fuer englische
Claims wieder einzuschalten, haette ein Sprach-Gate gebraucht — und das
vorhandene ``services.ner._detect_language`` haelt **82,9 %** der 2.603
dokumentierten deutschen Phrasings fuer Englisch (es zaehlt deutsche
Funktionswoerter; kurze deutsche Claims haben keine). Die Cosine-Messung
selbst steht im PR und in ``tools/englisch_cosine_messung.py``.

Dieses Modul bleibt literal wie ``services/_tippfehler.py``: Es uebersetzt
nicht, es **glossiert**. Steht im Claim ein englischer Begriff aus dem
Glossar, wird der deutsche Trigger-Begriff daneben geschrieben, und die
unveraenderte Trigger-Logik laeuft auf diesem Text. Composite-Regeln bleiben
Composite-Regeln — ein Fakt, der Thema UND Bundesland verlangt, verlangt
beides auch auf Englisch.

DREI SICHERUNGEN
================
  * **Sprach-Gate mit positiver Evidenz.** Der Pass laeuft nur, wenn der
    Claim mindestens ein englisches Funktionswort traegt und mehr englische
    als deutsche. Deutsche Homographen („was", „will", „die", „an", „in",
    „war") zaehlen nicht; „who" auch nicht — in deutschen Claims ist das die
    WHO. Gemessen: 1 falscher Alarm auf 3.766 deutschen Claims.
  * **Nur wenn exakt und tolerant leer blieben.** Ein Fakt, der exakt ankert,
    gewinnt immer; der Treffer wird ``_matched_exact=False`` markiert und darf
    kein „strukturell falsch" behaupten.
  * **Politik-Guard auf dem glossierten Text.** Ohne das waere ein englischer
    Partei-Korruptions-Superlativ ein Loch: Der Guard kennt nur deutsche
    Tokens. Deshalb glossiert das Glossar auch „corrupt" -> „korrupt" und
    ``find_matching_items`` prueft den Guard auf dem glossierten Text.

Englische Funktionswoerter werden aus dem Rest-Claim entfernt, bevor die
Trigger laufen: Ein deutsches Composite mit „ at " (Oesterreich-Kuerzel)
darf nicht von der englischen Praeposition „at" erfuellt werden.

DAS GLOSSAR
===========
Je Eintrag ein deutscher Begriff in der Form, in der er in den Triggern
steht, und seine englischen Varianten. Vergleich auf Wortgrenzen; das LETZTE
Wort einer Variante darf die englische Plural-/Genitiv-Endung tragen
(-s, -es, -'s, -ies). Eine Variante mit ``*`` am Ende ist ein Wortanfang
(„vaccin*" trifft vaccine, vaccines, vaccination) — nur fuer Einwort-
Varianten und mit mindestens fuenf Zeichen.

Kuratierungs-Regeln:
  * Keine falschen Freunde als englische Variante: „gift" (dt. Gift),
    „handy", „also", „fast", „kind", „art", „rat", „bad", „brief" fehlen
    bewusst.
  * Keine eigene Einordnung: Das Glossar uebersetzt den Begriff des Nutzers,
    es klassifiziert nichts. Politische Begriffe stehen nur, wo der Claim sie
    selbst traegt.
  * Nie „true"/„really": „Is it true that …" steht vor fast jedem
    Frage-Claim und darf nichts glossieren.
"""

from __future__ import annotations

import re
from functools import lru_cache

from services._schreibweise import normalisiere

# ---------------------------------------------------------------------------
# Sprachsignal
# ---------------------------------------------------------------------------

# Nur Woerter, die im Deutschen KEIN eigenes Wort sind. Bewusst draussen:
# in, an, am, was, will, so, war, die, man, hat, also, bald, who (WHO).
_EN_FUNKTIONSWOERTER = frozenset("""
    the is are were of and to that it its for on with by from have has had
    does do did than this these those they their there which what how why
    when be been being can could would should must more most not isn't
    aren't doesn't don't didn't wasn't won't can't no any all every much
    many
""".split())

_DE_FUNKTIONSWOERTER = frozenset("""
    der die das des den dem ein eine einen einem einer ist sind und nicht
    mit auf fuer von zu zum zur es sich dass wird werden hat haben kein keine
    keinen im bei nach aus als auch wie wer wenn oder aber noch nur sehr
    schon ueber unter vom gibt man wurde wurden waren mehr weniger jeder jede
    alle immer
""".split())

# Zusaetzlich aus dem Rest-Claim entfernt (nicht fuers Sprachsignal): kurze
# englische Woerter, die deutsche Trigger-Tokens erfuellen koennten.
_EN_ENTFERNEN = _EN_FUNKTIONSWOERTER | frozenset(
    "a an at in as or if per so up out off than then was will".split())

_WORT_RE = re.compile(r"[a-z0-9']+")


def _woerter(text: str) -> list[str]:
    t = normalisiere(text.replace("’", "'").replace("‘", "'"))
    return _WORT_RE.findall(t)


def sprachsignal(claim: str) -> tuple[int, int]:
    """(englische, deutsche) Funktionswoerter im Claim."""
    w = _woerter(claim)
    return (sum(x in _EN_FUNKTIONSWOERTER for x in w),
            sum(x in _DE_FUNKTIONSWOERTER for x in w))


def ist_englisch(claim: str) -> bool:
    """Positive englische Evidenz: mindestens ein englisches Funktionswort
    und mehr englische als deutsche."""
    en, de = sprachsignal(claim)
    return en >= 1 and en > de


# ---------------------------------------------------------------------------
# Glossar: (deutscher Trigger-Begriff, (englische Varianten, ...))
# ---------------------------------------------------------------------------

GLOSSAR: tuple[tuple[str, tuple[str, ...]], ...] = (
    # --- Regionen -----------------------------------------------------------
    ("österreich", ("austria",)),
    ("österreichisch", ("austrian",)),
    ("deutschland", ("germany",)),
    ("deutsche", ("german",)),
    ("wien", ("vienna", "viennese")),
    ("europa", ("europe",)),
    ("europäisch", ("european",)),
    ("europäische union", ("european union",)),
    ("schweiz", ("switzerland", "swiss")),
    ("tirol", ("tyrol", "tyrolean")),
    ("kärnten", ("carinthia",)),
    ("steiermark", ("styria",)),
    ("niederösterreich", ("lower austria",)),
    ("oberösterreich", ("upper austria",)),
    ("bundesland", ("federal state", "province")),
    ("bundesländer", ("federal states", "provinces")),
    ("russland", ("russia", "russian")),
    ("ungarn", ("hungary", "hungarian")),
    ("schweden", ("sweden", "swedish")),
    ("finnland", ("finland", "finnish")),
    ("frankreich", ("france", "french")),
    ("italien", ("italy", "italian")),
    ("polen", ("poland", "polish")),
    ("niederlande", ("netherlands", "dutch")),
    ("großbritannien", ("britain", "united kingdom", "british")),
    ("skandinavien", ("scandinavia", "scandinavian")),
    ("türkei", ("turkey",)),
    ("usa", ("united states", "america", "american", "the us")),
    ("brüssel", ("brussels",)),
    ("straßburg", ("strasbourg",)),
    ("indien", ("india", "indian")),
    ("afrika", ("africa", "african")),
    ("asien", ("asia", "asian")),
    ("weltweit", ("worldwide", "globally", "around the world",
                  "in the world", "of the world")),
    ("welt", ("world",)),

    # --- Personengruppen ----------------------------------------------------
    ("frauen", ("women",)),
    ("frau", ("woman",)),
    ("männer", ("men",)),
    ("mann", ("man",)),
    ("kinder", ("children", "kids")),
    ("kind", ("child",)),
    ("jugendliche", ("teenager", "adolescent", "young people", "youth")),
    ("schüler", ("pupil", "schoolchildren", "school children")),
    ("studierende", ("university students", "college students")),
    ("lehrer", ("teacher",)),
    ("eltern", ("parent",)),
    ("baby", ("infant", "newborn")),
    ("säugling", ("infant", "newborn")),
    ("migranten", ("migrant", "immigrant")),
    ("ausländer", ("foreigner", "foreign nationals", "non-citizens")),
    ("flüchtlinge", ("refugee",)),
    ("asylsuchende", ("asylum seeker",)),
    ("asyl", ("asylum",)),
    ("bürger", ("citizen",)),
    ("arbeitnehmer", ("employee", "worker")),
    ("pensionisten", ("pensioner", "retiree")),
    ("ärzte", ("doctor", "physician")),
    ("patienten", ("patient",)),
    ("bauern", ("farmer",)),
    ("muslime", ("muslim",)),
    ("christen", ("christian",)),
    ("juden", ("jew", "jewish")),
    ("politiker", ("politician",)),
    ("partei", ("party", "parties")),
    ("regierung", ("government",)),
    ("hund", ("dog",)),
    ("katze", ("cat",)),

    # --- Mengen und Fragen --------------------------------------------------
    ("wie viele", ("how many",)),
    ("wie viel", ("how much",)),
    ("wieviel", ("how much",)),
    ("wie hoch", ("how high",)),
    ("prozent", ("percent", "per cent")),
    ("millionen", ("million",)),
    ("milliarden", ("billion",)),
    ("hälfte", ("half",)),
    ("mehrheit", ("majority",)),
    ("anteil", ("share", "proportion")),
    ("quote", ("rate",)),
    ("anzahl", ("number of",)),
    ("verdoppelt", ("doubled",)),
    ("höher", ("higher",)),
    ("höchste", ("highest",)),
    ("größte", ("largest", "biggest")),
    ("kleinste", ("smallest",)),
    ("besser", ("better",)),
    ("schlechter", ("worse",)),
    ("täglich", ("daily", "every day", "a day", "per day")),
    ("jährlich", ("annually", "a year", "per year", "every year")),
    ("monatlich", ("monthly", "a month", "per month")),
    ("immer", ("always",)),
    ("nie", ("never",)),
    ("alle", ("everyone", "everybody")),
    ("kosten", ("cost",)),
    ("kostet", ("costs",)),
    ("preis", ("price",)),
    ("geld", ("money",)),
    ("steuer", ("tax",)),
    ("steuern", ("taxes",)),

    # --- Praedikate ---------------------------------------------------------
    ("gefährlich", ("dangerous", "hazardous")),
    ("schädlich", ("harmful",)),
    ("schadet", ("harms", "hurts", "damages", "is bad for", "are bad for")),
    ("sicher", ("safe", "secure")),
    ("harmlos", ("harmless",)),
    ("gesund", ("healthy",)),
    ("ungesund", ("unhealthy",)),
    ("giftig", ("poisonous", "toxic")),
    ("wirksam", ("effective",)),
    ("wirkt", ("works",)),
    ("wirkung", ("effect",)),
    ("wirkt nicht", ("doesn't work", "does not work", "don't work",
                     "do not work")),
    ("wirkungslos", ("ineffective",)),
    ("nutzlos", ("useless",)),
    ("sinnlos", ("pointless",)),
    ("bringt nichts", ("achieves nothing", "does nothing", "is pointless",
                       "is useless")),
    ("verursacht", ("causes", "caused")),
    ("heilt", ("cures", "cure", "heals")),
    ("verhindert", ("prevents", "prevent")),
    ("schützt", ("protects", "protect")),
    ("zerstört", ("destroys", "destroy", "destroyed")),
    ("ruiniert", ("ruins", "ruining", "ruined")),
    ("macht", ("makes",)),
    ("steigt", ("rising", "rises", "is increasing")),
    ("gestiegen", ("increased", "has risen", "have risen")),
    ("anstieg", ("increase",)),
    ("sinkt", ("falling", "falls", "declining")),
    ("rückgang", ("decline", "decrease")),
    ("explodiert", ("exploding", "explodes", "skyrocketing")),
    ("kollabiert", ("collapsing", "collapses", "collapse")),
    ("gescheitert", ("failed",)),
    ("verboten", ("banned", "prohibited", "forbidden")),
    ("erlaubt", ("allowed", "permitted")),
    ("abgeschafft", ("abolished",)),
    ("aufgehoben", ("overturned", "struck down", "repealed")),
    ("verfassungswidrig", ("unconstitutional",)),
    ("pflicht", ("mandatory", "compulsory", "obligatory")),
    ("erfunden", ("made up", "invented", "fabricated")),
    ("lüge", ("lie",)),
    ("mythos", ("myth",)),
    ("manipuliert", ("manipulated", "rigged")),
    ("übertrieben", ("exaggerated", "overblown")),
    ("faul", ("lazy",)),
    ("dumm", ("stupid", "dumb")),
    ("süchtig", ("addictive", "addicted")),
    ("sucht", ("addiction",)),
    ("abhängig", ("dependent",)),
    ("genug", ("enough",)),
    ("ausreichend", ("sufficient", "adequately", "adequate")),
    ("unnötig", ("unnecessary",)),
    ("gibt es nicht", ("doesn't exist", "does not exist")),

    # --- Allgemeine Sachbegriffe --------------------------------------------
    ("krank", ("sick", "ill")),
    ("krankheit", ("disease", "illness")),
    ("krebs", ("cancer",)),
    ("krebserregend", ("carcinogenic",)),
    ("tote", ("deaths", "dead")),
    ("sterben", ("die", "dying")),
    ("risiko", ("risk",)),
    ("studie", ("study", "studies")),
    ("wissenschaft", ("science",)),
    ("behandlung", ("treatment",)),
    ("therapie", ("therapy",)),
    ("empfohlen", ("recommended",)),
    ("empfehlung", ("recommendation",)),
    ("erstlinien", ("first-line", "first line")),
    ("leitlinie", ("guideline",)),
    ("impfung", ("vaccin*",)),
    ("medikament", ("medication", "medicine")),
    ("arbeitslosigkeit", ("unemployment", "joblessness")),
    ("arbeitslosenquote", ("unemployment rate", "jobless rate")),
    ("arbeitslose", ("unemployed",)),
    ("arbeitsplätze", ("jobs",)),
    ("lohn", ("wage", "salary", "salaries")),
    ("löhne", ("wages",)),
    ("wirtschaft", ("economy", "economic")),
    ("bip", ("gdp",)),
    ("pension", ("pension",)),
    ("rente", ("pension",)),
    ("miete", ("rent",)),
    ("mieten", ("rents",)),
    ("wohnung", ("apartment",)),
    ("wohnen", ("housing",)),
    ("wohnungsmangel", ("housing shortage",)),
    ("bildung", ("education",)),
    ("schule", ("school",)),
    ("universität", ("university", "universities")),
    ("lernen", ("learn", "learning")),
    ("gehirn", ("brain",)),
    ("schlaf", ("sleep",)),
    ("wasser", ("water",)),
    ("lebensmittel", ("food",)),
    ("ernährung", ("diet", "nutrition")),
    ("alkohol", ("alcohol",)),
    ("rauchen", ("smoking",)),
    ("strom", ("electricity",)),
    ("energie", ("energy",)),
    ("klima", ("climate",)),
    ("klimawandel", ("climate change",)),
    ("klimaschutz", ("climate protection", "climate action")),
    ("emissionen", ("emission",)),
    ("treibhausgas", ("greenhouse gas",)),
    ("verkehr", ("traffic", "transport")),
    ("auto", ("car",)),
    ("bahn", ("rail", "railway")),
    ("zug", ("train",)),
    ("krieg", ("war",)),
    ("demokratie", ("democracy", "democracies")),
    ("demokratisch", ("democratic",)),
    ("wahl", ("election", "vote", "voting")),
    ("wahlen", ("elections",)),
    ("parlament", ("parliament",)),
    ("gericht", ("court",)),
    ("urteil", ("ruling", "judgment", "judgement")),
    ("verfassungsgerichtshof", ("constitutional court",)),
    ("eugh", ("european court of justice", "ecj", "cjeu")),
    ("egmr", ("european court of human rights", "echr")),
    ("gesetz", ("law",)),
    ("kirche", ("church",)),
    ("sicherheit", ("security", "safety")),
    ("gewalt", ("violence",)),
    ("kriminalität", ("crime",)),
    ("kriminell", ("criminal",)),
    ("straftaten", ("offence", "offense")),
    ("diskriminierung", ("discrimination",)),
    ("datenschutz", ("data protection", "privacy")),
    ("soziale medien", ("social media",)),
    ("förderung", ("funding", "subsidy", "subsidies", "grant")),
    ("investitionen", ("investment",)),
    ("ausgaben", ("spending", "expenditure")),
    ("schulden", ("debt",)),
    ("haushalt", ("budget", "household")),
    ("zukunft", ("future",)),
    ("erfolg", ("success",)),
    ("erfolgreich", ("successful",)),

    # --- Politik-Guard (services/_topic_match.politik_guard_action) ---------
    # Der Guard kennt nur deutsche Tokens. Ohne diese Glossen waere ein
    # englischer Partei-Korruptions-Superlativ ein Loch.
    ("korruption", ("corruption",)),
    ("korrupt", ("corrupt",)),
    ("korrupteste", ("most corrupt",)),
    ("bestechung", ("bribery", "bribe")),
    ("geldwäsche", ("money laundering",)),
    ("skandal", ("scandal",)),
    ("schlimmste", ("worst",)),
    ("die meisten", ("the most",)),
    ("jede partei", ("every party",)),
    ("jeder politiker", ("every politician",)),
    ("grüne", ("greens", "green party")),
)


# ---------------------------------------------------------------------------
# Suche
# ---------------------------------------------------------------------------

def _grundformen(wort: str) -> set[str]:
    """Kandidaten, unter denen ein Claim-Wort im Index stehen kann: das Wort
    selbst und die Form ohne englische Plural-/Genitiv-Endung."""
    f = {wort}
    if wort.endswith("'s") or wort.endswith("s'"):
        f.add(wort[:-2])
    if wort.endswith("ies") and len(wort) > 4:
        f.add(wort[:-3] + "y")
    if wort.endswith("es") and len(wort) > 3:
        f.add(wort[:-2])
    if wort.endswith("s") and len(wort) > 2:
        f.add(wort[:-1])
    return f


def _endung_ok(wort: str, soll: str) -> bool:
    """``wort`` ist ``soll`` oder ``soll`` mit englischer Endung."""
    return wort == soll or soll in _grundformen(wort)


@lru_cache(maxsize=1)
def _index() -> tuple[dict[str, list[tuple[tuple[str, ...], str]]],
                      list[tuple[str, str]]]:
    """Erstes Wort der Variante -> [(Variante als Wortfolge, deutsch)],
    dazu die Wortanfangs-Varianten."""
    ganz: dict[str, list[tuple[tuple[str, ...], str]]] = {}
    anfang: list[tuple[str, str]] = []
    for deutsch, varianten in GLOSSAR:
        de_n = normalisiere(deutsch).strip()
        for v in varianten:
            if v.endswith("*"):
                stamm = normalisiere(v[:-1]).strip()
                anfang.append((stamm, de_n))
                continue
            teile = tuple(_WORT_RE.findall(normalisiere(v)))
            if teile:
                ganz.setdefault(teile[0], []).append((teile, de_n))
    return ganz, anfang


def glossen(claim: str) -> tuple[str, ...]:
    """Die deutschen Trigger-Begriffe zu allen englischen Glossar-Begriffen
    im Claim, in Fundreihenfolge, ohne Dubletten."""
    ganz, anfang = _index()
    w = _woerter(claim)
    gefunden: dict[str, None] = {}
    for i, wort in enumerate(w):
        for schluessel in _grundformen(wort):
            for teile, de_n in ganz.get(schluessel, ()):
                n = len(teile)
                if n == 1:
                    if _endung_ok(wort, teile[0]):
                        gefunden[de_n] = None
                    continue
                if (w[i:i + n - 1] == list(teile[:-1])
                        and i + n - 1 < len(w)
                        and _endung_ok(w[i + n - 1], teile[-1])):
                    gefunden[de_n] = None
        for stamm, de_n in anfang:
            if wort.startswith(stamm):
                gefunden[de_n] = None
    return tuple(gefunden)


@lru_cache(maxsize=512)
def englische_fassung(claim_lc: str) -> str | None:
    """Der glossierte Vergleichstext fuer den englischen Pass — oder None,
    wenn der Claim nicht englisch ist oder kein Glossar-Begriff darin steht.

    Form: ``"<deutsch> ; <deutsch> ; … ; <Rest-Claim ohne englische
    Funktionswoerter>"``. Das Semikolon trennt die Glossen, damit ein
    Mehrwort-Trigger nicht ueber zwei benachbarte Glossen hinweg trifft.
    Der Rest-Claim bleibt, weil Eigennamen, Zahlen und Kognaten („Vorarlberg",
    „PISA", „2024") die anderen Bedingungen eines Composite erfuellen.
    """
    if not claim_lc or not ist_englisch(claim_lc):
        return None
    g = glossen(claim_lc)
    if not g:
        return None
    rest = [x for x in _woerter(claim_lc) if x not in _EN_ENTFERNEN]
    return " ; ".join(g) + " ; " + " ".join(rest)
