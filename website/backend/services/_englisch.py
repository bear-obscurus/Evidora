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

from services._flexion import _muster as _flexion_muster
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

_WORT_RE = re.compile(r"[a-z0-9']+(?:[.,][0-9]+)*")

# Englische Zahlschreibweise auf die deutsche der Trigger bringen:
# Tausender-Komma -> Punkt ("15,000" -> "15.000"), Dezimalpunkt -> Komma
# ("0.5" -> "0,5"). Ein Punkt vor GENAU drei Ziffern bleibt ein Punkt —
# "100.000" ist in einem englischen Claim fast immer ein deutscher Import.
_TAUSENDER_RE = re.compile(r"(?<=\d),(?=\d{3}(?!\d))")
_DEZIMAL_RE = re.compile(r"(?<=\d)\.(?=\d{1,2}(?!\d)|\d{4,})")

# Englische Glossar-Woerter, die auch deutsche Woerter sind. Sie glossieren,
# zaehlen aber nicht als Evidenz fuer das Stichwort-Gate: „Hitler war
# Sozialist" traegt kein deutsches Funktionswort und „war" steht im Glossar.
_DE_HOMOGRAPHEN = frozenset(
    "die war man pension party rate standard test online job jobs".split())

# Stichwort-Gate: so viele eindeutig englische Glossar-Begriffe braucht ein
# Claim ohne jedes Funktionswort. Gemessen, siehe englisch_gate.
VOKABEL_EVIDENZ_MIND = 2


def _woerter(text: str) -> list[str]:
    t = normalisiere(text.replace("’", "'").replace("‘", "'"))
    t = _DEZIMAL_RE.sub(",", _TAUSENDER_RE.sub(".", t))
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
    # „students" allein: „studenten", nicht „studierende" — das endet auf
    # „de " und traf damit den ETER-Trigger „de " (Deutschland-Kuerzel).
    ("studenten", ("students", "student")),
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
    ("verdoppelt", ("doubled", "doubles", "double", "doubling")),
    ("höher", ("higher",)),
    ("höchster", ("highest",)),
    ("größte", ("largest", "biggest")),
    ("kleinste", ("smallest",)),
    ("besser", ("better",)),
    ("schlechter", ("worse",)),
    ("täglich", ("daily", "every day", "a day", "per day")),
    ("jährlich", ("annually", "a year", "per year", "every year", "annual")),
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
    ("sinnlos", ("pointless", "useless")),
    ("bringt nichts", ("achieves nothing", "does nothing", "is pointless",
                       "is useless", "achieve nothing")),
    ("verursacht", ("causes", "caused")),
    ("heilt", ("cures", "cure", "heals")),
    ("verhindert", ("prevents", "prevent")),
    ("schützt", ("protects", "protect")),
    ("zerstört", ("destroys", "destroy", "destroyed", "ruins", "ruin")),
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
    ("ausreichend", ("sufficient", "adequately", "adequate", "enough")),
    ("unnötig", ("unnecessary",)),
    ("gibt es nicht", ("doesn't exist", "does not exist", "there is no",
                       "there's no", "there are no")),

    # --- Allgemeine Sachbegriffe --------------------------------------------
    ("krank", ("sick", "ill")),
    ("krankheit", ("disease", "illness")),
    ("krebs", ("cancer",)),
    ("krebserregend", ("carcinogenic",)),
    ("tote", ("deaths", "dead")),
    ("sterben", ("die", "dying", "died")),
    ("risiko", ("risk",)),
    ("studie", ("study", "studies")),
    ("wissenschaft", ("science",)),
    ("behandlung", ("treatment",)),
    ("therapie", ("therapy",)),
    ("empfohlen", ("recommended",)),
    ("empfehlung", ("recommendation", "recommended")),
    ("erstlinien", ("first line",)),
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
    ("ausgaben", ("spending", "expenditure", "spend", "spends")),
    ("schulden", ("debt",)),
    ("haushalt", ("budget", "household")),
    ("zukunft", ("future",)),
    ("erfolg", ("success",)),
    ("erfolgreich", ("successful",)),

    # --- Themen: Alltag, Arbeitsmarkt, Gerichte, Leitlinien -----------------
    ("muss", ("must", "has to", "have to", "need to", "needs to")),
    ("zwingend", ("absolutely", "necessarily")),
    ("jeder mensch", ("everybody", "everyone", "every person")),
    ("lesen", ("reading", "read")),
    ("augen", ("eye", "eyesight")),
    ("dunkel", ("dark", "darkness")),
    ("stunden", ("hours",)),
    ("blasenentzündung", ("bladder infection", "urinary tract infection", "uti")),
    ("kalt", ("cold",)),
    ("erkältung", ("a cold", "common cold", "catch a cold", "catch cold")),
    ("nasse haare", ("wet hair",)),
    ("nass", ("wet",)),
    ("linkshänder", ("left handed", "left hander", "lefties")),
    ("lebenserwartung", ("life expectancy", "lifespan", "living longer",
                         "live longer")),
    ("sterblichkeit", ("mortality",)),
    ("kürzer", ("shorter",)),
    ("kaffee", ("coffee",)),
    ("tee", ("tea",)),
    ("koffein", ("caffeine",)),
    ("dehydrier", ("dehydrat*",)),
    ("vollmond", ("full moon",)),
    ("mondphase", ("moon phase", "lunar phase", "lunar cycle")),
    ("mond", ("moon",)),
    ("geburten", ("births",)),
    ("geburt", ("birth",)),
    ("notaufnahme", ("emergency room", "emergency department")),
    ("wunde", ("wound",)),
    ("verletzung", ("injury", "injuries")),
    ("jod", ("iodine",)),
    ("desinfektion", ("disinfect*",)),
    ("offene stellen", ("job vacancies", "job openings", "open positions")),
    ("langzeit", ("long term",)),
    ("beschäftigte", ("employed", "employment")),
    ("zeitreihe", ("over time", "time series")),
    ("historisch", ("historical", "historically")),
    ("industrie", ("industry",)),
    ("quelle", ("source",)),
    ("daten", ("data",)),
    ("vergleich", ("compared", "comparison", "versus", "vs")),
    ("unterschied", ("difference", "differ")),
    ("vernichtet", ("destroys", "destroy", "kills", "wipes out", "eliminates")),
    ("arbeitsplatzverlust", ("job losses", "job loss")),
    ("produktivität", ("productivity", "productive")),
    ("leistung", ("performance", "output")),
    ("homeoffice", ("working from home", "work from home", "home office",
                    "remote work", "remote working", "teleworking")),
    ("diagnose", ("diagnosis", "diagnosed")),
    ("anerkannt", ("recognised", "recognized", "official", "approved",
                   "granted", "accepted")),
    ("fachkräftemangel", ("skilled labour shortage", "skilled labor shortage",
                          "skilled worker shortage", "skills shortage",
                          "labour shortage", "labor shortage",
                          "worker shortage")),
    ("gewerkschaft", ("trade union", "labour union", "labor union", "unions")),
    ("kein einfluss", ("no effect", "no influence", "no impact")),
    ("ersetzt", ("replace", "replaces", "replacing")),
    ("drückt löhne", ("pushes wages down", "push wages down", "drives down wages",
                      "depresses wages", "lowers wages")),
    ("lohn-druck", ("wage pressure", "downward pressure on wages")),
    ("karenz", ("parental leave", "maternity leave", "paternity leave")),
    ("kinderbetreuungsgeld", ("childcare allowance", "childcare benefit")),
    ("lohnverlust", ("wage loss", "loss of earnings", "wage penalty")),
    ("lohneffekt", ("effect on wages", "wage effect")),
    ("weniger", ("less", "fewer")),
    ("pflegekraeftemangel", ("nursing shortage", "shortage of nurses",
                             "nurse shortage", "care worker shortage")),
    ("pflegepersonal", ("nursing staff", "nurses", "care workers")),
    ("scheinselbststaendigkeit", ("bogus self employment",
                                  "false self employment",
                                  "disguised employment")),
    ("plattform-arbeit", ("platform work", "gig work", "gig economy")),
    ("selbststaendig", ("self employed", "self employment",
                        "independent contractor")),
    ("ehe", ("marriage",)),
    ("gleichgeschlechtlich", ("same sex", "gay marriage")),
    ("sterbehilfe", ("assisted suicide", "euthanasia", "assisted dying")),
    ("impfpflicht", ("vaccine mandate", "vaccination mandate",
                     "compulsory vaccination", "mandatory vaccination")),
    ("gebühr", ("fee", "licence fee", "license fee")),
    ("vwgh", ("supreme administrative court",)),
    ("stichwahl", ("runoff", "run off")),
    ("kopftuch", ("headscarf", "headscarves", "hijab")),
    ("kopftuchverbot", ("headscarf ban", "hijab ban")),
    ("falschaussage", ("false testimony", "perjury", "false statement")),
    ("verurteilt", ("convicted", "sentenced")),
    ("hypertonie", ("hypertension", "high blood pressure")),
    ("koronare herzkrankheit", ("coronary heart disease",
                                "coronary artery disease")),
    ("herzinfarkt", ("heart attack", "myocardial infarction")),
    ("herzinsuffizienz", ("heart failure",)),
    ("schlaganfall", ("stroke",)),
    ("thrombolyse", ("thrombolysis",)),
    ("brustkrebs", ("breast cancer",)),
    ("darmkrebs", ("colorectal cancer", "bowel cancer", "colon cancer")),
    ("koloskopie", ("colonoscopy", "colonoscopies")),
    ("prostatakrebs", ("prostate cancer",)),
    ("lungenkrebs", ("lung cancer",)),
    ("schizophrenie", ("schizophrenia",)),
    ("psychose", ("psychosis",)),
    ("rückenschmerz", ("back pain", "low back pain", "lower back pain")),
    ("adipositas", ("obesity", "obese")),
    ("übergewicht", ("overweight",)),
    ("demenz", ("dementia",)),
    ("epilepsie", ("epilepsy",)),
    ("multiple sklerose", ("multiple sclerosis",)),
    ("palliativ", ("palliative",)),
    ("osteoporose", ("osteoporosis",)),
    ("niereninsuffizienz", ("kidney failure", "renal failure",
                            "chronic kidney disease")),
    ("nephropathie", ("nephropathy",)),
    ("schilddrüse", ("thyroid",)),
    ("hypothyreose", ("hypothyroidism",)),
    ("neurodermitis", ("eczema",)),
    ("atopische dermatitis", ("atopic dermatitis",)),
    ("rheumatoide arthritis", ("rheumatoid arthritis",)),
    ("suizid", ("suicide", "suicidal")),
    ("ace-hemmer", ("ace inhibitor",)),
    ("sglt-2", ("sglt2",)),
    ("betablocker", ("beta blocker",)),
    ("lernstil", ("learning style",)),
    ("lerntyp", ("learner type", "type of learner", "visual learner")),
    ("schlau", ("smart", "smarter", "clever", "cleverer")),
    ("gehirnhälfte", ("left brain", "right brain", "left brained",
                      "right brained", "brain hemisphere", "hemisphere")),
    ("effizient", ("efficient",)),
    ("früh", ("early", "earlier")),
    ("babys", ("babies",)),
    ("klassengröße", ("class size",)),
    ("hausaufgaben", ("homework",)),
    ("lernerfolg", ("learning success", "learning outcomes",
                    "academic achievement")),
    ("unterricht", ("teaching", "lessons", "instruction")),
    ("klassenwiederholung", ("repeating a grade", "grade retention",
                             "repeating a year")),
    ("wichtig", ("important",)),
    # --- Themen: Cybersicherheit, Datenschutz, Demokratie -------------------
    ("passwort", ("password",)),
    ("wechseln", ("change", "changed", "changing")),
    ("regelmäßig", ("regularly",)),
    ("viren", ("virus", "viruses")),
    ("öffentliches wlan", ("public wi fi", "public wifi", "public wlan")),
    ("umständlich", ("cumbersome", "inconvenient")),
    ("unsicher", ("insecure", "unsafe", "not safe", "not secure")),
    ("schreibfehler", ("spelling mistake", "spelling error", "typo")),
    ("rechtschreibung", ("spelling",)),
    ("erkennen", ("spot", "recognise", "recognize", "detect", "identify")),
    ("kmu", ("small business", "small businesses", "smes", "sme",
             "small companies")),
    ("in kraft", ("in force", "in effect")),
    ("enthüllt", ("revealed", "exposed", "leaked", "disclosed")),
    ("geheimdienst", ("intelligence service", "secret service",
                      "intelligence agency", "spy agency")),
    ("überwachung", ("surveillance", "monitoring")),
    ("überwacht", ("monitors", "monitored", "spies on")),
    ("massenüberwachung", ("mass surveillance",)),
    ("flächendeckend", ("across the board", "blanket", "everywhere")),
    ("verschwörungstheorie", ("conspiracy theory", "conspiracy theories")),
    ("verschwörung", ("conspiracy",)),
    ("klarnamenpflicht", ("real name requirement", "real name policy",
                          "real names")),
    ("auskunft", ("request", "requests", "disclosure")),
    ("bundestrojaner", ("federal trojan", "state trojan", "government trojan",
                        "state spyware", "government spyware")),
    ("personenbezogen", ("personal data", "personally identifiable")),
    ("briefwahl", ("postal vote", "postal voting", "mail in voting",
                   "mail in ballot", "absentee ballot", "voting by mail")),
    ("volksbegehren", ("popular initiative", "people's initiative",
                       "petition for a referendum")),
    ("ignoriert", ("ignored", "ignore")),
    ("bundesrat", ("federal council",)),
    ("schwach", ("weak", "powerless")),
    ("wahlbetrug", ("election fraud", "electoral fraud", "voter fraud")),
    ("online-petition", ("online petition",)),
    ("verfall", ("decline", "declining", "decay", "backsliding")),
    ("hervorragend", ("excellent", "outstanding")),
    ("gefährdet", ("threatened", "at risk", "endangered", "endanger")),
    ("us-wahlsystem", ("us electoral system", "american electoral system",
                       "us election system")),
    ("undemokratisch", ("undemocratic",)),
    ("vertrauen", ("trust", "confidence")),
    ("zufrieden", ("satisfied", "satisfaction")),
    ("freier fall", ("free fall",)),
    ("bevölkerung", ("population",)),
    ("einwohner", ("inhabitants", "residents")),
    # --- Themen: Statistik DE, Digitales, Bildung, Energie ------------------
    ("verbraucherpreis", ("consumer prices", "consumer price index", "cpi")),
    ("geburtenrate", ("birth rate", "fertility rate", "birthrate")),
    ("kinder je frau", ("children per woman",)),
    ("rezession", ("recession",)),
    ("wirtschaftswachstum", ("economic growth",)),
    ("altert", ("ageing", "aging", "getting older", "older and older")),
    ("bildschirmzeit", ("screen time",)),
    ("konzentration", ("concentration", "attention span", "focus")),
    ("aufmerksamkeit", ("attention",)),
    ("videospiele", ("video game", "computer game", "gaming")),
    ("aggressiv", ("aggressive", "aggression")),
    ("depressiv", ("depressed",)),
    ("schlimmer", ("worse",)),
    ("lese-app", ("reading app",)),
    ("genauso", ("just as", "as good as", "equally")),
    ("vorlesen", ("reading aloud", "read aloud")),
    ("vor", ("before",)),
    ("mangel", ("shortage", "lack of")),
    ("sitzenbleiben", ("repeating a grade", "repeat a year", "grade repetition",
                       "held back")),
    ("hilft", ("helps", "help")),
    ("lesekompetenz", ("reading literacy", "reading skills", "reading scores",
                       "reading results")),
    ("studiengebühr", ("tuition fees", "tuition fee", "tuition")),
    ("schulpflicht", ("compulsory schooling", "compulsory education",
                      "mandatory schooling")),
    ("atomkraft", ("nuclear power", "nuclear energy")),
    ("atomstrom", ("nuclear electricity", "nuclear power")),
    ("atom", ("nuclear",)),
    # „carbon free" nicht nach „klimaneutral": das traegt „neutral", und der
    # Neutralitaets-Fakt hat „neutral" in beiden Composite-Gruppen.
    ("klimaneutral", ("climate neutral",)),
    ("co2 frei", ("carbon free", "carbon neutral", "emission free",
                  "zero emission")),
    ("kohle", ("coal",)),
    ("kohlekraftwerk", ("coal plant", "coal power plant", "coal power station",
                        "coal fired power station")),
    ("e-auto", ("electric car", "electric vehicle", "evs", "e car")),
    ("verbrenner", ("combustion car", "combustion engine", "petrol car",
                    "diesel car", "internal combustion")),
    ("klimabilanz", ("climate footprint",)),
    ("co2 bilanz", ("carbon footprint", "carbon balance")),
    ("wärmepumpe", ("heat pump",)),
    ("frost", ("freezing",)),
    ("minus", ("sub zero", "below zero")),
    ("solarmodul", ("solar panel", "solar module")),
    ("recyclebar", ("recyclable", "recycled", "recycling")),
    ("windkraft", ("wind power", "wind turbine", "wind energy")),
    ("vögel", ("birds", "bird")),
    ("erneuerbare", ("renewables", "renewable")),
    ("versorgungssicherheit", ("security of supply", "supply security",
                               "energy security")),
    ("stromausfall", ("power outage", "power cut")),
    ("sauber", ("clean",)),
    ("import", ("imports", "import")),
    ("kauf", ("buys", "buy", "buying")),
    ("nordsee", ("north sea",)),
    ("windpark", ("wind farm", "wind park")),
    ("atomausstieg", ("nuclear phase out", "nuclear exit")),
    ("stromimport", ("electricity imports", "power imports")),
    # --- Themen: Ernaehrung, Esoterik ---------------------------------------
    ("eier", ("eggs", "egg")),
    ("cholesterin", ("cholesterol",)),
    ("herz", ("heart",)),
    ("sehkraft", ("eyesight", "vision")),
    ("saft", ("juice", "juices")),
    ("entgift", ("cleanse", "cleanses", "detoxif*")),
    ("nährstoff", ("nutrient",)),
    ("brauner zucker", ("brown sugar",)),
    ("gesünder", ("healthier",)),
    ("pestizid", ("pesticide",)),
    ("schleim", ("mucus", "phlegm")),
    ("heilstein", ("healing crystal", "healing stone", "crystal healing",
                   "gemstone")),
    ("heil", ("healing", "heal")),
    ("astrolog", ("astrology", "astrological")),
    ("horoskop", ("horoscope",)),
    ("sternzeichen", ("star sign", "zodiac sign")),
    ("charakter", ("character", "personality")),
    ("vorhersag", ("predict", "predicts", "prediction")),
    ("bach-blüt", ("bach flower", "bach flowers")),
    ("lindern", ("relieve", "relieves", "alleviate", "alleviates")),
    ("körper", ("body",)),
    ("wünschelrute", ("dowsing", "dowser", "divining rod")),
    ("find", ("find", "finds")),
    ("geistheil", ("spiritual healing", "faith healing", "psychic healing")),
    ("genes", ("recovery", "recover")),
    ("salz", ("salt", "salts")),
    ("bioresonanz", ("bioresonance",)),
    ("allergi", ("allergy", "allergies")),
    ("hellseh", ("psychic", "psychics", "clairvoyant", "clairvoyance")),
    ("wahrsage", ("fortune teller", "fortune telling")),
    ("rückführ", ("past life regression", "regression")),
    ("reinkarnat", ("reincarnation",)),
    ("frühere leben", ("past lives", "past life", "earlier lives",
                       "previous lives")),
    ("beweis", ("proves", "prove", "proof", "evidence")),
    ("irisdiagnose", ("iridology",)),
    ("verwirbel", ("vortexed", "vortex", "structured water", "energised water",
                   "energized water")),
    ("schwitz", ("sweating", "sweat")),
    ("giftstoff", ("toxin", "toxins")),
    ("münchen", ("munich",)),
    # --- Themen: Gerichte EU, Kriminalitaet, Eurobarometer, Finanzen --------
    ("transitzone", ("transit zone",)),
    ("wahlrecht", ("right to vote", "voting rights")),
    ("humanitäre visa", ("humanitarian visa", "humanitarian visas")),
    ("kruzifix", ("crucifix",)),
    ("mord", ("murder", "murders")),
    ("tatverdächtig", ("suspect", "suspects")),
    ("straftat", ("crimes",)),
    ("großteil", ("most of", "the majority of")),
    ("eu-bürger", ("eu citizens", "european citizens", "europeans")),
    ("thema", ("issue", "topic")),
    ("sorge", ("concern", "worry", "worries")),
    ("prioritäten", ("priority", "priorities")),
    ("austritt", ("leave", "leaving", "exit")),
    ("einwanderung", ("immigration",)),
    ("aktien", ("stocks", "shares", "stock market", "equities")),
    ("börse", ("stock exchange", "stock market")),
    ("glücksspiel", ("gambling", "gamble", "casino")),
    ("echte", ("real", "actual")),
    ("lohnt", ("worth it", "pays off", "worthwhile")),
    ("kalte progression", ("bracket creep", "cold progression", "fiscal drag")),
    ("heimlich", ("secretly", "secret", "hidden")),
    ("pyramide", ("pyramid scheme", "pyramid")),
    ("schneeballsystem", ("ponzi scheme", "snowball system")),
    ("sparer", ("savers", "saver")),
    ("negativzins", ("negative interest", "negative interest rates",
                     "negative rates")),
    ("enteignet", ("expropriated", "expropriate", "robbed", "dispossessed")),
    ("krypto", ("crypto", "cryptocurrency", "cryptocurrencies")),
    ("empfiehlt", ("recommends", "recommend", "endorses")),
    ("reich", ("rich", "wealthy", "richest")),
    ("verdienen", ("earn", "make money")),
    ("rendite", ("return", "returns", "yield")),
    ("garantiert", ("guaranteed",)),
    ("risikolos", ("risk free", "no risk", "riskless")),
    ("seriös", ("serious", "legitimate", "reputable")),
    ("geld verdienen", ("make money", "earn money")),
    ("inflationsschutz", ("protects against inflation", "inflation hedge",
                          "hedge against inflation")),
    ("lebensversicherung", ("life insurance", "endowment",
                            "endowment policy")),
    ("anlage", ("investment",)),
    ("schnell", ("fast", "quick", "quickly")),
    ("sofort", ("immediately",)),
    ("eilig", ("hurry", "urgent", "act fast")),
    ("sicherer hafen", ("safe haven",)),
    # --- Themen: Geographie, Geschichte -------------------------------------
    ("verschwund", ("disappear", "disappears", "disappeared", "vanish",
                    "vanished")),
    ("schiffe", ("ships", "ship")),
    ("flugzeug", ("plane", "planes", "aircraft")),
    ("berg", ("mountain",)),
    ("waschbecken", ("sink", "basin", "washbasin")),
    ("toilette", ("toilet",)),
    ("abfluss", ("drain", "plughole")),
    ("dreht", ("swirls", "rotates", "spins", "turns")),
    ("halbkugel", ("hemisphere",)),
    ("südhalbkugel", ("southern hemisphere",)),
    ("nordhalbkugel", ("northern hemisphere",)),
    ("wüste", ("desert",)),
    ("antarktis", ("antarctica", "antarctic")),
    ("tiere", ("animals",)),
    ("tier", ("animal",)),
    ("vatikan", ("vatican",)),
    ("sozialist", ("socialist",)),
    ("links", ("left wing", "leftist")),
    ("kriegsverbrech", ("war crime",)),
    ("unschuldig", ("innocent",)),
    ("nie stattgefunden", ("never happened", "did not happen",
                           "didn't happen")),
    ("wussten", ("knew",)),
    ("wissen", ("know",)),
    ("erstes opfer", ("first victim",)),
    ("gleich", ("same", "equal", "identical")),
    ("ideologisch", ("ideologically", "ideological")),
    ("zweiter weltkrieg", ("second world war", "world war ii", "world war 2",
                           "ww2", "wwii")),
    ("berliner mauer", ("berlin wall",)),
    ("mauer", ("wall",)),
    ("erschossen", ("shot", "shot dead")),
    ("niemand", ("nobody", "no one")),
    ("rein wirtschaft", ("pure economic", "purely economic")),
    ("bedingung", ("condition", "conditions", "strings attached")),
    ("notwendig", ("necessary", "needed")),
    ("irak", ("iraq",)),
    ("massenvernichtung", ("weapons of mass destruction", "mass destruction")),
    ("osterweiterung", ("eastward expansion", "expand eastwards",
                        "expand east", "eastern enlargement", "enlargement")),
    ("versprech", ("promised", "promise")),
    ("kein krieg", ("no war",)),
    ("geplant", ("planned", "plan")),
    ("mondlandung", ("moon landing",)),
    ("inszenier", ("staged", "faked", "fake")),
    ("wikinger", ("vikings",)),
    ("hörner", ("horns", "horned")),
    ("hörnerhelm", ("horned helmet",)),
    ("mittelalter", ("middle ages", "medieval")),
    ("flach", ("flat",)),
    ("erde", ("earth",)),
    ("dick", ("fat",)),
    ("hexen", ("witches", "witch")),
    ("hexenverbrennung", ("witch burning", "witch burnings")),
    ("gefoltert", ("tortured",)),
    ("verbrannt", ("burned", "burnt")),
    ("apfel", ("apple",)),
    ("kuchen", ("cake",)),
    ("schulversager", ("failed at school", "failed school", "bad student",
                       "poor student")),
    ("mathe", ("maths", "math")),
    ("glühbirne", ("light bulb", "lightbulb")),
    ("nase", ("nose",)),
    # --- Themen: Gesundheitsbehoerden, Gleichstellung, Inklusion ------------
    ("trinkwasser", ("drinking water", "tap water")),
    ("rotes fleisch", ("red meat",)),
    ("verarbeitetes fleisch", ("processed meat",)),
    ("babyflasche", ("baby bottle",)),
    ("vorbeugen", ("prevent", "prevents")),
    ("impfschäden", ("vaccine injury", "vaccine injuries", "vaccine damage")),
    ("nebenwirkungen", ("side effects", "side effect")),
    ("erbgut", ("genome", "genetic material")),
    ("verändert", ("alter", "alters", "modify", "modifies")),
    ("übersterblichkeit", ("excess mortality", "excess deaths")),
    ("nieren", ("kidneys", "kidney")),
    ("müd", ("drowsy", "tired", "sleepy", "drowsiness")),
    ("gratis", ("free", "free of charge", "at no cost")),
    ("freier welthandel", ("free world trade", "free trade")),
    ("frauenquote", ("women's quota", "women quota", "gender quota",
                     "quota for women", "quotas for women", "female quota")),
    ("femizid", ("femicide",)),
    ("ermordet", ("murdered", "killed")),
    ("vergewaltigung", ("rape",)),
    ("anzeige", ("report", "reports", "complaint")),
    ("falsch", ("false", "wrong")),
    ("sorgearbeit", ("care work", "unpaid care", "housework", "domestic work")),
    ("verteilung", ("shared", "distribution", "split")),
    ("frauen in politik", ("women in politics",)),
    ("politik", ("politics", "policy")),
    ("vertret", ("represented", "representation")),
    ("sexismus", ("sexism", "sexist")),
    ("heute", ("today", "nowadays", "these days")),
    ("kein thema", ("no longer an issue", "not an issue", "no issue")),
    ("mutterschaft", ("motherhood",)),
    ("mutter", ("mother",)),
    ("gleichberechtigung", ("gender equality", "equal rights", "equality")),
    ("unleistbar", ("unaffordable", "not affordable")),
    ("wohnkosten", ("housing costs", "housing cost")),
    ("belastung", ("burden",)),
    ("down-syndrom", ("down syndrome", "down's syndrome", "trisomy 21")),
    ("leben", ("live", "lives", "life")),
    ("alt", ("old",)),
    ("genetisch", ("genetically", "genetic")),
    ("festgelegt", ("fixed", "determined", "set in stone")),
    ("inklusion", ("inclusion", "inclusive")),
    ("sonderschule", ("special school", "special needs school")),
    ("pflegeheim", ("nursing home", "care home")),
    ("überfüllt", ("overcrowded", "overfilled", "overbooked")),
    ("behinderung", ("disability", "disabilities")),
    ("un-behindertenrechtskonvention", (
        "un convention on the rights of persons with disabilities",
        "convention on the rights of persons with disabilities",
        "un disability convention", "crpd")),
    ("umgesetzt", ("implemented", "implements", "implement")),
    ("autismus", ("autism", "autistic")),
    ("hochsensibilität", ("high sensitivity", "highly sensitive")),
    ("barrierefrei", ("accessible", "barrier free", "accessibility")),
    ("globale erwärmung", ("global warming",)),
    ("verlangsamt", ("slowed", "slowed down", "slowing", "paused")),
    ("weltwirtschaft", ("world economy", "global economy")),
    ("welthandel", ("world trade", "global trade")),
    ("ganztagsschule", ("all day school", "full day school",
                        "all day schooling")),
    ("größe", ("size",)),
    ("migrationshintergrund", ("migration background", "migrant background",
                               "immigrant background")),
    ("tertiär", ("tertiary",)),
    ("akademiker", ("graduates", "university graduates", "academics")),
    ("sonderpädagog", ("special educational needs", "special needs")),
    # --- Themen: Kultur, Landwirtschaft, Lebensmittel, Medien ---------------
    ("pyramiden", ("pyramids", "pyramid")),
    ("sklaven", ("slaves", "slave", "slavery")),
    ("armut", ("poverty", "poor", "penniless")),
    ("klein", ("short", "small", "little")),
    ("helm", ("helmet",)),
    ("kolumbus", ("columbus",)),
    ("geschrieben", ("wrote", "written", "write")),
    ("autor", ("author", "authorship")),
    ("gentechnik", ("gmo", "gmos", "genetically modified",
                    "genetic engineering", "gm food", "gm crops")),
    ("gesundheit", ("health",)),
    ("gesundheits-risiko", ("health hazard", "health risk")),
    ("biolandbau", ("organic farming", "organic agriculture")),
    ("ertrag", ("yield", "yields")),
    ("saatgut", ("seeds", "seed")),
    ("kontrolliert", ("control", "controls", "controlled")),
    ("subvention", ("subsidies", "subsidy", "subsidised", "subsidized")),
    ("selbstversorgung", ("self sufficiency", "self sufficient",
                          "feeds itself", "feed itself")),
    ("versorg", ("supply", "feeds", "feed")),
    ("welternährung", ("world hunger", "feeding the world", "world food",
                       "global food supply")),
    ("massentierhaltung", ("factory farming", "intensive farming",
                           "industrial farming", "livestock farming")),
    ("industriell", ("industrial",)),
    ("voller", ("full of",)),
    ("düngemittel", ("fertiliser", "fertilizer")),
    ("produzieren", ("produce", "produces", "production")),
    ("landwirtschaft", ("agriculture", "farming", "agricultural")),
    ("rinder", ("cattle", "cows", "cow")),
    ("schweine", ("pigs", "pig")),
    ("mehr", ("more",)),
    ("fleischkonsum", ("meat consumption", "meat eating", "eating meat")),
    ("spinat", ("spinach",)),
    ("aufgewärmt", ("reheated", "reheat", "reheating", "warmed up")),
    ("pilze", ("mushrooms", "mushroom")),
    ("kartoffel", ("potato", "potatoes")),
    ("grün", ("green",)),
    ("mikrowelle", ("microwave",)),
    ("honig", ("honey",)),
    ("kühlschrank", ("fridge", "refrigerator")),
    ("mhd", ("best before date", "best before", "expiry date", "use by date")),
    ("wegwerfen", ("thrown away", "throw away", "discard")),
    ("würstchen", ("sausages", "sausage", "hot dogs")),
    ("kühlkette", ("cold chain",)),
    ("unterbrochen", ("broken", "breaking", "interrupted")),
    ("egal", ("doesn't matter", "never matters", "does not matter")),
    ("auftauen", ("thaw", "thawing", "defrost", "defrosting")),
    ("theke", ("counter", "worktop", "countertop")),
    ("zimmertemperatur", ("room temperature",)),
    ("umfrage", ("poll", "polls", "survey", "surveys")),
    ("gefälscht", ("falsified", "faked", "fake", "doctored")),
    ("inserat", ("advertising", "advertisement", "ads", "adverts")),
    ("inseratenaffäre", ("advertising affair", "ads affair")),
    ("regierungsinserat", ("government advertising", "government ads")),
    # --- Themen: Psyche, Migration, Mobilitaet, Gesundheit, Integration -----
    ("charakterschwäche", ("weakness of character", "character weakness",
                           "character flaw")),
    ("schwäche", ("weakness",)),
    ("antidepressiva", ("antidepressant", "antidepressants")),
    ("psychotherapie", ("psychotherapy",)),
    ("darüber reden", ("talk about", "talking about", "discuss")),
    ("berichterstattung", ("reporting", "coverage")),
    ("launisch", ("moody",)),
    ("mode", ("fad", "fashion")),
    ("anderer mensch", ("different person", "another person")),
    ("persönlichkeit", ("personality",)),
    ("träume", ("dreams", "dream")),
    ("bedeutung", ("meaning", "significance")),
    ("redet", ("talk", "talks", "talking")),
    ("disziplin", ("discipline",)),
    ("selbstkontrolle", ("self control",)),
    ("austausch", ("replacement", "great replacement",
                   "population replacement", "replaced")),
    ("sozialleistung", ("welfare benefits", "welfare", "social benefits",
                        "benefits")),
    ("abschiebung", ("deportation", "deportations", "deport", "deported")),
    ("einfach", ("easy", "easily", "simple")),
    ("arbeit verboten", ("not allowed to work", "banned from working",
                         "work ban")),
    ("asylantrag", ("asylum application", "asylum claims",
                    "asylum request")),
    ("familiennachzug", ("family reunification", "family reunion")),
    ("unkontrolliert", ("uncontrolled",)),
    ("reichweite", ("range",)),
    ("pünktlich", ("punctual", "punctuality", "on time")),
    ("verspätung", ("delay", "delays", "late")),
    ("tempolimit", ("speed limit",)),
    ("klimaticket", ("climate ticket",)),
    ("bilanz", ("track record", "results")),
    ("wasserstoff", ("hydrogen",)),
    ("öpnv", ("public transport", "public transportation", "public transit")),
    ("finanziert", ("funded", "financed")),
    ("stadt", ("city", "cities", "town", "urban")),
    ("subventioniert", ("subsidised", "subsidized")),
    ("lkw-maut", ("truck toll", "lorry toll", "hgv toll")),
    ("bahn-investition", ("rail investment", "investment in rail",
                          "railway investment")),
    ("parken", ("parking",)),
    ("parksuchverkehr", ("searching for parking", "cruising for parking")),
    ("ladestation", ("charging station", "charging point", "chargers")),
    ("ladeinfrastruktur", ("charging infrastructure",)),
    ("spital", ("hospital", "hospitals")),
    ("betten", ("beds",)),
    ("schulkinder", ("schoolchildren", "school children")),
    ("dicker", ("fatter", "getting fatter")),
    ("nettozuwanderung", ("net immigration", "net migration")),
    ("migrationssaldo", ("migration balance",)),
    ("einbürgerung", ("naturalisation", "naturalization", "citizenship")),
    ("sprachkurs", ("language course", "german course")),
    ("integrationsmonitor", ("integration monitor",)),
    ("drittstaat", ("third country", "third country nationals")),
    ("asylberechtigt", ("recognised refugees", "recognized refugees",
                        "refugee status")),
    ("subsidiär", ("subsidiary protection",)),
    ("arbeitsmarkt", ("labour market", "labor market", "job market")),
    ("mindestsicherung", ("minimum income", "minimum income support",
                          "minimum income benefits",
                          "guaranteed minimum income")),
    ("anerkennung", ("recognition", "recognise", "recognize")),
    ("qualifikation", ("qualifications", "qualification", "degrees",
                       "diplomas")),
    ("wertekurs", ("values course", "values and orientation course")),
    ("beratung", ("counselling", "counseling", "advice")),
    ("grundversorgung", ("basic care", "basic support",
                         "reception conditions")),
    # --- Themen: Onkologie, Parteienfinanzierung, Recht, Religion -----------
    ("zucker", ("sugar",)),
    ("krebszelle", ("cancer cell",)),
    ("ketogen", ("ketogenic", "keto")),
    ("basisch", ("alkaline",)),
    ("alkalisch", ("alkaline",)),
    ("säure", ("acid", "acidic")),
    ("aprikosenkern", ("apricot kernel", "apricot seeds")),
    ("deo", ("deodorant", "antiperspirant")),
    ("mistel", ("mistletoe",)),
    ("früherkennung", ("early detection",)),
    ("mammographie", ("mammography", "mammogram")),
    ("alter", ("age",)),
    ("parteienfinanzierung", ("party financing", "party finance",
                              "party funding", "party finances")),
    ("parteienförderung", ("party funding", "party subsidies",
                           "public funding for parties")),
    ("parteiengesetz", ("political parties act", "party law")),
    ("geregelt", ("regulated", "regulates", "regulate")),
    ("gesetzlich", ("by law", "legally", "statutory")),
    ("pro wahlberechtigtem", ("per eligible voter", "per voter")),
    ("wahlberechtigt", ("eligible voter", "electorate")),
    ("wahlkampfkosten", ("campaign spending", "campaign costs",
                         "election campaign costs", "campaign expenditure")),
    ("höchstens", ("maximum", "limit", "cap", "at most")),
    ("parteispende", ("party donation", "donations to parties")),
    ("spenden", ("donations", "donation")),
    ("schwelle", ("threshold",)),
    ("veröffentlich", ("disclosure", "disclose", "published", "publication")),
    ("strafe", ("penalty", "penalties", "fine", "fines", "sanction")),
    ("rechenschaftsbericht", ("financial statement", "financial statements",
                              "annual report", "accountability report")),
    ("abgeben", ("submit", "file", "hand in")),
    ("rechnungshof", ("court of audit", "audit office", "court of auditors")),
    ("kontroll", ("oversees", "oversee", "audit", "audits")),
    ("klubförderung", ("parliamentary group funding", "club funding",
                       "parliamentary club funding", "group funding")),
    ("parteiakademie", ("party academy", "party academies")),
    ("notwehr", ("self defence", "self defense")),
    ("einbrecher", ("burglar", "intruder")),
    ("einbruch", ("burglary", "break in")),
    ("schießen", ("shoot", "shooting")),
    ("töten", ("kill",)),
    ("schimmel", ("mould", "mold")),
    ("vermieter", ("landlord",)),
    ("mieter", ("tenant",)),
    ("renovieren", ("renovate", "renovation", "redecorate", "decorate")),
    ("streichen", ("paint", "painting")),
    ("probezeit", ("probation period", "probationary period", "trial period")),
    ("kündigung", ("dismissed", "dismissal", "fired", "terminated",
                   "termination")),
    ("rauswerfen", ("throw out", "throw me out", "kick out", "thrown out")),
    ("geschäft", ("shop", "store")),
    ("enterben", ("disinherit", "disinherited")),
    ("erbe", ("inheritance", "heir")),
    ("pflichtteil", ("compulsory share", "forced share", "legal portion")),
    ("pausen", ("breaks", "break", "lunch break")),
    ("bezahlt", ("paid",)),
    ("kündigungsschutz", ("protection against dismissal", "job protection",
                          "dismissal protection")),
    ("nach", ("after",)),
    ("monaten", ("months",)),
    ("haftet", ("liable", "liability")),
    ("aussterben", ("dying out", "die out", "extinct")),
    ("radikal", ("radical", "radicalised", "radicalized", "extremist")),
    ("friedlich", ("peaceful",)),
    ("kastensystem", ("caste system",)),
    ("kasten", ("caste",)),
    ("überwunden", ("overcome", "a thing of the past")),
    ("kirchensteuer", ("church tax",)),
    ("kirchenbeitrag", ("church contribution",)),
    ("steuergeld", ("taxpayers' money", "taxpayers money", "taxpayer money",
                    "tax money")),
    ("zeugen jehovas", ("jehovah's witnesses", "jehovahs witnesses",
                        "jehovah's witness")),
    ("bluttransfusion", ("blood transfusion",)),
    ("blut", ("blood",)),
    ("sekte", ("cult", "sect")),
    ("destruktiv", ("destructive",)),
    ("kennzeichen", ("signs", "features", "characteristics", "warning signs")),
    ("missbrauch", ("abuse", "paedophilia", "pedophilia", "child abuse")),
    ("einzelfall", ("isolated case", "one off")),
    ("religionsunterricht", ("religious education", "religious instruction",
                             "religion classes", "religion lessons")),
    ("indoktrination", ("indoctrination",)),
    ("gott", ("god",)),
    ("katholisch", ("catholic",)),
    # --- Themen: Reproduktion, Surveillance, Sicherheit, Sozialstaat --------
    ("spermien", ("sperm",)),
    ("lebensdauer", ("lifespan", "survive")),
    ("eisprung", ("ovulation",)),
    ("schwanger werden", ("get pregnant", "become pregnant")),
    ("schwanger", ("pregnant",)),
    ("tag", ("day",)),
    ("geschlecht", ("sex of the child", "gender of the child", "sex",
                    "gender")),
    ("pille", ("the pill", "birth control pill", "contraceptive pill")),
    ("gewicht", ("weight", "weight gain")),
    ("zunehmen", ("gain weight", "put on weight")),
    ("vasektomie", ("vasectomy",)),
    ("periode", ("period", "periods")),
    ("menstruation", ("menstrual cycle", "menstrual")),
    ("synchronisier", ("synchronise", "synchronize", "sync", "synchronised",
                       "synchronized")),
    ("stillen", ("breastfeeding", "breastfeed")),
    ("wechseljahre", ("menopause",)),
    ("masern", ("measles",)),
    ("welle", ("wave", "outbreak", "surge")),
    ("tuberkulose", ("tuberculosis", "tb")),
    ("grippe", ("flu", "influenza")),
    ("neutralität", ("neutrality", "neutral")),
    ("verfassungsrang", ("constitutional status", "constitutional rank")),
    ("wehrpflicht", ("conscription", "military service",
                     "compulsory military service", "national service")),
    ("bestätigt", ("confirmed",)),
    ("beibehaltung", ("retain", "retaining", "keeping", "in favour",
                      "in favor")),
    ("mitgliedstaaten", ("member states", "members")),
    ("armee", ("army",)),
    ("drohnen", ("drone", "drone strikes")),
    ("atomwaffen", ("nuclear weapons", "nuclear weapon", "nukes",
                    "nuclear warheads")),
    ("bundesheer", ("austrian armed forces", "armed forces", "austrian army",
                    "federal army")),
    ("beigetreten", ("joined", "join")),
    ("bedrohung", ("threat",)),
    ("krim", ("crimea",)),
    ("annexion", ("annexation", "annexed")),
    ("cyberverteidigung", ("cyber defence", "cyber defense")),
    ("cybersicherheit", ("cybersecurity", "cyber security")),
    ("sanktionen", ("sanctions", "sanction")),
    ("blockier", ("block", "blocks", "blocked")),
    ("einstimmig", ("unanimous", "unanimity")),
    ("trittbrettfahrer", ("freeloaders", "freeloader", "free riders",
                          "free rider", "scroungers")),
    ("familienbeihilfe", ("family allowance", "child benefit",
                          "family benefit")),
    ("pflegegeld", ("long term care allowance", "care allowance",
                    "nursing allowance", "attendance allowance")),
    ("stufe", ("level", "levels", "tiers", "grades")),
    ("höhe", ("amount",)),
    ("niedrig", ("low", "too low")),
    ("vater", ("father", "fathers", "dads")),
    ("junge", ("the young", "young")),
    ("alte", ("the old", "old people", "elderly")),
    ("wohnbeihilfe", ("housing allowance", "housing benefit",
                      "housing subsidy")),
    ("einheitlich", ("uniform", "standardised", "standardized")),
    ("sozialhilfe", ("social assistance", "social welfare")),
    ("notstandshilfe", ("emergency assistance", "emergency aid",
                        "unemployment assistance")),
    ("arbeitslosengeld", ("unemployment benefit", "jobseeker's allowance")),
    ("studienbeihilfe", ("student grant", "student aid", "study grant")),
    ("stipendium", ("scholarship",)),
    ("bildungskarenz", ("educational leave", "education leave",
                        "training leave")),
    ("reha-geld", ("rehabilitation allowance", "rehab benefit")),
    ("invaliditaets-pension", ("invalidity pension", "disability pension")),
    ("pflegende angehörige", ("family caregivers", "family carers",
                              "caring relatives", "informal carers")),
    ("familienbonus", ("family bonus", "family bonus plus")),
    # --- Themen: Sport, Substanzen, Technik, Tiere, Verkehr -----------------
    ("bauchfett", ("belly fat", "stomach fat", "abdominal fat")),
    ("fett verbrennen", ("burn fat", "burns fat", "burning fat")),
    ("wachstum", ("growth", "grow")),
    ("muskelkater", ("muscle soreness", "sore muscles", "doms")),
    ("milchsäure", ("lactic acid", "lactate")),
    ("krafttraining", ("strength training", "weight training",
                       "lifting weights", "weightlifting")),
    ("muskulös", ("muscular", "bulky")),
    ("fettverbrennung", ("fat burning", "burn more fat")),
    ("abnehmen", ("lose weight", "weight loss")),
    ("puls", ("heart rate", "pulse")),
    ("bauchmuskel", ("abdominal muscles", "six pack", "sixpack")),
    ("küche", ("kitchen",)),
    ("weltmeisterschaft", ("world cup",)),
    ("halbfinale", ("semi final", "semifinal")),
    ("olympia", ("olympics", "olympic", "olympic games")),
    ("medaillen", ("medals", "medal")),
    ("jugendlich", ("teenage",)),
    ("kreativ", ("creative",)),
    ("rotwein", ("red wine",)),
    ("wein", ("wine",)),
    ("herzgesund", ("good for your heart", "good for the heart")),
    ("e-zigarette", ("e cigarette", "vape", "vapes")),
    ("natürlich", ("natural",)),
    ("drogentest", ("drug test", "drug screening")),
    ("wundermittel", ("miracle cure", "wonder drug", "miracle drug",
                      "panacea")),
    ("bewusst", ("conscious", "sentient", "self aware")),
    ("anonymis", ("anonymised", "anonymized", "anonymisation",
                  "anonymization")),
    ("geschwindigkeit", ("speed",)),
    ("sicherer", ("more secure", "safer", "more safe")),
    ("glasfaser", ("fibre", "fiber", "fibre optic", "fiber optic",
                   "broadband")),
    ("schwarz weiß", ("black and white",)),
    ("farbenblind", ("colour blind", "color blind", "colourblind")),
    ("angriff", ("attack", "attacks")),
    ("schokolade", ("chocolate",)),
    ("milch", ("milk",)),
    ("welpe", ("puppy", "puppies")),
    ("wildschwein", ("wild boar", "boar")),
    ("tollwut", ("rabies",)),
    ("rohfütterung", ("raw feeding", "raw food diet", "raw diet")),
    ("neun leben", ("nine lives",)),
    ("hundejahr", ("dog year",)),
    ("sieben menschenjahre", ("seven human years",)),
    ("pfoten landen", ("land on their feet", "lands on its feet",
                       "land on its feet")),
    ("pferdefleisch", ("horse meat", "horsemeat")),
    ("schikane", ("harassment", "chicanery", "red tape")),
    ("fahrradhelm", ("bike helmet", "bicycle helmet", "cycle helmet",
                     "cycling helmet")),
    ("helmpflicht", ("helmet law", "mandatory helmet", "compulsory helmet")),
    ("kindersitz", ("child seat", "car seat")),
    ("sitzerhöhung", ("booster seat", "booster cushion")),
    ("groß genug", ("big enough", "tall enough")),
    ("promille", ("per mille", "blood alcohol", "permille")),
    ("fahren", ("drive", "driving")),
    ("telefonieren", ("phoning", "talking on the phone", "on the phone")),
    ("freisprech", ("hands free", "hands free kit")),
    ("rote ampel", ("red light", "running a red light")),
    ("kein verkehr", ("no traffic",)),
    ("müdigkeit", ("tiredness", "fatigue", "drowsy driving")),
    ("bremsweg", ("braking distance", "stopping distance")),
    ("sommerreifen", ("summer tyres", "summer tires")),
    ("winterreifen", ("winter tyres", "winter tires")),
    ("vorsichtig", ("carefully", "careful")),
    # --- Themen: Verschwoerungen, Welthandel, Wirtschaftspolitik, Wohnen ----
    ("bundesrepublik", ("federal republic",)),
    ("gmbh", ("limited company", "ltd", "corporation")),
    ("steuert", ("controls", "steers", "rules", "runs")),
    ("diktatur", ("dictatorship",)),
    ("echt", ("genuine",)),
    ("patente", ("patents", "patent")),
    ("globalisierung", ("globalisation", "globalization")),
    ("reicher", ("richer", "wealthier")),
    ("export-quote", ("export ratio", "export share", "export rate")),
    ("binnenmarkt", ("single market", "internal market")),
    ("verliert", ("loses", "lose", "losing")),
    ("folgen", ("consequences", "effects", "impact")),
    ("monopol", ("monopoly",)),
    ("finanzier", ("pay for themselves", "pays for itself", "self financing",
                   "finance themselves")),
    ("schuldenbremse", ("debt brake",)),
    ("schwarze null", ("black zero", "balanced budget")),
    ("alternativlos", ("no alternative",)),
    ("sozialsystem", ("welfare system", "social system",
                      "social security system")),
    ("zahlt", ("pays", "pay")),
    ("vermögenssteuer", ("wealth tax",)),
    ("kapitalflucht", ("capital flight",)),
    ("privatisierung", ("privatisation", "privatization")),
    ("staatlich", ("the state", "state owned", "government made")),
    ("zusammenbruch", ("collapse",)),
    ("erbschaftssteuer", ("inheritance tax", "estate tax")),
    ("mittelstand", ("small and medium sized businesses",
                     "small and medium sized enterprises",
                     "family businesses")),
    ("steuerbelastung", ("tax burden",)),
    ("steuerlast", ("tax burden",)),
    ("reiche", ("the rich", "rich people", "millionaires", "billionaires")),
    ("niedriger", ("lower",)),
    ("mietregulierung", ("rent control", "rent regulation")),
    ("mietpreisbremse", ("rent cap", "rent brake", "rent control")),
    ("mietendeckel", ("rent cap", "rent freeze")),
    ("wohnungsbau", ("housing construction", "house building",
                     "new housing", "construction")),
    ("leerstand", ("vacancy", "empty homes", "vacant housing", "vacant homes",
                   "vacant properties", "empty flats", "empty apartments")),
    ("umverteilen", ("redistribute", "redistribution")),
    ("sozialwohnung", ("social housing", "council housing", "public housing")),
    ("wohnungsmarkt", ("housing market", "property market")),
    ("markt", ("market",)),
    ("regelt", ("sort out", "sorts out", "regulate itself")),
    ("eigentumsquote", ("home ownership", "homeownership",
                        "home ownership rate")),
    ("eigenheim", ("own home", "buy a home", "buying a home")),
    ("genossenschaft", ("cooperative", "co operative", "housing cooperative")),
    ("löst", ("solves", "solve")),
    ("wohngeld", ("housing benefit", "housing allowance")),
    ("spekulant", ("speculators", "speculator", "speculation")),
    ("leerstandsabgabe", ("vacancy tax", "vacancy levy", "empty homes tax",
                          "empty home tax", "vacant property tax",
                          "vacant homes tax", "tax on empty homes",
                          "tax on vacant homes")),

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

def _grundformen(wort: str) -> tuple[str, ...]:
    """Kandidaten, unter denen ein Claim-Wort im Index stehen kann: das Wort
    selbst und die Form ohne englische Plural-/Genitiv-Endung. Geordnet —
    ein set haette eine prozessabhaengige Reihenfolge (PYTHONHASHSEED), und
    welcher Begriff im Text an Ort und Stelle steht, darf nicht davon
    abhaengen."""
    f = [wort]
    if wort.endswith("'s") or wort.endswith("s'"):
        f.append(wort[:-2])
    if wort.endswith("ies") and len(wort) > 4:
        f.append(wort[:-3] + "y")
    if wort.endswith("es") and len(wort) > 3:
        f.append(wort[:-2])
    if wort.endswith("s") and len(wort) > 2:
        f.append(wort[:-1])
    return tuple(dict.fromkeys(f))


def _endung_ok(wort: str, soll: str) -> bool:
    """``wort`` ist ``soll`` oder ``soll`` mit englischer Endung."""
    return soll in _grundformen(wort)


@lru_cache(maxsize=1)
def _index() -> tuple[dict[str, list[tuple[tuple[str, ...], str]]],
                      list[tuple[str, str]]]:
    """Erstes Wort der Variante -> [(Variante als Wortfolge, deutsch)],
    dazu die Wortanfangs-Varianten. Reihenfolge = Glossar-Reihenfolge."""
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


def _treffer(w: list[str]) -> list[tuple[int, int, str]]:
    """Alle Glossar-Treffer in der Wortfolge als (Start, Ende, deutsch)."""
    ganz, anfang = _index()
    out: list[tuple[int, int, str]] = []
    for i, wort in enumerate(w):
        for schluessel in _grundformen(wort):
            for teile, de_n in ganz.get(schluessel, ()):
                n = len(teile)
                if (i + n <= len(w) and w[i:i + n - 1] == list(teile[:-1])
                        and _endung_ok(w[i + n - 1], teile[-1])):
                    out.append((i, i + n, de_n))
        for stamm, de_n in anfang:
            if wort.startswith(stamm):
                out.append((i, i + 1, de_n))
    return list(dict.fromkeys(out))


def glossen(claim: str) -> tuple[str, ...]:
    """Die deutschen Trigger-Begriffe zu allen englischen Glossar-Begriffen
    im Claim, in Fundreihenfolge, ohne Dubletten."""
    return tuple(dict.fromkeys(de for _, _, de in _treffer(_woerter(claim))))


def vokabel_evidenz(claim: str) -> int:
    """Wie viele Glossar-Begriffe im Claim sind eindeutig englisch — also
    keiner der deutschen Homographen („die", „war", „man" …)?"""
    w = _woerter(claim)
    return len({(s, e) for s, e, _ in _treffer(w)
                if not all(x in _DE_HOMOGRAPHEN for x in w[s:e])})


def _glossiert(claim: str) -> tuple[str, tuple[tuple[int, int], ...]]:
    """(Vergleichstext, Zeichen-Spannen der englischen Restwoerter).

    An jeder Position gewinnt der LAENGSTE Treffer und ersetzt seine Woerter
    im Text — so bleibt die Nachbarschaft erhalten, auf die Mehrwort-Trigger
    angewiesen sind („unemployment rate in Vienna" -> „arbeitslosenquote
    wien", „10 percent" -> „10 prozent"). Andere Glossar-Eintraege fuer
    DIESELBE Wortspanne kommen hinten dazu („destroys" -> „zerstoert", dazu
    „vernichtet"). Teil-Treffer innerhalb einer laengeren Spanne NICHT: aus
    „student grants" wird „studienbeihilfe", nicht zusaetzlich „studenten" —
    ein deutscher Nutzer schriebe das auch nicht dazu.

    Englische Funktionswoerter fallen weg. Die uebrigen englischen Woerter
    bleiben (Eigennamen, Zahlen, Kognaten wie „Vorarlberg", „PISA", „2024"),
    ihre Spannen gehen mit: in ihnen darf ein Trigger nur als ganzes Wort
    treffen (siehe ``englisch_match``).

    Der Text ist bereits normalisiert und wird NICHT noch einmal durch
    ``normalisiere`` geschickt — sonst verschoeben sich die Spannen."""
    w = _woerter(claim)
    tr = _treffer(w)
    beste: dict[int, tuple[int, str]] = {}
    for s, e, de in tr:
        if s not in beste or e > beste[s][0]:
            beste[s] = (e, de)
    stuecke: list[tuple[str, bool]] = []          # (Text, ist englischer Rest)
    spannen_im_text: set[tuple[int, int]] = set()
    i = 0
    while i < len(w):
        if i in beste:
            e, de = beste[i]
            stuecke.append((de, False))
            spannen_im_text.add((i, e))
            i = e
            continue
        if w[i] not in _EN_ENTFERNEN:
            stuecke.append((w[i], True))
        i += 1
    im_text = {(s, beste[s][1]) for s, _ in spannen_im_text}
    weitere = tuple(dict.fromkeys(
        de for s, e, de in tr
        if (s, e) in spannen_im_text and (s, de) not in im_text))
    text = " "
    rest: list[tuple[int, int]] = []
    for stueck, ist_rest in stuecke:
        if ist_rest:
            rest.append((len(text), len(text) + len(stueck)))
        text += stueck + " "
    if weitere:
        text += "; " + " ; ".join(weitere) + " "
    return text, tuple(rest)


def englisch_gate(claim: str) -> bool:
    """Laeuft der englische Pass fuer diesen Claim?

    Ja bei positiver englischer Evidenz (``ist_englisch``). Zusaetzlich fuer
    Stichwort-Claims ohne ein einziges Funktionswort („Austrian unemployment
    rate 2024"): kein deutsches Funktionswort und mindestens
    ``VOKABEL_EVIDENZ_MIND`` eindeutig englische Glossar-Begriffe."""
    if ist_englisch(claim):
        return True
    return (sprachsignal(claim)[1] == 0
            and vokabel_evidenz(claim) >= VOKABEL_EVIDENZ_MIND)


@lru_cache(maxsize=512)
def _fassung(claim_lc: str) -> tuple[str, tuple[tuple[int, int], ...]] | None:
    if not claim_lc or not englisch_gate(claim_lc) or not glossen(claim_lc):
        return None
    return _glossiert(claim_lc)


def englische_fassung(claim_lc: str) -> str | None:
    """Der glossierte Vergleichstext fuer den englischen Pass — oder None,
    wenn der Claim nicht englisch ist oder kein Glossar-Begriff darin steht.

    Form: ``" <Claim, Glossen an Ort und Stelle> ; <weitere> ; … "``. Das
    Semikolon trennt, damit ein Mehrwort-Trigger nicht ueber zwei
    unverbundene Glossen hinweg trifft. Die Leerzeichen an den Raendern sind
    Absicht: Trigger wie „ ms " brauchen sie. Der Politik-Guard prueft
    diesen Text."""
    f = _fassung(claim_lc)
    return f[0] if f else None


def _vorkommen(text: str, term_n: str):
    """Alle (Start, Ende) von ``term_n`` in ``text`` — Substring und, bei
    Mehrwort-Begriffen, die flexionstolerante Form aus services/_flexion.py."""
    i = text.find(term_n)
    while i >= 0:
        yield i, i + len(term_n)
        i = text.find(term_n, i + 1)
    if " " in term_n.strip():
        for m in _flexion_muster(term_n.strip()).finditer(text):
            yield m.start(), m.end()


def _grenzen_ok(s: int, e: int, text: str,
                rest: tuple[tuple[int, int], ...]) -> bool:
    """Ein Vorkommen darf ein englisches Restwort nur GANZ abdecken: es
    beginnt nicht mitten im Wort und endet hoechstens vor einer englischen
    Endung. Sonst traefe „ass" (ASS, Aspirin) in „glass", „ai" in „brains",
    „rac" in „attracts" — Woerter, die ein deutscher Claim nie enthaelt."""
    for ws, we in rest:
        if e <= ws or s >= we:
            continue
        if s > ws:
            return False
        if e < we and text[e:we] not in ("s", "es", "'s"):
            return False
    return True


def _trifft_en(text: str, rest: tuple[tuple[int, int], ...], tok) -> bool:
    if not isinstance(tok, str):
        return False
    term_n = normalisiere(tok)
    if not term_n.strip():
        return False
    return any(_grenzen_ok(s, e, text, rest) for s, e in _vorkommen(text, term_n))


def englisch_match(item: dict, claim_lc: str) -> bool:
    """Wie ``substring_or_composite_match``, aber auf der glossierten
    Fassung und mit Wortgrenzen in den englischen Restwoertern.

    Dieselbe Logik — ``trigger_keywords`` any-of, ``trigger_composite`` und
    ``trigger_all`` AND-of-OR. In den deutschen Glossen bleibt der Vergleich
    ein Substring wie im Deutschen („mangel" in „fachkraeftemangel")."""
    f = _fassung(claim_lc)
    if f is None:
        return False
    text, rest = f

    def trifft(tok) -> bool:
        return _trifft_en(text, rest, tok)

    for kw in item.get("trigger_keywords") or ():
        if trifft(kw):
            return True
    composite = item.get("trigger_composite") or []
    if composite and all(
        isinstance(alt, (list, tuple)) and any(trifft(tok) for tok in alt)
        for alt in composite
    ):
        return True
    for rule in item.get("trigger_all") or ():
        if rule and all(
            isinstance(alt, (list, tuple)) and any(trifft(tok) for tok in alt)
            for alt in rule
        ):
            return True
    return False
