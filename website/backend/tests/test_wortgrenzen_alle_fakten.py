"""Wortgrenzen-Sweep ueber ALLE Fakten mit Trigger-Feldern — die Fortsetzung
von #207, das neun Fakten gebunden hat.

Gemessen am 2026-09-26 mit der echten ``find_matching_items``
(``descriptor_fn=None``) ueber die 2.603 dokumentierten
``claim_phrasings_handled`` von Fakten MIT Trigger-Feldern plus 1.163
Stress-Test-Claims (``tools/stress_tests``, ``stress_test_100``). Zuordnung je
Treffer per Ablation: derselbe Stand, nur dieses eine Token entfernt.

    #207 -> dieser Stand            vorher   nachher
    dienst-fremde Paare              406      326    (-80, 0 neu)
    Stress-Treffer (Claim x Fakt)  1.219    1.159    (-60, 0 neu)
    Muss-Treffer exakt             2.549    2.549    (0 verloren)

Jeder weggefallene Treffer ist einzeln angesehen und themenfremd. Der erste
Durchlauf verlor einen Muss-Treffer („BHs blockieren …" — `" bh "` traf den
Plural nicht); die Wortform `" bhs "` holt ihn zurueck.

VIER KLASSEN
============
1. **Token mitten in einem fremden Wort** (Korpus-belegt):

       "ertrag"  VERTRAG, uebERTRAGbar      "lden"  SchuLDEN
       "essen"/"abs"  ABSchliessen           "upts"  haUPTSaechlich
       "stem"    SySTEM                      "ph"    GlyPHosat, SmartPHone
       "kater"   MusKATER                    "berg"  BilderBERG, VorarlBERG
       "zen"     KatZEN, ProZENt             "erbe"  StERBEn, WERBEindustrie

2. **Nur hinten gebunden** — `"at "` trifft HAT/STAAT, `"eu "` NEU/TREU,
   `"ms "` SYSTEMS, `"rus-"` VIRUS. Das `"at "` stand in 36 Fakten; „Deutschland
   hat …" erfuellte damit ueberall die Oesterreich-Gruppe.
3. **Stichwort-Degeneration** — ein Token in beiden AND-Gruppen macht aus der
   Regel ein Stichwort: `"ertrag"` (Bio-Landbau traf „Friedensvertrag"),
   `"russland"`/`"sanktionen"` (Duengemittel traf jede Russland-Aussage),
   `"karriere"`, `"schnee"` („Schneeballsystem"), `"buendnis"`.
4. **Matcher**: der Flexions-Regex haengte an 1-2-Buchstaben-Woerter
   Endungen an — „e-auto" traf „EIN Auto", „h-2" traf „HAT 2024". Seit
   ``MIN_FLEKTIERBAR`` erst ab drei Buchstaben (services/_flexion.py).

Dazu die latenten Faelle (0 Korpus-Treffer, Sonden unten): Kurz-Tokens, deren
Wirt ein Alltagswort ist — „nso" in EBENSO, „eth" in METHODE, „ev" in OEVP,
„eier" in STEIERMARK, „wein" in SCHWEIN, „rand" in BRANDgefahr.

Methodik wie #205/#206/#207: erst messen, Muss-Treffer-Kontrolle, jeder Gate
schlaegt nachweislich an (Gift-Proben). Gebunden wird mit Rand-Leerzeichen
und Wortformen, nie durch blindes Loeschen; wo ein Token aus einer Gruppe
fiel (Degeneration), traegt eine praezisere Regel die belegten Phrasings.

Dependency-light: reine Trigger-Tests, kein Netz/LLM.
"""
import copy
import glob
import json
import os
import re

import pytest

from services._flexion import MIN_FLEKTIERBAR, trifft
from services._schreibweise import normalisiere
from services._tippfehler import tippfehler_match
from services._topic_match import find_matching_items, substring_or_composite_match

BACKEND = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(BACKEND, "data")
TF = ("trigger_keywords", "trigger_composite", "trigger_all")


def _fid(it: dict) -> str:
    return it.get("id") or it.get("topic") or ""


def _laden():
    """{fakt_id: (datei, schluessel)} und [(datei, item)] aller Trigger-Fakten."""
    wo, alle = {}, []
    for p in sorted(glob.glob(os.path.join(DATA, "*.json"))):
        with open(p, encoding="utf-8") as fh:
            d = json.load(fh)
        if not isinstance(d, dict):
            continue
        for k, v in d.items():
            if not isinstance(v, list):
                continue
            for it in v:
                if isinstance(it, dict) and any(it.get(t) for t in TF):
                    wo.setdefault(_fid(it), (os.path.basename(p), k))
                    alle.append((os.path.basename(p), it))
    return wo, alle


WO, ALLE = _laden()
FAKT = {_fid(it): it for _, it in ALLE}


def _trifft_echt(fid: str, claim: str) -> bool:
    """Die echte Pipeline-Funktion: exakter Pass, dann tippfehler-tolerant."""
    datei, key = WO[fid]
    treffer = find_matching_items(os.path.join(DATA, datei), key,
                                  claim_lc=claim.lower(), full_claim=claim,
                                  descriptor_fn=None)
    return any(fid in (x.get("id"), x.get("topic")) for x in treffer)


def _gruppen(it: dict):
    """(label, liste) je Trigger-Gruppe; Regeln als Liste von Gruppen."""
    if it.get("trigger_keywords"):
        yield "kw", [it["trigger_keywords"]]
    if it.get("trigger_composite"):
        yield "c", it["trigger_composite"]
    for r, regel in enumerate(it.get("trigger_all") or ()):
        yield f"a{r}", regel


def _tokens(it: dict):
    for _, regel in _gruppen(it):
        for g in regel:
            yield from (t for t in g if isinstance(t, str))


# ---------------------------------------------------------------------------
# 1. Beide Richtungen: Leck zu, eigenes Thema trifft weiter
# ---------------------------------------------------------------------------

LECKE_KORPUS = [
    # Jede Zeile traf auf #207 (dokumentierte Phrasing oder Stress-Claim).
    ("bio_landbau_ertrags_oekobilanz_2026", "Es fehlt ein Friedensvertrag"),
    ("bio_landbau_ertrags_oekobilanz_2026", "Wien-Modell ist überall übertragbar"),
    ("bio_landbau_ertrags_oekobilanz_2026", "Antibiotika wirken nicht mehr"),
    ("bio_pestizidfrei_2026", "Biochemie nach Schüßler funktioniert"),
    ("environmental_noise_2026", "Schuldenbremse ist alternativlos"),
    ("abs_kitchen_2026", "Asylsuchende müssen Wertekurs absolvieren"),
    ("partg_2012_rechtsgrundlage", "Wien ist die Hauptstadt Österreichs."),
    ("erbrecht_pflichtteil_2026", "Zeugen Jehovas dürfen ihre Kinder sterben lassen."),
    ("gvo_gentechnik_risiko_2026",
     "Tempo 30 in Wohngebieten bringt nichts für die Sicherheit."),
    ("saeure_basen_krebs_mythos", "Glyphosat verursacht Krebs."),
    ("eb_eu_vertrauen_2024", "Bevölkerung Deutschlands sinkt"),
    ("eb_eu_mitgliedschaft_2024", "B1-Deutsch ist Voraussetzung für Einbürgerung"),
    ("eb_top_themen_2024",
     "Der Familiennachzug-Stopp in Österreich ist EU-rechtswidrig"),
    ("enisa_threat_landscape_2026", "Schlechtes Deutsch verrät Phishing-Versuche"),
    ("katze_milch_mythos", "Muskelkater entsteht durch Milchsäure"),
    ("awmf_multiple_sklerose",
     "Die AWMF publiziert evidenzbasierte medizinische Leitlinien in Deutschland."),
    ("at_wm_halbfinale_1954",
     "Die AWMF publiziert evidenzbasierte medizinische Leitlinien in Deutschland."),
    ("everest_hoechster_2026", "Bilderberg-Konferenz ist eine geheime Weltregierung"),
    ("buddhismus_friedlich_2026", "Katzen landen immer auf den Pfoten"),
    ("glyphosat_empirie_2026", "Sprachbonus Mindestsicherung"),
    ("glyphosat_empirie_2026", "Mistelpräparate stoppen Tumor"),
    ("mint_frauen_anteil_2026", "US-Wahlsystem ist undemokratisch"),
    ("alkohol_rotwein_herz_mythos", "Gesundheitssystem kollabiert"),
    ("menstruation_synchronisation_mythos",
     "Elektroautos sind über ihren Lebenszyklus klimaschädlicher als Verbrenner."),
    ("acht_glaeser_wasser_mythos",
     "Wasserstoff-Auto verbraucht weniger Strom als E-Auto"),
    ("gen_z_iphone_depression_mythos",
     "Wenn mein Hund jemanden beißt, hafte ich nur bei eigenem Verschulden."),
    ("e_auto_akku_co2_2026",
     "Klimaschutz wird in der Bevölkerung mehrheitlich abgelehnt."),
    ("ams_at_zeitreihe_2015_2024",
     "Das BVerwG hat 2020 entschieden, dass Deutschland Mit-Verantwortung "
     "für US-Drohnen-Strikes über Ramstein trägt."),
    ("ams_at_quote_2024_gesamt",
     "Spanien hat eine niedrigere Jugendarbeitslosigkeit als Deutschland."),
    ("at_strom_eckdaten_2024", "Deutschland hat 2024 mehr Strom aus Erneuerbaren "
                               "als aus fossilen Quellen erzeugt."),
    ("oenb_wohnpreise_2024", "Deutschland hat höchste Wohnkosten der Welt."),
    ("wahlbeteiligung_at_trends_2026",
     "Die ÖVP wird die nächste Nationalratswahl gewinnen."),
    ("mobilitaet-wasserstoff_pkw_2026",
     "Tesla hat 2024 mehr Elektroautos verkauft als BYD."),
    ("winterreifen_pflicht_konsens", "Bitcoin funktioniert wie ein Schneeballsystem"),
    ("duengemittel_importabhaengigkeit_2026", "Sanktionen wirken nicht"),
    ("duengemittel_importabhaengigkeit_2026", "Russland hat keine Pressefreiheit"),
    ("vereinbarkeit_familie_beruf_2026",
     "Hochbegabte Kinder erreichen automatisch hohe akademische Karrieren."),
]

LECKE_LATENT = [
    # 0 Korpus-Treffer; jede Sonde traf auf #207 (gemessen) — der Wirt ist
    # ein Alltagswort, die Nachbargruppen sind allgemein genug.
    ("mint_frauen_anteil_2026", "Das Gesundheitssystem benachteiligt Frauen."),
    ("korruption_index_2026", "Kurzfristig steigt die Arbeitslosigkeit in Österreich."),
    ("eier_cholesterin_2026", "Die Steiermark hat ein Herz für Touristen."),
    ("alkohol_rotwein_herz_mythos", "Schweinefleisch ist schlecht für das Herz."),
    ("gvo_gentechnik_risiko_2026", "Die DSGVO bringt keine Sicherheit."),
    ("pegasus_spyware_konsens_2026", "Ebenso werden Journalisten in Ungarn verfolgt."),
    ("krypto_pyramide_2026", "Diese Methode ist Betrug."),
    ("glyphosat_empirie_2026", "Der Wirkmechanismus von Paracetamol ist harmlos."),
    ("glyphosat_empirie_2026", "Die Sprache der Politik ist harmlos."),
    ("glyphosat_empirie_2026", "Das Patriarchat ist nicht harmlos."),
    ("massentierhaltung_empirie_2026", "Obama war ein guter Präsident."),
    ("massentierhaltung_empirie_2026", "Tagesmütter leisten gute Arbeit."),
    ("pestizid_rueckstaende_2026", "Tageszeitungen sind kaum noch rentabel."),
    ("mobilitaet-e_auto_reichweite_2026", "Die ÖVP will im Winter die Heizkosten senken."),
    ("mobilitaet-oebb_puenktlichkeit_2026", "Der Kundenservice der Post ist unzuverlässig."),
    ("hund_haftung_2026", "Seit Jahrhunderten tragen Eltern die Verantwortung."),
    ("hund_schokolade_toxizitaet_konsens", "Im 19. Jahrhundert war Zucker gefährlich."),
    ("destruktive_kulte_kennzeichen_2026", "Es gibt keine österreichische Kultur."),
    ("stier_rote_farbe_2026", "Existiert der rote Planet wirklich?"),
    ("mozart_effekt_2026", "Beobachtungen zeigen, dass Kinder schneller lernen."),
    ("at_strom_eckdaten_2024",
     "Der Anteil der Geschwindigkeitsübertretungen in Österreich steigt."),
    ("at_strom_eckdaten_2024", "Frankreich hat den höchsten Anteil an Atomstrom."),
    ("zinseszins_2026", "Herzinsuffizienz ist ein unterschätzter Effekt des Alterns."),
    ("zinseszins_2026", "Transparenz ist ein Mythos."),
    ("winterreifen_pflicht_konsens", "Die meisten Fahrzeuge sind ausreichend versichert."),
    ("mac_keine_viren_mythos", "Was macht ein Virus im Körper?"),
    ("bh_brustkrebs_mythos", "Krebs ist nicht vom Lebensstil abhängig."),
    ("bh_brustkrebs_mythos", "Verbraucher zahlen für Krebs-Medikamente zu viel."),
    ("deo_brustkrebs_mythos", "Videospiele verursachen Krebs."),
    ("aids_cia_labor_2026", "Das Archiv der US-Regierung ist öffentlich."),
    ("saeure_basen_krebs_mythos", "Das Smartphone verursacht Krebs."),
    ("duengemittel_importabhaengigkeit_2026",
     "Kalifornien verdreifacht die Preise für Wasser."),
    ("awmf_nvl_khk", "Die Geldleistung für Familien ist zu niedrig."),
    ("erbrecht_pflichtteil_2026", "Die Werbeindustrie hat Kinder als Zielgruppe."),
    ("everest_hoechster_2026", "Vorarlberg hat die höchsten Mieten."),
    ("ams_at_branche_sektor_2024",
     "Der Ausbau der Kinderbetreuung senkt die Arbeitslosigkeit in Österreich."),
    ("ams_at_branche_sektor_2024", "Arbeitslose werden in Österreich schlecht behandelt."),
    ("familienbeihilfe_wirkung_2026",
     "Der Aufbau neuer Kitas bringt nichts für die Geburtenrate."),
    ("awmf_chronische_niereninsuffizienz", "Die Lockdown-Empfehlung war falsch."),
    ("hai_angriff_2026", "Thailands Armee plant einen Angriff."),
    ("buddhismus_friedlich_2026", "90 Prozent der Proteste waren friedlich."),
    ("rki_atemwegsinfekte_saisonvergleich",
     "Das Szenario einer Rezession wird immer schlimmer."),
    ("vatikan_vermoegen_2026", "Senioren haben das größte Vermögen."),
    ("promille_grenzen_alkohol_kontext",
     "Tabakkonsum im Auto ist beim Fahren gefährlich und sicher nicht erlaubt."),
    ("autismus_therapie_wirkung_2026", "Tabak schadet der Gesundheit."),
    ("2fa_unnoetig_mythos", "Der Umfang der Reform ist unnötig."),
    ("russland_bedrohungs_empirie_2026", "Die Brandgefahr ist im Sommer ein Risiko."),
    ("russland_bedrohungs_empirie_2026", "Das Coronavirus ist eine Bedrohung."),
    ("theologische_aussagen_kategorie_2026", "Beim Arbeitslosengeld gibt es Mängel."),
    ("auftauen_zimmertemperatur_mythos", "Litauen ist ein sicheres Reiseland."),
    ("nato_mitgliedstaaten_at_diskurs_2026", "Der Senator will den Beitritt verhindern."),
    ("krim_donbas_ukraine_voelkerrecht_2026",
     "Diskriminierung am Arbeitsplatz ist illegal."),
    ("bahn_vs_auto_sicherheit_konsens", "Die Autobahn ist sicher."),
    ("erbschaftssteuer_mittelstand_2026",
     "In Österreich sterben immer mehr Familienunternehmen."),
    ("effektive_steuerlast_reiche_mittelschicht_2026",
     "Die Steuereinnahmen erreichen weniger als erwartet."),
    ("frueh_lesen_lernen_smartphone_mythos",
     "Im Kanton Zürich lernen Kinder früher lesen."),
    ("aktien_gluecksspiel_2026", "Der Integrationsfonds hilft normalen Bürgern."),
    ("krypto_promi_endorsement_mythos", "Der Kompromiss wurde geheim verhandelt."),
    ("krypto_promi_endorsement_mythos", "Muskelaufbau passiert automatisch."),
    ("eb_top_themen_2024", "Das Thema ist neu und wichtig."),
    ("awmf_multiple_sklerose", "Die Leitlinie des Systems ist veraltet."),
    ("apple_android_2026", "Radios sind sicherer als Smartphones."),
    # Stichwort-Degeneration
    ("at_neutralitaet_recht_2026",
     "Bündnis 90/Die Grünen sind gegen den EU-Beitritt der Türkei."),
    ("at_neutralitaet_recht_2026",
     "Das Bündnis Sahra Wagenknecht lehnt den EU-Beitritt der Ukraine ab."),
    ("vereinbarkeit_familie_beruf_2026", "Messi beendet seine Karriere."),
    ("duengemittel_importabhaengigkeit_2026",
     "Russland führt einen Krieg gegen die Ukraine."),
    ("bio_landbau_ertrags_oekobilanz_2026",
     "Kapitalerträge sollten höher besteuert werden."),
    # Matcher: keine Endungen an 1-2-Buchstaben-Woerter
    ("mobilitaet-e_auto_reichweite_2026", "Ich kaufe ein Auto mit großer Reichweite."),
    ("eu_militaerfonds_pesco_2026", "Eure Armee ist schwach."),
]

EIGENE_THEMEN = [
    # Die gebundenen Tokens treffen ihr Thema weiter — am Claim-Anfang, vor
    # Satzzeichen, im Bindestrich-Kompositum, im Plural.
    ("at_neutralitaet_recht_2026", "Österreich ist in keinem Bündnis."),
    ("at_neutralitaet_recht_2026", "Österreich ist bündnisfrei."),
    ("at_neutralitaet_recht_2026", "Österreich darf keinen Bündnissen beitreten."),
    ("at_neutralitaet_recht_2026", "Die Bündnisfreiheit ist in der Verfassung verankert."),
    ("duengemittel_importabhaengigkeit_2026", "Dünger aus Russland wird teurer."),
    ("duengemittel_importabhaengigkeit_2026", "Sanktionen treffen die EU-Bauern."),
    ("duengemittel_importabhaengigkeit_2026", "EU diversifiziert weg von Russland."),
    ("duengemittel_importabhaengigkeit_2026", "Kali-Dünger aus Belarus ist sanktioniert."),
    ("bio_landbau_ertrags_oekobilanz_2026", "Die Erträge im Ökolandbau sind geringer."),
    ("bio_landbau_ertrags_oekobilanz_2026", "Biobauern brauchen mehr Fläche."),
    ("bio_pestizidfrei_2026", "Bioprodukte sind frei von Pestiziden."),
    ("vereinbarkeit_familie_beruf_2026",
     "Mütter machen wegen der Kinder weniger Karriere."),
    ("winterreifen_pflicht_konsens",
     "Bei Schnee und Glatteis ist die Winterreifen-Pflicht Unsinn."),
    ("winterreifen_pflicht_konsens",
     "Auf Eis reichen Sommerreifen, wenn man vorsichtig ist."),
    ("hund_haftung_2026", "Hundehalter haften für jeden Biss."),
    ("hund_schokolade_toxizitaet_konsens", "Schokolade ist für Hunde giftig."),
    ("stier_rote_farbe_2026", "Stiere werden bei Rot aggressiv."),
    ("stier_rote_farbe_2026", "Bulls hate the color red."),
    ("mozart_effekt_2026", "Bachs Musik macht Babys schlau."),
    ("zinseszins_2026", "Der Zinseszins-Effekt ist ein Mythos."),
    ("zinseszins_2026", "Sparen lohnt sich wegen des Zinseszins-Effekts nicht."),
    ("eier_cholesterin_2026", "Eier sind schlecht für das Herz."),
    ("alkohol_rotwein_herz_mythos", "Ein Glas Wein am Tag schützt das Herz."),
    ("mint_frauen_anteil_2026", "In STEM-Fächern gibt es wenige Frauen."),
    ("korruption_index_2026", "Unter Kurz ist die Korruption in Österreich gestiegen."),
    ("mobilitaet-e_auto_reichweite_2026", "EVs haben im Winter weniger Reichweite."),
    ("mobilitaet-e_auto_reichweite_2026", "Das E-Auto verliert im Winter Reichweite."),
    ("mobilitaet-oebb_puenktlichkeit_2026", "Der ICE ist unpünktlicher als der Railjet."),
    ("mac_keine_viren_mythos", "Macs bekommen keine Viren."),
    ("bh_brustkrebs_mythos", "BHs verursachen Brustkrebs."),
    ("deo_brustkrebs_mythos", "Deos mit Aluminium verursachen Krebs."),
    ("aids_cia_labor_2026", "Die CIA hat HIV im Labor entwickelt."),
    ("saeure_basen_krebs_mythos", "Ein basischer pH-Wert heilt Krebs."),
    ("awmf_nvl_khk", "LDL-Cholesterin-Senkung ist Standard bei KHK."),
    ("erbrecht_pflichtteil_2026", "Man kann Kinder vollständig vom Erbe ausschließen."),
    ("everest_hoechster_2026", "Der Everest ist der höchste Berg der Welt."),
    ("ams_at_branche_sektor_2024", "Im Bau steigt die Arbeitslosigkeit in Österreich."),
    ("ams_at_branche_sektor_2024",
     "Im Einzelhandel steigt die Arbeitslosigkeit in Österreich."),
    ("ams_at_quote_2024_gesamt", "Die AMS-Quote in Österreich liegt bei 7 %."),
    ("eb_eu_vertrauen_2024", "Das Vertrauen in die EU sinkt."),
    ("eb_eu_mitgliedschaft_2024", "Österreich soll raus aus der EU."),
    ("at_strom_eckdaten_2024", "AT importiert 2024 mehr Strom."),
    ("russland_bedrohungs_empirie_2026", "RU-Hacker sind eine Bedrohung für Österreich."),
    ("russland_bedrohungs_empirie_2026", "Laut RAND ist Russland eine Bedrohung."),
    ("awmf_multiple_sklerose", "Bei MS ist Interferon Standard."),
    ("apple_android_2026", "iOS ist sicherer als Android."),
    ("gvo_gentechnik_risiko_2026", "GVO-Lebensmittel sind gefährlich."),
    ("pegasus_spyware_konsens_2026", "Die NSO-Software Pegasus überwacht Journalisten."),
    ("krypto_pyramide_2026", "ETH ist ein Schneeballsystem."),
    ("hai_angriff_2026", "Haie sind für Schwimmer gefährlich."),
    ("buddhismus_friedlich_2026", "Zen-Buddhismus ist immer friedlich."),
    ("destruktive_kulte_kennzeichen_2026", "Es gibt keine Kulte mehr in Österreich."),
    ("theologische_aussagen_kategorie_2026", "Engel existieren."),
    ("auftauen_zimmertemperatur_mythos",
     "Gefrorenes bei Zimmertemperatur tauen lassen ist sicher."),
    ("nato_mitgliedstaaten_at_diskurs_2026", "Österreich könnte einfach der NATO beitreten."),
    ("sky_shield_beitritts_diskurs_2026", "ESSI ist neutralitätskonform."),
    ("krim_donbas_ukraine_voelkerrecht_2026", "Die Annexion der Krim war illegal."),
    ("bahn_vs_auto_sicherheit_konsens", "Die Bahn ist sicherer als das Auto."),
    ("erbschaftssteuer_mittelstand_2026", "Erben zahlen die Hälfte an den Staat."),
    ("effektive_steuerlast_reiche_mittelschicht_2026",
     "Reiche zahlen weniger Steuern als die Mittelschicht."),
    ("aktien_gluecksspiel_2026", "Indexfonds sind reines Glücksspiel."),
    ("krypto_promi_endorsement_mythos", "Elon Musk empfiehlt diese Krypto-Plattform."),
    ("krypto_promi_endorsement_mythos",
     "Promis werben für Bitcoin und versprechen, schnell reich zu werden."),
]


@pytest.mark.parametrize("fid,claim", LECKE_KORPUS + LECKE_LATENT)
def test_leck_geschlossen(fid, claim):
    assert not _trifft_echt(fid, claim), f"{fid} trifft den themenfremden Claim {claim!r}"


@pytest.mark.parametrize("fid,claim", EIGENE_THEMEN)
def test_eigenes_thema_trifft(fid, claim):
    assert _trifft_echt(fid, claim), f"{fid} verliert {claim!r}"


# ---------------------------------------------------------------------------
# 2. Muss-Treffer-Kontrolle
# ---------------------------------------------------------------------------

# Die angefassten Fakten: blanke Tokens, die dort nicht mehr stehen duerfen.
# Gebunden heisst `" x "` (Kuerzel) oder `" x"` (Wortanfang; Endungen und
# Komposita hinten bleiben: „Hundehalter", „Biolebensmittel").
GEBUNDEN = {
    "2fa_unnoetig_mythos": ("mfa",),
    "abs_kitchen_2026": ("abs",),
    "acht_glaeser_wasser_mythos": ("braucht",),
    "adhs_ueberdiagnose_mythos": ("ads",),
    "agrar_subventionen_at_2026": ("gap",),
    "aids_cia_labor_2026": ("hiv", "cia"),
    "aktien_gluecksspiel_2026": ("fonds", "etf"),
    "alkohol_rotwein_herz_mythos": ("bier", "wein"),
    "ams_at_branche_sektor_2024": ("bau", " at", "handel"),
    "at_energie_importabhaengigkeit_2026": ("omv",),
    "at_strom_eckdaten_2024": ("wind",),
    "at_wm_halbfinale_1954": ("wm", "nie", "kam"),
    "auftauen_zimmertemperatur_mythos": ("tauen",),
    "autismus_therapie_wirkung_2026": ("aba",),
    "awmf_adipositas": ("dag",),
    "awmf_chronische_niereninsuffizienz": ("ckd",),
    "awmf_hashimoto_thyreoiditis": ("tsh",),
    "awmf_multiple_sklerose": ("ed",),
    "awmf_prostatakarzinom": ("dgu",),
    "awmf_schlaganfall_akut": ("dsg",),
    "bahn_vs_auto_sicherheit_konsens": ("bahn",),
    "bh_brustkrebs_mythos": ("bh", "bra"),
    "bio_landbau_ertrags_oekobilanz_2026": ("bio", "ha", "vs"),
    "bio_pestizidfrei_2026": ("bio", "chemie"),
    "buddhismus_friedlich_2026": ("zen",),
    "cost_of_inaction_2026": ("usd",),
    "demokratie_verfall_empirie_2026": ("usa",),
    "deo_brustkrebs_mythos": ("deo",),
    "destatis_geburten_2024": ("tfr",),
    "destruktive_kulte_kennzeichen_2026": ("kult",),
    "diesel_skandal_2026": ("nox",),
    "duengemittel_importabhaengigkeit_2026": ("oci", "kali"),
    "e_auto_akku_co2_2026": ("bev",),
    "eb_eu_mitgliedschaft_2024": ("raus",),
    "eb_top_themen_2024": ("top",),
    "effektive_steuerlast_reiche_mittelschicht_2026": ("reiche", "reichen"),
    "environmental_noise_2026": ("lden",),
    "erbrecht_pflichtteil_2026": ("erbe",),
    "erbschaftssteuer_mittelstand_2026": ("erben",),
    "erneuerbare_versorgungssicher_2026": ("wind",),
    "eter_uni_graz_2021": ("uni",),
    "eter_uni_innsbruck_2021": ("uni",),
    "eter_uni_wien_2021": ("uni",),
    "eu_militaerfonds_pesco_2026": ("epf",),
    "everest_hoechster_2026": ("berg", "berge"),
    "familienbeihilfe_wirkung_2026": ("fb",),
    "fledermaus_blind_2026": ("bat",),
    "frueh_lesen_lernen_smartphone_mythos": ("anton",),
    "gen_z_iphone_depression_mythos": ("igen",),
    "glyphosat_empirie_2026": ("rac", "echa", "iarc"),
    "gvo_gentechnik_risiko_2026": ("ngt", "gvo", "gmo"),
    "hai_angriff_2026": ("hai",),
    "hochsensibilitaet_hochbegabung_mythen_2026": ("hsp",),
    "hund_ein_jahr_sieben_mensch_jahre_mythos": ("hund", "hunde"),
    "hund_haftung_2026": ("hund", "hunde"),
    "hund_schokolade_toxizitaet_konsens": ("hund", "hunde"),
    "icf_vs_medizinisches_modell_2026": ("engel",),
    "inklusion_schule_vs_sonderschule_2026": ("easi",),
    "katze_milch_mythos": ("kater",),
    "ki_bewusstsein_2026": ("llm",),
    "korruption_index_2026": ("kurz",),
    "krim_donbas_ukraine_voelkerrecht_2026": ("krim",),
    "krypto_promi_endorsement_mythos": ("promi", "musk", "beck"),
    "kuehlschrank_eier_lagerung_konsens": ("egg",),
    "lernstile_2026": ("vak",),
    "mac_keine_viren_mythos": ("mac",),
    "massentierhaltung_empirie_2026": ("bio", "ama", "ages"),
    "menstruation_synchronisation_mythos": ("zyklus", "wg"),
    "mikrodosing_lsd_psilocybin_mythos": ("lsd",),
    "mint_frauen_anteil_2026": ("stem",),
    "mobilitaet-e_auto_reichweite_2026": ("ev",),
    "mobilitaet-oebb_puenktlichkeit_2026": ("ice",),
    "mozart_effekt_2026": ("bach",),
    "nato_mitgliedstaaten_at_diskurs_2026": ("nato",),
    "nbb_2024_spf_bestand_at": ("spf",),
    "oeif_familiennachzug_at_2023": ("bfa",),
    "omega_3_supplemente_konsens_2026": ("dha",),
    "partg_2012_rechtsgrundlage": ("upts",),
    "pegasus_spyware_konsens_2026": ("nso",),
    "pestizid_rueckstaende_2026": ("bio", "ages"),
    "pew_demokratie_2024": ("pew",),
    "promille_grenzen_alkohol_kontext": ("bak",),
    "regionale_versorgung_mythen_2026": ("wein",),
    "rki_atemwegsinfekte_saisonvergleich": ("ari", "rsv"),
    "russland_bedrohungs_empirie_2026": ("rand",),
    "russland_sanktionen_wirkung_2026": ("crea",),
    "saeure_basen_krebs_mythos": ("ph",),
    "sky_shield_beitritts_diskurs_2026": ("essi",),
    "smartmeter_datenschutz_konsens_2026": ("bsi",),
    "snowden_nsa_faktoide_konsens_2026": ("nsa",),
    "stier_rote_farbe_2026": ("stier", "bull"),
    "stillen_verhuetung_mythos": ("lam",),
    "subventions_kriege_2026": ("ira",),
    "theologische_aussagen_kategorie_2026": ("engel",),
    "vatikan_vermoegen_2026": ("ior",),
    "winterreifen_pflicht_konsens": ("eis",),
    "zinseszins_2026": ("zins", "sparen"),
}

# Nur-hinten-gebunden und Kurz-Kuerzel, die in KEINEM Fakt blank stehen
# duerfen: `"at "` trifft HAT, `"eu-"` normalisiert zu `"eu "` und trifft NEU.
UEBERALL_GEBUNDEN = (
    "at ", "at-", "at", "eu-", "eu ", "eu", "ams", "ms ", "ms-", "ru-", "rus-",
    "ios ", "ios", "lese-", "lese ", "gts ", "perso ", "wok ", "ldl", "arb",
    "alg", "fed", "eth", "epa", "ilo", "lfs", "fbi", "db", "uba", "eier",
)
_GEBUNDENE_FORMEN = {" " + normalisiere(t).strip() + " " for t in UEBERALL_GEBUNDEN}

ANGEFASST = sorted(set(GEBUNDEN) | {
    "at_neutralitaet_recht_2026", "vereinbarkeit_familie_beruf_2026",
} | {f for f, it in FAKT.items()
     if any(normalisiere(t) in _GEBUNDENE_FORMEN for t in _tokens(it))})

# Dokumentierte Phrasings angefasster Fakten, die schon auf #207 nicht exakt
# trafen — nicht dieser Fix (0 Muss-Treffer verloren, gemessen). Die meisten
# Dienste darunter haben ein eigenes Praedikat neben dem Pack-Matcher.
OHNE_EXAKTEN_TREFFER_SCHON_VORHER = {
    "Warum unterschiedliche Arbeitslosenzahlen",
    "Wie lange sind Arbeitslose in Österreich arbeitslos",
    "Arbeitskräftepotenzial Österreich",
    "Arbeitsmarkt-Krise AT",
    "Wann war die höchste Arbeitslosigkeit in Österreich",
    "Quelle AMS Daten",
    "Wie aktuell sind AMS-Statistiken",
    "Blutdruck-Ziel sollte unter 130/80 sein",
    "Salzreduktion senkt den Blutdruck nachweislich",
    "SGLT-2-Hemmer wirken renoprotektiv bei diabetischer Nierenerkrankung",
    "Subklinische Hypothyreose erfordert nicht immer L-Thyroxin",
    "Schüler in Österreich 2024",
    "Bildungs-Krise PISA",
    "Wie viele Lehrer in Österreich",
    "Lehrkräfte Bedarf AT",
    "Wie viel gibt Österreich für Bildung aus",
    "Österreich gibt 5% des BIP für Bildung aus",
    "AT zu wenig für Bildung",
    "Ganztagsschule Anteil Wien",
    "Wie groß sind Klassen in AT",
    "Klassen DACH-Vergleich",
    "Migrationshintergrund schlechtere PISA",
    "Schüler mit Migrationshintergrund Bildung",
    "Bildungsexpansion Österreich",
    "Inklusion Schule Österreich",
    "Sonderschüler AT Anzahl",
    "WHO empfiehlt 45 dB nachts",
    "WHO-Schaetzung Hoer-Kosten",
}


def test_angefasst_ist_vollstaendig():
    """Sonst prueft die Muss-Kontrolle ins Leere — angefasst wurden 166 Fakten
    (alle mit `" at "`, `" eu "`, `" ams "` … zaehlen mit, auch wenn das
    gebundene Token schon vorher dort stand)."""
    assert len(ANGEFASST) >= 166


@pytest.mark.parametrize("fid", ANGEFASST)
def test_dokumentierte_phrasings_treffen_exakt(fid):
    """Jede dokumentierte Phrasing trifft ihren Fakt im EXAKTEN Pass."""
    it = FAKT[fid]
    for ph in it.get("claim_phrasings_handled") or []:
        if ph in OHNE_EXAKTEN_TREFFER_SCHON_VORHER:
            continue
        assert substring_or_composite_match(it, ph.lower()), (
            f"{fid} verliert die dokumentierte Phrasing {ph!r}")


# ---------------------------------------------------------------------------
# 3. Matcher: Endungen erst ab drei Buchstaben
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("claim,tok,soll", [
    ("Ich kaufe ein Auto", "e-auto", False),
    ("Das E-Auto ist teuer", "e-auto", True),
    ("Tesla hat 2024 mehr verkauft", "h-2", False),
    ("H-2 im Tank", "h-2", True),
    ("Der Bundesrat beschließt", "de-bundesrat", False),
    ("Eure Armee ist schwach", "eu armee", False),
    ("Die EU-Armee kommt", "eu armee", True),
    # ab drei Buchstaben unveraendert: das Adjektiv flektiert vorne
    ("In Nordkorea gibt es keine freien Wahlen", "freie wahlen", True),
    ("Neue Wahlen im Herbst", "neu wahlen", True),
])
def test_endungen_erst_ab_drei_buchstaben(claim, tok, soll):
    assert MIN_FLEKTIERBAR == 3
    assert trifft(normalisiere(claim.lower()), tok) is soll


# ---------------------------------------------------------------------------
# 4. Contract — und jedes Gate schlaegt nachweislich an
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("fid", sorted(GEBUNDEN))
def test_kurz_tokens_nur_gebunden(fid):
    tokens = set(_tokens(FAKT[fid]))
    for tok in GEBUNDEN[fid]:
        assert tok not in tokens, (
            f"{fid}: blankes Token {tok!r} — trifft in fremden Woertern")


def test_ueberall_gebunden():
    for _, it in ALLE:
        blank = set(_tokens(it)) & set(UEBERALL_GEBUNDEN)
        assert not blank, f"{_fid(it)}: {sorted(blank)} nur halb oder gar nicht gebunden"


# --- 4a. Nur hinten gebunden -------------------------------------------------

# Bewusst offen: die Wirte sind am Thema (GEHÖR, ANSTIEG).
NUR_HINTEN_ERLAUBT = {"hoer ", "hoer-", "stieg "}


def _nur_hinten_gebunden(alle) -> list[tuple[str, str]]:
    """Einwort-Tokens bis fuenf Buchstaben, deren Grenze nur HINTEN steht —
    roh `"at "` oder `"eu-"` (normalisiert `"eu "`). Sie treffen jedes Wort
    mit dieser Endung: HAT, STAAT, NEU, TREU, SYSTEMS, VIRUS."""
    out = []
    for _, it in alle:
        for t in _tokens(it):
            n = normalisiere(t)
            if n.endswith(" ") and not n.startswith(" ") and " " not in n.strip() \
                    and len(n.strip()) <= 5 and t not in NUR_HINTEN_ERLAUBT:
                out.append((_fid(it), t))
    return out


def test_kein_nur_hinten_gebundenes_kurz_token():
    assert not _nur_hinten_gebunden(ALLE)


def test_gate_nur_hinten_schlaegt_an():
    it = copy.deepcopy(FAKT["at_strom_eckdaten_2024"])
    it["trigger_composite"][1].append("at ")
    assert ("at_strom_eckdaten_2024", "at ") in _nur_hinten_gebunden([("x", it)])


# --- 4b. Stichwort-Degeneration ----------------------------------------------

# Ein Token in JEDER AND-Gruppe einer Regel macht die Regel zum Stichwort.
# Erlaubt nur, wo das Token selbst das Thema IST.
STICHWORT_ERLAUBT = {
    ("awmf_nvl_khk", " ldl "),
    ("wahlfaelschungs_mythen_2026", "biden"),
    ("eb_eu_mitgliedschaft_2024", "brexit"),
    ("eb_eu_mitgliedschaft_2024", "dexit"),
    ("un_brk_konvention_2026", "staatenbericht"),
    ("barrierefreiheit_empirie_at_2026", "barrierefrei"),
    ("massentierhaltung_empirie_2026", "freiland"),
    ("screening_uebertherapie_grundprinzip", "ueberdiagnos"),
    ("screening_uebertherapie_grundprinzip", "uebertherapie"),
    ("mammographie_screening", "brustkrebs"),
}


def _stichwort_degeneration(alle) -> list[tuple[str, str]]:
    out = []
    for _, it in alle:
        for lab, regel in _gruppen(it):
            if lab == "kw" or len(regel) < 2:
                continue
            mengen = [{normalisiere(t) for t in g if isinstance(t, str)} for g in regel]
            for tok in sorted(set.intersection(*mengen)):
                if (_fid(it), tok) not in STICHWORT_ERLAUBT:
                    out.append((_fid(it), tok))
    return out


def test_keine_stichwort_degeneration():
    assert not _stichwort_degeneration(ALLE)


def test_gate_degeneration_schlaegt_an():
    """Gift-Probe: „ertrag" zurueck in Gruppe 0 des Bio-Landbaus — genau die
    Degeneration, die „Es fehlt ein Friedensvertrag" treffen liess."""
    it = copy.deepcopy(FAKT["bio_landbau_ertrags_oekobilanz_2026"])
    it["trigger_composite"][0].append("ertrag")
    assert ("bio_landbau_ertrags_oekobilanz_2026", "ertrag") in _stichwort_degeneration([("x", it)])
    assert substring_or_composite_match(it, "es fehlt ein friedensvertrag")


@pytest.mark.parametrize("fid,gruppe,tok", [
    # die aufgeloesten Degenerationen — das Token steht nur noch in EINER Gruppe
    ("bio_landbau_ertrags_oekobilanz_2026", 0, "ertrag"),
    ("vereinbarkeit_familie_beruf_2026", 0, "karriere"),
    ("winterreifen_pflicht_konsens", 1, "schnee"),
    ("at_neutralitaet_recht_2026", 0, "bündnis"),
    ("duengemittel_importabhaengigkeit_2026", 0, "russland"),
    ("duengemittel_importabhaengigkeit_2026", 0, "sanktionen"),
])
def test_degeneration_aufgeloest(fid, gruppe, tok):
    assert tok not in FAKT[fid]["trigger_composite"][gruppe]


# --- 4c. Blanke Kurz-Tokens --------------------------------------------------

def _korpus_woerter() -> set[str]:
    claims = [ph for _, it in ALLE for ph in it.get("claim_phrasings_handled") or []]
    for p in glob.glob(os.path.join(BACKEND, "tools", "stress_tests", "*.json")):
        with open(p, encoding="utf-8") as fh:
            claims += [c["claim"] for c in json.load(fh).get("claims") or []
                       if isinstance(c, dict) and c.get("claim")]
    with open(os.path.join(BACKEND, "tools", "stress_test_100_claims.json"),
              encoding="utf-8") as fh:
        claims += [c["claim"] for c in json.load(fh)]
    woerter = set()
    for c in claims:
        woerter |= set(re.findall(r"[a-zäöüß0-9]+", normalisiere(c.lower())))
    return woerter


WOERTER = _korpus_woerter()

# Gesichtet am 2026-09-26: blanke Kurz-Tokens (<= 3 Zeichen), die im Korpus
# mitten in einem Wort stecken — und trotzdem bleiben. Fast alle stehen in
# einer Praedikats-Gruppe („ok", „gut", „ist", „ab", Zahlen), deren Regel eine
# ANDERE Gruppe thematisch traegt; ein Innen-Treffer dort allein feuert
# nichts. Die uebrigen haben nur fremdsprachige oder eigennamige Wirte
# (NEWTON, REBOARD). Ein NEUES blankes Kurz-Token schlaegt hier an.
KURZ_BLANK_GESICHTET = {
    "5g_telekom_qos_2026": ("lte",),
    "atomwaffen_faktoide_2026": ("usa",),
    "auftauen_zimmertemperatur_mythos": ("ok",),
    "auslander_kriminalitaet_de_2024": ("bka",),
    "awmf_copd": ("ics",),
    "awmf_lungenkarzinom": ("alk",),
    "awmf_unipolare_depression": ("ect",),
    "bahn_vs_auto_sicherheit_konsens": ("zug",),
    "bitcoin_preis_meilensteine": ("ath", "etf"),
    "brauner_zucker_2026": ("gut",),
    "browser_fingerprinting_konsens_2026": ("eff", "tor"),
    "bundes_parteienfoerderung_2024": ("12", "13", "17", "26", "8"),
    "chinesische_mauer_2026": ("all", "iss"),
    "darmkrebs_screening": ("ab",),
    "deep_state_konspiration_2026": ("ist",),
    "detox_saefte_2026": ("kur",),
    "down_syndrom_realitaet_2026": ("alt",),
    "ee_anteil_de_2024": ("nur",),
    "eugh_kopftuch_arbeitsplatz": ("job",),
    "familienbonus_plus_2026": ("arm",),
    "femizide_at_de_2026": ("tot",),
    "fleischkonsum_at_trend_2026": ("kg",),
    "fruchtbarstes_fenster_mythos": ("tag", "vor"),
    "great_wall_china_alter_2026": ("alt",),
    "iq_foerderung_mythen_2026": ("roi",),
    "korruption_index_2026": ("75", "90", "usa"),
    "laerm_arbeit_2026": ("bau",),
    "mammographie_screening": ("ab",),
    "massentierhaltung_empirie_2026": ("gut",),
    "mobile_telefon_freisprech_konsens": ("ok",),
    "mobilitaet-bahn_investition_de_at_2026": ("ch",),
    "mobilitaet-oepnv_ausbau_empirie_2026": ("bus",),
    "mobilitaet-tempolimit_130_2026": ("wer",),
    "mozart_armer_2026": ("arm",),
    "multitasking_2026": ("gut",),
    "nbb_2024_lehrplan_reform_2023": ("neu",),
    "no_pain_no_gain_2026": ("no",),
    "oebb_puenktlichkeit_2024": ("zug",),
    "oeif_arbeitsmarkt_integration_2023": ("job",),
    "oeif_sprachkurse_2023": ("ger",),
    "parteienfin_total_2024_kontext": ("300",),
    "pegasus_spyware_konsens_2026": ("bka",),
    "praevention_kosten_effekt_2026": ("roi",),
    "pressefreiheit_trends_2026": ("gut", "usa"),
    "quantencomputer_2026": ("rsa",),
    "rechenschaftsbericht_pflicht_partg_par8": ("wer",),
    "rote_ampel_kein_verkehr_mythos": ("ok",),
    "safe_listening_standard_2026": ("itu",),
    "schulpflicht_at_2026": ("bis",),
    "spermien_lebensdauer_mythos": ("tot",),
    "stier_rote_farbe_2026": ("red", "rot"),
    "subventions_kriege_2026": ("wto",),
    "theologische_aussagen_kategorie_2026": ("tot",),
    "un_brk_konvention_2026": ("oar",),
    "vertical_farming_realitaet_2026": ("led",),
    "vfgh_g_258_2017_ehe_alle": ("ehe",),
    "vfgh_orf_beitrag_g_226_2022": ("gis", "orf"),
    "wechseljahre_alter_mythos": ("50",),
    "weltbank_einkommensklassifikation_at_2026": ("bne",),
    "wto_welthandel_2024": ("wto",),
    "zara_arbeitswelt_diskriminierung_2023": ("job",),
}


def _kurz_blank_mit_innen_wirt(alle, woerter) -> list[tuple[str, str, str]]:
    """Blanke Einwort-Tokens bis drei Zeichen, die im Korpus mitten in einem
    Wort stecken (Vorn-Gebundene koennen das per Konstruktion nicht)."""
    out = []
    for _, it in alle:
        erlaubt = KURZ_BLANK_GESICHTET.get(_fid(it), ())
        for t in _tokens(it):
            n = normalisiere(t)
            kern = n.strip()
            if n[:1] == " " or len(kern) > 3 or not re.fullmatch(r"[a-zäöüß0-9]+", kern):
                continue
            wirt = next((w for w in sorted(woerter) if kern in w[1:]), None)
            if wirt and t not in erlaubt:
                out.append((_fid(it), t, wirt))
    return out


def test_blanke_kurz_tokens_nur_gesichtet():
    assert not _kurz_blank_mit_innen_wirt(ALLE, WOERTER)


def test_gate_kurz_token_schlaegt_an():
    it = copy.deepcopy(FAKT["buddhismus_friedlich_2026"])
    it["trigger_composite"][0].append("zen")
    funde = _kurz_blank_mit_innen_wirt([("x", it)], WOERTER)
    assert any(f[1] == "zen" for f in funde), funde


# ---------------------------------------------------------------------------
# 5. Ueber-Trigger-Sweep auf den Fakten mit Korpus-Beleg — und er schlaegt an
# ---------------------------------------------------------------------------

def _phrasings() -> list[tuple[str, str]]:
    return [(d, ph) for d, it in ALLE for ph in it.get("claim_phrasings_handled") or []]


PHRASINGS = _phrasings()


def _fremde_treffer(fid: str, it: dict) -> set[str]:
    """Dokumentierte Phrasings aus ANDEREN data-Dateien, die den Fakt treffen
    (exakt oder tolerant — konservativ fuer die Leck-Suche)."""
    datei = WO[fid][0]
    return {ph for d, ph in PHRASINGS if d != datei
            and (substring_or_composite_match(it, ph.lower())
                 or tippfehler_match(it, ph.lower()))}


# Die 22 Fakten, von denen dieser Fix einen Korpus-Fremdtreffer nahm
SWEEP_FAKTEN = (
    "abs_kitchen_2026", "acht_glaeser_wasser_mythos", "alkohol_rotwein_herz_mythos",
    "bio_landbau_ertrags_oekobilanz_2026", "bio_pestizidfrei_2026",
    "buddhismus_friedlich_2026", "duengemittel_importabhaengigkeit_2026",
    "e_auto_akku_co2_2026", "eb_eu_mitgliedschaft_2024", "eb_eu_vertrauen_2024",
    "eb_top_themen_2024", "enisa_threat_landscape_2026", "environmental_noise_2026",
    "erbrecht_pflichtteil_2026", "everest_hoechster_2026", "glyphosat_empirie_2026",
    "gvo_gentechnik_risiko_2026", "katze_milch_mythos", "mint_frauen_anteil_2026",
    "partg_2012_rechtsgrundlage", "saeure_basen_krebs_mythos",
    "winterreifen_pflicht_konsens",
)

# Dienst-fremde Treffer, die bleiben — gesichtet, keiner ist ein Wortgrenzen-
# Leck: am Thema (Brexit -> EU-Mitgliedschaft, Cyber-Claims -> ENISA) oder
# eine inhaltliche Ueberschneidung (Wasser + gesund).
ERLAUBT = {
    "acht_glaeser_wasser_mythos": {
        "Basisches Wasser ist gesund gegen Tumor",
        "Ionisiertes Wasser hat gesundheitliche Vorteile",
        "Viel Wasser trinken macht den Drogen-Test negativ",
    },
    "bio_landbau_ertrags_oekobilanz_2026": {
        # Homonym „biologisch" — ein Wort, keine Wortgrenze
        "Frauen sind biologisch weniger begabt für Mathematik",
    },
    "bio_pestizidfrei_2026": {
        "Bio ist 100 Prozent pestizid-frei",
        "In Bio-Lebensmitteln sind auch Pestizide",
    },
    "eb_eu_mitgliedschaft_2024": {
        "Brexit hatte keine Folgen", "Brexit kostet UK 10 % BIP",
        "Brexit kostet UK 4 % BIP", "EU-Mitgliedschaft kostet uns Milliarden",
        "Mehrheit Briten will Brexit zurück", "UK profitiert von Brexit",
    },
    "eb_eu_vertrauen_2024": {
        "In Österreich vertrauen die Menschen dem Parlament mehr als im EU-Schnitt",
    },
    "enisa_threat_landscape_2026": {
        "AT hat 2000 Cyberangriffe pro Jahr",
        "Cyber-Angriffe aus Russland gegen AT",
        "Cybercrime Österreich Statistik Anzeigen",
        "Die Anzeigen wegen Cybercrime sind in Österreich stark gestiegen",
        "Wie viele Cybercrime-Fälle gibt es in Österreich",
    },
    "glyphosat_empirie_2026": {
        "Glyphosat ist sicher", "Glyphosat verursacht Krebs",
        "Roundup ist krebserregend",
    },
}


@pytest.mark.parametrize("fid", SWEEP_FAKTEN)
def test_sweep_keine_neuen_fremdtreffer(fid):
    neu = _fremde_treffer(fid, FAKT[fid]) - ERLAUBT.get(fid, set())
    assert not neu, f"{fid}: neue dienst-fremde Treffer {sorted(neu)}"


@pytest.mark.parametrize("fid,gruppe,gebunden,blank,erwartet", [
    ("environmental_noise_2026", "trigger_keywords", " lden ", "lden",
     "Schuldenbremse ist alternativlos"),
    ("mint_frauen_anteil_2026", 0, " stem ", "stem", "US-Wahlsystem ist undemokratisch"),
    ("everest_hoechster_2026", 0, " berg", "berg",
     "Bilderberg-Konferenz ist eine geheime Weltregierung"),
    ("duengemittel_importabhaengigkeit_2026", 0, "dünger", "russland",
     "Russland ist sehr korrupt"),
], ids=["lden", "stem", "berg", "russland"])
def test_sweep_schlaegt_an(fid, gruppe, gebunden, blank, erwartet):
    """Gift-Probe: das alte, blanke Token zurueck in eine Kopie des Fakts — der
    Sweep MUSS das bekannte Leck melden. Sonst ist er blind, und seine Null
    oben beweist nichts. (Bei „russland" kommt das Token ZUSAETZLICH in
    Gruppe 0 — die Degeneration braucht beide Gruppen.)"""
    it = copy.deepcopy(FAKT[fid])
    liste = it[gruppe] if isinstance(gruppe, str) else it["trigger_composite"][gruppe]
    assert gebunden in liste, f"{gebunden!r} nicht in {fid} — Probe veraltet"
    if blank == "russland":
        liste.append(blank)
    else:
        liste[liste.index(gebunden)] = blank
    assert erwartet in _fremde_treffer(fid, it)
    assert erwartet not in _fremde_treffer(fid, FAKT[fid])


# ---------------------------------------------------------------------------
# 6. Bekannte Reste — bewusst offen, beidseitig gepinnt
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("fid,claim,warum", [
    ("stier_rote_farbe_2026", "Red Bull verleiht Flügel.",
     "„bull“ UND „red“ stehen als ganze Woerter da; ohne Negation in der "
     "Trigger-Sprache nicht trennbar von „bulls see red“"),
    ("hund_schokolade_toxizitaet_konsens", "Hunderte Menschen essen täglich Schokolade.",
     "HUNDErte beginnt wie „hunde“; hinten binden kostet Hundehalter, Hundebiss"),
    ("winterreifen_pflicht_konsens", "Das Pensionssystem ist ein Schneeballsystem und Unsinn.",
     "SCHNEEballsystem beginnt wie „schnee“; hinten binden kostet Schneeketten. "
     "Seit dem Fix braucht es dazu ein Wort der Praedikats-Gruppe („Unsinn“)"),
    ("acht_glaeser_wasser_mythos", "Wasserstoff muss teuer importiert werden.",
     "WASSERstoff beginnt wie „wasser“; hinten binden kostet Wasserbedarf"),
    ("effektive_steuerlast_reiche_mittelschicht_2026",
     "Die Steuern reichen nicht, um weniger Schulden zu machen.",
     "Homonym: das Verb „reichen“ ist dasselbe Wort wie „den Reichen“"),
])
def test_bekannter_rest(fid, claim, warum):
    """Wer einen dieser Reste schliesst, dreht die Zeile um."""
    assert _trifft_echt(fid, claim), warum
