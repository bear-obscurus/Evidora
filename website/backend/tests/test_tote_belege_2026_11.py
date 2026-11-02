"""Die toten Beleg-Links aus dem Lauf vom 2.11.2026.

Der Wächter meldete 71 tote URLs, davon **14 in kuratierten Dateien**; die
übrigen 57 liegen in generierten Caches (MITRE/CORDIS, #243). Von den 14
waren 10 schon begründet geparkt, vier neu oder ungeparkt.

Alle 14 einzeln nachgeprüft: **alle 14 sind echt tot.** Der Prüfer hatte
recht.

## Der Prüfstand entschied, nicht die Seite

Vom Arbeitsrechner aus antwortete einer der 14 — die
Inflationsausgleichsgesetz-FAQ des BMF — dreimal mit HTTP 503 und einer
Myra-Cloud-Seite "Sicherheitsüberprüfung". Das sah nach der Erklärung der
Tot-Meldung aus, war aber die Bot-Sperre vor meinen wiederholten Abrufen.
Vom Prod-Host, von dem der Wächter läuft, kommt dreimal **HTTP 404 mit der
BMF-eigenen Fehlerseite** ("Nicht gefunden - not found", 172 KB, kein
Sperr-Marker), während die BMF-Startseite 200 liefert. Die Seite ist weg.

Die Lehre ist nicht "der Prüfer irrt", sondern: **eine Messung von einem
anderen Netz aus ist eine andere Messung.** Der Wächter läuft auf dem Host;
wer seine Befunde vom Laptop aus nachprüft, prüft etwas anderes nach.

Der Rumpf-Mitleser aus test_bot_sperre_ist_nicht_tot.py bleibt — er ist auf
einer Fehldiagnose entstanden, trägt aber unabhängig davon: derselbe
Wächter-Lauf hat 164 Antworten mit einer Bot-Sperre im Rumpf gefunden und
benennt sie jetzt getrennt.

## Sieben ersetzt — und zwei erfundene Aktenzeichen gefunden

Ersetzt wurde nur, wo der neue Link DIESELBE Aussage trägt. Dabei fiel bei
zwei Fakten auf, dass nicht die Links das Problem waren, sondern die
Zitate:

  * **recht_pack/screenshot_whatsapp_2026** hatte BEIDE Belege tot — der
    einzige Fakt dieser Runde ohne jede lebende Quelle. Das ``source_label``
    nannte "BGH XII ZB 124/20 + OGH 1 Ob 7/22w", und dieselben zwei
    Aktenzeichen standen im Datenfeld ``gerichtliche_verwendung``, also
    mitten im Prompt. Beide sind nicht belegbar: ``1Ob7/22w`` ergibt in der
    RIS-OGD-API null Treffer, während vier echte Nummern desselben Senats
    (1Ob210/14k, 1Ob181/15x, 1Ob36/16z, 1Ob142/17i) 6 bis 15 Treffer
    liefern; die BGH-Entscheidungssuche gibt zu ``XII ZB 124/20`` sechs
    Treffer zurück, von denen keiner dieses Aktenzeichen trägt, während die
    Gegenprobe ``V ZR 171/22`` sich selbst findet. Zweiter Fall von #235.
    An ihrer Stelle steht jetzt die freie Beweiswürdigung als Normtext
    (§ 286 ZPO DE wörtlich "Freie Beweiswürdigung", § 272 ZPO AT mit dem
    Schlagwort "Grundsatz der freien Beweiswürdigung").

  * **arbeitsmarkt_pack/ki_job_verdraengung_2026** zitierte
    "Acemoglu/Restrepo 2024 American Economic Review Papers and
    Proceedings 'The Simple Macroeconomics of AI'". Der Aufsatz existiert,
    aber als **Economic Policy 39(120):3-46** und von **Acemoglu allein**;
    in seiner eigenen, bis 2026 geführten Publikationsliste steht für 2024
    kein AER-P&P-Eintrag. Der tote Dateiname
    (``AcemogluPublishedRestrepoAERPP202405.pdf``) hatte zwei Arbeiten
    verschmolzen. Die ZAHL im Fakt stimmt dagegen: 20 % der Aufgaben sind
    AI-exponiert, davon 23 % in zehn Jahren profitabel automatisierbar —
    4,6 %, im Papier auch als 3,3 % plus 1,3 % des BIP.

## Sieben bleiben geparkt

NABU (die ganze Subdomain ist abgeschaltet, und nabu.de ist seitenweit
offline), AGES Tierwohl (die Sitemap führt 904 Seiten, keine davon zum
Thema), Amnesty ASA 01/4030/2021 (404 in beiden Schreibweisen, die
Dokumentsuche antwortet maschinell 403) und drei ACSM-Blogbeiträge. Beim
vierten ACSM-Link hat der Weg über die Primärliteratur doch getragen —
nicht über eine einzelne Studie, sondern über das ACSM-Positionspapier
selbst.

Keine Netzabfrage in diesen Tests.
"""

import json
from pathlib import Path

import pytest

BACKEND = Path(__file__).resolve().parents[1]
DATA = BACKEND / "data"

#: (Datei, toter Link, Ersatz) — jeder Ersatz am 2.11.2026 mit
#: ``-w '%{url_effective}'`` geprueft und im Rumpf gegengelesen.
ERSETZT = [
    # Acemoglu 2024, aber Economic Policy statt AER P&P (siehe Kopf).
    ("arbeitsmarkt_pack.json",
     "https://economics.mit.edu/sites/default/files/2024-04/"
     "AcemogluPublishedRestrepoAERPP202405.pdf",
     "https://economics.mit.edu/sites/default/files/2024-10/"
     "The%20Simple%20Macroeconomics%20of%20AI.pdf"),
    # ICCT 04/2024: Typzulassung 15,4 kWh/100km, real-world 29-44 % darueber
    # -> Reichweite 69-78 % der WLTP-Angabe, also die behaupteten ~70-80 %.
    ("mobilitaet_pack.json",
     "https://theicct.org/publication/"
     "real-world-energy-consumption-electric-vehicles-europe-jul24/",
     "https://theicct.org/wp-content/uploads/2024/04/"
     "ID-80-%E2%80%93-BEVs-size-Report-A4-70138-v8.pdf"),
    # ACSM Position Stand "Progression models in resistance training"
    # (MSSE 2009): "progressive resistance training (RT) protocols are
    # necessary". Nicht der chinesisch uebersetzte PDF-Nachdruck auf
    # acsm.org — der hat 19.467 CJK- gegen 4.344 lateinische Zeichen.
    ("sport_fitness_pack.json",
     "https://www.acsm.org/blog-detail/acsm-certified-blog/2018/10/15/"
     "progressive-overload-strength-training",
     "https://pubmed.ncbi.nlm.nih.gov/19204579/"),
    # Statt der entfernten Verbraucherzentrale-Seite der Normtext, der die
    # Beweismittel-Aussage traegt.
    ("recht_pack.json",
     "https://www.verbraucherzentrale.de/wissen/digitale-welt/datenschutz/"
     "whatsapp-chats-als-beweismittel-rechtslage-und-tipps-69466",
     "https://www.gesetze-im-internet.de/zpo/__286.html"),
    # Statt des juraforum-Lexikons die BGH-Pressemitteilung zur
    # Verwendungs-Seite: Weitergabe privater Chat-Korrespondenz, DSGVO-
    # Haushaltsausnahme, EuGH-Vorlage vom 17.9.2026 (I ZR 256/25 — in der
    # BGH-Entscheidungssuche exakter Treffer, anders als XII ZB 124/20).
    ("recht_pack.json",
     "https://www.juraforum.de/lexikon/screenshot-recht",
     "https://www.bundesgerichtshof.de/SharedDocs/Pressemitteilungen/DE/"
     "2026/2026169.html"),
    # eff.org/issues/online-anonymity ist weg; eff.org/issues/anonymity lebt,
    # belegt aber nur anonyme Rede und kein Wort zu VPN-Grenzen (#248).
    # Mozilla (im source_label genannt) belegt die Logging-Haelfte woertlich:
    # "Some VPNs will log your online activities so they can sell your data".
    ("cybersecurity_pack.json",
     "https://www.eff.org/issues/online-anonymity",
     "https://www.mozilla.org/en-US/products/vpn/resource-center/"
     "what-is-a-vpn/"),
    # Das SSD-Modul sagt es woertlich: "VPNs Don't Completely Anonymize You"
    # und "A VPN is not a tool for anonymity" (Stand 28.7.2026).
    ("tech_ki_pack.json",
     "https://www.eff.org/issues/vpn",
     "https://ssd.eff.org/module/choosing-vpn-thats-right-you"),
]

#: Tot, aber nicht ersetzbar — die Begruendung steht in url_bekannt_tot.json.
GEPARKT = [
    "https://bergenhusen.nabu.de/forschung/windenergie-und-voegel/index.html",
    "https://www.ages.at/themen/lebensmittel/lebensmittelsicherheit/"
    "tierwohl-und-tierschutz",
    "https://www.amnesty.org/en/documents/asa01/4030/2021/en/",
    "https://www.acsm.org/all-blog-posts/certification-blog/"
    "acsm-certified-blog/2018/03/27/listening-to-the-pain",
    "https://www.acsm.org/blog-detail/acsm-blog/2014/06/24/"
    "exercise-fuels-low-intensity-vs-high-intensity",
    "https://www.acsm.org/blog-detail/acsm-blog/2018/03/14/lean-body-mass",
    # Vom Host dreimal 404 mit der BMF-eigenen Fehlerseite; kein Nachfolger
    # nennt "kalte Progression" (Pressemitteilung zur Einkommensteuerreform,
    # Lohnsteuer-Themenseite, FAQ-Pfad ohne Datum alle geprueft).
    "https://www.bundesfinanzministerium.de/Content/DE/Standardartikel/Themen/"
    "Steuern/Steuerarten/Lohnsteuer/"
    "2024-12-23-Inflationsausgleichsgesetz-FAQ.html",
]

BEKANNT = json.loads((BACKEND / "tools" / "url_bekannt_tot.json")
                     .read_text(encoding="utf-8"))["tot"]


def _text(datei):
    return (DATA / datei).read_text(encoding="utf-8")


def _alle_daten():
    return "".join(p.read_text(encoding="utf-8")
                   for p in sorted(DATA.glob("*.json")))


# --------------------------------------------------------------------------
# Die Ersetzungen
# --------------------------------------------------------------------------

@pytest.mark.parametrize("datei,alt,neu", ERSETZT)
def test_toter_link_ist_weg(datei, alt, neu):
    assert alt not in _text(datei)


@pytest.mark.parametrize("datei,alt,neu", ERSETZT)
def test_ersatz_steht_drin(datei, alt, neu):
    assert neu in _text(datei)


@pytest.mark.parametrize("datei,alt,neu", ERSETZT)
def test_kein_ersetzter_link_steht_woanders_noch(datei, alt, neu):
    """Eine URL in zwei Kopien driftet — hier: der tote Link bleibt an einer
    zweiten Stelle stehen und wird naechste Woche erneut gemeldet."""
    assert alt not in _alle_daten()


def test_kein_ersatz_ist_selbst_als_tot_bekannt():
    assert not {n for _d, _a, n in ERSETZT} & set(BEKANNT)


# --------------------------------------------------------------------------
# Das Geparkte bleibt sichtbar
# --------------------------------------------------------------------------

@pytest.mark.parametrize("url", GEPARKT)
def test_geparkter_link_steht_begruendet_in_der_liste(url):
    assert url in BEKANNT, url
    assert len(BEKANNT[url].get("grund") or "") > 30, url


@pytest.mark.parametrize("url", GEPARKT)
def test_geparkter_link_steht_noch_in_den_daten(url):
    """Sonst waere die Luecke stillgelegt statt geschlossen."""
    assert url in _alle_daten(), url


def test_die_bmf_faq_ist_geparkt_und_nicht_als_sperre_entschuldigt():
    """Die Giftprobe gegen meine eigene Fehldiagnose.

    Der Link stand einen Nachmittag lang als "nicht tot, nur ausgesperrt"
    da. Vom Prod-Host aus ist er 404. Wer diesen Test fallen sieht, hat die
    Begruendung aus der Parkliste entfernt — dann bitte vom HOST aus
    nachmessen, nicht vom Arbeitsrechner.
    """
    bmf = [u for u in GEPARKT if "bundesfinanzministerium" in u]
    assert len(bmf) == 1
    grund = BEKANNT[bmf[0]]["grund"]
    assert "404" in grund and "Host" in grund


# --------------------------------------------------------------------------
# Die zwei Zitat-Korrekturen
# --------------------------------------------------------------------------

def test_die_nicht_belegbaren_aktenzeichen_sind_raus():
    """Sie standen im source_label UND im Prompt-Datenfeld."""
    alle = _alle_daten()
    for az in ("XII ZB 124/20", "1 Ob 7/22w", "1Ob7/22w"):
        assert az not in alle, az


def test_der_screenshot_fakt_nennt_die_freie_beweiswuerdigung():
    d = json.loads(_text("recht_pack.json"))
    f = [x for x in d["facts"]
         if (x.get("id") or x.get("topic")) == "screenshot_whatsapp_2026"][0]
    feld = f["data"]["gerichtliche_verwendung"]
    assert "§ 286 ZPO" in feld and "§ 272 ZPO" in feld
    assert len(feld) <= 400, len(feld)
    assert "286" in f["source_label"]


def test_der_screenshot_fakt_hat_wieder_zwei_lebende_belege():
    d = json.loads(_text("recht_pack.json"))
    f = [x for x in d["facts"]
         if (x.get("id") or x.get("topic")) == "screenshot_whatsapp_2026"][0]
    assert f["source_url"] == "https://www.gesetze-im-internet.de/zpo/__286.html"
    assert f["secondary_url"].startswith(
        "https://www.bundesgerichtshof.de/SharedDocs/Pressemitteilungen/")
    assert f["source_url"] not in BEKANNT
    assert f["secondary_url"] not in BEKANNT


def test_acemoglu_wird_der_richtigen_zeitschrift_zugeschrieben():
    alle = _alle_daten()
    assert "Acemoglu/Restrepo 2024 American Economic Review" not in alle
    d = json.loads(_text("arbeitsmarkt_pack.json"))
    f = [x for x in d["facts"]
         if (x.get("id") or x.get("topic")) == "ki_job_verdraengung_2026"][0]
    assert "Economic Policy 39(120):3-46" in f["source_label"]
    feld = f["data"]["acemoglu_2024_simple_macroeconomics"]
    assert "Economic Policy" in feld
    assert "Restrepo" not in feld, "der Aufsatz hat nur einen Autor"
    assert len(feld) <= 400, len(feld)


def test_die_zahl_im_acemoglu_feld_ist_die_aus_dem_papier():
    """20 % exponiert x 23 % automatisierbar = 4,6 %; TFP hoechstens 0,66 %."""
    d = json.loads(_text("arbeitsmarkt_pack.json"))
    f = [x for x in d["facts"]
         if (x.get("id") or x.get("topic")) == "ki_job_verdraengung_2026"][0]
    feld = f["data"]["acemoglu_2024_simple_macroeconomics"]
    for zahl in ("20 %", "23 %", "4,6 %", "0,66 %"):
        assert zahl in feld, zahl


# --------------------------------------------------------------------------
# Die Parkliste insgesamt
# --------------------------------------------------------------------------

def test_die_erledigten_stehen_nicht_mehr_in_der_parkliste():
    for _d, alt, _n in ERSETZT:
        assert alt not in BEKANNT, alt


def test_jeder_geparkte_link_ist_begruendet():
    ohne = [u for u, v in BEKANNT.items() if len(v.get("grund") or "") <= 30]
    assert not ohne, ohne
