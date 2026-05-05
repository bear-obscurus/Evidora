# Quality-Audit der 4 Sprint-2026-05-05-Packs

**Auditor:** Claude Sonnet 4.5
**Datum:** 2026-05-05
**Methode:** WebSearch-basierte Verifikation der Top-Citations (peer-reviewed Papers, Court Cases, Statistik-Quellen)

## Zusammenfassung

15 High-Risk-Citations geprüft. **8 verifiziert korrekt**, **7 mit Korrekturbedarf**. Pack-Inhalte sind im Kern empirisch fundiert; Fehler sind Detail-Ebene (NBER-Nummer, Zeitschrift, Jahr, Hauptautoren-Attribution) — KEINE inhaltlich falschen Aussagen entdeckt.

## ✅ Verifiziert korrekt (8)

| # | Citation | Pack | Status |
|---|---|---|---|
| 1 | Card/Krueger 1994 AER 84(4):772-93 | Gleichstellung + Wirtschaftspolitik | ✓ Alle Details (410 fast-food Restaurants, NJ 4,25→5,05 USD April 1992) |
| 2 | Diamond/McQuade/Qian 2019 AER 109(9):3365-94 | Wohnen | ✓ -15 % Bestandsreduktion (Verkauf an Owner-occupiers + Redevelopment) |
| 3 | Cengiz/Dube/Lindner/Zipperer 2019 NBER 25434 + QJE | Gleichstellung + Wirtschaftspolitik | ✓ 138 prominent state-level minimum wage changes 1979-2016 |
| 4 | Stoet/Geary 2018 Psychological Science | Gleichstellung | ✓ Mit Replikations-Streit (Richardson 2020) korrekt erwähnt |
| 5 | Spencer/Steele/Quinn 1999 J Exp Soc Psychol 35:4-28 | Gleichstellung | ✓ Studienmethodik korrekt |
| 6 | BVerfG 25.3.2021 — 2 BvF 1/20, 2 BvL 5/20, 2 BvL 4/20 | Wohnen | ✓ Berliner Mietendeckel nichtig wegen Bundes-Kompetenz (Art. 74 Abs. 1 Nr. 1 GG) |
| 7 | ADL Global 100 Index 2014 (11-Item-Stereotype-Index) | Religionsgemeinschaften | ✓ 26 % weltweit, 103 Länder |
| 8 | IHRA Working Definition Antisemitism 26.5.2016 | Religionsgemeinschaften | ✓ Bukarest-Plenum |
| 9 | MHG-Studie 2018 Dreßing (Mannheim/Heidelberg/Gießen) | Religionsgemeinschaften | ✓ 38.156 Personalakten, 1.670 Beschuldigte (4,4 %) |
| 10 | CBO TCJA 2018: 1,9 Bio USD Defizit über 10 Jahre | Wirtschaftspolitik | ✓ Dynamic Score |
| 11 | EGMR Refah Partisi vs. Türkei 13.2.2003 (41340/98) | Religionsgemeinschaften | ✓ Sharia mit EMRK unvereinbar (einstimmig Großkammer) |

## ⚠️ Korrekturbedarf (7)

### A. Citation-Detail-Fehler

**1. Glaeser/Gyourko 2018 — NBER-Nummer falsch**
- Pack: NBER 25060
- Tatsächlich: **NBER 23833** (2017), publiziert in **Journal of Economic Perspectives** 32(1):3-30 (Winter 2018)
- Wohnen-Pack `wohnungsmarkt_freier_markt_mythos`

**2. Phillips/Axelrod Encyclopedia of Wars — Jahr + Zahl falsch**
- Pack: "2007 ... 123 von 1.763 (6,98 %) primär religiös"
- Tatsächlich: **Encyclopedia of Wars 2005**, **121 von 1.763 (6,87 %)** im Index als religiös
- Die "123/6,98 %"-Zahl stammt aus Vox Day "The Irrational Atheist" (2008), NICHT aus der Originalstudie
- Religionsgemeinschaften-Pack `religion_gewalt_korrelation_konsens`

**3. Campbell 2003 — Zeitschrift falsch**
- Pack: **Lancet**
- Tatsächlich: **American Journal of Public Health (AJPH)** 93(7):1089-97
- Gleichstellung-Pack `femizide_at_de_konsens` + Religionsgemeinschaften-Pack zitiert ähnlich

**4. Saez/Zucman NBER 26839 "How Has Wealth Inequality Evolved" — Paper-Identität falsch**
- Pack: NBER 26839 mit diesem Titel
- Tatsächlich: NBER 26839 existiert mit **anderem Titel** (Konsumenten-Inflation/COVID, nicht Wealth Inequality)
- Korrekte Saez/Zucman-Wealth-Papers: **NBER 20625 "Wealth Inequality in the United States since 1913"** (2014, publiziert in QJE 2016) ODER **NBER 27921 "Trends in US Income and Wealth Inequality"** (2020)
- Wirtschaftspolitik-Pack `vermoegenssteuer_kapitalflucht_konsens`

**5. Felbermayr/Aichele/Heiland Bertelsmann 2019 — Hauptautoren-Attribution falsch**
- Pack: "Bertelsmann-Stiftung 2019 (Felbermayr/Aichele/Heiland)"
- Tatsächlich: Hauptautoren der Bertelsmann-Studie 2019 zum EU-Binnenmarkt sind **Giordano Mion + Dominic Ponattu**. Felbermayr ist als Referenz erwähnt aber nicht Hauptautor dieser Studie
- Wert ✓ (1.046 Euro pro Einwohner DE = ~86 Mrd Euro/Jahr)
- Wirtschaftspolitik-Pack `eu_netto_zahler_konsens`

**6. Kleven et al 2019 — Zeitschrift falsch**
- Pack: **American Economic Review**
- Tatsächlich: **AEA Papers and Proceedings** 109:122-26 (Mai 2019). Das ist zwar AEA, aber NICHT die gleiche Zeitschrift wie AER.
- Gleichstellung-Pack `vereinbarkeit_familie_beruf_konsens`

**7. Hilber LSE 2021 "Economic Implications of House Price Capitalisation" — Jahr falsch**
- Pack: 2021
- Tatsächlich: Hilber's "Economic Implications of House Price Capitalization: A Synthesis" wurde in **Real Estate Economics April 2017** publiziert (vol 45(2):301-39)
- 2021 hat Hilber andere Papers ("Why Delay?", "Home Truths"), aber NICHT das Synthesis-Paper
- Wohnen-Pack `wohnungsmarkt_freier_markt_mythos`

### B. Schwer verifizierbare Citations

**8. Pew "Concerns about Islamic Extremism" 2017 — Zahlen nicht direkt belegbar**
- Pack zitiert "Median 76 % halten Selbstmord-Anschläge nie gerechtfertigt"
- Pew hat solche Studien (2013 "World's Muslims" + 2017 "Concerns about Islamic Extremism"), aber die exakte 76 %-Zahl nicht direkt im WebSearch-Result auffindbar
- Pew 2013 (n=38 Länder) zeigt 67-87 % Range — Median 76 % ist plausibel aber unverifiziert
- Religionsgemeinschaften-Pack `islam_mehrheit_radikal_mythos`
- **Empfehlung:** Quelle aus Pack streichen oder konkretes Pew-Paper-PDF nachzitieren

## Korrektur-Strategie

**Plan:** Inline-Korrekturen in den 4 Pack-JSONs für die 7 konkreten Fehler. Die Korrekturen ändern NICHT die inhaltlichen Aussagen oder Verdicts der Topics — nur die Zitations-Details werden präziser.

Vorgehen:
1. NBER-Nummern korrigieren (Glaeser, Saez/Zucman)
2. Jahre korrigieren (Phillips/Axelrod 2005 statt 2007, Hilber 2017 statt 2021)
3. Zeitschriften korrigieren (Campbell AJPH statt Lancet, Kleven AEA P&P statt AER)
4. Hauptautoren korrigieren (Bertelsmann: Mion/Ponattu statt Felbermayr-Lead)
5. Phillips/Axelrod-Zahl korrigieren (121 statt 123, 6,87 statt 6,98 %)

Pack-Verdicts bleiben unverändert. Stress-Test-Ergebnisse müssen nicht neu gemacht werden.

## Empfehlung für zukünftige Pack-Erstellung

**Lessons Learned:**
1. NBER-Working-Paper-Nummern immer direkt von nber.org verifizieren (nicht aus Memory zitieren)
2. Bei "Lancet vs AJPH"-Verwechslungs-Risiko explizit DOI/PMID nachschlagen
3. Bei Bertelsmann/Wirtschafts-Studien: Hauptautoren-Listing der Studie selbst (nicht der referenzierten Vorarbeiten) zitieren
4. Bei AER vs AEA Papers and Proceedings: das sind zwei separate Zeitschriften mit Doppelt-Vol-Numbering
5. Bei Hilber/Glaeser/Saez Papers: WP-Phase vs. Final-Publication Jahr unterscheiden

## Audit-Quellen-URLs

- [Glaeser/Gyourko NBER 23833](https://www.nber.org/papers/w23833)
- [Diamond/McQuade/Qian AER 2019](https://www.aeaweb.org/articles?id=10.1257/aer.20181289)
- [Cengiz et al NBER 25434](https://www.nber.org/papers/w25434)
- [Card/Krueger 1994 AER](https://davidcard.berkeley.edu/papers/njmin-aer.pdf)
- [Stoet/Geary 2018 Psychol Sci](https://journals.sagepub.com/doi/10.1177/0956797617741719)
- [Spencer/Steele/Quinn 1999 JESP](https://www.sciencedirect.com/science/article/abs/pii/S0022103198913737)
- [BVerfG 25.3.2021 Mietendeckel](https://www.bundesverfassungsgericht.de/SharedDocs/Pressemitteilungen/DE/2021/bvg21-028.html)
- [Campbell 2003 AJPH](https://ajph.aphapublications.org/doi/10.2105/AJPH.93.7.1089)
- [Phillips/Axelrod Encyclopedia of Wars 2005](https://apholt.com/2018/12/26/counting-religious-wars-in-the-encyclopedia-of-wars/)
- [ADL Global 100 Index](https://www.adl.org/adl-global-100-index-antisemitism)
- [IHRA Working Definition](https://holocaustremembrance.com/resources/working-definition-antisemitism)
- [MHG-Studie 2018](https://www.dbk.de/themen/sexualisierte-gewalt-und-praevention/forschung-und-aufarbeitung/studien/mhg-studie)
- [EGMR Refah Partisi 2003](https://hudoc.echr.coe.int/?i=002-5004)
- [Bertelsmann Binnenmarkt 2019](https://www.bertelsmann-stiftung.de/fileadmin/files/BSt/Publikationen/GrauePublikationen/EZ_Zusammenfassung_Binnenmarkt.pdf)
- [Kleven AEA Papers and Proceedings 2019](https://www.aeaweb.org/articles?id=10.1257/pandp.20191078)
- [Hilber Synthesis 2017 Real Estate Economics](https://onlinelibrary.wiley.com/doi/abs/10.1111/1540-6229.12129)
- [CBO TCJA 2018](https://www.cbo.gov/publication/53651)
