const TRANSLATIONS = {
    de: {
        // Meta
        page_title: "Evidora.eu — Faktencheck",
        tagline: "Europäischer Faktencheck gegen Fake-News",

        // Search
        placeholder: 'Behauptung eingeben, z.B. "Impfungen verursachen Autismus"',
        search_btn: "Faktencheck",
        hint_health: "Gesundheit",
        hint_climate: "Klima",
        hint_economy: "Wirtschaft",
        hint_education: "Bildung",
        hint_migration: "Migration",
        hint_gender: "Gleichstellung",
        // Clickable example claims for the home page (Frontend-Polish A)
        examples_label: "Oder gleich ausprobieren:",
        example_claims: [
            { icon: "💉", text: "Impfungen verursachen Autismus." },
            { icon: "🌡️", text: "2024 war das wärmste Jahr in Wien seit Beginn der Aufzeichnungen." },
            { icon: "🚭", text: "Das Don't-smoke-Volksbegehren 2018 hatte über eine Million Unterschriften." },
            { icon: "📡", text: "Krebs wird hauptsächlich durch Handy-Strahlung verursacht." },
            { icon: "⚛️", text: "Atomkraft ist die sicherste Form der Energieerzeugung." },
            { icon: "❄️", text: "Wärmepumpen funktionieren in österreichischen Wintern auch bei Frost." },
        ],
        beta_notice: "Dieses Projekt befindet sich in aktiver Entwicklung.",
        beta_notice_online: "Dieses Projekt befindet sich in aktiver Entwicklung. Die KI-Analyse dieser Online-Version erfolgt über die Mistral Cloud API (EU-Server, Paris).",

        // Loading
        loading_analyze: "Analysiere Behauptung...",
        loading_search: "Durchsuche Quellen...",
        loading_synthesize: "Erstelle Bewertung...",
        step_analyze: "Claim-Analyse",
        step_search: "Quellen durchsuchen",
        step_synthesize: "Ergebnis erstellen",

        // Verdicts
        verdict_true: "Wahr",
        verdict_mostly_true: "Größtenteils wahr",
        verdict_mixed: "Gemischt",
        verdict_mostly_false: "Größtenteils falsch",
        verdict_false: "Falsch",
        verdict_unverifiable: "Nicht überprüfbar",

        // Source coverage
        source_coverage: "Quellenabdeckung:",
        source_coverage_detail: "{with} von {total} Quellen lieferten Ergebnisse",
        source_coverage_low: "Geringe Quellenabdeckung — Bewertung basiert auf wenigen Quellen. Ergebnis mit Vorsicht interpretieren.",
        source_coverage_single: "Nur eine einzige Quelle hat Ergebnisse geliefert. Die Bewertung ist daher wenig belastbar.",
        source_coverage_none: "Keine Quelle hat Ergebnisse geliefert. Die Behauptung konnte nicht überprüft werden.",

        // Evidence
        confidence: "Konfidenz:",
        confidence_tooltip: "Die Konfidenz gibt an, wie sicher das KI-Modell bei seiner Einschätzung ist — basierend auf der Eindeutigkeit der gefundenen Belege. Hohe Konfidenz (≥90%) bedeutet klare Datenlage; niedrige Konfidenz bedeutet widersprüchliche oder unvollständige Evidenz.",
        evidence_title: "Evidenz",
        strength_strong: "Stark",
        strength_moderate: "Mittel",
        strength_weak: "Schwach",
        sources_title: "Quellen im Detail",
        source_fallback: "Quelle",

        // Error
        error_title: "Fehler",
        error_retry: "Erneut versuchen",
        error_server: "Server-Fehler",
        error_claim_too_short: "Behauptung zu kurz — bitte mindestens 2 Wörter eingeben.",
        error_claim_too_long: "Behauptung zu lang — maximal 500 Zeichen erlaubt.",
        chars_remaining: "Zeichen verbleibend",
        tips_btn: "Tipps",
        tips_motto: "\u201eWas sich überhaupt sagen lässt, lässt sich klar sagen.\u201c — Wittgenstein",
        tips_1: "<strong>Konkret statt vage:</strong> \u201eImpfungen sind gefährlich\u201c → \u201emRNA-Impfstoffe erhöhen das Herzinfarktrisiko\u201c",
        tips_2: "<strong>Zahlen & Zeitraum nennen:</strong> \u201eDie Inflation ist hoch\u201c → \u201eDie Inflation in der EU lag 2024 über 5\u202f%\u201c",
        tips_3: "<strong>Eine Behauptung pro Check:</strong> Nicht mehrere Aussagen in einem Satz kombinieren.",
        tips_4: "<strong>Quelle weglassen:</strong> \u201eLaut Twitter sind ...\u201c → einfach die Behauptung selbst eingeben.",
        claim_display_label: "Geprüfte Behauptung",
        error_rate_limit: "Zu viele Anfragen. Bitte warte einen Moment.",
        error_empty: "Behauptung darf nicht leer sein.",
        error_credits_exhausted: "Der KI-Dienst ist vorübergehend nicht verfügbar (API-Guthaben aufgebraucht). Bitte versuche es später erneut.",

        // Disclaimer
        disclaimer_default: "Dies ist eine automatische Überprüfung und ersetzt keine professionelle Faktencheck-Redaktion.",

        // Actions
        btn_export_pdf: "Als PDF speichern",
        btn_share: "Link kopieren",
        share_copied: "Link kopiert!",
        // Toast messages (Frontend-Polish C)
        toast_pdf: "PDF wird vorbereitet …",
        toast_link_copied: "Link in die Zwischenablage kopiert.",
        toast_link_failed: "Link konnte nicht kopiert werden.",
        toast_mail_opened: "E-Mail-Programm wird geöffnet.",
        btn_report: "Ergebnis melden",
        report_subject: "Fehlerhaftes Ergebnis",
        report_claim: "Behauptung",
        report_verdict: "Bewertung",
        report_summary: "Zusammenfassung",
        report_reason: "Was ist falsch am Ergebnis? (bitte beschreiben)",
        dev_notice: "Dieses Projekt befindet sich in aktiver Entwicklung. Die KI-Analyse dieser Online-Version erfolgt über die Mistral Cloud API (EU-Server, Paris).",

        // Search history
        history_title: "Letzte Checks",
        history_clear: "Verlauf löschen",

        // Footer
        footer_main: "Evidora.eu — Europäischer Faktencheck",
        footer_note: "Automatische Überprüfung — ersetzt keine professionelle Faktencheck-Redaktion",
        footer_opensource: "Open Source auf GitHub",
        privacy_link: "Datenschutz",
        imprint_link: "Impressum",
        sources_link: "Quellen",
        disclaimer_link: "Haftungsausschluss",
        sources_html: `
            <h2>Quellen</h2>
            <p>Evidora prüft Behauptungen anhand folgender wissenschaftlicher Datenbanken und offizieller Quellen:</p>
            <h3>Wissenschaftliche Datenbanken</h3>
            <ul>
                <li><a href="https://pubmed.ncbi.nlm.nih.gov/" target="_blank" rel="noopener">PubMed</a> — Biomedizinische Literatur (NIH/NLM)</li>
                <li><a href="https://www.cochranelibrary.com/" target="_blank" rel="noopener">Cochrane Library</a> — Systematische Reviews & Meta-Analysen</li>
                <li><a href="https://europepmc.org/" target="_blank" rel="noopener">Europe PMC</a> — Europäische biomedizinische Literatur</li>
                <li><a href="https://www.semanticscholar.org/" target="_blank" rel="noopener">Semantic Scholar</a> — KI-gestützte Literaturdatenbank (Allen Institute)</li>
                <li><a href="https://openalex.org/" target="_blank" rel="noopener">OpenAlex</a> — Offener Katalog wissenschaftlicher Arbeiten</li>
                <li><a href="https://clinicaltrials.gov/" target="_blank" rel="noopener">ClinicalTrials.gov</a> — Register klinischer Studien (NIH)</li>
            </ul>
            <h3>Offizielle Institutionen</h3>
            <ul>
                <li><a href="https://www.who.int/data/gho" target="_blank" rel="noopener">WHO</a> — Weltgesundheitsorganisation</li>
                <li><a href="https://gateway.euro.who.int/en/hfa-explorer/" target="_blank" rel="noopener">WHO Europe (HFA)</a> — Health for All Explorer</li>
                <li><a href="https://www.ema.europa.eu/" target="_blank" rel="noopener">EMA</a> — Europäische Arzneimittel-Agentur</li>
                <li><a href="https://www.efsa.europa.eu/" target="_blank" rel="noopener">EFSA</a> — Europäische Behörde für Lebensmittelsicherheit</li>
                <li><a href="https://www.ecdc.europa.eu/" target="_blank" rel="noopener">ECDC</a> — Europäisches Zentrum für Krankheitsprävention</li>
                <li><a href="https://ec.europa.eu/eurostat" target="_blank" rel="noopener">Eurostat</a> — Statistisches Amt der EU</li>
                <li><a href="https://www.ecb.europa.eu/" target="_blank" rel="noopener">EZB</a> — Europäische Zentralbank</li>
                <li><a href="https://www.eea.europa.eu/" target="_blank" rel="noopener">EEA</a> — Europäische Umweltagentur</li>
                <li><a href="https://www.copernicus.eu/" target="_blank" rel="noopener">Copernicus</a> — EU-Erdbeobachtungsprogramm</li>
                <li><a href="https://www.oecd.org/" target="_blank" rel="noopener">OECD</a> — Organisation für wirtschaftliche Zusammenarbeit</li>
                <li><a href="https://www.unhcr.org/" target="_blank" rel="noopener">UNHCR</a> — UN-Flüchtlingshilfswerk</li>
                <li><a href="https://data.worldbank.org/" target="_blank" rel="noopener">World Bank</a> — Weltbank Open Data</li>
                <li><a href="https://ourworldindata.org/" target="_blank" rel="noopener">OWID</a> — Our World in Data</li>
                <li><a href="https://data.statistik.gv.at/" target="_blank" rel="noopener">Statistik Austria</a> — Österreichische amtliche Statistiken (OGD)</li>
                <li><a href="https://data.hub.geosphere.at/" target="_blank" rel="noopener">GeoSphere Austria</a> — Klima-Stationsdaten (klima-v2-1y)</li>
                <li><a href="https://www.basg.gv.at/" target="_blank" rel="noopener">BASG</a> — Bundesamt für Sicherheit im Gesundheitswesen (AT-Arzneimittelwarnungen)</li>
                <li><a href="https://www.ris.bka.gv.at/" target="_blank" rel="noopener">RIS</a> — Rechtsinformationssystem des Bundes (Bundesgesetzblatt)</li>
                <li><a href="https://www.bmi.gv.at/411/start.aspx" target="_blank" rel="noopener">BMI Volksbegehren</a> — alle bundesweiten Volksbegehren der zweiten Republik</li>
                <li><a href="https://www.bmi.gv.at/412/start.aspx" target="_blank" rel="noopener">BMI Wahlen</a> — Bundesergebnisse aller Nationalrats-, Bundespräsidenten- und Europawahlen</li>
                <li><a href="https://www.parlament.gv.at/recherchieren/open-data/" target="_blank" rel="noopener">Parlament Abstimmungen</a> — Klub-Abstimmungsverhalten zu Nationalrats-Beschlüssen seit 2017</li>
                <li><strong>AT Factbook</strong> — kuratierte AT-Faktoide aus offiziellen Berichten (Bildungsdirektion Wien, BMF Förderungsbericht)</li>
                <li><a href="https://v-dem.net/" target="_blank" rel="noopener">V-Dem</a> — Varieties of Democracy (Universität Göteborg)</li>
                <li><a href="https://www.transparency.org/en/cpi" target="_blank" rel="noopener">Transparency International</a> — Corruption Perceptions Index</li>
                <li><a href="https://rsf.org/en/index" target="_blank" rel="noopener">Reporter ohne Grenzen (RSF)</a> — World Press Freedom Index</li>
                <li><a href="https://www.sipri.org/databases/milex" target="_blank" rel="noopener">SIPRI</a> — Military Expenditure Database</li>
                <li><a href="https://www.idea.int/data-tools/data/voter-turnout-database" target="_blank" rel="noopener">International IDEA</a> — Voter Turnout Database (Wahlbeteiligung)</li>
                <li><a href="https://www.parlament.gv.at/recherchieren/open-data/" target="_blank" rel="noopener">Parlament.gv.at</a> — Österreichisches Parlament Open Data (Nationalrats-Klubstärken)</li>
            </ul>
            <h3>Faktencheck-Netzwerke</h3>
            <ul>
                <li><a href="https://www.google.com/factcheck/tools" target="_blank" rel="noopener">DataCommons / ClaimReview</a> — Google Fact Check Tools</li>
                <li><a href="https://gadmo.eu/" target="_blank" rel="noopener">GADMO</a> — German-Austrian Digital Media Observatory</li>
                <li><a href="https://euvsdisinfo.eu/" target="_blank" rel="noopener">EUvsDisinfo</a> — EU-Datenbank gegen Desinformation</li>
                <li><a href="https://efcsn.com/" target="_blank" rel="noopener">EFCSN</a> — European Fact-Checking Standards Network</li>
            </ul>
        `,

        // Austria Edition
        austria_edition_link: "🇦🇹 Österreich Edition — alle AT-Quellen ansehen",
        austria_html: `
            <h2>🇦🇹 Österreich Edition</h2>
            <p>Evidora legt einen besonderen Fokus auf österreichische Datenquellen. Für Behauptungen mit Österreich-Bezug werden diese Quellen bevorzugt abgefragt und ausgewiesen.</p>

            <h3>Primäre österreichische Quellen</h3>
            <ul>
                <li><strong><a href="https://data.statistik.gv.at/" target="_blank" rel="noopener">Statistik Austria — Open Government Data</a></strong> (CC BY 4.0) — aktuell 7 Datasets integriert:
                    <ul>
                        <li>Verbraucherpreisindex (VPI, Basis 2020, monatlich, COICOP-Kategorien)</li>
                        <li>Gesundheitsausgaben (SHA-Methodik, 1995–2024)</li>
                        <li>Sterblichkeit nach Kalenderwoche (inkl. Übersterblichkeits-Baseline 2015–2019)</li>
                        <li>Volkswirtschaftliche Gesamtrechnung / BIP (ESA 2010, 1995–2025)</li>
                        <li>Wanderungsstatistik &amp; Einbürgerungen (Bundesland-Detail)</li>
                        <li>Arbeitsmarkt / Arbeitskräfteerhebung (ILO-Arbeitslosenquote, NUTS2 × Alter/Geschlecht)</li>
                        <li>Armut &amp; Ungleichheit (EU-SILC: AROPE, Gini, S80/S20)</li>
                    </ul>
                </li>
                <li><strong><a href="https://www.parlament.gv.at/recherchieren/open-data/" target="_blank" rel="noopener">Parlament.gv.at — Open Data</a></strong> — Nationalrats-Klubstärken und Parlamentsdaten</li>
                <li><strong><a href="https://data.hub.geosphere.at/" target="_blank" rel="noopener">GeoSphere Austria — Data Hub</a></strong> (CC BY 4.0) — Stationsmessungen für die neun Bundesland-Hauptstädte (Wien, Salzburg, Innsbruck, Graz, Linz, Klagenfurt, Bregenz, Eisenstadt, St. Pölten), jährliche Lufttemperatur-Mittelwerte mit WMO-Referenzperiode 1991–2020 und linearem Trend</li>
                <li><strong><a href="https://www.basg.gv.at/marktbeobachtung/amtliche-nachrichten" target="_blank" rel="noopener">BASG — Bundesamt für Sicherheit im Gesundheitswesen</a></strong> — RSS-Feed der amtlichen Nachrichten zu Arzneimittel-Rückrufen, Chargensperren, Sicherheitsinformationen (DHPC) und Medizinprodukt-Warnungen. Trigger nur bei pharmakologischen Claims mit Österreich-Bezug</li>
                <li><strong><a href="https://www.ris.bka.gv.at/" target="_blank" rel="noopener">RIS — Rechtsinformationssystem des Bundes</a></strong> — Live-Suche im Bundesgesetzblatt (BGBl) via offizieller BKA-API. Liefert pro Treffer: Kurztitel, Volltitel, BGBl-Nummer, Ausgabedatum und ELI-URL (zitierfähig). Trigger nur bei juristischen Claims mit Österreich-Bezug und extrahierbarem Suchterm. Caveat: BGBl-Kundmachungen ≠ konsolidierte aktuelle Fassung</li>
                <li><strong><a href="https://www.bmi.gv.at/411/Alle_Volksbegehren_der_zweiten_Republik.aspx" target="_blank" rel="noopener">BMI — Volksbegehren der zweiten Republik</a></strong> — vollständige Liste aller bundesweiten Volksbegehren seit 1964 (Bundesministerium für Inneres, Abt. III/6 Wahlangelegenheiten). Pro Eintrag: Jahr, Betreff, Eintragungszeitraum, Anzahl gültiger Eintragungen, Stimmbeteiligung in %, Rang und Initiator/Unterstützung. Suche per Wort-Overlap und „Top"-Modus (erfolgreichstes / höchste Beteiligung / neuestes / ältestes Volksbegehren). Trigger nur bei AT-Kontext oder bekanntem Volksbegehren-Namen. Caveat: nur Bundes-Volksbegehren — Landes-Volksbegehren werden hier nicht abgedeckt; Volksbegehren ≠ Volksabstimmung ≠ Volksbefragung</li>
                <li><strong><a href="https://www.bmi.gv.at/412/start.aspx" target="_blank" rel="noopener">BMI — Wahlergebnisse (Bundeswahlbehörde)</a></strong> — Bundesergebnisse aller österreichischen Bundeswahlen seit 1986/1996: 12 Nationalratswahlen (1986–2024), 5 Bundespräsidentenwahlen (1998–2022), 7 Europawahlen (1996–2024). Pro Wahl: vollständige Parteinamen, Kurzkürzel, Stimmen, Prozent-Anteil und Mandate (wo verfügbar). Trigger nur bei explizitem Wahltyp + AT-Kontext, oder bei Partei/Kandidat + Jahr + AT. Caveats: nur Bundesergebnisse (keine Landes-/Wahlkreis-Detail-Aufschlüsselung); BPW 2016 zeigt nur die Wahl-Wiederholung vom 4. Dezember 2016; keine Wahlprognosen, keine politische Bewertung der Parteien</li>
                <li><strong><a href="https://www.parlament.gv.at/recherchieren/open-data/daten-und-lizenz/beschluesse" target="_blank" rel="noopener">Parlament — Abstimmungsverhalten (Open Data)</a></strong> — Klub-spezifisches Abstimmungsverhalten im Nationalrat seit GP XXVI (2017), aktuell ~1.260 Beschlüsse mit Voting-Daten. Pro Beschluss: Datum, Betreff, DOKTYP (RV/A/BUA/BRA), 3.-Lesungs-Ergebnis und welche Klubs dafür/dagegen waren (z.B. ÖVP, SPÖ, FPÖ, GRÜNE, NEOS). Erlaubt Behauptungen wie „FPÖ war einzige Gegenstimme bei X" oder „Wer hat 2023 dem Y zugestimmt?" zu prüfen. Trigger: Voting-Keyword + AT-Kontext. Caveats: nur Nationalrat (kein Bundesrat); seit 2017 (ältere Beschlüsse fehlen); nur DOKTYPs RV/A/BUA/BRA (Petitionen/Volksbegehren-Verhandlungen sind separat). Keine politische Bewertung des Klub-Verhaltens — nur Stimmenverteilung</li>
            </ul>

            <h3>Faktencheck-Partner mit AT-Bezug</h3>
            <ul>
                <li><a href="https://faktencheck.apa.at/" target="_blank" rel="noopener">APA-Faktencheck</a> — Österreichische Presse-Agentur (via GADMO-Feed)</li>
                <li><a href="https://www.mimikama.at/" target="_blank" rel="noopener">Mimikama</a> — österreichischer Verein zur Aufklärung über Internetmissbrauch (via GADMO-Feed)</li>
                <li><a href="https://gadmo.eu/" target="_blank" rel="noopener">GADMO</a> — German-Austrian Digital Media Observatory</li>
            </ul>

            <h3>Internationale Quellen mit AT-Daten</h3>
            <p>Auch folgende internationale Quellen liefern österreichspezifische Daten und werden bei AT-Claims genutzt:</p>
            <ul>
                <li>Eurostat — EU-Statistik inkl. AT-Daten</li>
                <li>WHO Europe (HFA) — Gesundheitsdaten für Österreich</li>
                <li>OECD — Wirtschafts- und Sozialdaten</li>
                <li>EZB — Finanz- und Zinsdaten</li>
                <li>OWID / World Bank — globale Zeitreihen mit AT-Filter</li>
            </ul>

            <p style="margin-top: 20px; font-size: 0.85rem; color: #6b7280;">Die Österreich Edition ist ein inhaltlicher Schwerpunkt, keine separate Version — alle europäischen und wissenschaftlichen Quellen bleiben unverändert verfügbar. Eine vollständige Übersicht aller Quellen findest du unter „<a href="#" onclick="closeModal(event); openModal('sources', event)">Quellen</a>".</p>
        `,

        // Imprint
        imprint_html_configured: `
            <h2>Impressum</h2>
            <h3>Angaben gemäß § 5 ECG</h3>
            <p><strong>{name}</strong></p>
            <p>{location}</p>
            <p>E-Mail: {email}</p>
            <h3>Haftung für Inhalte</h3>
            <p>Die Ergebnisse von Evidora werden automatisch generiert und stellen keine redaktionelle Bewertung dar. Für die Richtigkeit, Vollständigkeit und Aktualität der Ergebnisse wird keine Gewähr übernommen.</p>
            <h3>Haftung für Links</h3>
            <p>Evidora verlinkt auf externe Quellen (PubMed, WHO, EMA etc.). Für deren Inhalte sind ausschließlich die jeweiligen Betreiber verantwortlich.</p>
        `,
        imprint_html_unconfigured: `
            <h2>Impressum</h2>
            <p>Der Betreiber dieser Instanz hat noch kein Impressum konfiguriert.</p>
            <p>Wenn du diese Instanz betreibst, setze die folgenden Umgebungsvariablen in deiner <code>.env</code>-Datei:</p>
            <ul>
                <li><code>IMPRESSUM_NAME</code> — Dein Name</li>
                <li><code>IMPRESSUM_EMAIL</code> — Deine E-Mail-Adresse</li>
                <li><code>IMPRESSUM_LOCATION</code> — Dein Standort</li>
            </ul>
        `,

        // Disclaimer
        disclaimer_page_html: `
            <h2>Haftungsausschluss</h2>

            <h3>1. Automatisierter Dienst</h3>
            <p>Evidora ist ein automatisiertes Faktencheck-Tool. Die Ergebnisse werden von einem Sprachmodell (LLM) auf Basis öffentlicher Datenquellen generiert. <strong>Sie stellen keine redaktionelle, wissenschaftliche oder rechtliche Bewertung dar.</strong></p>

            <h3>2. Keine Gewährleistung</h3>
            <p>Für die Richtigkeit, Vollständigkeit und Aktualität der angezeigten Ergebnisse wird keine Gewähr übernommen. Fehlerhafte Bewertungen sind möglich, insbesondere bei:</p>
            <ul>
                <li>Mehrdeutigen oder komplexen Behauptungen</li>
                <li>Themen außerhalb der abgedeckten Datenquellen</li>
                <li>Aktuellen Ereignissen, die noch nicht in den Quellen erfasst sind</li>
            </ul>

            <h3>3. Keine Entscheidungsgrundlage</h3>
            <p>Die Ergebnisse von Evidora sollten <strong>nicht als alleinige Grundlage</strong> für persönliche, medizinische, finanzielle oder politische Entscheidungen verwendet werden. Ziehe im Zweifelsfall professionelle Quellen oder Fachpersonen zu Rate.</p>

            <h3>4. Externe Datenquellen</h3>
            <p>Evidora leitet Suchanfragen an externe APIs weiter (u.a. PubMed, WHO, EMA, Eurostat, EZB, UNHCR, EEA, ECDC, Copernicus, Google Fact Check). Für deren Verfügbarkeit, Richtigkeit und Vollständigkeit übernimmt Evidora keine Verantwortung.</p>
            <p>Je nach Konfiguration erfolgt die KI-Analyse lokal (Ollama) oder über die <strong>Mistral Cloud API</strong> (EU-Server, Paris). Details siehe <a href="#" onclick="openModal('privacy', event)">Datenschutzerklärung</a>.</p>

            <h3>5. Open Source</h3>
            <p>Die Software wird unter der MIT-Lizenz bereitgestellt — <strong>"as is", ohne Garantie jeglicher Art</strong>. Details siehe <a href="https://opensource.org/licenses/MIT" target="_blank" rel="noopener">MIT License</a>.</p>
        `,

        // Privacy
        privacy_html: `
            <h2>Datenschutzerklärung</h2>

            <h3>1. Überblick</h3>
            <p>Der Schutz deiner Daten ist uns wichtig. Diese Seite erklärt, welche Daten verarbeitet werden — und welche nicht.</p>

            <h3>2. Was wir NICHT tun</h3>
            <ul>
                <li>Keine Cookies (weder Tracking- noch Werbe-Cookies)</li>
                <li>Keine Nutzerkonten oder Registrierung</li>
                <li>Keine Analyse-Tools (kein Google Analytics o.Ä.)</li>
                <li>Keine Weitergabe von Daten an Dritte zu Werbezwecken</li>
                <li>Keine dauerhafte Speicherung deiner Eingaben</li>
            </ul>

            <h3>3. Welche Daten verarbeitet werden</h3>

            <h4>a) Deine Behauptung</h4>
            <p>Wenn du eine Behauptung eingibst, wird diese an unser Backend gesendet und dort verarbeitet. Die KI-Analyse erfolgt je nach Konfiguration durch:</p>
            <ul>
                <li><strong>Lokales Sprachmodell (Mistral 7B via Ollama)</strong> — deine Eingabe verlässt nicht unsere Infrastruktur</li>
                <li><strong>Mistral Cloud API (EU-Server, Paris)</strong> — deine Eingabe wird an Mistral AI (Frankreich) übermittelt. Mistral verarbeitet die Daten gemäß ihrer <a href="https://mistral.ai/terms/#privacy-policy" target="_blank" rel="noopener">Datenschutzrichtlinie</a> auf EU-Servern.</li>
            </ul>
            <p>In beiden Fällen werden deine Eingaben <strong>nicht dauerhaft gespeichert</strong> — weder bei uns noch bei Mistral (API-Modus: keine Trainingsnutzung laut Mistral-Richtlinien).</p>

            <h4>b) Externe Quellenabfragen</h4>
            <p>Um Fakten zu prüfen, werden <strong>Suchanfragen</strong> (nicht deine exakte Eingabe, sondern extrahierte Suchbegriffe) an folgende öffentliche APIs gesendet:</p>
            <ul>
                <li>PubMed (NIH, USA) — biomedizinische Studien</li>
                <li>WHO GHO — Gesundheitsdaten</li>
                <li>EMA — Medikamentendaten (EU)</li>
                <li>Copernicus CDS (ECMWF/EU) — Klimadaten</li>
                <li>Eurostat (EU) — europäische Statistiken</li>
                <li>EZB (EU) — Leitzinsen, Wechselkurse, Geldmenge</li>
                <li>UNHCR — Weltweite Flüchtlings- und Asylstatistiken</li>
                <li>EEA (EU) — Umweltdaten</li>
                <li>ECDC (EU) — Infektionskrankheiten</li>
                <li>Cochrane Reviews — Systematische Reviews (via PubMed)</li>
                <li>GADMO/APA — Deutschsprachige Faktenchecks</li>
                <li>EFCSN-Faktenchecker — bestehende Faktenchecks</li>
            </ul>
            <p>Diese Dienste unterliegen ihren eigenen Datenschutzrichtlinien. Es werden keine personenbezogenen Daten an sie übermittelt.</p>

            <h4>c) Spracheinstellung</h4>
            <p>Deine gewählte Sprache (DE/EN) wird im <strong>localStorage</strong> deines Browsers gespeichert, damit sie beim nächsten Besuch erhalten bleibt. Dies ist kein Cookie und wird nicht an den Server gesendet.</p>

            <h4>d) Server-Logs</h4>
            <p>Unser Backend protokolliert anonymisierte technische Daten (Fehlermeldungen, Kategorie der Anfrage, Antwortzeiten). <strong>Deine eingegebenen Behauptungen werden nicht geloggt.</strong> Logs werden beim Neustart des Servers automatisch gelöscht.</p>

            <h3>4. Rechtsgrundlage</h3>
            <p>Die Verarbeitung erfolgt auf Basis von Art. 6 Abs. 1 lit. f DSGVO (berechtigtes Interesse: Bereitstellung des Dienstes). Da keine personenbezogenen Daten gespeichert werden, fallen keine Auskunfts- oder Löschrechte an.</p>

            <h3>5. Datenquellen & Lizenzen</h3>
            <p>Evidora nutzt ausschließlich öffentliche, frei zugängliche Datenquellen:</p>
            <ul>
                <li><strong>PubMed / NCBI</strong> — öffentliche Datenbank der U.S. National Library of Medicine. Nutzung gemäß <a href="https://www.ncbi.nlm.nih.gov/home/about/policies/" target="_blank" rel="noopener">NCBI Policies</a></li>
                <li><strong>WHO GHO</strong> — offene Gesundheitsdaten der Weltgesundheitsorganisation. <a href="https://www.who.int/about/policies/publishing/copyright" target="_blank" rel="noopener">WHO Copyright Policy</a></li>
                <li><strong>EMA</strong> — offene Medikamentendaten der Europäischen Arzneimittel-Agentur (CC BY 4.0). <a href="https://www.ema.europa.eu/en/about-us/legal-notice" target="_blank" rel="noopener">EMA Legal Notice</a></li>
                <li><strong>EFSA</strong> — wissenschaftliche Gutachten der Europäischen Behörde für Lebensmittelsicherheit via CrossRef/EFSA Journal. <a href="https://www.efsa.europa.eu/en/about/legal" target="_blank" rel="noopener">EFSA Legal Notice</a></li>
                <li><strong>Copernicus CDS</strong> — Klimadaten des ECMWF/EU. <a href="https://cds.climate.copernicus.eu/datasets" target="_blank" rel="noopener">Copernicus Licence</a></li>
                <li><strong>Eurostat</strong> — offene EU-Statistiken (CC BY 4.0). <a href="https://ec.europa.eu/eurostat/web/main/help/copyright-notice" target="_blank" rel="noopener">Eurostat Copyright</a></li>
                <li><strong>EZB</strong> — Leitzinsen, Wechselkurse und Geldmengen der Europäischen Zentralbank. <a href="https://data.ecb.europa.eu/help/api/overview" target="_blank" rel="noopener">ECB Data Portal</a></li>
                <li><strong>UNHCR</strong> — Flüchtlings- und Asylstatistiken des UN-Flüchtlingshilfswerks. <a href="https://www.unhcr.org/refugee-statistics/" target="_blank" rel="noopener">UNHCR Refugee Data</a></li>
                <li><strong>EEA</strong> — Umweltdaten der Europäischen Umweltagentur. <a href="https://www.eea.europa.eu/en/legal-notice" target="_blank" rel="noopener">EEA Legal Notice</a></li>
                <li><strong>ECDC</strong> — Surveillance-Daten des European Centre for Disease Prevention and Control. <a href="https://www.ecdc.europa.eu/en/copyright" target="_blank" rel="noopener">ECDC Copyright</a></li>
                <li><strong>Statistik Austria</strong> — amtliche österreichische Statistiken (CC BY 4.0). <a href="https://data.statistik.gv.at/" target="_blank" rel="noopener">Statistik Austria OGD</a></li>
                <li><strong>GeoSphere Austria</strong> — Klima-Stationsdaten der Bundesanstalt für Meteorologie (CC BY 4.0). <a href="https://data.hub.geosphere.at/" target="_blank" rel="noopener">GeoSphere Data Hub</a></li>
                <li><strong>BASG</strong> — amtliche Mitteilungen des Bundesamts für Sicherheit im Gesundheitswesen (gemeinfrei, § 7 UrhG-AT). <a href="https://www.basg.gv.at/marktbeobachtung/amtliche-nachrichten" target="_blank" rel="noopener">BASG Amtliche Nachrichten</a></li>
                <li><strong>RIS</strong> — Rechtsinformationssystem des Bundes (BKA) via Open-Data-API (PSI/OGD, Attribution erforderlich). <a href="https://data.bka.gv.at/" target="_blank" rel="noopener">RIS Open Data</a></li>
                <li><strong>BMI Volksbegehren</strong> — amtliche Liste aller bundesweiten Volksbegehren der zweiten Republik (BMI, Abt. III/6 Wahlangelegenheiten; gemeinfrei, § 7 UrhG-AT). <a href="https://www.bmi.gv.at/411/Alle_Volksbegehren_der_zweiten_Republik.aspx" target="_blank" rel="noopener">BMI Volksbegehren</a></li>
                <li><strong>BMI Wahlen / Bundeswahlbehörde</strong> — amtliche Bundesergebnisse aller österreichischen Bundeswahlen (Nationalrats-, Bundespräsidenten-, Europawahlen; gemeinfrei, § 7 UrhG-AT). <a href="https://www.bmi.gv.at/412/start.aspx" target="_blank" rel="noopener">BMI Wahlen</a></li>
                <li><strong>Parlament Abstimmungen</strong> — Klub-Abstimmungsverhalten zu Nationalrats-Beschlüssen aus dem Open-Data-Portal des Parlaments (CC BY 4.0). <a href="https://www.parlament.gv.at/recherchieren/open-data/" target="_blank" rel="noopener">Parlament Open Data</a></li>
                <li><strong>AT Factbook</strong> — kuratierte österreichische Faktoide aus offiziellen Primärquellen (Bildungsdirektion Wien zu Religionsbekenntnissen an Wiener Pflichtschulen; BMF-Förderungsbericht zu Bundesförderungen; Statistik Austria zu ESVG-Förderquote). Manuell aktualisiert; ergänzt API-lose AT-Statistiken.</li>
                <li><strong>Cochrane Reviews</strong> — Systematische Reviews via PubMed (höchste Evidenzstufe)</li>
                <li><strong>GADMO Faktenchecks</strong> — Deutschsprachige Faktenchecks (APA). <a href="https://gadmo.eu" target="_blank" rel="noopener">GADMO</a></li>
                <li><strong>EFCSN-Faktenchecker</strong> — über die <a href="https://developers.google.com/fact-check/tools/api" target="_blank" rel="noopener">Google Fact Check Tools API</a> (ClaimReview-Daten von Correctiv, AFP, dpa u.a.)</li>
            </ul>
            <p>Suchicon: <a href="https://github.com/google/material-design-icons" target="_blank" rel="noopener">Google Material Design Icons</a> (Apache 2.0 Lizenz).</p>
        `,
    },

    en: {
        // Meta
        page_title: "Evidora.eu — Fact Check",
        tagline: "European fact-checking against fake news",

        // Search
        placeholder: 'Enter a claim, e.g. "Vaccines cause autism"',
        search_btn: "Fact Check",
        hint_health: "Health",
        hint_climate: "Climate",
        hint_economy: "Economy",
        hint_education: "Education",
        hint_migration: "Migration",
        hint_gender: "Gender Equality",
        // Clickable example claims for the home page
        examples_label: "Or try it directly:",
        example_claims: [
            { icon: "💉", text: "Vaccines cause autism." },
            { icon: "🌡️", text: "2024 was the warmest year on record globally." },
            { icon: "📡", text: "Cancer is mainly caused by mobile phone radiation." },
            { icon: "⚛️", text: "Nuclear power is the safest form of energy generation." },
            { icon: "📊", text: "The EU's CO2 emissions decreased by 30% since 1990." },
            { icon: "🏥", text: "Life expectancy in Italy is higher than in Germany." },
        ],
        beta_notice: "This project is under active development.",
        beta_notice_online: "This project is under active development. AI analysis on this online version is processed via the Mistral Cloud API (EU servers, Paris).",

        // Loading
        loading_analyze: "Analyzing claim...",
        loading_search: "Searching sources...",
        loading_synthesize: "Creating verdict...",
        step_analyze: "Claim analysis",
        step_search: "Search sources",
        step_synthesize: "Generate result",

        // Verdicts
        verdict_true: "True",
        verdict_mostly_true: "Mostly true",
        verdict_mixed: "Mixed",
        verdict_mostly_false: "Mostly false",
        verdict_false: "False",
        verdict_unverifiable: "Unverifiable",

        // Source coverage
        source_coverage: "Source coverage:",
        source_coverage_detail: "{with} of {total} sources returned results",
        source_coverage_low: "Low source coverage — verdict is based on few sources. Interpret with caution.",
        source_coverage_single: "Only a single source returned results. The verdict is therefore not very robust.",
        source_coverage_none: "No source returned results. The claim could not be verified.",

        // Evidence
        confidence: "Confidence:",
        confidence_tooltip: "Confidence indicates how certain the AI model is about its assessment — based on the clarity of the evidence found. High confidence (≥90%) means a clear evidence base; low confidence means contradictory or incomplete evidence.",
        evidence_title: "Evidence",
        strength_strong: "Strong",
        strength_moderate: "Moderate",
        strength_weak: "Weak",
        sources_title: "Sources in Detail",
        source_fallback: "Source",

        // Error
        error_title: "Error",
        error_retry: "Try again",
        error_server: "Server error",
        error_claim_too_short: "Claim too short — please enter at least 2 words.",
        error_claim_too_long: "Claim too long — maximum 500 characters allowed.",
        chars_remaining: "characters remaining",
        tips_btn: "Tips",
        tips_motto: "\u201cWhereof one can speak, thereof one must speak clearly.\u201d — Wittgenstein",
        tips_1: "<strong>Be specific:</strong> \u201cVaccines are dangerous\u201d → \u201cmRNA vaccines increase heart attack risk\u201d",
        tips_2: "<strong>Include numbers & timeframe:</strong> \u201cInflation is high\u201d → \u201cEU inflation exceeded 5% in 2024\u201d",
        tips_3: "<strong>One claim per check:</strong> Don\u2019t combine multiple statements in one sentence.",
        tips_4: "<strong>Drop the source:</strong> \u201cAccording to Twitter ...\u201d → just enter the claim itself.",
        claim_display_label: "Claim checked",
        error_rate_limit: "Too many requests. Please wait a moment.",
        error_empty: "Claim must not be empty.",
        error_credits_exhausted: "The AI service is temporarily unavailable (API credits exhausted). Please try again later.",

        // Disclaimer
        disclaimer_default: "This is an automated check and does not replace professional fact-checking.",

        // Actions
        btn_export_pdf: "Save as PDF",
        btn_share: "Copy link",
        share_copied: "Link copied!",
        // Toast messages
        toast_pdf: "Preparing PDF …",
        toast_link_copied: "Link copied to clipboard.",
        toast_link_failed: "Could not copy link.",
        toast_mail_opened: "Opening your email client.",
        btn_report: "Report result",
        report_subject: "Incorrect result",
        report_claim: "Claim",
        report_verdict: "Verdict",
        report_summary: "Summary",
        report_reason: "What is wrong with the result? (please describe)",
        dev_notice: "This project is under active development. The AI analysis of this online version uses the Mistral Cloud API (EU servers, Paris).",

        // Search history
        history_title: "Recent checks",
        history_clear: "Clear history",

        // Footer
        footer_main: "Evidora.eu — European Fact Check",
        footer_note: "Automated check — does not replace professional fact-checking",
        footer_opensource: "Open Source on GitHub",
        privacy_link: "Privacy Policy",
        imprint_link: "Legal Notice",
        sources_link: "Sources",
        disclaimer_link: "Disclaimer",
        sources_html: `
            <h2>Sources</h2>
            <p>Evidora checks claims against the following scientific databases and official sources:</p>
            <h3>Scientific Databases</h3>
            <ul>
                <li><a href="https://pubmed.ncbi.nlm.nih.gov/" target="_blank" rel="noopener">PubMed</a> — Biomedical literature (NIH/NLM)</li>
                <li><a href="https://www.cochranelibrary.com/" target="_blank" rel="noopener">Cochrane Library</a> — Systematic reviews & meta-analyses</li>
                <li><a href="https://europepmc.org/" target="_blank" rel="noopener">Europe PMC</a> — European biomedical literature</li>
                <li><a href="https://www.semanticscholar.org/" target="_blank" rel="noopener">Semantic Scholar</a> — AI-powered literature database (Allen Institute)</li>
                <li><a href="https://openalex.org/" target="_blank" rel="noopener">OpenAlex</a> — Open catalog of scholarly works</li>
                <li><a href="https://clinicaltrials.gov/" target="_blank" rel="noopener">ClinicalTrials.gov</a> — Clinical trials registry (NIH)</li>
            </ul>
            <h3>Official Institutions</h3>
            <ul>
                <li><a href="https://www.who.int/data/gho" target="_blank" rel="noopener">WHO</a> — World Health Organization</li>
                <li><a href="https://gateway.euro.who.int/en/hfa-explorer/" target="_blank" rel="noopener">WHO Europe (HFA)</a> — Health for All Explorer</li>
                <li><a href="https://www.ema.europa.eu/" target="_blank" rel="noopener">EMA</a> — European Medicines Agency</li>
                <li><a href="https://www.efsa.europa.eu/" target="_blank" rel="noopener">EFSA</a> — European Food Safety Authority</li>
                <li><a href="https://www.ecdc.europa.eu/" target="_blank" rel="noopener">ECDC</a> — European Centre for Disease Prevention and Control</li>
                <li><a href="https://ec.europa.eu/eurostat" target="_blank" rel="noopener">Eurostat</a> — Statistical Office of the EU</li>
                <li><a href="https://www.ecb.europa.eu/" target="_blank" rel="noopener">ECB</a> — European Central Bank</li>
                <li><a href="https://www.eea.europa.eu/" target="_blank" rel="noopener">EEA</a> — European Environment Agency</li>
                <li><a href="https://www.copernicus.eu/" target="_blank" rel="noopener">Copernicus</a> — EU Earth Observation Programme</li>
                <li><a href="https://www.oecd.org/" target="_blank" rel="noopener">OECD</a> — Organisation for Economic Co-operation and Development</li>
                <li><a href="https://www.unhcr.org/" target="_blank" rel="noopener">UNHCR</a> — UN Refugee Agency</li>
                <li><a href="https://data.worldbank.org/" target="_blank" rel="noopener">World Bank</a> — World Bank Open Data</li>
                <li><a href="https://ourworldindata.org/" target="_blank" rel="noopener">OWID</a> — Our World in Data</li>
                <li><a href="https://data.statistik.gv.at/" target="_blank" rel="noopener">Statistik Austria</a> — Austrian official statistics (OGD)</li>
                <li><a href="https://data.hub.geosphere.at/" target="_blank" rel="noopener">GeoSphere Austria</a> — Climate station data (klima-v2-1y)</li>
                <li><a href="https://www.basg.gv.at/" target="_blank" rel="noopener">BASG</a> — Austrian Federal Office for Safety in Health Care (medicines safety alerts)</li>
                <li><a href="https://www.ris.bka.gv.at/" target="_blank" rel="noopener">RIS</a> — Federal Legal Information System (Bundesgesetzblatt / Federal Law Gazette)</li>
                <li><a href="https://www.bmi.gv.at/411/start.aspx" target="_blank" rel="noopener">BMI Popular Initiatives</a> — official list of all Austrian federal popular initiatives (Volksbegehren) since 1964</li>
                <li><a href="https://www.bmi.gv.at/412/start.aspx" target="_blank" rel="noopener">BMI Elections</a> — federal results of all Austrian National Council, Federal Presidential and European Parliament elections</li>
                <li><a href="https://www.parlament.gv.at/recherchieren/open-data/" target="_blank" rel="noopener">Parliamentary Voting Records</a> — club voting behaviour for National Council decisions since 2017</li>
                <li><strong>AT Factbook</strong> — curated Austrian fact records from official primary sources (Vienna school authority, Federal Ministry of Finance subsidy report)</li>
                <li><a href="https://v-dem.net/" target="_blank" rel="noopener">V-Dem</a> — Varieties of Democracy (University of Gothenburg)</li>
                <li><a href="https://www.transparency.org/en/cpi" target="_blank" rel="noopener">Transparency International</a> — Corruption Perceptions Index</li>
                <li><a href="https://rsf.org/en/index" target="_blank" rel="noopener">Reporters Without Borders (RSF)</a> — World Press Freedom Index</li>
                <li><a href="https://www.sipri.org/databases/milex" target="_blank" rel="noopener">SIPRI</a> — Military Expenditure Database</li>
                <li><a href="https://www.idea.int/data-tools/data/voter-turnout-database" target="_blank" rel="noopener">International IDEA</a> — Voter Turnout Database</li>
                <li><a href="https://www.parlament.gv.at/recherchieren/open-data/" target="_blank" rel="noopener">Parlament.gv.at</a> — Austrian Parliament Open Data (Nationalrat seat shares)</li>
            </ul>
            <h3>Fact-Checking Networks</h3>
            <ul>
                <li><a href="https://www.google.com/factcheck/tools" target="_blank" rel="noopener">DataCommons / ClaimReview</a> — Google Fact Check Tools</li>
                <li><a href="https://gadmo.eu/" target="_blank" rel="noopener">GADMO</a> — German-Austrian Digital Media Observatory</li>
                <li><a href="https://euvsdisinfo.eu/" target="_blank" rel="noopener">EUvsDisinfo</a> — EU database against disinformation</li>
                <li><a href="https://efcsn.com/" target="_blank" rel="noopener">EFCSN</a> — European Fact-Checking Standards Network</li>
            </ul>
        `,

        // Austria Edition (proper noun "Österreich Edition" kept in German; subtitle translated)
        austria_edition_link: "🇦🇹 Österreich Edition — view all Austrian data sources",
        austria_html: `
            <h2>🇦🇹 Österreich Edition</h2>
            <p>Evidora puts a special focus on Austrian data sources. For claims with an Austrian context, these sources are queried preferentially and clearly labelled.</p>

            <h3>Primary Austrian sources</h3>
            <ul>
                <li><strong><a href="https://data.statistik.gv.at/" target="_blank" rel="noopener">Statistik Austria — Open Government Data</a></strong> (CC BY 4.0) — currently 7 datasets integrated:
                    <ul>
                        <li>Consumer Price Index (CPI/VPI, base 2020, monthly, COICOP categories)</li>
                        <li>Health expenditure (SHA methodology, 1995–2024)</li>
                        <li>Weekly mortality (including excess-mortality baseline 2015–2019)</li>
                        <li>National accounts / GDP (ESA 2010, 1995–2025)</li>
                        <li>Migration statistics &amp; naturalisations (federal-state detail)</li>
                        <li>Labour market / Labour Force Survey (ILO unemployment rate, NUTS2 × age/sex)</li>
                        <li>Poverty &amp; inequality (EU-SILC: AROPE, Gini, S80/S20)</li>
                    </ul>
                </li>
                <li><strong><a href="https://www.parlament.gv.at/recherchieren/open-data/" target="_blank" rel="noopener">Parlament.gv.at — Open Data</a></strong> — National Council seat shares and parliamentary data</li>
                <li><strong><a href="https://data.hub.geosphere.at/" target="_blank" rel="noopener">GeoSphere Austria — Data Hub</a></strong> (CC BY 4.0) — station measurements for the nine federal-state capitals (Vienna, Salzburg, Innsbruck, Graz, Linz, Klagenfurt, Bregenz, Eisenstadt, St. Pölten), annual mean air-temperature values with WMO reference period 1991–2020 and linear trend</li>
                <li><strong><a href="https://www.basg.gv.at/marktbeobachtung/amtliche-nachrichten" target="_blank" rel="noopener">BASG — Austrian Medicines and Medical Devices Agency</a></strong> — RSS feed of official notices on medicine recalls, batch withdrawals, safety information (DHPC), and medical device warnings. Triggered only for pharmacological claims with Austrian context</li>
                <li><strong><a href="https://www.ris.bka.gv.at/" target="_blank" rel="noopener">RIS — Federal Legal Information System</a></strong> — live search of the Federal Law Gazette (Bundesgesetzblatt) via the official BKA API. Per result: short title, full title, BGBl number, publication date, and stable ELI (European Legislation Identifier) URL. Triggered only for legal claims with Austrian context and an extractable search term. Caveat: BGBl publications are publication events, not the consolidated current text</li>
                <li><strong><a href="https://www.bmi.gv.at/411/Alle_Volksbegehren_der_zweiten_Republik.aspx" target="_blank" rel="noopener">BMI — Popular Initiatives of the Second Republic</a></strong> — full list of all Austrian federal popular initiatives (Volksbegehren) since 1964 (Federal Ministry of the Interior, Department III/6 — Electoral Affairs). Per entry: year, subject, signing period, valid signatures, turnout in %, rank, and initiator/support. Search by word overlap and "top" mode (most successful / highest turnout / most recent / earliest). Triggered only on Austrian context or a known initiative name. Caveat: federal initiatives only — state-level initiatives are not covered; popular initiative ≠ referendum ≠ public consultation</li>
                <li><strong><a href="https://www.bmi.gv.at/412/start.aspx" target="_blank" rel="noopener">BMI — Election Results (Federal Electoral Commission)</a></strong> — federal results of all Austrian federal-level elections since 1986/1996: 12 National Council elections (1986–2024), 5 Federal Presidential elections (1998–2022), 7 European Parliament elections (1996–2024). Per election: full party names, short codes, votes, percentage, and seats (where available). Triggered only on explicit election type + Austrian context, or on party/candidate + year + Austria. Caveats: federal-level results only (no state/constituency breakdown); BPW 2016 entry covers only the re-run on 4 December 2016; no election forecasts, no political evaluation of parties or candidates</li>
                <li><strong><a href="https://www.parlament.gv.at/recherchieren/open-data/daten-und-lizenz/beschluesse" target="_blank" rel="noopener">Austrian Parliament — Voting Records (Open Data)</a></strong> — club-specific voting behaviour for National Council decisions since legislative period XXVI (2017), currently ~1,260 motions with voting data. Per motion: date, subject, document type (RV/A/BUA/BRA), 3rd-reading outcome, and which clubs voted for/against (e.g. ÖVP, SPÖ, FPÖ, GRÜNE, NEOS). Enables fact-checking statements like "FPÖ was the only opposition vote on X" or "Who voted for Y in 2023?". Triggered on voting keywords + Austrian context. Caveats: National Council only (no Bundesrat); since 2017 only (older records missing); only DOKTYP RV/A/BUA/BRA (petitions and Volksbegehren proceedings are tracked separately). No political evaluation of club behaviour — vote distribution only</li>
            </ul>

            <h3>Fact-checking partners with AT coverage</h3>
            <ul>
                <li><a href="https://faktencheck.apa.at/" target="_blank" rel="noopener">APA-Faktencheck</a> — Austrian Press Agency (via GADMO feed)</li>
                <li><a href="https://www.mimikama.at/" target="_blank" rel="noopener">Mimikama</a> — Austrian association against online disinformation (via GADMO feed)</li>
                <li><a href="https://gadmo.eu/" target="_blank" rel="noopener">GADMO</a> — German-Austrian Digital Media Observatory</li>
            </ul>

            <h3>International sources with Austrian data</h3>
            <p>The following international sources also provide Austria-specific data and are used for AT claims:</p>
            <ul>
                <li>Eurostat — EU statistics including Austrian data</li>
                <li>WHO Europe (HFA) — health data for Austria</li>
                <li>OECD — economic and social data</li>
                <li>ECB — financial and interest-rate data</li>
                <li>OWID / World Bank — global time series with AT filter</li>
            </ul>

            <p style="margin-top: 20px; font-size: 0.85rem; color: #6b7280;">The Österreich Edition is a content focus, not a separate version — all European and scientific sources remain fully available. For a complete list of sources, see &bdquo;<a href="#" onclick="closeModal(event); openModal('sources', event)">Sources</a>&ldquo;.</p>
        `,

        // Imprint
        imprint_html_configured: `
            <h2>Legal Notice</h2>
            <h3>Information pursuant to § 5 ECG (Austrian E-Commerce Act)</h3>
            <p><strong>{name}</strong></p>
            <p>{location}</p>
            <p>Email: {email}</p>
            <h3>Liability for content</h3>
            <p>The results of Evidora are generated automatically and do not constitute an editorial assessment. No guarantee is given for the accuracy, completeness, or timeliness of the results.</p>
            <h3>Liability for links</h3>
            <p>Evidora links to external sources (PubMed, WHO, EMA, etc.). The respective operators are solely responsible for their content.</p>
        `,
        imprint_html_unconfigured: `
            <h2>Legal Notice</h2>
            <p>The operator of this instance has not yet configured a legal notice.</p>
            <p>If you are running this instance, set the following environment variables in your <code>.env</code> file:</p>
            <ul>
                <li><code>IMPRESSUM_NAME</code> — Your name</li>
                <li><code>IMPRESSUM_EMAIL</code> — Your email address</li>
                <li><code>IMPRESSUM_LOCATION</code> — Your location</li>
            </ul>
        `,

        // Disclaimer
        disclaimer_page_html: `
            <h2>Disclaimer</h2>

            <h3>1. Automated service</h3>
            <p>Evidora is an automated fact-checking tool. Results are generated by a language model (LLM) based on public data sources. <strong>They do not constitute an editorial, scientific, or legal assessment.</strong></p>

            <h3>2. No warranty</h3>
            <p>No guarantee is given for the accuracy, completeness, or timeliness of the displayed results. Incorrect assessments are possible, particularly for:</p>
            <ul>
                <li>Ambiguous or complex claims</li>
                <li>Topics outside the covered data sources</li>
                <li>Current events not yet captured in the sources</li>
            </ul>

            <h3>3. Not a basis for decisions</h3>
            <p>Evidora's results should <strong>not be used as the sole basis</strong> for personal, medical, financial, or political decisions. When in doubt, consult professional sources or experts.</p>

            <h3>4. External data sources</h3>
            <p>Evidora forwards search queries to external APIs (including PubMed, WHO, EMA, Eurostat, ECB, UNHCR, EEA, ECDC, Copernicus, Google Fact Check). Evidora assumes no responsibility for their availability, accuracy, or completeness.</p>
            <p>Depending on the configuration, AI analysis is performed locally (Ollama) or via the <strong>Mistral Cloud API</strong> (EU servers, Paris). See <a href="#" onclick="openModal('privacy', event)">Privacy Policy</a> for details.</p>

            <h3>5. Open Source</h3>
            <p>The software is provided under the MIT License — <strong>"as is", without warranty of any kind</strong>. See <a href="https://opensource.org/licenses/MIT" target="_blank" rel="noopener">MIT License</a> for details.</p>
        `,

        // Privacy
        privacy_html: `
            <h2>Privacy Policy</h2>

            <h3>1. Overview</h3>
            <p>Protecting your data is important to us. This page explains what data is processed — and what is not.</p>

            <h3>2. What we do NOT do</h3>
            <ul>
                <li>No cookies (neither tracking nor advertising cookies)</li>
                <li>No user accounts or registration</li>
                <li>No analytics tools (no Google Analytics etc.)</li>
                <li>No sharing of data with third parties for advertising</li>
                <li>No permanent storage of your inputs</li>
            </ul>

            <h3>3. What data is processed</h3>

            <h4>a) Your claim</h4>
            <p>When you enter a claim, it is sent to our backend for processing. Depending on the configuration, AI analysis is performed by:</p>
            <ul>
                <li><strong>Local language model (Mistral 7B via Ollama)</strong> — your input does not leave our infrastructure</li>
                <li><strong>Mistral Cloud API (EU servers, Paris)</strong> — your input is transmitted to Mistral AI (France). Mistral processes data in accordance with their <a href="https://mistral.ai/terms/#privacy-policy" target="_blank" rel="noopener">Privacy Policy</a> on EU servers.</li>
            </ul>
            <p>In both cases, your inputs are <strong>not stored permanently</strong> — neither by us nor by Mistral (API mode: no training use per Mistral's policies).</p>

            <h4>b) External source queries</h4>
            <p>To verify facts, <strong>search queries</strong> (not your exact input, but extracted search terms) are sent to the following public APIs:</p>
            <ul>
                <li>PubMed (NIH, USA) — biomedical studies</li>
                <li>WHO GHO — health data</li>
                <li>EMA — medication data (EU)</li>
                <li>Copernicus CDS (ECMWF/EU) — climate data</li>
                <li>Eurostat (EU) — European statistics</li>
                <li>ECB (EU) — key interest rates, exchange rates, money supply</li>
                <li>UNHCR — Global refugee and asylum statistics</li>
                <li>EEA (EU) — environmental data</li>
                <li>ECDC (EU) — infectious diseases</li>
                <li>Cochrane Reviews — systematic reviews (via PubMed)</li>
                <li>GADMO/APA — German-language fact-checks</li>
                <li>EFCSN fact-checkers — existing fact checks</li>
            </ul>
            <p>These services are subject to their own privacy policies. No personal data is transmitted to them.</p>

            <h4>c) Language setting</h4>
            <p>Your chosen language (DE/EN) is stored in your browser's <strong>localStorage</strong> so it persists across visits. This is not a cookie and is not sent to the server.</p>

            <h4>d) Server logs</h4>
            <p>Our backend logs anonymized technical data (error messages, request category, response times). <strong>Your entered claims are not logged.</strong> Logs are automatically deleted when the server restarts.</p>

            <h3>4. Legal basis</h3>
            <p>Processing is based on Art. 6(1)(f) GDPR (legitimate interest: providing the service). Since no personal data is stored, no data access or deletion rights apply.</p>

            <h3>5. Data Sources & Licenses</h3>
            <p>Evidora uses exclusively public, freely accessible data sources:</p>
            <ul>
                <li><strong>PubMed / NCBI</strong> — public database of the U.S. National Library of Medicine. Used per <a href="https://www.ncbi.nlm.nih.gov/home/about/policies/" target="_blank" rel="noopener">NCBI Policies</a></li>
                <li><strong>WHO GHO</strong> — open health data by the World Health Organization. <a href="https://www.who.int/about/policies/publishing/copyright" target="_blank" rel="noopener">WHO Copyright Policy</a></li>
                <li><strong>EMA</strong> — open medication data by the European Medicines Agency (CC BY 4.0). <a href="https://www.ema.europa.eu/en/about-us/legal-notice" target="_blank" rel="noopener">EMA Legal Notice</a></li>
                <li><strong>Copernicus CDS</strong> — climate data by ECMWF/EU. <a href="https://cds.climate.copernicus.eu/datasets" target="_blank" rel="noopener">Copernicus Licence</a></li>
                <li><strong>Eurostat</strong> — open EU statistics (CC BY 4.0). <a href="https://ec.europa.eu/eurostat/web/main/help/copyright-notice" target="_blank" rel="noopener">Eurostat Copyright</a></li>
                <li><strong>ECB</strong> — key interest rates, exchange rates and money supply by the European Central Bank. <a href="https://data.ecb.europa.eu/help/api/overview" target="_blank" rel="noopener">ECB Data Portal</a></li>
                <li><strong>UNHCR</strong> — refugee and asylum statistics by the UN Refugee Agency. <a href="https://www.unhcr.org/refugee-statistics/" target="_blank" rel="noopener">UNHCR Refugee Data</a></li>
                <li><strong>EEA</strong> — environmental data by the European Environment Agency. <a href="https://www.eea.europa.eu/en/legal-notice" target="_blank" rel="noopener">EEA Legal Notice</a></li>
                <li><strong>ECDC</strong> — surveillance data by the European Centre for Disease Prevention and Control. <a href="https://www.ecdc.europa.eu/en/copyright" target="_blank" rel="noopener">ECDC Copyright</a></li>
                <li><strong>Statistik Austria</strong> — Austrian official statistics (CC BY 4.0). <a href="https://data.statistik.gv.at/" target="_blank" rel="noopener">Statistik Austria OGD</a></li>
                <li><strong>GeoSphere Austria</strong> — climate station data by the Federal Institute for Meteorology (CC BY 4.0). <a href="https://data.hub.geosphere.at/" target="_blank" rel="noopener">GeoSphere Data Hub</a></li>
                <li><strong>BASG</strong> — official notices by the Austrian Federal Office for Safety in Health Care (public domain, § 7 UrhG-AT). <a href="https://www.basg.gv.at/marktbeobachtung/amtliche-nachrichten" target="_blank" rel="noopener">BASG Official Notices</a></li>
                <li><strong>RIS</strong> — Federal Legal Information System (Federal Chancellery) via Open Data API (PSI/OGD, attribution required). <a href="https://data.bka.gv.at/" target="_blank" rel="noopener">RIS Open Data</a></li>
                <li><strong>BMI Popular Initiatives</strong> — official list of all Austrian federal popular initiatives (Volksbegehren) of the Second Republic (Federal Ministry of the Interior, Department III/6 — Electoral Affairs; public domain, § 7 UrhG-AT). <a href="https://www.bmi.gv.at/411/Alle_Volksbegehren_der_zweiten_Republik.aspx" target="_blank" rel="noopener">BMI Popular Initiatives</a></li>
                <li><strong>BMI Elections / Federal Electoral Commission</strong> — official federal results of all Austrian federal-level elections (National Council, Federal Presidential, European Parliament; public domain, § 7 UrhG-AT). <a href="https://www.bmi.gv.at/412/start.aspx" target="_blank" rel="noopener">BMI Elections</a></li>
                <li><strong>Austrian Parliament Voting Records</strong> — club voting behaviour on National Council decisions, via the Parliament's Open Data portal (CC BY 4.0). <a href="https://www.parlament.gv.at/recherchieren/open-data/" target="_blank" rel="noopener">Parlament Open Data</a></li>
                <li><strong>AT Factbook</strong> — curated Austrian fact records from official primary sources (Vienna school authority on religious affiliations in compulsory schools; Federal Ministry of Finance subsidy report; Statistik Austria ESA-2010 subsidy ratio). Manually maintained; complements Austrian statistics for which no public API exists.</li>
                <li><strong>Cochrane Reviews</strong> — systematic reviews via PubMed (highest level of medical evidence)</li>
                <li><strong>GADMO fact-checks</strong> — German-language fact-checks (APA). <a href="https://gadmo.eu" target="_blank" rel="noopener">GADMO</a></li>
                <li><strong>EFCSN fact-checkers</strong> — via <a href="https://developers.google.com/fact-check/tools/api" target="_blank" rel="noopener">Google Fact Check Tools API</a> (ClaimReview data from Correctiv, AFP, dpa and others)</li>
            </ul>
            <p>Search icon: <a href="https://github.com/google/material-design-icons" target="_blank" rel="noopener">Google Material Design Icons</a> (Apache 2.0 License).</p>
        `,
    },
};

let currentLang = localStorage.getItem("evidora-lang") || "de";

function t(key) {
    return (TRANSLATIONS[currentLang] && TRANSLATIONS[currentLang][key]) || TRANSLATIONS.de[key] || key;
}

function setLanguage(lang) {
    currentLang = lang;
    localStorage.setItem("evidora-lang", lang);
    document.documentElement.lang = lang;
    document.title = t("page_title");
    applyTranslations();
    updateLangToggle();
    // Re-render example claims (their text is language-specific)
    if (typeof renderExampleClaims === "function") {
        renderExampleClaims();
    }
}

function applyTranslations() {
    const isOnline = window.location.hostname === "evidora.eu";
    document.querySelectorAll("[data-i18n]").forEach((el) => {
        let key = el.getAttribute("data-i18n");
        // On evidora.eu, use the extended beta notice with data processing info
        if (key === "beta_notice" && isOnline) {
            key = "beta_notice_online";
        }
        // Use innerHTML for tips (contain <strong> tags), textContent for everything else
        if (key.startsWith("tips_") && key !== "tips_btn") {
            el.innerHTML = t(key);
        } else {
            el.textContent = t(key);
        }
    });
    document.querySelectorAll("[data-i18n-placeholder]").forEach((el) => {
        const key = el.getAttribute("data-i18n-placeholder");
        el.placeholder = t(key);
    });
}

function updateLangToggle() {
    document.querySelectorAll(".lang-btn").forEach((btn) => {
        btn.classList.toggle("active", btn.dataset.lang === currentLang);
    });
}

function getVerdictLabels() {
    return {
        true: t("verdict_true"),
        mostly_true: t("verdict_mostly_true"),
        mixed: t("verdict_mixed"),
        mostly_false: t("verdict_mostly_false"),
        false: t("verdict_false"),
        unverifiable: t("verdict_unverifiable"),
    };
}

function getStrengthLabel(strength) {
    const map = { strong: t("strength_strong"), moderate: t("strength_moderate"), weak: t("strength_weak") };
    return map[strength] || "";
}
