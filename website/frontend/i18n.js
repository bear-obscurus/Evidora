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
        error_rate_limit: "Zu viele Anfragen. Bitte warte einen Moment.",
        error_empty: "Behauptung darf nicht leer sein.",
        error_credits_exhausted: "Der KI-Dienst ist vorübergehend nicht verfügbar (API-Guthaben aufgebraucht). Bitte versuche es später erneut.",

        // Disclaimer
        disclaimer_default: "Dies ist eine automatische Überprüfung und ersetzt keine professionelle Faktencheck-Redaktion.",

        // Actions
        btn_export_pdf: "Als PDF speichern",
        btn_share: "Link kopieren",
        share_copied: "Link kopiert!",

        // Search history
        history_title: "Letzte Checks",
        history_clear: "Verlauf löschen",

        // Footer
        footer_main: "Evidora.eu — Europäischer Faktencheck | Quellen: PubMed, Cochrane, WHO, EMA, ECDC, Eurostat, EZB, UNHCR, EEA, Copernicus, OECD, GADMO, EFCSN",
        footer_note: "Automatische Überprüfung — ersetzt keine professionelle Faktencheck-Redaktion",
        footer_opensource: "Open Source auf GitHub",
        privacy_link: "Datenschutz",
        imprint_link: "Impressum",
        disclaimer_link: "Haftungsausschluss",

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
                <li><strong>Copernicus CDS</strong> — Klimadaten des ECMWF/EU. <a href="https://cds.climate.copernicus.eu/datasets" target="_blank" rel="noopener">Copernicus Licence</a></li>
                <li><strong>Eurostat</strong> — offene EU-Statistiken (CC BY 4.0). <a href="https://ec.europa.eu/eurostat/web/main/help/copyright-notice" target="_blank" rel="noopener">Eurostat Copyright</a></li>
                <li><strong>EZB</strong> — Leitzinsen, Wechselkurse und Geldmengen der Europäischen Zentralbank. <a href="https://data.ecb.europa.eu/help/api/overview" target="_blank" rel="noopener">ECB Data Portal</a></li>
                <li><strong>UNHCR</strong> — Flüchtlings- und Asylstatistiken des UN-Flüchtlingshilfswerks. <a href="https://www.unhcr.org/refugee-statistics/" target="_blank" rel="noopener">UNHCR Refugee Data</a></li>
                <li><strong>EEA</strong> — Umweltdaten der Europäischen Umweltagentur. <a href="https://www.eea.europa.eu/en/legal-notice" target="_blank" rel="noopener">EEA Legal Notice</a></li>
                <li><strong>ECDC</strong> — Surveillance-Daten des European Centre for Disease Prevention and Control. <a href="https://www.ecdc.europa.eu/en/copyright" target="_blank" rel="noopener">ECDC Copyright</a></li>
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
        error_rate_limit: "Too many requests. Please wait a moment.",
        error_empty: "Claim must not be empty.",
        error_credits_exhausted: "The AI service is temporarily unavailable (API credits exhausted). Please try again later.",

        // Disclaimer
        disclaimer_default: "This is an automated check and does not replace professional fact-checking.",

        // Actions
        btn_export_pdf: "Save as PDF",
        btn_share: "Copy link",
        share_copied: "Link copied!",

        // Search history
        history_title: "Recent checks",
        history_clear: "Clear history",

        // Footer
        footer_main: "Evidora.eu — European Fact Check | Sources: PubMed, Cochrane, WHO, EMA, ECDC, Eurostat, ECB, UNHCR, EEA, Copernicus, OECD, GADMO, EFCSN",
        footer_note: "Automated check — does not replace professional fact-checking",
        footer_opensource: "Open Source on GitHub",
        privacy_link: "Privacy Policy",
        imprint_link: "Legal Notice",
        disclaimer_link: "Disclaimer",

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
}

function applyTranslations() {
    const isOnline = window.location.hostname === "evidora.eu";
    document.querySelectorAll("[data-i18n]").forEach((el) => {
        let key = el.getAttribute("data-i18n");
        // On evidora.eu, use the extended beta notice with data processing info
        if (key === "beta_notice" && isOnline) {
            key = "beta_notice_online";
        }
        el.textContent = t(key);
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
