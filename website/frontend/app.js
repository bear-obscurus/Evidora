const form = document.getElementById("search-form");
const input = document.getElementById("claim-input");
const searchSection = document.getElementById("search-section");
const loading = document.getElementById("loading");
const results = document.getElementById("results");
const error = document.getElementById("error");

// --- Character counter ---
const MAX_CLAIM = 500;
const charsRemainingEl = document.getElementById("chars-remaining");
const charCounterEl = document.getElementById("char-counter");

function updateCharCounter() {
    const remaining = MAX_CLAIM - input.value.length;
    charsRemainingEl.textContent = remaining;
    charCounterEl.classList.toggle("warn", remaining <= 50 && remaining > 0);
    charCounterEl.classList.toggle("limit", remaining <= 0);
}

input.addEventListener("input", updateCharCounter);
updateCharCounter();

// --- Tips toggle ---
const tipsBtn = document.getElementById("tips-btn");
const tipsPanel = document.getElementById("tips-panel");

tipsBtn.addEventListener("click", () => {
    const open = !tipsPanel.classList.contains("hidden");
    tipsPanel.classList.toggle("hidden");
    tipsBtn.setAttribute("aria-expanded", !open);
});

form.addEventListener("submit", async (e) => {
    e.preventDefault();
    const claim = input.value.trim();
    if (!claim) return;
    if (claim.length < 10 || claim.split(/\s+/).filter(Boolean).length < 2) {
        searchSection.className = "compact";
        showError(t("error_claim_too_short"));
        return;
    }
    if (claim.length > MAX_CLAIM) {
        searchSection.className = "compact";
        showError(t("error_claim_too_long"));
        return;
    }

    showLoading();

    try {
        const response = await fetch("/api/check", {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
                Accept: "text/event-stream",
            },
            body: JSON.stringify({ claim, lang: currentLang }),
        });

        if (!response.ok) {
            if (response.status === 429) {
                throw new Error(t("error_rate_limit"));
            }
            const err = await response.json().catch(() => ({}));
            throw new Error(err.detail || `${t("error_server")} (${response.status})`);
        }

        const reader = response.body.getReader();
        const decoder = new TextDecoder();
        let buffer = "";
        let streamDone = false;

        while (!streamDone) {
            const { done, value } = await reader.read();
            if (done) break;

            buffer += decoder.decode(value, { stream: true });
            const lines = buffer.split("\n");
            buffer = lines.pop();

            let eventType = null;
            for (const line of lines) {
                if (line.startsWith("event:")) {
                    eventType = line.slice(6).trim();
                } else if (line.startsWith("data:") && eventType) {
                    const data = JSON.parse(line.slice(5).trim());
                    if (eventType === "step") {
                        setStep(data.step);
                    } else if (eventType === "error") {
                        if (data.detail === "MISTRAL_CREDITS_EXHAUSTED") {
                            throw new Error(t("error_credits_exhausted"));
                        }
                        throw new Error(data.detail);
                    } else if (eventType === "result") {
                        showResults(data);
                    } else if (eventType === "done") {
                        streamDone = true;
                        reader.cancel();
                        break;
                    }
                    eventType = null;
                }
            }
        }
    } catch (err) {
        showError(err.message);
    }
});

function showLoading() {
    searchSection.className = "compact";
    results.classList.add("hidden");
    error.classList.add("hidden");
    loading.classList.remove("hidden");
    setStep("analyze");
}

function setStep(step) {
    const steps = ["analyze", "search", "synthesize"];
    const textKeys = {
        analyze: "loading_analyze",
        search: "loading_search",
        synthesize: "loading_synthesize",
    };

    document.getElementById("loading-text").textContent = t(textKeys[step]);

    const currentIndex = steps.indexOf(step);
    steps.forEach((s, i) => {
        const el = document.getElementById(`step-${s}`);
        el.className = "step";
        if (i < currentIndex) el.classList.add("done");
        if (i === currentIndex) el.classList.add("active");
    });
}

function showResults(data) {
    loading.classList.add("hidden");
    results.classList.remove("hidden");

    const claim = input.value.trim();
    const claimDisplay = document.getElementById("claim-display");
    if (claim) {
        claimDisplay.innerHTML = `<p class="claim-display-label">${t("claim_display_label")}</p><blockquote class="claim-display-text">${escapeHtml(claim)}</blockquote>`;
    } else {
        claimDisplay.innerHTML = "";
    }

    renderVerdict(data);
    renderEvidence(data.evidence || []);
    renderSources(data.raw_sources || []);
    renderDisclaimer(data.disclaimer);

    if (claim) saveToHistory(claim, sanitizeVerdict(data.verdict));
}

function showError(message) {
    loading.classList.add("hidden");
    results.classList.add("hidden");
    error.classList.remove("hidden");
    document.getElementById("error-text").textContent = message;
}

function resetSearch() {
    searchSection.className = "hero";
    results.classList.add("hidden");
    error.classList.add("hidden");
    loading.classList.add("hidden");
    input.value = "";
    input.focus();
}

function buildConfidenceTooltip(data) {
    const confidence = Math.round((data.confidence || 0) * 100);
    const coverage = data.source_coverage || {};
    const queried = coverage.queried || 0;
    const withResults = coverage.with_results || 0;
    const evidence = data.evidence || [];

    const strong = evidence.filter(e => e.strength === "strong").length;
    const moderate = evidence.filter(e => e.strength === "moderate").length;
    const weak = evidence.filter(e => e.strength === "weak").length;

    if (currentLang === "en") {
        if (withResults === 0) {
            return "No source returned relevant results — confidence 0% (unverifiable).";
        }
        const parts = [`${withResults} of ${queried} source${queried !== 1 ? "s" : ""} returned results.`];
        if (strong > 0) parts.push(`${strong} strong piece${strong > 1 ? "s" : ""} of evidence.`);
        if (moderate > 0) parts.push(`${moderate} moderate piece${moderate > 1 ? "s" : ""} of evidence.`);
        if (weak > 0) parts.push(`${weak} weak piece${weak > 1 ? "s" : ""} of evidence.`);
        if (confidence >= 80) parts.push("Clear evidence base → high confidence.");
        else if (confidence >= 50) parts.push("Partially supported evidence → moderate confidence.");
        else parts.push("Contradictory or incomplete evidence → low confidence.");
        return parts.join(" ");
    } else {
        if (withResults === 0) {
            return "Keine Quelle lieferte relevante Ergebnisse — Konfidenz 0% (nicht überprüfbar).";
        }
        const parts = [`${withResults} von ${queried} Quelle${queried !== 1 ? "n" : ""} lieferte${withResults !== 1 ? "n" : ""} Ergebnisse.`];
        if (strong > 0) parts.push(`${strong} ${strong > 1 ? "starke Belege" : "starker Beleg"}.`);
        if (moderate > 0) parts.push(`${moderate} ${moderate > 1 ? "mittlere Belege" : "mittlerer Beleg"}.`);
        if (weak > 0) parts.push(`${weak} ${weak > 1 ? "schwache Belege" : "schwacher Beleg"}.`);
        if (confidence >= 80) parts.push("Klare Datenlage → hohe Konfidenz.");
        else if (confidence >= 50) parts.push("Teilweise belegte Datenlage → mittlere Konfidenz.");
        else parts.push("Widersprüchliche oder unvollständige Evidenz → niedrige Konfidenz.");
        return parts.join(" ");
    }
}

function renderVerdict(data) {
    const verdict = sanitizeVerdict(data.verdict);
    const labels = getVerdictLabels();
    const label = labels[verdict];
    const confidence = Math.round((data.confidence || 0) * 100);
    const coverage = data.source_coverage || {};
    const queried = coverage.queried || 0;
    const withResults = coverage.with_results || 0;
    const namesWithResults = new Set(coverage.names || []);
    const allNames = coverage.all_names || [];
    const sourceListHtml = allNames.map(n => {
        const has = namesWithResults.has(n);
        return `<span class="source-tag ${has ? "source-hit" : "source-miss"}">${escapeHtml(n)}</span>`;
    }).join(" ");

    let coverageWarning = "";
    if (queried > 0 && withResults === 0) {
        coverageWarning = `<div class="coverage-warning coverage-none">${t("source_coverage_none")}</div>`;
    } else if (withResults === 1) {
        coverageWarning = `<div class="coverage-warning coverage-low">${t("source_coverage_single")}</div>`;
    } else if (queried > 0 && withResults <= Math.floor(queried / 3)) {
        coverageWarning = `<div class="coverage-warning coverage-low">${t("source_coverage_low")}</div>`;
    }

    const coverageDetail = queried > 0
        ? t("source_coverage_detail").replace("{with}", withResults).replace("{total}", queried)
        : "";

    document.getElementById("verdict-card").innerHTML = `
        <div class="verdict-${verdict}">
            <span class="verdict-badge badge-${verdict}">${label}</span>
            <p class="verdict-summary">${escapeHtml(data.summary || "")}</p>
            ${data.nuance ? `<p class="verdict-nuance">${escapeHtml(data.nuance)}</p>` : ""}
            <div class="metrics-grid">
                <span class="metric-label">
                    <span class="tooltip-anchor" aria-label="${buildConfidenceTooltip(data)}">
                        <svg class="info-icon" viewBox="0 0 16 16" width="13" height="13" aria-hidden="true"><circle cx="8" cy="8" r="7.5" stroke="currentColor" stroke-width="1" fill="none"/><text x="8" y="12" text-anchor="middle" font-size="10" fill="currentColor" font-family="serif" font-style="italic">i</text></svg>
                        <span class="tooltip-text">${escapeHtml(buildConfidenceTooltip(data))}</span>
                    </span>
                    ${t("confidence")}
                </span>
                <div class="confidence-track">
                    <div class="confidence-fill" style="width: ${confidence}%"></div>
                </div>
                <span class="metric-value">${confidence}%</span>
                ${queried > 0 ? `
                <span class="metric-label">${t("source_coverage")}</span>
                <div class="coverage-track">
                    <div class="coverage-fill" style="width: ${Math.round((withResults / queried) * 100)}%"></div>
                </div>
                <span class="metric-value">${coverageDetail}</span>` : ""}
            </div>
            ${queried > 0 ? `
            ${allNames.length ? `<div class="coverage-sources">${sourceListHtml}</div>` : ""}
            ${coverageWarning}` : ""}
        </div>
    `;

    const card = document.getElementById("verdict-card");
    card.className = `verdict-${verdict}`;
}

function renderEvidence(evidence) {
    const section = document.getElementById("evidence-section");
    if (!evidence.length) {
        section.innerHTML = "";
        return;
    }

    section.innerHTML = `
        <h2>${t("evidence_title")}</h2>
        ${evidence
            .map(
                (e) => `
            <div class="evidence-card">
                <div class="evidence-header">
                    <span class="evidence-source">${escapeHtml(e.source || "")}</span>
                    <span class="evidence-strength strength-${e.strength || "weak"}">
                        ${getStrengthLabel(e.strength)}
                    </span>
                </div>
                <p class="evidence-finding">${escapeHtml(e.finding || "")}</p>
                ${e.url && sanitizeUrl(e.url) ? `<a class="evidence-link" href="${sanitizeUrl(e.url)}" target="_blank" rel="noopener">${escapeHtml(e.url)}</a>` : ""}
            </div>
        `
            )
            .join("")}
    `;
}

function renderSources(sources) {
    const section = document.getElementById("sources-section");
    if (!sources.length) {
        section.innerHTML = "";
        return;
    }

    const groups = sources.filter((s) => s.results && s.results.length > 0);
    if (!groups.length) {
        section.innerHTML = "";
        return;
    }

    section.innerHTML = `
        <h2>${t("sources_title")}</h2>
        ${groups
            .map(
                (group) => `
            <div class="source-group">
                <h3>${escapeHtml(group.source || "")}</h3>
                ${group.results
                    .map(
                        (r) => `
                    <div class="source-item">
                        ${r.url && sanitizeUrl(r.url) ? `<a class="source-title" href="${sanitizeUrl(r.url)}" target="_blank" rel="noopener">${escapeHtml(r.title || r.name || r.indicator_name || t("source_fallback"))}</a>` : `<span class="source-title">${escapeHtml(r.title || r.name || r.indicator_name || t("source_fallback"))}</span>`}
                        <div class="source-meta">
                            ${r.authors ? escapeHtml(r.authors) + " | " : ""}
                            ${r.journal ? escapeHtml(r.journal) + " | " : ""}
                            ${r.date ? escapeHtml(r.date) : ""}
                            ${r.status ? "Status: " + escapeHtml(r.status) : ""}
                            ${r.source ? escapeHtml(r.source) : ""}
                        </div>
                    </div>
                `
                    )
                    .join("")}
                ${group.attribution ? `<div class="source-attribution">${escapeHtml(group.attribution)}</div>` : ""}
            </div>
        `
            )
            .join("")}
    `;
}

function renderDisclaimer(text) {
    const section = document.getElementById("disclaimer-section");
    section.textContent = text || t("disclaimer_default");
}

function escapeHtml(str) {
    const div = document.createElement("div");
    div.textContent = str;
    return div.innerHTML;
}

function sanitizeUrl(url) {
    try {
        const parsed = new URL(url);
        if (["http:", "https:"].includes(parsed.protocol)) {
            return escapeHtml(parsed.href);
        }
    } catch {}
    return "";
}

const VALID_VERDICTS = ["true", "mostly_true", "mixed", "mostly_false", "false", "unverifiable"];

function sanitizeVerdict(verdict) {
    return VALID_VERDICTS.includes(verdict) ? verdict : "unverifiable";
}

// --- Search History ---
const HISTORY_KEY = "evidora_history";
const MAX_HISTORY = 10;

function getHistory() {
    try {
        return JSON.parse(localStorage.getItem(HISTORY_KEY) || "[]");
    } catch { return []; }
}

function saveToHistory(claim, verdict) {
    const history = getHistory().filter(h => h.claim !== claim);
    history.unshift({ claim, verdict, date: Date.now() });
    localStorage.setItem(HISTORY_KEY, JSON.stringify(history.slice(0, MAX_HISTORY)));
    renderHistory();
}

function clearHistory() {
    localStorage.removeItem(HISTORY_KEY);
    renderHistory();
}

function renderHistory() {
    const container = document.getElementById("search-history");
    const history = getHistory();

    if (!history.length) {
        container.classList.add("hidden");
        return;
    }

    container.classList.remove("hidden");
    container.innerHTML = `
        <div class="history-header">
            <span>${t("history_title")}</span>
            <button class="history-clear" onclick="clearHistory()">${t("history_clear")}</button>
        </div>
        ${history.map(h => `
            <div class="history-item" onclick="useHistoryItem('${escapeHtml(h.claim).replace(/'/g, "\\'")}')">
                <span class="history-verdict hv-${h.verdict}">${getVerdictLabels()[h.verdict] || "?"}</span>
                <span>${escapeHtml(h.claim)}</span>
            </div>
        `).join("")}
    `;
}

function useHistoryItem(claim) {
    input.value = claim;
    document.getElementById("search-history").classList.add("hidden");
    form.dispatchEvent(new Event("submit", { bubbles: true, cancelable: true }));
}

// Show history on input focus or click (hero mode only)
// Note: Safari doesn't reliably fire "focus" on mouse click, so we listen to both
function maybeShowHistory() {
    if (searchSection.classList.contains("hero") && getHistory().length) {
        renderHistory();
    }
}
input.addEventListener("focus", maybeShowHistory);
input.addEventListener("click", maybeShowHistory);

// Hide history when clicking outside search area
document.addEventListener("click", (e) => {
    const historyEl = document.getElementById("search-history");
    if (!historyEl.contains(e.target) && !form.contains(e.target)) {
        historyEl.classList.add("hidden");
    }
});

// --- PDF Export ---
function exportPDF() {
    window.print();
}

// --- Share ---
function copyToClipboard(text) {
    // Modern API (requires HTTPS)
    if (navigator.clipboard && navigator.clipboard.writeText) {
        return navigator.clipboard.writeText(text);
    }
    // Fallback for HTTP or older browsers
    return new Promise((resolve, reject) => {
        const textarea = document.createElement("textarea");
        textarea.value = text;
        textarea.style.position = "fixed";
        textarea.style.opacity = "0";
        document.body.appendChild(textarea);
        textarea.select();
        try {
            document.execCommand("copy") ? resolve() : reject();
        } catch (e) {
            reject(e);
        } finally {
            document.body.removeChild(textarea);
        }
    });
}

function shareResult() {
    const claim = input.value.trim();
    const url = `${window.location.origin}?claim=${encodeURIComponent(claim)}`;

    copyToClipboard(url).then(() => {
        const btn = document.getElementById("share-btn");
        btn.classList.add("copied");
        btn.textContent = t("share_copied");
        setTimeout(() => {
            btn.classList.remove("copied");
            btn.innerHTML = `<svg viewBox="0 0 24 24" width="16" height="16" aria-hidden="true"><path fill="currentColor" d="M18 16.08c-.76 0-1.44.3-1.96.77L8.91 12.7c.05-.23.09-.46.09-.7s-.04-.47-.09-.7l7.05-4.11c.54.5 1.25.81 2.04.81 1.66 0 3-1.34 3-3s-1.34-3-3-3-3 1.34-3 3c0 .24.04.47.09.7L8.04 9.81C7.5 9.31 6.79 9 6 9c-1.66 0-3 1.34-3 3s1.34 3 3 3c.79 0 1.5-.31 2.04-.81l7.12 4.16c-.05.21-.08.43-.08.65 0 1.61 1.31 2.92 2.92 2.92 1.61 0 2.92-1.31 2.92-2.92s-1.31-2.92-2.92-2.92z"/></svg> ${t("btn_share")}`;
        }, 2000);
    }).catch(() => {
        // Silent fail — button text stays unchanged
    });
}

// --- Auto-fill from URL ---
function checkUrlParams() {
    const params = new URLSearchParams(window.location.search);
    const claim = params.get("claim");
    if (claim) {
        input.value = claim;
        form.dispatchEvent(new Event("submit", { bubbles: true, cancelable: true }));
    }
}

// Legal modals (Privacy, Imprint, Disclaimer)
let imprintData = null;

async function openModal(type, e) {
    e.preventDefault();
    const body = document.getElementById("legal-body");

    if (type === "privacy") {
        body.innerHTML = t("privacy_html");
    } else if (type === "disclaimer") {
        body.innerHTML = t("disclaimer_page_html");
    } else if (type === "imprint") {
        if (!imprintData) {
            try {
                const res = await fetch("/api/legal");
                imprintData = await res.json();
            } catch {
                imprintData = { configured: false };
            }
        }
        if (imprintData.configured) {
            body.innerHTML = t("imprint_html_configured")
                .replace(/\{name\}/g, escapeHtml(imprintData.name))
                .replace(/\{email\}/g, escapeHtml(imprintData.email))
                .replace(/\{location\}/g, escapeHtml(imprintData.location));
        } else {
            body.innerHTML = t("imprint_html_unconfigured");
        }
    }

    document.getElementById("legal-modal").classList.remove("hidden");
    document.body.style.overflow = "hidden";
}

function closeModal(e) {
    e.preventDefault();
    document.getElementById("legal-modal").classList.add("hidden");
    document.body.style.overflow = "";
}

document.addEventListener("keydown", (e) => {
    if (e.key === "Escape") {
        const modal = document.getElementById("legal-modal");
        if (!modal.classList.contains("hidden")) {
            closeModal(e);
        }
    }
});

// Apply saved language on load, check URL params
document.addEventListener("DOMContentLoaded", () => {
    setLanguage(currentLang);
    checkUrlParams();
});
