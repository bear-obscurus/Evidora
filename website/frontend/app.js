const form = document.getElementById("search-form");
const input = document.getElementById("claim-input");
const searchSection = document.getElementById("search-section");
const loading = document.getElementById("loading");
const results = document.getElementById("results");
const error = document.getElementById("error");

form.addEventListener("submit", async (e) => {
    e.preventDefault();
    const claim = input.value.trim();
    if (!claim) return;

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

        while (true) {
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
                        throw new Error(data.detail);
                    } else if (eventType === "result") {
                        showResults(data);
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

    renderVerdict(data);
    renderEvidence(data.evidence || []);
    renderSources(data.raw_sources || []);
    renderDisclaimer(data.disclaimer);
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

function renderVerdict(data) {
    const verdict = sanitizeVerdict(data.verdict);
    const labels = getVerdictLabels();
    const label = labels[verdict];
    const confidence = Math.round((data.confidence || 0) * 100);
    const coverage = data.source_coverage || {};
    const queried = coverage.queried || 0;
    const withResults = coverage.with_results || 0;
    const sourceNames = (coverage.names || []).map(n => escapeHtml(n)).join(", ");

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
            <div class="confidence-bar">
                <span>${t("confidence")}</span>
                <div class="confidence-track">
                    <div class="confidence-fill" style="width: ${confidence}%"></div>
                </div>
                <span>${confidence}%</span>
            </div>
            ${queried > 0 ? `
            <div class="source-coverage">
                <span>${t("source_coverage")}</span>
                <div class="coverage-track">
                    <div class="coverage-fill" style="width: ${Math.round((withResults / queried) * 100)}%"></div>
                </div>
                <span>${coverageDetail}</span>
            </div>
            ${sourceNames ? `<div class="coverage-sources">${sourceNames}</div>` : ""}
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

// Apply saved language on load
document.addEventListener("DOMContentLoaded", () => {
    setLanguage(currentLang);
});
