"""ClinicalTrials.gov — registry of 500K+ clinical studies worldwide.

Uses the free v2 API (no key required). Essential for verifying claims about:
- Drug efficacy and safety
- Treatment comparisons
- Vaccine trials
- Medical procedures
"""

import httpx
import logging

logger = logging.getLogger("evidora")

BASE_URL = "https://clinicaltrials.gov/api/v2/studies"


async def search_clinicaltrials(analysis: dict) -> dict:
    queries = analysis.get("pubmed_queries", [])
    if not queries:
        return {"source": "ClinicalTrials.gov", "results": []}

    # Use the most specific query
    search_term = queries[0]

    params = {
        "query.term": search_term,
        "pageSize": 5,
        "sort": "@relevance",
        "fields": "NCTId,BriefTitle,OverallStatus,Phase,EnrollmentCount,"
                  "StartDate,CompletionDate,Condition,InterventionName,"
                  "LeadSponsorName,StudyType",
        "format": "json",
    }

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(BASE_URL, params=params)
        resp.raise_for_status()
        data = resp.json()

    studies = data.get("studies", [])
    if not studies:
        return {"source": "ClinicalTrials.gov", "results": []}

    results = []
    for study in studies:
        proto = study.get("protocolSection", {})
        ident = proto.get("identificationModule", {})
        status_mod = proto.get("statusModule", {})
        design = proto.get("designModule", {})
        sponsor_mod = proto.get("sponsorCollaboratorsModule", {})
        conditions_mod = proto.get("conditionsModule", {})
        interventions_mod = proto.get("armsInterventionsModule", {})

        nct_id = ident.get("nctId", "")
        title = ident.get("briefTitle", "")
        if not title:
            continue

        status = status_mod.get("overallStatus", "")
        phases = design.get("phases", [])
        phase = ", ".join(phases) if phases else ""
        enrollment = design.get("enrollmentInfo", {}).get("count", "")

        # Sponsor
        lead_sponsor = sponsor_mod.get("leadSponsor", {})
        sponsor = lead_sponsor.get("name", "")

        # Conditions
        conditions = conditions_mod.get("conditions", [])
        condition_str = ", ".join(conditions[:3])

        # Interventions
        interventions = interventions_mod.get("interventions", [])
        intervention_names = ", ".join(
            i.get("name", "") for i in interventions[:3]
        )

        # Dates
        start = status_mod.get("startDateStruct", {}).get("date", "")
        completion = status_mod.get("completionDateStruct", {}).get("date", "")

        # Status translation for display
        status_map = {
            "COMPLETED": "Abgeschlossen",
            "RECRUITING": "Rekrutierend",
            "ACTIVE_NOT_RECRUITING": "Aktiv (keine Rekrutierung)",
            "NOT_YET_RECRUITING": "Noch nicht rekrutierend",
            "TERMINATED": "Abgebrochen",
            "WITHDRAWN": "Zurückgezogen",
            "SUSPENDED": "Ausgesetzt",
        }
        status_display = status_map.get(status, status)

        meta_parts = []
        if phase:
            meta_parts.append(phase)
        if status_display:
            meta_parts.append(status_display)
        if enrollment:
            meta_parts.append(f"n={enrollment}")
        if sponsor:
            meta_parts.append(sponsor)

        results.append({
            "title": title,
            "nct_id": nct_id,
            "status": status_display,
            "phase": phase,
            "conditions": condition_str,
            "interventions": intervention_names,
            "enrollment": str(enrollment) if enrollment else "",
            "date": f"{start} – {completion}" if start else "",
            "url": f"https://clinicaltrials.gov/study/{nct_id}",
            "meta": " | ".join(meta_parts),
        })

    logger.info(f"ClinicalTrials.gov: {len(results)} studies for '{search_term[:80]}'")
    return {"source": "ClinicalTrials.gov", "type": "clinical_trial", "results": results}
