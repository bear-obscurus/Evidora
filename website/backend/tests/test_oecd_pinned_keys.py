"""Contract-Test für die gepinnten OECD-SDMX-Keys (Audit 2026-07-07).

Beide OECD-Connectoren bauten den SDMX-Key als literal '/all' und filterten
nur REF_AREA clientseitig → ALLE anderen Dimensionen (SEX/AGE/UNIT/MEASURE/
SPENDING_TYPE/…) blieben ungefiltert und der Parser zeigte 3-5 ZUFALLSZEILEN
(Arbeitslosenquote = beliebiges Subgroup, CPI = Index-Level statt YoY-%,
Familienausgaben in 'National currency' statt % BIP …). Jetzt: gepinnter
Key pro Dataset/Domain, der alle Dims außer REF_AREA festlegt.

Dependency-light: importiert nur die Connector-Module, kein Netz.
"""
from services.oecd import OECD_DATASETS
from services import oecd_sdmx


def test_every_oecd_dataset_has_pinned_key():
    """Kein oecd.py-Dataset darf ohne 'key' laufen (sonst '/all' → Zufall)."""
    for ds_id, info in OECD_DATASETS.items():
        key = info.get("key")
        assert key, f"OECD-Dataset '{ds_id}' hat keinen gepinnten 'key'"
        assert key != "all", f"'{ds_id}' nutzt wieder '/all'"
        # REF_AREA bleibt offen (führender Punkt) → Client-seitiger Filter
        assert key.startswith("."), f"'{ds_id}'-Key muss REF_AREA offen lassen"


def test_dead_employment_flow_replaced():
    """Der tote DF_LFS_EMPSTAT_GENDER-Flow (HTTP 404) darf nicht zurückkehren."""
    for info in OECD_DATASETS.values():
        assert "DF_LFS_EMPSTAT_GENDER" not in info["flow"], \
            "toter employment-Flow DF_LFS_EMPSTAT_GENDER wieder in Nutzung"


def test_oecd_sdmx_domains_use_pinned_keys_not_all():
    """talis/socx/family/housing dürfen nicht mehr auf '/all' fallen."""
    for dom_id, flow in [
        ("talis", "OECD.EDU.ECS,DSD_TALIS@DF_TALIS,1.0"),
        ("socx", "OECD.ELS.SPD,DSD_SOCX_AGG@DF_PUB_PRV,1.0"),
        ("family", "OECD.ELS.SPD,DSD_SOCX_AGG@DF_PUB_FAM,1.0"),
        ("housing", "OECD.ECO.MPD,DSD_AN_HOUSE_PRICES@DF_HOUSE_PRICES,1.0"),
    ]:
        url = oecd_sdmx._build_domain_url(dom_id, flow, ["AUT"], "2020")
        assert url, f"'{dom_id}' liefert leere URL"
        # der Key-Segment darf nicht literal 'all' sein
        key_seg = url.split(flow + "/", 1)[1].split("?", 1)[0]
        assert key_seg != "all", f"'{dom_id}' nutzt wieder '/all'"
        assert key_seg.startswith("."), f"'{dom_id}'-Key lässt REF_AREA nicht offen"

# CI-Verifikation: dieser PR bestätigt, dass alle 14 dependency-light-Suiten
# im pull_request-Kontext laufen (Log-geprüft, danach geschlossen).
