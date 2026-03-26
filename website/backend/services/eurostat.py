import httpx
import logging

logger = logging.getLogger("evidora")

BASE_URL = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data"

# Map keywords (DE + EN) to Eurostat dataset codes + query parameters
DATASET_MAP = {
    # Inflation / Preise
    "inflation": {
        "dataset": "prc_hicp_manr",
        "label": "Inflationsrate (HVPI)",
        "label_en": "Inflation Rate (HICP)",
        "params": {"coicop": "CP00", "lastTimePeriod": "12"},
        "unit": "%",
    },
    "preise": {
        "dataset": "prc_hicp_manr",
        "label": "Inflationsrate (HVPI)",
        "label_en": "Inflation Rate (HICP)",
        "params": {"coicop": "CP00", "lastTimePeriod": "12"},
        "unit": "%",
    },
    "teuerung": {
        "dataset": "prc_hicp_manr",
        "label": "Inflationsrate (HVPI)",
        "label_en": "Inflation Rate (HICP)",
        "params": {"coicop": "CP00", "lastTimePeriod": "12"},
        "unit": "%",
    },
    "prices": {
        "dataset": "prc_hicp_manr",
        "label": "Inflationsrate (HVPI)",
        "label_en": "Inflation Rate (HICP)",
        "params": {"coicop": "CP00", "lastTimePeriod": "12"},
        "unit": "%",
    },
    # Bevölkerung / Demografie
    "bevölkerung": {
        "dataset": "demo_pjan",
        "label": "Bevölkerung am 1. Januar",
        "label_en": "Population on 1 January",
        "params": {"sex": "T", "age": "TOTAL", "lastTimePeriod": "5"},
        "unit": "Personen",
    },
    "population": {
        "dataset": "demo_pjan",
        "label": "Bevölkerung am 1. Januar",
        "label_en": "Population on 1 January",
        "params": {"sex": "T", "age": "TOTAL", "lastTimePeriod": "5"},
        "unit": "Personen",
    },
    "einwohner": {
        "dataset": "demo_pjan",
        "label": "Bevölkerung am 1. Januar",
        "label_en": "Population on 1 January",
        "params": {"sex": "T", "age": "TOTAL", "lastTimePeriod": "5"},
        "unit": "Personen",
    },
    "geburtenrate": {
        "dataset": "demo_frate",
        "label": "Fertilitätsrate",
        "label_en": "Fertility Rate",
        "params": {"lastTimePeriod": "5"},
        "unit": "Kinder/Frau",
    },
    "fertility": {
        "dataset": "demo_frate",
        "label": "Fertilitätsrate",
        "label_en": "Fertility Rate",
        "params": {"lastTimePeriod": "5"},
        "unit": "Kinder/Frau",
    },
    # Migration — politische Behauptungen meinen meist Asyl/Flucht
    "migration": {
        "dataset": "migr_asyappctza",
        "label": "Asyl-Erstanträge",
        "label_en": "First-time Asylum Applications",
        "params": {"citizen": "TOTAL", "sex": "T", "age": "TOTAL", "applicant": "FRST", "unit": "PER", "lastTimePeriod": "5"},
        "unit": "Anträge",
    },
    "flüchtlinge": {
        "dataset": "migr_asyappctza",
        "label": "Asyl-Erstanträge",
        "label_en": "First-time Asylum Applications",
        "params": {"citizen": "TOTAL", "sex": "T", "age": "TOTAL", "applicant": "FRST", "unit": "PER", "lastTimePeriod": "5"},
        "unit": "Anträge",
    },
    "flüchtling": {
        "dataset": "migr_asyappctza",
        "label": "Asyl-Erstanträge",
        "label_en": "First-time Asylum Applications",
        "params": {"citizen": "TOTAL", "sex": "T", "age": "TOTAL", "applicant": "FRST", "unit": "PER", "lastTimePeriod": "5"},
        "unit": "Anträge",
    },
    "refugees": {
        "dataset": "migr_asyappctza",
        "label": "Asyl-Erstanträge",
        "label_en": "First-time Asylum Applications",
        "params": {"citizen": "TOTAL", "sex": "T", "age": "TOTAL", "applicant": "FRST", "unit": "PER", "lastTimePeriod": "5"},
        "unit": "Anträge",
    },
    "refugee": {
        "dataset": "migr_asyappctza",
        "label": "Asyl-Erstanträge",
        "label_en": "First-time Asylum Applications",
        "params": {"citizen": "TOTAL", "sex": "T", "age": "TOTAL", "applicant": "FRST", "unit": "PER", "lastTimePeriod": "5"},
        "unit": "Anträge",
    },
    "asyl": {
        "dataset": "migr_asyappctza",
        "label": "Asyl-Erstanträge",
        "label_en": "First-time Asylum Applications",
        "params": {"citizen": "TOTAL", "sex": "T", "age": "TOTAL", "applicant": "FRST", "unit": "PER", "lastTimePeriod": "5"},
        "unit": "Anträge",
    },
    "asylum": {
        "dataset": "migr_asyappctza",
        "label": "Asyl-Erstanträge",
        "label_en": "First-time Asylum Applications",
        "params": {"citizen": "TOTAL", "sex": "T", "age": "TOTAL", "applicant": "FRST", "unit": "PER", "lastTimePeriod": "5"},
        "unit": "Anträge",
    },
    "zuwanderung": {
        "dataset": "migr_asyappctza",
        "label": "Asyl-Erstanträge",
        "label_en": "First-time Asylum Applications",
        "params": {"citizen": "TOTAL", "sex": "T", "age": "TOTAL", "applicant": "FRST", "unit": "PER", "lastTimePeriod": "5"},
        "unit": "Anträge",
    },
    "aufnahme": {
        "dataset": "migr_asyappctza",
        "label": "Asyl-Erstanträge",
        "label_en": "First-time Asylum Applications",
        "params": {"citizen": "TOTAL", "sex": "T", "age": "TOTAL", "applicant": "FRST", "unit": "PER", "lastTimePeriod": "5"},
        "unit": "Anträge",
    },
    # Allgemeine Einwanderung (nicht Asyl)
    "einwanderung": {
        "dataset": "migr_imm1ctz",
        "label": "Einwanderung nach Staatsangehörigkeit",
        "label_en": "Immigration by Citizenship",
        "params": {"agedef": "COMPLET", "age": "TOTAL", "sex": "T", "lastTimePeriod": "5"},
        "unit": "Personen",
    },
    "immigration": {
        "dataset": "migr_imm1ctz",
        "label": "Einwanderung nach Staatsangehörigkeit",
        "label_en": "Immigration by Citizenship",
        "params": {"agedef": "COMPLET", "age": "TOTAL", "sex": "T", "lastTimePeriod": "5"},
        "unit": "Personen",
    },
    # Energie
    "energie": {
        "dataset": "nrg_bal_c",
        "label": "Energiebilanz",
        "label_en": "Energy Balance",
        "params": {"nrg_bal": "GEP", "siec": "TOTAL", "unit": "KTOE", "lastTimePeriod": "5"},
        "unit": "ktoe",
    },
    "energy": {
        "dataset": "nrg_bal_c",
        "label": "Energiebilanz",
        "label_en": "Energy Balance",
        "params": {"nrg_bal": "GEP", "siec": "TOTAL", "unit": "KTOE", "lastTimePeriod": "5"},
        "unit": "ktoe",
    },
    "strom": {
        "dataset": "nrg_bal_c",
        "label": "Energiebilanz",
        "label_en": "Energy Balance",
        "params": {"nrg_bal": "GEP", "siec": "E7000", "unit": "GWH", "lastTimePeriod": "5"},
        "unit": "GWh",
    },
    "electricity": {
        "dataset": "nrg_bal_c",
        "label": "Energiebilanz",
        "label_en": "Energy Balance",
        "params": {"nrg_bal": "GEP", "siec": "E7000", "unit": "GWH", "lastTimePeriod": "5"},
        "unit": "GWh",
    },
    "erneuerbare": {
        "dataset": "nrg_ind_ren",
        "label": "Anteil erneuerbarer Energien",
        "label_en": "Share of Renewable Energy",
        "params": {"nrg_bal": "REN", "lastTimePeriod": "5"},
        "unit": "%",
    },
    "renewable": {
        "dataset": "nrg_ind_ren",
        "label": "Anteil erneuerbarer Energien",
        "label_en": "Share of Renewable Energy",
        "params": {"nrg_bal": "REN", "lastTimePeriod": "5"},
        "unit": "%",
    },
    "kohle": {
        "dataset": "nrg_bal_c",
        "label": "Energiebilanz (Kohle)",
        "label_en": "Energy Balance (Coal)",
        "params": {"nrg_bal": "GEP", "siec": "C0000X0350-0370", "unit": "KTOE", "lastTimePeriod": "5"},
        "unit": "ktoe",
    },
    "coal": {
        "dataset": "nrg_bal_c",
        "label": "Energiebilanz (Kohle)",
        "label_en": "Energy Balance (Coal)",
        "params": {"nrg_bal": "GEP", "siec": "C0000X0350-0370", "unit": "KTOE", "lastTimePeriod": "5"},
        "unit": "ktoe",
    },
    # Kriminalität
    "kriminalität": {
        "dataset": "crim_off_cat",
        "label": "Polizeilich erfasste Straftaten",
        "label_en": "Police-Recorded Offences",
        "params": {"iccs": "ICCS0101", "unit": "NR", "lastTimePeriod": "5"},
        "unit": "Fälle",
    },
    "crime": {
        "dataset": "crim_off_cat",
        "label": "Polizeilich erfasste Straftaten",
        "label_en": "Police-Recorded Offences",
        "params": {"iccs": "ICCS0101", "unit": "NR", "lastTimePeriod": "5"},
        "unit": "Fälle",
    },
    "mord": {
        "dataset": "crim_off_cat",
        "label": "Polizeilich erfasste Straftaten (Tötungsdelikte)",
        "label_en": "Police-Recorded Offences (Homicide)",
        "params": {"iccs": "ICCS0101", "unit": "NR", "lastTimePeriod": "5"},
        "unit": "Fälle",
    },
    "homicide": {
        "dataset": "crim_off_cat",
        "label": "Polizeilich erfasste Straftaten (Tötungsdelikte)",
        "label_en": "Police-Recorded Offences (Homicide)",
        "params": {"iccs": "ICCS0101", "unit": "NR", "lastTimePeriod": "5"},
        "unit": "Fälle",
    },
    # Arbeitsmarkt
    "arbeitslosigkeit": {
        "dataset": "une_rt_m",
        "label": "Arbeitslosenquote",
        "label_en": "Unemployment Rate",
        "params": {"sex": "T", "age": "TOTAL", "s_adj": "SA", "unit": "PC_ACT", "lastTimePeriod": "12"},
        "unit": "%",
    },
    "unemployment": {
        "dataset": "une_rt_m",
        "label": "Arbeitslosenquote",
        "label_en": "Unemployment Rate",
        "params": {"sex": "T", "age": "TOTAL", "s_adj": "SA", "unit": "PC_ACT", "lastTimePeriod": "12"},
        "unit": "%",
    },
    "jugendarbeitslosigkeit": {
        "dataset": "une_rt_m",
        "label": "Jugendarbeitslosenquote (unter 25)",
        "label_en": "Youth Unemployment Rate (under 25)",
        "params": {"sex": "T", "age": "Y_LT25", "s_adj": "SA", "unit": "PC_ACT", "lastTimePeriod": "12"},
        "unit": "%",
    },
    "youth unemployment": {
        "dataset": "une_rt_m",
        "label": "Jugendarbeitslosenquote (unter 25)",
        "label_en": "Youth Unemployment Rate (under 25)",
        "params": {"sex": "T", "age": "Y_LT25", "s_adj": "SA", "unit": "PC_ACT", "lastTimePeriod": "12"},
        "unit": "%",
    },
    "jobs": {
        "dataset": "une_rt_m",
        "label": "Arbeitslosenquote",
        "label_en": "Unemployment Rate",
        "params": {"sex": "T", "age": "TOTAL", "s_adj": "SA", "unit": "PC_ACT", "lastTimePeriod": "12"},
        "unit": "%",
    },
    # Handel / Sanktionen
    "handel": {
        "dataset": "ext_lt_maineu",
        "label": "Außenhandel mit Nicht-EU-Ländern",
        "label_en": "International Trade with Non-EU Countries",
        "params": {"partner": "EXT_EU27_2020", "flow": "BAL", "sitc06": "TOTAL", "lastTimePeriod": "5"},
        "unit": "Mio. €",
    },
    "trade": {
        "dataset": "ext_lt_maineu",
        "label": "Außenhandel mit Nicht-EU-Ländern",
        "label_en": "International Trade with Non-EU Countries",
        "params": {"partner": "EXT_EU27_2020", "flow": "BAL", "sitc06": "TOTAL", "lastTimePeriod": "5"},
        "unit": "Mio. €",
    },
    "sanktionen": {
        "dataset": "nama_10_gdp",
        "label": "Bruttoinlandsprodukt (Auswirkung Sanktionen)",
        "label_en": "GDP (Sanctions Impact)",
        "params": {"na_item": "B1GQ", "unit": "CLV_PCH_PRE", "lastTimePeriod": "5"},
        "unit": "% Veränderung",
    },
    "sanctions": {
        "dataset": "nama_10_gdp",
        "label": "Bruttoinlandsprodukt (Auswirkung Sanktionen)",
        "label_en": "GDP (Sanctions Impact)",
        "params": {"na_item": "B1GQ", "unit": "CLV_PCH_PRE", "lastTimePeriod": "5"},
        "unit": "% Veränderung",
    },
    # Bildung
    "bildung": {
        "dataset": "edat_lfse_03",
        "label": "Bildungsstand der Bevölkerung",
        "label_en": "Educational Attainment",
        "params": {"sex": "T", "age": "Y25-64", "isced11": "TOTAL", "lastTimePeriod": "5"},
        "unit": "%",
    },
    "education": {
        "dataset": "edat_lfse_03",
        "label": "Bildungsstand der Bevölkerung",
        "label_en": "Educational Attainment",
        "params": {"sex": "T", "age": "Y25-64", "isced11": "TOTAL", "lastTimePeriod": "5"},
        "unit": "%",
    },
    "studenten": {
        "dataset": "educ_uoe_enrt01",
        "label": "Studierende im Tertiärbereich",
        "label_en": "Tertiary Education Students",
        "params": {"sex": "T", "isced11": "ED5-8", "lastTimePeriod": "5"},
        "unit": "Personen",
    },
    "students": {
        "dataset": "educ_uoe_enrt01",
        "label": "Studierende im Tertiärbereich",
        "label_en": "Tertiary Education Students",
        "params": {"sex": "T", "isced11": "ED5-8", "lastTimePeriod": "5"},
        "unit": "Personen",
    },
    # BIP / Wirtschaft
    "bip": {
        "dataset": "nama_10_gdp",
        "label": "Bruttoinlandsprodukt",
        "label_en": "Gross Domestic Product",
        "params": {"na_item": "B1GQ", "unit": "CP_MEUR", "lastTimePeriod": "5"},
        "unit": "Mio. €",
    },
    "gdp": {
        "dataset": "nama_10_gdp",
        "label": "Bruttoinlandsprodukt",
        "label_en": "Gross Domestic Product",
        "params": {"na_item": "B1GQ", "unit": "CP_MEUR", "lastTimePeriod": "5"},
        "unit": "Mio. €",
    },
    "wirtschaft": {
        "dataset": "nama_10_gdp",
        "label": "Bruttoinlandsprodukt",
        "label_en": "Gross Domestic Product",
        "params": {"na_item": "B1GQ", "unit": "CP_MEUR", "lastTimePeriod": "5"},
        "unit": "Mio. €",
    },
    "economy": {
        "dataset": "nama_10_gdp",
        "label": "Bruttoinlandsprodukt",
        "label_en": "Gross Domestic Product",
        "params": {"na_item": "B1GQ", "unit": "CP_MEUR", "lastTimePeriod": "5"},
        "unit": "Mio. €",
    },
    # Armut
    "armut": {
        "dataset": "ilc_li02",
        "label": "Armutsgefährdungsquote",
        "label_en": "At-Risk-of-Poverty Rate",
        "params": {"hhtyp": "TOTAL", "indic_il": "LI_R_MD60", "lastTimePeriod": "5"},
        "unit": "%",
    },
    "poverty": {
        "dataset": "ilc_li02",
        "label": "Armutsgefährdungsquote",
        "label_en": "At-Risk-of-Poverty Rate",
        "params": {"hhtyp": "TOTAL", "indic_il": "LI_R_MD60", "lastTimePeriod": "5"},
        "unit": "%",
    },
    # CO2 / Treibhausgase
    "co2": {
        "dataset": "env_air_gge",
        "label": "Treibhausgasemissionen",
        "label_en": "Greenhouse Gas Emissions",
        "params": {"airpol": "GHG", "src_crf": "TOTX4_MEMO", "unit": "MIO_T", "lastTimePeriod": "5"},
        "unit": "Mio. t CO2-Äquiv.",
    },
    "emissionen": {
        "dataset": "env_air_gge",
        "label": "Treibhausgasemissionen",
        "label_en": "Greenhouse Gas Emissions",
        "params": {"airpol": "GHG", "src_crf": "TOTX4_MEMO", "unit": "MIO_T", "lastTimePeriod": "5"},
        "unit": "Mio. t CO2-Äquiv.",
    },
    "emissions": {
        "dataset": "env_air_gge",
        "label": "Treibhausgasemissionen",
        "label_en": "Greenhouse Gas Emissions",
        "params": {"airpol": "GHG", "src_crf": "TOTX4_MEMO", "unit": "MIO_T", "lastTimePeriod": "5"},
        "unit": "Mio. t CO2-Äquiv.",
    },
    "treibhausgas": {
        "dataset": "env_air_gge",
        "label": "Treibhausgasemissionen",
        "label_en": "Greenhouse Gas Emissions",
        "params": {"airpol": "GHG", "src_crf": "TOTX4_MEMO", "unit": "MIO_T", "lastTimePeriod": "5"},
        "unit": "Mio. t CO2-Äquiv.",
    },
    "greenhouse": {
        "dataset": "env_air_gge",
        "label": "Treibhausgasemissionen",
        "label_en": "Greenhouse Gas Emissions",
        "params": {"airpol": "GHG", "src_crf": "TOTX4_MEMO", "unit": "MIO_T", "lastTimePeriod": "5"},
        "unit": "Mio. t CO2-Äquiv.",
    },
    "klimagase": {
        "dataset": "env_air_gge",
        "label": "Treibhausgasemissionen",
        "label_en": "Greenhouse Gas Emissions",
        "params": {"airpol": "GHG", "src_crf": "TOTX4_MEMO", "unit": "MIO_T", "lastTimePeriod": "5"},
        "unit": "Mio. t CO2-Äquiv.",
    },
    # Lebenserwartung
    "lebenserwartung": {
        "dataset": "demo_mlexpec",
        "label": "Lebenserwartung bei Geburt",
        "label_en": "Life Expectancy at Birth",
        "params": {"sex": "T", "age": "Y_LT1", "lastTimePeriod": "5"},
        "unit": "Jahre",
    },
    "life expectancy": {
        "dataset": "demo_mlexpec",
        "label": "Lebenserwartung bei Geburt",
        "label_en": "Life Expectancy at Birth",
        "params": {"sex": "T", "age": "Y_LT1", "lastTimePeriod": "5"},
        "unit": "Jahre",
    },
    # Gesundheitsausgaben
    "gesundheitsausgaben": {
        "dataset": "hlth_sha11_hf",
        "label": "Gesundheitsausgaben",
        "label_en": "Health Expenditure",
        "params": {"icha11_hf": "TOT_HF", "unit": "PC_GDP", "lastTimePeriod": "5"},
        "unit": "% des BIP",
    },
    "health expenditure": {
        "dataset": "hlth_sha11_hf",
        "label": "Gesundheitsausgaben",
        "label_en": "Health Expenditure",
        "params": {"icha11_hf": "TOT_HF", "unit": "PC_GDP", "lastTimePeriod": "5"},
        "unit": "% des BIP",
    },
    "gesundheitskosten": {
        "dataset": "hlth_sha11_hf",
        "label": "Gesundheitsausgaben",
        "label_en": "Health Expenditure",
        "params": {"icha11_hf": "TOT_HF", "unit": "PC_GDP", "lastTimePeriod": "5"},
        "unit": "% des BIP",
    },
    # Immobilienpreise
    "immobilienpreise": {
        "dataset": "prc_hpi_q",
        "label": "Immobilienpreisindex",
        "label_en": "House Price Index",
        "params": {"purchase": "TOTAL", "unit": "I15_Q", "lastTimePeriod": "8"},
        "unit": "Index (2015=100)",
    },
    "housing prices": {
        "dataset": "prc_hpi_q",
        "label": "Immobilienpreisindex",
        "label_en": "House Price Index",
        "params": {"purchase": "TOTAL", "unit": "I15_Q", "lastTimePeriod": "8"},
        "unit": "Index (2015=100)",
    },
    "wohnungspreise": {
        "dataset": "prc_hpi_q",
        "label": "Immobilienpreisindex",
        "label_en": "House Price Index",
        "params": {"purchase": "TOTAL", "unit": "I15_Q", "lastTimePeriod": "8"},
        "unit": "Index (2015=100)",
    },
    "mieten": {
        "dataset": "prc_hpi_q",
        "label": "Immobilienpreisindex",
        "label_en": "House Price Index",
        "params": {"purchase": "TOTAL", "unit": "I15_Q", "lastTimePeriod": "8"},
        "unit": "Index (2015=100)",
    },
    # Mindestlohn
    "mindestlohn": {
        "dataset": "earn_mw_cur",
        "label": "Gesetzlicher Mindestlohn",
        "label_en": "Statutory Minimum Wage",
        "params": {"currency": "EUR", "lastTimePeriod": "5"},
        "unit": "EUR/Monat",
    },
    "minimum wage": {
        "dataset": "earn_mw_cur",
        "label": "Gesetzlicher Mindestlohn",
        "label_en": "Statutory Minimum Wage",
        "params": {"currency": "EUR", "lastTimePeriod": "5"},
        "unit": "EUR/Monat",
    },
    # Staatsschulden
    "staatsschulden": {
        "dataset": "gov_10dd_edpt1",
        "label": "Staatsverschuldung",
        "label_en": "Government Debt",
        "params": {"na_item": "GD", "sector": "S13", "unit": "PC_GDP", "lastTimePeriod": "5"},
        "unit": "% des BIP",
    },
    "staatsverschuldung": {
        "dataset": "gov_10dd_edpt1",
        "label": "Staatsverschuldung",
        "label_en": "Government Debt",
        "params": {"na_item": "GD", "sector": "S13", "unit": "PC_GDP", "lastTimePeriod": "5"},
        "unit": "% des BIP",
    },
    "government debt": {
        "dataset": "gov_10dd_edpt1",
        "label": "Staatsverschuldung",
        "label_en": "Government Debt",
        "params": {"na_item": "GD", "sector": "S13", "unit": "PC_GDP", "lastTimePeriod": "5"},
        "unit": "% des BIP",
    },
    "schulden": {
        "dataset": "gov_10dd_edpt1",
        "label": "Staatsverschuldung",
        "label_en": "Government Debt",
        "params": {"na_item": "GD", "sector": "S13", "unit": "PC_GDP", "lastTimePeriod": "5"},
        "unit": "% des BIP",
    },
    # Einkommensungleichheit (Gini)
    "ungleichheit": {
        "dataset": "ilc_di12",
        "label": "Gini-Koeffizient (Einkommensungleichheit)",
        "label_en": "Gini Coefficient (Income Inequality)",
        "params": {"lastTimePeriod": "5"},
        "unit": "Gini (0-100)",
    },
    "inequality": {
        "dataset": "ilc_di12",
        "label": "Gini-Koeffizient (Einkommensungleichheit)",
        "label_en": "Gini Coefficient (Income Inequality)",
        "params": {"lastTimePeriod": "5"},
        "unit": "Gini (0-100)",
    },
    "gini": {
        "dataset": "ilc_di12",
        "label": "Gini-Koeffizient (Einkommensungleichheit)",
        "label_en": "Gini Coefficient (Income Inequality)",
        "params": {"lastTimePeriod": "5"},
        "unit": "Gini (0-100)",
    },
    # Tourismus
    "tourismus": {
        "dataset": "tour_occ_ninat",
        "label": "Übernachtungen in Beherbergungsbetrieben",
        "label_en": "Nights Spent at Tourist Accommodation",
        "params": {"c_resid": "TOTAL", "nace_r2": "I551-I553", "unit": "NR", "lastTimePeriod": "5"},
        "unit": "Übernachtungen",
    },
    "tourism": {
        "dataset": "tour_occ_ninat",
        "label": "Übernachtungen in Beherbergungsbetrieben",
        "label_en": "Nights Spent at Tourist Accommodation",
        "params": {"c_resid": "TOTAL", "nace_r2": "I551-I553", "unit": "NR", "lastTimePeriod": "5"},
        "unit": "Übernachtungen",
    },
}

# Map country names to Eurostat geo codes
COUNTRY_CODES = {
    "österreich": "AT", "austria": "AT",
    "deutschland": "DE", "germany": "DE",
    "frankreich": "FR", "france": "FR",
    "italien": "IT", "italy": "IT",
    "spanien": "ES", "spain": "ES",
    "niederlande": "NL", "netherlands": "NL",
    "belgien": "BE", "belgium": "BE",
    "polen": "PL", "poland": "PL",
    "schweden": "SE", "sweden": "SE",
    "dänemark": "DK", "denmark": "DK",
    "finnland": "FI", "finland": "FI",
    "irland": "IE", "ireland": "IE",
    "portugal": "PT", "portugal": "PT",
    "griechenland": "EL", "greece": "EL",
    "tschechien": "CZ", "czechia": "CZ",
    "rumänien": "RO", "romania": "RO",
    "ungarn": "HU", "hungary": "HU",
    "kroatien": "HR", "croatia": "HR",
    "bulgarien": "BG", "bulgaria": "BG",
    "slowakei": "SK", "slovakia": "SK",
    "slowenien": "SI", "slovenia": "SI",
    "luxemburg": "LU", "luxembourg": "LU",
    "estland": "EE", "estonia": "EE",
    "lettland": "LV", "latvia": "LV",
    "litauen": "LT", "lithuania": "LT",
    "malta": "MT", "zypern": "CY", "cyprus": "CY",
    "eu": "EU27_2020", "europa": "EU27_2020", "europe": "EU27_2020",
}


def _find_datasets(analysis: dict) -> list[dict]:
    """Find matching Eurostat datasets based on claim analysis."""
    entities = analysis.get("entities", [])
    subcategory = analysis.get("subcategory", "")
    category = analysis.get("category", "")
    claim = analysis.get("claim", "")
    search_terms = entities + [subcategory, category, claim]

    matched = {}
    for term in search_terms:
        for keyword, ds in DATASET_MAP.items():
            if keyword in term.lower():
                key = ds["dataset"]
                if key not in matched:
                    matched[key] = ds
    return list(matched.values())


def _find_country(analysis: dict) -> str:
    """Extract country code from entities."""
    entities = analysis.get("entities", [])
    for entity in entities:
        for name, code in COUNTRY_CODES.items():
            if name in entity.lower():
                return code
    return "EU27_2020"


def _parse_json_stat(data: dict, dataset_info: dict, geo_code: str) -> list[dict]:
    """Parse Eurostat JSON-stat 2.0 response into readable results."""
    results = []

    dimensions = data.get("id", [])
    sizes = data.get("size", [])
    values = data.get("value", {})
    dim_data = data.get("dimension", {})

    if not values or not dimensions:
        return results

    # Find time and geo dimension indices
    time_dim = None
    geo_dim = None
    for i, dim_id in enumerate(dimensions):
        if dim_id == "time" or dim_id == "TIME_PERIOD":
            time_dim = i
        if dim_id == "geo":
            geo_dim = i

    # Get geo and time labels
    geo_labels = {}
    time_labels = {}
    if geo_dim is not None and "geo" in dim_data:
        cat = dim_data["geo"].get("category", {})
        geo_labels = cat.get("label", {})
    if time_dim is not None:
        time_key = dimensions[time_dim]
        if time_key in dim_data:
            cat = dim_data[time_key].get("category", {})
            time_labels = cat.get("label", {})

    # Calculate strides for index mapping
    strides = []
    for i in range(len(sizes)):
        stride = 1
        for j in range(i + 1, len(sizes)):
            stride *= sizes[j]
        strides.append(stride)

    # Get category indices for each dimension
    dim_indices = []
    for dim_id in dimensions:
        if dim_id in dim_data:
            cat = dim_data[dim_id].get("category", {})
            index = cat.get("index", {})
            if isinstance(index, dict):
                dim_indices.append(index)
            else:
                dim_indices.append({})
        else:
            dim_indices.append({})

    # Iterate over values (sparse dict with string keys)
    for flat_idx_str, value in values.items():
        flat_idx = int(flat_idx_str)

        # Decode flat index into per-dimension indices
        remaining = flat_idx
        per_dim = []
        for s in strides:
            per_dim.append(remaining // s)
            remaining %= s

        # Get time and geo for this observation
        time_val = ""
        geo_val = geo_code
        if time_dim is not None:
            for code, idx in dim_indices[time_dim].items():
                if idx == per_dim[time_dim]:
                    time_val = time_labels.get(code, code)
                    break
        if geo_dim is not None:
            for code, idx in dim_indices[geo_dim].items():
                if idx == per_dim[geo_dim]:
                    geo_val = geo_labels.get(code, code)
                    break

        results.append({
            "title": f"{dataset_info['label']}: {geo_val} {time_val} — {value} {dataset_info['unit']}",
            "indicator": dataset_info["label"],
            "country": geo_val,
            "year": time_val,
            "value": f"{value} {dataset_info['unit']}",
            "source": "Eurostat",
            "url": f"https://ec.europa.eu/eurostat/databrowser/view/{dataset_info['dataset']}/default/table",
        })

    # Sort by time descending and limit
    results.sort(key=lambda r: r["year"], reverse=True)
    return results[:5]


# All EU-27 geo codes for multi-country queries
EU27_GEO_CODES = [
    "AT", "BE", "BG", "HR", "CY", "CZ", "DK", "EE", "FI", "FR",
    "DE", "EL", "HU", "IE", "IT", "LV", "LT", "LU", "MT", "NL",
    "PL", "PT", "RO", "SK", "SI", "ES", "SE", "EU27_2020",
]

SUPERLATIVE_KEYWORDS = [
    "höchste", "höchsten", "niedrigste", "niedrigsten", "meiste", "meisten",
    "größte", "größten", "beste", "besten", "schlechteste", "schlechtesten",
    "wenigste", "wenigsten", "stärkste", "stärksten",
    "highest", "lowest", "most", "least", "largest", "smallest", "best", "worst",
]


def _is_superlative_claim(analysis: dict) -> bool:
    """Check if the claim contains superlative keywords."""
    claim = analysis.get("claim", "").lower()
    entities = " ".join(analysis.get("entities", [])).lower()
    text = f"{claim} {entities}"
    return any(kw in text for kw in SUPERLATIVE_KEYWORDS)


def _parse_multi_country(data: dict, dataset_info: dict) -> list[dict]:
    """Parse Eurostat JSON-stat 2.0 response with multiple countries.

    Returns only the most recent value per country, sorted by value descending.
    """
    dimensions = data.get("id", [])
    sizes = data.get("size", [])
    values = data.get("value", {})
    dim_data = data.get("dimension", {})

    if not values or not dimensions:
        return []

    # Find dimension indices
    time_dim = geo_dim = None
    for i, dim_id in enumerate(dimensions):
        if dim_id in ("time", "TIME_PERIOD"):
            time_dim = i
        if dim_id == "geo":
            geo_dim = i

    # Get labels
    geo_labels = {}
    time_labels = {}
    if geo_dim is not None and "geo" in dim_data:
        geo_labels = dim_data["geo"].get("category", {}).get("label", {})
    if time_dim is not None:
        time_key = dimensions[time_dim]
        if time_key in dim_data:
            time_labels = dim_data[time_key].get("category", {}).get("label", {})

    # Calculate strides
    strides = []
    for i in range(len(sizes)):
        stride = 1
        for j in range(i + 1, len(sizes)):
            stride *= sizes[j]
        strides.append(stride)

    # Build dimension index maps
    dim_indices = []
    for dim_id in dimensions:
        if dim_id in dim_data:
            index = dim_data[dim_id].get("category", {}).get("index", {})
            dim_indices.append(index if isinstance(index, dict) else {})
        else:
            dim_indices.append({})

    # Collect all observations: {geo_code: {time: value}}
    observations: dict[str, dict[str, float]] = {}
    for flat_idx_str, value in values.items():
        flat_idx = int(flat_idx_str)
        remaining = flat_idx
        per_dim = []
        for s in strides:
            per_dim.append(remaining // s)
            remaining %= s

        geo_code = geo_label = ""
        time_val = ""
        if geo_dim is not None:
            for code, idx in dim_indices[geo_dim].items():
                if idx == per_dim[geo_dim]:
                    geo_code = code
                    geo_label = geo_labels.get(code, code)
                    break
        if time_dim is not None:
            for code, idx in dim_indices[time_dim].items():
                if idx == per_dim[time_dim]:
                    time_val = time_labels.get(code, code)
                    break

        if geo_code and time_val:
            if geo_code not in observations:
                observations[geo_code] = {}
            observations[geo_code][time_val] = (value, geo_label)

    # For each country, pick the most recent year
    latest_per_country = []
    for geo_code, time_data in observations.items():
        if not time_data:
            continue
        latest_time = max(time_data.keys())
        value, geo_label = time_data[latest_time]
        try:
            num_value = float(value)
        except (ValueError, TypeError):
            continue
        latest_per_country.append({
            "geo_code": geo_code,
            "country": geo_label,
            "year": latest_time,
            "value": num_value,
        })

    # Sort by value descending (highest first)
    latest_per_country.sort(key=lambda x: x["value"], reverse=True)

    # Format results with ranking
    results = []
    for rank, entry in enumerate(latest_per_country, 1):
        results.append({
            "title": f"#{rank} {entry['country']}: {entry['value']} {dataset_info['unit']} ({entry['year']})",
            "indicator": dataset_info["label"],
            "country": entry["country"],
            "geo": entry["geo_code"],
            "year": entry["year"],
            "value": f"{entry['value']} {dataset_info['unit']}",
            "rank": rank,
            "source": "Eurostat",
            "url": f"https://ec.europa.eu/eurostat/databrowser/view/{dataset_info['dataset']}/default/table",
        })

    return results


async def search_eurostat(analysis: dict) -> dict:
    """Search Eurostat for relevant EU statistics."""
    datasets = _find_datasets(analysis)
    geo_code = _find_country(analysis)
    superlative = _is_superlative_claim(analysis)

    all_results = []

    async with httpx.AsyncClient(timeout=30.0) as client:
        for ds in datasets[:2]:  # Max 2 datasets to keep it fast
            try:
                if superlative:
                    # Multi-country query: fetch all EU27 for ranking
                    params = {
                        "format": "JSON",
                        "lang": "EN",
                        "geo": EU27_GEO_CODES,
                        "lastTimePeriod": "1",  # Only most recent period
                        **{k: v for k, v in ds["params"].items() if k != "lastTimePeriod"},
                    }

                    resp = await client.get(
                        f"{BASE_URL}/{ds['dataset']}",
                        params=params,
                    )
                    resp.raise_for_status()
                    data = resp.json()

                    parsed = _parse_multi_country(data, ds)
                    # Keep top 10 + the mentioned country if outside top 10
                    top_results = parsed[:10]
                    mentioned_in_top = any(r["geo"] == geo_code for r in top_results)
                    if not mentioned_in_top and geo_code != "EU27_2020":
                        for r in parsed:
                            if r.get("geo") == geo_code:
                                top_results.append(r)
                                break
                    all_results.extend(top_results)
                else:
                    # Single-country query (default behavior)
                    params = {
                        "format": "JSON",
                        "lang": "EN",
                        "geo": geo_code,
                        **ds["params"],
                    }

                    resp = await client.get(
                        f"{BASE_URL}/{ds['dataset']}",
                        params=params,
                    )
                    resp.raise_for_status()
                    data = resp.json()

                    parsed = _parse_json_stat(data, ds, geo_code)
                    all_results.extend(parsed)

            except Exception as e:
                logger.warning(f"Eurostat request failed for {ds['dataset']}: {e}")
                all_results.append({
                    "title": f"{ds['label']}: Daten nicht verfügbar",
                    "indicator": ds["label"],
                    "country": geo_code,
                    "year": "",
                    "value": "Daten nicht verfügbar",
                    "source": "Eurostat",
                    "url": f"https://ec.europa.eu/eurostat/databrowser/view/{ds['dataset']}/default/table",
                })

    return {
        "source": "Eurostat (EU)",
        "type": "official_data",
        "results": all_results,
    }
