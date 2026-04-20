import asyncio

import httpx
import logging

logger = logging.getLogger("evidora")

BASE_URL = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data"

# Map keywords (DE + EN) to Eurostat dataset codes + query parameters
DATASET_MAP = {
    # Inflation / Preise — prc_hicp_aind liefert Jahreswerte (annual rate of change),
    # passt zu Jahres-Vergleichs-Claims ("Inflation 2024 in AT vs. EU-Durchschnitt").
    # Der frühere monatliche Datensatz prc_hicp_manr passte schlecht zu Jahres-Claims.
    "inflation": {
        "dataset": "prc_hicp_aind",
        "label": "Jährliche Inflationsrate (HVPI)",
        "label_en": "Annual Inflation Rate (HICP)",
        "params": {"coicop": "CP00", "unit": "RCH_A_AVG", "lastTimePeriod": "5"},
        "unit": "%",
    },
    "preise": {
        "dataset": "prc_hicp_aind",
        "label": "Jährliche Inflationsrate (HVPI)",
        "label_en": "Annual Inflation Rate (HICP)",
        "params": {"coicop": "CP00", "unit": "RCH_A_AVG", "lastTimePeriod": "5"},
        "unit": "%",
    },
    "teuerung": {
        "dataset": "prc_hicp_aind",
        "label": "Jährliche Inflationsrate (HVPI)",
        "label_en": "Annual Inflation Rate (HICP)",
        "params": {"coicop": "CP00", "unit": "RCH_A_AVG", "lastTimePeriod": "5"},
        "unit": "%",
    },
    "prices": {
        "dataset": "prc_hicp_aind",
        "label": "Jährliche Inflationsrate (HVPI)",
        "label_en": "Annual Inflation Rate (HICP)",
        "params": {"coicop": "CP00", "unit": "RCH_A_AVG", "lastTimePeriod": "5"},
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
    "bruttoinlandsprodukt": {
        "dataset": "nama_10_gdp",
        "label": "Bruttoinlandsprodukt",
        "label_en": "Gross Domestic Product",
        "params": {"na_item": "B1GQ", "unit": "CP_MEUR", "lastTimePeriod": "5"},
        "unit": "Mio. €",
    },
    "wirtschaftsleistung": {
        "dataset": "nama_10_gdp",
        "label": "Bruttoinlandsprodukt",
        "label_en": "Gross Domestic Product",
        "params": {"na_item": "B1GQ", "unit": "CP_MEUR", "lastTimePeriod": "5"},
        "unit": "Mio. €",
    },
    "wirtschaftswachstum": {
        "dataset": "nama_10_gdp",
        "label": "Bruttoinlandsprodukt (Wachstum)",
        "label_en": "GDP (Growth)",
        "params": {"na_item": "B1GQ", "unit": "CLV_PCH_PRE", "lastTimePeriod": "5"},
        "unit": "% Veränderung",
    },
    # BIP pro Kopf (für "reichstes Land" etc.)
    "reich": {
        "dataset": "nama_10_pc",
        "label": "BIP pro Kopf (KKS)",
        "label_en": "GDP per Capita (PPS)",
        "params": {"na_item": "B1GQ", "unit": "CP_PPS_HAB", "lastTimePeriod": "5"},
        "unit": "KKS pro Kopf",
    },
    "wohlstand": {
        "dataset": "nama_10_pc",
        "label": "BIP pro Kopf (KKS)",
        "label_en": "GDP per Capita (PPS)",
        "params": {"na_item": "B1GQ", "unit": "CP_PPS_HAB", "lastTimePeriod": "5"},
        "unit": "KKS pro Kopf",
    },
    "pro kopf": {
        "dataset": "nama_10_pc",
        "label": "BIP pro Kopf (KKS)",
        "label_en": "GDP per Capita (PPS)",
        "params": {"na_item": "B1GQ", "unit": "CP_PPS_HAB", "lastTimePeriod": "5"},
        "unit": "KKS pro Kopf",
    },
    "per capita": {
        "dataset": "nama_10_pc",
        "label": "BIP pro Kopf (KKS)",
        "label_en": "GDP per Capita (PPS)",
        "params": {"na_item": "B1GQ", "unit": "CP_PPS_HAB", "lastTimePeriod": "5"},
        "unit": "KKS pro Kopf",
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
    """Find matching Eurostat datasets based on claim analysis.

    Search order: specific terms first (keywords, claim text) before generic
    ones (subcategory, category) so that e.g. "Jugendarbeitslosigkeit" matches
    the youth-specific dataset before "unemployment" catches the generic one.
    """
    keywords = analysis.get("spacy_keywords", [])
    entities = analysis.get("entities", [])
    claim = analysis.get("claim", "")
    subcategory = analysis.get("subcategory", "")
    category = analysis.get("category", "")
    search_terms = keywords + entities + [claim, subcategory, category]

    matched = {}
    # Sort keywords longest-first so specific matches (e.g. "jugendarbeitslosigkeit")
    # win over generic substrings (e.g. "arbeitslosigkeit") for the same dataset
    sorted_keywords = sorted(DATASET_MAP.keys(), key=len, reverse=True)
    for term in search_terms:
        term_lower = term.lower()
        for keyword in sorted_keywords:
            if keyword in term_lower:
                ds = DATASET_MAP[keyword]
                key = ds["dataset"]
                if key not in matched:
                    matched[key] = ds
    return list(matched.values())


def _find_country(analysis: dict) -> str:
    """Extract country code from claim text.

    Prioritizes SpaCy NER entities (deterministic, from actual claim text)
    over LLM-extracted entities to avoid hallucinated country references.
    Falls back to checking the claim text directly for country name substrings.
    """
    # 1. SpaCy NER countries — guaranteed from actual claim text
    ner_countries = analysis.get("ner_entities", {}).get("countries", [])
    for country in ner_countries:
        country_lower = country.lower()
        for name, code in COUNTRY_CODES.items():
            if name in country_lower:
                return code

    # 2. Check claim text directly (catches adjective forms like "österreichische")
    claim = analysis.get("claim", "")
    claim_lower = claim.lower()
    for name, code in COUNTRY_CODES.items():
        if name in claim_lower:
            return code

    # 3. Default to EU-27 (no country hallucinated by LLM)
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
    # Grundlegende Superlative
    "höchste", "höchsten", "niedrigste", "niedrigsten", "meiste", "meisten",
    "größte", "größten", "kleinste", "kleinsten",
    "beste", "besten", "schlechteste", "schlechtesten",
    "wenigste", "wenigsten", "stärkste", "stärksten", "schwächste", "schwächsten",
    # Wirtschaft / Wohlstand
    "reichste", "reichsten", "ärmste", "ärmsten",
    "teuerste", "teuersten", "billigste", "billigsten", "günstigste", "günstigsten",
    "produktivste", "produktivsten",
    # Wachstum / Geschwindigkeit
    "schnellste", "schnellsten", "langsamste", "langsamsten",
    # Demografie
    "älteste", "ältesten", "jüngste", "jüngsten",
    # Umwelt / Sicherheit
    "sicherste", "sichersten", "gefährlichste", "gefährlichsten",
    "sauberste", "saubersten", "schmutzigste", "schmutzigsten",
    # Informelle Ranking-Begriffe
    "führend", "führende", "führendes", "führenden",
    "spitzenreiter", "schlusslicht", "vorreiter",
    "nummer eins", "number one", "platz eins", "platz 1",
    # Englisch
    "highest", "lowest", "most", "least", "largest", "smallest", "best", "worst",
    "richest", "poorest", "safest", "cleanest", "fastest", "slowest",
    "oldest", "youngest", "cheapest", "most expensive",
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

    async def _fetch_dataset(client: httpx.AsyncClient, ds: dict) -> list[dict]:
        """Fetch a single Eurostat dataset (single- or multi-country)."""
        try:
            if superlative:
                params = {
                    "format": "JSON",
                    "lang": "EN",
                    "geo": EU27_GEO_CODES,
                    "lastTimePeriod": "1",
                    **{k: v for k, v in ds["params"].items() if k != "lastTimePeriod"},
                }
                resp = await client.get(f"{BASE_URL}/{ds['dataset']}", params=params)
                resp.raise_for_status()
                parsed = _parse_multi_country(resp.json(), ds)
                top_results = parsed[:10]
                if not any(r["geo"] == geo_code for r in top_results) and geo_code != "EU27_2020":
                    for r in parsed:
                        if r.get("geo") == geo_code:
                            top_results.append(r)
                            break
                return top_results
            else:
                # Fetch country data
                params = {
                    "format": "JSON",
                    "lang": "EN",
                    "geo": geo_code,
                    **ds["params"],
                }
                resp = await client.get(f"{BASE_URL}/{ds['dataset']}", params=params)
                resp.raise_for_status()
                # Cap country rows at 3 most recent so the LLM still sees EU comparison
                # rows within its 5-row per-source limit (3 country + 2 EU = 5).
                country_results = _parse_json_stat(resp.json(), ds, geo_code)[:3]

                # Country-vs-EU comparison: fetch EU27_2020 aggregate alongside
                # (Bug D). Without this, claims like "AT inflation above EU avg"
                # can only see AT rows and fail the comparison. Silent-skip when
                # the dataset has no EU aggregate or the claim is already EU-wide.
                aggregate_geos = {"EU27_2020", "EA", "EA20", "EA19"}
                if geo_code not in aggregate_geos:
                    try:
                        eu_params = {
                            "format": "JSON",
                            "lang": "EN",
                            "geo": "EU27_2020",
                            **ds["params"],
                        }
                        eu_resp = await client.get(f"{BASE_URL}/{ds['dataset']}", params=eu_params)
                        eu_resp.raise_for_status()
                        eu_results = _parse_json_stat(eu_resp.json(), ds, "EU27_2020")[:2]
                        country_results.extend(eu_results)
                    except Exception as eu_err:
                        logger.debug(
                            f"Eurostat EU27 aggregate fetch failed for {ds['dataset']}: {eu_err}"
                        )

                return country_results
        except Exception as e:
            logger.warning(f"Eurostat request failed for {ds['dataset']}: {e}")
            return [{
                "title": f"{ds['label']}: Daten nicht verfügbar",
                "indicator": ds["label"],
                "country": geo_code,
                "year": "",
                "value": "Daten nicht verfügbar",
                "source": "Eurostat",
                "url": f"https://ec.europa.eu/eurostat/databrowser/view/{ds['dataset']}/default/table",
            }]

    async with httpx.AsyncClient(timeout=30.0) as client:
        # Fetch datasets in parallel instead of sequentially
        tasks = [_fetch_dataset(client, ds) for ds in datasets[:2]]
        results_list = await asyncio.gather(*tasks)
        for results in results_list:
            all_results.extend(results)

    # Add GDP/economy multi-dimensional context caveat
    gdp_datasets = {"nama_10_gdp", "nama_10_pc"}
    if any(ds["dataset"] in gdp_datasets for ds in datasets[:2]) and all_results:
        all_results.append({
            "title": "WICHTIGER KONTEXT: BIP ist kein umfassendes Wohlstandsmaß",
            "indicator": "Methodische Einordnung",
            "country": "",
            "year": "",
            "value": "",
            "source": "Eurostat / OECD",
            "url": "https://ec.europa.eu/eurostat/databrowser/view/nama_10_gdp/default/table",
            "description": (
                "Das Bruttoinlandsprodukt (BIP) misst den Marktwert aller produzierten Güter und "
                "Dienstleistungen innerhalb der Landesgrenzen. Es ist KEIN Wohlstandsmaß. "
                "Einschränkungen: "
                "(1) BIP absolut vs. pro Kopf vs. KKS — Luxemburg hat das höchste BIP pro Kopf in der EU, "
                "aber nur wegen der 200.000+ Grenzpendler, die zur Produktion beitragen, aber nicht "
                "zur Bevölkerung zählen. Kaufkraftstandards (KKS) berücksichtigen Preisniveauunterschiede. "
                "(2) Verteilung — hohes BIP sagt nichts über Ungleichheit aus; der Gini-Koeffizient, "
                "Median-Einkommen und Armutsgefährdungsquote sind aussagekräftiger für Lebensstandards. "
                "(3) Unbezahlte Arbeit — Hausarbeit, Pflege und Ehrenamt fehlen im BIP. "
                "(4) Umweltkosten — Ressourcenverbrauch und Umweltzerstörung fließen positiv ins BIP ein "
                "(z.B. Aufräumarbeiten nach Naturkatastrophen). "
                "(5) Gesundheit & Bildung — BIP misst weder Lebenserwartung noch Bildungsqualität "
                "noch subjektives Wohlbefinden (vgl. HDI, Better Life Index, BIP und Wohlfahrt). "
                "(6) Nominell vs. real — ohne Inflationsbereinigung sind Zeitvergleiche irreführend."
            ),
        })

    # Add migration multi-dimensional context caveat if asylum/migration data was returned
    migration_datasets = {"migr_asyappctza", "migr_imm1ctz"}
    if any(ds["dataset"] in migration_datasets for ds in datasets[:2]) and all_results:
        all_results.append({
            "title": "WICHTIGER KONTEXT: Migrations- und Asylzahlen sind mehrdimensional",
            "indicator": "Methodische Einordnung",
            "country": "",
            "year": "",
            "value": "",
            "source": "Eurostat",
            "url": "https://ec.europa.eu/eurostat/databrowser/view/migr_asyappctza/default/table",
            "description": (
                "Eurostat-Asylstatistiken (migr_asyappctza) zählen Erstanträge — nicht Anerkennungen, "
                "nicht Gesamtzuwanderung. Einschränkungen: "
                "(1) Nur Asylanträge — Arbeitsmigration, EU-Binnenmobilität, Familiennachzug und "
                "Studierendenmigration (~80 % der Zuwanderung in viele EU-Länder) fehlen. "
                "(2) Absolute vs. Pro-Kopf-Zahlen — Deutschland hat die meisten Erstanträge absolut, "
                "pro Kopf führen oft kleinere Länder (z.B. Zypern, Österreich). "
                "(3) Anträge ≠ Anerkennungen — die Schutzquote variiert stark nach Herkunftsland "
                "(z.B. Syrien >90 %, Serbien <5 %) und Aufnahmeland. "
                "(4) Dublin-Verfahren — Anträge werden im Ersteinreiseland registriert, was "
                "Grenzstaaten (Griechenland, Italien) überproportional belastet. "
                "(5) Integration — Antragszahlen sagen nichts über Beschäftigung, Bildungsteilhabe "
                "oder fiskalische Effekte der Migration aus."
            ),
        })

    # Add CO₂ multi-dimensional context caveat if emission data was returned
    co2_datasets = {"env_air_gge"}
    if any(ds["dataset"] in co2_datasets for ds in datasets[:2]) and all_results:
        all_results.append({
            "title": "WICHTIGER KONTEXT: CO₂-Emissionen sind mehrdimensional",
            "indicator": "Methodische Einordnung",
            "country": "",
            "year": "",
            "value": "",
            "source": "Eurostat / IPCC",
            "url": "https://ec.europa.eu/eurostat/databrowser/view/env_air_gge/default/table",
            "description": (
                "Territoriale Treibhausgasemissionen (Eurostat env_air_gge) messen nur die Emissionen "
                "innerhalb der Landesgrenzen. Sie erfassen NICHT: "
                "(1) Konsumbasierte Emissionen — importierte Güter verlagern Emissionen ins Ausland; "
                "Länder mit viel Industrie-Import (z.B. Schweiz, Luxemburg) erscheinen sauberer als sie sind. "
                "(2) Pro-Kopf vs. absolut — kleine Länder haben niedrige Absolutwerte, aber teils hohe "
                "Pro-Kopf-Emissionen (z.B. Luxemburg: ~15 t/Kopf vs. EU-Schnitt ~6 t/Kopf). "
                "(3) Historische Kumulativ-Emissionen — die Klimawirkung hängt von der Gesamtmenge seit "
                "Industrialisierung ab, nicht nur vom aktuellen Jahreswert. "
                "(4) Methan & N₂O — 'CO₂-Äquivalente' gewichten CH₄ (GWP-100: 28×) und N₂O (265×) mit ein; "
                "reines CO₂ allein unterschätzt den Klimaeffekt der Landwirtschaft. "
                "(5) LULUCF — Landnutzung und Forstwirtschaft (CO₂-Senken) sind in TOTX4_MEMO ausgeschlossen. "
                "Ein vollständiger Emissionsvergleich erfordert alle Dimensionen."
            ),
        })

    return {
        "source": "Eurostat (EU)",
        "type": "official_data",
        "results": all_results,
    }
