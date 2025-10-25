"""
Africa Energy Data ETL Pipeline
-------------------------------
1. Extract data from Africa Energy Portal API using cloudscraper.
2. Transform and structure the data into a unified yearly format.
3. Validate and clean for missing or duplicate records.
4. Load into MongoDB Atlas cluster.

Author: Peter Kiptoo Ronoh
"""

import cloudscraper
import json
import os
import time
from collections import defaultdict, Counter
from pymongo import MongoClient
from urllib.parse import quote_plus
import pandas as pd


# 1. DATA EXTRACTION

def extract_data(output_file="africa_energy_data.json"):
    """Scrape electricity data from Africa Energy Portal."""
    scraper = cloudscraper.create_scraper(
        browser={"browser": "chrome", "platform": "windows", "mobile": False}
    )

    url = "https://africa-energy-portal.org/get-database-data"
    payload = {
        "mainGroup": "Electricity",
        "mainIndicator[]": ["Access", "Supply", "Technical"],
        "mainIndicatorValue[]": [
            "Population access to electricity-National (% of population)",
            "Population access to electricity-Rural (% of population)",
            "Population access to electricity-Urban (% of population)",
            "Population with access to electricity-National (millions of people)",
            "Population with access to electricity-Rural (millions of people)",
            "Population with access to electricity-Urban (millions of people)",
            "Population without access to electricity-National (millions of people)",
            "Population without access to electricity-Rural (millions of people)",
            "Population without access to electricity-Urban (millions of people)",
            "Electricity export (GWh)",
            "Electricity final consumption (GWh)",
            "Electricity final consumption per capita (KWh)",
            "Electricity generated from biofuels and waste (GWh)",
            "Electricity generated from fossil fuels (GWh)",
            "Electricity generated from geothermal energy (GWh)",
            "Electricity generated from hydropower (GWh)",
            "Electricity generated from nuclear power (GWh)",
            "Electricity generated from renewable sources (GWh)",
            "Electricity generated from solar, wind, tide, wave and other sources (GWh)",
            "Electricity generation per capita (KWh)",
            "Electricity generation, Total (GWh)",
            "Electricity import (GWh)",
            "Electricity: Net imports (+ GWh)",
            "Electricity installed capacity in Bioenergy (MW)",
            "Electricity installed capacity in Fossil fuels (MW)",
            "Electricity installed capacity in Geothermal (MW)",
            "Electricity installed capacity in Hydropower (MW)",
            "Electricity installed capacity in Non-renewable energy (MW)",
            "Electricity installed capacity in Nuclear (MW)",
            "Electricity installed capacity in Solar (MW)",
            "Electricity installed capacity in Total renewable energy (MW)",
            "Electricity installed capacity in Wind (MW)",
            "Electricity installed capacity in other Non-renewable energy (MW)",
            "Electricity installed capacity, Total (MW)"
        ],
        "year[]": list(range(2000, 2023)),
        "name[]": [
            "Algeria", "Angola", "Benin", "Botswana", "Burkina Faso", "Burundi",
            "Cameroon", "Cape Verde", "Central African Republic", "Chad",
            "Comoros", "Congo Democratic Republic", "Congo Republic", "Cote d'Ivoire",
            "Djibouti", "Egypt", "Equatorial Guinea", "Eritrea", "Eswatini",
            "Ethiopia", "Gabon", "Gambia", "Ghana", "Guinea", "Guinea Bissau",
            "Kenya", "Lesotho", "Liberia", "Libya", "Madagascar", "Malawi",
            "Mali", "Mauritania", "Mauritius", "Morocco", "Mozambique",
            "Namibia", "Niger", "Nigeria", "Rwanda", "Sao Tome and Principe",
            "Senegal", "Seychelles", "Sierra Leone", "Somalia", "South Africa",
            "South Sudan", "Sudan", "Tanzania", "Togo", "Tunisia", "Uganda",
            "Zambia", "Zimbabwe"
        ]
    }

    print("Fetching data from Africa Energy Portal...")
    response = scraper.post(url, data=payload)
    print(f"Status Code: {response.status_code}")

    try:
        data = response.json()
        if isinstance(data, list):
            records = data
        elif isinstance(data, dict):
            records = data.get("data", [])
        else:
            records = []

        print(f"Total records fetched: {len(records)}")

        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(records, f, ensure_ascii=False, indent=2)
        print(f"Data saved to {output_file}")

    except Exception as e:
        print("Error during extraction:", e)


# 2. TRANSFORMATION

def transform_data(input_file="africa_energy_data.json", output_file="formatted_africa_energy_data.json"):
    """Format raw API data into a structured table by year and country."""
    with open(input_file, "r", encoding="utf-8") as f:
        raw_data = json.load(f)

    years = list(range(2000, 2025))
    formatted = {}

    for record in raw_data:
        country = record.get("name")
        code = record.get("id")
        metric = record.get("indicator_name")
        unit = record.get("unit")
        sector = record.get("indicator_group")
        sub_sector = record.get("indicator_topic")
        year = record.get("year")
        value = record.get("score")
        link = "https://africa-energy-portal.org" + record.get("url", "")

        key = (country, metric)
        if key not in formatted:
            formatted[key] = {
                "country": country,
                "country_serial": code,
                "metric": metric,
                "unit": unit,
                "sector": sector,
                "sub_sector": sub_sector,
                "source_link": link,
                "source": "Africa Energy Portal"
            }
            for y in years:
                formatted[key][str(y)] = None

        if str(year) in formatted[key]:
            formatted[key][str(year)] = value

    formatted_list = list(formatted.values())
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(formatted_list, f, ensure_ascii=False, indent=2)

    print(f"Formatted {len(formatted_list)} records saved to {output_file}")


# 3. VALIDATION

def validate_data(input_file="formatted_africa_energy_data.json"):
    """Perform data integrity checks."""
    with open(input_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    years = [str(y) for y in range(2000, 2023)]
    expected_subsectors = {"Access", "Supply", "Technical"}

    missing_years_summary = []
    duplicates = set()
    seen = set()
    country_subsectors = defaultdict(set)

    for record in data:
        country = record.get("country")
        metric = record.get("metric")
        subsector = record.get("sub_sector")

        if country and subsector:
            country_subsectors[country].add(subsector)

        combo_key = (country, metric)
        if combo_key in seen:
            duplicates.add(combo_key)
        else:
            seen.add(combo_key)

        missing_years = [year for year in years if record.get(year) in (None, "", "NaN")]
        if missing_years:
            missing_years_summary.append({
                "country": country,
                "metric": metric,
                "missing_years": missing_years
            })

    missing_subsector_summary = []
    for country, subsectors in country_subsectors.items():
        missing = expected_subsectors - subsectors
        if missing:
            missing_subsector_summary.append({
                "country": country,
                "missing_subsectors": list(missing)
            })

    with open("validation_report.json", "w", encoding="utf-8") as f:
        json.dump({
            "missing_years": missing_years_summary,
            "duplicates": list(duplicates),
            "missing_subsectors": missing_subsector_summary
        }, f, ensure_ascii=False, indent=2)

    print("Validation report saved as validation_report.json")


# 4. LOAD TO MONGODB ATLAS

def load_to_mongo(json_file="formatted_africa_energy_data.json"):
    """Upload cleaned data to MongoDB Atlas."""
    DB_USERNAME = "pitronsoy_db_user"
    DB_PASSWORD = "K1pl4ng'4t18@64.."
    CLUSTER_HOST = "africaenergydb.9daoan.mongodb.net"
    DB_NAME = "AfricaEnergyDB"
    COLLECTION_NAME = "EnergyData"

    encoded_password = quote_plus(DB_PASSWORD)
    MONGO_URI = f"mongodb+srv://{DB_USERNAME}:{encoded_password}@{CLUSTER_HOST}/?retryWrites=true&w=majority"

    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]
        print("Connected to MongoDB Atlas.")
    except Exception as e:
        print("Connection failed:", e)
        return

    if not os.path.exists(json_file):
        print(f"File not found: {json_file}")
        return

    with open(json_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    batch_size = 500
    try:
        if isinstance(data, list):
            for i in range(0, len(data), batch_size):
                collection.insert_many(data[i:i + batch_size])
            print(f"Inserted {len(data)} records into '{COLLECTION_NAME}'.")
        else:
            collection.insert_one(data)
    except Exception as e:
        print("Failed to insert data:", e)
        return

    print("Data successfully uploaded to MongoDB Atlas.")


# EXECUTION PIPELINE

if __name__ == "__main__":
    extract_data()
    transform_data()
    validate_data()
    load_to_mongo()

