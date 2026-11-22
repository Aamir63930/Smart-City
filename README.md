
# Smart City: Air Quality and Pollution Intelligence

An end-to-end data engineering project that collects air quality, weather and city data for Indian cities, cleans it with **PySpark on Databricks**, organises it into **Bronze → Silver → Gold** Delta tables, and shows the results in an interactive **Power BI dashboard**.

> **Course:** B.Tech CSE (AI & ML), KR Mangalam University
> **Project type:** Data Engineering Industry Project
> **Team:** CodeSlayers

---

## Table of Contents

1. [Problem Statement](#problem-statement)
2. [What This Project Does](#what-this-project-does)
3. [Tech Stack](#tech-stack)
4. [Architecture](#architecture)
5. [Data Sources](#data-sources)
6. [Project Structure](#project-structure)
7. [How the Pipeline Works](#how-the-pipeline-works)
8. [Power BI Dashboard](#power-bi-dashboard)
9. [Key Findings](#key-findings)
10. [How to Run This Project](#how-to-run-this-project)
11. [Challenges and How We Solved Them](#challenges-and-how-we-solved-them)
12. [Limitations and Future Work](#limitations-and-future-work)
13. [Team](#team)
14. [Acknowledgements](#acknowledgements)

---

## Problem Statement

Air pollution is a big problem in Indian cities. To understand it, we need to know:

- Where is pollution the worst?
- At what time does it peak?
- How does the weather affect it?

But the raw data is spread across APIs and web pages, comes in different formats, and cannot be used directly. This project builds a clean, repeatable pipeline that solves this.

## What This Project Does

- Collects data from **3 sources** (2 APIs + 1 web-scraped page)
- Saves the raw data safely before changing anything
- Cleans and checks the data using **PySpark** on **Databricks**
- Stores data in **Delta Lake** using the **Medallion architecture** (Bronze, Silver, Gold)
- Builds **6 Gold tables** that answer real questions (hotspots, daily trends, alerts, weather impact)
- Shows everything in a **4-page Power BI dashboard**

## Tech Stack

| Part | Tools Used |
| --- | --- |
| Data extraction | Python, Requests, BeautifulSoup, Pandas (Google Colab) |
| Raw storage | Google Drive, Unity Catalog Volume |
| Processing | Databricks Free Edition, PySpark, Spark SQL |
| Storage format | Delta Lake, Unity Catalog |
| Dashboard | Power BI Desktop |
| Version control | Git and GitHub |

## Architecture

```
  Open-Meteo Air Quality API ─┐
  Open-Meteo Weather API ─────┼──►  Google Colab     ──►  Google Drive
  Wikipedia city table ───────┘     (Python notebooks)     (raw CSV files)
                                                                │
                                                                ▼
                                                    Databricks (PySpark)
                                              ┌────────────────────────────────┐
                                              │ BRONZE  →  SILVER  →  GOLD     │
                                              │ raw        cleaned    ready    │
                                              │            + typed    for BI   │
                                              └────────────────────────────────┘
                                                                │
                                                                ▼
                                                  Power BI Dashboard (4 pages)
```

## Data Sources

| Source | What we get | How we get it |
| --- | --- | --- |
| **Open-Meteo Air Quality API** | PM10, PM2.5, CO, NO₂, SO₂, Ozone, US AQI (hourly) | REST API (no key needed) |
| **Open-Meteo Weather API** | Temperature, humidity, wind speed, rainfall (hourly) | REST API (no key needed) |
| **Wikipedia: List of cities in India by population** | City, state, Census 2011 and 2001 population | Web scraping (BeautifulSoup) |

**Cities:** Delhi, Mumbai, Kolkata, Chennai, Bengaluru, Hyderabad, Ahmedabad, Pune, Jaipur, Lucknow

**Data size:** 31 days of hourly data, 744 rows per city, 7,440 rows for each API source (collected on 7 November 2026).

**Responsible data collection:**
- The APIs are free and public, and we wait 1 second between calls.
- We checked Wikipedia's `robots.txt` in code before scraping.
- The scraper makes only one request and uses a proper User-Agent.

> **Note:** Open-Meteo air quality values are *modelled* data, not readings from ground monitoring stations. They are good for comparing cities, but may differ from official readings.

## Project Structure

```
smart-city-air-quality-pipeline/
│
├── extraction/                 # Google Colab notebooks (Layer 1)
│   ├── 01_extract_air_quality.ipynb
│   ├── 02_extract_weather.ipynb
│   └── 03_scrape_city_table.ipynb
│
├── dataset/                    # Raw data files
│   ├── air_quality_raw.csv     # 7,440 rows
│   ├── weather_raw.csv         # 7,440 rows
│   └── cities_raw.csv          # 340 rows
│
├── databricks/                 # Databricks notebooks (Layer 2)
│   └── SmartAir_360_New_Measurements_Ingestion.ipynb
│
├── Dashboard/                  # Power BI files (Layer 3)
│   ├── BigDataDashboard.pbix
│   ├── Dashboard1.png
│   ├── Dashboard2.png
│   ├── Dashboard3.png
│   └── Dashboard4.png
│
├── docs/
│   ├── Smart_City_AQI_Project_Report_v3.docx
│   └── Smart_City_AQI_Updated.pptx
│
└── README.md
```

## How the Pipeline Works

### Layer 1: Data Extraction (Google Colab)

Databricks Free Edition blocks internet access, so we collect data in Google Colab first.

| Notebook | What it does | Output |
| --- | --- | --- |
| `01_extract_air_quality` | Gets city coordinates, then downloads hourly air quality | `air_quality_raw.csv` |
| `02_extract_weather` | Downloads matching hourly weather | `weather_raw.csv` |
| `03_scrape_city_table` | Checks robots.txt, then scrapes the Wikipedia city table | `cities_raw.csv` |

**Reliability features:** request timeouts, `raise_for_status()` for HTTP errors, `try/except` for each city (one failure does not stop the run), and an ingestion timestamp on every record.

### Layer 2: Data Engineering (Databricks + PySpark)

Catalog: `workspace` | Schema: `smartair` | Volume: `raw_data`

#### 🥉 Bronze: raw data

- Raw JSON files are uploaded to a Unity Catalog Volume and read with Spark (`multiLine` option).
- Data is **appended** to `bronze_openaq_measurements` with `mergeSchema` turned on, so old and new file layouts both load.
- Bronze keeps data exactly as received. No cleaning here.

#### 🥈 Silver: cleaned data

- The nested `results` array is **exploded** so each measurement becomes one row.
- Different JSON layouts are combined using `coalesce()`.
- Columns are renamed and given correct types (double, timestamp).
- Rows with missing sensor, city, pollutant, time or value are removed.
- **Duplicates are removed** using `row_number()`.
- New rows are added with `MERGE INTO ... WHEN NOT MATCHED`, so running the notebook again never creates duplicates.
- A null check on key columns returned **0 nulls**.

| Silver Table | Rows |
| --- | --- |
| `silver_openaq_measurements` | 101,042 |
| `silver_weather` | 1,488 |

#### 🥇 Gold: ready for analysis

| Gold Table | Rows | Purpose |
| --- | --- | --- |
| `gold_pollutant_analysis` | 6 | Average, min, max for each pollutant |
| `gold_daily_aqi_trends` | 1,416 | Daily values by city, location and pollutant |
| `gold_pollution_hotspots` | 52 | Top 10 locations per pollutant |
| `gold_city_aqi_summary` | 50 | Summary for each city and pollutant |
| `gold_weather_pollution_analysis` | 6,541 | Pollution joined with weather by hour |
| `gold_aqi_alerts` | 31,095 | PM10 and PM2.5 with Low / Moderate / High alert |

**Alert levels:**

| Pollutant | Low | Moderate | High |
| --- | --- | --- | --- |
| PM10 | below 50 | 50 to below 100 | 100 or more |
| PM2.5 | 30 or less | above 30 to 60 | above 60 |

### Layer 3: Power BI Dashboard

Power BI Desktop connects to the Gold tables using the **Azure Databricks connector** (Import mode).

## Power BI Dashboard

| Page | What it shows |
| --- | --- |
| **1. Alert Analytics** | Alert level donut chart, PM10 vs PM2.5 over time, average concentration by city, map |
| **2. City-Wise Analysis** | Sensors per city, min / avg / max values, treemap, map |
| **3. Daily Trends** | Day-by-day pollution, city comparison, pollutant share |
| **4. Weather Impact** | KPI cards, hourly trend, temperature vs pollution scatter chart, map |

### Screenshots

**Page 1: Alert Analytics**
![Dashboard Page 1](Dashboard/Dashboard1.png)

**Page 2: City-Wise Analysis**
![Dashboard Page 2](Dashboard/Dashboard2.png)

**Page 3: Daily Trends**
![Dashboard Page 3](Dashboard/Dashboard3.png)

**Page 4: Weather Impact**
![Dashboard Page 4](Dashboard/Dashboard4.png)

## Key Findings

- **43.14%** of PM10 / PM2.5 records are at **High** alert, **37.48%** are Moderate and **19.38%** are Low. About 4 out of 5 records are above the Low level.
- **Gurugram and Delhi** have the highest PM10 + PM2.5 levels. **Hyderabad** is the lowest.
- PM10 stays higher than PM2.5 for the whole period.
- The highest values appear around **8 November**.
- The 52 sensors are not evenly spread (Delhi has 8, Hyderabad has only 3), so Hyderabad's low ranking is based on less data.
- The weather scatter plot shows **no clear pattern**. This is only an indication, and it does **not** prove that weather causes pollution changes.

> Some dashboard charts add up values (for example, total concentration). Use them for **ranking and comparison**, not to read the real pollution level.


## Challenges and How We Solved Them

| Challenge | Solution |
| --- | --- |
| Databricks Free Edition blocks internet access | Collected data in Colab, then uploaded a zip file to a Databricks Volume |
| 46 of 176 JSON files were empty (0 bytes) | Found them by file size and `json.loads`; loaded only the 130 valid files and checked that 130 + 46 = 176 |
| Old and new JSON files had different layouts | Used `mergeSchema` in Bronze and `coalesce()` in Silver |
| Bronze append ran twice (duplicate records) | Silver removes duplicates and uses `MERGE`, so Silver stayed at 101,042 rows |
| Pollution and weather had different time and city formats | Joined on lower-case city name and the hour using `date_trunc` |
| Power BI adds up numbers by default, which inflates totals | Used average measures for KPI cards |

## Limitations and Future Work

**Limitations**
- Only about 1 month of data, so we cannot study seasons.
- Air quality values are modelled, not from ground stations.
- Moving data from Drive to Databricks is manual.
- The dashboard "Total Readings" value (about 23K) could not be matched with a notebook count.

**Future work**
- Schedule the pipeline to run daily with Lakeflow Jobs
- Add more cities and ground-station data
- Add automatic data-quality checks and alerts
- Find the main pollutant per city and build AQI forecasting

## Team

| Name | Roll No. | Role |
| --- | --- | --- |
| Devi Singh | 2401730260 | Data Extraction |
| Aamir | 2401730238 | Data Engineering / ETL |
| Sukesh | 2401730273 | Dashboard Development |
| Azhar | 2401730134 | Data Validation and Documentation |