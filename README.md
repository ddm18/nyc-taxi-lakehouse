# NYC Urban Mobility & Fare Dynamics – Data Engineering Project

## 1. Project Overview

The goal of this project is to build a consolidated and historical data foundation
for **New York City taxi mobility demand and fare dynamics**, enriched with
weather information (and later airport traffic data), in order to support
urban mobility analysis and **recent operational trend monitoring**.

The platform will enable analysis of:

- Short-term fare trends
- Trip demand patterns
- Peak vs off-peak behavior
- Differences across city zones
- Weather impact on mobility and pricing
- Airport-related mobility dynamics

The platform focuses primarily on **recent mobility dynamics**, with analytical
workloads typically covering **the most recent three months of data**, while
preserving full historical raw data for reproducibility and backfill.

The focus of this project is **data engineering**, not real-time processing
or machine learning.

The objective is to design a **production-like data platform**
with governance, historical retention, data contracts, and quality controls.

---

## 2. Stakeholders

### Primary Stakeholder

- Urban mobility analysts
- Transportation strategy teams
- Urban planning observers

### Stakeholder Characteristics

- Non-technical but quantitatively oriented
- Interested in **trend analysis and behavioral patterns**
- Require structured and reliable datasets
- Need reproducibility and reliability
- Focused on business interpretation rather than raw data

---

## 3. Business Requirements

Stakeholders want to:

- Understand how taxi fares evolve over recent periods
- Analyze when demand is higher (hourly, daily, seasonally)
- Compare mobility behavior across city zones
- Identify peak congestion periods
- Monitor short-term demand changes
- Compare weekday vs weekend demand
- Evaluate whether weather conditions influence trip volume and pricing
- Analyze airport-related mobility patterns

### Non-Functional Constraints

- Data does **not need to be real-time**
- Monthly freshness is sufficient
- Base granularity must be **trip-level**
- Historical raw data must be **fully retained**
- Analytical workloads focus primarily on **recent operational windows**
- Reproducibility must be guaranteed
- No manual workflows (e.g., Excel exports)
- Incremental ingestion must be supported
- Schema evolution must be handled safely

---

## 4. Key Analytical Dimensions

### Time

- Trip timestamp (base grain)
- Hour of day
- Day
- Week
- Month
- Quarter
- Season
- Day-over-day comparisons
- Week-over-week comparisons
- Month-over-month comparisons

### Geography

- Taxi zones (official TLC zones)
- Borough
- Business-defined behavioral clusters
- Airport vs non-airport zones

### Trip Characteristics

- Fare amount
- Trip distance
- Passenger count
- Payment type
- Tip amount
- Trip duration

### Weather (Phase 2)

- Temperature
- Precipitation
- Snow indicators
- Severe weather flags

### Airport Traffic (Phase 3)

- Passenger arrivals per airport
- Daily airport traffic volume
- Airport demand index

---

## 5. Business Views

### 5.1 Geographical Interpretation

From a behavioral perspective, NYC can be grouped into:

#### Business Districts
- Midtown
- Financial District
- Downtown Manhattan

#### Residential Zones
- Upper East Side
- Upper West Side
- Brooklyn residential districts

#### Nightlife & Entertainment Areas
- Lower Manhattan
- Williamsburg
- SoHo
- Meatpacking District

#### Airport Zones
- JFK
- LaGuardia
- Newark

#### Commuter Corridors
- Manhattan ↔ Brooklyn
- Manhattan ↔ Queens
- Borough-to-borough interconnections

These clusters represent **behavioral groupings**, not strict administrative boundaries.

---

### 5.2 Temporal Interpretation

Business-relevant time classifications may include:

- Rush hours
- Late-night mobility
- Weekend demand shifts
- Holiday peaks
- Severe weather days
- Summer vs winter patterns
- Airport high-demand season

Definitions such as **peak demand periods** or **mobility disruptions**
must be formalized in the semantic layer.

---

## 6. Data Sources

### Source Overview

| Domain | Source | Format | Granularity | Ingestion Mode |
|--------|--------|--------|-------------|----------------|
| Taxi Trips | NYC TLC | Parquet | Trip-level | Monthly append |
| Taxi Zones | NYC TLC | GeoJSON / CSV | Static | Static |
| Weather (Phase 2) | NOAA GHCN | CSV / API | Station × Day | Historical + Incremental |
| Airport Traffic (Phase 3) | TSA / Aviation data | CSV | Airport × Day | Monthly |
| Holidays & Events | Public datasets | CSV / API | Event × Date | Incremental |

### Reference Links

- https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- https://registry.opendata.aws/nyc-tlc-trip-records-pds/
- https://www.ncdc.noaa.gov/cdo-web/webservices/v2
- https://www.tsa.gov/travel/passenger-volumes

---

## 7. Data Management Principles

- External datasets are treated as **upstream providers**
- Data must be **persisted internally**
- Backfill ingestion must be supported
- Monthly incremental ingestion must be automated
- Pipelines must be **idempotent**
- Schema evolution must be detected
- Data contracts must be enforced in Silver
- Quality metrics must be tracked over time
- No direct live dependency on upstream S3 queries

---

## 8. Data Semantics & Modeling

### Fact Table

**Fact_Trips**

- One row per trip
- Base grain: trip-level
- Surrogate keys referencing dimensions

### Dimension Tables

- Dim_Time
- Dim_Zone
- Dim_Borough
- Dim_Weather
- Dim_Airport
- Dim_Event

### Temporal Modeling & Historical Retention

- Append-only bronze layer
- Partitioning by year/month
- Reprocessing capability per partition
- Metadata tracking (`ingestion_ts`, `source_file`, versioning)

Historical raw data is preserved indefinitely while analytical
queries typically focus on **recent operational time windows**.

### Weather Integration

- Join daily weather observations by trip date
- Optional hourly alignment
- Flag extreme weather events

### Airport Integration (Phase 3)

- Identify airport zones
- Classify airport-related trips
- Join with daily airport passenger volumes
- Compute airport demand index

---


## 9. Assumptions, Limitations & Open Questions

- What is the optimal **analytical horizon** for operational trend monitoring?
- How should **hot vs cold data** be managed across layers?
- How to detect republished monthly datasets?
- What defines severe weather disruption?
- How to formally classify behavioral clusters?
- What data quality thresholds are acceptable?
- Should airport traffic be treated as a dimension or fact?

---

## 10. Scope Boundaries

Out of scope:

- Real-time streaming architectures
- Machine learning pipelines
- Demand forecasting models
- Dynamic pricing engines
- Simulation frameworks

The project focuses exclusively on:

> Building a robust, versioned, production-like  
> data foundation for **operational urban mobility analytics**.