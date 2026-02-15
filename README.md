# NYC Urban Mobility & Fare Dynamics – Data Engineering Project

## 1. Project Overview

The goal of this project is to build a consolidated and historical data foundation
for **New York City taxi mobility demand and fare dynamics**, enriched with
weather information (and later airport traffic data), in order to support
urban mobility analysis.

The platform will enable analysis of:

- Fare trends over time  
- Trip demand patterns  
- Peak vs off-peak behavior  
- Differences across city zones  
- Weather impact on mobility and pricing  
- Airport-related mobility dynamics  

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
- Interested in trend analysis and comparisons
- Require structured and historical data
- Need reproducibility and reliability
- Focused on business interpretation rather than raw data


---

## 3. Business Requirements

Stakeholders want to:

- Understand how taxi fares evolve over time  
- Analyze when demand is higher (hourly, daily, seasonally)  
- Compare mobility behavior across city zones  
- Identify peak congestion periods  
- Evaluate whether weather conditions influence trip volume and pricing  
- Analyze airport-related mobility patterns  
- Perform year-over-year comparisons  
- Compare weekday vs weekend demand  

### Non-Functional Constraints

- Data does **not need to be real-time**
- Monthly freshness is sufficient
- Base granularity must be **trip-level**
- Historical data must be fully retained
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
- Year-over-Year comparisons

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

### Weather

- Temperature
- Precipitation
- Snow indicators
- Severe weather flags

### Airport Traffic (Phase 2)

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


### 5.2 Temporal Interpretation

Business-relevant time classifications may include:

- Rush hours
- Late-night mobility
- Weekend demand shifts
- Holiday peaks
- Severe weather days
- Summer vs winter patterns
- Airport high-demand season

Definitions such as “peak season” or “disruption period”
must be formalized in the semantic layer.


---

## 6. Data Sources

### Source Overview

| Domain | Source | Format | Granularity | Ingestion Mode |
|--------|--------|--------|-------------|----------------|
| Taxi Trips | NYC TLC | Parquet | Trip-level | Monthly append |
| Taxi Zones | NYC TLC | GeoJSON / CSV | Static | Static |
| Weather | NOAA GHCN | CSV / API | Station × Day | Historical + Incremental |
| Airport Traffic (Phase 2) | TSA / Aviation data | CSV | Airport × Day | Monthly |
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
- Pipeline must be idempotent
- Schema evolution must be detected
- Data contracts must be enforced at each layer
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

### Weather Integration

- Join daily weather observations by trip date
- Optional hourly alignment
- Flag extreme weather events

### Airport Integration (Phase 2)

- Identify airport zones
- Classify airport-related trips
- Join with daily airport passenger volumes
- Compute airport demand index


---

## 9. Architecture & Data Flow

The system follows a layered architecture:

### Bronze Layer

- Raw monthly parquet ingestion
- Metadata augmentation
- Source file preservation

### Silver Layer

- Type enforcement
- Deduplication
- Data quality validation
- Zone mapping
- Weather enrichment

### Gold Layer

- Aggregated business-ready views
- Zone-level daily revenue
- Hourly demand analysis
- Airport corridor analytics
- Weather impact comparisons

Orchestration must support:

- Historical backfill
- Monthly incremental ingestion
- Selective partition reprocessing


---

## 10. Assumptions, Limitations & Open Questions

- How many years of history should be retained?
- How to detect republished monthly datasets?
- What defines severe weather disruption?
- How to formally classify behavioral clusters?
- What data quality thresholds are acceptable?
- Should airport traffic be treated as a dimension or fact?


---

## 11. Scope Boundaries

Out of scope:

- Real-time streaming architectures
- Machine learning pipelines
- Demand forecasting models
- Dynamic pricing engines
- Simulation frameworks

The project focuses exclusively on:

> Building a robust, versioned, production-like
> data foundation for urban mobility analytics.
