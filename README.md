# Boston Short-Term Rentals – Data Engineering Project

## 1. Project Overview

The goal of this project is to build a consolidated and historical data foundation
for short-term rental pricing and availability in **Boston**, enriched with
weather information, in order to support business analysis on:

- pricing trends
- availability patterns
- seasonal effects
- differences across neighborhoods
- potential impact of weather conditions

The focus of the project is **data engineering**, not real-time systems or machine learning.



## 2. Stakeholders

### Primary Stakeholder
- Operators renting short-term tourist accommodations in Boston

### Stakeholder Characteristics
- Non-technical
- Business-oriented
- Interested in comparisons across time and locations
- Currently lack a structured and historical internal data repository



## 3. Business Requirements

The stakeholders want to:

- Understand **how much it costs to stay in Boston during different periods**
- Analyze **when prices are higher and when availability is higher**
- Compare **different neighborhoods and zones**
- Observe **seasonal patterns**
- Evaluate whether **weather conditions influence prices and availability**
- Perform **comparisons across equivalent periods** (e.g. year-over-year)

### Non-Functional Constraints
- Data does **not** need to be real-time
- Weekly or monthly freshness is sufficient
- Base data granularity must be **daily**
- Data must be **historically retained**, not re-fetched each time
- Data is **not intended for machine learning**
- The system must avoid reliance on ad-hoc Excel files



## 4. Key Analytical Dimensions

The analysis should be enabled across the following dimensions:

### Time
- Day (base granularity)
- Week
- Month
- Season

### Geography
- Neighborhoods / zones within Boston

### Weather
- Temperature
- Precipitation

### Events
- Public events and festivities
- Periods with increased tourism or city activity



## 5. Business Views
### 5.1 Geographical Interpretation

From a business perspective, Boston is informally divided into behavioral zones:

### Premium / Central Areas
- Back Bay
- Beacon Hill
- Downtown
- Seaport

### High-End Residential Areas
- South End
- North End
- West End

### University / Student Areas
- Allston
- Brighton
- Fenway–Kenmore
- Areas near Harvard/MIT (Cambridge, although outside Boston proper)

### Family / Suburban Areas (within city boundaries)
- More residential
- More stable pricing patterns

### Less Premium Areas
- Lower prices
- Lower weekend demand
- Higher sensitivity to external conditions

These groupings reflect **business perception**, not administrative boundaries.

### 5.2 Temporal Interpretation
cosa conta come season

se l’anno è spezzato in periodi rilevanti

se certi mesi sono confrontabili tra loro

Esempi reali:

“alta stagione”

“bassa stagione”

“periodo universitario”

“weekend lungo”

“holiday season”
cosa intende il business con queste cose? ancora da chiedere.


### 5.2 Temporal Interpretation
OTHER VIEWS TO ADD

## 6. Data Sources

### Source Overview

| Domain | Source | Format | Granularity | Ingestion Mode |
|--------|--------|--------|-------------|----------------|
| Listings | Airbnb | CSV.gz | Per entity | Snapshot |
| Daily Pricing | Airbnb | CSV.gz | Listing × Day | Snapshot → Fact |
| Geography | Airbnb | GeoJSON | Polygons | Static |
| Observed Weather | NOAA GHCN | CSV / API | Station × Day × Variable | Historical + Incremental |
| Forecast Weather | NWS API | JSON | Forecast horizon | Scheduled pull ingestion |

### Reference Links
- https://insideairbnb.com/get-the-data/
- https://www.ncdc.noaa.gov/cdo-web/webservices/v2#gettingStarted
- https://www.weather.gov/documentation/services-web-api



## 7. Data Management Principles

- Data must be **persisted internally** and not re-downloaded on each use
- Historical data must be preserved even if source formats change
- Data ingestion will **simulate periodic arrivals** of external datasets
- External sources should be treated as **upstream providers**, not as live dependencies



## 8. Data Semantics & Modeling (TBD)

This section will be completed after direct inspection of source datasets.

### Price & Availability Definitions
- Operational definition of nightly price
- Definition of availability at daily and aggregated levels
- Treatment of unavailable or missing days

### Data Granularity & Grain
- Guaranteed grain of each source dataset
- Target analytical grain(s)

### Temporal Modeling & Historical Retention
- Snapshot vs append-only ingestion
- Handling of changes in listing attributes over time
- Historical retention strategy

### Geographic Modeling
- Assignment of listings to neighborhoods
- Mapping from administrative areas to business-defined zones

### Weather Data Integration
- Selection and aggregation of weather stations
- Temporal alignment with rental data



## 9. Architecture & Data Flow (High-Level, TBD)

This section will outline the system architecture once ingestion and storage patterns
are validated.

- Ingestion approach
- Data storage layers
- Transformation responsibilities
- Update cadence



## 10. Assumptions, Limitations & Open Questions

The following points must be clarified during the discovery phase:

- How much historical data is required (e.g. number of years)?
- Which holidays or city events should always be tracked?
- Is geographic analysis needed only at group/neighborhood level or also by coordinates?
- Should full historical retention be maintained even when data sources change?



## 11. Scope Boundaries

Out of scope for this project:

- Real-time data processing
- Machine learning pipelines
- Automated pricing recommendations
- Streaming architectures

The project focuses exclusively on **building a reliable, reusable, and analyzable data foundation**.
