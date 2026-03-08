# Data Discovery - NYC Taxi Dataset

!!! abstract "Goal"
    Validate data shape, quality, and modeling implications before defining stable contracts and production pipelines.

## Discovery Checklist

- [x] Initial schema scan (yellow/green)
- [x] Type drift identification across years
- [ ] Grain validation and duplicate strategy
- [ ] Volume profiling by month/day/hour
- [ ] Null and outlier profiling
- [ ] Referential integrity with taxi zones
- [ ] Final quality gate proposal

## 1. Dataset Overview

???+ question "What to check"
    - Months selected for analysis
    - Years selected for comparison
    - File format (`Parquet` / `CSV`)
    - File size per month

???+ info "Question"
    - What is the scale of the dataset?
    - How large is a typical monthly partition?
    - Is a storage/partition strategy required?

!!! note "Findings"
    Pending completion.

## 2. Schema Validation

???+ question "What to check"
    - Column list per selected month
    - Data type per column
    - Year-over-year differences
    - Added/removed/renamed columns

???+ info "Question"
    - Is schema stable across time?
    - Is schema versioning required?
    - Are there breaking changes across years?

### Yellow Taxi (reference snapshot)

Columns observed from 2013 onward:

```text
VendorID                          int64
tpep_pickup_datetime     datetime64[us]
tpep_dropoff_datetime    datetime64[us]
passenger_count                   int64
trip_distance                   float64
RatecodeID                        int64
store_and_fwd_flag                  str
PULocationID                      int64
DOLocationID                      int64
payment_type                      int64
fare_amount                     float64
extra                           float64
mta_tax                         float64
tip_amount                      float64
tolls_amount                    float64
improvement_surcharge           float64
total_amount                    float64
congestion_surcharge             integer
airport_fee                      integer
```

Type changes observed:

| Column | From | To |
|---|---|---|
| `RatecodeID` | `int64` | `float64` |
| `airport_fee` | `integer` | `float64` |
| `congestion_surcharge` | `integer` | `float64` |
| `passenger_count` | `int64` | `float64` |

### Green Taxi (reference snapshot)

Columns observed from 2014 onward:

```text
VendorID                          int64
lpep_pickup_datetime     datetime64[us]
lpep_dropoff_datetime    datetime64[us]
store_and_fwd_flag                  str
RatecodeID                        int64
PULocationID                      int64
DOLocationID                      int64
passenger_count                   int64
trip_distance                   float64
fare_amount                     float64
extra                           float64
mta_tax                         float64
tip_amount                      float64
tolls_amount                    float64
ehail_fee                        integer
improvement_surcharge           float64
total_amount                    float64
payment_type                      int64
trip_type                       float64
congestion_surcharge             integer
```

Type changes observed:

| Column | From | To |
|---|---|---|
| `RatecodeID` | `int64` | `float64` |
| `congestion_surcharge` | `integer` | `float64` |
| `ehail_fee` | `integer` | `float64` |
| `passenger_count` | `int64` | `float64` |
| `payment_type` | `int64` | `float64` |
| `trip_type` | `float64` | `int64` |

!!! warning "Exploratory conclusion"
    Column sets are mostly stable, but key types are not stable and oscillate (`int`/`float`/`integer`) across years.

!!! success "Implication"
    Define schema versioning with logical schema + explicit mapping/casting rules to prevent pipeline breaks.

!!! tip "Open question"
    Verify whether type drift is driven by null/mixed values or by real upstream semantic changes via targeted sampling.

## 3. Grain Confirmation

???+ question "What to check"
    - Primary grain (trip-level?)
    - Duplicate row detection
    - Unique key candidates
    - Timestamp consistency

???+ info "Question"
    - What is the true fact grain?
    - Is a surrogate trip identifier needed?
    - Is the dataset append-only?

!!! note "Findings"
    Pending completion.

## 4. Volume Profiling

???+ question "What to check"
    - Total records by month
    - Average records/day
    - Hourly trip distribution
    - Seasonality patterns

???+ info "Question"
    - What is the ingestion scale?
    - Is `year/month` partitioning sufficient?
    - Are there demand peaks affecting modeling?

!!! note "Findings"
    Pending completion.

## 5. Null & Missing Value Analysis

???+ question "What to check"
    - `%` null per column
    - Null patterns by time/zone
    - Unexpected missing values

???+ info "Question"
    - Are critical fields reliable (fare, location, distance)?
    - Which DQ rules are mandatory?
    - Should nulls block Silver ingestion?

!!! note "Findings"
    Pending completion.

## 6. Range & Distribution Analysis

???+ question "What to check"
    **Fare**
    - Minimum / Maximum
    - Percentiles (`P50`, `P90`, `P99`)
    - `%` negative values

    **Trip Distance**
    - `%` zero values
    - `%` extreme values
    - Maximum distance

    **Trip Duration**
    - Negative duration
    - Very long trips (`> 24h`)

???+ info "Question"
    - Which outliers require filtering?
    - Are anomaly flags needed?
    - What should be considered invalid data?

!!! note "Findings"
    Pending completion.

## 7. Referential Integrity (Zone Mapping)

???+ question "What to check"
    - Distinct `pickup_location_id`
    - Distinct `dropoff_location_id`
    - Missing zone mappings
    - Invalid IDs

???+ info "Question"
    - Can every trip be mapped to a zone?
    - Is an `Unknown zone` bucket required?
    - Is the zone dimension complete?

!!! note "Findings"
    Pending completion.

## 8. Airport Zone Identification

???+ question "What to check"
    - Airport zone IDs
    - `%` of trips touching airport zones
    - Airport-related revenue share

???+ info "Question"
    - How significant are airport trips?
    - Should airport trips be flagged in Silver?
    - Is separate modeling needed?

!!! note "Findings"
    Pending completion.

## 9. Temporal Behavior

???+ question "What to check"
    - Weekday vs weekend distribution
    - Peak-hour windows
    - Winter vs summer comparison

???+ info "Question"
    - Are dedicated temporal business dimensions needed?
    - Should `rush_hour` be a derived attribute?

!!! note "Findings"
    Pending completion.

## 10. Discovery Conclusions

???+ question "Strategic questions"
    - Is monthly partitioning enough?
    - Is schema stable enough for a contract?
    - Which DQ checks are mandatory?
    - Is deduplication logic required?
    - Estimated 5-year storage footprint?

!!! note "Final notes"
    Pending completion.
