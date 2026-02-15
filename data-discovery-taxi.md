# Data Discovery – NYC Taxi Dataset

## 1. Dataset Overview

### 🔎 What to check
- Months selected for analysis
- Years selected for comparison
- File format (Parquet / CSV)
- File size per month

### ❓ Question we are answering
- What is the scale of the dataset?
- How large is a typical monthly partition?
- Is storage/partitioning strategy necessary?

### 📝 Notes / Findings



---

## 2. Schema Validation

### 🔎 What to check
- List of columns per selected month
- Data types per column
- Differences between years
- Added/removed/renamed columns

### ❓ Question we are answering
- Is the schema stable across time?
- Do we need schema versioning?
- Are there breaking changes across years?

### 📝 Notes / Findings



---

## 3. Grain Confirmation

### 🔎 What to check
- Identify primary grain (trip-level?)
- Duplicate row detection
- Unique keys (if any)
- Timestamp structure

### ❓ Question we are answering
- What is the true grain of the fact table?
- Do we need a surrogate trip identifier?
- Is the dataset append-only?

### 📝 Notes / Findings



---

## 4. Volume Profiling

### 🔎 What to check
- Total records per month
- Average records per day
- Hourly trip distribution
- Seasonality patterns

### ❓ Question we are answering
- What is the ingestion scale?
- Do we need partitioning by month/year?
- Are there demand peaks affecting modeling?

### 📝 Notes / Findings



---

## 5. Null & Missing Value Analysis

### 🔎 What to check
- % null per column
- Patterns of null (correlated with zones or dates?)
- Columns with unexpected missing values

### ❓ Question we are answering
- Are critical fields reliable (fare, location, distance)?
- Do we need data quality rules?
- Should nulls block silver ingestion?

### 📝 Notes / Findings



---

## 6. Range & Distribution Analysis

### 🔎 What to check

#### Fare
- Minimum
- Maximum
- Percentiles (P50, P90, P99)
- % negative values

#### Trip Distance
- % zero
- % extreme values
- Maximum distance

#### Trip Duration
- Negative duration?
- Very long trips (> 24h?)

### ❓ Question we are answering
- Are there outliers requiring filtering?
- Do we need anomaly detection rules?
- What constitutes invalid data?

### 📝 Notes / Findings



---

## 7. Referential Integrity (Zone Mapping)

### 🔎 What to check
- Distinct pickup_location_id
- Distinct dropoff_location_id
- Missing zone mappings
- Invalid IDs

### ❓ Question we are answering
- Can every trip be mapped to a zone?
- Do we need an "Unknown zone" bucket?
- Is the dimension table complete?

### 📝 Notes / Findings



---

## 8. Airport Zone Identification

### 🔎 What to check
- Identify airport zone IDs
- % trips touching airport zones
- Revenue share airport-related

### ❓ Question we are answering
- How significant are airport trips?
- Should airport trips be flagged in silver layer?
- Is separate modeling required?

### 📝 Notes / Findings



---

## 9. Temporal Behavior

### 🔎 What to check
- Weekday vs weekend distribution
- Peak hour detection
- Seasonal comparison (winter vs summer)

### ❓ Question we are answering
- Do we need specific business temporal dimensions?
- Should we define “rush hour” as derived attribute?

### 📝 Notes / Findings



---

## 10. Discovery Conclusions

### ❓ Strategic Questions

- Is monthly partitioning sufficient?
- Is schema stable enough for contract definition?
- What are mandatory data quality checks?
- Do we need deduplication logic?
- What is the estimated storage footprint (5 years)?

### 📝 Final Notes
