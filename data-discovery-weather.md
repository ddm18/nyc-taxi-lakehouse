# Data Discovery – NOAA Weather Dataset

## 1. Dataset Selection

### 🔎 What to check
- Selected weather stations
- Coverage years
- Variables available (TMAX, TMIN, PRCP, SNOW, etc.)
- API vs bulk dataset

### ❓ Question we are answering
- Which dataset should be used for production?
- Does coverage align with taxi dataset timeline?

### 📝 Notes / Findings



---

## 2. Schema & Variables

### 🔎 What to check
- Variable names
- Measurement units
- Missing value encoding
- Quality flags

### ❓ Question we are answering
- How should weather variables be interpreted?
- Do we need unit normalization?
- Are quality flags required in modeling?

### 📝 Notes / Findings



---

## 3. Temporal Coverage

### 🔎 What to check
- Missing days
- Multi-day gaps
- Complete yearly coverage

### ❓ Question we are answering
- Is weather data continuous?
- Do we need gap filling?
- Can we rely on day-level joins?

### 📝 Notes / Findings



---

## 4. Multi-Station Strategy

### 🔎 What to check
- Number of stations covering NYC
- Overlapping coverage
- Station reliability

### ❓ Question we are answering
- Should we select a primary station?
- Should we compute station average?
- Is Manhattan-specific weather needed?

### 📝 Notes / Findings



---

## 5. Value Sanity Checks

### 🔎 What to check

#### Temperature
- Min / Max plausible range
- Unusual spikes

#### Precipitation
- Extreme daily values

#### Snow
- Winter consistency

### ❓ Question we are answering
- Do we need anomaly flags?
- Are extreme values realistic?
- Should we cap outliers?

### 📝 Notes / Findings



---

## 6. Alignment with Taxi Data

### 🔎 What to check
- Date alignment (timezone?)
- Day-level vs hour-level need
- Time granularity compatibility

### ❓ Question we are answering
- Can we safely join on date?
- Is hourly weather required?
- Do we risk temporal mismatch?

### 📝 Notes / Findings



---

## 7. Missing Data Strategy

### 🔎 What to check
- % missing per variable
- Seasonal missing pattern
- Station reliability differences

### ❓ Question we are answering
- Should missing values block enrichment?
- Should missing weather create a separate flag?
- Do we need imputation?

### 📝 Notes / Findings



---

## 8. Weather Modeling Decisions

### ❓ Strategic Questions

- Use daily or hourly weather?
- Single station or aggregated?
- Create derived features (rain_flag, snow_flag)?
- Define "extreme weather day"?

### 📝 Final Notes
