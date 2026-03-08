# Data Discovery - NOAA Weather Dataset

!!! abstract "Goal"
    Validate weather data suitability for enrichment of taxi analytics at the required temporal granularity.

## Discovery Checklist

- [ ] Dataset/source final selection
- [ ] Variable dictionary and units validation
- [ ] Temporal coverage validation
- [ ] Station strategy decision
- [ ] Missing/anomaly strategy
- [ ] Join strategy with taxi data

## 1. Dataset Selection

???+ question "What to check"
    - Selected stations
    - Coverage years
    - Available variables (`TMAX`, `TMIN`, `PRCP`, `SNOW`, ...)
    - API vs bulk source

???+ info "Question"
    - Which dataset should be production source?
    - Does coverage align with taxi timeline?

!!! note "Findings"
    Pending completion.

## 2. Schema & Variables

???+ question "What to check"
    - Variable names
    - Measurement units
    - Missing-value encoding
    - Quality flags

???+ info "Question"
    - How should variables be interpreted?
    - Is unit normalization required?
    - Are quality flags needed in modeling?

!!! note "Findings"
    Pending completion.

## 3. Temporal Coverage

???+ question "What to check"
    - Missing days
    - Multi-day gaps
    - Year-level completeness

???+ info "Question"
    - Is weather data continuous enough?
    - Is gap filling required?
    - Can we rely on day-level joins?

!!! note "Findings"
    Pending completion.

## 4. Multi-Station Strategy

???+ question "What to check"
    - Number of NYC-relevant stations
    - Coverage overlap
    - Station reliability

???+ info "Question"
    - Primary station vs aggregate?
    - Is Manhattan-specific weather needed?

!!! note "Findings"
    Pending completion.

## 5. Value Sanity Checks

???+ question "What to check"
    **Temperature**
    - Plausible min/max
    - Unusual spikes

    **Precipitation**
    - Extreme daily values

    **Snow**
    - Winter consistency

???+ info "Question"
    - Are extreme values realistic?
    - Do we need anomaly flags or caps?

!!! note "Findings"
    Pending completion.

## 6. Alignment with Taxi Data

???+ question "What to check"
    - Date alignment and timezone assumptions
    - Day-level vs hour-level requirement
    - Granularity compatibility

???+ info "Question"
    - Is date-level join safe?
    - Is hourly weather required?
    - Is temporal mismatch a risk?

!!! note "Findings"
    Pending completion.

## 7. Missing Data Strategy

???+ question "What to check"
    - `%` missing per variable
    - Seasonal missing patterns
    - Missing patterns by station

???+ info "Question"
    - Should missing data block enrichment?
    - Should missing weather set explicit flags?
    - Is imputation required?

!!! note "Findings"
    Pending completion.

## 8. Weather Modeling Decisions

???+ question "Strategic questions"
    - Daily or hourly weather?
    - Single station or multi-station aggregate?
    - Derived features (`rain_flag`, `snow_flag`)?
    - Definition of `extreme_weather_day`?

!!! note "Final notes"
    Pending completion.
