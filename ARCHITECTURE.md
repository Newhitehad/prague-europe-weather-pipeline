# Architecture

## Overview
The pipeline follows a warehouse-style layered design:
- `raw` stores original API payloads and ingestion metadata
- `staging` flattens nested JSON into typed relational tables
- `mart` provides latest-snapshot analytical tables for querying and validation
- a lightweight FastAPI dashboard reads only from `mart`

The full local implementation now covers:
1. ingestion
2. raw storage
3. raw database loading
4. staging transforms
5. mart transforms
6. quality checks
7. read-only dashboard rendering
8. fixture-based CI smoke validation

## Data flow
1. Python fetches weather data from Open-Meteo for five configured cities.
2. Each API response is written to `data/raw/` using a timestamped partition
   path.
3. Raw JSON files are loaded into `raw.weather_api_responses`.
4. SQL transformations flatten current conditions into
   `staging.weather_current_conditions`.
5. SQL transformations explode daily forecast arrays into
   `staging.weather_daily_forecast`.
6. Mart SQL builds the latest analytical snapshot into:
   - `mart.city_current_weather`
   - `mart.daily_city_weather_summary`
   - `mart.daily_temperature_rankings`
   - `mart.city_rolling_3day_metrics`
7. Python quality checks validate nulls, duplicates, value ranges, and row
   counts in the mart layer.
8. FastAPI + Jinja renders the mart layer into a read-only dashboard.
9. GitHub Actions runs lint, unit tests, and a fixture-based DB smoke flow.

## Storage design
### Raw files
Raw files are partitioned by source and run timestamp:

```text
data/raw/source=open_meteo/year=YYYY/month=MM/day=DD/run_id=.../city=prague.json
```

This keeps the source payload replayable, inspectable, and easy to reload.

### Raw database table
`raw.weather_api_responses` stores:
- source system
- city
- ingestion timestamp
- full JSON payload
- raw file path

`raw_file_path` is unique, which makes raw loading idempotent.

### Staging tables
`staging.weather_current_conditions`
- one row per raw record
- flattened current weather metrics

`staging.weather_daily_forecast`
- one row per forecast date within each raw record
- exploded from the daily arrays in the API response

### Mart tables
`mart.city_current_weather`
- one latest row per city

`mart.daily_city_weather_summary`
- one latest row per city and forecast date
- derived metrics such as average temperature and daily temperature range

`mart.daily_temperature_rankings`
- daily cross-city rankings based on max temperature

`mart.city_rolling_3day_metrics`
- rolling three-day temperature and precipitation metrics

## Latest-snapshot mart semantics
`raw` and `staging` preserve historical runs. This is useful for traceability and
reruns, but it means their row counts can grow over time.

`mart` is intentionally different:
- it is rebuilt with a full refresh
- it selects the latest available snapshot per city
- it exposes the current analytical view instead of full history

This design keeps the MVP simple and makes mart row counts predictable.

## Presentation layer
The dashboard intentionally reads only from `mart`. This keeps the UI:
- decoupled from raw ingestion details
- stable even if raw and staging grow historically
- aligned with how analytics consumers usually access warehouse data

The HTML layer is server-rendered with Jinja, while JSON endpoints expose the
same mart-backed data for lightweight inspection.

## Idempotency
- ingestion writes timestamped raw files instead of overwriting previous runs
- raw load uses `ON CONFLICT (raw_file_path) DO NOTHING`
- staging load uses `ON CONFLICT` on primary keys to avoid duplicate inserts
- mart load uses `TRUNCATE + INSERT` to rebuild the analytical snapshot safely

## Quality checks
The mart layer is validated with explicit checks for:
- null critical fields
- duplicate city/date keys
- impossible temperature ranges
- negative precipitation
- expected row counts

If any quality check fails, the command exits non-zero.

## CI strategy
CI uses a PostgreSQL job service plus checked-in raw fixtures. It does not call
the live weather API. This keeps pipeline smoke validation deterministic while
still exercising:
- schema creation
- raw loading
- staging transforms
- mart transforms
- quality checks

## Operational notes
- PostgreSQL runs locally in Docker Compose on port `5433`
- Docker has a healthcheck, and `make db-wait` blocks until Postgres is ready
- Python commands run through `.venv`
- SQL files are mounted into the container at `/sql`
