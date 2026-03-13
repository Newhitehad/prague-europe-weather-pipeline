# Usage Guide

## Recommended local flow
```bash
make venv
source .venv/bin/activate
make install
make install-cli
prague-weather init
prague-weather check
```

After `make install-cli`, `prague-weather` is available through `~/.local/bin` and can be run outside the repository directory.

## Global CLI commands
```bash
prague-weather init
prague-weather check
prague-weather ingest
prague-weather raw
prague-weather stage
prague-weather mart
prague-weather dq
prague-weather ui
prague-weather db up
prague-weather db wait
prague-weather smoke
```

## Repo-local make aliases
```bash
make up
make wait
make init
make check
make ingest
make raw
make stage
make mart
make dq
make ui
```

## What each command does
- `prague-weather init`: full local pipeline refresh from DB startup to quality checks
- `prague-weather check`: lint + unit tests from any terminal
- `prague-weather ui`: starts the FastAPI dashboard at `http://127.0.0.1:8000`
- `prague-weather smoke`: loads fixture raw data into the configured database
  and validates the SQL pipeline without calling the live API
- `make init`: full local pipeline refresh from DB startup to quality checks
- `make check`: lint + unit tests
- `make ui`: starts the FastAPI dashboard at `http://127.0.0.1:8000`
- `make dq`: runs mart-layer data quality checks
- `make smoke`: loads fixture raw data into the configured database and validates
  the SQL pipeline without calling the live API

## Deterministic smoke test
The smoke path is intended for CI parity and repeatable local verification.

```bash
prague-weather db up
prague-weather db wait
prague-weather smoke
```

This path:
1. creates schemas and tables
2. loads checked-in fixture raw data
3. runs staging and mart SQL
4. runs quality checks

## Dashboard run
```bash
prague-weather init
prague-weather ui
```

Then open:

```text
http://127.0.0.1:8000
```

If the default port is busy:

```bash
prague-weather ui --port 8010
```

## Useful SQL checks
```sql
SELECT city, current_time_utc, temperature_c
FROM mart.city_current_weather
ORDER BY city;
```

```sql
SELECT forecast_date, city, max_temperature_rank, temperature_max_c
FROM mart.daily_temperature_rankings
ORDER BY forecast_date, max_temperature_rank, city;
```
