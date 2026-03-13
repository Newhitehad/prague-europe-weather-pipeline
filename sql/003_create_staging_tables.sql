CREATE TABLE IF NOT EXISTS staging.weather_current_conditions (
    raw_record_id BIGINT PRIMARY KEY REFERENCES raw.weather_api_responses(raw_record_id),
    city TEXT NOT NULL,
    current_time_utc TIMESTAMPTZ NOT NULL,
    temperature_c NUMERIC(6,2),
    relative_humidity_pct NUMERIC(6,2),
    wind_speed_kmh NUMERIC(6,2),
    source_ingested_at_utc TIMESTAMPTZ NOT NULL,
    loaded_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS staging.weather_daily_forecast (
    raw_record_id BIGINT NOT NULL REFERENCES raw.weather_api_responses(raw_record_id),
    city TEXT NOT NULL,
    forecast_date DATE NOT NULL,
    temperature_max_c NUMERIC(6,2),
    temperature_min_c NUMERIC(6,2),
    precipitation_mm NUMERIC(8,2),
    source_ingested_at_utc TIMESTAMPTZ NOT NULL,
    loaded_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (raw_record_id, forecast_date)
);

CREATE INDEX IF NOT EXISTS idx_staging_daily_city_date
ON staging.weather_daily_forecast (city, forecast_date);
