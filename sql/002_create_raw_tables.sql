CREATE TABLE IF NOT EXISTS raw.weather_api_responses (
    raw_record_id BIGSERIAL PRIMARY KEY,
    source TEXT NOT NULL,
    city TEXT NOT NULL,
    ingested_at_utc TIMESTAMPTZ NOT NULL,
    payload JSONB NOT NULL,
    raw_file_path TEXT NOT NULL,
    loaded_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_raw_weather_file UNIQUE (raw_file_path)
);

CREATE INDEX IF NOT EXISTS idx_raw_weather_city_ingested_at
ON raw.weather_api_responses (city, ingested_at_utc);
