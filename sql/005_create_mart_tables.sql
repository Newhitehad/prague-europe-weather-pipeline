CREATE TABLE IF NOT EXISTS mart.city_current_weather (
    city TEXT PRIMARY KEY,
    current_time_utc TIMESTAMPTZ NOT NULL,
    temperature_c NUMERIC(6,2),
    relative_humidity_pct NUMERIC(6,2),
    wind_speed_kmh NUMERIC(6,2),
    source_ingested_at_utc TIMESTAMPTZ NOT NULL,
    refreshed_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS mart.daily_city_weather_summary (
    city TEXT NOT NULL,
    forecast_date DATE NOT NULL,
    temperature_max_c NUMERIC(6,2),
    temperature_min_c NUMERIC(6,2),
    temperature_avg_c NUMERIC(6,2),
    diurnal_temp_range_c NUMERIC(6,2),
    precipitation_mm NUMERIC(8,2),
    is_wet_day BOOLEAN NOT NULL,
    source_ingested_at_utc TIMESTAMPTZ NOT NULL,
    refreshed_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (city, forecast_date)
);

CREATE TABLE IF NOT EXISTS mart.daily_temperature_rankings (
    forecast_date DATE NOT NULL,
    city TEXT NOT NULL,
    temperature_max_c NUMERIC(6,2),
    temperature_avg_c NUMERIC(6,2),
    precipitation_mm NUMERIC(8,2),
    max_temperature_rank BIGINT NOT NULL,
    refreshed_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (forecast_date, city)
);

CREATE TABLE IF NOT EXISTS mart.city_rolling_3day_metrics (
    city TEXT NOT NULL,
    forecast_date DATE NOT NULL,
    temperature_avg_c NUMERIC(6,2),
    rolling_3day_avg_temp_c NUMERIC(6,2),
    precipitation_mm NUMERIC(8,2),
    rolling_3day_precip_mm NUMERIC(8,2),
    refreshed_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (city, forecast_date)
);

CREATE INDEX IF NOT EXISTS idx_mart_daily_summary_date
ON mart.daily_city_weather_summary (forecast_date, city);

CREATE INDEX IF NOT EXISTS idx_mart_temperature_rankings_date
ON mart.daily_temperature_rankings (forecast_date, max_temperature_rank);

CREATE INDEX IF NOT EXISTS idx_mart_rolling_city_date
ON mart.city_rolling_3day_metrics (city, forecast_date);
