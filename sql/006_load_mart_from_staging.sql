TRUNCATE TABLE
    mart.city_current_weather,
    mart.daily_city_weather_summary,
    mart.daily_temperature_rankings,
    mart.city_rolling_3day_metrics;

WITH ranked_current AS (
    SELECT
        city,
        current_time_utc,
        temperature_c,
        relative_humidity_pct,
        wind_speed_kmh,
        source_ingested_at_utc,
        ROW_NUMBER() OVER (
            PARTITION BY city
            ORDER BY source_ingested_at_utc DESC, raw_record_id DESC
        ) AS row_num
    FROM staging.weather_current_conditions
)
INSERT INTO mart.city_current_weather (
    city,
    current_time_utc,
    temperature_c,
    relative_humidity_pct,
    wind_speed_kmh,
    source_ingested_at_utc
)
SELECT
    city,
    current_time_utc,
    temperature_c,
    relative_humidity_pct,
    wind_speed_kmh,
    source_ingested_at_utc
FROM ranked_current
WHERE row_num = 1;

WITH latest_daily_snapshot AS (
    SELECT
        city,
        MAX(source_ingested_at_utc) AS latest_source_ingested_at_utc
    FROM staging.weather_daily_forecast
    GROUP BY city
),
ranked_daily AS (
    SELECT
        d.city,
        d.forecast_date,
        d.temperature_max_c,
        d.temperature_min_c,
        d.precipitation_mm,
        d.source_ingested_at_utc,
        ROW_NUMBER() OVER (
            PARTITION BY d.city, d.forecast_date
            ORDER BY d.source_ingested_at_utc DESC, d.raw_record_id DESC
        ) AS row_num
    FROM staging.weather_daily_forecast AS d
    INNER JOIN latest_daily_snapshot AS latest
        ON latest.city = d.city
       AND latest.latest_source_ingested_at_utc = d.source_ingested_at_utc
)
INSERT INTO mart.daily_city_weather_summary (
    city,
    forecast_date,
    temperature_max_c,
    temperature_min_c,
    temperature_avg_c,
    diurnal_temp_range_c,
    precipitation_mm,
    is_wet_day,
    source_ingested_at_utc
)
SELECT
    city,
    forecast_date,
    temperature_max_c,
    temperature_min_c,
    ROUND(((temperature_max_c + temperature_min_c) / 2.0)::numeric, 2) AS temperature_avg_c,
    ROUND((temperature_max_c - temperature_min_c)::numeric, 2) AS diurnal_temp_range_c,
    precipitation_mm,
    precipitation_mm > 0 AS is_wet_day,
    source_ingested_at_utc
FROM ranked_daily
WHERE row_num = 1;

INSERT INTO mart.daily_temperature_rankings (
    forecast_date,
    city,
    temperature_max_c,
    temperature_avg_c,
    precipitation_mm,
    max_temperature_rank
)
SELECT
    forecast_date,
    city,
    temperature_max_c,
    temperature_avg_c,
    precipitation_mm,
    RANK() OVER (
        PARTITION BY forecast_date
        ORDER BY temperature_max_c DESC, city ASC
    ) AS max_temperature_rank
FROM mart.daily_city_weather_summary;

INSERT INTO mart.city_rolling_3day_metrics (
    city,
    forecast_date,
    temperature_avg_c,
    rolling_3day_avg_temp_c,
    precipitation_mm,
    rolling_3day_precip_mm
)
SELECT
    city,
    forecast_date,
    temperature_avg_c,
    ROUND(
        AVG(temperature_avg_c) OVER (
            PARTITION BY city
            ORDER BY forecast_date
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        )::numeric,
        2
    ) AS rolling_3day_avg_temp_c,
    precipitation_mm,
    ROUND(
        SUM(precipitation_mm) OVER (
            PARTITION BY city
            ORDER BY forecast_date
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        )::numeric,
        2
    ) AS rolling_3day_precip_mm
FROM mart.daily_city_weather_summary;
