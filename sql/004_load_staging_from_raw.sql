INSERT INTO staging.weather_current_conditions (
    raw_record_id,
    city,
    current_time_utc,
    temperature_c,
    relative_humidity_pct,
    wind_speed_kmh,
    source_ingested_at_utc
)
SELECT
    r.raw_record_id,
    r.city,
    ((r.payload #>> '{api_response,current,time}')::timestamp AT TIME ZONE 'UTC') AS current_time_utc,
    (r.payload #>> '{api_response,current,temperature_2m}')::numeric(6,2) AS temperature_c,
    (r.payload #>> '{api_response,current,relative_humidity_2m}')::numeric(6,2) AS relative_humidity_pct,
    (r.payload #>> '{api_response,current,wind_speed_10m}')::numeric(6,2) AS wind_speed_kmh,
    r.ingested_at_utc
FROM raw.weather_api_responses r
WHERE (r.payload #>> '{api_response,current,time}') IS NOT NULL
ON CONFLICT (raw_record_id) DO NOTHING;


WITH exploded AS (
    SELECT
        r.raw_record_id,
        r.city,
        r.ingested_at_utc AS source_ingested_at_utc,
        d_time.value AS forecast_date_txt,
        d_max.value AS temperature_max_txt,
        d_min.value AS temperature_min_txt,
        d_prcp.value AS precipitation_txt
    FROM raw.weather_api_responses r
    JOIN LATERAL jsonb_array_elements_text(r.payload #> '{api_response,daily,time}')
        WITH ORDINALITY AS d_time(value, ordinality) ON TRUE
    JOIN LATERAL jsonb_array_elements_text(r.payload #> '{api_response,daily,temperature_2m_max}')
        WITH ORDINALITY AS d_max(value, ordinality) ON d_max.ordinality = d_time.ordinality
    JOIN LATERAL jsonb_array_elements_text(r.payload #> '{api_response,daily,temperature_2m_min}')
        WITH ORDINALITY AS d_min(value, ordinality) ON d_min.ordinality = d_time.ordinality
    JOIN LATERAL jsonb_array_elements_text(r.payload #> '{api_response,daily,precipitation_sum}')
        WITH ORDINALITY AS d_prcp(value, ordinality) ON d_prcp.ordinality = d_time.ordinality
)
INSERT INTO staging.weather_daily_forecast (
    raw_record_id,
    city,
    forecast_date,
    temperature_max_c,
    temperature_min_c,
    precipitation_mm,
    source_ingested_at_utc
)
SELECT
    raw_record_id,
    city,
    forecast_date_txt::date,
    temperature_max_txt::numeric(6,2),
    temperature_min_txt::numeric(6,2),
    precipitation_txt::numeric(8,2),
    source_ingested_at_utc
FROM exploded
ON CONFLICT (raw_record_id, forecast_date) DO NOTHING;
