DROP TABLE IF EXISTS WEATHERANALYTICS.WEATHER_ANALYTICS.city_registry;
CREATE TABLE IF NOT EXISTS WEATHERANALYTICS.WEATHER_ANALYTICS.city_registry (
    city_name    STRING NOT NULL,
    country_code STRING NOT NULL,
    state_code   STRING,
    start_date   TIMESTAMP_NTZ NOT NULL,
    end_date     TIMESTAMP_NTZ NOT NULL,
    lat          NUMBER(9, 6),
    lon          NUMBER(9, 6),
    PRIMARY KEY (city_name, country_code, start_date)
);

-- INSERT INTO WEATHERANALYTICS.WEATHER_ANALYTICS.city_registry (
--     city_name,
--     country_code,
--     start_date,
--     end_date
-- ) VALUES (
--     'London',
--     'GB',
--     TO_TIMESTAMP_NTZ(DATEADD(year, -1, CURRENT_DATE())),
--     TO_TIMESTAMP_NTZ(CURRENT_DATE())
-- );

DROP TABLE IF EXISTS WEATHERANALYTICS.WEATHER_ANALYTICS.ingestion_state;
CREATE TABLE WEATHERANALYTICS.WEATHER_ANALYTICS.ingestion_state (
    city_id      STRING NOT NULL,
    started_at   TIMESTAMP_NTZ NOT NULL,
    finished_at  TIMESTAMP_NTZ NOT NULL,
    job_status   STRING NOT NULL DEFAULT 'pending',
    PRIMARY KEY (city_id)
);

DROP TABLE IF EXISTS WEATHERANALYTICS.WEATHER_ANALYTICS.raw_history_json;
CREATE TABLE WEATHERANALYTICS.WEATHER_ANALYTICS.raw_history_json (
    payload   VARIANT,
    file_name STRING NOT NULL,
    loaded_at TIMESTAMP_NTZ,
    PRIMARY KEY (file_name)
);

-- Test script: flatten raw JSON weather payloads into tabular form.
-- This can be executed in Snowflake to inspect the structure of ingested files.
-- Replace the WHERE clause as needed to target specific files or time windows.
/*
WITH base AS (
    SELECT
        file_name,
        TO_TIMESTAMP_NTZ(loaded_at) AS loaded_at,
        payload
    FROM WEATHERANALYTICS.WEATHER_ANALYTICS.raw_history_json
    WHERE file_name LIKE 'London_GB_%'
    ORDER BY loaded_at DESC
    LIMIT 1
),
records AS (
    SELECT
        file_name,
        loaded_at,
        value AS record
    FROM base,
         LATERAL FLATTEN(input => payload)
),
metrics AS (
    SELECT
        r.file_name,
        r.loaded_at,
        r.record:"_airbyte_raw_id"::STRING                            AS airbyte_raw_id,
        TO_TIMESTAMP_NTZ(r.record:"_airbyte_extracted_at"::NUMBER/1000) AS extracted_at,
        r.record:"_airbyte_data":"dt"::TIMESTAMP_NTZ                  AS observation_time,
        r.record:"_airbyte_data":"start_date"::TIMESTAMP_NTZ          AS batch_start,
        r.record:"_airbyte_data":"end_date"::TIMESTAMP_NTZ            AS batch_end,
        r.record:"_airbyte_data":"lat"::FLOAT                         AS latitude,
        r.record:"_airbyte_data":"lon"::FLOAT                         AS longitude,

        r.record:"_airbyte_data":"main":"temp"::FLOAT                 AS temperature,
        r.record:"_airbyte_data":"main":"feels_like"::FLOAT           AS feels_like,
        r.record:"_airbyte_data":"main":"temp_min"::FLOAT             AS temperature_min,
        r.record:"_airbyte_data":"main":"temp_max"::FLOAT             AS temperature_max,
        r.record:"_airbyte_data":"main":"pressure"::FLOAT             AS pressure,
        r.record:"_airbyte_data":"main":"humidity"::FLOAT             AS humidity,
        r.record:"_airbyte_data":"main":"sea_level"::FLOAT            AS sea_level_pressure,
        r.record:"_airbyte_data":"main":"grnd_level"::FLOAT           AS ground_level_pressure,

        r.record:"_airbyte_data":"wind":"speed"::FLOAT                AS wind_speed,
        r.record:"_airbyte_data":"wind":"deg"::FLOAT                  AS wind_direction,
        r.record:"_airbyte_data":"wind":"gust"::FLOAT                 AS wind_gust,

        r.record:"_airbyte_data":"clouds":"all"::FLOAT                AS cloud_cover,
        r.record:"_airbyte_data":"visibility"::FLOAT                  AS visibility,

        r.record:"_airbyte_data":"rain":"1h"::FLOAT                   AS rain_1h_mm,
        r.record:"_airbyte_data":"rain":"3h"::FLOAT                   AS rain_3h_mm,
        r.record:"_airbyte_data":"snow":"1h"::FLOAT                   AS snow_1h_mm,
        r.record:"_airbyte_data":"snow":"3h"::FLOAT                   AS snow_3h_mm,

        weather.value:"id"::INT                                       AS weather_id,
        weather.value:"main"::STRING                                  AS weather_main,
        weather.value:"description"::STRING                           AS weather_description,
        weather.value:"icon"::STRING                                  AS weather_icon
    FROM records AS r
    ,    LATERAL FLATTEN(input => r.record:"_airbyte_data":"weather", outer => TRUE) AS weather
)
SELECT *
FROM metrics
ORDER BY observation_time, weather_id;
*/
