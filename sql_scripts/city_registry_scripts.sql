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
    city_id      NUMBER(10, 0) NOT NULL,
    started_at   INTEGER NOT NULL,
    finished_at  NUMBER(10, 0) NOT NULL,
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
