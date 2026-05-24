-- sql/create_table.sql
-- Crea la tabla taxi_trips en PostgreSQL con constraints explícitas.
-- Se ejecuta automáticamente al iniciar el contenedor postgres via init script,
-- o manualmente: psql -U taxi_user -d chicago_taxi -f sql/create_table.sql

CREATE TABLE IF NOT EXISTS taxi_trips (
    trip_id                   TEXT          PRIMARY KEY,
    trip_start_timestamp      TIMESTAMPTZ,
    trip_end_timestamp        TIMESTAMPTZ,
    trip_seconds              INTEGER,
    trip_miles                DOUBLE PRECISION,
    fare                      DOUBLE PRECISION,
    tips                      DOUBLE PRECISION,
    tolls                     DOUBLE PRECISION,
    extras                    DOUBLE PRECISION,
    trip_total                DOUBLE PRECISION,
    payment_type              VARCHAR(50),
    company                   VARCHAR(100),
    pickup_community_area     SMALLINT,
    dropoff_community_area    SMALLINT,
    pickup_centroid_latitude  DOUBLE PRECISION,
    pickup_centroid_longitude DOUBLE PRECISION,
    dropoff_centroid_latitude DOUBLE PRECISION,
    dropoff_centroid_longitude DOUBLE PRECISION,
    trip_year                 SMALLINT,
    trip_month                SMALLINT,
    trip_hour                 SMALLINT,
    trip_day_of_week          SMALLINT,
    anomalia                  BOOLEAN       DEFAULT FALSE,
    loaded_at                 TIMESTAMPTZ   DEFAULT NOW(),

    CONSTRAINT chk_trip_seconds   CHECK (trip_seconds BETWEEN 1 AND 86400),
    CONSTRAINT chk_trip_total     CHECK (trip_total >= 0),
    CONSTRAINT chk_fare           CHECK (fare >= 0),
    CONSTRAINT chk_trip_year      CHECK (trip_year BETWEEN 2000 AND 2100),
    CONSTRAINT chk_trip_month     CHECK (trip_month BETWEEN 1 AND 12),
    CONSTRAINT chk_trip_hour      CHECK (trip_hour BETWEEN 0 AND 23),
    CONSTRAINT chk_day_of_week    CHECK (trip_day_of_week BETWEEN 0 AND 6)
);

-- Índices para consultas analíticas frecuentes
CREATE INDEX IF NOT EXISTS idx_trip_year        ON taxi_trips (trip_year);
CREATE INDEX IF NOT EXISTS idx_trip_hour        ON taxi_trips (trip_hour);
CREATE INDEX IF NOT EXISTS idx_pickup_area      ON taxi_trips (pickup_community_area);
CREATE INDEX IF NOT EXISTS idx_company          ON taxi_trips (company);
