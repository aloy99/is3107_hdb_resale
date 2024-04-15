CREATE TABLE IF NOT EXISTS staging.stg_reservoirs(
    id SERIAL PRIMARY KEY,
    reservoir TEXT UNIQUE,
    latitude FLOAT,
    longitude FLOAT
);