CREATE TABLE IF NOT EXISTS staging.stg_parks(
    id SERIAL PRIMARY KEY,
    park TEXT UNIQUE,
    latitude FLOAT,
    longitude FLOAT
);