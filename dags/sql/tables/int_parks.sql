CREATE TABLE IF NOT EXISTS warehouse.int_parks(
    id SERIAL PRIMARY KEY,
    park TEXT UNIQUE,
    latitude FLOAT,
    longitude FLOAT
);