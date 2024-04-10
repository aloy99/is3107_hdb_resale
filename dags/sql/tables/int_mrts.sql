CREATE TABLE IF NOT EXISTS warehouse.int_mrts(
    id SERIAL PRIMARY KEY,
    mrt TEXT UNIQUE NOT NULL,
    opening_date DATE,
    latitude FLOAT,
    longitude FLOAT
);