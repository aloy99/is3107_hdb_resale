CREATE TABLE IF NOT EXISTS staging.stg_mrts(
    id SERIAL PRIMARY KEY,
    mrt TEXT UNIQUE,
    opening_date DATE
);