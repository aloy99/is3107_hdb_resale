CREATE TABLE IF NOT EXISTS staging.stg_mrts(
    id SERIAL,
    mrt TEXT,
    opening_date DATE,
    latitude FLOAT,
    longitude FLOAT,
    PRIMARY KEY (mrt)
);