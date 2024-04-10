CREATE TABLE IF NOT EXISTS staging.stg_mrts(
    id SERIAL,
    mrt TEXT,
    opening_date DATE,
    PRIMARY KEY (mrt)
);