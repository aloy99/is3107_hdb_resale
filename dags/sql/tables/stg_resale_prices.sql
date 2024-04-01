CREATE TABLE IF NOT EXISTS staging.stg_resale_prices(
    id SERIAL,
    transaction_month TEXT,
    town TEXT,
    flat_type TEXT,
    block TEXT,
    street_name TEXT,
    storey_range TEXT,
    floor_area_sqm FLOAT,
    flat_model TEXT,
    lease_commence_date TEXT,
    remaining_lease TEXT,
    resale_price FLOAT
);