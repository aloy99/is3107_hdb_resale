CREATE TABLE IF NOT EXISTS warehouse.int_resale_prices(
    id SERIAL PRIMARY KEY,
    transaction_month DATE,
    town TEXT,
    flat_type TEXT,
    block TEXT,
    street_name TEXT,
    storey_range TEXT,
    floor_area_sqm FLOAT,
    flat_model TEXT,
    lease_commence_date TEXT,
    remaining_lease TEXT,
    resale_price FLOAT,
    real_resale_price FLOAT,
    postal TEXT,
    longitude FLOAT,
    latitude FLOAT,
    distance_from_cbd FLOAT
)