CREATE TABLE IF NOT EXISTS warehouse.int_resale_prices(
    id TEXT NOT NULL,
    transaction_month TEXT NOT NULL,
    town TEXT NOT NULL,
    flat_type TEXT NOT NULL,
    block_num TEXT NOT NULL,
    street_name TEXT NOT NULL,
    storey_range TEXT NOT NULL,
    floor_area_sqm FLOAT NOT NULL,
    flat_model TEXT NOT NULL,
    lease_commence_date TEXT NOT NULL,
    remaining_lease TEXT NOT NULL,
    resale_price INT NOT NULL,
    longitude FLOAT,
    latitude FLOAT
)