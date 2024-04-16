CREATE TABLE IF NOT EXISTS warehouse.int_supermarkets(
    id SERIAL PRIMARY KEY,
    business_name TEXT,
    premise_address TEXT UNIQUE,
    longitude FLOAT,
    latitude FLOAT
);
CREATE INDEX IF NOT EXISTS idx_supermarkets ON warehouse.int_supermarkets(premise_address);