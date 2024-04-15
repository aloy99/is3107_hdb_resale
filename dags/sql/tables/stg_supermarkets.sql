CREATE TABLE IF NOT EXISTS staging.stg_supermarkets(
    id SERIAL PRIMARY KEY,
    business_name TEXT,
    premise_address TEXT UNIQUE
);
CREATE INDEX IF NOT EXISTS idx_premise_address ON staging.stg_supermarkets(premise_address);