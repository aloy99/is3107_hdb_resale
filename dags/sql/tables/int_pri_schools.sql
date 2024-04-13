CREATE TABLE IF NOT EXISTS warehouse.int_pri_schools(
    id SERIAL PRIMARY KEY,
    school_name TEXT UNIQUE,
    postal_code TEXT, 
    type_code TEXT,
    nature_code TEXT,
    sap_ind TEXT,
    autonomous_ind TEXT,
    gifted_ind TEXT,
    longitude FLOAT,
    latitude FLOAT
);
CREATE INDEX IF NOT EXISTS idx_school_name ON warehouse.int_pri_schools(school_name);
