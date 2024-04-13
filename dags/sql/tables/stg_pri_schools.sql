CREATE TABLE IF NOT EXISTS staging.stg_pri_schools(
    id SERIAL PRIMARY KEY,
    school_name TEXT UNIQUE,
    postal_code TEXT, 
    type_code TEXT,
    nature_code TEXT,
    sap_ind TEXT,
    autonomous_ind TEXT,
    gifted_ind TEXT
);
CREATE INDEX idx_school_name ON staging.stg_pri_schools(school_name);