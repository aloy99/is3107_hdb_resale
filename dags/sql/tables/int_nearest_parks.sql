CREATE TABLE IF NOT EXISTS warehouse.int_nearest_parks (
  id SERIAL PRIMARY KEY,
  flat_id INT NOT NULL,
  park_id INT NOT NULL,
  distance FLOAT NOT NULL,
  FOREIGN KEY (park_id) REFERENCES warehouse.int_parks(id)
);
CREATE INDEX IF NOT EXISTS idx_flat_id ON warehouse.int_nearest_parks(flat_id);
CREATE INDEX IF NOT EXISTS idx_nearest_parks_id ON warehouse.int_nearest_parks(park_id);
CREATE INDEX IF NOT EXISTS idx_nearest_parks_distance ON warehouse.int_nearest_parks(distance);
