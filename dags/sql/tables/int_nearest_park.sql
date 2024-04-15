CREATE TABLE IF NOT EXISTS warehouse.int_nearest_park (
  id SERIAL PRIMARY KEY,
  flat_id INT UNIQUE NOT NULL,
  nearest_park_id INT NOT NULL,
  num_parks_within_radius INT,
  nearest_distance FLOAT NOT NULL,
  FOREIGN KEY (nearest_park_id) REFERENCES warehouse.int_parks(id)
);
CREATE INDEX IF NOT EXISTS idx_flat_id ON warehouse.int_nearest_park(flat_id);
CREATE INDEX IF NOT EXISTS idx_nearest_park_id ON warehouse.int_nearest_mrt(nearest_mrt_id);
CREATE INDEX IF NOT EXISTS idx_nearest_park_distance ON warehouse.int_nearest_park(nearest_distance);
