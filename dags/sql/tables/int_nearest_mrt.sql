CREATE TABLE IF NOT EXISTS warehouse.int_nearest_mrt (
  id SERIAL PRIMARY KEY,
  flat_id INT UNIQUE NOT NULL,
  nearest_mrt_id INT NOT NULL,
  num_mrts_within_radius INT,
  distance FLOAT NOT NULL,
  FOREIGN KEY (nearest_mrt_id) REFERENCES warehouse.int_mrts(id)
);
CREATE INDEX IF NOT EXISTS idx_flat_id ON warehouse.int_nearest_mrt(flat_id);
CREATE INDEX IF NOT EXISTS idx_nearest_mrt_id ON warehouse.int_nearest_mrt(nearest_mrt_id);
CREATE INDEX IF NOT EXISTS idx_nearest_mrt_distance ON warehouse.int_nearest_mrt(distance);
