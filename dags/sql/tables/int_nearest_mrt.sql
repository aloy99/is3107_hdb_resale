CREATE TABLE IF NOT EXISTS warehouse.int_nearest_mrt (
  id SERIAL PRIMARY KEY,
  flat_id INT NOT NULL,
  mrt_id INT NOT NULL,
  distance FLOAT NOT NULL,
  FOREIGN KEY (mrt_id) REFERENCES warehouse.int_mrts(id),
  UNIQUE (flat_id, mrt_id)
);
CREATE INDEX idx_flat_id ON warehouse.int_nearest_mrt(flat_id);
CREATE INDEX idx_mrt_id ON warehouse.int_nearest_mrt(mrt_id);
CREATE INDEX idx_nearest_mrt_distance ON warehouse.int_nearest_mrt(distance);
