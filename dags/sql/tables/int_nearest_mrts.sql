CREATE TABLE IF NOT EXISTS warehouse.int_nearest_mrts (
  id SERIAL PRIMARY KEY,
  flat_id INT NOT NULL,
  mrt_id INT NOT NULL,
  distance FLOAT NOT NULL,
  FOREIGN KEY (mrt_id) REFERENCES warehouse.int_mrts(id),
  UNIQUE (flat_id, mrt_id)
);
CREATE INDEX idx_flat_id ON warehouse.int_nearest_mrts(flat_id);
CREATE INDEX idx_mrt_id ON warehouse.int_nearest_mrts(mrt_id);
CREATE INDEX idx_distance ON warehouse.int_nearest_mrts(distance);
