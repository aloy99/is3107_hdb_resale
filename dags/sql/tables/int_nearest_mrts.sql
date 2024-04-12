CREATE TABLE IF NOT EXISTS warehouse.int_nearest_mrts (
  id SERIAL PRIMARY KEY,
  flat_id INT NOT NULL,
  mrt_id INT NOT NULL,
  distance FLOAT NOT NULL,
  FOREIGN KEY (mrt_id) REFERENCES warehouse.int_mrts(id),
  UNIQUE (flat_id, mrt_id)
);