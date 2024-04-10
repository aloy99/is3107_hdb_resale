CREATE TABLE IF NOT EXISTS warehouse.int_nearest_mrts (
  flat_id SERIAL NOT NULL,
  mrt_id SERIAL NOT NULL,
  distance FLOAT NOT NULL,
  FOREIGN KEY (mrt_id) REFERENCES warehouse.int_mrts(id)
);