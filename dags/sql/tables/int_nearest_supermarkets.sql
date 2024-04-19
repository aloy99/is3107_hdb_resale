CREATE TABLE IF NOT EXISTS warehouse.int_nearest_supermarkets (
  id BIGSERIAL PRIMARY KEY,
  flat_id INT NOT NULL,
  supermarket_id INT NOT NULL,
  distance FLOAT NOT NULL,
  FOREIGN KEY (supermarket_id) REFERENCES warehouse.int_supermarkets(id)
);

CREATE INDEX IF NOT EXISTS idx_flat_id ON warehouse.int_nearest_supermarkets(flat_id);
CREATE INDEX IF NOT EXISTS idx_nearest_supermarket_id ON warehouse.int_nearest_supermarkets(supermarket_id);
CREATE INDEX IF NOT EXISTS idx_distance ON warehouse.int_nearest_supermarkets(distance);