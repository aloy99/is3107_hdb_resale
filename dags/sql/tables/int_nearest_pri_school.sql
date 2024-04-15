CREATE TABLE IF NOT EXISTS warehouse.int_nearest_pri_school (
  id SERIAL PRIMARY KEY,
  flat_id INT UNIQUE NOT NULL,
  nearest_pri_sch_id INT NOT NULL,
  num_pri_sch_within_radius INT,
  nearest_distance FLOAT NOT NULL,
  FOREIGN KEY (nearest_pri_sch_id) REFERENCES warehouse.int_pri_schools(id)
);
CREATE INDEX IF NOT EXISTS idx_flat_id ON warehouse.int_nearest_pri_school(flat_id);
CREATE INDEX IF NOT EXISTS idx_nearest_pri_sch_id ON warehouse.int_nearest_pri_school(nearest_pri_sch_id);
CREATE INDEX IF NOT EXISTS idx_nearest_pri_sch_distance ON warehouse.int_nearest_pri_school(nearest_distance);
