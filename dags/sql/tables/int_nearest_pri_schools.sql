CREATE TABLE IF NOT EXISTS warehouse.int_nearest_pri_schools (
  id SERIAL PRIMARY KEY,
  flat_id INT NOT NULL,
  pri_sch_id INT NOT NULL,
  distance FLOAT NOT NULL,
  FOREIGN KEY (pri_sch_id) REFERENCES warehouse.int_pri_schools(id)
);

