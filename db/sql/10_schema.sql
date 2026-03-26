-- Core administrative hierarchy
CREATE TABLE IF NOT EXISTS admin_city (
  id BIGSERIAL PRIMARY KEY,
  city_code TEXT,
  city_name TEXT NOT NULL,
  geom geometry(MultiPolygon, 4326) NOT NULL
);

CREATE TABLE IF NOT EXISTS admin_district (
  id BIGSERIAL PRIMARY KEY,
  district_code TEXT,
  district_name TEXT NOT NULL,
  city_id BIGINT REFERENCES admin_city(id),
  geom geometry(MultiPolygon, 4326) NOT NULL
);

CREATE TABLE IF NOT EXISTS admin_neighborhood (
  id BIGSERIAL PRIMARY KEY,
  neighborhood_code TEXT,
  neighborhood_name TEXT NOT NULL,
  district_id BIGINT REFERENCES admin_district(id),
  geom geometry(MultiPolygon, 4326) NOT NULL
);

-- Roads, buildings, doors
CREATE TABLE IF NOT EXISTS roads (
  id BIGSERIAL PRIMARY KEY,
  road_code TEXT,
  road_name TEXT,
  road_type TEXT,
  neighborhood_id BIGINT REFERENCES admin_neighborhood(id),
  geom geometry(MultiLineString, 4326) NOT NULL
);

CREATE TABLE IF NOT EXISTS buildings (
  id BIGSERIAL PRIMARY KEY,
  building_code TEXT,
  building_no TEXT,
  road_id BIGINT REFERENCES roads(id),
  neighborhood_id BIGINT REFERENCES admin_neighborhood(id),
  geom geometry(MultiPolygon, 4326) NOT NULL
);

CREATE TABLE IF NOT EXISTS doors (
  id BIGSERIAL PRIMARY KEY,
  door_code TEXT,
  external_no TEXT,
  building_id BIGINT REFERENCES buildings(id),
  geom geometry(Point, 4326) NOT NULL
);

-- Spatial indexes
CREATE INDEX IF NOT EXISTS idx_admin_city_geom ON admin_city USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_admin_district_geom ON admin_district USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_admin_neighborhood_geom ON admin_neighborhood USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_roads_geom ON roads USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_buildings_geom ON buildings USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_doors_geom ON doors USING GIST (geom);

-- Common FK indexes
CREATE INDEX IF NOT EXISTS idx_admin_district_city_id ON admin_district(city_id);
CREATE INDEX IF NOT EXISTS idx_admin_neighborhood_district_id ON admin_neighborhood(district_id);
CREATE INDEX IF NOT EXISTS idx_roads_neighborhood_id ON roads(neighborhood_id);
CREATE INDEX IF NOT EXISTS idx_buildings_road_id ON buildings(road_id);
CREATE INDEX IF NOT EXISTS idx_buildings_neighborhood_id ON buildings(neighborhood_id);
CREATE INDEX IF NOT EXISTS idx_doors_building_id ON doors(building_id);
