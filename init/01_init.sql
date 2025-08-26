-- Enable PostGIS in the default DB
CREATE EXTENSION IF NOT EXISTS postgis;

-- A tiny demo schema
DROP SCHEMA IF EXISTS demo CASCADE;
CREATE SCHEMA demo;

-- Simple tables: points (cities) and polygons (an arbitrary square)
CREATE TABLE demo.cities (
  id SERIAL PRIMARY KEY,
  name TEXT,
  geom geometry(Point, 4326)
);

CREATE TABLE demo.areas (
  id SERIAL PRIMARY KEY,
  label TEXT,
  geom geometry(Polygon, 4326)
);

-- Data: two points and one polygon (roughly around Tel Aviv area)
INSERT INTO demo.cities (name, geom) VALUES
  ('Point A', ST_SetSRID(ST_MakePoint(34.78, 32.08), 4326)), -- lon,lat
  ('Point B', ST_SetSRID(ST_MakePoint(34.81, 32.10), 4326));

-- A small square polygon around those points
INSERT INTO demo.areas (label, geom) VALUES
  ('Square-1', ST_SetSRID(
     ST_MakePolygon(
       ST_GeomFromText('LINESTRING(34.76 32.06,34.86 32.06,34.86 32.12,34.76 32.12,34.76 32.06)')
     ), 4326));

-- Useful spatial indexes
CREATE INDEX ON demo.cities USING GIST (geom);
CREATE INDEX ON demo.areas  USING GIST (geom);