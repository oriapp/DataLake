# duck/job.py
import os
import datetime
from pathlib import Path
import duckdb

# -----------------------------
# 0) Connect + extensions + Postgres attach
# -----------------------------
# Connect to the persistent DuckDB file (mapped from ./data)
con = duckdb.connect("/work/geo.duckdb")

# Load needed extensions
con.sql("INSTALL postgres; LOAD postgres;")
con.sql("INSTALL spatial;  LOAD spatial;")

# Attach Postgres (from env)
PGHOST      = os.getenv("POSTGRES_HOST", "postgis")
PGPORT      = int(os.getenv("POSTGRES_PORT", "5432"))
PGUSER      = os.getenv("POSTGRES_USER", "postgres")
PGPASSWORD  = os.getenv("POSTGRES_PASSWORD", "postgres")
PGDATABASE  = os.getenv("POSTGRES_DB", "gis")


dsn = f"postgresql://{PGUSER}:{PGPASSWORD}@{PGHOST}:{PGPORT}/{PGDATABASE}"
con.sql(f"ATTACH '{dsn}' AS pgdb (TYPE POSTGRES);")

# Ensure standard schemas exist (idempotent)
for schema in ["raw", "stage", "core", "marts"]:
    con.sql(f"CALL postgres_execute('pgdb', 'CREATE SCHEMA IF NOT EXISTS {schema};')")
print("-> Schemas verified: raw, stage, core, marts")

# -----------------------------
# 1) Routing knobs (env)
# -----------------------------
JOB_TASK   = os.getenv("JOB_TASK", "join")          # join | ingest | buffers | checks
SINKS      = {s.strip() for s in os.getenv("JOB_SINKS", "postgis,parquet").split(",") if s.strip()}
LAKE_ROOT  = os.getenv("LAKE_ROOT", "/work/lake")   # maps to ./data/lake on host
LAKE_TIER  = os.getenv("LAKE_TIER", "marts")        # also used as PostGIS schema
DATASET    = os.getenv("DATASET_NAME", "city_in_area")
RUN_DATE   = datetime.date.today().isoformat()

print(f"-> TASK={JOB_TASK} | SINKS={','.join(sorted(SINKS))} | TIER={LAKE_TIER} | DATASET={DATASET}")

# -----------------------------
# 2) Helpers
# -----------------------------
def sink_postgis(source_table: str, schema: str, table: str, ddl_cols_sql: str):
    con.sql(f"CALL postgres_execute('pgdb', 'DROP TABLE IF EXISTS {schema}.{table};')")
    con.sql(f"CALL postgres_execute('pgdb', 'CREATE TABLE {schema}.{table} ({ddl_cols_sql});')")
    con.sql(f"INSERT INTO pgdb.{schema}.{table} SELECT * FROM {source_table};")
    con.sql(f"CALL postgres_execute('pgdb', 'ANALYZE {schema}.{table};')")
    print(f"-> Wrote PostGIS: {schema}.{table}")

def sink_parquet(source_table: str, lake_root: str, tier: str, dataset: str, dt: str):
    out_dir  = Path(lake_root) / tier / dataset / f"dt={dt}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "data.parquet"
    con.sql(f"COPY {source_table} TO '{out_path}' (FORMAT PARQUET);")
    print(f"-> Wrote lake: {out_path}")

def push_geotable_to_postgis(source_table, target_schema, target_table, geom_type="POINT", srid=4326):
    con.sql(f"CALL postgres_execute('pgdb', 'DROP TABLE IF EXISTS {target_schema}.{target_table};')")
    con.sql(f"""
      CALL postgres_execute('pgdb', '
        CREATE TABLE {target_schema}.{target_table} (
          id BIGINT,
          props JSONB,
          geom geometry({geom_type}, {srid})
        );
      ');
    """)
    con.sql(f"""
      INSERT INTO pgdb.{target_schema}.{target_table} (id, props, geom)
      SELECT
        COALESCE(id, row_number() OVER()) AS id,
        to_json(*) EXCEPT (geom)          AS props,
        ST_AsWKB(geom)                    AS geom
      FROM {source_table};
    """)
    con.sql(f"CALL postgres_execute('pgdb', 'CREATE INDEX ON {target_schema}.{target_table} USING GIST(geom); ANALYZE {target_schema}.{target_table};')")
    print(f"-> Wrote PostGIS (geom): {target_schema}.{target_table}")

# -----------------------------
# 3) TASKS
# -----------------------------
def task_ingest():
    """Land a GeoJSON, clean lightly, publish to PostGIS (marts.my_points)."""
    src = "/work/input/my_points.geojson"  # host: ./data/input/my_points.geojson
    if not Path(src).exists():
        raise SystemExit("Place a file at data/input/my_points.geojson and re-run with JOB_TASK=ingest")

    con.sql("DROP TABLE IF EXISTS raw_points;")
    con.sql(f"CREATE TABLE raw_points AS SELECT * FROM ST_Read('{src}');")

    con.sql("DROP TABLE IF EXISTS stage_points;")
    con.sql("""
      CREATE TABLE stage_points AS
      SELECT
        COALESCE(id, row_number() OVER()) AS id,
        to_json(*) EXCEPT (geom)          AS props,
        ST_MakeValid(
          CASE WHEN ST_SRID(geom)=0 THEN ST_SetSRID(geom,4326) ELSE geom END
        ) AS geom
      FROM raw_points
      WHERE geom IS NOT NULL;
    """)

    push_geotable_to_postgis("stage_points", "marts", "my_points", geom_type="POINT", srid=4326)

def task_join():
    """Demo: spatial join cities↔areas from PostGIS, sink to PostGIS/Parquet."""
    con.sql("DROP TABLE IF EXISTS cities;")
    con.sql("""
      CREATE TABLE cities AS
      SELECT id, name, CAST(ST_GeomFromWKB(CAST(geom AS BLOB)) AS GEOMETRY) AS geom
      FROM pgdb.demo.cities;
    """)
    con.sql("DROP TABLE IF EXISTS areas;")
    con.sql("""
      CREATE TABLE areas AS
      SELECT id, label, CAST(ST_GeomFromWKB(CAST(geom AS BLOB)) AS GEOMETRY) AS geom
      FROM pgdb.demo.areas;
    """)

    con.sql("DROP TABLE IF EXISTS city_in_area;")
    con.sql("""
      CREATE TABLE city_in_area AS
      SELECT c.id AS city_id, c.name, a.id AS area_id, a.label,
             ST_Intersects(c.geom, a.geom) AS intersects
      FROM cities c
      JOIN areas  a
        ON ST_Intersects(c.geom, a.geom);
    """)

    if "postgis" in SINKS:
        sink_postgis(
            source_table="city_in_area",
            schema=LAKE_TIER,
            table=DATASET,
            ddl_cols_sql="""
              city_id INTEGER,
              name TEXT,
              area_id INTEGER,
              label TEXT,
              intersects BOOLEAN
            """
        )
    if "parquet" in SINKS:
        sink_parquet("city_in_area", LAKE_ROOT, LAKE_TIER, DATASET, RUN_DATE)

def task_buffers():
    """Demo: 500m buffers around cities → PostGIS (marts.city_buffers)."""
    con.sql("""
      CREATE OR REPLACE TABLE cities AS
      SELECT id, name, CAST(ST_GeomFromWKB(CAST(geom AS BLOB)) AS GEOMETRY) AS geom
      FROM pgdb.demo.cities;
    """)
    con.sql("DROP TABLE IF EXISTS city_buffers;")
    con.sql("""
      CREATE TABLE city_buffers AS
      SELECT id AS city_id, name,
             (ST_Buffer(geom::GEOGRAPHY, 500))::GEOMETRY AS geom
      FROM cities;
    """)
    con.sql("CALL postgres_execute('pgdb', 'DROP TABLE IF EXISTS marts.city_buffers;')")
    con.sql("""
      CALL postgres_execute('pgdb', '
        CREATE TABLE marts.city_buffers (
          city_id INTEGER, name TEXT, geom geometry(Polygon,4326)
        );
      ');
    """)
    con.sql("INSERT INTO pgdb.marts.city_buffers SELECT city_id, name, ST_AsWKB(geom) FROM city_buffers;")
    con.sql("CALL postgres_execute('pgdb', 'CREATE INDEX ON marts.city_buffers USING GIST(geom); ANALYZE marts.city_buffers;')")
    print("-> Wrote PostGIS: marts.city_buffers")

def task_checks():
    """Basic data quality queries against PostGIS via DuckDB."""
    checks = {
        "demo.cities_rows":        "SELECT COUNT(*) AS n FROM pgdb.demo.cities",
        "demo.areas_rows":         "SELECT COUNT(*) AS n FROM pgdb.demo.areas",
        "marts.my_points_rows":    "SELECT COUNT(*) AS n FROM pgdb.marts.my_points",
        "marts.my_points_srid":    "SELECT DISTINCT ST_SRID(geom) AS srid FROM pgdb.marts.my_points",
        "marts.my_points_invalid": "SELECT COUNT(*) AS n FROM pgdb.marts.my_points WHERE NOT ST_IsValid(geom)"
    }
    for name, sql in checks.items():
        try:
            print(name, "=>", con.sql(sql).df().to_dict(orient="records"))
        except Exception as e:
            print(name, "=> ERROR:", e)

# -----------------------------
# 4) Dispatch
# -----------------------------
if JOB_TASK == "ingest":
    task_ingest(); print("✅ Ingest done."); raise SystemExit(0)
elif JOB_TASK == "buffers":
    task_buffers(); print("✅ Buffers done."); raise SystemExit(0)
elif JOB_TASK == "checks":
    task_checks(); print("✅ Checks done."); raise SystemExit(0)
else:  # default
    task_join(); print("✅ Join done."); raise SystemExit(0)