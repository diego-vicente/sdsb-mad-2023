-- Consolidating all infrastructure
CREATE OR REPLACE TABLE `<PROJECT>.<DATASET>.madrid_bike_all_infrastructure`
CLUSTER BY (geom)
AS (
  WITH
    infra_cte AS (
      SELECT geom, length, usage_type
      FROM `cartobq.docs.madrid_bike_infrastructure`
      UNION ALL
      SELECT geom, length, usage_type
      FROM `cartobq.docs.madrid_bike_quiet_streets`
    ),
    weight_cte AS (
      SELECT
        geom,
        usage_type,
        length,
        CASE
          WHEN usage_type = 'CALLE TRANQUILA' THEN 0.2
          WHEN usage_type = 'GIROS Y SENTIDOS' THEN 0.3
          WHEN usage_type = 'VÍA USO COMPARTIDO' THEN 0.3
          WHEN usage_type = 'VÍA PREFERENTE BICI' THEN 0.5
          WHEN usage_type = 'VÍA EXCLUSIVA BICI' THEN 0.8
          WHEN usage_type = 'ANILLO VERDE CICLISTA' THEN 1
        END AS weight
      FROM
        infra_cte
    )
  SELECT
    geom,
    usage_type,
    length * weight AS lane_value,
  FROM weight_cte
);


-- Find nearby bike parkings
CALL `carto-un`.carto.CREATE_ISOLINES(
  '<API_ENDPOINT>',
  '<LDS_TOKEN>',
  'cartobq.docs.madrid_bike_parkings',
  '<PROJECT>.<DATASET>.madrid_bike_parkings_5min',
  'geom',
  'walk', 5 * 60, 'time',
  NULL
);

CALL `carto-un`.carto.CREATE_ISOLINES(
  '<API_ENDPOINT>',
  '<LDS_TOKEN>',
  'cartobq.docs.madrid_bike_parkings',
  '<PROJECT>.<DATASET>.madrid_bike_parkings_10min',
  'geom',
  'walk', 10 * 60, 'time',
  NULL
);


-- Enter spatial indexes
CREATE TABLE <PROJECT>.<DATASET>.madrid_h3_10
CLUSTER BY (h3)
AS (
  SELECT h3 FROM UNNEST (
    (
      SELECT `carto-un`.carto.H3_POLYFILL(geom, 10)
      FROM cartobq.docs.madrid_city_boundaries
    )
  ) AS h3
);


-- Enrich the grid using points
CALL `carto-un`.carto.ENRICH_GRID(
  -- Index type
  'h3',
  -- Grid query and name of the index column
  '''
  SELECT h3 FROM cartobq.docs.madrid_h3_10
  ''',
  'h3',
  -- Input query and name of the geometry column
  '''
  SELECT id, geom FROM cartobq.docs.madrid_bike_parkings
  ''',
  'geom',
  -- Columns to enrich and aggregation function
  [('id', 'count')],
  -- Output table
  ['`<PROJECT>.<DATASET>.madrid_bike_parkings_h3`']
);


-- Enrich the grid using lines
CALL `carto-un`.carto.ENRICH_GRID(
  -- Index type
  'h3',
  -- Grid query and name of the index column
  '''
  SELECT h3 FROM cartobq.docs.madrid_h3_10
  ''',
  'h3',
  -- Input query and name of the geometry column
  '''
  SELECT geom, lane_level 
  FROM cartobq.docs.madrid_bike_all_infrastructure
  ''',
  'geom',
  -- Columns to enrich and aggregation function
  [('lane_level', 'sum')],
  -- Output table
  ['`<PROJECT>.<DATASET>.madrid_bike_lane_level_h3`']
);


-- Enrich the grid using polygons
CALL `carto-un`.carto.ENRICH_GRID_RAW(
  -- Index type
  'h3',
  -- Grid query and name of the index column
  '''
  SELECT h3 FROM cartobq.docs.madrid_h3_10
  ''',
  'h3',
  -- Input query and variables to enrich
  '''
  SELECT ST_SIMPLIFY(geom, 20) AS geom, True AS placeholder
  FROM cartobq.docs.madrid_bike_parkings_5min_area
  ''',
  'geom', ['placeholder'],
  -- Output table
  ['`<PROJECT>.<DATASET>.madrid_bike_parkings_5min_h3`']
);

CREATE OR REPLACE TABLE `<PROJECT>.<DATASET>.madrid_bike_parkings_5min_h3`
CLUSTER BY (h3)
AS (
  WITH
    areas_cte AS (
      SELECT
        h3,
        enrichment.__carto_intersection AS covered_area,
        ST_AREA(`carto-un`.carto.H3_BOUNDARY(h3)) AS total_area
      FROM cartobq.docs.madrid_bike_parkings_5min_h3,
      UNNEST (__carto_enrichment) AS enrichment
    )
  SELECT
    h3, covered_area, total_area,
    covered_area / total_area AS covered_percentage
  FROM areas_cte
);

CALL `carto-un`.carto.ENRICH_GRID_RAW(
  -- Index type
  'h3',
  -- Grid query and name of the index column
  '''
  SELECT h3 FROM cartobq.docs.madrid_h3_10
  ''',
  'h3',
  -- Input query and variables to enrich
  '''
  SELECT ST_SIMPLIFY(geom, 20) AS geom, True AS placeholder
  FROM cartobq.docs.madrid_bike_parkings_10min_area
  ''',
  'geom', ['placeholder'],
  -- Output table
  ['`<PROJECT>.<DATASET>.madrid_bike_parkings_10min_h3`']
);

CREATE OR REPLACE TABLE `<PROJECT>.<DATASET>.madrid_bike_parkings_10min_h3`
CLUSTER BY (h3)
AS (
  WITH
    areas_cte AS (
      SELECT
        h3,
        enrichment.__carto_intersection AS covered_area,
        ST_AREA(`carto-un`.carto.H3_BOUNDARY(h3)) AS total_area
      FROM cartobq.docs.madrid_bike_parkings_10min_h3,
      UNNEST (__carto_enrichment) AS enrichment
    )
  SELECT
    h3, covered_area, total_area,
    covered_area / total_area AS covered_percentage
  FROM areas_cte
);
