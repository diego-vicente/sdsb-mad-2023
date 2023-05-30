-- Snap accidents to the H3 grid
CALL `carto-un`.carto.ENRICH_GRID(
  'h3',
  '''
  SELECT h3 FROM cartobq.docs.madrid_h3_10
  ''',
  'h3',
  '''
  SELECT
    geom,
    n_involved,
    max_severity,
    True as accident
  FROM cartobq.docs.madrid_bike_accidents
  ''',
  'geom',
  [('n_involved', 'sum'), ('accident', 'count'), ('max_severity', 'avg')],
  ['`<PROJECT>.<DATASET>.madrid_bike_accidents_h3`']
);


-- Computing Getis-Ord
CREATE TABLE `<PROJECT>.<DATASET>.madrid_bike_index_gi`
CLUSTER BY (h3)
AS (
  SELECT
    getis_ord.index AS h3,
    getis_ord.gi AS gi,
    getis_ord.p_value AS p_value
  FROM
    UNNEST ((
      SELECT
        `carto-un`.carto.GETIS_ORD_H3(
          ARRAY_AGG(STRUCT(h3, spatial_score)),
          3, 'gaussian'
        ) AS getis_ord
      FROM
        cartobq.docs.madrid_bike_index_h3
    )) AS getis_ord
);

CREATE TABLE `<PROJECT>.<DATASET>.madrid_bike_accidents_gi`
CLUSTER BY (h3)
AS (
  SELECT
    getis_ord.index AS h3,
    getis_ord.gi AS gi,
    getis_ord.p_value AS p_value
  FROM
    UNNEST ((
      SELECT
        `carto-un`.carto.GETIS_ORD_H3(
          ARRAY_AGG(STRUCT(h3, accident_count)),
          3, 'gaussian'
        ) AS getis_ord
      FROM
        cartobq.docs.madrid_bike_accidents_h3
    )) AS getis_ord
);