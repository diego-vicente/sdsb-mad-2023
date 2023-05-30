-- Get accidents within the hotspots' k-ring
SELECT
  id,
  date,
  accident_type,
  n_involved,
  involved,
  max_severity,
  weather,
  geom
FROM
  cartobq.docs.madrid_bike_accidents
WHERE
  `carto-un`.carto.H3_FROMGEOGPOINT(geom, 10) IN (
    SELECT DISTINCT
      neighbors AS h3
    FROM
      cartobq.docs.madrid_bike_accidents_vs_index,
      UNNEST (`carto-un`.carto.H3_KRING(h3, 3)) AS neighbors
    WHERE
      high_levels = 'accidents'
  )
