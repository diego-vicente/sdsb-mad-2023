-- Starting with a basic SELECT
SELECT
  * EXCEPT (geom)
FROM
  cartobq.docs.madrid_districts;


-- Adding new features
SELECT
  name,
  area,
  name IN (
    'Centro', 'Arganzuela',
    'Retiro', 'Salamanca',
    'Chamartín', 'Tetuán',
    'Chamberí'
  ) AS within_m30
FROM
  cartobq.docs.madrid_districts;


-- Using Common Table Expressions
WITH
  m30_cte AS (
    SELECT
      *,
      name IN (
        'Centro', 'Arganzuela',
        'Retiro', 'Salamanca',
        'Chamartín', 'Tetuán',
        'Chamberí'
      ) AS within_m30
    FROM
      cartobq.docs.madrid_districts
  )
SELECT
  name,
  area,
  within_m30
FROM
  m30_cte;


-- Basics of grouping
WITH
  m30_cte AS (
    SELECT
      *,
      name IN (
        'Centro', 'Arganzuela',
        'Retiro', 'Salamanca',
        'Chamartín', 'Tetuán',
        'Chamberí'
      ) AS within_m30
    FROM
      cartobq.docs.madrid_districts
  )
SELECT
  within_m30,
  COUNT(*) AS n_districts,
  SUM(area) AS total_area
FROM
  m30_cte
GROUP BY
  within_m30
ORDER BY
  total_area DESC;


-- Basics of filtering
WITH
  m30_cte AS (
    SELECT
      *,
      name IN (
        'Centro', 'Arganzuela',
        'Retiro', 'Salamanca',
        'Chamartín', 'Tetuán',
        'Chamberí'
      ) AS within_m30
    FROM
      cartobq.docs.madrid_districts
  )
SELECT
  within_m30,
  COUNT(*) AS n_districts,
  SUM(area) AS total_area
FROM
  m30_cte
WHERE
  name LIKE 'C%'
GROUP BY
  within_m30
HAVING
  n_districts > 2;


-- Using window functions
WITH
  m30_cte AS (
    SELECT
      *,
      name IN (
        'Centro', 'Arganzuela',
        'Retiro', 'Salamanca',
        'Chamartín', 'Tetuán',
        'Chamberí'
      ) AS within_m30
    FROM
      cartobq.docs.madrid_districts
  )
SELECT
  name,
  area,
  within_m30,
  RANK() OVER (
    PARTITION BY
      within_m30
    ORDER BY
      area DESC
  ) AS m30_ranking
FROM
  m30_cte
QUALIFY
  m30_ranking <= 3
ORDER BY
  m30_ranking DESC;


-- Working with arrays (I)
SELECT
  name,
  SPLIT(name, ' ') AS words
FROM
  cartobq.docs.madrid_districts;


-- Working with arrays (II)
WITH
  words_cte AS (
    SELECT
      name,
      SPLIT(name, ' ') AS words
    FROM
      cartobq.docs.madrid_districts
  )
SELECT
  name,
  word
FROM
  words_cte,
  UNNEST (words) AS word
WHERE
  LENGTH(word) > 2;
