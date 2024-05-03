-- NJ total mapped ksi
WITH
  queried_crashes AS (
    SELECT
      cnj.geometry AS geom,
      cnj.casenumber,
      cnj.countyname,
      cnj.sri_std_rte_identifier_ AS sri,
      ROUND(CAST(cnj.newmp AS NUMERIC), 2) AS mp,
      cnj.latitude,
      cnj.longitude,
      cnj.totalkilled,
      cnj.major_injury::NUMERIC,
      cnf.fatal_or_maj_inj,
      cnf.injury,
      cnf.pedestrian,
      cnf.bicycle
    FROM
      input.crash_newjersey cnj
      JOIN input.crash_nj_flag cnf ON cnj.casenumber = cnf.casenumber
    WHERE
      cnf.fatal_or_maj_inj = 'True'
      OR cnf.pedestrian = 'True'
      OR cnf.bicycle = 'True'
  ),
  -- adds sri/mp to locations with just a lat/long snapping to closest road within 10m 
  nj_lat_long AS (
    SELECT
      cnj.casenumber,
      ST_Transform(ST_SetSRID(ST_MakePoint((longitude * -1), latitude), 4326), 26918) AS geom
    FROM
      queried_crashes cnj
    WHERE
      (
        sri IS NULL
        OR mp IS NULL
      )
      AND (
        latitude IS NOT NULL
        AND longitude IS NOT NULL
      )
  ),
  append_missing AS (
    SELECT DISTINCT
      ON (nj.geom) nj.casenumber,
      lrs.sri,
      (ST_LineLocatePoint(lrs.geometry, nj.geom) * (lrs.mp_end - lrs.mp_start)) + lrs.mp_start AS mp
    FROM
      nj_lat_long nj
      JOIN INPUT.njdot_lrs lrs ON ST_DWithin (nj.geom, lrs.geometry, 10)
    ORDER BY
      nj.geom,
      ST_Distance(nj.geom, lrs.geometry)
  ), 
all_crash as (
SELECT
  njc.casenumber,
  njc.countyname,
  CASE
    WHEN njc.casenumber IN (SELECT a.casenumber FROM append_missing a) THEN a.sri
    ELSE njc.sri
  END AS sri,
  CASE
    WHEN njc.casenumber IN (SELECT a.casenumber FROM append_missing a) THEN a.mp
    ELSE njc.mp
  END AS mp,
  njc.totalkilled,
  njc.major_injury,
  njc.fatal_or_maj_inj,
  njc.injury,
  njc.pedestrian,
  njc.bicycle
FROM
  queried_crashes njc
  FULL JOIN append_missing a ON njc.casenumber = a.casenumber)
SELECT
  COUNT(casenumber),
  UPPER(countyname)
FROM
  all_crash
WHERE
  (
    sri IS NOT NULL
    AND mp IS NOT NULL
  )
  AND (fatal_or_maj_inj = 'True')
GROUP BY
  UPPER(countyname);

-- NJ ksi 2+ road miles
SELECT
  SUM(window_to - window_from)
FROM
  nj_ksi_hin_gis;

-- NJ ksi 2+ road miles by county
WITH
  a AS (
    SELECT
      ST_Intersection (r.geom, c.geometry) AS segmented_geom,
      c.co_name
    FROM
      output.nj_ksi_hin_gis r
      JOIN input.countyboundaries c ON ST_Intersects (r.geom, c.geometry)
  )
SELECT
  SUM(st_length (segmented_geom)) * 0.0006213712 AS miles,
  co_name
FROM
  a
GROUP BY
  co_name;

-- NJ ksi on 2+ HIN
SELECT
  SUM(crashcount)
FROM
  output.nj_ksi_hin_gis;

-- NJ ksi 2+ road miles without Limited Access roads
SELECT
  SUM(window_to - window_from)
FROM
  nj_ksi_hin_gis
WHERE
  CLASS NOT LIKE 'Lim%'
  OR route_subtype NOT IN (1, 4);

-- NJ ksi 2+ road miles by county without Limited Access roads
WITH
  a AS (
    SELECT
      ST_Intersection (r.geom, c.geometry) AS segmented_geom,
      c.co_name
    FROM
      output.nj_ksi_hin_gis r
      JOIN input.countyboundaries c ON ST_Intersects (r.geom, c.geometry)
    WHERE
      r.class NOT LIKE 'Lim%'
      OR r.route_subtype NOT IN (1, 4)
  )
SELECT
  SUM(st_length (segmented_geom)) * 0.0006213712 AS miles,
  co_name
FROM
  a
GROUP BY
  co_name;

-----------------------------------------------------
-- PA ksi 2+ road miles by county all RMS
WITH
  a AS (
    SELECT DISTINCT
      (hin_id),
      id,
      window_to,
      window_from
    FROM
      pa_ksi_2_or_more
  )
SELECT
  SUM(window_to - window_from) * 0.0001893939 AS miles,
  LEFT(id, 2) AS cty
FROM
  a
GROUP BY
  LEFT(id, 2);

-- PA ksi 2+ road miles by county local roads only
WITH
  a AS (
    SELECT DISTINCT
      (hin_id),
      window_to,
      window_from,
      l.cty_code AS cty
    FROM
      pa_lr_ksi_2_or_more pa
      JOIN input.padot_localroads l ON pa.lr_id = l.lr_id
  )
SELECT
  SUM(window_to - window_from) * 0.0001893939 AS miles,
  cty
FROM
  a
GROUP BY
  cty;

-- PA ksi 2+ road miles by county RMS without Limited Access roads
WITH
  a AS (
    SELECT DISTINCT
      (hin_id),
      id,
      window_to,
      window_from,
      access_ctr
    FROM
      pa_ksi_2_or_more
  )
SELECT
  SUM(window_to - window_from) * 0.0001893939 AS miles,
  LEFT(id, 2) AS cty
FROM
  a
WHERE
  a.access_ctr != '1'
GROUP BY
  LEFT(id, 2);

WITH
  a AS (
    SELECT DISTINCT
      (hin_id),
      window_to,
      window_from
    FROM
      pa_lr_bp_2_or_more
  )
SELECT
  SUM(window_to - window_from) * 0.0001893939 AS miles
FROM
  a


