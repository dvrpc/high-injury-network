-- NJ total mapped ksi
WITH
  a AS (
    SELECT
      cnj.sri_std_rte_identifier_,
      cnj.newmp,
      cnj.casenumber,
      cnj.countyname,
      cnj.totalkilled,
      cnj.major_injury::NUMERIC,
      cnf.fatal_or_maj_inj,
      cnf.injury,
      cnf.pedestrian,
      cnf.bicycle
    FROM
      input.crash_newjersey cnj
      JOIN input.crash_nj_flag cnf ON cnj.casenumber = cnf.casenumber
      JOIN input.njdot_lrs c ON c.sri = cnj.sri_std_rte_identifier_
      AND cnj.newmp BETWEEN ROUND(c.mp_start::NUMERIC, 2) AND ROUND(c.mp_end::NUMERIC, 2)
    WHERE
      cnf.fatal_or_maj_inj = 'True'
  )
SELECT
  LOWER(countyname) AS county,
  COUNT(DISTINCT (casenumber))
FROM
  a
GROUP BY
  LOWER(countyname);

-- NJ ksi 2+ road miles
SELECT
  SUM(window_to - window_from)
FROM
  nj_ksi_2_or_more;

-- NJ ksi 2+ road miles by county
WITH
  a AS (
    SELECT
      ST_Intersection (r.geom, c.geometry) AS segmented_geom,
      c.co_name
    FROM
      output.nj_ksi_2_or_more_gis r
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
  output.nj_ksi_2_or_more nkom;

-- NJ ksi 2+ road miles without Limited Access roads
SELECT
  SUM(window_to - window_from)
FROM
  nj_ksi_2_or_more
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
      output.nj_ksi_2_or_more_gis r
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


