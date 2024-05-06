/*
query nj crash data for KSI and bike/ped (eliminates Trenton City)
 */
BEGIN;
CREATE OR REPLACE VIEW
  output.nj_ksi_bp_crashes AS
WITH
  queried_crashes AS (
    SELECT
      cnj.geometry AS geom,
      cnj.casenumber,
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
      (cnf.fatal_or_maj_inj = 'True'
      OR cnf.pedestrian = 'True'
      OR cnf.bicycle = 'True')
      AND cnj.municipalityname not like ('%TRENTON%')
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
      JOIN input.njdot_lrs lrs ON ST_DWithin (nj.geom, lrs.geometry, 10)
    ORDER BY
      nj.geom,
      ST_Distance(nj.geom, lrs.geometry)
  )
SELECT
  njc.casenumber,
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
  FULL JOIN append_missing a ON njc.casenumber = a.casenumber;
COMMIT;
/*
creating nj .5 mile sliding window segments and summarizing crash data for those windows
 */
BEGIN;
CREATE OR REPLACE VIEW output.nj_slidingwindow AS
WITH RECURSIVE
  windows AS (
    SELECT
      r.sri,
      r.mp_start AS road_mp_start,
      r.mp_end AS road_mp_end,
      r.mp_start AS window_from,
      LEAST (r.mp_start + :windowsize, r.mp_end) AS window_to,
      class,
      route_subtype
    FROM
      input.nj_lrs_access r
    WHERE
      r.sri IS NOT NULL
    UNION ALL
    SELECT
      w.sri,
      w.road_mp_start,
      w.road_mp_end,
      w.window_from + :window_increment AS window_from, -- 0.01 increments
      LEAST (w.window_from + :windowsize, w.road_mp_end) AS window_to,
      class,
      route_subtype
    FROM
      windows w
    WHERE
      (w.window_from + :windowsize) <= w.road_mp_end + :window_increment)
SELECT
  windows.sri,
  ROUND(CAST(windows.road_mp_start AS NUMERIC), 2) AS road_mp_start,
  ROUND(CAST(windows.road_mp_end AS NUMERIC), 2) AS road_mp_end,
  ROUND(CAST(windows.window_from AS NUMERIC), 2) AS window_from,
  ROUND(CAST(windows.window_to AS NUMERIC), 2) AS window_to,
  class,
  route_subtype
FROM
  windows
WHERE
  window_from < road_mp_end
ORDER BY
  sri,
  window_from ASC;
COMMIT;
-- KSI 2+ .5 mile HIN
BEGIN;
CREATE OR REPLACE VIEW output.nj_ksi_hin AS
WITH
  nj_ksi_only AS (
    SELECT
      *
    FROM
      output.nj_ksi_bp_crashes
    WHERE
      fatal_or_maj_inj = 'True'
  ),
  hin AS (
    SELECT
      s.sri,
      s.window_from,
      s.window_to,
      COUNT(c.casenumber) AS crashcount,
      s.class,
      s.route_subtype
    FROM
      output.nj_slidingwindow s
      LEFT JOIN nj_ksi_only c ON c.sri = s.sri
      AND c.mp >= s.window_from
      AND c.mp <= s.window_to
    GROUP BY
      s.sri,
      s.window_from,
      s.window_to, 
      s.class, 
      s.route_subtype
    HAVING
      COUNT(c.casenumber) >= :crashcount
    ORDER BY
      s.sri,
      s.window_from
  ),
  find_breaks AS (
    SELECT
      *,
      CASE
        WHEN window_from - lag (window_to, 1, 0) OVER (
          PARTITION BY
            sri,
            class
          ORDER BY
            sri,
            window_from
        ) > :gap THEN 1 -- threshold distance to other segments 2500ft
        ELSE 0
      END AS break
    FROM
      hin
  ),
  agg_segs AS ( -- aggregates overlapping segments
    SELECT
      sri,
      MIN(window_from) AS window_from,
      MAX(window_to) AS window_to
    FROM
      (
        SELECT
          *,
          SUM(break) OVER (
            ORDER BY
              sri,
              window_from
          ) AS grp
        FROM
          find_breaks
      ) AS grouped
    GROUP BY
      sri,
      grp
    ORDER BY
      sri,
      window_from asc
  )
SELECT  -- joins ksi crashes back to aggregated segments
  ROW_NUMBER() OVER (
    ORDER BY
      a.sri,
      a.window_from
  ) AS hin_id,
  a.*,
  COUNT(c.casenumber) AS crashcount,
  SUM(c.totalkilled) AS total_killed,
  SUM(c.major_injury) AS total_maj_inj,
  lrs.class,
  lrs.route_subtype
FROM
  agg_segs a
  LEFT JOIN nj_ksi_only c ON c.sri = a.sri AND c.mp >= a.window_from AND c.mp <= a.window_to
  LEFT JOIN input.nj_lrs_access lrs ON a.sri = lrs.sri AND a.window_from >= round(lrs.mp_start::numeric, 2) AND a.window_to <= round(lrs.mp_end::numeric, 2)
GROUP BY
  a.sri,
  a.window_from,
  a.window_to,
  lrs.class,
  lrs.route_subtype;
COMMIT;
BEGIN;
-- Bike/Ped 2+ .5 HIN
CREATE OR REPLACE VIEW output.nj_bp_hin AS
WITH
  nj_bp_only AS (
    SELECT
      *
    FROM
      output.nj_ksi_bp_crashes
    WHERE
      pedestrian = 'True' or bicycle = 'True'
  ),
  hin AS (
    SELECT
      s.sri,
      s.window_from,
      s.window_to,
      COUNT(c.casenumber) AS crashcount,
      s.class,
      s.route_subtype
    FROM
      output.nj_slidingwindow s
      LEFT JOIN nj_bp_only c ON c.sri = s.sri
      AND c.mp >= s.window_from
      AND c.mp <= s.window_to
    GROUP BY
      s.sri,
      s.window_from,
      s.window_to,
      s.class,
      s.route_subtype
    HAVING
      COUNT(c.casenumber) >= :crashcount
    ORDER BY
      s.sri,
      s.window_from
  ),
  find_breaks AS (
    SELECT
      *,
      CASE
        WHEN window_from - lag (window_to, 1, 0) OVER (
          PARTITION BY
            sri,
            class
          ORDER BY
            sri,
            window_from
        ) > :gap THEN 1 -- threshold distance to other segments 2500ft
        ELSE 0
      END AS break
    FROM
      hin
  ),
  agg_segs AS ( -- aggregates overlapping segments
    SELECT
      sri,
      MIN(window_from) AS window_from,
      MAX(window_to) AS window_to
    FROM
      (
        SELECT
          *,
          SUM(break) OVER (
            ORDER BY
              sri,
              window_from
          ) AS grp
        FROM
          find_breaks
      ) AS grouped
    GROUP BY
      sri,
      grp
    ORDER BY
      sri,
      window_from asc
  )
SELECT  -- joins bp crashes back to aggregated segments
  ROW_NUMBER() OVER (
    ORDER BY
      a.sri,
      a.window_from
  ) AS hin_id,
  a.*,
  COUNT(c.casenumber) AS crashcount,
  SUM(c.totalkilled) AS total_killed,
  SUM(c.major_injury) AS total_maj_inj,
  lrs.class,
  lrs.route_subtype
FROM
  agg_segs a
  LEFT JOIN nj_bp_only c ON c.sri = a.sri AND c.mp >= a.window_from AND c.mp <= a.window_to
  LEFT JOIN input.nj_lrs_access lrs ON a.sri = lrs.sri AND a.window_from >= round(lrs.mp_start::numeric, 2) AND a.window_to <= round(lrs.mp_end::numeric, 2)
GROUP BY
  a.sri,
  a.window_from,
  a.window_to,
  lrs.class,
  lrs.route_subtype;
COMMIT;
/*
prepare the pa crash data
 */
BEGIN;
CREATE OR REPLACE VIEW output.pa_ksi_bp_crashes AS (
-- add roadway info, query ksi+, query year range 2018-2022
WITH pa_crashes_cleaned AS (
SELECT
	cpa.geometry as geom,
	cpa.crn,
	cpa.crash_year,
  cpa.fatal_count,
	cpa.maj_inj_count,
	fpa.major_injury,
	fpa.fatal,
	fpa.injury,
	fpa.fatal_or_susp_serious_inj,
	fpa.bicycle,
	fpa.pedestrian,
	rpa.adj_rdwy_seq,
	CASE
		WHEN length(rpa.county) = 1 THEN '0'::text || rpa.county
		ELSE rpa.county
	END AS county,
	rpa.route,
	CASE
		WHEN length(rpa.segment) = 1 THEN '000'::text || rpa.segment
		WHEN length(rpa.segment) = 2 THEN '00'::text || rpa.segment
		WHEN length(rpa.segment) = 3 THEN '0'::text || rpa.segment
		ELSE rpa.segment
	END AS segment,
  CASE
    WHEN rpa.offset_ IN ('ERRO','9999') THEN NULL -- handles weird offset values for 2022
    ELSE rpa.offset_::numeric
  END AS "offset",
	concat(
    CASE WHEN length(rpa.county) = 1 THEN '0'::text || rpa.county 
    ELSE rpa.county 
    END,
	rpa.route,
	rpa.side_ind) AS id
FROM
	input.crash_pennsylvania cpa
JOIN input.crash_pa_flag fpa ON
	fpa.crn = cpa.crn
JOIN input.crash_pa_roadway rpa ON
	rpa.crn = cpa.crn
WHERE
	(crash_year between 2018 AND 2022)
	AND rpa.adj_rdwy_seq = 3
	AND (major_injury = 1
		or fatal = 1
		or bicycle = 1
		or pedestrian = 1)),
-- format rms with lrs id
rms AS (
SELECT
	concat(padot_rms.cty_code, padot_rms.st_rt_no, padot_rms.side_ind) AS id,
	padot_rms.seg_no,
	padot_rms.cum_offset,
	padot_rms.cum_offs_1
FROM
	input.padot_rms
ORDER BY
	(concat(padot_rms.cty_code, padot_rms.st_rt_no, padot_rms.side_ind)),
	padot_rms.cum_offset),
-- add lrs info to the crash data that includes roadway info
add_lrs_info AS (
SELECT
	pc.geom,
	pc.crn,
	pc.crash_year,
  pc.fatal_count,
	pc.maj_inj_count,
	pc.major_injury,
	pc.fatal,
	pc.injury,
	pc.fatal_or_susp_serious_inj,
	pc.bicycle,
	pc.pedestrian,
	pc.adj_rdwy_seq,
	pc.county,
	pc.route,
	pc.segment,
	pc.offset,
	pc.id,
	pc.offset + rms.cum_offset AS cum_offset
FROM
	pa_crashes_cleaned pc
JOIN rms ON
	rms.id = pc.id
	AND rms.seg_no = pc.segment
WHERE
	pc.route IS NOT NULL AND pc.segment IS NOT NULL AND pc.offset IS NOT NULL),
-- create pa lrs with m value (not sure it's really needed)
create_pa_lrs AS (
SELECT
	concat(cty_code, st_rt_no, side_ind) AS id,
	seg_no,
	cum_offset,
	cum_offs_1,
	ST_AddMeasure(ST_MakeLine(geometry), cum_offset, cum_offs_1) AS geom
FROM
	input.padot_rms
WHERE
	geometry IS NOT NULL
GROUP BY
	concat(cty_code, st_rt_no, side_ind),
	seg_no,
	cum_offset,
	cum_offs_1),
-- add lrs info to the crash data that didn't include roadway info, crashes w/in 10 meters give it LRS id and measure
add_missing_measure AS (
SELECT DISTINCT ON (pc.geom)
	pc.geom,
	pc.crn,
	pc.crash_year,
  pc.fatal_count,
	pc.maj_inj_count,
	pc.major_injury,
	pc.fatal,
	pc.injury,
	pc.fatal_or_susp_serious_inj,
	pc.bicycle,
	pc.pedestrian,
	pc.adj_rdwy_seq,
	pc.county,
	pc.route,
	pc.segment,
	pc.offset,
	pl.id,
	(ST_LineLocatePoint(pl.geom, pc.geom) * (pl.cum_offs_1 - pl.cum_offset)) + pl.cum_offset AS cum_offset
FROM
	pa_crashes_cleaned pc
JOIN
  create_pa_lrs pl
ON
	ST_DWithin(pc.geom, pl.geom, 10)
WHERE
	pc.route IS NULL OR pc.segment IS NULL OR pc.offset IS NULL
ORDER BY 
  pc.geom, ST_Distance(pc.geom, pl.geom))
-- union 2 crash cte 
SELECT
	*
FROM
	add_missing_measure
UNION
SELECT
	*
FROM
	add_lrs_info);
COMMIT;
/*
creating pa .5 mile sliding window segments and summarizing crash data for those windows
 */
BEGIN;
-- makes longer rms segs based on county and route number
CREATE
OR REPLACE VIEW output.pa_long_segs_ft AS
WITH
  rms_cleaned AS (
    SELECT
      concat (rms.cty_code, rms.st_rt_no, rms.side_ind) AS id,
      rms.seg_no,
      0 AS b_offset,
      rms.seg_lngth_ AS e_offset,
      rms.cum_offset AS tot_measure_b,
      rms.cum_offs_1 AS tot_measure_e,
      rms.interst_ne,
      rms.access_ctr
    FROM
      input.padot_rms rms
    ORDER BY
      (concat (rms.cty_code, rms.st_rt_no, rms.seg_no)),
      rms.side_ind
  )
SELECT
  rms_cleaned.id,
  MIN(rms_cleaned.tot_measure_b) AS b_feet,
  MAX(rms_cleaned.tot_measure_e) AS e_feet,
  rms_cleaned.interst_ne as interstate,
  rms_cleaned.access_ctr
FROM
  rms_cleaned
GROUP BY
  rms_cleaned.id,
  rms_cleaned.interst_ne,
  rms_cleaned.access_ctr
ORDER BY
  rms_cleaned.id;
COMMIT;
BEGIN;
-- creates sliding window segs
CREATE OR REPLACE VIEW output.pa_slidingwindow AS
WITH RECURSIVE
  windows AS (
    SELECT
      r.id,
      r.b_feet AS road_ft_start,
      r.e_feet AS road_ft_end,
      r.b_feet AS window_from,
      LEAST (r.b_feet + (5280 * :windowsize), r.e_feet) AS window_to,
      r.interstate,
      r.access_ctr
    FROM
      output.pa_long_segs_ft r
    WHERE
      r.id IS NOT NULL
    UNION ALL
    SELECT
      w.id,
      w.road_ft_start,
      w.road_ft_end,
      w.window_from + (5280 * :window_increment) AS window_from,
      LEAST (w.window_from + (5280 * :windowsize), w.road_ft_end) AS window_to,
      w.interstate,
      w.access_ctr
    FROM
      windows w
    WHERE
      (w.window_from + (5280 * :windowsize)) <= w.road_ft_end + ((5280 * :window_increment)-0.1)
  )
SELECT
  windows.id,
  ROUND(CAST(windows.road_ft_start AS NUMERIC), 2) AS road_ft_start,
  ROUND(CAST(windows.road_ft_end AS NUMERIC), 2) AS road_ft_end,
  ROUND(CAST(windows.window_from AS NUMERIC), 2) AS window_from,
  ROUND(CAST(windows.window_to AS NUMERIC), 2) AS window_to,
  windows.interstate,
  windows.access_ctr
FROM
  windows
WHERE
  windows.window_from < windows.road_ft_end
ORDER BY
  windows.id,
  windows.window_from;
COMMIT;
-- PA KSI 2+ .5 mile HIN
BEGIN;
CREATE OR REPLACE VIEW output.pa_ksi_hin AS
WITH
  pa_ksi_only AS (
    SELECT
      *
    FROM
      output.pa_ksi_bp_crashes
    WHERE
      major_injury = 1
      OR fatal = 1
  ),
  hin AS (
    SELECT
      s.id,
      s.window_from,
      s.window_to,
      COUNT(c.crn) AS crashcount
    FROM
      output.pa_slidingwindow s
      LEFT JOIN pa_ksi_only c ON c.id = s.id
      AND c.cum_offset >= s.window_from
      AND c.cum_offset <= s.window_to
    GROUP BY
      s.id,
      s.window_from,
      s.window_to
    HAVING
      COUNT(c.crn) >= :crashcount
    ORDER BY
      s.id,
      s.window_from
  ),
  find_breaks AS (
    SELECT
      hin.id,
      hin.window_from,
      hin.window_to,
      hin.crashcount,
      CASE
        WHEN (
          hin.window_from - lag (hin.window_to, 1, 0) OVER (
            PARTITION BY
              hin.id
            ORDER BY
              hin.id,
              hin.window_from
          )
        ) > (round(:gap,0) * 5280) THEN 1 -- threshold distance to other segments 2500ft
        ELSE 0
      END AS break
    FROM
      hin
  ),
  agg_segs AS ( -- aggregates overlapping segments
    SELECT
      grouped.id,
      MIN(grouped.window_from) AS window_from,
      MAX(grouped.window_to) AS window_to
    FROM
      (
        SELECT
          find_breaks.id,
          find_breaks.window_from,
          find_breaks.window_to,
          find_breaks.crashcount,
          find_breaks.break,
          SUM(find_breaks.break) OVER (
            ORDER BY
              find_breaks.id,
              find_breaks.window_from
          ) AS grp
        FROM
          find_breaks
      ) grouped
    GROUP BY
      grouped.id,
      grouped.grp
    ORDER BY
      grouped.id,
      (MIN(grouped.window_from))
  )
SELECT
  ROW_NUMBER() OVER (
    ORDER BY
      a.id,
      a.window_from
  ) AS hin_id,
  a.id,
  a.window_from,
  a.window_to,
  COUNT(c.crn) AS crashcount,
  SUM(c.fatal_count) as total_killed,
  SUM(c.maj_inj_count) as total_maj_inj,
  segs.interstate,
  segs.access_ctr
FROM
  agg_segs a
  LEFT JOIN pa_ksi_only c ON c.id = a.id AND c.cum_offset >= a.window_from AND c.cum_offset <= a.window_to
  LEFT JOIN output.pa_long_segs_ft segs ON segs.id = a.id AND a.window_from >= segs.b_feet AND a.window_to <= segs.e_feet
GROUP BY
  a.id,
  a.window_from,
  a.window_to,
  segs.interstate,
  segs.access_ctr;
COMMIT;
-- Bike/Ped 2+ .5 mile HIN
BEGIN;
CREATE OR REPLACE VIEW output.pa_bp_hin AS
WITH
  pa_bp_only AS (
    SELECT
      *
    FROM
      output.pa_ksi_bp_crashes
    WHERE
      bicycle = 1
      OR pedestrian = 1
  ),
  hin AS (
    SELECT
      s.id,
      s.window_from,
      s.window_to,
      COUNT(c.crn) AS crashcount
    FROM
      output.pa_slidingwindow s
      LEFT JOIN pa_bp_only c ON c.id = s.id
      AND c.cum_offset >= s.window_from
      AND c.cum_offset <= s.window_to
    GROUP BY
      s.id,
      s.window_from,
      s.window_to
    HAVING
      COUNT(c.crn) >= :crashcount
    ORDER BY
      s.id,
      s.window_from
  ),
  find_breaks AS (
    SELECT
      hin.id,
      hin.window_from,
      hin.window_to,
      hin.crashcount,
      CASE
        WHEN (hin.window_from - lag (hin.window_to, 1, 0) OVER 
          (PARTITION BY hin.id ORDER BY hin.id, hin.window_from)) > (round(:gap,0) * 5280) THEN 1
        ELSE 0
      END AS break
    FROM
      hin
  ),
  agg_segs AS (
    SELECT
      grouped.id,
      MIN(grouped.window_from) AS window_from,
      MAX(grouped.window_to) AS window_to
    FROM
      (
        SELECT
          find_breaks.id,
          find_breaks.window_from,
          find_breaks.window_to,
          find_breaks.crashcount,
          find_breaks.break,
          SUM(find_breaks.break) OVER (
            ORDER BY
              find_breaks.id,
              find_breaks.window_from
          ) AS grp
        FROM
          find_breaks
      ) grouped
    GROUP BY
      grouped.id,
      grouped.grp
    ORDER BY
      grouped.id,
      (MIN(grouped.window_from))
  )
SELECT
  ROW_NUMBER() OVER (ORDER BY a.id, a.window_from) AS hin_id,
  a.id,
  a.window_from,
  a.window_to,
  COUNT(c.crn) AS crashcount,
  SUM(c.fatal_count) as total_killed,
  SUM(c.maj_inj_count) as total_maj_inj,
  segs.interstate,
  segs.access_ctr
FROM
  agg_segs a
  LEFT JOIN pa_bp_only c ON c.id = a.id AND c.cum_offset >= a.window_from AND c.cum_offset <= a.window_to
  LEFT JOIN output.pa_long_segs_ft segs ON segs.id = a.id AND a.window_from >= segs.b_feet AND a.window_to <= segs.e_feet
GROUP BY
  a.id,
  a.window_from,
  a.window_to,
  segs.interstate,
  segs.access_ctr;
COMMIT;
/*
PA Local Road HIN processing
 */
-- create long segments of the local road network based on lr_id
BEGIN;
CREATE
OR REPLACE VIEW output.pa_long_lrsegs_ft as
SELECT
  lr_id,
  MIN(cum_offset_bgn)::numeric AS b_feet,
  MAX(cum_offset_end)::numeric AS e_feet
FROM
  input.padot_localroads lr
GROUP BY
 	lr_id
ORDER BY
  lr_id;
COMMIT;
-- create .5 sliding window for local road segments
BEGIN;
CREATE OR REPLACE VIEW output.pa_lr_slidingwindow AS
WITH RECURSIVE
  windows AS (
    SELECT
      r.lr_id,
      r.b_feet AS road_ft_start,
      r.e_feet AS road_ft_end,
      r.b_feet AS window_from,
      LEAST (r.b_feet + (5280 * :windowsize), r.e_feet) AS window_to
    FROM
      output.pa_long_lrsegs_ft r
    WHERE
      r.lr_id IS NOT NULL
    UNION ALL
    SELECT
      w.lr_id,
      w.road_ft_start,
      w.road_ft_end,
      w.window_from + (5280 * :window_increment) AS window_from,
      LEAST (w.window_from + (5280 * :windowsize), w.road_ft_end) AS window_to
    FROM
      windows w
    WHERE
      (w.window_from + (5280 * :windowsize)) <= w.road_ft_end + ((5280 * :window_increment)-0.1)
  )
SELECT
  windows.lr_id,
  ROUND(CAST(windows.road_ft_start AS NUMERIC), 2) AS road_ft_start,
  ROUND(CAST(windows.road_ft_end AS NUMERIC), 2) AS road_ft_end,
  ROUND(CAST(windows.window_from AS NUMERIC), 2) AS window_from,
  ROUND(CAST(windows.window_to AS NUMERIC), 2) AS window_to
FROM
  windows
WHERE
  windows.window_from < windows.road_ft_end
ORDER BY
  windows.lr_id,
  windows.window_from;
COMMIT;
-- KSI 2+ .5 mile HIN on PA local roads, only for those KSI that didn't map on RMS
BEGIN;
CREATE OR REPLACE VIEW output.pa_lr_ksi_hin AS
WITH
  missing_crashes AS (
  SELECT
    cp.geometry AS geom,
    cp.crn,
    cp.crash_year,
    cp.fatal_count,
    cp.maj_inj_count,
    fpa.major_injury,
    fpa.fatal,
    fpa.injury,
    fpa.fatal_or_susp_serious_inj,
    fpa.bicycle,
    fpa.pedestrian
  FROM
    input.crash_pennsylvania cp
  JOIN
    input.crash_pa_flag fpa
  ON
    fpa.crn = cp.crn
  WHERE
    cp.crn NOT IN ( -- crashes not mapped with RMS
    SELECT
      crn
    FROM
      output.pa_ksi_bp_crashes)
    AND cp.crash_year BETWEEN 2018 AND 2022
    AND (fpa.major_injury = 1
      OR fpa.fatal = 1
      --		or fpa.bicycle = 1
      --		or fpa.pedestrian = 1
      )
    AND ST_ISEMPTY(cp.geometry) IS FALSE),
  pa_lr_ksi_only AS (
  SELECT
    DISTINCT
  ON
    (ms.geom) ms.geom,
    ms.crn,
    ms.crash_year,
    ms.fatal_count,
    ms.maj_inj_count,
    ms.major_injury,
    ms.fatal,
    ms.injury,
    ms.fatal_or_susp_serious_inj,
    ms.bicycle,
    ms.pedestrian,
    lr.cty_code,
    lr.lr_id,
    (ST_LineLocatePoint(lr.geometry, ms.geom) * (lr.cum_offset_end - lr.cum_offset_bgn)) + lr.cum_offset_bgn AS cum_offset
  FROM
    missing_crashes ms
  JOIN (
    SELECT
      *
    FROM
      input.padot_localroads
    WHERE
      lr_id IS NOT NULL) lr
  ON
    ST_DWITHIN(ms.geom, lr.geometry, 10)
  ORDER BY
    ms.geom,
    ST_DISTANCE(ms.geom, lr.geometry)),
  hin AS (
  SELECT
    s.lr_id,
    s.window_from,
    s.window_to,
    COUNT(c.crn) AS crashcount
  FROM
    output.pa_lr_slidingwindow s
  LEFT JOIN
    pa_lr_ksi_only c
  ON
    c.lr_id = s.lr_id
    AND c.cum_offset >= s.window_from
    AND c.cum_offset <= s.window_to
  GROUP BY
    s.lr_id,
    s.window_from,
    s.window_to
  HAVING
    COUNT(c.crn) >= :crashcount
  ORDER BY
    s.lr_id,
    s.window_from),
  find_breaks AS (
  SELECT
    hin.lr_id,
    hin.window_from,
    hin.window_to,
    hin.crashcount,
    CASE
      WHEN (hin.window_from - lag (hin.window_to, 1, 0) OVER 
      (PARTITION BY hin.lr_id ORDER BY hin.lr_id, hin.window_from)) > (round(:gap,0) * 5280) THEN 1 -- threshold distance to other segments 2500ft
    ELSE 0
    END AS break
  FROM
    hin),
  agg_segs AS (
    SELECT
      grouped.lr_id,
      MIN(grouped.window_from) AS window_from,
      MAX(grouped.window_to) AS window_to
    FROM
      (
        SELECT
          find_breaks.lr_id,
          find_breaks.window_from,
          find_breaks.window_to,
          find_breaks.crashcount,
          find_breaks.break,
          SUM(find_breaks.break) OVER (
            ORDER BY
              find_breaks.lr_id,
              find_breaks.window_from
          ) AS grp
        FROM
          find_breaks
      ) grouped
    GROUP BY
      grouped.lr_id,
      grouped.grp
    ORDER BY
      grouped.lr_id,
      (MIN(grouped.window_from))
  )
SELECT
  ROW_NUMBER() OVER (ORDER BY a.lr_id, a.window_from ) AS hin_id,
  a.lr_id,
  a.window_from,
  a.window_to,
  COUNT(c.crn) AS crashcount,
  SUM(c.fatal_count) as total_killed,
  SUM(c.maj_inj_count) as total_maj_inj
FROM
  agg_segs a
LEFT JOIN
  pa_lr_ksi_only c
ON
  c.lr_id = a.lr_id
  AND c.cum_offset >= a.window_from
  AND c.cum_offset <= a.window_to
GROUP BY
  a.lr_id,
  a.window_from,
  a.window_to;
COMMIT;
-- Bike/Ped 2+ .5 mile HIN on PA local roads, only for those KSI that didn't map on RMS
BEGIN;
CREATE OR REPLACE VIEW output.pa_lr_bp_hin AS
WITH
  missing_crashes AS (
  SELECT
    cp.geometry AS geom,
    cp.crn,
    cp.crash_year,
    cp.fatal_count,
    cp.maj_inj_count,
    fpa.major_injury,
    fpa.fatal,
    fpa.injury,
    fpa.fatal_or_susp_serious_inj,
    fpa.bicycle,
    fpa.pedestrian
  FROM
    input.crash_pennsylvania cp
  JOIN
    input.crash_pa_flag fpa
  ON
    fpa.crn = cp.crn
  WHERE
    cp.crn NOT IN ( -- crashes not mapped with RMS
    SELECT
      crn
    FROM
      output.pa_ksi_bp_crashes)
    AND cp.crash_year BETWEEN 2018 AND 2022
    AND (fpa.bicycle = 1 or fpa.pedestrian = 1)
    AND ST_ISEMPTY(cp.geometry) IS FALSE),
  pa_lr_bp_only AS (
  SELECT
    DISTINCT
  ON
    (ms.geom) ms.geom,
    ms.crn,
    ms.crash_year,
    ms.fatal_count,
    ms.maj_inj_count,
    ms.major_injury,
    ms.fatal,
    ms.injury,
    ms.fatal_or_susp_serious_inj,
    ms.bicycle,
    ms.pedestrian,
    lr.cty_code,
    lr.lr_id,
    (ST_LineLocatePoint(lr.geometry, ms.geom) * (lr.cum_offset_end - lr.cum_offset_bgn)) + lr.cum_offset_bgn AS cum_offset
  FROM
    missing_crashes ms
  JOIN (
    SELECT
      *
    FROM
      input.padot_localroads
    WHERE
      lr_id IS NOT NULL) lr
  ON
    ST_DWITHIN(ms.geom, lr.geometry, 10)
  ORDER BY
    ms.geom,
    ST_DISTANCE(ms.geom, lr.geometry)),
  hin AS (
  SELECT
    s.lr_id,
    s.window_from,
    s.window_to,
    COUNT(c.crn) AS crashcount
  FROM
    output.pa_lr_slidingwindow s
  LEFT JOIN
    pa_lr_bp_only c
  ON
    c.lr_id = s.lr_id
    AND c.cum_offset >= s.window_from
    AND c.cum_offset <= s.window_to
  GROUP BY
    s.lr_id,
    s.window_from,
    s.window_to
  HAVING
    COUNT(c.crn) >= :crashcount
  ORDER BY
    s.lr_id,
    s.window_from),
  find_breaks AS (
  SELECT
    hin.lr_id,
    hin.window_from,
    hin.window_to,
    hin.crashcount,
    CASE
      WHEN (hin.window_from - lag (hin.window_to, 1, 0) OVER 
      (PARTITION BY hin.lr_id ORDER BY hin.lr_id, hin.window_from)) > (round(:gap,0) * 5280) THEN 1 -- threshold distance to other segments 2500ft
    ELSE 0
    END AS break
  FROM
    hin),
  agg_segs AS (
    SELECT
      grouped.lr_id,
      MIN(grouped.window_from) AS window_from,
      MAX(grouped.window_to) AS window_to
    FROM
      (
        SELECT
          find_breaks.lr_id,
          find_breaks.window_from,
          find_breaks.window_to,
          find_breaks.crashcount,
          find_breaks.break,
          SUM(find_breaks.break) OVER (
            ORDER BY
              find_breaks.lr_id,
              find_breaks.window_from
          ) AS grp
        FROM
          find_breaks
      ) grouped
    GROUP BY
      grouped.lr_id,
      grouped.grp
    ORDER BY
      grouped.lr_id,
      (MIN(grouped.window_from))
  )
SELECT
  ROW_NUMBER() OVER (ORDER BY a.lr_id, a.window_from ) AS hin_id,
  a.lr_id,
  a.window_from,
  a.window_to,
  COUNT(c.crn) AS crashcount,
  SUM(c.fatal_count) as total_killed,
  SUM(c.maj_inj_count) as total_maj_inj
FROM
  agg_segs a
LEFT JOIN
  pa_lr_bp_only c
ON
  c.lr_id = a.lr_id
  AND c.cum_offset >= a.window_from
  AND c.cum_offset <= a.window_to
GROUP BY
  a.lr_id,
  a.window_from,
  a.window_to;
COMMIT;
/*
PA HSNS Overlap Stat
 */
-- Coverage of .5 KSI 2+ HIN on PennDOT HSNS (example)
BEGIN;
CREATE OR REPLACE VIEW
 output.percent_hin_on_hsns AS 
WITH hsns_segs AS (
SELECT
	*
FROM
	input.pa_hsns_urban_segs
WHERE 
  cty_code != '67'
UNION
SELECT
	*
FROM
	input.pa_hsns_rural_segs
WHERE 
  cty_code != '67'),
bgn_offset AS (
SELECT
	hsns.*,
	CASE
		WHEN right(hsns.seg_bgn, 1)::int % 2 = 0 THEN 1
		ELSE 2
	END AS side_ind,
	rms.cum_offset + hsns.offset_bgn AS cum_offset_bgn
FROM
	hsns_segs hsns
JOIN 
  input.padot_rms rms 
  ON
	hsns.cty_code = rms.cty_code AND hsns.st_rt_no = rms.st_rt_no AND hsns.seg_bgn = rms.seg_no
),
hsns AS (
SELECT
	concat(hsns.cty_code, hsns.st_rt_no, hsns.side_ind::TEXT) AS id,
	hsns.seg_bgn,
	hsns.seg_end,
	hsns.hsns_id,
	cum_offset_bgn,
	rms.cum_offset + hsns.offset_end AS cum_offset_end
FROM
	bgn_offset hsns
JOIN
  input.padot_rms rms 
  ON
	hsns.cty_code = rms.cty_code AND hsns.st_rt_no = rms.st_rt_no AND hsns.seg_end = rms.seg_no),
calcs AS (
SELECT
	hsns.id,
	hsns.hsns_id,
	hsns.cum_offset_bgn,
	hsns.cum_offset_end,
	hin.hin_id,
	hin.window_from,
	hin.window_to,
	CASE
		WHEN hsns.cum_offset_bgn >= hin.window_from AND hsns.cum_offset_end <= hin.window_to THEN 1
		WHEN hin.window_from <= hsns.cum_offset_end AND hin.window_from >= hsns.cum_offset_bgn THEN (hsns.cum_offset_end-hin.window_from)/(hsns.cum_offset_end-hsns.cum_offset_bgn)
		WHEN hsns.cum_offset_bgn BETWEEN hin.window_from and hin.window_to THEN (hin.window_to-hsns.cum_offset_bgn)/(hsns.cum_offset_end-hsns.cum_offset_bgn)
			ELSE 0
	END AS overlap_percentage
FROM
	hsns
JOIN 
  output.pa_ksi_hin hin
ON
	hsns.id = hin.id
WHERE 
  hin.id not like '67%')
SELECT
	hin_overlap_ft/total_ft AS percent_hin_coverage_hsns
FROM
	(
	SELECT
		(
		SELECT
			SUM((calcs.cum_offset_end - calcs.cum_offset_bgn) * calcs.overlap_percentage) AS hin_overlap_ft
		FROM
			calcs
		WHERE
			calcs.overlap_percentage > 0
        ) AS hin_overlap_ft,
		(
		SELECT
			sum(cum_offset_end-cum_offset_bgn)
		FROM
			hsns
        ) AS total_ft
) AS stats;
COMMIT;
/*
NJ HSIP Overlap Stat
 */
-- Coverage of .5 KSI 2+ HIN on NJ HSIP Corridor (example)
BEGIN;
CREATE OR REPLACE VIEW 
  output.percent_hin_on_hsip_corridor AS 
WITH hsip AS (
  SELECT 
    sri, 
    round(milepostfrom::numeric,2) as milepostfrom, 
    round(milepostto::numeric,2) as milepostto 
  FROM 
    input.nj_hsip_corridor
),
calcs as (
SELECT
	hsip.sri,
	hsip.milepostfrom,
	hsip.milepostto,
	hin.hin_id,
	hin.window_from,
	hin.window_to,
	CASE
		WHEN hsip.milepostfrom >= hin.window_from AND hsip.milepostto <= hin.window_to THEN 1
		WHEN hin.window_from <= hsip.milepostto AND hin.window_from >= hsip.milepostfrom THEN (hsip.milepostto-hin.window_from)/(hsip.milepostto-hsip.milepostfrom)
		WHEN hsip.milepostfrom BETWEEN hin.window_from and hin.window_to THEN (hin.window_to-hsip.milepostfrom)/(hsip.milepostto-hsip.milepostfrom)
			ELSE 0
	END AS overlap_percentage
FROM
	hsip
JOIN 
  output.nj_ksi_hin hin 
ON
	hsip.sri = hin.sri)
SELECT
	hin_overlap_mi/total_mi AS percent_hin_coverage_hsip
FROM
	(
	SELECT
		(
		SELECT
			SUM((calcs.milepostto - calcs.milepostfrom) * calcs.overlap_percentage) AS hin_overlap_mi
		FROM
			calcs
		WHERE
			calcs.overlap_percentage > 0
        ) AS hin_overlap_mi,
		(
		SELECT
			sum(milepostto::numeric-milepostfrom::numeric)
		FROM
			input.nj_hsip_corridor
        ) AS total_mi
) AS stats;
COMMIT;