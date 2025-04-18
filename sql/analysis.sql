/*
Add m values to NJDOT LRS table
 */
CREATE MATERIALIZED VIEW input.njdot_lrs_m AS 
SELECT
      lrs.sri,
      lrs.mp_start,
      lrs.mp_end,
      lrs.route_subt AS route_subtype,
      -- Create a LineStringM on the fly from the stored geometry and M values
      -- This assumes you have stored M values as an array or can extract them somehow
      CASE
        -- If you stored M values as a text field that can be parsed as an array
        WHEN lrs.m_values IS NOT NULL THEN
          (SELECT 
            ST_SetSRID(
              ST_MakeLine(
                ARRAY(
                  SELECT ST_MakePointM(ST_X(geom), ST_Y(geom), m)
                  FROM (
                    SELECT (ST_DumpPoints(lrs.geometry)).geom, 
                           unnest(string_to_array(trim(both '[]' from lrs.m_values), ',')::float[]) as m
                  ) AS points
                )
              ),
              ST_SRID(lrs.geometry)
            )
          )-- If you don't have M values stored, use segment's own MP values to interpolate
        ELSE 
          ST_AddMeasure(
            lrs.geometry, 
            lrs.mp_start::float, 
            lrs.mp_end::float
          )
      END AS geom
   FROM input.njdot_lrs lrs;
COMMIT;
BEGIN;
CREATE INDEX idx_njdot_lrs_m_geom ON input.njdot_lrs_m USING GIST (geom);
COMMIT;
/*
query nj crash data for KSI and bike/ped
 */
BEGIN;
CREATE MATERIALIZED VIEW 
  input.nj_crashes AS
WITH
-- adds sri/mp to locations with just a lat/long snapping to closest road within 10m
nj_lat_long AS (
  SELECT
    cnj.casenumber,
    ST_Transform(ST_SetSRID(ST_MakePoint(
      CAST(NULLIF(longitude, '') AS numeric) * -1, 
      CAST(NULLIF(latitude, '') AS numeric)
    ), 4326), 26918) AS geom
  FROM
    input.crash_newjersey cnj
  WHERE
    (sri_std_rte_identifier IS NULL OR milepost IS NULL)
    AND (latitude IS NOT NULL AND longitude IS NOT NULL)
),
-- Find closest LRS point and get the M value directly
append_missing AS (
  SELECT DISTINCT ON (nj.geom) 
    nj.casenumber,
    lrs.sri,
    ST_LineInterpolatePoint(lrs.geom, ST_LineLocatePoint(lrs.geom, nj.geom)) AS snap_point,
    ST_M(ST_LineInterpolatePoint(lrs.geom, ST_LineLocatePoint(lrs.geom, nj.geom))) AS mp
  FROM
    nj_lat_long nj
    -- Join with the LRS table that has M values
    JOIN input.njdot_lrs_m lrs ON ST_DWithin(nj.geom, lrs.geom, 10)
  WHERE
    -- Ensure the geometry has M values
    ST_HasM(lrs.geom)
  ORDER BY
    nj.geom,
    ST_Distance(nj.geom, lrs.geom)
),
-- Fallback for cases where M values aren't available
append_missing_fallback AS (
  SELECT DISTINCT ON (nj.geom) 
    nj.casenumber,
    lrs.sri,
    -- Fall back to calculating MP based on relative position
    (ST_LineLocatePoint(lrs.geometry, nj.geom) * (lrs.mp_end - lrs.mp_start)) + lrs.mp_start AS mp
  FROM
    nj_lat_long nj
    JOIN input.njdot_lrs lrs ON ST_DWithin(nj.geom, lrs.geometry, 10)
    -- Only get cases not handled by the M value approach
    LEFT JOIN append_missing am ON nj.casenumber = am.casenumber
    WHERE am.casenumber IS NULL
  ORDER BY
    nj.geom,
    ST_Distance(nj.geom, lrs.geometry)
),
-- Combine both approaches
all_missing AS (
  SELECT casenumber, sri, mp FROM append_missing
  UNION ALL
  SELECT casenumber, sri, mp FROM append_missing_fallback
)
SELECT
  njc.*,
  COALESCE(a.sri, njc.sri_std_rte_identifier) AS sri,
  COALESCE(a.mp, CAST(njc.milepost AS numeric)) AS mp
FROM
  input.crash_newjersey njc
  LEFT JOIN all_missing a ON njc.casenumber = a.casenumber
WHERE
  CAST(RIGHT(njc.crash_date,4) AS INTEGER) >= :start_year 
  AND CAST(RIGHT(njc.crash_date,4) AS INTEGER) <= (:start_year + 4);
COMMIT;
/*
creating nj .5 mile sliding window segments and summarizing crash data for those windows
 */
BEGIN;
CREATE OR REPLACE VIEW input.nj_slidingwindow AS
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
  AND (class IS NULL OR class != 'Limited Access') AND (route_subtype IS NULL OR route_subtype NOT IN (1,4)) -- removes limited access roads
ORDER BY
  sri,
  window_from ASC;
COMMIT;
-- KSI 2+ .5 mile HIN
BEGIN;
CREATE OR REPLACE VIEW output.nj_ksi_hin as
WITH
  nj_ksi_only AS (
    SELECT c.* 
    FROM input.nj_crashes c
    INNER JOIN (
      -- Occupant with KSI
      SELECT 
          casenumber
      FROM (
          SELECT
              casenumber,
              MAX(CASE WHEN physical_condition = '01' THEN 1 ELSE 0 END) AS has_fatal,
              MAX(CASE WHEN physical_condition = '02' THEN 1 ELSE 0 END) AS has_major
          FROM input.crash_nj_occupant
          GROUP BY casenumber
      ) occ
      WHERE has_fatal = 1 OR has_major = 1
      UNION
      -- Pedestrian with KSI
      SELECT 
          casenumber
      FROM (
          SELECT
              casenumber,
              MAX(CASE WHEN physical_condition = '01' THEN 1 ELSE 0 END) AS has_fatal,
              MAX(CASE WHEN physical_condition = '02' THEN 1 ELSE 0 END) AS has_major
          FROM input.crash_nj_pedestrian
          GROUP BY casenumber
      ) ped
      WHERE has_fatal = 1 OR has_major = 1
    ) ksi ON ksi.casenumber = c.casenumber
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
      input.nj_slidingwindow s
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
        ) > :gap THEN 1 -- threshold distance to other segments 2500ft (0.47343 miles)
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
  ),
  -- Base HIN segments without geometry
  base_hin AS (
    SELECT  -- joins ksi crashes back to aggregated segments
      ROW_NUMBER() OVER (
        ORDER BY
          a.sri,
          a.window_from
      ) AS hin_id,
      a.sri,
      a.window_from,
      a.window_to,
      COUNT(c.casenumber) AS ksi_count,
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
      lrs.route_subtype
  ),
  -- Extract geometry for each HIN segment using the existing M geometry
  hin_geometries AS (
    SELECT
      h.hin_id,
      h.sri,
      h.window_from,
      h.window_to,
      h.ksi_count,
      h.class,
      h.route_subtype,
      -- Extract the segment between the two measure values
      ST_LocateBetween(lrs.geom, h.window_from, h.window_to) AS geometry_part
    FROM
      base_hin h
    JOIN 
      input.njdot_lrs_m lrs ON h.sri = lrs.sri
      -- Only get LRS segments that overlap with our HIN window
      AND h.window_from < lrs.mp_end
      AND h.window_to > lrs.mp_start
  ),
  -- Combine all geometry parts for each HIN segment
  combined_geometries AS (
    SELECT
      hin_id,
      sri,
      window_from,
      window_to,
      ksi_count,
      class,
      route_subtype,
      -- Combine all geometry parts for each HIN segment
      ST_Union(geometry_part) AS geometry
    FROM
      hin_geometries
    GROUP BY
      hin_id,
      sri,
      window_from,
      window_to,
      ksi_count,
      class,
      route_subtype
  )
-- Final result with geometry
SELECT
  h.hin_id,
  h.sri,
  h.window_from,
  h.window_to,
  h.ksi_count,
  h.class,
  h.route_subtype,
  COALESCE(cg.geometry, NULL) AS geometry
FROM
  base_hin h
LEFT JOIN
  combined_geometries cg ON h.hin_id = cg.hin_id
ORDER BY
  h.hin_id;
COMMIT;
BEGIN;
-- Bike/Ped 2+ .5 HIN
CREATE OR REPLACE VIEW output.nj_bp_hin AS
WITH
  nj_bp_only AS (
    SELECT
      *
    FROM
      input.nj_crashes
    WHERE
      crash_type_code IN ('13','14')
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
      input.nj_slidingwindow s
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
      COUNT(c.casenumber) >= 2
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
        ) > :gap THEN 1 -- threshold distance to other segments 2500ft (0.47343 miles)
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
  ),
  -- Base HIN segments without geometry
  base_hin AS (
    SELECT  -- joins ksi crashes back to aggregated segments
      ROW_NUMBER() OVER (
        ORDER BY
          a.sri,
          a.window_from
      ) AS hin_id,
      a.sri,
      a.window_from,
      a.window_to,
      COUNT(c.casenumber) AS bp_count,
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
      lrs.route_subtype
  ),
  -- Extract geometry for each HIN segment using the existing M geometry
  hin_geometries AS (
    SELECT
      h.hin_id,
      h.sri,
      h.window_from,
      h.window_to,
      h.bp_count,
      h.class,
      h.route_subtype,
      -- Extract the segment between the two measure values
      ST_LocateBetween(lrs.geom, h.window_from, h.window_to) AS geometry_part
    FROM
      base_hin h
    JOIN 
      input.njdot_lrs_m lrs ON h.sri = lrs.sri
      -- Only get LRS segments that overlap with our HIN window
      AND h.window_from < lrs.mp_end
      AND h.window_to > lrs.mp_start
  ),
  -- Combine all geometry parts for each HIN segment
  combined_geometries AS (
    SELECT
      hin_id,
      sri,
      window_from,
      window_to,
      bp_count,
      class,
      route_subtype,
      -- Combine all geometry parts for each HIN segment
      ST_Union(geometry_part) AS geometry
    FROM
      hin_geometries
    GROUP BY
      hin_id,
      sri,
      window_from,
      window_to,
      bp_count,
      class,
      route_subtype
  )
-- Final result with geometry
SELECT
  h.hin_id,
  h.sri,
  h.window_from,
  h.window_to,
  h.bp_count,
  h.class,
  h.route_subtype,
  COALESCE(cg.geometry, NULL) AS geometry
FROM
  base_hin h
LEFT JOIN
  combined_geometries cg ON h.hin_id = cg.hin_id
ORDER BY
  h.hin_id;
COMMIT;
/*
prepare the pa crash data
 */
BEGIN;
CREATE MATERIALIZED VIEW input.pa_crashes AS
WITH
-- RMS crashes section
pa_crash_filter AS (
    SELECT
        cpa.geometry AS geom,
        cpa.crn,
        rpa.adj_rdwy_seq,
        CASE
            WHEN LENGTH(rpa.rdwy_county::TEXT) = 1 THEN '0'::TEXT || rpa.rdwy_county::TEXT
            ELSE rpa.rdwy_county::TEXT
        END AS county,
        rpa.route,
        CASE
            WHEN LENGTH(rpa.segment) = 1 THEN '000'::TEXT || rpa.segment
            WHEN LENGTH(rpa.segment) = 2 THEN '00'::TEXT || rpa.segment
            WHEN LENGTH(rpa.segment) = 3 THEN '0'::TEXT || rpa.segment
            ELSE rpa.segment
        END AS segment,
        rpa.offset_ AS offset
    FROM
        INPUT.crash_pennsylvania cpa
    JOIN INPUT.crash_pa_roadway rpa ON rpa.crn = cpa.crn
    WHERE
        (crash_year BETWEEN :start_year AND :start_year + 4) -- crash year range
        AND rpa.adj_rdwy_seq = 3
        AND cpa.county != '67' -- exclude Philadelphia County
),
pa_crashes_cleaned AS (
    SELECT
        c.*,
        CONCAT(c.county, c.route, pr.side_ind) AS id
    FROM
        pa_crash_filter c
    LEFT JOIN INPUT.padot_rms pr ON c.county = pr.cty_code
        AND c.route = pr.st_rt_no
        AND c.segment = pr.seg_no
),
-- format rms with lrs id
rms AS (
    SELECT
        concat(cty_code, st_rt_no, side_ind) AS id,
        seg_no,
        cum_offset_bgn_t1,
        cum_offset_end_t1
    FROM
        input.padot_rms
    WHERE 
        cty_code != '67' -- exclude Philadelphia County
    ORDER BY
        (concat(cty_code, st_rt_no, side_ind)),
        cum_offset_bgn_t1
),
-- add lrs info to the crash data that includes roadway info
add_lrs_info AS (
    SELECT
        pc.geom,
        pc.crn,
        pc.id,
        pc.offset + rms.cum_offset_bgn_t1 AS cumulative_offset,
        'rms' AS source_type
    FROM
        pa_crashes_cleaned pc
    JOIN rms ON rms.id = pc.id and rms.seg_no = pc.segment
    WHERE
        pc.route IS NOT NULL AND pc.segment IS NOT NULL AND pc.offset IS NOT NULL
),
-- create pa lrs with m value
create_pa_lrs AS (
    SELECT
        concat(cty_code, st_rt_no, side_ind) AS id,
        cum_offset_bgn_t1,
        cum_offset_end_t1,
        ST_AddMeasure(ST_MakeLine(geometry), cum_offset_bgn_t1, cum_offset_end_t1) AS geom
    FROM
        input.padot_rms
    WHERE
        geometry IS NOT NULL
        AND cty_code != '67' -- exclude Philadelphia County
    GROUP BY
        concat(cty_code, st_rt_no, side_ind),
        cum_offset_bgn_t1,
        cum_offset_end_t1
),
-- add lrs info to the crash data that didn't include roadway info, crashes w/in 10 meters give it LRS id and measure
add_missing_measure AS (
    SELECT DISTINCT ON (pc.geom)
        pc.geom,
        pc.crn,
        pl.id,
        (ST_LineLocatePoint(pl.geom, pc.geom) * (pl.cum_offset_end_t1 - cum_offset_bgn_t1)) + cum_offset_bgn_t1 AS cumulative_offset,
        'rms' AS source_type
    FROM
        pa_crashes_cleaned pc
    JOIN create_pa_lrs pl ON ST_DWithin(pc.geom, pl.geom, 10)
    WHERE
        pc.route IS NULL OR pc.segment IS NULL OR pc.offset IS NULL
    ORDER BY
        pc.geom, ST_Distance(pc.geom, pl.geom)
),
-- combine both RMS crash tables
rms_crashes AS (
    SELECT geom, crn, id, cumulative_offset, source_type FROM add_missing_measure
    UNION
    SELECT geom, crn, id, cumulative_offset, source_type FROM add_lrs_info
),
-- get crashes missing from RMS mapping for local roads
missing_crashes AS (
    SELECT
        cp.geometry AS geom,
        cp.crn
    FROM
        INPUT.crash_pennsylvania cp
    LEFT JOIN rms_crashes rc ON cp.crn = rc.crn
    WHERE
        rc.crn IS NULL
        AND (crash_year BETWEEN :start_year AND :start_year + 4)
        AND st_isempty(cp.geometry) IS false
        AND cp.county != '67' -- exclude Philadelphia County
),
-- local roads crash processing
local_roads_crashes AS (
    SELECT DISTINCT ON (ms.geom)
        ms.geom,
        ms.crn,
        lr.cty_code AS county,
        lr.lr_id AS id,
        st_linelocatepoint(lr.geometry, ms.geom) * (lr.cum_offset_end - lr.cum_offset_bgn)::DOUBLE PRECISION + 
        lr.cum_offset_bgn::DOUBLE PRECISION AS cumulative_offset,
        'local' AS source_type
    FROM
        missing_crashes ms
    JOIN (
        SELECT
            lr.geometry,
            lr.cty_code,
            lr.lr_id,
            lr.segment_number,
            lr.cum_offset_bgn,
            lr.cum_offset_end
        FROM
            INPUT.padot_localroads lr
        WHERE
            lr.lr_id IS NOT NULL
            AND lr.cty_code != '67' -- exclude Philadelphia County
    ) lr ON st_dwithin(ms.geom, lr.geometry, 10::DOUBLE PRECISION)
    ORDER BY
        ms.geom,
        (st_distance(ms.geom, lr.geometry))
)
-- combine RMS and local roads crashes
SELECT 
    geom,
    crn,
    id,
    cumulative_offset,
    source_type
FROM 
    rms_crashes
UNION ALL
SELECT 
    geom,
    crn,
    id::text,
    cumulative_offset,
    source_type
FROM 
    local_roads_crashes;
COMMIT;
/*
creating pa .5 mile sliding window segments and summarizing crash data for those windows
 */
BEGIN;
CREATE OR REPLACE VIEW input.pa_long_segs as
WITH 
  -- RMS segments
  rms_cleaned AS (
    SELECT
      concat(rms.cty_code, rms.st_rt_no, rms.side_ind) AS id,
      0 AS b_offset,
      rms.seg_lngth_feet AS e_offset,
      rms.cum_offset_bgn_t1 AS tot_measure_b,
      rms.cum_offset_end_t1 AS tot_measure_e,
      'rms' AS road_type
    FROM
      input.padot_rms rms
    WHERE 
        cty_code != '67' or access_ctrl != '1' -- exclude Philadelphia County and limited access roads
    ORDER BY
      (concat(rms.cty_code, rms.st_rt_no, rms.seg_no)),
      rms.side_ind
  ),
  rms_long_segs AS (
    SELECT
      r.id,
      MIN(r.tot_measure_b) AS b_feet,
      MAX(r.tot_measure_e) AS e_feet,
      r.road_type
    FROM
      rms_cleaned r
    GROUP BY
      r.id,
      r.road_type
  ),
  -- Local road segments
  lr_long_segs AS (
    SELECT
      lr_id::text AS id,
      MIN(cum_offset_bgn)::numeric AS b_feet,
      MAX(cum_offset_end)::numeric AS e_feet,
      'local' AS road_type
    FROM
      input.padot_localroads
    WHERE 
      cty_code != '67' -- exclude Philadelphia County
    GROUP BY
      lr_id
  )
  -- combine RMS and local road segments
    SELECT * FROM rms_long_segs
    UNION ALL
    SELECT * FROM lr_long_segs;
COMMIT;
-- create pa sliding window
BEGIN;
CREATE VIEW input.pa_slidingwindow AS
  -- Generate sliding windows recursively
  with RECURSIVE windows AS (
    SELECT
      r.id,
      r.b_feet AS road_ft_start,
      r.e_feet AS road_ft_end,
      r.b_feet AS window_from,
      LEAST(r.b_feet + (5280 * (:windowsize + :window_increment)), r.e_feet) AS window_to,
      r.road_type
    FROM
      input.pa_long_segs r
    WHERE
      r.id IS NOT NULL
    UNION ALL
    SELECT
      w.id,
      w.road_ft_start,
      w.road_ft_end,
      w.window_from + (5280 * :window_increment) AS window_from,
      LEAST(w.window_from + (5280 * :windowsize), w.road_ft_end) AS window_to,
      w.road_type
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
  windows.road_type
FROM
  windows
WHERE
  windows.window_from < windows.road_ft_end
ORDER BY
  windows.road_type,
  windows.id,
  windows.window_from;
COMMIT;
-- PA KSI 2+ .5 mile HIN
BEGIN;
CREATE OR REPLACE VIEW output.pa_ksi_hin as
WITH
  pa_ksi_only AS (
    SELECT
      c.*
    FROM
      input.pa_crashes c
    LEFT JOIN input.crash_pa_flag cf ON c.crn = cf.crn
    WHERE
      cf.fatal_or_susp_serious_inj = 1  
  ),
  hin AS (
    SELECT
      s.id,
      s.window_from,
      s.window_to,
      s.road_type 
    FROM
      input.pa_slidingwindow s
      LEFT JOIN pa_ksi_only c ON c.id = s.id
      AND c.cumulative_offset >= s.window_from
      AND c.cumulative_offset <= s.window_to
      AND c.source_type = s.road_type
    GROUP BY
      s.id,
      s.window_from,
      s.window_to,
      s.road_type
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
      hin.road_type,
      CASE
        WHEN (
          hin.window_from - lag (hin.window_to, 1, 0) OVER (
            PARTITION BY
              hin.id
            ORDER BY
              hin.id,
              hin.window_from,
              hin.road_type
          )
        ) > (:gap * 5280) THEN 1 -- threshold distance to other segments 2500ft
        ELSE 0
      END AS break
    FROM
      hin
  ),
  agg_segs AS ( -- aggregates overlapping segments
    SELECT
      grouped.id,
      MIN(grouped.window_from) AS window_from,
      MAX(grouped.window_to) AS window_to,
      grouped.road_type
    FROM
      (
        SELECT
          find_breaks.id,
          find_breaks.window_from,
          find_breaks.window_to,
          find_breaks.road_type,
          find_breaks.break,
          SUM(find_breaks.break) OVER (
            ORDER BY
              find_breaks.id,
              find_breaks.window_from,
              find_breaks.road_type
          ) AS grp
        FROM
          find_breaks
      ) grouped
    GROUP BY
      grouped.id,
      grouped.grp,
      grouped.road_type
    ORDER BY
      grouped.id,
      (MIN(grouped.window_from))
  ),
  -- Create base HIN segments without geometry
  base_hin AS (
    SELECT
      ROW_NUMBER() OVER (
        ORDER BY
          a.id,
          a.window_from
      ) AS hin_id,
      a.id,
      a.window_from,
      a.window_to,
      a.road_type,
      COUNT(c.crn) AS ksi_count
    FROM
      agg_segs a
      LEFT JOIN pa_ksi_only c ON c.id = a.id 
        AND c.cumulative_offset >= a.window_from 
        AND c.cumulative_offset <= a.window_to 
        AND c.source_type = a.road_type
      LEFT JOIN input.pa_long_segs segs ON segs.id = a.id 
        AND a.window_from >= segs.b_feet 
        AND a.window_to <= segs.e_feet
    GROUP BY
      a.id,
      a.window_from,
      a.window_to,
      a.road_type
  ),
  -- Extract geometries for RMS roads
  rms_geoms AS (
    SELECT
      h.hin_id,
      h.id,
      h.window_from,
      h.window_to, 
      h.road_type,
      h.ksi_count,
      -- Extract the portion of the line between the two measure values
      ST_LineSubstring(
        rms.geom,
        LEAST(1, GREATEST(0, (h.window_from - rms.cum_offset_bgn_t1) / NULLIF(rms.cum_offset_end_t1 - rms.cum_offset_bgn_t1, 0))), 
        LEAST(1, GREATEST(0, (h.window_to - rms.cum_offset_bgn_t1) / NULLIF(rms.cum_offset_end_t1 - rms.cum_offset_bgn_t1, 0)))
      ) AS geometry
    FROM
      base_hin h
    JOIN (
      -- Create a combined geometry for each route ID with measures
      SELECT
        concat(cty_code, st_rt_no, side_ind) AS id,
        cum_offset_bgn_t1,
        cum_offset_end_t1,
        ST_AddMeasure(ST_MakeLine(geometry), cum_offset_bgn_t1, cum_offset_end_t1) AS geom
      FROM
        input.padot_rms
      WHERE
        geometry IS NOT NULL
        AND cty_code != '67' -- exclude Philadelphia County
      GROUP BY
        concat(cty_code, st_rt_no, side_ind),
        cum_offset_bgn_t1,
        cum_offset_end_t1
    ) rms ON h.id = rms.id
      -- Only get RMS segments that overlap our HIN window
      AND h.window_from < rms.cum_offset_end_t1 
      AND h.window_to > rms.cum_offset_bgn_t1
      AND h.road_type = 'rms'
  ),
  -- Extract geometries for local roads
  local_geoms AS (
    SELECT
      h.hin_id,
      h.id,
      h.window_from,
      h.window_to,
      h.road_type,
      h.ksi_count,
      -- Extract the portion of the line between the two measure values
      ST_LineSubstring(
        lr.geometry,
        LEAST(1, GREATEST(0, (h.window_from - lr.cum_offset_bgn) / NULLIF(lr.cum_offset_end - lr.cum_offset_bgn, 0))),
        LEAST(1, GREATEST(0, (h.window_to - lr.cum_offset_bgn) / NULLIF(lr.cum_offset_end - lr.cum_offset_bgn, 0)))
      ) AS geometry
    FROM
      base_hin h
    JOIN input.padot_localroads lr ON h.id = lr.lr_id::text
      -- Only get local road segments that overlap our HIN window
      AND h.window_from < lr.cum_offset_end
      AND h.window_to > lr.cum_offset_bgn
      AND h.road_type = 'local'
  ),
  -- Combine all geometries
  segment_geoms AS (
    -- Union all segments for each HIN ID
    SELECT
      hin_id,
      id,
      window_from,
      window_to,
      road_type,
      ksi_count,
      ST_Union(geometry) AS geometry
    FROM (
      SELECT * FROM rms_geoms
      UNION ALL
      SELECT * FROM local_geoms
    ) all_geoms
    GROUP BY
      hin_id,
      id,
      window_from,
      window_to,
      road_type,
      ksi_count
  )
-- Final output with all HIN segments
SELECT
  h.hin_id,
  h.id,
  h.window_from,
  h.window_to,
  h.road_type,
  h.ksi_count,
  COALESCE(sg.geometry, NULL) AS geometry
FROM
  base_hin h
LEFT JOIN segment_geoms sg ON h.hin_id = sg.hin_id
ORDER BY
  h.hin_id;
COMMIT;
-- Bike/Ped 2+ .5 mile HIN
BEGIN;
CREATE OR REPLACE VIEW output.pa_bp_hin AS
WITH
  pa_bp_only AS (
    SELECT
      c.*
    FROM
      input.pa_crashes c
    LEFT JOIN input.crash_pa_flag cf ON c.crn = cf.crn
    WHERE
      cf.bicycle = 1 or cf.pedestrian = 1
  ),
  hin AS (
    SELECT
      s.id,
      s.window_from,
      s.window_to,
      s.road_type 
    FROM
      input.pa_slidingwindow s
      LEFT JOIN pa_bp_only c ON c.id = s.id
      AND c.cumulative_offset >= s.window_from
      AND c.cumulative_offset <= s.window_to
      AND c.source_type = s.road_type
    GROUP BY
      s.id,
      s.window_from,
      s.window_to,
      s.road_type
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
      hin.road_type,
      CASE
        WHEN (
          hin.window_from - lag (hin.window_to, 1, 0) OVER (
            PARTITION BY
              hin.id
            ORDER BY
              hin.id,
              hin.window_from,
              hin.road_type
          )
        ) > (:gap * 5280) THEN 1 -- threshold distance to other segments 2500ft
        ELSE 0
      END AS break
    FROM
      hin
  ),
  agg_segs AS ( -- aggregates overlapping segments
    SELECT
      grouped.id,
      MIN(grouped.window_from) AS window_from,
      MAX(grouped.window_to) AS window_to,
      grouped.road_type
    FROM
      (
        SELECT
          find_breaks.id,
          find_breaks.window_from,
          find_breaks.window_to,
          find_breaks.road_type,
          find_breaks.break,
          SUM(find_breaks.break) OVER (
            ORDER BY
              find_breaks.id,
              find_breaks.window_from,
              find_breaks.road_type
          ) AS grp
        FROM
          find_breaks
      ) grouped
    GROUP BY
      grouped.id,
      grouped.grp,
      grouped.road_type
    ORDER BY
      grouped.id,
      (MIN(grouped.window_from))
  ),
  -- Create base HIN segments without geometry
  base_hin AS (
    SELECT
      ROW_NUMBER() OVER (
        ORDER BY
          a.id,
          a.window_from
      ) AS hin_id,
      a.id,
      a.window_from,
      a.window_to,
      a.road_type,
      COUNT(c.crn) AS bp_count
    FROM
      agg_segs a
      LEFT JOIN pa_bp_only c ON c.id = a.id 
        AND c.cumulative_offset >= a.window_from 
        AND c.cumulative_offset <= a.window_to 
        AND c.source_type = a.road_type
      LEFT JOIN input.pa_long_segs segs ON segs.id = a.id 
        AND a.window_from >= segs.b_feet 
        AND a.window_to <= segs.e_feet
    GROUP BY
      a.id,
      a.window_from,
      a.window_to,
      a.road_type
  ),
  -- Extract geometries for RMS roads
  rms_geoms AS (
    SELECT
      h.hin_id,
      h.id,
      h.window_from,
      h.window_to, 
      h.road_type,
      h.bp_count,
      -- Extract the portion of the line between the two measure values
      ST_LineSubstring(
        rms.geom,
        LEAST(1, GREATEST(0, (h.window_from - rms.cum_offset_bgn_t1) / NULLIF(rms.cum_offset_end_t1 - rms.cum_offset_bgn_t1, 0))), 
        LEAST(1, GREATEST(0, (h.window_to - rms.cum_offset_bgn_t1) / NULLIF(rms.cum_offset_end_t1 - rms.cum_offset_bgn_t1, 0)))
      ) AS geometry
    FROM
      base_hin h
    JOIN (
      -- Create a combined geometry for each route ID with measures
      SELECT
        concat(cty_code, st_rt_no, side_ind) AS id,
        cum_offset_bgn_t1,
        cum_offset_end_t1,
        ST_AddMeasure(ST_MakeLine(geometry), cum_offset_bgn_t1, cum_offset_end_t1) AS geom
      FROM
        input.padot_rms
      WHERE
        geometry IS NOT NULL
        AND cty_code != '67' -- exclude Philadelphia County
      GROUP BY
        concat(cty_code, st_rt_no, side_ind),
        cum_offset_bgn_t1,
        cum_offset_end_t1
    ) rms ON h.id = rms.id
      -- Only get RMS segments that overlap our HIN window
      AND h.window_from < rms.cum_offset_end_t1 
      AND h.window_to > rms.cum_offset_bgn_t1
      AND h.road_type = 'rms'
  ),
  -- Extract geometries for local roads
  local_geoms AS (
    SELECT
      h.hin_id,
      h.id,
      h.window_from,
      h.window_to,
      h.road_type,
      h.bp_count,
      -- Extract the portion of the line between the two measure values
      ST_LineSubstring(
        lr.geometry,
        LEAST(1, GREATEST(0, (h.window_from - lr.cum_offset_bgn) / NULLIF(lr.cum_offset_end - lr.cum_offset_bgn, 0))),
        LEAST(1, GREATEST(0, (h.window_to - lr.cum_offset_bgn) / NULLIF(lr.cum_offset_end - lr.cum_offset_bgn, 0)))
      ) AS geometry
    FROM
      base_hin h
    JOIN input.padot_localroads lr ON h.id = lr.lr_id::text
      -- Only get local road segments that overlap our HIN window
      AND h.window_from < lr.cum_offset_end
      AND h.window_to > lr.cum_offset_bgn
      AND h.road_type = 'local'
  ),
  -- Combine all geometries
  segment_geoms AS (
    -- Union all segments for each HIN ID
    SELECT
      hin_id,
      id,
      window_from,
      window_to,
      road_type,
      bp_count,
      ST_Union(geometry) AS geometry
    FROM (
      SELECT * FROM rms_geoms
      UNION ALL
      SELECT * FROM local_geoms
    ) all_geoms
    GROUP BY
      hin_id,
      id,
      window_from,
      window_to,
      road_type,
      bp_count
  )
-- Final output with all HIN segments
SELECT
  h.hin_id,
  h.id,
  h.window_from,
  h.window_to,
  h.road_type,
  h.bp_count,
  COALESCE(sg.geometry, NULL) AS geometry
FROM
  base_hin h
LEFT JOIN segment_geoms sg ON h.hin_id = sg.hin_id
ORDER BY
  h.hin_id;
COMMIT;
