-- One off summaries of data (not required to run for RHIN generation)
-----------------------------------------------------
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
  
-----------------------------------------------------
-- NJ intersection count on HIN
with a as (
select
	c.*,
	h.hin_id
from
	output.nj_ksi_bp_crashes c
join output.nj_ksi_hin h on
	c.sri = h.sri
	and (c.mp between h.window_from and h.window_to)
where
	c.fatal_or_maj_inj = 'True'
	and h.class = 'Limited Access'
	or h.route_subtype not in (1, 4)
),
b as (
select
	hin_id,
	count(*) as count
from
	a
where
	a.intersection = 'True'
group by
	hin_id,
	sri,
	mp)
select
	sum(count)
from
	b

-----------------------------------------------------
	-- PA intersection count on HIN
with a as (
	select
		c.*,
		h.hin_id
	from
		output.pa_ksi_bp_crashes c
	join output.pa_ksi_hin h on
		c.id = h.id
		and (c.cum_offset between h.window_from and h.window_to)
	where
		(c.major_injury = 1
			or fatal = 1)
		and h.access_ctr != '1'
),
	b as (
	select
		hin_id,
		count(*) as count
	from
		a
	where
		a.intersection = 1
	group by
		hin_id,
		id,
		cum_offset)
select
	sum(count)
from
	b

	-- PA intersection count on local HIN
with
  missing_crashes as (
	select
		cp.geometry as geom,
		cp.crn,
		cp.crash_year,
		cp.fatal_count,
		cp.maj_inj_count,
		fpa.major_injury,
		fpa.fatal,
		fpa.injury,
		fpa.fatal_or_susp_serious_inj,
		fpa.bicycle,
		fpa.pedestrian,
		fpa.intersection
	from
		input.crash_pennsylvania cp
	join
    input.crash_pa_flag fpa
  on
		fpa.crn = cp.crn
	where
		cp.crn not in (
		-- crashes not mapped with RMS
		select
			crn
		from
			output.pa_ksi_bp_crashes)
		and cp.crash_year between 2018 and 2022
		and (fpa.major_injury = 1
			or fpa.fatal = 1)
		and ST_ISEMPTY(cp.geometry) is false),
	pa_lr_ksi_only as (
	select
		distinct
  on
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
		ms.intersection,
		lr.cty_code,
		lr.lr_id,
		(ST_LineLocatePoint(lr.geometry,
		ms.geom) * (lr.cum_offset_end - lr.cum_offset_bgn)) + lr.cum_offset_bgn as cum_offset
	from
		missing_crashes ms
	join (
		select
			*
		from
			input.padot_localroads
		where
			lr_id is not null) lr
  on
		ST_DWITHIN(ms.geom,
		lr.geometry,
		10)
	order by
		ms.geom,
		ST_DISTANCE(ms.geom,
		lr.geometry)),
	a as (
	select
		c.*,
		h.hin_id
	from
		pa_lr_ksi_only c
	join output.pa_lr_ksi_hin h on
		c.lr_id = h.lr_id
		and (c.cum_offset between h.window_from and h.window_to)),
	b as (
	select
		hin_id,
		count(*) as count
	from
		a
	where
		a.intersection = 1
	group by
		hin_id,
		lr_id,
		cum_offset)
select
	sum(count)
from
	b

-----------------------------------------------------
-- pa intersection with lane count/aadt/urban summary
with a as (
select
		c.*,
		h.hin_id
from
		output.pa_ksi_bp_crashes c
join output.pa_ksi_hin h on
		c.id = h.id
	and (c.cum_offset between h.window_from and h.window_to)
where
		(c.major_injury = 1
		or fatal = 1)
	and c.intersection = 1
	and h.access_ctr != '1'
),
b as(
select
		a.hin_id,
		a.id,
		a.cum_offset,
		count(a.*)
from
		a
group by
		a.hin_id,
	 	a.id,
	 	a.cum_offset), 	
rms as (
select
	concat(cty_code,
	st_rt_no,
	side_ind) as id,
	seg_no,
	cum_offset,
	cum_offs_1,
	case
		when dir_ind != 'B' then lane_cnt * 2
		else lane_cnt
	end as lane_cnt,
	urban_rura,
	case
		when dir_ind != 'B' then cur_aadt * 2
		else cur_aadt
	end as cur_aadt
from
	input.padot_rms
order by
	(concat(cty_code,
	st_rt_no,
	side_ind)),
	cum_offset),
joined as (
select
	b.*,
	rms.seg_no,
	rms.lane_cnt,
	rms.urban_rura,
	rms.cur_aadt
from
	b
left join rms on
	b.id = rms.id
	and (b.cum_offset >= rms.cum_offset
		and b.cum_offset < rms.cum_offs_1))
select
	count(*)
from
	joined
where
	lane_cnt >= 4
	and cur_aadt >= 9000
	--and id not like '67%'

-- nj intersection with lane count/aadt/urban  (requires SLD tables to be loaded into db)
with a as (
select
	c.*,
	h.hin_id
from
	output.nj_ksi_bp_crashes_wtrenton c
join output.nj_ksi_hin_wtrenton h on
	c.sri = h.sri
	and (c.mp between h.window_from and h.window_to)
where
	(c.fatal_or_maj_inj = 'True'
		and c.intersection = 'True')
	and (h.class != 'Limited Access'
		or h.route_subtype not in (1, 4))
),
b as (
select
	sri,
	mp
from
	a
group by
	sri,
	mp),
joined as (
select
	b.*,
	aadt.descr as aadt,
	case
		when h."type" = 1 then l.lane_cnt * 2
		else lane_cnt
	end as lane_cnt,
	u.is_urban
from
	b
left join input.dbo_ln_aadt_flow aadt on
	b.sri = aadt.sri
	and b.mp >= aadt.mp_start
	and b.mp < aadt.mp_end
left join input.dbo_ln_lane_count l on
	b.sri = l.sri
	and b.mp >= l.mp_start
	and b.mp < l.mp_end
left join input.dbo_ln_urban_code u on
	b.sri = u.sri
	and b.mp >= u.mp_start
	and b.mp < u.mp_end
left join input.dbo_ln_highway_type h on
	b.sri = h.sri
	and b.mp >= h.mp_start
	and b.mp < h.mp_end)
	select
	count(*)
from
	joined
where
	lane_cnt >= 4
	and aadt >= 9000