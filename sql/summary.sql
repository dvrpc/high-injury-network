-- Sample summaries of data (not required to run for RHIN generation)
-----------------------------------------------------
-- NJ Limited Access only KSI crash query 
WITH crashes AS (
  SELECT
    c.*,
    cn.countyname
  FROM
    nj_crashes_gis c
  LEFT JOIN input.nj_lrs_access lrs ON
    c.sri = lrs.sri
    AND c.mp BETWEEN lrs.mp_start AND lrs.mp_end
  LEFT JOIN input.crash_newjersey cn ON
    c.casenumber = cn.casenumber
  WHERE
    (class NOT LIKE 'Lim%'
    OR class IS NULL)
    OR (route_subtype NOT IN (1, 4))
)
SELECT
  UPPER(countyname) AS county,
  COUNT(DISTINCT CASE WHEN totalkilled > 0 THEN casenumber END) AS K,
  COUNT(DISTINCT CASE WHEN totalkilled = 0 AND major_injury > 0 THEN casenumber END) AS SI,
  COUNT(DISTINCT CASE WHEN totalkilled > 0 OR major_injury IS NOT NULL THEN casenumber END) AS KSI
FROM crashes
GROUP BY UPPER(countyname)
ORDER BY UPPER(countyname);

-- NJ RHIN miles by county no Limited Access
 WITH
  a AS (
    SELECT
      ST_Intersection (r.geom, c.geometry) AS segmented_geom,
      c.co_name,
      class,
      route_subtype
    FROM
      output.nj_ksi_hin_gis r
      JOIN input.countyboundaries c ON ST_Intersects (r.geom, c.geometry)
  )
SELECT
  SUM(st_length (segmented_geom)) * 0.0006213712 AS miles,
  co_name
FROM
  a
where 
(class NOT LIKE 'Lim%' or class is null)
  OR (route_subtype NOT IN (1, 4))
GROUP BY
  co_name;
  
 -- NJ RHIN crash count by county no Limited Access
 WITH
  a AS (
    SELECT
      ST_Intersection (r.geom, c.geometry) AS segmented_geom,
      c.co_name,
      r.crashcount,
      class,
      route_subtype
    FROM
      output.nj_ksi_hin_gis r
      JOIN input.countyboundaries c ON ST_Intersects (r.geom, c.geometry)
  )
SELECT
  sum(crashcount) as crashcount,
  co_name
FROM
  a
where 
(class NOT LIKE 'Lim%' or class is null)
  OR (route_subtype NOT IN (1, 4))
GROUP BY
  co_name;

-----------------------------------------------------
-- PA Limited Access only KSI crash query
 with crashes as (
select distinct(c.crn) as id, c.* from output.pa_crashes_gis c LEFT JOIN input.padot_rms pr 
    ON c.county = pr.cty_code 
    AND c.route = pr.st_rt_no 
    AND c.segment = pr.seg_no
WHERE (access_ctr != '1' OR access_ctr IS NULL)
)
SELECT
county,
COUNT(distinct CASE WHEN fatal = 1 THEN crn END) as K,
COUNT(distinct CASE WHEN fatal = 0 AND major_injury = 1 THEN crn END) as SI,
COUNT(distinct case when fatal = 1 or major_injury = 1 then crn END) as KSI
FROM crashes c
GROUP BY county;

-- PA RMS road RHIN miles no Limited Access
WITH
  a AS (
    SELECT DISTINCT
      (hin_id),
      id,
      window_to,
      window_from,
      access_ctr
    FROM
      output.pa_bp_hin
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

-- PA local road RHIN miles 
WITH
  a AS (
    SELECT DISTINCT
      (hin_id),
      window_to,
      window_from,
      pl.cty_code
    FROM
      output.pa_lr_bp_hin h 
    join 
      input.padot_localroads pl on h.lr_id = pl.lr_id
  )
select
	cty_code,
  SUM(window_to - window_from) * 0.0001893939 AS miles
FROM
  a
group by 
cty_code

-- PA RMS RHIN crash totals no Limited Access
WITH
  a AS (
    SELECT DISTINCT
      (hin_id),
      id,
      window_to,
      window_from,
      crashcount,
      access_ctr
    FROM
      output.pa_bp_hin
  )
SELECT
  SUM(crashcount) AS crashcount,
  LEFT(id, 2) AS cty
FROM
  a
WHERE
  a.access_ctr != '1'
GROUP BY
  LEFT(id, 2);
  

-- PA Local Road RHIN crash totals
WITH
  a AS (
    SELECT DISTINCT
      (hin_id),
      window_to,
      window_from,
      crashcount,
      pl.cty_code
    FROM
      output.pa_lr_bp_hin h
    join 
      input.padot_localroads pl on h.lr_id = pl.lr_id
  )
select
cty_code,
  SUM(crashcount) AS crashcount
FROM
  a
GROUP BY
  cty_code;
  
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