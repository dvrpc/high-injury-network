/*
Creating output tables with geometry for each HIN
 */
-- Map NJ ksi HIN using sri/mp to LRS
BEGIN;
CREATE TABLE
    output.nj_ksi_hin_gis AS
WITH lrs_data AS (
    SELECT
        sri,
        ROUND(mp_start::NUMERIC, 2) AS mp_start,
        ROUND(mp_end::NUMERIC, 2) AS mp_end,
        route_subtype,
        geometry
    FROM INPUT.njdot_lrs
)
SELECT
    hin.*,
    ST_LineSubstring(
        lrs.geometry,
        (GREATEST(hin.window_from, lrs.mp_start) - lrs.mp_start) / (lrs.mp_end - lrs.mp_start),
        (LEAST(hin.window_to, lrs.mp_end) - lrs.mp_start) / (lrs.mp_end - lrs.mp_start)
    ) AS geom
FROM output.nj_ksi_hin hin
JOIN lrs_data lrs ON lrs.sri = hin.sri
WHERE (hin.window_from <= lrs.mp_end AND hin.window_to >= lrs.mp_start) 
   OR (hin.window_from >= lrs.mp_start AND hin.window_to <= lrs.mp_end);
COMMIT;
-- Map NJ bike/ped HIN using sri/mp to LRS
BEGIN;
CREATE TABLE
    output.nj_bp_hin_gis AS
WITH lrs_data AS (
    SELECT
        sri,
        ROUND(mp_start::NUMERIC, 2) AS mp_start,
        ROUND(mp_end::NUMERIC, 2) AS mp_end,
        route_subtype,
        geometry
    FROM INPUT.njdot_lrs
)
SELECT
    hin.*,
    ST_LineSubstring(
        lrs.geometry,
        (GREATEST(hin.window_from, lrs.mp_start) - lrs.mp_start) / (lrs.mp_end - lrs.mp_start),
        (LEAST(hin.window_to, lrs.mp_end) - lrs.mp_start) / (lrs.mp_end - lrs.mp_start)
    ) AS geom
FROM output.nj_bp_hin hin
JOIN lrs_data lrs ON lrs.sri = hin.sri
WHERE (hin.window_from <= lrs.mp_end AND hin.window_to >= lrs.mp_start) 
   OR (hin.window_from >= lrs.mp_start AND hin.window_to <= lrs.mp_end);
COMMIT;
-- Map PA ksi HIN using county/route/seg/offset to LRS
BEGIN;
CREATE TABLE output.pa_ksi_hin_gis AS
SELECT
    hin.*,
    ST_LineSubstring(
        lrs.geometry,
        (GREATEST(hin.window_from, lrs.cum_offset) - lrs.cum_offset) / (lrs.cum_offs_1 - lrs.cum_offset),
        (LEAST(hin.window_to, lrs.cum_offs_1) - lrs.cum_offset) / (lrs.cum_offs_1 - lrs.cum_offset)
    ) AS geom
FROM output.pa_ksi_hin hin
JOIN (
    SELECT
        CONCAT(cty_code, st_rt_no, side_ind) AS id,
        cum_offset,
        cum_offs_1,
        ST_AddMeasure(geometry, cum_offset, cum_offs_1) AS geometry
    FROM INPUT.padot_rms
) AS lrs ON lrs.id = hin.id
WHERE 
    hin.window_from < lrs.cum_offs_1
    AND hin.window_to > lrs.cum_offset;
COMMIT;
-- Map PA bike/ped HIN using county/route/seg/offset to LRS
BEGIN;
CREATE TABLE output.pa_bp_hin_gis AS
SELECT
    hin.*,
    ST_LineSubstring(
        lrs.geometry,
        (GREATEST(hin.window_from, lrs.cum_offset) - lrs.cum_offset) / (lrs.cum_offs_1 - lrs.cum_offset),
        (LEAST(hin.window_to, lrs.cum_offs_1) - lrs.cum_offset) / (lrs.cum_offs_1 - lrs.cum_offset)
    ) AS geom
FROM output.pa_bp_hin hin
JOIN (
    SELECT
        CONCAT(cty_code, st_rt_no, side_ind) AS id,
        cum_offset,
        cum_offs_1,
        ST_AddMeasure(geometry, cum_offset, cum_offs_1) AS geometry
    FROM INPUT.padot_rms
) AS lrs ON lrs.id = hin.id
WHERE 
    hin.window_from < lrs.cum_offs_1
    AND hin.window_to > lrs.cum_offset;
COMMIT;
-- Map PA local road ksi HIN using county/route/seg/offset to local road LRS 
BEGIN;
CREATE TABLE output.pa_lr_ksi_hin_gis AS
SELECT
   hin.*,  
   lrs.cty_code,
   ST_LineSubstring(
       lrs.geometry,
       (GREATEST(hin.window_from, lrs.cum_offset_bgn) - lrs.cum_offset_bgn) / (lrs.cum_offset_end - lrs.cum_offset_bgn),
       (LEAST(hin.window_to, lrs.cum_offset_end) - lrs.cum_offset_bgn) / (lrs.cum_offset_end - lrs.cum_offset_bgn)
   ) AS geom
FROM output.pa_lr_ksi_hin hin
JOIN (
   SELECT
       lr_id,
       cty_code, 
       cum_offset_bgn,
       cum_offset_end,
       ST_AddMeasure(geometry, cum_offset_bgn, cum_offset_end) AS geometry
   FROM INPUT.padot_localroads
) AS lrs ON lrs.lr_id = hin.lr_id
WHERE
   hin.window_from < lrs.cum_offset_end 
   AND hin.window_to > lrs.cum_offset_bgn;
COMMIT;
-- Map PA local road bp HIN using county/route/seg/offset to local road LRS 
BEGIN;
CREATE TABLE output.pa_lr_bp_hin_gis AS
SELECT
   hin.*,  
   lrs.cty_code,
   ST_LineSubstring(
       lrs.geometry,
       (GREATEST(hin.window_from, lrs.cum_offset_bgn) - lrs.cum_offset_bgn) / (lrs.cum_offset_end - lrs.cum_offset_bgn),
       (LEAST(hin.window_to, lrs.cum_offset_end) - lrs.cum_offset_bgn) / (lrs.cum_offset_end - lrs.cum_offset_bgn)
   ) AS geom
FROM output.pa_lr_bp_hin hin
JOIN (
   SELECT
       lr_id,
       cty_code, 
       cum_offset_bgn,
       cum_offset_end,
       ST_AddMeasure(geometry, cum_offset_bgn, cum_offset_end) AS geometry
   FROM INPUT.padot_localroads
) AS lrs ON lrs.lr_id = hin.lr_id
WHERE
   hin.window_from < lrs.cum_offset_end 
   AND hin.window_to > lrs.cum_offset_bgn;
COMMIT;
-- NJ Crashes mapped
BEGIN;
CREATE TABLE
    output.nj_crashes_gis AS
WITH lrs AS (
    SELECT 
        sri,
        geometry,
        mp_start::NUMERIC AS mp_start,
        mp_end::NUMERIC AS mp_end
    FROM input.njdot_lrs
)
SELECT
    c.*,
    ST_Force2D(
        ST_StartPoint(
            ST_LineSubstring(
                rn.geometry,
                GREATEST(
                    NULLIF((c.mp - rn.mp_start) / NULLIF((rn.mp_end - rn.mp_start), 0), 0),
                    0
                ),
                LEAST(
                    NULLIF((c.mp - rn.mp_start) / NULLIF((rn.mp_end - rn.mp_start), 0), 1),
                    1
                )
            )
        )
    ) AS geom
FROM output.nj_ksi_bp_crashes c
JOIN lrs rn ON c.sri = rn.sri
    AND c.mp BETWEEN rn.mp_start AND rn.mp_end;
COMMIT;
-- PA crashes mapped
BEGIN;
CREATE TABLE
    output.pa_crashes_gis AS
SELECT 
    geom, 
    crn, 
    crash_year, 
    fatal_count, 
    maj_inj_count, 
    major_injury, 
    fatal, 
    injury, 
    fatal_or_susp_serious_inj, 
    bicycle, pedestrian, 
    intersection, 
    adj_rdwy_seq, 
    county, 
    route, 
    segment, 
    "offset", 
    id, 
    null as lr_id, 
    cum_offset 
FROM 
    output.pa_ksi_bp_crashes
UNION
SELECT 
    geom, 
    crn, 
    crash_year, 
    fatal_count, 
    maj_inj_count, 
    major_injury, 
    fatal, 
    injury, 
    fatal_or_susp_serious_inj, 
    bicycle, 
    pedestrian, 
    null as intersection, 
    null as adj_rdwy_seq, 
    cty_code as county, 
    '' as route, 
    '' as segment, 
    null as "offset", 
    '' as id, 
    lr_id, 
    cum_offset 
FROM 
    output.pa_lr_ksi_bp_crashes
COMMIT;
-- One RHIN to rule them all
BEGIN;
CREATE TABLE
    output.rhin_gis AS
WITH
    ksi AS (
        SELECT
            hin_id,
            id,
            'pa' AS state,
            'ksi' AS
        TYPE,
        window_from,
        window_to,
        crashcount,
        total_killed,
        total_maj_inj,
        CASE
            WHEN access_ctr = '1'
            OR access_ctr IS NULL THEN 'y'
            ELSE 'n'
        END AS limited_access,
        'rms' AS lrs,
        geom
        FROM
            output.pa_ksi_hin_gis r
        WHERE
            LEFT(id, 2) != '67'
        UNION
        SELECT
            hin_id * 1000 AS hin_id,
            lr_id::TEXT AS id,
            'pa' AS state,
            'ksi' AS
        TYPE,
        window_from,
        window_to,
        crashcount,
        total_killed,
        total_maj_inj,
        'n' AS limited_access,
        'local' AS lrs,
        geom
        FROM
            output.pa_lr_ksi_hin_gis lr
        WHERE
            cty_code != '67'
        UNION
        SELECT
            hin_id,
            sri AS id,
            'nj' AS state,
            'ksi' AS
        TYPE,
        window_from,
        window_to,
        crashcount,
        total_killed,
        total_maj_inj,
        CASE
            WHEN (
                CLASS NOT LIKE 'Lim%'
                OR CLASS IS NULL
            )
            AND (route_subtype NOT IN (1, 4)) THEN 'n'
            ELSE 'y'
        END AS limited_access,
        'njdot' AS lrs,
        geom
        FROM
            output.nj_ksi_hin_gis
    ),
    ksi_all AS (
        SELECT
            hin_id,
            id,
            state,
        TYPE,
        window_from,
        window_to,
        crashcount,
        total_killed,
        total_maj_inj,
        limited_access,
        lrs,
        st_force2d (st_union (geom)) AS geom
        FROM
            ksi
        GROUP BY
            hin_id,
            id,
            state,
        TYPE,
        window_from,
        window_to,
        crashcount,
        total_killed,
        total_maj_inj,
        limited_access,
        lrs
    ),
    bp AS (
        SELECT
            hin_id,
            id,
            'pa' AS state,
            'bp' AS
        TYPE,
        window_from,
        window_to,
        crashcount,
        total_killed,
        total_maj_inj,
        CASE
            WHEN access_ctr = '1'
            OR access_ctr IS NULL THEN 'y'
            ELSE 'n'
        END AS limited_access,
        'rms' AS lrs,
        geom
        FROM
            output.pa_bp_hin_gis r
        WHERE
            LEFT(id, 2) != '67'
        UNION
        SELECT
            hin_id * 1000 AS hin_id,
            lr_id::TEXT AS id,
            'pa' AS state,
            'bp' AS
        TYPE,
        window_from,
        window_to,
        crashcount,
        total_killed,
        total_maj_inj,
        'n' AS limited_access,
        'local' AS lrs,
        geom
        FROM
            output.pa_lr_bp_hin_gis lr
        WHERE
            cty_code != '67'
        UNION
        SELECT
            hin_id,
            sri AS id,
            'nj' AS state,
            'bp' AS
        TYPE,
        window_from,
        window_to,
        crashcount,
        total_killed,
        total_maj_inj,
        CASE
            WHEN (
                CLASS NOT LIKE 'Lim%'
                OR CLASS IS NULL
            )
            AND (route_subtype NOT IN (1, 4)) THEN 'n'
            ELSE 'y'
        END AS limited_access,
        'njdot' AS lrs,
        geom
        FROM
            output.nj_bp_hin_gis
    ),
    bp_all AS (
        SELECT
            hin_id,
            id,
            state,
        TYPE,
        window_from,
        window_to,
        crashcount,
        total_killed,
        total_maj_inj,
        limited_access,
        lrs,
        st_force2d (st_union (geom)) AS geom
        FROM
            bp
        GROUP BY
            hin_id,
            id,
            state,
        TYPE,
        window_from,
        window_to,
        crashcount,
        total_killed,
        total_maj_inj,
        limited_access,
        lrs
    )
SELECT
    *
FROM
    ksi_all
UNION
SELECT
    *
FROM
    bp_all
COMMIT;