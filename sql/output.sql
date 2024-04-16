/*
Creating output tables with geometry for each HIN
 */
-- Map NJ ksi HIN using sri/mp to LRS
BEGIN;
CREATE TABLE
    output.nj_ksi_hin_gis AS
SELECT
    hin.*,
    ST_LineSubstring (
        lrs.geometry,
        (GREATEST(hin.window_from, lrs.mp_start) - lrs.mp_start) / (lrs.mp_end - lrs.mp_start),
        (LEAST(hin.window_to, lrs.mp_end) - lrs.mp_start) / (lrs.mp_end - lrs.mp_start)
    ) AS geom
FROM
    output.nj_ksi_hin hin
    JOIN (
        SELECT
            sri,
            ROUND(mp_start::NUMERIC, 2) AS mp_start,
            ROUND(mp_end::NUMERIC, 2) AS mp_end,
            route_subtype,
            geometry
        FROM
            INPUT.njdot_lrs
    ) AS lrs ON lrs.sri = hin.sri
WHERE
    hin.window_from <= lrs.mp_end
    AND hin.window_to >= lrs.mp_start
UNION
SELECT
    hin.*,
    ST_LineSubstring (
        lrs.geometry,
        (GREATEST(hin.window_from, lrs.mp_start) - lrs.mp_start) / (lrs.mp_end - lrs.mp_start),
        (LEAST(hin.window_to, lrs.mp_end) - lrs.mp_start) / (lrs.mp_end - lrs.mp_start)
    ) AS geom
FROM
    output.nj_ksi_hin hin
    JOIN (
        SELECT
            sri,
            ROUND(mp_start::NUMERIC, 2) AS mp_start,
            ROUND(mp_end::NUMERIC, 2) AS mp_end,
            route_subtype,
            geometry
        FROM
            INPUT.njdot_lrs
    ) AS lrs ON lrs.sri = hin.sri
WHERE
    hin.window_from >= lrs.mp_start
    AND hin.window_to <= lrs.mp_end;
COMMIT;
-- Map NJ bike/ped HIN using sri/mp to LRS
BEGIN;
CREATE TABLE
    output.nj_bp_hin_gis AS
SELECT
    hin.*,
    ST_LineSubstring (
        lrs.geometry,
        (GREATEST(hin.window_from, lrs.mp_start) - lrs.mp_start) / (lrs.mp_end - lrs.mp_start),
        (LEAST(hin.window_to, lrs.mp_end) - lrs.mp_start) / (lrs.mp_end - lrs.mp_start)
    ) AS geom
FROM
    output.nj_bp_hin hin
    JOIN (
        SELECT
            sri,
            ROUND(mp_start::NUMERIC, 2) AS mp_start,
            ROUND(mp_end::NUMERIC, 2) AS mp_end,
            route_subtype,
            geometry
        FROM
            INPUT.njdot_lrs
    ) AS lrs ON lrs.sri = hin.sri
WHERE
    hin.window_from <= lrs.mp_end
    AND hin.window_to >= lrs.mp_start
UNION
SELECT
    hin.*,
    ST_LineSubstring (
        lrs.geometry,
        (GREATEST(hin.window_from, lrs.mp_start) - lrs.mp_start) / (lrs.mp_end - lrs.mp_start),
        (LEAST(hin.window_to, lrs.mp_end) - lrs.mp_start) / (lrs.mp_end - lrs.mp_start)
    ) AS geom
FROM
    output.nj_bp_hin hin
    JOIN (
        SELECT
            sri,
            ROUND(mp_start::NUMERIC, 2) AS mp_start,
            ROUND(mp_end::NUMERIC, 2) AS mp_end,
            route_subtype,
            geometry
        FROM
            INPUT.njdot_lrs
    ) AS lrs ON lrs.sri = hin.sri
WHERE
    hin.window_from >= lrs.mp_start
    AND hin.window_to <= lrs.mp_end;
COMMIT;
-- Map PA ksi HIN using county/route/seg/offset to LRS
BEGIN;
CREATE TABLE
    output.pa_ksi_hin_gis AS
SELECT
    hin.*,
    ST_LineSubstring (
        lrs.geometry,
        (GREATEST(hin.window_from, lrs.cum_offset) - lrs.cum_offset) / (lrs.cum_offs_1 - lrs.cum_offset),
        (LEAST(hin.window_to, lrs.cum_offs_1) - lrs.cum_offset) / (lrs.cum_offs_1 - lrs.cum_offset)
    ) AS geom
FROM
    output.pa_ksi_hin hin
    JOIN (
        SELECT
            CONCAT(cty_code, st_rt_no, side_ind) AS id,
            cum_offset,
            cum_offs_1,
            ST_AddMeasure (geometry, cum_offset, cum_offs_1) AS geometry
        FROM
            INPUT.padot_rms
    ) AS lrs ON lrs.id = hin.id
WHERE
    hin.window_from <= lrs.cum_offs_1
    AND hin.window_to >= lrs.cum_offset
UNION
SELECT
    hin.*,
    ST_LineSubstring (
        lrs.geometry,
        (GREATEST(hin.window_from, lrs.cum_offset) - lrs.cum_offset) / (lrs.cum_offs_1 - lrs.cum_offset),
        (LEAST(hin.window_to, lrs.cum_offs_1) - lrs.cum_offset) / (lrs.cum_offs_1 - lrs.cum_offset)
    ) AS geom
FROM
    output.pa_ksi_hin hin
    JOIN (
        SELECT
            CONCAT(cty_code, st_rt_no, side_ind) AS id,
            cum_offset,
            cum_offs_1,
            ST_AddMeasure (geometry, cum_offset, cum_offs_1) AS geometry
        FROM
            INPUT.padot_rms
    ) AS lrs ON lrs.id = hin.id
WHERE
    hin.window_from >= lrs.cum_offset
    AND hin.window_to <= lrs.cum_offs_1;
COMMIT;
-- Map PA bike/ped HIN using county/route/seg/offset to LRS
BEGIN;
CREATE TABLE
    output.pa_bp_hin_gis AS
SELECT
    hin.*,
    ST_LineSubstring (
        lrs.geometry,
        (GREATEST(hin.window_from, lrs.cum_offset) - lrs.cum_offset) / (lrs.cum_offs_1 - lrs.cum_offset),
        (LEAST(hin.window_to, lrs.cum_offs_1) - lrs.cum_offset) / (lrs.cum_offs_1 - lrs.cum_offset)
    ) AS geom
FROM
    output.pa_bp_hin hin
    JOIN (
        SELECT
            CONCAT(cty_code, st_rt_no, side_ind) AS id,
            cum_offset,
            cum_offs_1,
            ST_AddMeasure (geometry, cum_offset, cum_offs_1) AS geometry
        FROM
            INPUT.padot_rms
    ) AS lrs ON lrs.id = hin.id
WHERE
    hin.window_from <= lrs.cum_offs_1
    AND hin.window_to >= lrs.cum_offset
UNION
SELECT
    hin.*,
    ST_LineSubstring (
        lrs.geometry,
        (GREATEST(hin.window_from, lrs.cum_offset) - lrs.cum_offset) / (lrs.cum_offs_1 - lrs.cum_offset),
        (LEAST(hin.window_to, lrs.cum_offs_1) - lrs.cum_offset) / (lrs.cum_offs_1 - lrs.cum_offset)
    ) AS geom
FROM
    output.pa_bp_hin hin
    JOIN (
        SELECT
            CONCAT(cty_code, st_rt_no, side_ind) AS id,
            cum_offset,
            cum_offs_1,
            ST_AddMeasure (geometry, cum_offset, cum_offs_1) AS geometry
        FROM
            INPUT.padot_rms
    ) AS lrs ON lrs.id = hin.id
WHERE
    hin.window_from >= lrs.cum_offset
    AND hin.window_to <= lrs.cum_offs_1;
COMMIT;
-- Map PA local road ksi HIN using county/route/seg/offset to local road LRS 
BEGIN;
CREATE TABLE
    output.pa_lr_ksi_hin_gis AS
SELECT
    hin.*,
    lrs.cty_code,
    ST_LineSubstring (
        lrs.geometry,
        (GREATEST(hin.window_from, lrs.cum_offset_bgn) - lrs.cum_offset_bgn) / (lrs.cum_offset_end - lrs.cum_offset_bgn),
        (LEAST(hin.window_to, lrs.cum_offset_end) - lrs.cum_offset_bgn) / (lrs.cum_offset_end - lrs.cum_offset_bgn)
    ) AS geom
FROM
    output.pa_lr_ksi_hin hin
    JOIN (
        SELECT
            lr_id,
            cty_code,
            cum_offset_bgn,
            cum_offset_end,
            ST_AddMeasure (geometry, cum_offset_bgn, cum_offset_end) AS geometry
        FROM
            INPUT.padot_localroads
    ) AS lrs ON lrs.lr_id = hin.lr_id
WHERE
    hin.window_from <= lrs.cum_offset_end
    AND hin.window_to >= lrs.cum_offset_bgn
UNION
SELECT
    hin.*,
    lrs.cty_code,
    ST_LineSubstring (
        lrs.geometry,
        (GREATEST(hin.window_from, lrs.cum_offset_bgn) - lrs.cum_offset_bgn) / (lrs.cum_offset_end - lrs.cum_offset_bgn),
        (LEAST(hin.window_to, lrs.cum_offset_end) - lrs.cum_offset_bgn) / (lrs.cum_offset_end - lrs.cum_offset_bgn)
    ) AS geom
FROM
    output.pa_lr_ksi_hin hin
    JOIN (
        SELECT
            lr_id,
            cty_code,
            cum_offset_bgn,
            cum_offset_end,
            ST_AddMeasure (geometry, cum_offset_bgn, cum_offset_end) AS geometry
        FROM
            INPUT.padot_localroads
    ) AS lrs ON lrs.lr_id = hin.lr_id
WHERE
    hin.window_from >= lrs.cum_offset_bgn
    AND hin.window_to <= lrs.cum_offset_end;
COMMIT;
-- Map PA local road ksi HIN using county/route/seg/offset to local road LRS 
BEGIN;
CREATE TABLE
    output.pa_lr_bp_hin_gis AS
SELECT
    hin.*,
    lrs.cty_code,
    ST_LineSubstring (
        lrs.geometry,
        (GREATEST(hin.window_from, lrs.cum_offset_bgn) - lrs.cum_offset_bgn) / (lrs.cum_offset_end - lrs.cum_offset_bgn),
        (LEAST(hin.window_to, lrs.cum_offset_end) - lrs.cum_offset_bgn) / (lrs.cum_offset_end - lrs.cum_offset_bgn)
    ) AS geom
FROM
    output.pa_lr_bp_hin hin
    JOIN (
        SELECT
            lr_id,
            cty_code,
            cum_offset_bgn,
            cum_offset_end,
            ST_AddMeasure (geometry, cum_offset_bgn, cum_offset_end) AS geometry
        FROM
            INPUT.padot_localroads
    ) AS lrs ON lrs.lr_id = hin.lr_id
WHERE
    hin.window_from <= lrs.cum_offset_end
    AND hin.window_to >= lrs.cum_offset_bgn
UNION
SELECT
    hin.*,
    lrs.cty_code,
    ST_LineSubstring (
        lrs.geometry,
        (GREATEST(hin.window_from, lrs.cum_offset_bgn) - lrs.cum_offset_bgn) / (lrs.cum_offset_end - lrs.cum_offset_bgn),
        (LEAST(hin.window_to, lrs.cum_offset_end) - lrs.cum_offset_bgn) / (lrs.cum_offset_end - lrs.cum_offset_bgn)
    ) AS geom
FROM
    output.pa_lr_bp_hin hin
    JOIN (
        SELECT
            lr_id,
            cty_code,
            cum_offset_bgn,
            cum_offset_end,
            ST_AddMeasure (geometry, cum_offset_bgn, cum_offset_end) AS geometry
        FROM
            INPUT.padot_localroads
    ) AS lrs ON lrs.lr_id = hin.lr_id
WHERE
    hin.window_from >= lrs.cum_offset_bgn
    AND hin.window_to <= lrs.cum_offset_end;
COMMIT;
