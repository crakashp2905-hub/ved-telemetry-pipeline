-- silver_sessionize.sql — rebuild trips from raw pings with gaps-and-islands,
-- then summarise each rebuilt session.
--
-- Timestamp(ms) is trip-relative (every Trip starts at 0), so we partition by
-- (veh_id, trip) and order by ts_ms. A gap larger than {gap_ms} ms starts a new
-- session; the running SUM of those breaks is the session index. This means a
-- rebuilt session always sits inside exactly one recorded Trip, so validating
-- against the Trip column = "how often did one Trip contain a long stop?".

-- silver = cleaned, deduped pings labelled with a session.
CREATE OR REPLACE TABLE silver AS
WITH deduped AS (
  -- drop exact duplicate natural keys, keep the first one that loaded
  SELECT *
  FROM bronze
  QUALIFY row_number() OVER (PARTITION BY veh_id, trip, ts_ms ORDER BY rowid) = 1
),
clean AS (
  -- null-out physically impossible values instead of dropping the row
  SELECT
    source_file, day_num, veh_id, trip, ts_ms,
    CASE WHEN lat BETWEEN  -90 AND  90 THEN lat END AS lat,
    CASE WHEN lon BETWEEN -180 AND 180 THEN lon END AS lon,
    CASE WHEN speed_kmh BETWEEN 0 AND 200 THEN speed_kmh END AS speed_kmh,
    rpm, abs_load_pct, oat_c, fuel_rate_lph, ac_kw, heater_w,
    hv_current_a,
    CASE WHEN hv_soc_pct   BETWEEN 0 AND 100 THEN hv_soc_pct   END AS hv_soc_pct,
    CASE WHEN hv_voltage_v BETWEEN 0 AND 500 THEN hv_voltage_v END AS hv_voltage_v
  FROM deduped
),
marked AS (
  SELECT *,
    CASE WHEN ts_ms - LAG(ts_ms) OVER w > {gap_ms} THEN 1 ELSE 0 END AS is_break
  FROM clean
  WINDOW w AS (PARTITION BY veh_id, trip ORDER BY ts_ms)
),
sessioned AS (
  SELECT *,
    SUM(is_break) OVER (PARTITION BY veh_id, trip ORDER BY ts_ms) AS session_idx
  FROM marked
)
SELECT
  * EXCLUDE (is_break),
  CAST(veh_id AS VARCHAR) || '-' || CAST(trip AS VARCHAR) || '-' || CAST(session_idx AS VARCHAR) AS session_key,
  -- interval to the next ping in the SAME session; resets at a session break
  LEAD(ts_ms) OVER (PARTITION BY veh_id, trip, session_idx ORDER BY ts_ms) - ts_ms AS dt_ms
FROM sessioned;

-- silver_sessions = one row per rebuilt session (the deliverable shape).
CREATE OR REPLACE TABLE silver_sessions AS
WITH steps AS (
  SELECT
    session_key, source_file, veh_id, trip, session_idx,
    day_num, ts_ms, dt_ms, speed_kmh, hv_soc_pct, oat_c, lat, lon,
    LAG(lat) OVER w AS plat,
    LAG(lon) OVER w AS plon
  FROM silver
  WINDOW w AS (PARTITION BY session_key ORDER BY ts_ms)
),
seg AS (
  SELECT *,
    -- haversine distance (km) between consecutive pings
    CASE WHEN plat IS NOT NULL AND lat IS NOT NULL THEN
      2 * 6371.0 * asin(sqrt(
        pow(sin(radians(lat - plat) / 2), 2) +
        cos(radians(plat)) * cos(radians(lat)) * pow(sin(radians(lon - plon) / 2), 2)
      ))
    END AS seg_km
  FROM steps
)
SELECT
  session_key, source_file, veh_id, trip, session_idx,
  MIN(day_num)                              AS start_day,
  MAX(day_num)                              AS end_day,
  (MAX(ts_ms) - MIN(ts_ms)) / 1000.0        AS duration_s,
  COUNT(*)                                  AS n_pings,
  ROUND(SUM(COALESCE(seg_km, 0)), 3)        AS distance_km,
  -- moving vs idle split by speed, weighted by the interval each ping covers
  ROUND(SUM(CASE WHEN speed_kmh >  3 THEN COALESCE(dt_ms, 0) ELSE 0 END) / 1000.0, 1) AS moving_s,
  ROUND(SUM(CASE WHEN speed_kmh <= 3 OR speed_kmh IS NULL THEN COALESCE(dt_ms, 0) ELSE 0 END) / 1000.0, 1) AS idle_s,
  ROUND(AVG(speed_kmh), 1)                  AS avg_speed_kmh,
  ROUND(AVG(oat_c), 1)                      AS avg_oat_c,
  arg_min(hv_soc_pct, ts_ms)                AS soc_start,   -- first/last non-null SOC by time
  arg_max(hv_soc_pct, ts_ms)                AS soc_end
FROM seg
GROUP BY session_key, source_file, veh_id, trip, session_idx;

-- silver_trip_validation = rebuilt sessions vs the recorded Trip column.
CREATE OR REPLACE TABLE silver_trip_validation AS
SELECT
  veh_id, trip,
  COUNT(*) AS n_sessions,
  CASE WHEN COUNT(*) = 1 THEN 'matches_trip' ELSE 'trip_split_by_gap' END AS verdict
FROM silver_sessions
GROUP BY veh_id, trip;
