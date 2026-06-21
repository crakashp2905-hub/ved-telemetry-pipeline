-- dq_checks.sql — the structural data-quality checks that need real SQL
-- (window functions / gaps-and-islands). The per-column null-rate and the
-- config-driven physical-range checks are generated from config/dq_rules.csv
-- in main.py and UNION-ed onto the output of this file to form pipeline_health.
--
-- bronze.rowid is the original load order, so "out of order" = a ping that
-- arrived after one with a larger trip-relative timestamp.

-- Per-file structural metrics: duplicate natural keys + late/out-of-order pings.
CREATE OR REPLACE TABLE dq_structural AS
WITH ordered AS (
  SELECT
    source_file, veh_id, trip, ts_ms,
    LAG(ts_ms) OVER (PARTITION BY veh_id, trip ORDER BY rowid) AS prev_ts
  FROM bronze
)
SELECT
  source_file,
  COUNT(*)                                                       AS n_rows,
  COUNT(*) - COUNT(DISTINCT (veh_id, trip, ts_ms))               AS n_dup_key,
  SUM(CASE WHEN prev_ts IS NOT NULL AND ts_ms < prev_ts THEN 1 ELSE 0 END) AS n_out_of_order
FROM ordered
GROUP BY source_file;

-- Sensor-dropout run lengths: the longest consecutive stretch of missing
-- readings per signal, per file. Classic gaps-and-islands — the difference of
-- two row_numbers is constant within a run of NULLs, so it labels each run.
CREATE OR REPLACE TABLE dq_dropout AS
WITH long AS (
  SELECT source_file, veh_id, trip, rowid AS rid, signal, val
  FROM (
    SELECT rowid, source_file, veh_id, trip,
           speed_kmh, rpm, oat_c, hv_soc_pct
    FROM bronze
  )
  UNPIVOT INCLUDE NULLS (val FOR signal IN (speed_kmh, rpm, oat_c, hv_soc_pct))
),
islands AS (
  SELECT *,
    row_number() OVER (PARTITION BY source_file, veh_id, trip, signal ORDER BY rid)
      - row_number() OVER (PARTITION BY source_file, veh_id, trip, signal, (val IS NULL) ORDER BY rid) AS grp
  FROM long
),
runs AS (
  SELECT source_file, signal, COUNT(*) AS run_len
  FROM islands
  WHERE val IS NULL
  GROUP BY source_file, signal, veh_id, trip, grp
)
SELECT
  source_file,
  signal,
  MAX(run_len)        AS max_null_run,
  ROUND(AVG(run_len), 1) AS avg_null_run
FROM runs
GROUP BY source_file, signal;
