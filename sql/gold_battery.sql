-- gold_battery.sql — energy & efficiency for the battery-capable vehicles,
-- then the headline: how efficiency varies with outside air temperature.
--
-- Scope: only the 27 PHEV/EV vehicles (joined from the `vehicles` dim that
-- main.py loads from the static xlsx) AND only pings with real V & I readings.
-- Power = V * I; energy per interval = power * dt / 3.6e6 -> Wh. In this dataset
-- the current sign convention makes power NEGATIVE on discharge (verified: net
-- power correlates -0.71 with SOC drop), so discharge is the negative side and
-- regen/charging is the positive side. net consumed = discharge - regen.

CREATE OR REPLACE TABLE gold_session_energy AS
WITH ev_pings AS (
  SELECT s.*, v.engine_type
  FROM silver s
  JOIN vehicles v ON s.veh_id = v.veh_id
  WHERE v.engine_type IN ('PHEV', 'EV')
    AND s.hv_voltage_v IS NOT NULL
    AND s.hv_current_a IS NOT NULL
),
energy AS (
  SELECT
    session_key,
    veh_id,
    any_value(engine_type) AS engine_type,
    SUM(CASE WHEN hv_voltage_v * hv_current_a < 0
             THEN -hv_voltage_v * hv_current_a * COALESCE(dt_ms, 0) / 3.6e6 ELSE 0 END) AS discharge_wh,
    SUM(CASE WHEN hv_voltage_v * hv_current_a > 0
             THEN  hv_voltage_v * hv_current_a * COALESCE(dt_ms, 0) / 3.6e6 ELSE 0 END) AS regen_wh
  FROM ev_pings
  GROUP BY session_key, veh_id
)
SELECT
  e.session_key,
  e.veh_id,
  e.engine_type,
  ss.avg_oat_c,
  ss.distance_km,
  ss.duration_s,
  ss.soc_start,
  ss.soc_end,
  ss.soc_start - ss.soc_end                                  AS soc_drop_pct,
  ROUND(e.discharge_wh, 1)                                   AS discharge_wh,
  ROUND(e.regen_wh, 1)                                       AS regen_wh,
  ROUND(e.discharge_wh - e.regen_wh, 1)                      AS net_consumed_wh,
  -- two efficiency views: SOC-based (sign-unambiguous) and Wh-based
  CASE WHEN ss.distance_km > 0.5
       THEN ROUND((ss.soc_start - ss.soc_end) / ss.distance_km, 3) END AS soc_pct_per_km,
  CASE WHEN ss.distance_km > 0.5
       THEN ROUND((e.discharge_wh - e.regen_wh) / ss.distance_km, 1) END AS wh_per_km
FROM energy e
JOIN silver_sessions ss USING (session_key);

-- The headline table: efficiency bucketed by outside air temperature.
-- Filtered to genuine driving that actually drew down the battery.
CREATE OR REPLACE TABLE gold_efficiency_by_temp AS
SELECT
  CASE WHEN avg_oat_c <  0 THEN '1. < 0C'
       WHEN avg_oat_c < 10 THEN '2. 0-10C'
       WHEN avg_oat_c < 20 THEN '3. 10-20C'
       WHEN avg_oat_c < 30 THEN '4. 20-30C'
       ELSE                     '5. 30C+'  END        AS temp_band,
  COUNT(*)                       AS n_sessions,
  ROUND(AVG(avg_oat_c), 1)       AS mean_oat_c,
  ROUND(AVG(soc_pct_per_km), 3)  AS avg_soc_pct_per_km,
  ROUND(AVG(wh_per_km), 1)       AS avg_wh_per_km,
  ROUND(AVG(distance_km), 2)     AS avg_distance_km,
  ROUND(AVG(soc_drop_pct), 1)    AS avg_soc_drop_pct
FROM gold_session_energy
WHERE distance_km > 0.5
  AND soc_drop_pct > 0
GROUP BY temp_band
ORDER BY temp_band;

-- Supporting view: cabin heating is the usual culprit for cold-weather drain.
CREATE OR REPLACE TABLE gold_hvac_by_temp AS
SELECT
  CASE WHEN avg_oat_c <  0 THEN '1. < 0C'
       WHEN avg_oat_c < 10 THEN '2. 0-10C'
       WHEN avg_oat_c < 20 THEN '3. 10-20C'
       WHEN avg_oat_c < 30 THEN '4. 20-30C'
       ELSE                     '5. 30C+'  END AS temp_band,
  COUNT(*)                          AS n_sessions,
  ROUND(AVG(heater_kw), 3)          AS avg_heater_kw,
  ROUND(AVG(ac_kw), 3)              AS avg_ac_kw
FROM (
  SELECT s.session_key,
         AVG(s.oat_c)             AS avg_oat_c,
         AVG(s.heater_w) / 1000.0 AS heater_kw,
         AVG(s.ac_kw)             AS ac_kw
  FROM silver s
  JOIN vehicles v ON s.veh_id = v.veh_id
  WHERE v.engine_type IN ('PHEV', 'EV')
  GROUP BY s.session_key
)
GROUP BY temp_band
ORDER BY temp_band;
