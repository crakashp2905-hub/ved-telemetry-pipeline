-- bronze.sql — land all 54 weekly CSVs into one typed table.
--
-- Two deliberate choices for messy real data:
--  1. all_varchar=true reads every column as text so a stray "NaN" or junk
--     value can never break type inference; we then TRY_CAST to the real type,
--     which yields NULL on anything uncastable instead of aborting the load.
--  2. nullstr='NaN' turns the dataset's literal "NaN" tokens into SQL NULL up
--     front. store_rejects keeps any structurally broken lines so we can count
--     them (quarantine) rather than silently dropping them.
--
-- Column names are normalised to snake_case here; everything downstream then
-- avoids the "Vehicle Speed[km/h]" bracket-quoting noise. {data_glob} is filled
-- in by main.py so the file paths aren't hardcoded.

CREATE OR REPLACE TABLE bronze AS
SELECT
  parse_filename(filename)                          AS source_file,   -- VED_171101_week.csv, not the full path
  TRY_CAST("DayNum"        AS DOUBLE)               AS day_num,
  TRY_CAST("VehId"         AS INTEGER)              AS veh_id,
  TRY_CAST("Trip"          AS INTEGER)              AS trip,
  TRY_CAST("Timestamp(ms)" AS BIGINT)               AS ts_ms,         -- trip-relative: resets to 0 each Trip
  TRY_CAST("Latitude[deg]"  AS DOUBLE)              AS lat,
  TRY_CAST("Longitude[deg]" AS DOUBLE)              AS lon,
  TRY_CAST("Vehicle Speed[km/h]" AS DOUBLE)         AS speed_kmh,
  TRY_CAST("MAF[g/sec]"     AS DOUBLE)              AS maf_g_s,
  TRY_CAST("Engine RPM[RPM]" AS DOUBLE)             AS rpm,
  TRY_CAST("Absolute Load[%]" AS DOUBLE)            AS abs_load_pct,
  TRY_CAST("OAT[DegC]"      AS DOUBLE)              AS oat_c,
  TRY_CAST("Fuel Rate[L/hr]" AS DOUBLE)             AS fuel_rate_lph,
  TRY_CAST("Air Conditioning Power[kW]"    AS DOUBLE) AS ac_kw,
  TRY_CAST("Air Conditioning Power[Watts]" AS DOUBLE) AS ac_w,
  TRY_CAST("Heater Power[Watts]" AS DOUBLE)         AS heater_w,
  TRY_CAST("HV Battery Current[A]" AS DOUBLE)       AS hv_current_a,
  TRY_CAST("HV Battery SOC[%]"     AS DOUBLE)       AS hv_soc_pct,
  TRY_CAST("HV Battery Voltage[V]" AS DOUBLE)       AS hv_voltage_v,
  TRY_CAST("Short Term Fuel Trim Bank 1[%]" AS DOUBLE) AS stft_b1,
  TRY_CAST("Short Term Fuel Trim Bank 2[%]" AS DOUBLE) AS stft_b2,
  TRY_CAST("Long Term Fuel Trim Bank 1[%]"  AS DOUBLE) AS ltft_b1,
  TRY_CAST("Long Term Fuel Trim Bank 2[%]"  AS DOUBLE) AS ltft_b2
FROM read_csv_auto(
       '{data_glob}',
       header      = true,
       all_varchar = true,     -- read as text, then TRY_CAST below
       nullstr     = 'NaN',    -- the dataset's literal NaN -> SQL NULL
       filename    = true,
       ignore_errors = true,   -- structurally broken lines are skipped...
       strict_mode = false     -- ...and counted as rejects in main.py
     );
