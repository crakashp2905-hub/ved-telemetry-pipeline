"""
VED telemetry pipeline — bronze -> silver -> gold, orchestrated over DuckDB.

Python only orchestrates: it wires up paths, loads the static vehicle metadata,
runs the .sql files in /sql, and writes the scorecard / plots. All of the
transformation logic lives in SQL.

    python main.py                 # full run, all 54 weekly files
    python main.py --sample 3      # dev run, first 3 files
    python main.py --layer dq      # re-run a single layer against existing bronze
"""
from __future__ import annotations
import argparse
import time
from pathlib import Path

import duckdb
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

# --- Config: all paths and tunables live in one place ----------------------
ROOT       = Path(__file__).resolve().parent
DATA_GLOB  = (ROOT / "VED_DynamicData_Part*" / "VED_*_week.csv").as_posix()
SQL_DIR    = ROOT / "sql"
CONFIG     = ROOT / "config" / "dq_rules.csv"
OUTPUT_DIR = ROOT / "output"
PLOTS_DIR  = OUTPUT_DIR / "plots"
DB_PATH    = ROOT / "ved.duckdb"

STATIC_ICE_HEV = ROOT / "VED_Static_Data_ICE&HEV.xlsx"
STATIC_PHEV_EV = ROOT / "VED_Static_Data_PHEV&EV.xlsx"

SESSION_GAP_MS = 30_000   # a pause longer than this starts a new session


def run_sql_file(con, name: str, params: dict | None = None):
    sql = (SQL_DIR / name).read_text(encoding="utf-8")
    if params:
        sql = sql.format(**params)
    con.execute(sql)


def sample_glob(sample: int | None) -> str:
    """Limit ingestion to the first N files in dev mode by handing DuckDB an
    explicit file list instead of the wildcard glob."""
    if not sample:
        return f"'{DATA_GLOB}'"
    import glob
    files = sorted(glob.glob(DATA_GLOB))[:sample]
    return "[" + ", ".join(f"'{Path(f).as_posix()}'" for f in files) + "]"


# --- Static metadata: which vehicles are PHEV/EV ---------------------------
def load_vehicles(con):
    ren = {"VehId": "veh_id", "Vehicle Type": "engine_type", "EngineType": "engine_type",
           "Vehicle Class": "vehicle_class", "Generalized_Weight": "weight"}
    keep = ["veh_id", "engine_type", "vehicle_class", "weight"]
    frames = [pd.read_excel(f).rename(columns=ren)[keep]
              for f in (STATIC_ICE_HEV, STATIC_PHEV_EV)]
    vehicles = pd.concat(frames, ignore_index=True)
    con.register("vehicles_df", vehicles)
    con.execute("CREATE OR REPLACE TABLE vehicles AS SELECT * FROM vehicles_df")
    con.unregister("vehicles_df")
    n = con.execute("SELECT engine_type, COUNT(*) FROM vehicles GROUP BY 1 ORDER BY 2 DESC").fetchall()
    print("  vehicles dim:", dict(n))


# --- Bronze ----------------------------------------------------------------
def build_bronze(con, sample):
    print("[bronze] ingesting raw CSVs...")
    # inject either a quoted glob or an explicit [list] of files, replacing the
    # whole '{data_glob}' token (quotes included) so the list form stays valid SQL
    sql = (SQL_DIR / "bronze.sql").read_text(encoding="utf-8").replace(
        "'{data_glob}'", sample_glob(sample))
    con.execute(sql)

    rows, files = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT source_file) FROM bronze").fetchone()
    dups = con.execute(
        "SELECT COUNT(*) - COUNT(DISTINCT (veh_id, trip, ts_ms)) FROM bronze").fetchone()[0]
    # quarantine accounting: raw data lines (minus one header per file) that
    # didn't make it into bronze were dropped as structurally malformed.
    import glob as _glob
    scoped = sorted(_glob.glob(DATA_GLOB))[:sample] if sample else sorted(_glob.glob(DATA_GLOB))
    raw_lines = sum(sum(1 for _ in open(f, "rb")) for f in scoped)
    rejects = max(0, (raw_lines - len(scoped)) - rows)
    print(f"         {rows:,} rows from {files} files | "
          f"{dups:,} duplicate keys | {rejects:,} malformed lines quarantined")


# --- Data quality ----------------------------------------------------------
def build_dq(con):
    print("[dq]     building config-driven scorecard...")
    run_sql_file(con, "dq_checks.sql")          # -> dq_structural, dq_dropout

    cols = [r[0] for r in con.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = 'bronze' AND column_name <> 'source_file' "
        "ORDER BY ordinal_position").fetchall()]

    # null rate for every column (missingness applies to all columns)
    null_parts = [f"""
      SELECT source_file, 'null_rate' AS check_category, '{c}' AS check_name,
             COUNT(*) AS n_total, COUNT(*) - COUNT("{c}") AS n_flagged,
             ROUND(100.0 * (COUNT(*) - COUNT("{c}")) / COUNT(*), 2) AS flagged_pct,
             ROUND(100.0 * (COUNT(*) - COUNT("{c}")) / COUNT(*), 2) AS metric_value
      FROM bronze GROUP BY source_file""" for c in cols]

    # physical-range checks — one row in dq_rules.csv == one check here
    rules = pd.read_csv(CONFIG)
    range_parts = [f"""
      SELECT source_file, 'range_violation' AS check_category, '{r.column}' AS check_name,
             COUNT("{r.column}") AS n_total,
             SUM(CASE WHEN "{r.column}" < {r.min_value} OR "{r.column}" > {r.max_value} THEN 1 ELSE 0 END) AS n_flagged,
             ROUND(100.0 * SUM(CASE WHEN "{r.column}" < {r.min_value} OR "{r.column}" > {r.max_value} THEN 1 ELSE 0 END)
                   / NULLIF(COUNT("{r.column}"), 0), 4) AS flagged_pct,
             ROUND(100.0 * SUM(CASE WHEN "{r.column}" < {r.min_value} OR "{r.column}" > {r.max_value} THEN 1 ELSE 0 END)
                   / NULLIF(COUNT("{r.column}"), 0), 4) AS metric_value
      FROM bronze GROUP BY source_file""" for r in rules.itertuples()]

    structural = """
      SELECT source_file, 'duplicate_key', 'natural_key_dup', n_rows, n_dup_key,
             ROUND(100.0 * n_dup_key / n_rows, 4), ROUND(100.0 * n_dup_key / n_rows, 4)
      FROM dq_structural
      UNION ALL
      SELECT source_file, 'out_of_order', 'late_timestamp', n_rows, n_out_of_order,
             ROUND(100.0 * n_out_of_order / n_rows, 4), ROUND(100.0 * n_out_of_order / n_rows, 4)
      FROM dq_structural"""

    dropout = """
      SELECT source_file, 'dropout_run', signal,
             NULL, NULL, NULL, max_null_run
      FROM dq_dropout"""

    con.execute("CREATE OR REPLACE TABLE pipeline_health AS\n"
                + "\nUNION ALL\n".join(null_parts + range_parts + [structural, dropout]))

    # overall score weights physical-validity checks; legitimate sparsity
    # (EV-only signals missing on ICE cars) is reported but not penalised.
    score = con.execute("""
        SELECT ROUND(100 - AVG(flagged_pct), 2) FROM pipeline_health
        WHERE check_category IN ('range_violation', 'duplicate_key', 'out_of_order')
    """).fetchone()[0]
    con.execute(f"CREATE OR REPLACE TABLE dq_summary AS SELECT {score} AS overall_score")

    out = (OUTPUT_DIR / "pipeline_health.csv").as_posix()
    con.execute(f"COPY pipeline_health TO '{out}' (HEADER, DELIMITER ',')")
    print(f"         overall DQ score: {score}/100  ->  {out}")

    print("         worst physical-validity checks:")
    worst = con.execute("""
        SELECT check_category, check_name, ROUND(AVG(flagged_pct), 3) AS pct
        FROM pipeline_health
        WHERE check_category IN ('range_violation', 'duplicate_key', 'out_of_order')
        GROUP BY 1, 2 HAVING AVG(flagged_pct) > 0 ORDER BY pct DESC LIMIT 5""").fetchall()
    for cat, name, pct in worst:
        print(f"           {cat:>16}  {name:<18} {pct}%")


# --- Silver ----------------------------------------------------------------
def build_silver(con):
    print("[silver] reconstructing sessions (gaps-and-islands)...")
    run_sql_file(con, "silver_sessionize.sql", {"gap_ms": SESSION_GAP_MS})

    n_sess, n_trips = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT (veh_id, trip)) FROM silver_sessions").fetchone()
    split = con.execute(
        "SELECT COUNT(*) FROM silver_trip_validation WHERE verdict = 'trip_split_by_gap'").fetchone()[0]
    print(f"         {n_sess:,} sessions from {n_trips:,} recorded trips "
          f"| {split:,} trips ({100*split/n_trips:.1f}%) split by a >30s gap")


# --- Gold ------------------------------------------------------------------
def build_gold(con):
    print("[gold]   energy + efficiency on PHEV/EV vehicles...")
    run_sql_file(con, "gold_battery.sql")
    eff = con.execute("SELECT * FROM gold_efficiency_by_temp ORDER BY temp_band").df()
    con.execute(f"COPY gold_efficiency_by_temp TO '{(OUTPUT_DIR/'efficiency_by_temp.csv').as_posix()}' (HEADER)")
    print(eff.to_string(index=False))
    return eff


# --- Plots -----------------------------------------------------------------
def make_plots(con):
    print("[plots]  writing PNGs...")

    # 1. DQ scorecard: null rates + physical-violation rates
    nulls = con.execute("""
        SELECT check_name, ROUND(AVG(flagged_pct), 1) pct FROM pipeline_health
        WHERE check_category = 'null_rate' GROUP BY 1 ORDER BY pct DESC LIMIT 12""").df()
    viol = con.execute("""
        SELECT check_name, AVG(flagged_pct) pct FROM pipeline_health
        WHERE check_category = 'range_violation' GROUP BY 1 ORDER BY pct DESC""").df()
    score = con.execute("SELECT overall_score FROM dq_summary").fetchone()[0]
    fig, (a, b) = plt.subplots(1, 2, figsize=(13, 5))
    a.barh(nulls.check_name, nulls.pct, color="#4c72b0"); a.invert_yaxis()
    a.set_title("Null rate by column (avg % across files)"); a.set_xlabel("% null")
    b.barh(viol.check_name, viol.pct, color="#c44e52"); b.invert_yaxis()
    b.set_title("Out-of-range rate by signal (% of present values)"); b.set_xlabel("% out of range")
    fig.suptitle(f"VED data-quality scorecard   |   overall score {score}/100", fontsize=13)
    fig.tight_layout(); fig.savefig(PLOTS_DIR / "dq_scorecard.png", dpi=110); plt.close(fig)

    # 2. Trip reconstruction
    spt = con.execute("""
        SELECT LEAST(n_sessions, 5) AS sessions_per_trip, COUNT(*) n
        FROM silver_trip_validation GROUP BY 1 ORDER BY 1""").df()
    dur = con.execute("""
        SELECT duration_s/60.0 AS minutes FROM silver_sessions
        WHERE duration_s BETWEEN 0 AND 7200""").df()
    fig, (a, b) = plt.subplots(1, 2, figsize=(13, 5))
    a.bar(spt.sessions_per_trip.astype(str), spt.n, color="#55a868")
    a.set_title("Rebuilt sessions per recorded Trip"); a.set_xlabel("sessions in trip (5 = 5+)")
    a.set_ylabel("trip count")
    b.hist(dur.minutes, bins=40, color="#8172b3")
    b.set_title("Session duration distribution"); b.set_xlabel("minutes"); b.set_ylabel("sessions")
    fig.suptitle("Trip reconstruction from raw pings", fontsize=13)
    fig.tight_layout(); fig.savefig(PLOTS_DIR / "trip_reconstruction.png", dpi=110); plt.close(fig)

    # 3. Efficiency vs temperature (the headline)
    eff = con.execute("SELECT * FROM gold_efficiency_by_temp ORDER BY temp_band").df()
    hv = con.execute("SELECT * FROM gold_hvac_by_temp ORDER BY temp_band").df()
    fig, ax = plt.subplots(figsize=(9, 5.5))
    ax.bar(eff.temp_band, eff.avg_soc_pct_per_km, color="#4c72b0", label="battery use (%SOC/km)")
    ax.set_ylabel("%SOC drawn per km"); ax.set_xlabel("outside air temperature band")
    ax2 = ax.twinx()
    ax2.plot(hv.temp_band, hv.avg_heater_kw, "o-", color="#c44e52", label="avg heater power (kW)")
    ax2.set_ylabel("avg heater power (kW)")
    ax.set_title("EV/PHEV energy use rises in the cold (heating drives it)")
    fig.legend(loc="upper right", bbox_to_anchor=(0.9, 0.88))
    fig.tight_layout(); fig.savefig(PLOTS_DIR / "efficiency_vs_temperature.png", dpi=110); plt.close(fig)

    # 4. EV vs PHEV: who pays the cold-weather penalty
    ep = con.execute("""
        SELECT engine_type,
          CASE WHEN avg_oat_c < 0 THEN '< 0'  WHEN avg_oat_c < 10 THEN '0-10'
               WHEN avg_oat_c < 20 THEN '10-20' WHEN avg_oat_c < 30 THEN '20-30'
               ELSE '30+' END AS band,
          AVG(wh_per_km) AS wh_per_km
        FROM gold_session_energy
        WHERE distance_km > 0.5 AND soc_drop_pct > 0 AND wh_per_km IS NOT NULL
        GROUP BY 1, 2""").df()
    order = ["< 0", "0-10", "10-20", "20-30", "30+"]
    fig, ax = plt.subplots(figsize=(9, 5.5))
    for et, color in [("EV", "#c44e52"), ("PHEV", "#4c72b0")]:
        d = ep[ep.engine_type == et].set_index("band").reindex(order)
        ax.plot(order, d.wh_per_km, "o-", label=et, color=color)
    ax.set_title("Cold-weather energy penalty: pure EVs hit hardest")
    ax.set_xlabel("outside air temperature band (C)"); ax.set_ylabel("Wh per km"); ax.legend()
    fig.tight_layout(); fig.savefig(PLOTS_DIR / "ev_vs_phev_by_temp.png", dpi=110); plt.close(fig)

    # 5. DQ heatmap: null rate per file x column (the observability view)
    nm = con.execute("""
        SELECT source_file, check_name, flagged_pct
        FROM pipeline_health WHERE check_category = 'null_rate'""").df()
    piv = nm.pivot(index="source_file", columns="check_name", values="flagged_pct").sort_index()
    fig, ax = plt.subplots(figsize=(13, 9))
    im = ax.imshow(piv.values, aspect="auto", cmap="magma")
    ax.set_xticks(range(len(piv.columns))); ax.set_xticklabels(piv.columns, rotation=90, fontsize=7)
    ax.set_yticks(range(len(piv.index))); ax.set_yticklabels(piv.index, fontsize=6)
    ax.set_title("Null rate (%) by file and column  —  dark = complete, bright = missing")
    fig.colorbar(im, ax=ax, label="% null"); fig.tight_layout()
    fig.savefig(PLOTS_DIR / "null_rate_heatmap.png", dpi=110); plt.close(fig)

    # 6. GPS coverage: where the driving actually happened
    pts = con.execute("""
        SELECT lon, lat FROM bronze
        WHERE lat BETWEEN -90 AND 90 AND lon BETWEEN -180 AND 180
        USING SAMPLE 60000 ROWS""").df()
    fig, ax = plt.subplots(figsize=(8, 8))
    ax.scatter(pts.lon, pts.lat, s=1, alpha=0.15, color="#4c72b0")
    ax.set_title("GPS ping coverage (60k-row sample)")
    ax.set_xlabel("longitude"); ax.set_ylabel("latitude")
    ax.set_aspect("equal", "datalim")
    fig.tight_layout(); fig.savefig(PLOTS_DIR / "gps_coverage.png", dpi=110); plt.close(fig)

    # 7. Session-level efficiency: energy drawn vs distance, coloured by temperature
    sc = con.execute("""
        SELECT distance_km, net_consumed_wh, avg_oat_c
        FROM gold_session_energy
        WHERE distance_km BETWEEN 0.5 AND 40 AND net_consumed_wh BETWEEN 0 AND 8000
          AND soc_drop_pct > 0""").df()
    fig, ax = plt.subplots(figsize=(9, 6))
    s = ax.scatter(sc.distance_km, sc.net_consumed_wh, c=sc.avg_oat_c, cmap="coolwarm",
                   s=8, alpha=0.4)
    ax.set_title("Energy drawn vs distance per session (colour = temperature)")
    ax.set_xlabel("distance (km)"); ax.set_ylabel("net energy consumed (Wh)")
    fig.colorbar(s, ax=ax, label="avg outside air temp (C)")
    fig.tight_layout(); fig.savefig(PLOTS_DIR / "energy_vs_distance.png", dpi=110); plt.close(fig)

    # 8. One EV trip's battery SOC discharge curve (raw signal, concrete)
    pick = con.execute("""
        SELECT ss.session_key FROM silver_sessions ss
        JOIN gold_session_energy ge USING (session_key)
        WHERE ge.engine_type = 'EV' AND ss.distance_km > 5 AND ge.soc_drop_pct > 8
          AND ss.n_pings > 200
        ORDER BY ss.n_pings DESC LIMIT 1""").fetchone()
    if pick:
        sk = pick[0]
        curve = con.execute(f"""
            SELECT ts_ms / 60000.0 AS minutes, hv_soc_pct FROM silver
            WHERE session_key = '{sk}' AND hv_soc_pct IS NOT NULL ORDER BY ts_ms""").df()
        fig, ax = plt.subplots(figsize=(9, 5))
        ax.plot(curve.minutes, curve.hv_soc_pct, color="#1d9e75")
        ax.set_title(f"Battery SOC over one EV trip (session {sk})")
        ax.set_xlabel("minutes into session"); ax.set_ylabel("HV battery SOC (%)")
        fig.tight_layout(); fig.savefig(PLOTS_DIR / "soc_discharge_curve.png", dpi=110); plt.close(fig)

    # 9. Telemetry volume across the year (one bar per weekly file)
    vol = con.execute("SELECT source_file, COUNT(*) AS pings FROM bronze GROUP BY 1").df()
    vol["week"] = pd.to_datetime(vol.source_file.str.extract(r"VED_(\d{6})_")[0], format="%y%m%d")
    vol = vol.sort_values("week")
    fig, ax = plt.subplots(figsize=(12, 5))
    ax.bar(vol.week, vol.pings, width=5, color="#4c72b0")
    ax.set_title("Telemetry volume by week (Nov 2017 - Nov 2018)")
    ax.set_xlabel("week"); ax.set_ylabel("pings ingested")
    fig.tight_layout(); fig.savefig(PLOTS_DIR / "volume_over_time.png", dpi=110); plt.close(fig)

    print(f"         9 plots -> {PLOTS_DIR.as_posix()}")


def export_powerbi(con):
    """Write the serving layer as Parquet for Power BI. One flat `sessions` fact
    table (every rebuilt session + its trip metrics + battery energy where it
    exists), plus the vehicle dimension, the DQ scorecard, and the headline
    aggregates. Power BI: Get Data -> Folder, point at output/powerbi/."""
    print("[export] writing Power BI tables (Parquet)...")
    pdir = OUTPUT_DIR / "powerbi"
    pdir.mkdir(exist_ok=True)
    con.execute(f"""
        COPY (
          SELECT ss.*,
                 v.engine_type, v.vehicle_class, v.weight,
                 ge.discharge_wh, ge.regen_wh, ge.net_consumed_wh,
                 ge.soc_drop_pct, ge.soc_pct_per_km, ge.wh_per_km
          FROM silver_sessions ss
          LEFT JOIN vehicles v           ON ss.veh_id = v.veh_id
          LEFT JOIN gold_session_energy ge USING (session_key)
        ) TO '{(pdir / 'sessions.parquet').as_posix()}' (FORMAT PARQUET)""")
    for tbl in ["vehicles", "pipeline_health", "gold_efficiency_by_temp", "gold_hvac_by_temp"]:
        con.execute(f"COPY {tbl} TO '{(pdir / (tbl + '.parquet')).as_posix()}' (FORMAT PARQUET)")
    print(f"         5 tables -> {pdir.as_posix()}")


def main():
    ap = argparse.ArgumentParser(description="VED bronze->silver->gold pipeline")
    ap.add_argument("--sample", type=int, default=None,
                    help="Ingest only the first N weekly files (dev mode).")
    ap.add_argument("--layer", choices=["all", "bronze", "dq", "silver", "gold", "plots", "export"],
                    default="all")
    args = ap.parse_args()

    OUTPUT_DIR.mkdir(exist_ok=True); PLOTS_DIR.mkdir(exist_ok=True)
    t0 = time.time()
    con = duckdb.connect(str(DB_PATH))
    con.execute("SET memory_limit='6GB'")
    con.execute(f"SET temp_directory='{(OUTPUT_DIR / 'duck_tmp').as_posix()}'")
    try:
        load_vehicles(con)
        if args.layer in ("all", "bronze"): build_bronze(con, args.sample)
        if args.layer in ("all", "dq"):     build_dq(con)
        if args.layer in ("all", "silver"): build_silver(con)
        if args.layer in ("all", "gold"):   build_gold(con)
        if args.layer in ("all", "plots"):  make_plots(con)
        if args.layer in ("all", "export"): export_powerbi(con)
    finally:
        con.close()
    print(f"Done in {time.time() - t0:.0f}s.")


if __name__ == "__main__":
    main()
