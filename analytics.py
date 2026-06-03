"""
Analytics layer — DuckDB over Parquet files.

Why DuckDB / Presto instead of querying MySQL row-by-row?
  - Columnar Parquet + vectorised execution reads only the columns needed.
  - Partition pruning skips entire directories based on WHERE predicates.
  - Aggregations over millions of rows run in seconds, not minutes.
  - No server to manage — DuckDB runs in-process.

These are the kinds of queries firmware engineers actually run:
  - "Show me all drives that failed cert in zone B1 this week"
  - "What's the average read throughput by firmware version?"
  - "Which machines are running hot (> 50 °C)?"
  - "How many reallocated sectors appeared in the last 24 hours?"
"""

from pathlib import Path
import duckdb


class HddAnalytics:
    def __init__(self, data_dir: Path = None,
                 s3_bucket: str = "",
                 s3_prefix: str = "hdd-data",
                 aws_access_key: str = "",
                 aws_secret_key: str = "",
                 aws_region: str = "us-west-2"):
        self._con = duckdb.connect()

        if s3_bucket and aws_access_key:
            # S3 mode — DuckDB reads Parquet directly from S3 via httpfs.
            # Same queries work unchanged; no Athena needed for a dashboard.
            self._con.execute("INSTALL httpfs; LOAD httpfs;")
            self._con.execute(f"SET s3_region='{aws_region}';")
            self._con.execute(f"SET s3_access_key_id='{aws_access_key}';")
            self._con.execute(f"SET s3_secret_access_key='{aws_secret_key}';")
            glob = f"s3://{s3_bucket}/{s3_prefix.rstrip('/')}/**/*.parquet"
        else:
            # Local mode — reads from the local data directory
            glob = str(Path(data_dir) / "**" / "*.parquet")

        self._con.execute(f"""
            CREATE OR REPLACE VIEW hdd_data AS
            SELECT * FROM read_parquet('{glob}', hive_partitioning=true)
        """)

    def query(self, sql: str) -> "duckdb.DuckDBPyRelation":
        return self._con.execute(sql).df()

    # ------------------------------------------------------------------
    # Canned queries for firmware / QA engineers
    # ------------------------------------------------------------------

    def fail_rate_by_phase(self) -> "pd.DataFrame":
        """Pass/fail breakdown per test phase — overall quality signal."""
        return self.query("""
            SELECT
                test_phase,
                COUNT(*) AS total,
                SUM(CASE WHEN status = 'fail'  THEN 1 ELSE 0 END) AS failed,
                SUM(CASE WHEN status = 'pass'  THEN 1 ELSE 0 END) AS passed,
                ROUND(100.0 * SUM(CASE WHEN status = 'fail' THEN 1 ELSE 0 END)
                      / COUNT(*), 2) AS fail_pct
            FROM hdd_data
            GROUP BY test_phase
            ORDER BY fail_pct DESC
        """)

    def throughput_by_firmware(self) -> "pd.DataFrame":
        """Average read/write MB/s per firmware version — regression detection."""
        return self.query("""
            SELECT
                firmware_version,
                ROUND(AVG(read_mbps),  2) AS avg_read_mbps,
                ROUND(AVG(write_mbps), 2) AS avg_write_mbps,
                COUNT(*) AS sample_count
            FROM hdd_data
            WHERE status IN ('pass', 'fail')
            GROUP BY firmware_version
            ORDER BY firmware_version
        """)

    def hot_machines(self, threshold_c: float = 50.0) -> "pd.DataFrame":
        """Machines with avg temperature above threshold — cooling issue detector."""
        return self.query(f"""
            SELECT
                machine_id,
                factory_zone,
                ROUND(AVG(temperature_c), 1) AS avg_temp_c,
                ROUND(MAX(temperature_c), 1) AS max_temp_c,
                COUNT(*) AS readings
            FROM hdd_data
            GROUP BY machine_id, factory_zone
            HAVING AVG(temperature_c) > {threshold_c}
            ORDER BY avg_temp_c DESC
        """)

    def reallocated_sector_trend(self) -> "pd.DataFrame":
        """Daily count of drives with any reallocated sectors — early wear signal."""
        return self.query("""
            SELECT
                CAST(timestamp_utc AS DATE) AS date,
                COUNT(*) AS drives_with_realloc,
                SUM(reallocated_sectors) AS total_realloc_sectors
            FROM hdd_data
            WHERE reallocated_sectors > 0
            GROUP BY CAST(timestamp_utc AS DATE)
            ORDER BY date
        """)

    def zone_failure_heatmap(self) -> "pd.DataFrame":
        """Failure counts by factory zone — spot problem areas on the floor."""
        return self.query("""
            SELECT
                factory_zone,
                test_phase,
                SUM(CASE WHEN status = 'fail' THEN 1 ELSE 0 END) AS failures,
                COUNT(*) AS total,
                ROUND(100.0 * SUM(CASE WHEN status = 'fail' THEN 1 ELSE 0 END)
                      / COUNT(*), 2) AS fail_pct
            FROM hdd_data
            GROUP BY factory_zone, test_phase
            ORDER BY factory_zone, fail_pct DESC
        """)

    def recent_failures(self, hours: int = 24) -> "pd.DataFrame":
        """Raw records for drives that failed in the last N hours — for triage."""
        return self.query(f"""
            SELECT
                timestamp_utc, serial_number, machine_id, factory_zone,
                firmware_version, test_phase, temperature_c,
                read_mbps, write_mbps, reallocated_sectors, vibration_rms_mg
            FROM hdd_data
            WHERE status = 'fail'
              AND timestamp_utc >= NOW() - INTERVAL '{hours}' HOUR
            ORDER BY timestamp_utc DESC
            LIMIT 100
        """)

    def vibration_outliers(self, sigma: float = 2.5) -> "pd.DataFrame":
        """Drives with vibration more than N std-devs above mean — mechanical issues."""
        return self.query(f"""
            WITH stats AS (
                SELECT AVG(vibration_rms_mg) AS mu,
                       STDDEV(vibration_rms_mg) AS sigma
                FROM hdd_data
            )
            SELECT
                serial_number, machine_id, factory_zone,
                ROUND(vibration_rms_mg, 3) AS vibration_rms_mg,
                ROUND((vibration_rms_mg - stats.mu) / NULLIF(stats.sigma, 0), 2)
                    AS z_score,
                timestamp_utc
            FROM hdd_data, stats
            WHERE vibration_rms_mg > stats.mu + {sigma} * stats.sigma
            ORDER BY z_score DESC
            LIMIT 50
        """)

    def summary_stats(self) -> "pd.DataFrame":
        """Single-row dashboard summary."""
        return self.query("""
            SELECT
                COUNT(DISTINCT serial_number)  AS unique_drives,
                COUNT(DISTINCT machine_id)     AS active_machines,
                COUNT(*)                       AS total_records,
                ROUND(100.0 * SUM(CASE WHEN status = 'fail' THEN 1 ELSE 0 END)
                      / COUNT(*), 2)           AS overall_fail_pct,
                ROUND(AVG(temperature_c), 1)   AS avg_temp_c,
                ROUND(AVG(read_mbps),  1)      AS avg_read_mbps,
                ROUND(AVG(write_mbps), 1)      AS avg_write_mbps,
                MIN(timestamp_utc)             AS earliest_record,
                MAX(timestamp_utc)             AS latest_record
            FROM hdd_data
        """)
