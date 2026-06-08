"""
Partitioned Parquet storage with idempotent (deduplication) writes.

Partition scheme:
  data/year=YYYY/month=MM/day=DD/zone=ZZ/part_NNN.parquet

Why this partition layout?
  - Firmware engineers filter by date range and zone → both are partition keys,
    so the query engine skips irrelevant files entirely (partition pruning).
  - Columnar Parquet means reading only the columns you need
    (e.g. temperature, status) without loading all 15+ fields.
  - Hive-style path naming lets DuckDB, Presto, Spark auto-discover partitions.

Idempotent writes:
  - Each batch write checks record_id against a seen-set loaded from a
    small bloom-filter-style index file.
  - Duplicate record_ids are skipped, so re-running an ingestion job
    after a failure won't create double-counted rows.
"""

import io
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from decoder import HddRecord

logger = logging.getLogger(__name__)

SCHEMA_VERSION = 1

SCHEMA = pa.schema([
    pa.field("record_id",           pa.string()),
    pa.field("machine_id",          pa.int32()),
    pa.field("factory_zone",        pa.string()),
    pa.field("firmware_version",    pa.string()),
    pa.field("timestamp_utc",       pa.timestamp("ms", tz="UTC")),
    pa.field("serial_number",       pa.string()),
    pa.field("test_phase",          pa.string()),
    pa.field("status",              pa.string()),
    pa.field("temperature_c",       pa.float32()),
    pa.field("rpm",                 pa.int32()),
    pa.field("read_mbps",           pa.float32()),
    pa.field("write_mbps",          pa.float32()),
    pa.field("reallocated_sectors", pa.int16()),
    pa.field("pending_sectors",     pa.int16()),
    pa.field("vibration_rms_mg",    pa.float32()),
    pa.field("raw_bytes_b64",       pa.string()),
])

RECORDS_PER_FILE = 5_000


class ParquetStorage:
    def __init__(self, data_dir: Path,
                 s3_bucket: str = "",
                 s3_prefix: str = "hdd-data",
                 aws_region: str = "us-west-2"):
        self._root = Path(data_dir)
        self._root.mkdir(parents=True, exist_ok=True)
        self._s3_bucket = s3_bucket
        self._s3_prefix = s3_prefix.rstrip("/")
        self._s3 = boto3.client("s3", region_name=aws_region) if s3_bucket else None
        self._dedup_index: set[str] = self._load_dedup_index()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def write_batch(self, records: Sequence[HddRecord]) -> dict:
        """
        Write a batch of HddRecords to Parquet, skipping duplicates.
        Returns a summary dict with counts.
        """
        new_records, duplicates = self._filter_duplicates(records)
        if not new_records:
            logger.info("write_batch: all %d records were duplicates", len(records))
            return {"written": 0, "duplicates": duplicates, "total": len(records)}

        # Group by partition key (date + zone) for efficient file placement
        groups: dict[tuple, list[HddRecord]] = {}
        for rec in new_records:
            key = self._partition_key(rec)
            groups.setdefault(key, []).append(rec)

        written = 0
        for partition_key, group in groups.items():
            written += self._write_partition_group(partition_key, group)

        self._save_dedup_index()
        self._save_last_ingested()
        logger.info("write_batch: wrote %d, skipped %d duplicates",
                    written, duplicates)
        return {"written": written, "duplicates": duplicates, "total": len(records)}

    def root_path(self) -> Path:
        return self._root

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _partition_key(self, rec: HddRecord) -> tuple[int, int, int, str]:
        ts = rec.timestamp_utc
        return (ts.year, ts.month, ts.day, rec.factory_zone)

    def _partition_dir(self, key: tuple) -> Path:
        year, month, day, zone = key
        path = (self._root
                / f"year={year}"
                / f"month={month:02d}"
                / f"day={day:02d}"
                / f"zone={zone}")
        path.mkdir(parents=True, exist_ok=True)
        return path

    def _next_part_path(self, partition_dir: Path) -> Path:
        existing = sorted(partition_dir.glob("part_*.parquet"))
        n = len(existing)
        return partition_dir / f"part_{n:04d}.parquet"

    def _s3_key(self, key: tuple, part_n: int) -> str:
        year, month, day, zone = key
        return (f"{self._s3_prefix}/year={year}/month={month:02d}"
                f"/day={day:02d}/zone={zone}/part_{part_n:04d}.parquet")

    def _write_partition_group(self, key: tuple,
                                group: list[HddRecord]) -> int:
        """Splits a group into RECORDS_PER_FILE-sized Parquet files."""
        pdir  = self._partition_dir(key)
        total = 0

        for chunk_start in range(0, len(group), RECORDS_PER_FILE):
            chunk = group[chunk_start: chunk_start + RECORDS_PER_FILE]
            table = self._records_to_arrow(chunk)

            if self._s3:
                # Write to an in-memory buffer and upload directly — no temp file needed
                part_n  = len(list(pdir.glob("part_*.parquet"))) + chunk_start // RECORDS_PER_FILE
                s3_key  = self._s3_key(key, part_n)
                buf     = io.BytesIO()
                pq.write_table(table, buf, compression="snappy")
                buf.seek(0)
                self._s3.upload_fileobj(buf, self._s3_bucket, s3_key)
                logger.debug("uploaded %d records → s3://%s/%s",
                             len(chunk), self._s3_bucket, s3_key)
            else:
                path = self._next_part_path(pdir)
                tmp  = path.with_suffix(".parquet.tmp")
                pq.write_table(table, tmp, compression="snappy")
                tmp.rename(path)
                logger.debug("wrote %d records → %s", len(chunk), path)

            total += len(chunk)

        return total

    def _records_to_arrow(self, records: list[HddRecord]) -> pa.Table:
        rows = [r.to_dict() for r in records]
        df   = pd.DataFrame(rows)
        # Convert ISO string back to datetime for proper Parquet timestamp type
        df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True, format="ISO8601")
        table = pa.Table.from_pandas(df, schema=SCHEMA, preserve_index=False)
        return table.replace_schema_metadata({"schema_version": str(SCHEMA_VERSION)})

    # ------------------------------------------------------------------
    # Deduplication index  (in production: a Redis set or bloom filter)
    # ------------------------------------------------------------------

    def _dedup_index_path(self) -> Path:
        return self._root / "_dedup_index.txt"

    def _load_dedup_index(self) -> set[str]:
        p = self._dedup_index_path()
        if not p.exists():
            return set()
        return set(p.read_text().splitlines())

    def _save_dedup_index(self) -> None:
        self._dedup_index_path().write_text("\n".join(self._dedup_index))

    def _filter_duplicates(
        self, records: Sequence[HddRecord]
    ) -> tuple[list[HddRecord], int]:
        new, dup_count = [], 0
        for r in records:
            if r.record_id in self._dedup_index:
                dup_count += 1
            else:
                self._dedup_index.add(r.record_id)
                new.append(r)
        return new, dup_count

    # ------------------------------------------------------------------
    # Freshness tracking
    # ------------------------------------------------------------------

    def _last_ingested_path(self) -> Path:
        return self._root / "_last_ingested.txt"

    def _save_last_ingested(self) -> None:
        self._last_ingested_path().write_text(
            datetime.now(timezone.utc).isoformat()
        )

    def last_ingested_at(self) -> datetime | None:
        p = self._last_ingested_path()
        if not p.exists():
            return None
        return datetime.fromisoformat(p.read_text().strip())
