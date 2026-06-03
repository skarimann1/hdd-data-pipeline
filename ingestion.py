"""
Ingestion layer — ties together decoding, validation, and storage.

Two modes:

  BATCH ingestion
    - Used for historical backfill or end-of-shift bulk uploads.
    - Processes all frames at once, writes to Parquet in sorted order.
    - Trade-off: higher latency to first record, but higher throughput
      and better Parquet file consolidation (fewer, fuller files).

  STREAMING ingestion
    - Simulates a live feed from machines (e.g. Kafka consumer or TCP socket).
    - Processes frames one at a time in arrival order.
    - Trade-off: low latency, but many small Parquet files need periodic
      compaction (not implemented here — would be a scheduled Spark job).

Both modes:
  - Decode via decoder.py
  - Validate via validator.py → bad records → DeadLetterQueue
  - Write via storage.py (idempotent)
"""

import logging
import time
from pathlib import Path
from typing import Iterable, Iterator

from decoder import decode_frame, DecodeError, HddRecord
from validator import validate, DeadLetterQueue
from storage import ParquetStorage

logger = logging.getLogger(__name__)


class IngestionStats:
    def __init__(self):
        self.total       = 0
        self.decoded_ok  = 0
        self.decode_err  = 0
        self.valid       = 0
        self.invalid     = 0
        self.written     = 0
        self.duplicates  = 0
        self.elapsed_s   = 0.0

    def __str__(self) -> str:
        tps = self.total / self.elapsed_s if self.elapsed_s else 0
        return (
            f"  Total frames    : {self.total:>7,}\n"
            f"  Decoded OK      : {self.decoded_ok:>7,}\n"
            f"  Decode errors   : {self.decode_err:>7,}  → DLQ (corrupt/bad CRC)\n"
            f"  Schema valid    : {self.valid:>7,}\n"
            f"  Schema invalid  : {self.invalid:>7,}  → DLQ (out-of-range values)\n"
            f"  Written         : {self.written:>7,}\n"
            f"  Duplicates skip : {self.duplicates:>7,}  (idempotent write)\n"
            f"  Elapsed         : {self.elapsed_s:>7.2f}s\n"
            f"  Throughput      : {tps:>7.0f} frames/s"
        )


def _process_frames(
    frames: Iterable[bytes],
    storage: ParquetStorage,
    dlq: DeadLetterQueue,
    batch_size: int = 500,
) -> tuple[list[HddRecord], IngestionStats]:
    """
    Core loop shared by both batch and streaming modes.
    Returns the list of valid records AND stats.
    """
    stats     = IngestionStats()
    valid_buf: list[HddRecord] = []
    all_valid: list[HddRecord] = []
    t0 = time.perf_counter()

    for frame in frames:
        stats.total += 1

        # --- decode ---
        try:
            record = decode_frame(frame)
            stats.decoded_ok += 1
        except DecodeError as exc:
            stats.decode_err += 1
            # Build a minimal ValidationResult shell just for the DLQ writer
            from decoder import HddRecord
            from validator import ValidationResult
            from dataclasses import fields
            dummy = _make_dummy_record(frame, str(exc))
            dlq.write(ValidationResult(valid=False, record=dummy, errors=[str(exc)]))
            continue

        # --- validate ---
        result = validate(record)
        if result.valid:
            stats.valid += 1
            valid_buf.append(record)
        else:
            stats.invalid += 1
            dlq.write(result)

        # --- flush buffer ---
        if len(valid_buf) >= batch_size:
            write_result = storage.write_batch(valid_buf)
            stats.written    += write_result["written"]
            stats.duplicates += write_result["duplicates"]
            all_valid.extend(valid_buf)
            valid_buf.clear()

    # --- final flush ---
    if valid_buf:
        write_result = storage.write_batch(valid_buf)
        stats.written    += write_result["written"]
        stats.duplicates += write_result["duplicates"]
        all_valid.extend(valid_buf)

    stats.elapsed_s = time.perf_counter() - t0
    return all_valid, stats


def run_batch_ingestion(
    frames: Iterable[bytes],
    data_dir: Path,
    dlq_dir: Path,
) -> IngestionStats:
    """
    Batch mode: collect all frames first, sort by timestamp, then write.
    Sorted writes produce better-compressed Parquet and cleaner partitions.
    """
    logger.info("batch ingestion started")
    storage = ParquetStorage(data_dir)
    dlq     = DeadLetterQueue(dlq_dir)

    # First pass: decode everything into memory (feasible for batch jobs)
    raw_frames    = list(frames)
    valid_records: list[HddRecord] = []
    stats = IngestionStats()
    t0 = time.perf_counter()

    for frame in raw_frames:
        stats.total += 1
        try:
            record = decode_frame(frame)
            stats.decoded_ok += 1
        except DecodeError as exc:
            stats.decode_err += 1
            from validator import ValidationResult
            dummy = _make_dummy_record(frame, str(exc))
            dlq.write(ValidationResult(valid=False, record=dummy, errors=[str(exc)]))
            continue

        result = validate(record)
        if result.valid:
            stats.valid += 1
            valid_records.append(record)
        else:
            stats.invalid += 1
            dlq.write(result)

    # Sort by timestamp before writing — improves compression and query perf
    valid_records.sort(key=lambda r: r.timestamp_utc)

    write_result    = storage.write_batch(valid_records)
    stats.written   = write_result["written"]
    stats.duplicates = write_result["duplicates"]
    stats.elapsed_s = time.perf_counter() - t0

    logger.info("batch ingestion complete\n%s", stats)
    return stats


def run_streaming_ingestion(
    frame_iterator: Iterator[bytes],
    data_dir: Path,
    dlq_dir: Path,
    flush_every: int = 200,
) -> IngestionStats:
    """
    Streaming mode: process and write frames as they arrive.
    flush_every controls write latency vs. file fragmentation trade-off.
    """
    logger.info("streaming ingestion started (flush_every=%d)", flush_every)
    storage = ParquetStorage(data_dir)
    dlq     = DeadLetterQueue(dlq_dir)
    _, stats = _process_frames(frame_iterator, storage, dlq, batch_size=flush_every)
    logger.info("streaming ingestion complete\n%s", stats)
    return stats


# ---------------------------------------------------------------------------
# Helper — creates a stub HddRecord for decode failures (so DLQ has a record)
# ---------------------------------------------------------------------------

def _make_dummy_record(raw_frame: bytes, error_msg: str):
    from decoder import HddRecord
    from datetime import timezone
    return HddRecord(
        record_id          = "DECODE_ERROR",
        machine_id         = -1,
        factory_zone       = "UNKNOWN",
        firmware_version   = "UNKNOWN",
        timestamp_utc      = __import__("datetime").datetime.now(timezone.utc),
        serial_number      = "UNKNOWN",
        test_phase         = "UNKNOWN",
        status             = "error",
        temperature_c      = 0.0,
        rpm                = 0,
        read_mbps          = 0.0,
        write_mbps         = 0.0,
        reallocated_sectors= 0,
        pending_sectors    = 0,
        vibration_rms_mg   = 0.0,
        raw_bytes_b64      = (raw_frame.decode() if isinstance(raw_frame, bytes)
                              else str(raw_frame))[:200],
    )
