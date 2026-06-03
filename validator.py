"""
Schema validation for decoded HDD records.

A record is valid if all business rules pass. Invalid records are written
to a dead-letter queue (DLQ) directory with the rejection reason attached,
so ops can inspect and reprocess them without losing data.

Why a DLQ matters in manufacturing pipelines:
  - Machines occasionally send out-of-range sensor values during calibration.
  - A failed record must never silently drop — it needs human review.
  - DLQ enables retroactive reprocessing once the root cause is fixed.
"""

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any

from decoder import HddRecord

logger = logging.getLogger(__name__)

# Business rules — these would come from machine specs / firmware docs.
RULES = {
    "temperature_c":       (-10.0,   85.0),   # °C — drive operating range
    "rpm":                 (4000,    12000),
    "read_mbps":           (1.0,     800.0),
    "write_mbps":          (1.0,     600.0),
    "reallocated_sectors": (0,       100),
    "pending_sectors":     (0,       50),
    "vibration_rms_mg":    (0.0,     200.0),
}

# Timestamps more than 24 h in the future or 30 days old are suspicious.
MAX_FUTURE_SKEW  = timedelta(hours=24)
MAX_RECORD_AGE   = timedelta(days=30)


@dataclass
class ValidationResult:
    valid:    bool
    record:   HddRecord
    errors:   list[str]


def validate(record: HddRecord) -> ValidationResult:
    errors: list[str] = []

    # Range checks
    for field, (lo, hi) in RULES.items():
        val = getattr(record, field)
        if not (lo <= val <= hi):
            errors.append(f"{field}={val} out of range [{lo}, {hi}]")

    # Timestamp staleness / future-dating
    now = datetime.now(timezone.utc)
    age = now - record.timestamp_utc
    skew = record.timestamp_utc - now
    if skew > MAX_FUTURE_SKEW:
        errors.append(f"timestamp {record.timestamp_utc.isoformat()} is "
                      f"{skew.total_seconds()/3600:.1f}h in the future")
    if age > MAX_RECORD_AGE:
        errors.append(f"record is {age.days} days old (max {MAX_RECORD_AGE.days})")

    # Serial number format sanity (not empty, reasonable length)
    sn = record.serial_number
    if not sn or len(sn) < 5 or len(sn) > 20:
        errors.append(f"invalid serial_number format: '{sn}'")

    # Known test phase / status values
    valid_phases   = {"burn_in", "vibration", "acoustic", "cert_read_write"}
    valid_statuses = {"pass", "fail", "in_progress", "error"}
    if record.test_phase not in valid_phases:
        errors.append(f"unknown test_phase: '{record.test_phase}'")
    if record.status not in valid_statuses:
        errors.append(f"unknown status: '{record.status}'")

    return ValidationResult(valid=not errors, record=record, errors=errors)


class DeadLetterQueue:
    """
    Writes rejected records to a directory as NDJSON files, one per hour.

    Each line: {"record": {...}, "errors": [...], "rejected_at": "..."}

    Ops can tail the file, grep for patterns, or bulk-reprocess after a fix.
    """

    def __init__(self, dlq_dir: Path):
        self._dir = Path(dlq_dir)
        self._dir.mkdir(parents=True, exist_ok=True)
        self._count = 0

    def _current_file(self) -> Path:
        # Partition DLQ by hour so files stay manageable
        hour_str = datetime.now(timezone.utc).strftime("%Y%m%d_%H")
        return self._dir / f"dlq_{hour_str}.ndjson"

    def write(self, result: ValidationResult) -> None:
        entry: dict[str, Any] = {
            "rejected_at": datetime.now(timezone.utc).isoformat(),
            "errors":      result.errors,
            "record":      result.record.to_dict(),
        }
        with self._current_file().open("a") as fh:
            fh.write(json.dumps(entry) + "\n")
        self._count += 1
        logger.warning("DLQ: %s | %s", result.record.record_id, result.errors)

    @property
    def count(self) -> int:
        return self._count

    def all_entries(self) -> list[dict]:
        entries = []
        for f in sorted(self._dir.glob("dlq_*.ndjson")):
            with f.open() as fh:
                for line in fh:
                    line = line.strip()
                    if line:
                        entries.append(json.loads(line))
        return entries
