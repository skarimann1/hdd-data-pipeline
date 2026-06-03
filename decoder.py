"""
Decodes binary-encoded HDD machine frames into Python dicts.

Steps:
  1. base64-decode the transport envelope
  2. Verify magic bytes (fast rejection of non-HDD frames)
  3. Verify CRC32 checksum (detect bit-flip / transmission errors)
  4. Unpack the struct fields into typed values
  5. Return a normalized record dict

Raises DecodeError for any corrupt or unrecognised frame.
The ingestion layer catches these and routes them to the dead-letter queue.
"""

import struct
import zlib
import base64
import binascii
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from generator import (MAGIC, PROTOCOL_VERSION, FRAME_FORMAT,
                       FRAME_SIZE_NO_CRC, FULL_FRAME_SIZE,
                       TEST_PHASES, STATUSES, FACTORY_ZONES, FIRMWARE_MAP)


class DecodeError(Exception):
    """Raised when a frame cannot be decoded — routed to dead-letter queue."""


@dataclass
class HddRecord:
    record_id:          str         # SHA-1 of serial + timestamp (idempotency key)
    machine_id:         int
    factory_zone:       str
    firmware_version:   str
    timestamp_utc:      datetime
    serial_number:      str
    test_phase:         str
    status:             str
    temperature_c:      float
    rpm:                int
    read_mbps:          float
    write_mbps:         float
    reallocated_sectors: int
    pending_sectors:    int
    vibration_rms_mg:   float
    raw_bytes_b64:      str         # kept for audit / reprocessing

    def to_dict(self) -> dict[str, Any]:
        d = self.__dict__.copy()
        d["timestamp_utc"] = self.timestamp_utc.isoformat()
        return d


def decode_frame(b64_frame: bytes | str) -> HddRecord:
    """
    Decodes a single base64-encoded machine frame.
    Raises DecodeError if the frame is malformed, corrupt, or unknown version.
    """
    # --- 1. base64 decode ---
    try:
        raw = base64.b64decode(b64_frame)
    except (binascii.Error, ValueError) as exc:
        raise DecodeError(f"base64 decode failed: {exc}") from exc

    if len(raw) != FULL_FRAME_SIZE:
        raise DecodeError(
            f"unexpected frame length {len(raw)}, expected {FULL_FRAME_SIZE}"
        )

    # --- 2. Magic bytes ---
    if raw[:4] != MAGIC:
        raise DecodeError(f"bad magic bytes: {raw[:4].hex()}")

    # --- 3. CRC32 ---
    payload, received_crc_bytes = raw[:-4], raw[-4:]
    expected_crc = zlib.crc32(payload) & 0xFFFFFFFF
    (received_crc,) = struct.unpack("!I", received_crc_bytes)
    if expected_crc != received_crc:
        raise DecodeError(
            f"CRC mismatch: expected {expected_crc:#010x}, got {received_crc:#010x}"
        )

    # --- 4. Unpack struct ---
    try:
        (magic, version, machine_id, timestamp_ms,
         serial_bytes, phase, status,
         temp_tenths, rpm_hundreds,
         read_kbps, write_kbps,
         realloc, pending, vib_mg) = struct.unpack(FRAME_FORMAT, payload)
    except struct.error as exc:
        raise DecodeError(f"struct unpack failed: {exc}") from exc

    if version != PROTOCOL_VERSION:
        raise DecodeError(f"unsupported protocol version: {version}")

    # --- 5. Normalise ---
    serial = serial_bytes.rstrip(b"\x00").decode("ascii", errors="replace")
    ts     = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)

    import hashlib
    record_id = hashlib.sha1(
        f"{serial}:{timestamp_ms}:{machine_id}".encode()
    ).hexdigest()

    return HddRecord(
        record_id          = record_id,
        machine_id         = machine_id,
        factory_zone       = FACTORY_ZONES.get(machine_id, "UNKNOWN"),
        firmware_version   = FIRMWARE_MAP.get(machine_id, "UNKNOWN"),
        timestamp_utc      = ts,
        serial_number      = serial,
        test_phase         = TEST_PHASES.get(phase, f"unknown_{phase}"),
        status             = STATUSES.get(status, f"unknown_{status}"),
        temperature_c      = round(temp_tenths / 10, 1),
        rpm                = rpm_hundreds * 100,
        read_mbps          = round(read_kbps / 1024, 2),
        write_mbps         = round(write_kbps / 1024, 2),
        reallocated_sectors= realloc,
        pending_sectors    = pending,
        vibration_rms_mg   = round(vib_mg, 3),
        raw_bytes_b64      = (b64_frame if isinstance(b64_frame, str)
                              else b64_frame.decode()),
    )
