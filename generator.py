"""
Simulates HDD manufacturing machines emitting binary-encoded telemetry.

Real machines output structured binary frames over serial/TCP.
We replicate that with Python's struct module — same concepts, dummy data.

Binary frame layout (big-endian, 59 bytes):
  Offset  Size  Type    Field
  0       4     char[]  magic "WDHD"
  4       1     uint8   protocol version
  5       4     uint32  machine_id
  9       8     uint64  timestamp_ms (unix epoch)
  17      16    char[]  drive serial number (null-padded)
  33      1     uint8   test_phase
  34      1     uint8   status
  35      2     int16   temperature_tenths_c  (250 = 25.0 °C)
  37      2     uint16  rpm_hundreds          (72 = 7200 RPM)
  39      4     uint32  read_kbps
  43      4     uint32  write_kbps
  47      2     uint16  reallocated_sectors
  49      2     uint16  pending_sectors
  51      4     float32 vibration_rms_mg
  55      4     uint32  crc32 checksum
Total: 59 bytes → base64-encoded for transport (~80 chars)
"""

import struct
import random
import string
import zlib
import base64
import time
from datetime import datetime, timezone, timedelta
from typing import Iterator

MAGIC = b"WDHD"
PROTOCOL_VERSION = 1

# fmt: off
FRAME_FORMAT = "!4sBIQ16sBBhHIIHHf"  # 55 bytes before checksum
# fmt: on
FRAME_SIZE_NO_CRC = struct.calcsize(FRAME_FORMAT)  # 55
FULL_FRAME_SIZE   = FRAME_SIZE_NO_CRC + 4           # 59

TEST_PHASES = {0: "burn_in", 1: "vibration", 2: "acoustic", 3: "cert_read_write"}
STATUSES    = {0: "pass", 1: "fail", 2: "in_progress", 3: "error"}

MACHINE_IDS     = list(range(1001, 1017))   # 16 test stations
FACTORY_ZONES   = {1001: "A1", 1002: "A1", 1003: "A2", 1004: "A2",
                   1005: "B1", 1006: "B1", 1007: "B2", 1008: "B2",
                   1009: "C1", 1010: "C1", 1011: "C2", 1012: "C2",
                   1013: "D1", 1014: "D1", 1015: "D2", 1016: "D2"}
FIRMWARE_MAP    = {1001: "FW_7.4.2", 1002: "FW_7.4.2", 1003: "FW_7.5.0",
                   1004: "FW_7.5.0", 1005: "FW_7.4.2", 1006: "FW_7.5.0",
                   1007: "FW_7.5.0", 1008: "FW_7.6.1", 1009: "FW_7.6.1",
                   1010: "FW_7.6.1", 1011: "FW_7.6.1", 1012: "FW_7.5.0",
                   1013: "FW_7.5.0", 1014: "FW_7.4.2", 1015: "FW_7.6.1",
                   1016: "FW_7.6.1"}


def _random_serial() -> str:
    prefix = random.choice(["WDC", "HTS", "MFR"])
    digits = "".join(random.choices(string.digits, k=10))
    return f"{prefix}{digits}"


def _encode_frame(machine_id: int, timestamp_ms: int, serial: str,
                  phase: int, status: int, temp_c: float,
                  rpm: int, read_kbps: int, write_kbps: int,
                  realloc: int, pending: int, vib_mg: float) -> bytes:
    serial_bytes = serial.encode()[:16].ljust(16, b"\x00")
    temp_tenths  = int(temp_c * 10)
    rpm_hundreds = rpm // 100

    body = struct.pack(
        FRAME_FORMAT,
        MAGIC, PROTOCOL_VERSION, machine_id, timestamp_ms,
        serial_bytes, phase, status,
        temp_tenths, rpm_hundreds,
        read_kbps, write_kbps,
        realloc, pending, vib_mg,
    )
    crc = zlib.crc32(body) & 0xFFFFFFFF
    return body + struct.pack("!I", crc)


def generate_frame(machine_id: int | None = None,
                   timestamp: datetime | None = None,
                   inject_corrupt: bool = False,
                   inject_duplicate_serial: str | None = None) -> bytes:
    """
    Returns a single base64-encoded binary frame.
    inject_corrupt=True flips a byte to simulate transmission errors.
    inject_duplicate_serial forces a serial to test deduplication.
    """
    mid  = machine_id or random.choice(MACHINE_IDS)
    ts   = timestamp or datetime.now(timezone.utc)
    ts_ms = int(ts.timestamp() * 1000)

    # Realistic HDD manufacturing test distributions
    phase   = random.choices([0, 1, 2, 3], weights=[40, 20, 20, 20])[0]
    # Fail rate ~3%, error rate ~1%
    status  = random.choices([0, 1, 2, 3], weights=[56, 3, 40, 1])[0]
    temp_c  = round(random.gauss(38.0, 4.0), 1)          # ~38°C ± 4
    rpm     = random.choice([5400, 7200, 7200, 10000])
    read_kbps  = int(random.gauss(180_000, 15_000))       # ~180 MB/s
    write_kbps = int(random.gauss(160_000, 20_000))
    realloc = random.choices([0, 0, 0, 1, 2, 5], weights=[80, 8, 5, 4, 2, 1])[0]
    pending = random.choices([0, 0, 1, 2],        weights=[85, 8, 4, 3])[0]
    vib_mg  = round(abs(random.gauss(12.0, 3.5)), 3)

    serial = inject_duplicate_serial or _random_serial()
    raw = _encode_frame(mid, ts_ms, serial, phase, status,
                        temp_c, rpm, read_kbps, write_kbps,
                        realloc, pending, vib_mg)

    if inject_corrupt:
        # Flip a byte in the payload (not the CRC) to fail checksum
        ba = bytearray(raw)
        ba[10] ^= 0xFF
        raw = bytes(ba)

    return base64.b64encode(raw)


def stream_frames(count: int = 100,
                  start_time: datetime | None = None,
                  corrupt_rate: float = 0.02,
                  duplicate_rate: float = 0.01) -> Iterator[bytes]:
    """
    Yields base64-encoded frames simulating a live machine stream.
    Injects corrupt frames and duplicates at specified rates.
    """
    ts = start_time or datetime.now(timezone.utc)
    seen_serials: list[str] = []

    for i in range(count):
        ts += timedelta(seconds=random.uniform(0.05, 0.5))  # ~2–20 frames/sec
        corrupt   = random.random() < corrupt_rate
        duplicate = seen_serials and random.random() < duplicate_rate

        dup_serial = random.choice(seen_serials) if duplicate else None
        frame = generate_frame(timestamp=ts,
                               inject_corrupt=corrupt,
                               inject_duplicate_serial=dup_serial)

        # Track serials for future duplicate injection
        if not corrupt and not duplicate:
            serial_guess = f"SIM{i:010d}"
            seen_serials.append(serial_guess)
            if len(seen_serials) > 500:
                seen_serials.pop(0)

        yield frame


def batch_frames(count: int = 1000,
                 days_back: int = 7) -> Iterator[bytes]:
    """
    Yields frames spread across a historical window — simulates a batch load.
    """
    start = datetime.now(timezone.utc) - timedelta(days=days_back)
    interval = timedelta(days=days_back) / count
    for i in range(count):
        ts = start + interval * i
        yield generate_frame(timestamp=ts)
