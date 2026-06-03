# HDD Manufacturing Data Pipeline — System Design

**Author:** Sakthi Karimanal  
**Role:** Software Engineer — Data Platform  
**Company:** Western Digital  

---

## 1. Problem Statement

HDD manufacturing test stations emit continuous binary-encoded telemetry —
temperature, RPM, read/write throughput, vibration, sector health — for every
drive under test. This data needs to be:

- **Decoded** from proprietary binary frames into structured records
- **Validated** and bad records quarantined without data loss
- **Stored** efficiently for long-term retention
- **Queryable** by firmware and QA engineers for failure analysis and regression detection
- **Visualised** in a live dashboard

---

## 2. Requirements

### Functional
- Ingest binary telemetry from 16+ test stations across 4 factory zones
- Decode, validate, and deduplicate records before storage
- Route invalid records to a Dead-Letter Queue for inspection and reprocessing
- Support both batch (historical backfill) and streaming (live feed) ingestion
- Expose analytics queries via a REST API
- Serve a live web dashboard for firmware engineers

### Non-Functional
- **Correctness:** idempotent writes — re-running ingestion must not create duplicates
- **Durability:** no silent data loss — bad records go to DLQ, not /dev/null
- **Query performance:** aggregations over millions of rows in under 5 seconds
- **Cost efficiency:** pay-per-query model preferred over always-on cluster
- **Security:** AWS credentials never exposed to the browser

---

## 3. High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        FACTORY FLOOR                                │
│                                                                     │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐           │
│  │ Station  │  │ Station  │  │ Station  │  │ Station  │  (×16)     │
│  │  1001    │  │  1002    │  │  1003    │  │  1004    │           │
│  │ Zone A1  │  │ Zone A1  │  │ Zone A2  │  │ Zone A2  │           │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘           │
│       │              │              │              │                │
│       └──────────────┴──────────────┴──────────────┘               │
│                            │                                        │
│               Binary frames (59 bytes, base64)                     │
└───────────────────────────────────────────────────────────────────-┘
                             │
                             ▼
┌────────────────────────────────────────────────────────────────────┐
│                     INGESTION LAYER                                │
│                                                                    │
│   ┌─────────────┐       ┌──────────────┐       ┌───────────────┐  │
│   │  generator  │──────►│   decoder    │──────►│   validator   │  │
│   │  (source)   │       │  CRC32 check │       │ range checks  │  │
│   └─────────────┘       │  struct      │       │ timestamp     │  │
│                         │  unpack      │       │ format checks │  │
│   Batch mode:           └──────────────┘       └──────┬────────┘  │
│   sort → write                                        │            │
│                                               ┌───────┴──────┐    │
│   Stream mode:                                │              │    │
│   flush every N                            VALID          INVALID  │
│                                               │              │    │
│                                               ▼              ▼    │
│                                         dedup check        DLQ    │
│                                         (SHA-1 hash)  (NDJSON)    │
└───────────────────────────────────────────────────────────────────┘
                             │
                             ▼
┌───────────────────────────────────────────────────────────────────┐
│                      STORAGE LAYER                                │
│                                                                   │
│   Amazon S3  (object storage)                                     │
│                                                                   │
│   s3://hddpipeline-100160018995-us-west-2-an/                     │
│   └── hdd-data/                                                   │
│       └── year=2026/                                              │
│           └── month=06/                                           │
│               ├── day=01/                                         │
│               │   ├── zone=A1/part_0000.parquet  (Snappy)         │
│               │   ├── zone=A2/part_0000.parquet                   │
│               │   ├── zone=B1/part_0000.parquet                   │
│               │   └── ...                                        │
│               └── day=02/                                         │
│                   └── ...                                        │
│                                                                   │
│   Format: Parquet (columnar, Snappy compression)                  │
│   Partitions: year / month / day / zone                           │
└───────────────────────────────────────────────────────────────────┘
                             │
                             ▼
┌───────────────────────────────────────────────────────────────────┐
│                      QUERY LAYER                                  │
│                                                                   │
│   DuckDB (in-process, stateless)                                  │
│   - reads Parquet from S3 via httpfs extension                    │
│   - partition pruning skips irrelevant files                      │
│   - column projection reads only requested fields                 │
│   - all queries defined in analytics.py                           │
└───────────────────────────────────────────────────────────────────┘
                             │
                             ▼
┌───────────────────────────────────────────────────────────────────┐
│                        API LAYER                                  │
│                                                                   │
│   FastAPI + Uvicorn  (port 8000)                                  │
│                                                                   │
│   GET /api/summary              → dashboard stat cards            │
│   GET /api/fail-rate-by-phase   → bar chart data                  │
│   GET /api/throughput-by-firmware → firmware regression           │
│   GET /api/zone-heatmap         → failure map by zone             │
│   GET /api/reallocated-trend    → sector health over time         │
│   GET /api/vibration-outliers   → mechanical anomalies            │
│   GET /api/recent-failures      → last N hours triage             │
│   GET /api/hot-machines         → temperature alerts              │
└───────────────────────────────────────────────────────────────────┘
                             │
                             ▼
┌───────────────────────────────────────────────────────────────────┐
│                     PRESENTATION LAYER                            │
│                                                                   │
│   dashboard/index.html  (served by FastAPI at GET /)              │
│                                                                   │
│   ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐        │
│   │  Drives  │  │Machines  │  │  Fail %  │  │ Avg Temp │        │
│   │   500    │  │   16     │  │   1.4%   │  │  38.1°C  │        │
│   └──────────┘  └──────────┘  └──────────┘  └──────────┘        │
│                                                                   │
│   ┌─────────────────────┐  ┌─────────────────────┐               │
│   │ Fail Rate by Phase  │  │ Throughput by FW Ver │               │
│   │  [bar chart]        │  │  [grouped bar chart] │               │
│   └─────────────────────┘  └─────────────────────┘               │
│                                                                   │
│   ┌─────────────────────┐  ┌─────────────────────┐               │
│   │ Realloc Sector Trend│  │ Vibration Outliers   │               │
│   │  [line chart]       │  │  [table with z-score]│               │
│   └─────────────────────┘  └─────────────────────┘               │
│                                                                   │
│   ┌───────────────────────────────────────────────┐               │
│   │ Zone × Phase Failure Heatmap  [color table]   │               │
│   └───────────────────────────────────────────────┘               │
└───────────────────────────────────────────────────────────────────┘
```

---

## 4. Binary Frame Format

Each machine emits one frame per measurement cycle (every few seconds per drive).

```
Offset  Size  Type     Field                    Notes
──────────────────────────────────────────────────────────────────
0       4     char[]   magic                    "WDHD" — frame identifier
4       1     uint8    protocol_version         currently 1
5       4     uint32   machine_id               1001–1016
9       8     uint64   timestamp_ms             Unix epoch milliseconds
17      16    char[]   serial_number            null-padded ASCII
33      1     uint8    test_phase               0=burn_in 1=vib 2=acoustic 3=cert
34      1     uint8    status                   0=pass 1=fail 2=in_progress 3=error
35      2     int16    temperature_tenths_c     250 = 25.0 °C
37      2     uint16   rpm_hundreds             72 = 7200 RPM
39      4     uint32   read_kbps
43      4     uint32   write_kbps
47      2     uint16   reallocated_sectors
49      2     uint16   pending_sectors
51      4     float32  vibration_rms_mg
55      4     uint32   crc32_checksum           over bytes 0–54
──────────────────────────────────────────────────────────────────
Total: 59 bytes  →  base64-encoded (~80 chars) for transport
```

---

## 5. Data Flow — Step by Step

```
Machine emits frame
        │
        │  base64("WDHD" + fields + CRC32)
        ▼
ingestion.py receives frame
        │
        ├─► decoder.py
        │     1. base64 decode
        │     2. check magic bytes "WDHD"
        │     3. verify CRC32 checksum
        │     4. struct.unpack all fields
        │     5. build HddRecord dataclass
        │         └─► DecodeError? ──► DLQ (corrupt/bad CRC)
        │
        ├─► validator.py
        │     check: -10 ≤ temp ≤ 85 °C
        │     check: 4000 ≤ rpm ≤ 12000
        │     check: serial length 5–20 chars
        │     check: timestamp not stale / not future
        │         └─► invalid? ──► DLQ (out-of-range values)
        │
        ├─► deduplication
        │     record_id = SHA-1(serial + timestamp_ms + machine_id)
        │         └─► already seen? ──► skip silently
        │
        └─► storage.py
              group by (year, month, day, zone)
              write Parquet with Snappy compression
              upload to S3
```

---

## 6. Storage Design

### Why Parquet over MySQL for this workload

```
Query: SELECT AVG(read_mbps) FROM hdd_data WHERE firmware_version = 'FW_7.6.1'

MySQL (row store)                    Parquet (columnar)
─────────────────                    ──────────────────
Read row 1: all 16 fields     →      Read only read_mbps + firmware_version columns
Read row 2: all 16 fields     →      Skip year/month/day/zone partitions that don't match
Read row 3: all 16 fields     →      Vectorised execution over contiguous column data
...× 10,000,000 rows                 
                                     Result: 10–100x less I/O
```

### Partition pruning example

```
Query: WHERE zone = 'B1' AND timestamp BETWEEN '2026-06-01' AND '2026-06-02'

S3 files scanned:
  ✓  year=2026/month=06/day=01/zone=B1/part_0000.parquet
  ✗  year=2026/month=06/day=01/zone=A1/  ← skipped (wrong zone)
  ✗  year=2026/month=06/day=01/zone=C2/  ← skipped (wrong zone)
  ✗  year=2026/month=05/                 ← skipped (wrong month)

Without partitioning: scan all files
With partitioning:    scan 1 file out of hundreds
```

---

## 7. Ingestion Modes

### Batch vs Streaming Trade-offs

```
                    BATCH                        STREAMING
                    ─────                        ─────────
Latency             Minutes to hours             Seconds (flush_every N frames)
Throughput          Higher (sort before write)   Lower (many small writes)
File quality        Large, well-compressed        Small, fragmented files
Use case            Historical backfill           Live machine feed
Parquet files       Few, full files              Many small files → needs compaction
Sort order          Sorted by timestamp           Arrival order
```

### Deduplication

Every record gets a deterministic ID before writing:

```python
record_id = SHA-1(serial_number + timestamp_ms + machine_id)
```

Re-running an ingestion job after a failure produces the same IDs.
Duplicate IDs are skipped — writes are safe to retry.

In production this set would live in **Redis** or **DynamoDB** so multiple
ingestion workers can share state. The current implementation uses a local
text file (single-process only).

---

## 8. Dead-Letter Queue

```
Frame arrives
     │
     ├─► Decode fails (bad CRC, wrong length, unknown version)
     │         │
     │         └──► dlq/stream/dlq_20260601_22.ndjson
     │               {
     │                 "rejected_at": "2026-06-01T22:14:05Z",
     │                 "errors": ["CRC mismatch: expected 0x530653fc"],
     │                 "record": { "raw_bytes_b64": "V0RIR...", ... }
     │               }
     │
     └─► Validation fails (temp out of range, stale timestamp)
               │
               └──► dlq/stream/dlq_20260601_22.ndjson
                     {
                       "rejected_at": "2026-06-01T22:14:07Z",
                       "errors": ["temperature_c=999.0 out of range [-10.0, 85.0]"],
                       "record": { "serial_number": "WDC123...", ... }
                     }
```

**Why this matters:** A firmware bug might cause machines to emit temperatures
in Fahrenheit instead of Celsius. Without a DLQ those records silently vanish.
With a DLQ, ops sees a spike in rejections, identifies the firmware version,
fixes the bug, and replays the DLQ records through the pipeline.

---

## 9. API Design

```
Client (browser)          FastAPI (api.py)           DuckDB → S3
────────────────          ───────────────            ──────────
                          credentials stay
                          server-side — never
                          exposed to browser

GET /api/summary     ──►  summary_stats()       ──►  SELECT COUNT(*), AVG(...)
                     ◄──  { unique_drives: 500 }

GET /api/zone-heatmap ──► zone_failure_heatmap() ──► GROUP BY zone, test_phase
                      ◄── [ { zone: "A1", ... } ]

GET /api/recent-failures?hours=24
                      ──► recent_failures(24)    ──►  WHERE timestamp > NOW()-24h
                      ◄── [ { serial: "WDC...", status: "fail" } ]
```

All endpoints return JSON. FastAPI auto-converts Python dicts/lists.
CORS is open (`allow_origins=["*"]`) for the demo — in production this would
be locked to the internal dashboard domain.

---

## 10. Security Considerations

| Risk | Mitigation |
|---|---|
| AWS credentials leaked to browser | API layer keeps all credentials server-side |
| .env committed to git | .gitignore excludes .env |
| CRC bypass / malformed frames | Magic byte + CRC32 check before any field parsing |
| Re-ingestion creates duplicates | SHA-1 record_id deduplication |
| Stale / future-dated records | Timestamp validation in validator.py |

---

## 11. Production Scaling Path

```
Current (demo)                    Production at WD scale
──────────────                    ──────────────────────
Single Python process             Multiple ingestion workers (Kubernetes pods)
Local dedup index file            Redis dedup set (shared across workers)
Uvicorn --reload                  Gunicorn + Nginx reverse proxy
DuckDB reads S3 directly          Same — DuckDB scales vertically
                                  OR Athena for concurrent multi-user queries
Manual pipeline runs              Apache Airflow DAG (scheduled ingestion)
Local DLQ files                   DLQ alerts to PagerDuty / Slack

Data volume: ~500 records/run     Data volume: millions/day across global factories
```

---

## 12. Tech Stack Summary

| Layer | Technology | Why |
|---|---|---|
| Binary encoding | Python `struct` + CRC32 | Matches real embedded system output |
| Object storage | Amazon S3 | Cheap, durable, scales to petabytes |
| File format | Parquet + Snappy | Columnar, compressed, self-describing schema |
| Query engine | DuckDB | In-process, reads S3 directly, no cluster needed |
| API framework | FastAPI + Uvicorn | Async, auto-generates OpenAPI docs, fast |
| Frontend | Vanilla JS + Chart.js | No build step, runs in any browser |
| Dead-letter queue | NDJSON files (→ Redis in prod) | Append-only, inspectable, replayable |
| Deduplication | SHA-1 record_id set (→ Redis in prod) | Idempotent writes, crash-safe |

---

## 13. File Structure

```
hdd_pipeline/
├── generator.py       Simulates machine binary frame output
├── decoder.py         CRC32 verify + struct unpack → HddRecord
├── validator.py       Business rule validation + DeadLetterQueue
├── storage.py         Partitioned Parquet writes (local + S3)
├── ingestion.py       Batch and streaming orchestration
├── analytics.py       DuckDB queries (local and S3 mode)
├── api.py             FastAPI REST endpoints
├── config.py          Reads .env — AWS creds, storage mode
├── pipeline.py        End-to-end demo runner
├── requirements.txt
├── .env               AWS credentials (git-ignored)
├── .env.example       Template for new developers
├── .gitignore
├── dashboard/
│   └── index.html     Frontend — Chart.js dashboard
├── data/              Local Parquet files (git-ignored)
└── dlq/               Dead-letter queue files (git-ignored)
```
