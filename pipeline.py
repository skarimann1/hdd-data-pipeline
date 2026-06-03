"""
End-to-end demo of the HDD manufacturing data pipeline.

Run:
    python pipeline.py                  # full demo
    python pipeline.py --mode batch     # batch ingestion only
    python pipeline.py --mode stream    # streaming ingestion only
    python pipeline.py --mode query     # analytics queries only (needs existing data)
    python pipeline.py --frames 2000    # control how many frames to generate
    python pipeline.py --clean          # wipe data dir and start fresh
"""

import argparse
import logging
import shutil
import sys
from pathlib import Path

# ── paths ──────────────────────────────────────────────────────────────────
BASE_DIR  = Path(__file__).parent
DATA_DIR  = BASE_DIR / "data"
DLQ_DIR   = BASE_DIR / "dlq"

# ── logging ────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("pipeline")

SEPARATOR = "─" * 60


def section(title: str) -> None:
    print(f"\n{SEPARATOR}")
    print(f"  {title}")
    print(SEPARATOR)


# ── demo stages ────────────────────────────────────────────────────────────

def demo_batch(n_frames: int) -> None:
    from generator import batch_frames
    from ingestion import run_batch_ingestion

    section(f"BATCH INGESTION  ({n_frames:,} frames, 7-day window)")
    frames = list(batch_frames(count=n_frames, days_back=7))
    print(f"  Generated {len(frames):,} base64-encoded binary frames")

    stats = run_batch_ingestion(frames, DATA_DIR / "batch", DLQ_DIR / "batch")
    print(stats)


def demo_streaming(n_frames: int) -> None:
    from generator import stream_frames
    from ingestion import run_streaming_ingestion

    section(f"STREAMING INGESTION  ({n_frames:,} frames, live feed)")
    stats = run_streaming_ingestion(
        stream_frames(count=n_frames, corrupt_rate=0.03, duplicate_rate=0.02),
        DATA_DIR / "stream",
        DLQ_DIR / "stream",
        flush_every=100,
    )
    print(stats)


def demo_dlq() -> None:
    from validator import DeadLetterQueue

    section("DEAD-LETTER QUEUE  (rejected records)")
    for label, sub in [("batch", "batch"), ("stream", "stream")]:
        dlq_path = DLQ_DIR / sub
        if not dlq_path.exists():
            continue
        dlq = DeadLetterQueue(dlq_path)
        entries = dlq.all_entries()
        if not entries:
            print(f"  [{label}] no DLQ entries")
            continue
        print(f"\n  [{label}] {len(entries)} rejected records")
        # Show first 3 for illustration
        for e in entries[:3]:
            print(f"    serial={e['record']['serial_number']}"
                  f"  errors={e['errors']}")
        if len(entries) > 3:
            print(f"    ... and {len(entries) - 3} more")


def demo_analytics() -> None:
    from analytics import HddAnalytics

    # Merge both ingestion outputs into one view by pointing at the parent
    section("ANALYTICS QUERIES  (DuckDB over Parquet)")

    # Try batch dir first; fall back to stream; try parent as combined view
    dirs_to_try = [DATA_DIR / "batch", DATA_DIR / "stream", DATA_DIR]
    analytics_dir = next(
        (d for d in dirs_to_try
         if d.exists() and list(d.rglob("*.parquet"))),
        None,
    )
    if analytics_dir is None:
        print("  No Parquet data found — run ingestion first.")
        return

    print(f"  Reading from: {analytics_dir}")
    an = HddAnalytics(analytics_dir)

    print("\n── Summary ──────────────────────────────────────")
    print(an.summary_stats().to_string(index=False))

    print("\n── Fail rate by test phase ───────────────────────")
    print(an.fail_rate_by_phase().to_string(index=False))

    print("\n── Throughput by firmware version ───────────────")
    print(an.throughput_by_firmware().to_string(index=False))

    print("\n── Hot machines (avg temp > 48 °C) ──────────────")
    hot = an.hot_machines(threshold_c=48.0)
    if hot.empty:
        print("  None found (lower the threshold to see results)")
    else:
        print(hot.to_string(index=False))

    print("\n── Reallocated sector trend (by day) ────────────")
    trend = an.reallocated_sector_trend()
    if trend.empty:
        print("  No drives with reallocated sectors found")
    else:
        print(trend.to_string(index=False))

    print("\n── Zone failure heatmap ─────────────────────────")
    print(an.zone_failure_heatmap().to_string(index=False))

    print("\n── Vibration outliers (z > 2.5σ) ────────────────")
    outliers = an.vibration_outliers(sigma=2.5)
    if outliers.empty:
        print("  No outliers found")
    else:
        print(outliers.head(10).to_string(index=False))


def demo_partition_layout() -> None:
    section("PARTITION LAYOUT  (Hive-style directory tree)")
    parquet_files = sorted(DATA_DIR.rglob("*.parquet"))
    if not parquet_files:
        print("  No data written yet.")
        return
    print(f"  {len(parquet_files)} Parquet files written:\n")
    for f in parquet_files[:20]:
        rel = f.relative_to(DATA_DIR)
        size_kb = f.stat().st_size / 1024
        print(f"  {rel}  ({size_kb:.1f} KB)")
    if len(parquet_files) > 20:
        print(f"  ... and {len(parquet_files) - 20} more files")


# ── main ───────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(description="HDD Manufacturing Data Pipeline Demo")
    p.add_argument("--mode",   choices=["all", "batch", "stream", "query"],
                   default="all")
    p.add_argument("--frames", type=int, default=800,
                   help="Number of frames to generate per ingestion mode")
    p.add_argument("--clean",  action="store_true",
                   help="Delete data/ and dlq/ directories before running")
    return p.parse_args()


def main():
    args = parse_args()

    if args.clean:
        for d in [DATA_DIR, DLQ_DIR]:
            if d.exists():
                shutil.rmtree(d)
                print(f"  Removed {d}")

    print("\n" + "═" * 60)
    print("  HDD MANUFACTURING DATA PIPELINE  —  DEMO")
    print("═" * 60)
    print(f"  data dir : {DATA_DIR}")
    print(f"  dlq dir  : {DLQ_DIR}")
    print(f"  frames   : {args.frames:,} per mode")

    if args.mode in ("all", "batch"):
        demo_batch(args.frames)

    if args.mode in ("all", "stream"):
        demo_streaming(args.frames)

    if args.mode in ("all",):
        demo_dlq()
        demo_partition_layout()

    if args.mode in ("all", "query"):
        demo_analytics()

    print(f"\n{'═' * 60}")
    print("  Pipeline demo complete.")
    print("═" * 60 + "\n")


if __name__ == "__main__":
    main()
