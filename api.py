"""
FastAPI backend — exposes HddAnalytics queries as JSON endpoints.

The frontend dashboard fetches from these endpoints.
DuckDB runs in-process, reading Parquet from local disk or S3
depending on STORAGE_MODE in .env.

Run:
    uvicorn api:app --reload --port 8000
"""

from pathlib import Path
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

import config
from analytics import HddAnalytics

app = FastAPI(title="HDD Pipeline Analytics")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

# Serve the frontend dashboard from the dashboard/ directory
dashboard_dir = Path(__file__).parent / "dashboard"
app.mount("/static", StaticFiles(directory=str(dashboard_dir)), name="static")


def _get_analytics() -> HddAnalytics:
    if config.STORAGE_MODE == "s3" and config.s3_configured():
        return HddAnalytics(
            s3_bucket      = config.S3_BUCKET,
            s3_prefix      = config.S3_PREFIX,
            aws_access_key = config.AWS_ACCESS_KEY_ID,
            aws_secret_key = config.AWS_SECRET_ACCESS_KEY,
            aws_region     = config.AWS_REGION,
        )
    # Fall back to local Parquet — works even without AWS credentials
    local_dirs = [config.DATA_DIR / "batch", config.DATA_DIR / "stream", config.DATA_DIR]
    data_dir   = next((d for d in local_dirs if d.exists() and list(d.rglob("*.parquet"))), None)
    if data_dir is None:
        raise HTTPException(status_code=503, detail="No data found. Run the pipeline first.")
    return HddAnalytics(data_dir=data_dir)


@app.get("/")
def serve_dashboard():
    return FileResponse(dashboard_dir / "index.html")


@app.get("/api/summary")
def summary():
    an = _get_analytics()
    df = an.summary_stats()
    return df.to_dict(orient="records")[0]


@app.get("/api/fail-rate-by-phase")
def fail_rate_by_phase():
    an = _get_analytics()
    return an.fail_rate_by_phase().to_dict(orient="records")


@app.get("/api/throughput-by-firmware")
def throughput_by_firmware():
    an = _get_analytics()
    return an.throughput_by_firmware().to_dict(orient="records")


@app.get("/api/zone-heatmap")
def zone_heatmap():
    an = _get_analytics()
    return an.zone_failure_heatmap().to_dict(orient="records")


@app.get("/api/reallocated-trend")
def reallocated_trend():
    an = _get_analytics()
    df = an.reallocated_sector_trend()
    df["date"] = df["date"].astype(str)
    return df.to_dict(orient="records")


@app.get("/api/vibration-outliers")
def vibration_outliers():
    an = _get_analytics()
    df = an.vibration_outliers()
    df["timestamp_utc"] = df["timestamp_utc"].astype(str)
    return df.to_dict(orient="records")


@app.get("/api/recent-failures")
def recent_failures(hours: int = 24):
    an = _get_analytics()
    df = an.recent_failures(hours=hours)
    df["timestamp_utc"] = df["timestamp_utc"].astype(str)
    return df.to_dict(orient="records")


@app.get("/api/hot-machines")
def hot_machines(threshold: float = 45.0):
    an = _get_analytics()
    return an.hot_machines(threshold_c=threshold).to_dict(orient="records")
