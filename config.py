"""
Central config — reads from environment variables or a .env file.

Copy .env.example to .env and fill in your AWS credentials.
Never commit .env to git.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")

# AWS
AWS_ACCESS_KEY_ID     = os.environ.get("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
AWS_REGION            = os.environ.get("AWS_REGION", "us-east-1")
S3_BUCKET             = os.environ.get("S3_BUCKET", "")
S3_PREFIX             = os.environ.get("S3_PREFIX", "hdd-data")

# Storage mode: "local" uses ./data/, "s3" uploads to S3
STORAGE_MODE = os.environ.get("STORAGE_MODE", "local")

# Local paths (always used for temp writes before S3 upload)
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
DLQ_DIR  = BASE_DIR / "dlq"


def s3_configured() -> bool:
    return bool(AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY and S3_BUCKET)
