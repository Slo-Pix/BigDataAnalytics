"""Data lake paths (no heavy imports, safe to use from lightweight containers)."""
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = Path(os.environ.get("BDA_DATA", ROOT / "data"))
RAW, STAGING, CURATED, STREAM = (DATA / d for d in ("raw", "staging", "curated", "stream"))
