"""Load analysis results into Postgres (schema `analytics`) for the Grafana "Batch Analytics" dashboard.

Every CSV in analysis/results becomes a table analytics.<file name> (replaced on each run). Wide tables
from A1/A2 are also stored in long form (`*_long`) because Grafana bar charts are easier to drive that way.
analytics.dataset_inventory lists every Parquet table with its row count and size.

Usage: uv run python -m dashboards.load_results
"""
import io

import pandas as pd
import psycopg
import pyarrow.dataset as ds

from analysis.style import RESULTS_DIR
from common.paths import DATA

PG = "postgresql://bda:bda@localhost:5432/torrent"
LONG = {  # wide CSV -> (id column, name for the melted column)
    "a1_movie_resolution_share": ("year", "resolution"),
    "a2_first_upload_source_mix": ("release_year", "source"),
}


def pg_type(s: pd.Series) -> str:
    if pd.api.types.is_bool_dtype(s):
        return "boolean"
    if pd.api.types.is_integer_dtype(s):
        return "bigint"
    if pd.api.types.is_float_dtype(s):
        return "double precision"
    if pd.api.types.is_datetime64_any_dtype(s):
        return "timestamptz"
    return "text"


def load(cur, name: str, df: pd.DataFrame):
    # *_ist columns are local-time copies for matplotlib; Grafana converts the UTC column itself
    df = df.drop(columns=[c for c in df.columns if c.endswith("_ist")])
    for col in df.columns:  # parse ISO timestamps / dates written by pandas (stored as UTC)
        if pd.api.types.is_string_dtype(df[col]) and df[col].dropna().astype(str).str.match(r"^\d{4}-\d{2}-\d{2}").all() \
                and df[col].notna().any():
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)
    cols = ", ".join(f'"{c}" {pg_type(df[c])}' for c in df.columns)
    cur.execute(f'DROP TABLE IF EXISTS analytics."{name}"')
    cur.execute(f'CREATE TABLE analytics."{name}" ({cols})')
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False)
    with cur.copy(f'COPY analytics."{name}" FROM STDIN WITH (FORMAT csv)') as cp:
        cp.write(buf.getvalue())
    print(f"analytics.{name}: {len(df):,} rows")


def inventory() -> pd.DataFrame:
    rows = []
    for layer in ("staging", "curated", "stream"):
        for p in sorted((DATA / layer).iterdir()):
            if not p.is_dir() or p.name.startswith("_") or p.name in ("rarbg_bronze",) or "jsonl" in p.name:
                continue
            try:
                n = ds.dataset(p, format="parquet", partitioning="hive", exclude_invalid_files=True).count_rows()
            except Exception:  # noqa: BLE001 - not a Parquet dataset
                continue
            size = sum(f.stat().st_size for f in p.rglob("*.parquet"))
            rows.append({"layer": layer, "table_name": p.name, "rows": n, "size_mb": round(size / 1e6, 1)})
    return pd.DataFrame(rows).sort_values("rows", ascending=False)


def main():
    with psycopg.connect(PG) as con, con.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS analytics")
        for csv in sorted(RESULTS_DIR.glob("*.csv")):
            df = pd.read_csv(csv)
            load(cur, csv.stem, df)
            if csv.stem in LONG:
                idc, var = LONG[csv.stem]
                load(cur, csv.stem + "_long", df.melt(id_vars=idc, var_name=var, value_name="share"))
        load(cur, "dataset_inventory", inventory())


if __name__ == "__main__":
    main()
