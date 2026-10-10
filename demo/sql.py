"""Interactive Spark SQL console over the whole data lake, for the live demo.

Every Parquet table in dataset/staging, dataset/curated and dataset/stream is registered as a view under its
folder name (wiki_pageviews, torrents, torrents_enriched, swarm, swarm_fullscan, ...). The live demo stream's
output is `demo_swarm`; it is re-read before every query that mentions it, so repeated queries show new rows.

Commands:  \\tables   list views and row counts      \\list   prepared queries (demo/queries.sql)
           \\run N    run prepared query N           \\q      quit
Anything else is SQL; end it with `;` (it may span lines).

Usage: uv run python -m demo.sql
"""
import re
import time

from common.spark import CURATED, ROOT, STAGING, STREAM, get_spark

QUERIES = ROOT / "demo" / "queries.sql"
LAKE = {**{p.name: p for d in (STAGING, CURATED) for p in sorted(d.iterdir()) if p.is_dir() and not p.name.startswith("_")},
        "swarm": STREAM / "swarm"}
DEMO_PATH = STREAM / "demo"


def prepared() -> list[tuple[str, str]]:
    """[(title, sql)] from demo/queries.sql: each block starts with a `-- N. title` comment."""
    out, title, lines = [], None, []
    for line in QUERIES.read_text().splitlines():
        m = re.match(r"--\s*(\d+)\.\s*(.*)", line)
        if m:
            if title:
                out.append((title, "\n".join(lines).strip()))
            title, lines = f"{m.group(1)}. {m.group(2)}", []
        elif title and not line.startswith("--"):
            lines.append(line)
    if title:
        out.append((title, "\n".join(lines).strip()))
    return out


def run(spark, sql: str):
    sql = sql.strip().rstrip(";")
    if "demo_swarm" in sql:
        if not DEMO_PATH.exists():
            print("demo stream not running yet: start it with demo/live_demo.sh start")
            return
        spark.read.parquet(str(DEMO_PATH)).createOrReplaceTempView("demo_swarm")
    t0 = time.time()
    try:
        df = spark.sql(sql)
        rows = df.limit(50).collect()
    except Exception as e:  # noqa: BLE001 - show any SQL error and keep the console alive
        print(f"error: {str(e).splitlines()[0]}")
        return
    spark.createDataFrame(rows, df.schema).show(50, truncate=60) if rows else print("(no rows)")
    print(f"{len(rows)} row(s) in {time.time() - t0:.1f} s")


def main():
    print("starting Spark (local[*]) ...")
    spark = get_spark("demo-sql", mem="5g")
    spark.sparkContext.setLogLevel("ERROR")
    for name, path in LAKE.items():
        spark.read.parquet(str(path)).createOrReplaceTempView(name)
    print(f"{len(LAKE)} tables registered. \\tables, \\list, \\run N, \\q — or type SQL ending with ;\n")
    buf = []
    while True:
        try:
            line = input("spark-sql> " if not buf else "      ...> ")
        except (EOFError, KeyboardInterrupt):
            break
        cmd = line.strip()
        if not buf and cmd in ("\\q", "quit", "exit"):
            break
        if not buf and cmd == "\\tables":
            for name in LAKE:
                print(f"  {name:28} {spark.table(name).count():>15,} rows")
            continue
        if not buf and cmd == "\\list":
            for title, _ in prepared():
                print(" ", title)
            continue
        if not buf and cmd.startswith("\\run"):
            qs = dict((t.split(".")[0], (t, s)) for t, s in prepared())
            key = cmd.split()[-1]
            if key in qs:
                print(f"-- {qs[key][0]}\n{qs[key][1]}\n")
                run(spark, qs[key][1])
            else:
                print("unknown query number; \\list shows them")
            continue
        buf.append(line)
        if cmd.endswith(";"):
            run(spark, "\n".join(buf))
            buf = []
    spark.stop()


if __name__ == "__main__":
    main()
